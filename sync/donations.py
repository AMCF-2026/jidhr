"""
Donations Sync
==============
Syncs donation aggregates from CSuite to HubSpot contact properties.

Matching: CSuite profile.primary_email → HubSpot contact.email

HubSpot Properties Updated:
- lifetime_giving: Total donation amount
- last_donation_date: Most recent donation date
- last_donation_amount: Most recent donation amount  
- donation_count: Number of donations
- csuite_profile_id: CSuite profile ID (for future direct linking)
"""

import logging
import time
from datetime import datetime
from collections import defaultdict
from clients.csuite import CSuiteClient
from clients.hubspot import HubSpotClient
from sync.profile_state import (PROFILE_EXISTS, PROFILE_MISSING,
                                PROFILE_UNREADABLE, ProfileStateCache)

logger = logging.getLogger(__name__)

# How many profiles and donations a `quick` run looks at.
SAMPLE_SIZE = 500

# HubSpot's Search API is capped at **5 requests per second across all object
# types**, separately from the per-app burst limit (100/10s on Free and
# Starter, 190/10s on Professional and Enterprise) and regardless of tier.
# Documented at
#   https://developers.hubspot.com/docs/developer-tooling/platform/usage-guidelines
#   https://developers.hubspot.com/docs/api/usage-details
# Search responses carry no rate-limit headers, so there is nothing to read
# back: the only way to stay under it is to pace the calls.
#
# This sync searches once per donor profile with an email — 7,604 of them
# measured against the mirror on 2026-10-06 — so unpaced it would exceed the
# cap within the first second and keep exceeding it for 25 minutes.
#
# 4/s, not 5/s, deliberately: the limit is enforced per second with no credit
# for an idle second, and anything else in the portal doing a search at the
# same moment shares the same cap.
HUBSPOT_SEARCH_PER_SECOND = 4.0

# A 429 on a search is waited out and retried, three times. Longer than
# CSuite's 30/60/120 would be pointless — a secondly limiter clears in a
# second — but a flat retry with no pause just spends another request.
SEARCH_BACKOFFS = (1.0, 2.0, 4.0)


def hubspot_error(response):
    """The error in a HubSpot response, or None.

    HubSpot answers a rate-limited search with HTTP 429 and a JSON body:
    {"status": "error", "errorType": "RATE_LIMIT", ...}. clients/hubspot's
    _parse_response returns that body verbatim, so it carries NO "error" key
    and no status_code — which meant _read_contact saw a dict with no
    "results" and reported "no such contact". A throttled search read as a
    donor who is not in HubSpot.
    """
    if not isinstance(response, dict):
        return f"unreadable response: {type(response).__name__}"
    if "error" in response:
        return str(response["error"])[:200]
    if response.get("status") == "error" or response.get("errorType"):
        return (f"{response.get('errorType') or 'error'}: "
                f"{str(response.get('message') or '')[:160]}")
    status = response.get("status_code")
    if isinstance(status, int) and status >= 400:
        return f"HTTP {status}"
    return None


def is_rate_limited(response) -> bool:
    if not isinstance(response, dict):
        return False
    if response.get("errorType") == "RATE_LIMIT":
        return True
    if response.get("status_code") == 429:
        return True
    return "rate limit" in str(response.get("error") or "").lower()


class SearchPacer:
    """Keeps searches under HUBSPOT_SEARCH_PER_SECOND, and retries a 429."""

    def __init__(self, per_second: float = HUBSPOT_SEARCH_PER_SECOND,
                 sleeper=None, clock=None):
        self.interval = 1.0 / per_second if per_second else 0.0
        self._sleep = sleeper or time.sleep
        self._clock = clock or time.monotonic
        self._last = None
        self.waits = 0
        self.rate_limit_waits = 0

    def wait(self):
        if self.interval <= 0:
            return
        if self._last is not None:
            due = self._last + self.interval - self._clock()
            if due > 0:
                self.waits += 1
                self._sleep(due)
        self._last = self._clock()

    def backoff(self, attempt: int) -> bool:
        """Pause before retry `attempt`. False when the retries are spent."""
        if attempt >= len(SEARCH_BACKOFFS):
            return False
        self.rate_limit_waits += 1
        self._sleep(SEARCH_BACKOFFS[attempt])
        self._last = self._clock()
        return True


class PartialReadRefused(RuntimeError):
    """A CSuite sweep did not finish, so a live run was abandoned.

    A short donation list produces UNDERSTATED lifetime_giving totals, and
    writing those over the correct ones is worse than not writing at all —
    the contact then reads as a smaller donor than they are, with nothing
    saying the figure is wrong. A preview carries on and is labelled partial;
    a write does not.
    """


class DonationSyncDisabled(RuntimeError):
    """A live donation sync was asked for while the flag is off.

    Raised, not returned as a result with zero updates. "0 contacts updated"
    is also what a successful run over an empty CSuite looks like, and a
    refusal that reads as a clean run is the failure mode this repo has spent
    three weeks removing.
    """


def donation_sync_allowed() -> bool:
    """Is a WRITING donation sync permitted? Reads config at call time.

    Not at import: Config is re-read by tests and by anything that reloads the
    module, and a flag captured at import is a flag that answers for the
    environment as it was, not as it is.
    """
    import config
    return bool(getattr(config.Config, "CSUITE_DONATION_SYNC_ENABLED", False))


# What happened to csuite_profile_id for one contact, and what to say about it.
class LinkDecision:
    """Whether this contact's csuite_profile_id may be written, and why not.

    The four donation fields are written in every case — the money is right
    even when the link is disputed (decision 2026-10-06). Only the LINK is
    withheld, and never silently: every withheld link is counted and logged
    against the contact id.
    """

    def __init__(self, write_link: bool, counter: str, action: str,
                 stored=None, state=None):
        self.write_link = write_link
        self.counter = counter
        self.action = action
        self.stored = stored
        self.state = state

    def log(self, contact_id, profile_id):
        logger.warning(
            "contact %s: csuite_profile_id NOT written — stored %s, donations "
            "say %s (%s). The four donation fields were still written.",
            contact_id, self.stored, profile_id, self.action)


class DonationSync:
    """Sync donation data from CSuite to HubSpot"""
    
    def __init__(self, pace_ms=None, progress=None):
        self.csuite = CSuiteClient()
        self.hubspot = HubSpotClient()
        self.pace_ms = pace_ms
        # A callable taking one line of text, or None. The CLI prints it; the
        # chat path has nowhere to put it.
        self.progress = progress
        self.reset_counters()

    def reset_counters(self):
        self.csuite_calls = 0
        self.hubspot_searches = 0
        # Counted per system. "We were throttled" is not useful; "CSuite
        # throttled us twice and HubSpot forty times" says which pacing to
        # change.
        self.rate_limit_waits = 0            # CSuite
        self.profiles_complete = True
        self.donations_complete = True
        self.profiles_error = None
        self.donations_error = None
        self.searches = SearchPacer()        # carries its own HubSpot waits

    def _report(self, line: str):
        if self.progress:
            self.progress(line)
    
    def get_profile_emails(self, limit: int = None) -> dict:
        """{profile_id: primary_email} from CSuite, with `complete` recorded.

        Until 2026-10-06 this was a hand-rolled `while True` whose only
        failure branch was `if not result.get("success"): break` — so a page
        that FAILED was indistinguishable from the end of the data. A 429
        ended the sweep, the caller got a short dict, and nothing said so.
        ~18,800 profiles paged 100 at a time is 189 unpaced calls.

        clients.csuite_fetch.fetch_all already solves this: it paces, waits
        out a 429 (Retry-After if CSuite sends one, else 30s/60s/120s, three
        waits) and reports `complete`. sync/mirror.py refuses to write
        anything at all when it sees complete=False, and this now does the
        same.

        Args:
            limit: stop after roughly this many profiles (None = all)
        """
        from clients.csuite_fetch import fetch_all

        pages = max(1, -(-limit // 100)) if limit else None
        result = fetch_all(self.csuite, "profile/list", pace_ms=self.pace_ms,
                           **({"max_pages": pages} if pages else {}))

        # A capped sweep stops early on purpose, so "did not reach the end"
        # is only a failure when nothing asked it to stop.
        self.profiles_complete = bool(result.complete or pages)
        self.profiles_error = None if self.profiles_complete else (
            result.error or "the profile sweep did not finish")
        if not self.profiles_complete:
            logger.error("CSuite profile sweep INCOMPLETE after %d row(s): "
                         "%s", len(result.records), self.profiles_error)

        rows = result.records[:limit] if limit else result.records
        profile_emails = {}
        for profile in rows:
            profile_id = profile.get("profile_id")
            email = profile.get("primary_email")
            if profile_id is not None and email:
                profile_emails[profile_id] = email

        self.csuite_calls += result.calls
        self.rate_limit_waits += result.total_429s
        logger.info("Fetched %d profiles (%d with emails) in %d call(s), "
                    "%d rate-limit wait(s)", len(rows), len(profile_emails),
                    result.calls, result.total_429s)
        self._report(f"profiles: {len(rows):,} read, "
                     f"{len(profile_emails):,} with an email")
        return profile_emails

    def aggregate_donations(self, donations: list) -> dict:
        """Aggregate donations by profile_id
        
        Returns:
            dict: {profile_id: {
                'total': float,
                'count': int,
                'last_date': str,
                'last_amount': float
            }}
        """
        aggregates = defaultdict(lambda: {
            'total': 0.0,
            'count': 0,
            'last_date': None,
            'last_amount': 0.0,
            'donations': []
        })
        
        for donation in donations:
            profile_id = donation.get("profile_id")
            if not profile_id:
                continue
            
            amount_str = donation.get("donation_amount", "0")
            try:
                amount = float(amount_str)
            except (ValueError, TypeError):
                amount = 0.0
            
            date_str = donation.get("donation_date", "")
            
            agg = aggregates[profile_id]
            agg['total'] += amount
            agg['count'] += 1
            agg['donations'].append({
                'amount': amount,
                'date': date_str
            })
        
        # Calculate last donation for each profile
        for profile_id, agg in aggregates.items():
            if agg['donations']:
                # Sort by date descending
                sorted_donations = sorted(
                    agg['donations'],
                    key=lambda x: x['date'] or '',
                    reverse=True
                )
                agg['last_date'] = sorted_donations[0]['date']
                agg['last_amount'] = sorted_donations[0]['amount']
            
            # Clean up - don't need full list anymore
            del agg['donations']
        
        return dict(aggregates)
    
    def format_date_for_hubspot(self, date_str: str) -> str:
        """Convert CSuite date to HubSpot format (midnight UTC)"""
        if not date_str:
            return None
        
        try:
            # CSuite format: YYYY-MM-DD
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            # HubSpot wants midnight UTC
            return dt.strftime("%Y-%m-%dT00:00:00.000Z")
        except ValueError:
            logger.warning(f"Invalid date format: {date_str}")
            return None
    
    def _read_contact(self, email: str):
        """(contact dict, error). Both None-able, and they mean different
        things: (None, None) is "no such contact", (None, "…") is "the lookup
        failed". A failed read must not be counted as an absent contact — see
        clients/hubspot.TicketLookupFailed for the same distinction."""
        found = None
        for attempt in range(len(SEARCH_BACKOFFS) + 1):
            self.searches.wait()
            try:
                found = self.hubspot.search_contact_by_email(email)
            except Exception as e:
                return None, str(e)
            self.hubspot_searches += 1
            if not is_rate_limited(found):
                break
            if not self.searches.backoff(attempt):
                return None, ("HubSpot rate limited the contact search and "
                              "did not relent after "
                              f"{len(SEARCH_BACKOFFS)} waits")

        error = hubspot_error(found)
        if error:
            return None, error
        rows = found.get("results")
        if not (isinstance(rows, list) and rows):
            return None, None
        row = rows[0]
        if not row.get("id"):
            return None, "a contact came back with no id"
        return row, None

    def _link_decision(self, contact: dict, profile_id, states,
                       resolve: bool = True) -> LinkDecision:
        """May this contact's csuite_profile_id be overwritten?

        Until 2026-10-06 the answer was always yes: the sync wrote
        csuite_profile_id on every matched contact from an email match,
        without reading what was there. The DAF duplicate guard reads that
        field to decide whether a donor already has a profile, and 68
        production contacts carry ids that do not resolve — so a wrong value
        here makes the guard refuse a real donor, or point staff at a profile
        that is not theirs.

        `resolve=False` skips the CSuite read, for a dry run that resolves
        existence only for the rows it shows.
        """
        props = contact.get("properties") or {}
        stored = (props.get("csuite_profile_id") or "").strip() or None

        if not stored:
            return LinkDecision(True, "link_written", "write", stored)
        if stored == str(profile_id):
            # Writing it would be a no-op that still costs an audit row.
            return LinkDecision(False, "link_unchanged", "same", stored,
                                PROFILE_EXISTS)
        if not resolve:
            # Dry run: the comparison is free, the verdict is not. Its own
            # counter, because calling it a conflict would claim the stored id
            # resolves to a live profile — which is exactly what has not been
            # checked yet.
            return LinkDecision(False, "link_differs", "differs", stored)

        state, _record = states.state(stored)
        if state == PROFILE_MISSING:
            # Stale. Counted, never repointed: overwriting it destroys the
            # only record of what it pointed at, and a backfill is its own
            # job with its own flag (decision 2026-10-06).
            return LinkDecision(False, "link_stale", "stale", stored, state)
        if state == PROFILE_UNREADABLE:
            return LinkDecision(False, "link_unverifiable", "unverifiable",
                                stored, state)
        # Two live profiles for one donor is a merge, not a sync decision.
        return LinkDecision(False, "link_conflict", "conflict", stored, state)

    def resolve_shown_links(self, results: dict, limit: int = 25) -> list:
        """Verify the proposed ids for the rows the report will SHOW.

        Only those: the column exists to make a sample inspectable, and
        resolving every row would turn a two-call dry run into one
        profile/display per contact. The report labels the column accordingly,
        because a verdict on 25 rows is not a verdict on the run.
        """
        rows = (results.get("link_rows") or [])[:max(limit, 0)]
        states = ProfileStateCache(self.csuite)
        for row in rows:
            state, _record = states.state(row["proposed"])
            row["proposed_exists"] = {
                PROFILE_EXISTS: "yes",
                PROFILE_MISSING: "NO — not in CSuite",
            }.get(state, "unknown — CSuite would not say")
            if row.get("current"):
                current_state, _ = states.state(row["current"])
                row["current_exists"] = {
                    PROFILE_EXISTS: "yes",
                    PROFILE_MISSING: "NO — stale",
                }.get(current_state, "unknown")
            else:
                row["current_exists"] = ""
        results["profile_reads"] = states.reads
        return rows

    def sync(self, dry_run: bool = False, quick: bool = False) -> dict:
        """Run the full donation sync
        
        Args:
            dry_run: If True, don't actually update HubSpot
            quick: If True, only process a sample (faster for testing)
        
        Returns:
            dict: Sync results with stats
        """
        # The gate is HERE, not in the chat handler, because the chat handler
        # is not the only way in: "sync all" calls run_donation_sync(
        # dry_run=False) directly (intents/sync_commands._run_all_syncs), so a
        # check in _sync_donations alone would be bypassed by typing "sync
        # all". Every path that can write passes through this method.
        #
        # A dry run is deliberately NOT gated: it writes nothing to HubSpot,
        # and the preview is what the decision to enable should be made on.
        if not dry_run and not donation_sync_allowed():
            logger.warning(
                "donation sync REFUSED: CSUITE_DONATION_SYNC_ENABLED is off. "
                "Nothing was read and nothing was written. Run it with "
                "'dry run' to preview, which is not gated.")
            raise DonationSyncDisabled(
                "CSUITE_DONATION_SYNC_ENABLED is off, so no HubSpot writes "
                "were made. Say \"sync donations dry run\" to preview it "
                "safely, or set the flag to run it for real.")

        results = {
            'updated': 0,
            'skipped_no_email': 0,
            'skipped_not_found': 0,
            'errors': 0,
            'details': [],
            # csuite_profile_id outcomes. The four donation fields are written
            # in every one of these cases; only the LINK is withheld.
            'link_written': 0,        # the contact had none
            'link_unchanged': 0,      # stored id already equals the proposed
            'link_conflict': 0,       # stored id resolves to a live profile
            'link_differs': 0,        # differs, and not checked (dry run)
            'link_stale': 0,          # stored id resolves to nothing
            'link_unverifiable': 0,   # CSuite would not say
            # One HubSpot contact claimed by more than one CSuite profile.
            # Nothing at all is written to it — see the loop.
            'shared_email': 0,
            'shared_email_rows': [],
            # One row per contact a live run would PATCH. Dry run only.
            'link_rows': [],
            'profile_reads': 0,
            # False when the run paged everything, so a report cannot call a
            # sample a total.
            'sampled': True,
            # What was actually looked at, so a footer can state the cost
            # rather than estimate it.
            'profiles_read': 0,
            'donations_read': 0,
            # True when a CSuite sweep did not reach the end. A partial read
            # is never reported as a total.
            'partial': False,
            'partial_reason': None,
            'csuite_calls': 0,
            'hubspot_searches': 0,
            'csuite_rate_limit_waits': 0,
            'hubspot_rate_limit_waits': 0,
        }
        self.reset_counters()
        
        # Sampling is `quick`'s job, and only `quick`'s.
        #
        # It used to be `dry_run or quick`, which made every preview a sample
        # of 500 profiles and 500 donations and left no way to preview the run
        # that would actually happen. The counts that matter before enabling
        # this — how many contacts are claimed by two CSuite profiles, how
        # many stored links are stale — cannot be read off a sample of 500 out
        # of ~18,800. A preview you have to extrapolate from is not a preview.
        profile_limit = SAMPLE_SIZE if quick else None
        donation_limit = SAMPLE_SIZE if quick else None
        results['sampled'] = bool(quick)
        
        logger.info(f"Starting donation sync... (dry_run={dry_run}, quick={quick})")
        
        # Step 1: Get profile email mapping
        logger.info("Step 1: Getting profile emails from CSuite...")
        profile_emails = self.get_profile_emails(limit=profile_limit)
        results['profiles_read'] = len(profile_emails or {})
        
        if not profile_emails:
            logger.error("No profile emails found")
            results['details'].append("No profiles with emails found in CSuite")
            results['csuite_calls'] = self.csuite_calls
            if not self.profiles_complete:
                # "No profiles" and "we never finished asking" are different
                # answers, and only one of them means CSuite is empty.
                results['partial'] = True
                results['partial_reason'] = self.profiles_error
                if not dry_run:
                    raise PartialReadRefused(
                        f"the CSuite profile sweep did not finish "
                        f"({self.profiles_error}). Nothing was written.")
            return results
        
        # Step 2: Get donations
        logger.info("Step 2: Getting donations from CSuite...")
        donations = self.get_donations_with_limit(limit=donation_limit)
        
        if not donations:
            logger.warning("No donations found")
            results['details'].append("No donations found in CSuite")
            return results
        
        results['donations_read'] = len(donations)

        # Both sweeps have run. If either stopped short, say so now — before
        # any aggregate is computed from a list that is missing rows.
        if not (self.profiles_complete and self.donations_complete):
            reason = "; ".join(
                part for part in (self.profiles_error, self.donations_error)
                if part)
            results['partial'] = True
            results['partial_reason'] = reason
            logger.error("CSuite read INCOMPLETE: %s", reason)
            if not dry_run:
                # Nothing has been written yet: the write loop is below.
                raise PartialReadRefused(
                    f"a CSuite sweep did not finish ({reason}), so the "
                    "donation totals would be understated. Nothing was "
                    "written. Re-run when CSuite is answering.")
            self._report(f"PARTIAL: {reason}")
        logger.info(f"Found {len(donations)} donations")
        
        # Step 3: Aggregate by profile
        logger.info("Step 3: Aggregating donations by profile...")
        aggregates = self.aggregate_donations(donations)
        logger.info(f"Aggregated donations for {len(aggregates)} profiles")
        
        # Step 4: Update HubSpot contacts
        logger.info("Step 4: Updating HubSpot contacts...")
        
        states = ProfileStateCache(self.csuite)

        # Which CSuite profiles would land on the same HubSpot contact.
        #
        # Matching is profile.primary_email -> contact.email, and CSuite
        # permits two profiles with the same address. Both then resolve to ONE
        # contact, and the loop below used to process them one after the other:
        # the second PATCH overwrote the first donor's figures with the
        # second's, so the contact ended up showing one profile's giving and
        # nothing recorded that the other existed. Measured on 2026-10-06 with
        # contact 542578284242, profiles 21333 ($5,000, 4 gifts) and 21325
        # ($250, 1 gift): the contact finished with $250 and a donation_count
        # of 1, because 21325 happened to be processed second.
        #
        # Which one won depended on the order donations came back from CSuite.
        #
        # They are NOT summed. Two profiles sharing an address may be one
        # person entered twice or two people in a household, and this sync
        # cannot tell. Summing would invent a donor; writing one of them
        # silently picks a winner. So the contact is left alone and listed.
        from collections import defaultdict

        from sync.readback import normalise_email

        sharers = defaultdict(list)
        for candidate in aggregates:
            key = normalise_email(profile_emails.get(candidate))
            if key:
                sharers[key].append(candidate)
        shared_handled = set()

        for profile_id, agg in aggregates.items():
            email = profile_emails.get(profile_id)
            
            if not email:
                results['skipped_no_email'] += 1
                continue

            key = normalise_email(email)
            claimants = sharers.get(key) or [profile_id]
            if len(claimants) > 1:
                # Counted once per CONTACT, not once per profile: the thing
                # needing a human is the contact.
                if key in shared_handled:
                    continue
                shared_handled.add(key)
                contact, contact_error = self._read_contact(email)
                if contact_error:
                    results['errors'] += 1
                    logger.error("could not read the HubSpot contact shared "
                                 "by profiles %s: %s", claimants,
                                 contact_error)
                    continue
                if contact is None:
                    results['skipped_not_found'] += 1
                    continue
                results['shared_email'] += 1
                results['shared_email_rows'].append({
                    "contact_id": contact["id"],
                    "profiles": [str(p) for p in claimants],
                    "current": ((contact.get("properties") or {})
                                .get("csuite_profile_id") or "").strip(),
                })
                logger.warning(
                    "contact %s is claimed by %d CSuite profiles (%s): "
                    "NOTHING was written to it. Donation totals are not "
                    "summed — merge or separate the profiles in CSuite.",
                    contact["id"], len(claimants),
                    ", ".join(str(p) for p in claimants))
                continue

            # The contact is READ before anything is decided, so the stored
            # csuite_profile_id can be compared instead of overwritten. This
            # used to go straight to update_contact_by_email, which searches
            # and then PATCHes — the search happened and its answer was
            # thrown away. One search either way.
            contact, contact_error = self._read_contact(email)
            if contact_error:
                results['errors'] += 1
                logger.error("could not read the HubSpot contact for a "
                             "donation update: %s", contact_error)
                continue
            if contact is None:
                results['skipped_not_found'] += 1
                logger.debug("contact not found in HubSpot for profile %s",
                             profile_id)
                continue

            # Build HubSpot properties
            properties = {
                'lifetime_giving': str(round(agg['total'], 2)),
                'donation_count': str(agg['count']),
                'last_donation_amount': str(round(agg['last_amount'], 2)),
            }
            
            # Add last donation date if available
            formatted_date = self.format_date_for_hubspot(agg['last_date'])
            if formatted_date:
                properties['last_donation_date'] = formatted_date

            decision = self._link_decision(contact, profile_id, states,
                                           resolve=not dry_run)
            results[decision.counter] += 1
            if decision.write_link:
                properties['csuite_profile_id'] = str(profile_id)
            else:
                decision.log(contact["id"], profile_id)

            if dry_run:
                # One row per contact a live run would PATCH, for the report.
                # Capped where it is rendered, not here: the counts above have
                # to be the real totals or a truncated table reads as the
                # whole picture.
                results['link_rows'].append({
                    "contact_id": contact["id"],
                    "current": decision.stored or "",
                    "proposed": str(profile_id),
                    "action": decision.action,
                })
                logger.debug(f"[DRY RUN] Would update contact "
                             f"{contact['id']}: {sorted(properties)}")
                results['updated'] += 1
                continue
            
            # Update HubSpot
            update_result = self.hubspot.update_contact(contact["id"],
                                                        properties)
            
            if "error" in update_result:
                if "not found" in update_result["error"].lower():
                    results['skipped_not_found'] += 1
                    logger.debug(f"Contact not found in HubSpot: {email}")
                else:
                    results['errors'] += 1
                    logger.error(f"Error updating {email}: {update_result['error']}")
            else:
                results['updated'] += 1
                logger.debug(f"Updated {email}: ${agg['total']:.2f} lifetime")
        
        results['profile_reads'] = states.reads
        results['csuite_calls'] = self.csuite_calls
        results['hubspot_searches'] = self.hubspot_searches
        results['csuite_rate_limit_waits'] = self.rate_limit_waits
        results['hubspot_rate_limit_waits'] = self.searches.rate_limit_waits

        # Summary
        mode = "[DRY RUN] " if dry_run else ""
        mode += "[QUICK] " if quick else ""
        logger.info(f"{mode}Sync complete: {results['updated']} updated, "
                   f"{results['skipped_no_email']} skipped (no email), "
                   f"{results['skipped_not_found']} skipped (not in HubSpot), "
                   f"{results['errors']} errors")
        
        return results
    
    def get_donations_with_limit(self, limit: int = None) -> list:
        """Donations from CSuite, with `complete` recorded.

        Same change as get_profile_emails, and it matters more here: a
        partial donation set produces UNDERSTATED lifetime_giving totals,
        which a live run would then write over the correct ones. ~26,600
        donations is 266 paged calls.
        """
        from clients.csuite_fetch import fetch_all

        pages = max(1, -(-limit // 100)) if limit else None
        result = fetch_all(self.csuite, "donation/list", pace_ms=self.pace_ms,
                           **({"max_pages": pages} if pages else {}))

        self.donations_complete = bool(result.complete or pages)
        self.donations_error = None if self.donations_complete else (
            result.error or "the donation sweep did not finish")
        if not self.donations_complete:
            logger.error("CSuite donation sweep INCOMPLETE after %d row(s): "
                         "%s", len(result.records), self.donations_error)

        rows = result.records[:limit] if limit else result.records
        self.csuite_calls += result.calls
        self.rate_limit_waits += result.total_429s
        logger.info("Fetched %d donations in %d call(s), %d rate-limit "
                    "wait(s)", len(rows), result.calls, result.total_429s)
        self._report(f"donations: {len(rows):,} read")
        return rows


def run_donation_sync(dry_run: bool = False, quick: bool = False,
                      resolve_shown: int = 0) -> dict:
    """Convenience function to run donation sync
    
    Args:
        dry_run: Preview changes without applying them
        quick: Use sample data for faster testing (500 profiles, 500 donations)
        resolve_shown: On a dry run, verify the proposed profile ids for this
            many rows — the ones the report will print. 0 verifies none.
            Deliberately not "all": one profile/display per contact would turn
            a preview into the most expensive read in the app.
    """
    sync = DonationSync()
    results = sync.sync(dry_run=dry_run, quick=quick)
    if dry_run and resolve_shown:
        sync.resolve_shown_links(results, limit=resolve_shown)
    return results
