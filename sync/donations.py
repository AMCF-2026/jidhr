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
from datetime import datetime
from collections import defaultdict
from clients.csuite import CSuiteClient
from clients.hubspot import HubSpotClient
from sync.profile_state import (PROFILE_EXISTS, PROFILE_MISSING,
                                PROFILE_UNREADABLE, ProfileStateCache)

logger = logging.getLogger(__name__)

# How many profiles and donations a `quick` run looks at.
SAMPLE_SIZE = 500


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
    
    def __init__(self):
        self.csuite = CSuiteClient()
        self.hubspot = HubSpotClient()
    
    def get_profile_emails(self, limit: int = None) -> dict:
        """Get mapping of profile_id → email from CSuite
        
        Args:
            limit: Max number of profiles to fetch (None = all)
        """
        profile_emails = {}
        offset = 0
        batch_size = 100
        total_fetched = 0
        
        while True:
            result = self.csuite.get_profiles(limit=batch_size, offset=offset)
            
            if not result.get("success"):
                logger.error(f"Failed to get profiles at offset {offset}")
                break
            
            data = result.get("data", {})
            profiles = data.get("results", [])
            
            if not profiles:
                break
            
            total_fetched += len(profiles)
            
            for profile in profiles:
                profile_id = profile.get("profile_id")
                email = profile.get("primary_email")
                if profile_id and email:
                    profile_emails[profile_id] = email.lower().strip()
            
            # Check if we've hit the limit on TOTAL profiles fetched
            if limit and total_fetched >= limit:
                break
            
            # Check if we got fewer than batch_size (last page)
            if len(profiles) < batch_size:
                break
            
            offset += batch_size
        
        logger.info(f"Fetched {total_fetched} profiles, {len(profile_emails)} have emails")
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
        try:
            found = self.hubspot.search_contact_by_email(email)
        except Exception as e:
            return None, str(e)
        if not isinstance(found, dict):
            return None, f"unreadable response: {type(found).__name__}"
        if "error" in found:
            return None, str(found["error"])[:200]
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
        }
        
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
            return results
        
        # Step 2: Get donations
        logger.info("Step 2: Getting donations from CSuite...")
        donations = self.get_donations_with_limit(limit=donation_limit)
        
        if not donations:
            logger.warning("No donations found")
            results['details'].append("No donations found in CSuite")
            return results
        
        results['donations_read'] = len(donations)
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

        # Summary
        mode = "[DRY RUN] " if dry_run else ""
        mode += "[QUICK] " if quick else ""
        logger.info(f"{mode}Sync complete: {results['updated']} updated, "
                   f"{results['skipped_no_email']} skipped (no email), "
                   f"{results['skipped_not_found']} skipped (not in HubSpot), "
                   f"{results['errors']} errors")
        
        return results
    
    def get_donations_with_limit(self, limit: int = None) -> list:
        """Get donations with optional limit"""
        all_donations = []
        offset = 0
        batch_size = 100
        
        while True:
            result = self.csuite.get_donations(limit=batch_size, offset=offset)
            
            if not result.get("success"):
                logger.error(f"Failed to get donations at offset {offset}")
                break
            
            data = result.get("data", {})
            donations = data.get("results", [])
            
            if not donations:
                break
            
            all_donations.extend(donations)
            
            # Check if we've hit the limit
            if limit and len(all_donations) >= limit:
                all_donations = all_donations[:limit]
                break
            
            # Check if we got fewer than batch_size (last page)
            if len(donations) < batch_size:
                break
            
            offset += batch_size
            
            # Log progress every 500 donations
            if offset % 500 == 0:
                logger.info(f"Fetched {offset} donations so far...")
        
        logger.info(f"Retrieved {len(all_donations)} donations")
        return all_donations


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
