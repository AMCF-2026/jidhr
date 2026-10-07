"""
Jidhr Sync Commands
===================
Handles sync operations: donations, events, newsletter, and sync-all.

Chat-surface trigger for the sync/ package: matches the sync phrases,
invokes the requested sync, and formats the result for the user.
"""

import logging
from sync import (DonationSyncDisabled, NewsletterSyncDisabled,
                  run_donation_sync, run_newsletter_sync)
from sync import event_apply

logger = logging.getLogger(__name__)

from intents import anchors


# ---------------------------------------------------------------------------
# Access control
# ---------------------------------------------------------------------------

# Nothing is donor-facing yet; every handler is staff-and-above.
ALLOWED_ROLES = frozenset({"admin", "staff"})

# ---------------------------------------------------------------------------
# Trigger phrases (exact substring matches)
# ---------------------------------------------------------------------------

DONATION_SYNC_PHRASES = ['sync donations', 'sync donation', 'update donations']
EVENT_SYNC_PHRASES = ['sync events', 'update events']  # "sync event [name]" → events.py attendee sync
NEWSLETTER_SYNC_PHRASES = ['sync newsletter', 'sync newsletters', 'update newsletter', 'sync subscriptions']
# Deliberately NOT in ALL_SYNC_PHRASES: "sync all" must not reach the
# registrations sync. Phase 1 writes nothing, but the day it does, a
# three-word message should not be what starts it.
REGISTRATION_SYNC_PHRASES = ['sync registrations', 'sync registration']
ALL_SYNC_PHRASES = ['sync all', 'sync everything', 'run all syncs']


# ---------------------------------------------------------------------------
# Registry interface
# ---------------------------------------------------------------------------

def can_handle(query: str, **kwargs) -> bool:
    """Check if query is a sync command."""
    # Anchored, not scanned: the trigger must LEAD the message.
    # See intents/anchors.py for the two production failures.
    q = query.lower().strip()
    if anchors.yields_to_content(query):
        return False
    return (
        anchors.anchored(q, DONATION_SYNC_PHRASES) or
        anchors.anchored(q, EVENT_SYNC_PHRASES) or
        anchors.anchored(q, NEWSLETTER_SYNC_PHRASES) or
        anchors.anchored(q, REGISTRATION_SYNC_PHRASES) or
        q in ALL_SYNC_PHRASES
    )


def handle(query: str, ctx) -> str:
    """
    Route to the appropriate sync operation.

    Args:
        query: The user's message
        ctx: RequestContext (not used directly, but keeps the interface
             consistent across all intent modules)

    Returns:
        Formatted result string
    """
    q = query.lower().strip()

    # Before the event phrases: "sync registrations" contains neither, but
    # keeping it first makes the ordering independent of that.
    if any(p in q for p in REGISTRATION_SYNC_PHRASES):
        return _sync_registrations(q)

    if any(p in q for p in DONATION_SYNC_PHRASES):
        return _sync_donations(q)

    if any(p in q for p in EVENT_SYNC_PHRASES):
        return _sync_events(q)

    if any(p in q for p in NEWSLETTER_SYNC_PHRASES):
        return _sync_newsletter(q)

    if q in ALL_SYNC_PHRASES:
        return _run_all_syncs()

    return "❌ Unrecognised sync command."


# ---------------------------------------------------------------------------
# Individual sync handlers
# ---------------------------------------------------------------------------

def _sync_donations(query_lower: str) -> str:
    """Run donation sync (CSuite → HubSpot).

    A plain "sync donations" WRITES, and is gated on
    CSUITE_DONATION_SYNC_ENABLED. "sync donations dry run" is not gated — it
    makes no HubSpot writes, and it is what the decision to enable should rest
    on. The gate itself lives in DonationSync.sync, because "sync all" reaches
    the same code without coming through here.
    """
    dry_run = 'dry run' in query_lower or 'test' in query_lower
    # A preview was always a 500-profile sample, because quick was wired to
    # dry_run. "full" unwires them: it pages every profile and every donation
    # and writes nothing, which is the only way to see the real counts before
    # turning the flag on.
    full = dry_run and 'full' in query_lower
    if full:
        # Not run here. A full preview reads ~18,800 profiles and ~26,600
        # donations, then searches HubSpot once per donor with an email —
        # 7,604 of them. HubSpot caps the Search API at 5 requests per second
        # across all object types and this paces at 4 to stay clear of it, so
        # the HubSpot half alone takes ~32 minutes (7,604 / 4 = 1,901s). Even
        # pacing right at the cap it would be ~25. gunicorn kills a request at
        # 180s, so the worker dies about a tenth of the way in, taking its
        # other in-flight requests with it, and the work is thrown away.
        logger.info("full donation preview requested; handing over to the CLI")
        return DONATION_FULL_PREVIEW_REPLY
    logger.info("Running donation sync (dry_run=%s, full=%s)...",
                dry_run, full)

    try:
        results = run_donation_sync(dry_run=dry_run,
                                    quick=dry_run and not full,
                                    resolve_shown=LINK_ROWS_SHOWN
                                    if dry_run else 0)
        return _format_donation_sync_results(results, dry_run)
    except DonationSyncDisabled as e:
        # Not an error line. Nothing failed and nothing was attempted, and a
        # ❌ would send someone looking for a fault that is not there.
        logger.info("donation sync is off: %s", e)
        return ("⏸️ **Donation sync is turned off.**\n\n"
                "No HubSpot contact was read or changed. It writes five "
                "properties per matched contact, including "
                "`csuite_profile_id`, which the DAF duplicate guard reads — "
                "so it stays off until that is scoped.\n\n"
                "• Say *\"sync donations dry run\"* to preview it safely "
                "(500 profiles, 500 donations, no writes).\n"
                "• Set `CSUITE_DONATION_SYNC_ENABLED=true` to run it for "
                "real.")
    except Exception as e:
        logger.error(f"Donation sync error: {e}")
        return f"❌ Donation sync failed: {e}"


# The words that make an event sync WRITE. Everything else previews.
#
# It used to be the other way round: "sync events" wrote, and only "dry run"
# or "test" in the message held it back. A one-line chat message is a thin
# thing to hang eleven marketing events on, and the dry run is free.
EVENT_APPLY_PHRASE = "apply"


def _event_limit(query_lower: str):
    """The record cap for a live run: "limit N", or "no limit", or the
    default. Updates count toward it as well as creates."""
    import re

    if "no limit" in query_lower or "unlimited" in query_lower:
        return None
    found = re.search(r"limit\s+(\d+)", query_lower)
    if found:
        return max(int(found.group(1)), 0)
    return event_apply.CHAT_DEFAULT_LIMIT


def _event_options(query_lower: str) -> dict:
    """The brakes a chat message can ask for.

    Defaults are the cautious ones: creates are future-only, and anything
    plan() flagged for review is withheld whatever the message says. "no
    limit" lifts the record cap, never the withholding.
    """
    import re

    return {
        "updates_only": "updates only" in query_lower,
        # Deliberately NOT exposed in chat: future_only. A chat message is
        # one line, and "create 74 marketing events for things that already
        # happened" is not a decision one line should be able to make. The
        # CLI has --past-creates for that.
        # Past creates need their id naming, one at a time. On 2026-10-06
        # that was 74 of 77 creates, 73 of them archived in CSuite.
        "include_ids": re.findall(r"include\s+(\d+)", query_lower),
    }


def _sync_registrations(query_lower: str) -> str:
    """Preview the registrations sync. Phase 1 writes nothing, ever."""
    from sync import registrations as reg

    live = "dry run" not in query_lower and "preview" not in query_lower
    logger.info("Running registrations sync (live asked for=%s)...", live)
    try:
        results = reg.run(dry_run=not live)
        return _format_registration_results(results)
    except reg.RegistrationsSyncDisabled as e:
        return ("⏸️ **Registrations sync is turned off.**\n\n"
                f"{e}\n\n"
                '• Say *"sync registrations dry run"* to preview it.')
    except reg.PhaseOnePreviewOnly as e:
        return ("🚧 **Registrations sync is preview-only.**\n\n"
                f"{e}\n\n"
                '• Say *"sync registrations dry run"* for the preview.')
    except Exception as e:
        logger.error("Registrations sync error: %s", e, exc_info=True)
        return f"❌ Registrations sync failed: {e}"


# How many events in review the preview names before summarising.
REGISTRATION_REVIEW_ROWS_SHOWN = 10


def _format_registration_results(results: dict) -> str:
    """Counts in the units they are measured in.

    A registration is a (person, event) pair, not a person: dedupe runs per
    event, so somebody at two events is two registrations. Reporting one
    number for both units is how 83 + 20 came to be read against 100.
    """
    if results.get("error"):
        return f"❌ **Registrations preview stopped.**\n\n{results['error']}"

    lines = ["🧪 **Registrations — PREVIEW** (nothing was written)", "",
             f"📊 **Across {results.get('events_read', 0)} event(s):**",
             f"• **{results.get('registrant_rows', 0)}** registrant rows in "
             f"CSuite",
             f"• **{results.get('unique_emails', 0)}** registrations after "
             f"dedupe by email within each event "
             f"({results.get('duplicates_dropped', 0)} duplicate(s) dropped)",
             f"• **{results.get('would_register', 0)}** would be sent as "
             f"REGISTERED, by contact id",
             f"• **{results.get('withheld', 0)}** withheld — no HubSpot "
             f"contact for that address, and none would be created",
             f"• **{results.get('already', 0)}** already registered by an "
             f"earlier run"]

    if results.get("non_marketing"):
        lines += ["", f"⚠️ **{results['non_marketing']}** of those contacts "
                      "are deliberately NON-marketing. They would be "
                      "registered, and `hs_marketable_status` is never "
                      "touched — it is read-only to the API, so this sync "
                      "cannot change who may be emailed."]

    rows = results.get("review_rows") or []
    if rows:
        lines += ["", "🔎 **Needs a human — nothing planned for these "
                      "events:**"]
        for event_id, why in rows[:REGISTRATION_REVIEW_ROWS_SHOWN]:
            lines.append(f"   `{event_id}` — {str(why)[:110]}")
        if len(rows) > REGISTRATION_REVIEW_ROWS_SHOWN:
            lines.append(f"   … and "
                         f"**{len(rows) - REGISTRATION_REVIEW_ROWS_SHOWN}** "
                         "more; the count above is the total")

    lines += ["", f"📞 {results.get('csuite_calls', 0)} CSuite call(s), "
                  f"{results.get('hubspot_calls', 0)} HubSpot read call(s)",
              "✍️ **0 HubSpot writes — phase 1 has no write path.**"]

    if not results.get("migration_applied"):
        lines += ["", "⚠️ `hubsync.registration_map` does not exist, so this "
                      "run has no memory of earlier ones: everything reads "
                      "as new, and no cancellation could be inferred even if "
                      "phase 1 tried. Apply "
                      "`migrations/005_registration_map.sql`."]
    if results.get("run_logged"):
        lines += ["", f"📒 Recorded as `hubsync.run_log` id "
                      f"{results.get('run_id')} with "
                      f"{results.get('unique_emails', 0)} per-record input "
                      f"row(s) — hashed addresses, never addresses."]
    return "\n".join(lines)


def _sync_events(query_lower: str) -> str:
    """Plan an event sync, and write only if the message says to.

    One implementation, shared with scripts/event_sync.py — sync/events.py was
    retired on 2026-10-06. It had its own payload shape, no notion of an
    update, a dedup check that called a HubSpot endpoint which 404s for every
    id, and a different externalAccountId from the module whose job was to own
    that value.
    """
    apply = EVENT_APPLY_PHRASE in query_lower
    limit = _event_limit(query_lower) if apply else None
    logger.info("Running event sync (apply=%s, limit=%s)...", apply, limit)

    options = _event_options(query_lower)
    try:
        results = event_apply.run(dry_run=not apply, limit=limit, **options)
        return _format_event_sync_results(results)
    except Exception as e:
        logger.error(f"Event sync error: {e}", exc_info=True)
        return f"❌ Event sync failed: {e}"


def _sync_newsletter(query_lower: str) -> str:
    """Run newsletter sync (CSuite → HubSpot)."""
    logger.info("Running newsletter sync...")
    dry_run = 'dry run' in query_lower or 'test' in query_lower

    try:
        results = run_newsletter_sync(dry_run=dry_run, quick=dry_run)
        return _format_newsletter_sync_results(results, dry_run)
    except NewsletterSyncDisabled as e:
        logger.info("newsletter sync is off: %s", e)
        return ("⏸️ **Newsletter sync is turned off.**\n\n"
                "No HubSpot subscription was read or changed. It POSTs a "
                "subscription change per opted-in CSuite profile, which is a "
                "change to a real person's communication preferences.\n\n"
                "• Say *\"sync newsletter dry run\"* to preview it safely.\n"
                "• Set `CSUITE_NEWSLETTER_SYNC_ENABLED=true` to run it for "
                "real.")
    except Exception as e:
        logger.error(f"Newsletter sync error: {e}")
        return f"❌ Newsletter sync failed: {e}"


# ---------------------------------------------------------------------------
# Sync-all
# ---------------------------------------------------------------------------

def _run_all_syncs() -> str:
    """Run all sync operations sequentially."""
    responses = []

    try:
        donation_results = run_donation_sync(dry_run=False)
        responses.append(f"✅ Donations: {donation_results['updated']} updated")
    except DonationSyncDisabled:
        # "sync all" is the path that would have walked straight past a gate
        # placed in _sync_donations. Named distinctly so a skipped sync is not
        # read as a failed one, or as a sync that ran and found nothing.
        responses.append("⏸️ Donations: skipped — "
                         "CSUITE_DONATION_SYNC_ENABLED is off")
    except Exception as e:
        responses.append(f"❌ Donations: {e}")

    try:
        # Capped, like a chat live run: "sync all" is one line too.
        event_results = event_apply.run(
            dry_run=False, limit=event_apply.CHAT_DEFAULT_LIMIT)
        if event_results.get("error"):
            responses.append(f"❌ Events: {event_results['error']}")
        else:
            responses.append(
                f"✅ Events: {event_results['created']} created, "
                f"{event_results['updated']} updated, "
                f"{event_results['unchanged']} unchanged"
                + (f", {event_results['review']} need a human"
                   if event_results.get("review") else ""))
    except Exception as e:
        responses.append(f"❌ Events: {e}")

    try:
        newsletter_results = run_newsletter_sync(dry_run=False)
        responses.append(f"✅ Newsletter: {newsletter_results['subscribed']} subscribed")
    except NewsletterSyncDisabled:
        # Same reason as donations: "sync all" is the path that walks past a
        # gate placed in the chat handler.
        responses.append("⏸️ Newsletter: skipped — "
                         "CSUITE_NEWSLETTER_SYNC_ENABLED is off")
    except Exception as e:
        responses.append(f"❌ Newsletter: {e}")

    return "✅ **All Syncs Complete**\n\n" + "\n".join(responses)


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

# What chat says instead of attempting a full preview inline. The numbers are
# measured, not estimated: 18,823 profiles and 26,597 donations in the mirror
# on 2026-10-06, of which 7,604 donor profiles carry an email.
DONATION_FULL_PREVIEW_REPLY = (
    "📚 **A full preview has to run from the command line.**\n\n"
    "It reads ~18,800 CSuite profiles and ~26,600 donations, then searches "
    "HubSpot once per donor with an email — about 7,604 searches. HubSpot "
    "caps its Search API at **5 requests per second** and this paces at 4 to "
    "stay clear of it, so that half alone takes **~32 minutes**, and a web "
    "request is killed at 180 seconds. Run from chat it would die about a "
    "tenth of the way in, take the worker's other requests down with it, and "
    "throw away everything it had read.\n\n"
    "```\n"
    "python scripts/donation_preview.py --full --out donation_preview.md\n"
    "```\n\n"
    "It writes nothing, prints progress as it goes, and uses the same planner "
    "and the same report as this command.\n\n"
    '• Say *"sync donations dry run"* for the 500-row sample, which does run '
    "here.")

# Roughly how many CSuite profiles a full preview pages, so the hint can state
# the cost before someone asks for it. Measured 18,797 on 2026-10-01 via the
# unfiltered profile/list total; it only has to be the right order of
# magnitude, and the full run reports what it actually read.
DONATION_PROFILE_ESTIMATE = 18800

# Kept in step with sync.donations.SAMPLE_SIZE, imported rather than repeated.
from sync.donations import SAMPLE_SIZE as DONATION_SAMPLE_SIZE  # noqa: E402

# How many shared-email contacts the donation report prints before summarising.
SHARED_EMAIL_ROWS_SHOWN = 10

# How many "needs a human" rows the event report prints before summarising.
EVENT_REVIEW_ROWS_SHOWN = 10

# How many withheld rows the event report prints before summarising.
EVENT_WITHHELD_ROWS_SHOWN = 10

# How many link rows the dry-run table shows. The counts above it are the real
# totals; this caps only what is printed, so a long run stays readable.
LINK_ROWS_SHOWN = 25


def _format_donation_sync_results(results: dict, dry_run: bool) -> str:
    prefix = "🧪 **DRY RUN (Sample)** - " if dry_run else ""
    verb = "would be updated" if dry_run else "updated"

    response = f"""{prefix}✅ **Donation Sync Complete**

📊 **Results:**
• **{results['updated']}** contacts {verb} with donation data
• **{results['skipped_no_email']}** profiles skipped (no email in CSuite)
• **{results['skipped_not_found']}** profiles skipped (not found in HubSpot)
• **{results['errors']}** errors

💡 Fields: `lifetime_giving`, `last_donation_date`, `last_donation_amount`, `donation_count`, `csuite_profile_id`"""

    response += _format_link_outcomes(results, dry_run)

    if dry_run:
        # It used to say "Run `sync donations` without 'dry run' for full
        # sync", which was an instruction that no longer works and never
        # should have: a plain "sync donations" is refused unless the flag is
        # set, and telling someone to type it invites them to read the
        # refusal as a fault.
        if results.get("partial") and results.get("sampled", True):
            response += (
                f"\n\n🛑 *PARTIAL READ — the sample stopped early "
                f"({results.get('partial_reason') or 'reason not recorded'}). "
                "Not even a sample of the size asked for.*")
        elif results.get("sampled", True):
            response += (
                f"\n\n⚡ *Sampled: {DONATION_SAMPLE_SIZE} profiles, "
                f"{DONATION_SAMPLE_SIZE} donations — not the whole database, "
                f"so these counts are not totals.*\n"
                "💡 *For the real figures — the shared-email and stale-link "
                "counts cannot be read off a sample — run "
                "`python scripts/donation_preview.py --full`. That reads "
                f"roughly {DONATION_PROFILE_ESTIMATE:,} profiles and takes "
                "tens of minutes, which is why it is not a chat command. It "
                "writes nothing.*")
        elif results.get("partial"):
            # The one thing this footer must never do. A sweep that stopped
            # short still has a profiles_read figure, and before 2026-10-06 a
            # failed page read as the end of the data — so "the whole
            # database, so these counts are totals" would have been printed
            # over a read that a 429 cut off.
            response += (
                f"\n\n🛑 *PARTIAL READ — {results.get('profiles_read', 0):,} "
                f"profiles and {results.get('donations_read', 0):,} donations "
                f"were read before CSuite stopped answering "
                f"({results.get('partial_reason') or 'reason not recorded'}). "
                "**These counts are NOT totals** and the missing rows are "
                "not estimated. Re-run when CSuite is answering.*")
        else:
            response += (
                f"\n\n📚 *Full preview: {results.get('profiles_read', 0):,} "
                f"profiles and {results.get('donations_read', 0):,} donations "
                f"read — the whole database, so these counts are totals.*")
        response += (
            "\n🔒 *A live run needs `CSUITE_DONATION_SYNC_ENABLED=true`. "
            "Without it `sync donations` is refused and nothing is written.*")

    return response


def _format_link_outcomes(results: dict, dry_run: bool) -> str:
    """What happened to csuite_profile_id, and the per-contact table.

    Separated out because it is the part a person has to read before enabling
    the sync: `csuite_profile_id` is the field the DAF duplicate guard trusts,
    and this sync is the other thing that writes it.
    """
    keys = ("link_written", "link_unchanged", "link_conflict", "link_differs",
            "link_stale", "link_unverifiable", "shared_email")
    if not any(key in results for key in keys):
        return ""            # an older result dict; nothing to add

    labels = [
        ("link_written", "written (contact had none)"),
        ("link_unchanged", "already correct — not rewritten"),
        ("link_conflict", "**left alone — points at a different live "
                          "profile**"),
        ("link_stale", "**left alone — stored id is not in CSuite**"),
        ("link_unverifiable", "left alone — CSuite would not confirm the "
                              "stored id"),
        ("link_differs", "left alone — differs from the stored id, which this "
                         "preview did not check"),
    ]
    lines = ["", "",
             "🔗 **csuite_profile_id**  "
             "*(the four donation fields are written either way)*"]
    for key, label in labels:
        count = results.get(key, 0)
        if count:
            lines.append(f"• **{count}** {label}")
    if not any(results.get(key) for key, _ in labels):
        lines.append("• nothing to link")

    if results.get("link_stale"):
        lines.append("   ⚠️ A stale id is NOT repointed — overwriting it "
                     "destroys the only record of what it pointed at.")

    shared = results.get("shared_email_rows") or []
    if results.get("shared_email"):
        lines += ["", f"👥 **{results['shared_email']} contact(s) claimed by "
                      f"more than one CSuite profile — nothing was written "
                      f"to them**"]
        for row in shared[:SHARED_EMAIL_ROWS_SHOWN]:
            current = row.get("current") or "none"
            lines.append(f"• `{row['contact_id']}` ← profiles "
                         f"{', '.join(row['profiles'])} "
                         f"(stored link: {current})")
        if len(shared) > SHARED_EMAIL_ROWS_SHOWN:
            lines.append(f"• … and **{len(shared) - SHARED_EMAIL_ROWS_SHOWN}** "
                         "more; the count above is the total")
        lines.append("   ⚠️ Totals are **not summed** — two profiles on one "
                     "address may be one person entered twice or two people "
                     "in a household, and this sync cannot tell. Merge or "
                     "separate them in CSuite.")

    rows = results.get("link_rows") or []
    if dry_run and rows:
        shown = rows[:LINK_ROWS_SHOWN]
        resolved = any("proposed_exists" in row for row in shown)
        lines += [
            "",
            f"**Proposed links — first {len(shown)} of {len(rows)}**"
            + ("  \n*(existence checked for the rows shown)*" if resolved
               else ""),
            "",
            "| contact | current | proposed | proposed in CSuite? | action |",
            "|---|---|---|---|---|",
        ]
        for row in shown:
            current = row.get("current") or "—"
            if row.get("current_exists"):
                current = f"{current} ({row['current_exists']})"
            lines.append(
                f"| `{row['contact_id']}` | {current} | "
                f"`{row['proposed']}` | "
                f"{row.get('proposed_exists', 'not checked')} | "
                f"{row.get('action', '')} |")
        if len(rows) > len(shown):
            lines.append("")
            lines.append(f"… and **{len(rows) - len(shown)}** more rows not "
                         "shown. The counts above are the full totals.")

    if results.get("profile_reads"):
        lines.append("")
        lines.append(f"*{results['profile_reads']} CSuite profile read(s) "
                     "were made to answer this.*")

    return "\n".join(lines)


def _format_event_sync_results(results: dict) -> str:
    """Six outcomes, six lines. None of them stands in for another.

    The old formatter reported `created` and `skipped_exists` and had no words
    for an update, for a record it deferred, for a create that came back
    without an id, or for one a person has to look at. A sync that can only
    say "created 7" cannot tell you it changed nothing, and that was true of
    it on every run after the first.
    """
    if results.get("error"):
        return f"❌ **Event sync stopped.**\n\n{results['error']}"

    dry_run = results.get("dry_run", True)
    head = ("🧪 **Event Sync — DRY RUN**" if dry_run
            else "✅ **Event Sync — APPLIED**")
    lines = [head, ""]

    verb = "would be " if dry_run else ""
    lines += [
        "📊 **Outcomes:**",
        f"• **{results.get('created', 0)}** {verb}created",
        f"• **{results.get('updated', 0)}** {verb}updated",
        f"• **{results.get('unchanged', 0)}** unchanged — nothing to send",
    ]
    lines.append(
        f"• **{results.get('withheld', 0)}** withheld — planned, counted, "
        f"and deliberately NOT written")
    if not dry_run:
        lines.append(
            f"• **{results.get('deferred', 0)}** deferred — the "
            f"`limit` was reached, so they were not attempted")
        lines.append(
            f"• **{results.get('unknown', 0)}** unknown — the create came "
            f"back with no id, so it may or may not have landed. "
            f"**Never retried**; the next run resolves it by lookup")
        lines.append(f"• **{results.get('failed', 0)}** failed")
    # `review` is an overlay: plan() appends to it IN ADDITION to a
    # record's create/update bucket, so it double-counts against the lines
    # above. Measured 2026-10-06, all 79 review entries were also creates
    # (73) or updates (6). Saying so is the difference between a reader
    # reconciling the numbers and a reader hunting for 19 missing records.
    lines.append(
        f"• **{results.get('review', 0)}** need a human — *also counted "
        f"above*, not a separate group. All of these are withheld.")
    lines.append(
        f"• **{results.get('skipped', 0)}** not syncable (no event date in "
        f"CSuite)")

    withheld = results.get("withheld_rows") or []
    if withheld:
        lines += ["", "🚫 **Withheld — nothing was written to these:**"]
        from collections import Counter
        kinds = Counter(
            "needs a human" if str(why).startswith("needs a human")
            else "starts in the past" if "starts in the past" in str(why)
            else "updates only" if "updates only" in str(why)
            else "other"
            for _id, why in withheld)
        for kind, count in kinds.most_common():
            lines.append(f"• **{count}** {kind}")
        for record_id, why in withheld[:EVENT_WITHHELD_ROWS_SHOWN]:
            lines.append(f"   `{record_id}` — {str(why)[:96]}")
        if len(withheld) > EVENT_WITHHELD_ROWS_SHOWN:
            lines.append(f"   … and **"
                         f"{len(withheld) - EVENT_WITHHELD_ROWS_SHOWN}** more; "
                         "the count above is the total")

    rows = results.get("review_rows") or []
    if rows:
        lines += ["", "🔎 **Needs a human:**"]
        for eventdate_id, name, reason in rows[:EVENT_REVIEW_ROWS_SHOWN]:
            label = (name or "(no name)")[:60]
            lines.append(f"• `{eventdate_id}` {label} — {reason}")
        if len(rows) > EVENT_REVIEW_ROWS_SHOWN:
            lines.append(f"• … and **{len(rows) - EVENT_REVIEW_ROWS_SHOWN}** "
                         "more; the count above is the total")

    if results.get("stopped"):
        lines += ["", f"🛑 **Stopped:** {results['stopped']}",
                  "Nothing further was created. Re-run after checking "
                  "HubSpot — the next run resolves the ambiguous record by "
                  "lookup rather than retrying it."]

    # Writes, stated rather than inferred. The report used to end with read
    # counts only, so a run that withheld everything read identically to one
    # that wrote nothing by luck — and the write_audit ids make the reply
    # checkable against the table instead of merely believable.
    if results.get("dry_run", True):
        lines += ["", "✍️ **0 HubSpot writes — nothing was sent.**"]
    else:
        attempted = results.get("writes_attempted", 0)
        succeeded = results.get("writes_succeeded", 0)
        ids = results.get("write_audit_ids") or []
        trail = (" (write_audit " + ", ".join(str(i) for i in ids) + ")"
                 if ids else " (no write_audit ids — the audit could not be "
                             "read back)")
        line = (f"✍️ **{attempted} HubSpot write(s) attempted, {succeeded} "
                f"succeeded**{trail}")
        if attempted and succeeded < attempted:
            line += f"  \n   ⚠️ {attempted - succeeded} did not succeed."
        lines += ["", line]

    notes = [(record_id, note) for record_id, note in
             (results.get("duration_notes") or [])]
    if notes:
        lines += ["", "🕒 **End times are assumed** — CSuite carries no end "
                      "time, so HubSpot's own duration was reapplied:"]
        for record_id, note in notes[:EVENT_WITHHELD_ROWS_SHOWN]:
            lines.append(f"   `{record_id}` — {note}")

    lines += ["", f"📞 {results.get('csuite_calls', 0)} CSuite call(s), "
                  f"{results.get('hubspot_calls', 0)} HubSpot read call(s), "
                  f"{results.get('event_dates_read', 0)} event date(s) read"]

    if dry_run:
        # Says what was actually written. The CLI printed "nothing was
        # written to HubSpot or to hubsync" while record_run had just
        # inserted a run_log row — a message that contradicted the code one
        # line above it.
        written = ("One `hubsync.run_log` row was written, recording that "
                   "this preview happened."
                   if results.get("run_logged")
                   else "Nothing was written anywhere — `hubsync` is not "
                        "available, so not even the run log.")
        lines += ["", f"ℹ️ **Nothing was written to HubSpot.** {written}", "",
                  'Say *"sync events apply"* to write it, '
                  f'capped at {event_apply.CHAT_DEFAULT_LIMIT} records — '
                  'add *"limit 20"* or *"no limit"* to change that.']
    if results.get("updates_only"):
        lines += ["", "ℹ️ **Updates only** — every create was withheld."]
    elif not results.get("future_only", True):
        lines += ["", "⚠️ **future_only is off** — past events can be "
                      "created."]
    if results.get("include_ids"):
        lines += ["", "ℹ️ Past creates allowed by id: "
                      + ", ".join(f"`{i}`" for i in results["include_ids"])]

    if results.get("limit") is not None and not results.get("dry_run", True):
        lines += ["", f"ℹ️ Capped at {results['limit']} record(s) this run. "
                      'Say *"no limit"* to lift it.']

    if not results.get("migration_applied"):
        lines += ["", "⚠️ `hubsync.event_map` does not exist, so this run had "
                      "no duplicate guard and no memory of previous runs. "
                      "Apply `migrations/001_hubsync_event_map.sql`."]

    return "\n".join(lines)


def _format_newsletter_sync_results(results: dict, dry_run: bool) -> str:
    prefix = "🧪 **DRY RUN (Sample)** - " if dry_run else ""

    response = f"""{prefix}✅ **Newsletter Sync Complete**

📊 **Results:**
• **{results['subscribed']}** contacts {"would be subscribed" if dry_run else "subscribed"}
• **{results['already_subscribed']}** already subscribed
• **{results['skipped_not_found']}** not found in HubSpot
• **{results['errors']}** errors"""

    if dry_run:
        response += "\n\n⚡ *This dry run used sample data. Run `sync newsletter` without 'dry run' for full sync.*"

    return response