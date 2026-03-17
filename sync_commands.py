"""
Jidhr Sync Commands
===================
Handles sync operations: donations, events, newsletter, and sync-all.

Extracted from assistant.py lines 727-863 — logic unchanged.
"""

import logging
from sync import run_donation_sync, run_event_sync, run_newsletter_sync

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Trigger phrases (exact substring matches)
# ---------------------------------------------------------------------------

DONATION_SYNC_PHRASES = ['sync donations', 'sync donation', 'update donations']
EVENT_SYNC_PHRASES = ['sync events', 'sync event', 'update events']
NEWSLETTER_SYNC_PHRASES = ['sync newsletter', 'sync newsletters', 'update newsletter', 'sync subscriptions']
ALL_SYNC_PHRASES = ['sync all', 'sync everything', 'run all syncs']


# ---------------------------------------------------------------------------
# Registry interface
# ---------------------------------------------------------------------------

def can_handle(query: str, **kwargs) -> bool:
    """Check if query is a sync command."""
    q = query.lower().strip()
    return (
        any(p in q for p in DONATION_SYNC_PHRASES) or
        any(p in q for p in EVENT_SYNC_PHRASES) or
        any(p in q for p in NEWSLETTER_SYNC_PHRASES) or
        q in ALL_SYNC_PHRASES
    )


def handle(query: str, assistant) -> str:
    """
    Route to the appropriate sync operation.

    Args:
        query: The user's message
        assistant: JidhrAssistant instance (not used directly, but keeps
                   the interface consistent across all intent modules)

    Returns:
        Formatted result string
    """
    q = query.lower().strip()

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
    """Run donation sync (CSuite → HubSpot)."""
    logger.info("Running donation sync...")
    dry_run = 'dry run' in query_lower or 'test' in query_lower

    try:
        results = run_donation_sync(dry_run=dry_run, quick=dry_run)
        return _format_donation_sync_results(results, dry_run)
    except Exception as e:
        logger.error(f"Donation sync error: {e}")
        return f"❌ Donation sync failed: {e}"


def _sync_events(query_lower: str) -> str:
    """Run event sync (CSuite → HubSpot)."""
    logger.info("Running event sync...")
    dry_run = 'dry run' in query_lower or 'test' in query_lower

    try:
        results = run_event_sync(dry_run=dry_run)
        return _format_event_sync_results(results, dry_run)
    except Exception as e:
        logger.error(f"Event sync error: {e}")
        return f"❌ Event sync failed: {e}"


def _sync_newsletter(query_lower: str) -> str:
    """Run newsletter sync (CSuite → HubSpot)."""
    logger.info("Running newsletter sync...")
    dry_run = 'dry run' in query_lower or 'test' in query_lower

    try:
        results = run_newsletter_sync(dry_run=dry_run, quick=dry_run)
        return _format_newsletter_sync_results(results, dry_run)
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
    except Exception as e:
        responses.append(f"❌ Donations: {e}")

    try:
        event_results = run_event_sync(dry_run=False)
        responses.append(f"✅ Events: {event_results['created']} created")
    except Exception as e:
        responses.append(f"❌ Events: {e}")

    try:
        newsletter_results = run_newsletter_sync(dry_run=False)
        responses.append(f"✅ Newsletter: {newsletter_results['subscribed']} subscribed")
    except Exception as e:
        responses.append(f"❌ Newsletter: {e}")

    return "✅ **All Syncs Complete**\n\n" + "\n".join(responses)


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

def _format_donation_sync_results(results: dict, dry_run: bool) -> str:
    prefix = "🧪 **DRY RUN (Sample)** - " if dry_run else ""

    response = f"""{prefix}✅ **Donation Sync Complete**

📊 **Results:**
• **{results['updated']}** contacts {"would be updated" if dry_run else "updated"} with donation data
• **{results['skipped_no_email']}** profiles skipped (no email in CSuite)
• **{results['skipped_not_found']}** profiles skipped (not found in HubSpot)
• **{results['errors']}** errors

💡 Fields: `lifetime_giving`, `last_donation_date`, `last_donation_amount`, `donation_count`, `csuite_profile_id`"""

    if dry_run:
        response += "\n\n⚡ *This dry run used sample data (500 profiles, 500 donations). Run `sync donations` without 'dry run' for full sync.*"

    return response


def _format_event_sync_results(results: dict, dry_run: bool) -> str:
    prefix = "🧪 **DRY RUN** - " if dry_run else ""

    response = f"""{prefix}✅ **Event Sync Complete**

📊 **Results:**
• **{results['created']}** events created in HubSpot
• **{results['skipped_exists']}** events skipped (already exist)
• **{results['skipped_past']}** events skipped (past events)
• **{results['skipped_archived']}** events skipped (archived)
• **{results['errors']}** errors"""

    if results.get('details'):
        response += "\n\n📅 **Events:**"
        for detail in results['details'][:5]:
            response += f"\n• {detail}"

    return response


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