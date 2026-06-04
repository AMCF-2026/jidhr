"""
Jidhr Social Sync
=================
On-demand chat command that runs the social broadcast backfill —
captures published HubSpot posts into content_history.

Thin wrapper around content.social_capture.backfill_social_content().
The heavy lifting (pre-filter, extraction, INSERT, ON CONFLICT) lives
there; this module is just the chat-surface trigger.

Not registered in intents/__init__.py yet — HANDLER_CHAIN registration
is a deliberate separate step.
"""

import logging
from content.social_capture import backfill_social_content

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Trigger phrases (exact substring matches)
# ---------------------------------------------------------------------------

SOCIAL_SYNC_PHRASES = [
    'sync social',
    'capture social',
    'refresh social',
    'update social',
]


# ---------------------------------------------------------------------------
# Registry interface
# ---------------------------------------------------------------------------

def can_handle(query: str, **kwargs) -> bool:
    """Check if query is a social sync command."""
    q = query.lower().strip()
    return any(p in q for p in SOCIAL_SYNC_PHRASES)


def handle(query: str, assistant) -> str:
    """
    Run the social broadcast backfill on demand.

    Args:
        query: The user's message
        assistant: JidhrAssistant instance (not used directly, but keeps
                   the interface consistent across all intent modules)

    Returns:
        Formatted result string
    """
    logger.info("Running social backfill...")

    try:
        result = backfill_social_content()
    except Exception as e:
        logger.error(f"Social sync error: {e}")
        return f"❌ Social sync failed: {e}"

    if result.get("error") is not None:
        return (
            f"❌ Social sync failed: {result['error']}\n\n"
            "Try again, or check the logs for details."
        )

    return _format_social_sync_results(result)


# ---------------------------------------------------------------------------
# Formatter
# ---------------------------------------------------------------------------

def _format_social_sync_results(result: dict) -> str:
    fetched = result.get("fetched", 0)
    inserted = result.get("inserted", 0)
    skipped = result.get("skipped_duplicate", 0)
    tx_failures = result.get("topic_extraction_failures", 0)

    if inserted == 0:
        response = f"""✅ **Social Sync Complete** — nothing new to capture

📊 **Results:**
• **{fetched}** post(s) checked
• **{skipped}** already captured"""
    else:
        response = f"""✅ **Social Sync Complete**

📊 **Results:**
• **{fetched}** post(s) checked
• **{inserted}** new post(s) captured
• **{skipped}** duplicate(s) skipped"""

    if tx_failures > 0:
        response += f"\n\n⚠️ **{tx_failures}** post(s) failed topic extraction (logged for review)."

    return response
