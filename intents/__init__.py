"""
Jidhr Intent Registry
=====================
Central router that checks handlers in priority order (most specific first).

Usage from assistant.py:
    from intents import route_intent
    handler = route_intent(query, ctx)
    if handler:
        name, func = handler
        response = func(query, ctx)

Handlers are skipped outright when the actor's role is not in the module's
ALLOWED_ROLES, before can_handle is consulted — a handler must not get a say
in whether it applies to someone who may not use it at all.

queries.py is NOT in this registry — it's a context gatherer, not a handler.
assistant.py calls it directly as the fallback path.
"""

import logging

from intents import sync_commands
from intents import social_sync
from intents import content_report
from intents import content
from intents import daf_workflow
from intents import events
from intents import notes
from intents import donor_prep
from intents import reports

logger = logging.getLogger(__name__)

# Priority order: most specific first, broadest last.
# Each entry: (name, module)
HANDLER_CHAIN = [
    ("sync_commands",  sync_commands),
    ("social_sync",    social_sync),
    # content_report must win before content — its phrases (e.g. "what
    # have we been posting") would otherwise risk being shadowed if the
    # broader content handler ever expands its triggers.
    ("content_report", content_report),
    ("content",        content),
    ("daf_workflow",   daf_workflow),
    ("events",         events),
    ("notes",          notes),
    ("donor_prep",     donor_prep),
    ("reports",        reports),
]


def route_intent(query: str, ctx):
    """
    Check handlers in priority order and return the first match.

    Args:
        query: The user's raw message
        ctx: RequestContext — supplies the actor whose role gates each
             handler, plus the draft/workflow state can_handle inspects.

    Returns:
        Tuple of (module_name: str, handle: callable) if matched, else None.
        The caller invokes handle(query, ctx) to get the response.
    """
    for name, module in HANDLER_CHAIN:
        allowed = getattr(module, "ALLOWED_ROLES", frozenset())
        if not ctx.is_allowed(allowed):
            logger.debug(
                f"Skipping handler '{name}': role '{ctx.actor.role}' not in "
                f"{sorted(allowed)}"
            )
            continue

        try:
            if module.can_handle(
                query,
                draft_state=ctx.draft_state,
                workflow_state=ctx.workflow_state,
            ):
                logger.info(f"Intent matched: {name}")
                return (name, module.handle)
        except Exception as e:
            # Still swallowed so one broken matcher cannot take down routing,
            # but no longer silently: a handler that raises here never matches
            # anything, and that used to be invisible.
            logger.warning(
                f"Handler '{name}' can_handle() raised {type(e).__name__}: {e} "
                f"— skipping it for this query",
                exc_info=True,
            )

    logger.info("No specific intent matched — falling back to context + Claude")
    return None