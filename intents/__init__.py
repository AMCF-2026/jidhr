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
    provisional = None

    for name, module in HANDLER_CHAIN:
        allowed = getattr(module, "ALLOWED_ROLES", frozenset())
        if not ctx.is_allowed(allowed):
            logger.debug(
                f"Skipping handler '{name}': role '{ctx.actor.role}' not in "
                f"{sorted(allowed)}"
            )
            continue

        try:
            matched = module.can_handle(
                query,
                draft_state=ctx.draft_state,
                workflow_state=ctx.workflow_state,
            )
        except Exception as e:
            # Still swallowed so one broken matcher cannot take down routing,
            # but no longer silently: a handler that raises here never matches
            # anything, and that used to be invisible.
            logger.warning(
                f"Handler '{name}' can_handle() raised {type(e).__name__}: {e} "
                f"— skipping it for this query",
                exc_info=True,
            )
            continue

        if not matched:
            continue

        # A handler can say "I match, but only because I hold open state" —
        # an active draft, say. That is a weaker claim than a handler that
        # recognises the message itself, so keep scanning and let a later
        # handler take it. Priority order still decides between equals.
        if _claims_weakly(module, query, ctx):
            if provisional is None:
                logger.debug(
                    f"Handler '{name}' claims only via open state — holding")
                provisional = (name, module)
            continue

        logger.info(f"Intent matched: {name}")
        return (name, _guarded(name, module.handle))

    if provisional is not None:
        name, module = provisional
        logger.info(f"Intent matched: {name} (via open state)")
        return (name, _guarded(name, module.handle))

    logger.info("No specific intent matched — falling back to context + Claude")
    return None


def _guarded(name: str, handle):
    """Wrap a handler so a crash becomes a plain failure line, not a 500.

    The guarantee belongs here rather than in the caller: whoever invokes
    route_intent's callable gets the same behaviour, and the message says
    plainly that nothing changed instead of leaking a traceback.

    A handler that raises part-way through may already have written to
    HubSpot or CSuite. "Nothing was changed" is about this turn's outcome
    being unusable, so the message stays deliberately about the failure —
    anything a handler completed before raising is reported by the handler
    itself when it succeeds, never here.
    """

    def guarded_handle(query, ctx):
        try:
            return handle(query, ctx)
        except Exception as e:
            logger.warning(
                f"Handler '{name}' handle() raised {type(e).__name__}: {e}",
                exc_info=True,
            )
            return (
                f"⚠️ {name} hit an error: {e}. Nothing was changed."
            )

    guarded_handle.__name__ = getattr(handle, "__name__", "handle")
    guarded_handle.__doc__ = getattr(handle, "__doc__", None)
    guarded_handle.__wrapped__ = handle
    return guarded_handle


def _claims_weakly(module, query: str, ctx) -> bool:
    """Does this module match only because it is holding open state?

    Modules opt in by exposing `claims_by_draft_only`. Anything that does not
    is treated as a full-strength match, so this cannot change routing for a
    handler that has not asked for it.
    """
    predicate = getattr(module, "claims_by_draft_only", None)
    if predicate is None:
        return False

    try:
        return bool(predicate(
            query,
            draft_state=ctx.draft_state,
            workflow_state=ctx.workflow_state,
        ))
    except Exception as e:
        logger.warning(
            f"claims_by_draft_only() raised {type(e).__name__}: {e} — "
            f"treating the match as full strength", exc_info=True)
        return False