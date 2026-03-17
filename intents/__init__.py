"""
Jidhr Intent Registry
=====================
Central router that checks handlers in priority order (most specific first).

Usage from assistant.py:
    from intents import route_intent
    handler = route_intent(query, draft_state, workflow_state)
    if handler:
        module, func = handler
        response = func(query, assistant)

queries.py is NOT in this registry — it's a context gatherer, not a handler.
assistant.py calls it directly as the fallback path.
"""

import logging

from intents import sync_commands
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
    ("sync_commands", sync_commands),
    ("content",       content),
    ("daf_workflow",  daf_workflow),
    ("events",        events),
    ("notes",         notes),
    ("donor_prep",    donor_prep),
    ("reports",       reports),
]


def route_intent(query: str, draft_state: dict, workflow_state: dict):
    """
    Check handlers in priority order and return the first match.

    Args:
        query: The user's raw message
        draft_state: Current email/social draft state dict
        workflow_state: Current DAF/endowment workflow state dict

    Returns:
        Tuple of (module_name: str, handle: callable) if matched, else None.
        The caller invokes handle(query, assistant) to get the response.
    """
    for name, module in HANDLER_CHAIN:
        try:
            if module.can_handle(query, draft_state=draft_state, workflow_state=workflow_state):
                logger.info(f"Intent matched: {name}")
                return (name, module.handle)
        except Exception as e:
            logger.error(f"Error checking handler '{name}': {e}")

    logger.info("No specific intent matched — falling back to context + Claude")
    return None