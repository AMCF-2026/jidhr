"""
Jidhr Assistant
===============
Slim orchestrator that routes queries to intent handlers
and falls back to context-enhanced Claude conversations.

Jidhr v1.3 — Refactored from 1,061-line monolith into modular intents.
"""

import logging
from datetime import datetime
from config import SYSTEM_PROMPT
from clients import OpenRouterClient, HubSpotClient, CSuiteClient
from intents import route_intent
from intents.queries import gather_context
from intents.daf_workflow import default_workflow_state

logger = logging.getLogger(__name__)


class JidhrAssistant:
    """Main assistant that orchestrates queries across systems.

    Draft state and workflow state are stored in the Flask session cookie
    so they survive across gunicorn workers.  The assistant loads them at
    the start of each request and saves them back at the end.

    Conversation history remains in-memory (per-worker) — losing it
    across workers is acceptable; losing draft/workflow state is not.
    """

    # Default empty states (used when session has nothing)
    _DEFAULT_DRAFT = {
        "active": False,
        "type": None,
        "subject": None,
        "body": None,
        "platform": None,
        "template": None,
        "link_url": None,
        "photo_url": None,
    }

    def __init__(self):
        logger.info("Initializing Jidhr Assistant")
        self.claude = OpenRouterClient()
        self.hubspot = HubSpotClient()
        self.csuite = CSuiteClient()
        self.conversation_history = []

        # In-memory defaults — overwritten by session on each request
        self.draft_state = dict(self._DEFAULT_DRAFT)
        self.workflow_state = default_workflow_state()

    def _load_state_from_session(self, flask_session):
        """Load draft and workflow state from Flask session cookie."""
        saved_draft = flask_session.get("draft_state")
        if saved_draft and isinstance(saved_draft, dict):
            self.draft_state.update(saved_draft)
            logger.debug(f"Loaded draft_state from session: active={saved_draft.get('active')}")

        saved_workflow = flask_session.get("workflow_state")
        if saved_workflow and isinstance(saved_workflow, dict):
            self.workflow_state.update(saved_workflow)
            logger.debug(f"Loaded workflow_state from session: active={saved_workflow.get('active')}")

    def _save_state_to_session(self, flask_session):
        """Persist draft and workflow state back to the Flask session cookie."""
        flask_session["draft_state"] = dict(self.draft_state)
        flask_session["workflow_state"] = dict(self.workflow_state)
        flask_session.modified = True

    def get_system_prompt(self) -> str:
        """Get system prompt with current date."""
        return SYSTEM_PROMPT.format(
            current_date=datetime.now().strftime("%B %d, %Y")
        )

    def process_query(self, user_message: str, flask_session=None) -> str:
        """
        Process a user query and return response.

        Routing priority:
          1. Intent handlers (sync, content, daf_workflow, notes, donor_prep, reports)
          2. Context gathering + Claude fallback

        Args:
            user_message: The user's raw message
            flask_session: Flask session object for cross-worker state persistence.
                          If provided, draft_state and workflow_state are loaded
                          from it at the start and saved back at the end.
        """
        # Load state from session cookie (survives across workers)
        if flask_session is not None:
            self._load_state_from_session(flask_session)

        logger.info(f"Processing query: {user_message[:50]}...")

        try:
            # --- 1. Check intent handlers ---
            match = route_intent(user_message, self.draft_state, self.workflow_state)
            if match:
                name, handler = match
                logger.info(f"Routing to intent: {name}")
                try:
                    response = handler(user_message, self)
                except Exception as e:
                    logger.error(f"Intent handler '{name}' error: {e}")
                    response = f"❌ Something went wrong with {name}: {e}"
                self._add_to_history(user_message, response)
                return response

            # --- 2. Fallback: gather context + send to Claude ---
            self.conversation_history.append({
                "role": "user",
                "content": user_message,
            })

            context = gather_context(user_message, self.hubspot, self.csuite)
            if context:
                enhanced = f"{user_message}\n\n[System Context - Real Data]\n{context}"
                self.conversation_history[-1]["content"] = enhanced
                logger.info(f"Added context: {len(context)} chars")

            response = self.claude.chat(
                messages=self.conversation_history,
                system_prompt=self.get_system_prompt(),
            )

            self.conversation_history.append({
                "role": "assistant",
                "content": response,
            })

            # Keep history manageable (last 20 exchanges)
            if len(self.conversation_history) > 40:
                self.conversation_history = self.conversation_history[-40:]
                logger.info("Trimmed conversation history")

            return response

        finally:
            # Always persist draft/workflow state back to the session cookie
            if flask_session is not None:
                self._save_state_to_session(flask_session)

    def clear_history(self, flask_session=None):
        """Clear conversation history and all active states."""
        logger.info("Clearing conversation history and states")
        self.conversation_history = []
        self.draft_state.update(dict(self._DEFAULT_DRAFT))
        self.workflow_state.update(default_workflow_state())

        # Clear session cookie state too
        if flask_session is not None:
            flask_session.pop("draft_state", None)
            flask_session.pop("workflow_state", None)
            flask_session.modified = True

    # ----- Internal helpers -----

    def _add_to_history(self, user_message: str, response: str):
        """Append a user/assistant exchange to conversation history."""
        self.conversation_history.append({"role": "user", "content": user_message})
        self.conversation_history.append({"role": "assistant", "content": response})

        if len(self.conversation_history) > 40:
            self.conversation_history = self.conversation_history[-40:]
            logger.info("Trimmed conversation history")


# ---------------------------------------------------------------------------
# Per-user assistant instances (per-worker; reconstructed if missing)
# ---------------------------------------------------------------------------

_assistants: dict[str, JidhrAssistant] = {}


def get_assistant(user_id: str = "default") -> JidhrAssistant:
    """Get or create an assistant instance for the given user.

    Each gunicorn worker maintains its own dict.  If a user's assistant
    doesn't exist on this worker (e.g. request routed to a different
    worker than last time), a fresh instance is created transparently.
    Conversation history is lost across workers — this is acceptable
    for a small team and avoids external session stores.
    """
    if user_id not in _assistants:
        logger.info(f"Creating new assistant instance for user: {user_id}")
        _assistants[user_id] = JidhrAssistant()
    return _assistants[user_id]
