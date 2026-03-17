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
    """Main assistant that orchestrates queries across systems."""

    def __init__(self):
        logger.info("Initializing Jidhr Assistant")
        self.claude = OpenRouterClient()
        self.hubspot = HubSpotClient()
        self.csuite = CSuiteClient()
        self.conversation_history = []

        # Draft state for conversational content creation (email/social)
        self.draft_state = {
            "active": False,
            "type": None,       # "email" or "social"
            "subject": None,
            "body": None,
            "platform": None,   # for social: facebook, twitter, etc.
            "template": None,   # for email: amcf, giving circle
            "link_url": None,
            "photo_url": None,
        }

        # Workflow state for DAF/endowment inquiry processing
        self.workflow_state = default_workflow_state()

    def get_system_prompt(self) -> str:
        """Get system prompt with current date."""
        return SYSTEM_PROMPT.format(
            current_date=datetime.now().strftime("%B %d, %Y")
        )

    def process_query(self, user_message: str) -> str:
        """
        Process a user query and return response.

        Routing priority:
          1. Intent handlers (sync, content, daf_workflow, notes, donor_prep, reports)
          2. Context gathering + Claude fallback
        """
        logger.info(f"Processing query: {user_message[:50]}...")

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

    def clear_history(self):
        """Clear conversation history and all active states."""
        logger.info("Clearing conversation history and states")
        self.conversation_history = []
        self.draft_state.update({
            "active": False,
            "type": None,
            "subject": None,
            "body": None,
            "platform": None,
            "template": None,
            "link_url": None,
            "photo_url": None,
        })
        self.workflow_state.update(default_workflow_state())

    # ----- Internal helpers -----

    def _add_to_history(self, user_message: str, response: str):
        """Append a user/assistant exchange to conversation history."""
        self.conversation_history.append({"role": "user", "content": user_message})
        self.conversation_history.append({"role": "assistant", "content": response})

        if len(self.conversation_history) > 40:
            self.conversation_history = self.conversation_history[-40:]
            logger.info("Trimmed conversation history")


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_assistant = None


def get_assistant() -> JidhrAssistant:
    """Get or create the assistant instance."""
    global _assistant
    if _assistant is None:
        _assistant = JidhrAssistant()
    return _assistant
