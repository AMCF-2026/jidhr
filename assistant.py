"""
Jidhr Assistant
===============
Slim orchestrator that routes queries to intent handlers
and falls back to context-enhanced Claude conversations.

Per-request orchestration in ~190 lines: loads draft/workflow state
from the Flask session, routes the query through the intent registry,
falls back to context + Claude, then saves state back to the session.
"""

import logging
from datetime import datetime
from config import SYSTEM_PROMPT
from clients import OpenRouterClient, HubSpotClient, CSuiteClient
from clients import drafts
from clients.openrouter import OpenRouterError
from intents import route_intent
from intents.context import (
    Actor,
    RequestContext,
    Services,
    current_actor,
    current_intent,
    new_draft_state,
)
from intents.queries import gather_context
from intents.daf_workflow import default_workflow_state

logger = logging.getLogger(__name__)


# Openings a model uses when it thinks it has produced an email. On
# 2026-09-25 general chat echoed a pasted brief back under
# "📧 Email Formatted for HubSpot" — nothing was generated and nothing
# was stored, so the save command that followed found no draft. The reply
# looked exactly like success.
_DRAFT_HEADERS = (
    "📧", "email formatted", "email draft", "formatted for hubspot",
    "here is your email", "here's your email", "subject:",
    "**subject:**", "draft email", "newsletter draft",
)

NOT_A_DRAFT_NOTICE = (
    "⚠️ This is not a saved draft. To generate one, start with "
    "\"Format it for a HubSpot email:\"\n\n"
)


def _mark_if_not_a_draft(response: str, draft_active: bool) -> str:
    """Prepend a warning when a chat reply is dressed up as a draft.

    Only when no draft is actually pending. The cost of the missing line
    is someone typing "save this" at a reply that was never stored and
    being told there is nothing to save — twice over, because the obvious
    next move is to paste the brief again.
    """
    if draft_active or not response:
        return response
    head = " ".join(response.strip().split())[:120].lower()
    if any(marker in head for marker in _DRAFT_HEADERS):
        return NOT_A_DRAFT_NOTICE + response
    return response


class JidhrAssistant:
    """Main assistant that orchestrates queries across systems.

    Draft state and workflow state are stored in the Flask session cookie
    so they survive across gunicorn workers.  The assistant loads them at
    the start of each request and saves them back at the end.

    Conversation history remains in-memory (per-worker) — losing it
    across workers is acceptable; losing draft/workflow state is not.
    """

    def __init__(self):
        logger.info("Initializing Jidhr Assistant")
        self.claude = OpenRouterClient()
        self.hubspot = HubSpotClient()
        self.csuite = CSuiteClient()
        self.conversation_history = []

        # One Services instance for the life of the assistant. The clients are
        # stateless HTTP wrappers, so rebuilding them per request would only
        # throw away connection reuse.
        self.services = Services(
            hubspot=self.hubspot,
            csuite=self.csuite,
            claude=self.claude,
        )

        # In-memory defaults — overwritten by session on each request
        self.draft_state = new_draft_state()
        self.workflow_state = default_workflow_state()

    def build_context(self, user_row) -> RequestContext:
        """Assemble the RequestContext for one request.

        `user_row` is either an Actor or a mapping shaped like a `users` row
        (what clients.users returns). The state fields are passed by
        reference, not copied: handlers mutate them in place and
        _save_state_to_session writes the result back afterwards, which is
        exactly how it behaved when handlers held the assistant itself.
        """
        if isinstance(user_row, Actor):
            actor = user_row
        else:
            actor = Actor(
                user_id=int(user_row["user_id" if "user_id" in user_row else "id"]),
                email=user_row["email"],
                role=user_row.get("role") or "staff",
                csuite_profile_id=(
                    str(user_row["csuite_profile_id"])
                    if user_row.get("csuite_profile_id") is not None
                    else None
                ),
            )

        # Published for code too deep to be handed the context — currently
        # only clients/audit.py, which needs to say who made a write.
        current_actor.set(actor)

        return RequestContext(
            actor=actor,
            services=self.services,
            draft_state=self.draft_state,
            workflow_state=self.workflow_state,
            conversation_history=self.conversation_history,
        )

    def _load_state_from_session(self, flask_session):
        """Load workflow state from the Flask session cookie.

        The DRAFT no longer travels in the cookie — see clients/drafts.py.
        A newsletter draft measured 3,851 signed bytes against a browser's
        4,096-byte cap, so the largest and most valuable drafts were
        exactly the ones the browser silently dropped.
        """
        saved_draft = None
        if saved_draft and isinstance(saved_draft, dict):
            self.draft_state.update(saved_draft)
            logger.debug(f"Loaded draft_state from session: active={saved_draft.get('active')}")

        saved_workflow = flask_session.get("workflow_state")
        if saved_workflow and isinstance(saved_workflow, dict):
            self.workflow_state.update(saved_workflow)
            logger.debug(f"Loaded workflow_state from session: active={saved_workflow.get('active')}")

    def _save_state_to_session(self, flask_session):
        """Persist workflow state back to the Flask session cookie.

        The draft goes to Postgres instead, in _persist_draft.
        """
        flask_session.pop("draft_state", None)
        flask_session["workflow_state"] = dict(self.workflow_state)
        flask_session.modified = True

    # -- the pending draft, in Postgres --------------------------------

    def _load_draft(self, actor):
        """Restore the pending draft for this requester, if there is one."""
        user_id = getattr(actor, "user_id", None) or (
            actor.get("id") if isinstance(actor, dict) else None)
        stored = drafts.load(user_id) if user_id else None
        self.draft_state = new_draft_state()
        if stored:
            self.draft_state.update(stored)
            logger.debug("loaded pending draft: active=%s",
                         stored.get("active"))

    def _persist_draft(self, actor):
        """Write the draft back, or delete it if it is no longer live.

        Deleting on inactive is what makes save and cancel stick: both
        clear the state in place, and a row left behind would be offered
        back on the next message.
        """
        user_id = getattr(actor, "user_id", None) or (
            actor.get("id") if isinstance(actor, dict) else None)
        if not user_id:
            return
        if self.draft_state.get("active"):
            drafts.save(user_id, dict(self.draft_state))
        else:
            drafts.clear(user_id)

    def get_system_prompt(self) -> str:
        """Get system prompt with current date."""
        return SYSTEM_PROMPT.format(
            current_date=datetime.now().strftime("%B %d, %Y")
        )

    def process_query(self, user_message: str, actor, flask_session=None) -> str:
        """
        Process a user query and return response.

        Routing priority:
          1. Intent handlers (sync, content, daf_workflow, notes, donor_prep, reports)
          2. Context gathering + Claude fallback

        Args:
            user_message: The user's raw message
            actor: Actor (or a users-row mapping) identifying who is asking.
                   Handlers receive it via ctx.actor, and the router uses its
                   role to decide which handlers are eligible at all.
            flask_session: Flask session object for cross-worker state persistence.
                          If provided, draft_state and workflow_state are loaded
                          from it at the start and saved back at the end.
        """
        # Load state from session cookie (survives across workers) BEFORE the
        # context is built, so ctx points at the restored dicts rather than
        # the empty defaults.
        if flask_session is not None:
            self._load_state_from_session(flask_session)
        self._load_draft(actor)

        ctx = self.build_context(actor)

        logger.info(f"Processing query: {user_message[:50]}...")

        try:
            # --- 1. Check intent handlers ---
            match = route_intent(user_message, ctx)
            if match:
                name, handler = match
                logger.info(f"Routing to intent: {name}")
                # Set here rather than inside route_intent: the intent has to
                # be live while the handler runs (that is when writes happen),
                # and route_intent has returned by then. Reset in `finally`
                # so a nested call cannot inherit a stale label.
                intent_token = current_intent.set(name)
                # route_intent already wraps the handler so a crash comes back
                # as a plain failure line. This catch is the backstop for a
                # caller that got its handler some other way.
                try:
                    response = handler(user_message, ctx)
                except Exception as e:
                    logger.exception(f"Intent handler '{name}' error: {e}")
                    response = (
                        f"⚠️ {name} hit an error: {e}. "
                        "This action may not have completed — check before "
                        "retrying."
                    )
                finally:
                    current_intent.reset(intent_token)
                self._add_to_history(user_message, response)
                return response

            # --- 2. Fallback: gather context + send to Claude ---
            # The user turn is built here but only committed to history once
            # the model actually answers. A failed call used to leave the
            # question in history with the error string as its "answer",
            # which then went back to the model on the next turn as context.
            user_turn = {"role": "user", "content": user_message}

            # workflow_state is passed so the fallback gatherer can remember a
            # numbered fund pick between messages; without it the list is
            # printed but a following "1" resolves to nothing.
            context = gather_context(
                user_message, ctx.services.hubspot, ctx.services.csuite,
                ctx.workflow_state)
            if context:
                enhanced = f"{user_message}\n\n[System Context - Real Data]\n{context}"
                user_turn["content"] = enhanced
                logger.info(f"Added context: {len(context)} chars")

            try:
                response = ctx.services.claude.chat(
                    messages=self.conversation_history + [user_turn],
                    system_prompt=self.get_system_prompt(),
                )
            except OpenRouterError as e:
                logger.error(
                    "OpenRouter call failed (status=%s): %s", e.status, e.message)
                # Nothing is appended: this turn did not happen as far as the
                # conversation is concerned, so a retry starts clean.
                return (
                    f"⚠️ The AI service didn't respond (HTTP {e.status}). "
                    "Your message wasn't lost — try again in a moment."
                )

            # General chat must never look like it produced a draft.
            response = _mark_if_not_a_draft(response,
                                            self.draft_state.get("active"))

            self.conversation_history.append(user_turn)
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
            # Always persist: the workflow to the cookie, the draft to
            # Postgres. In `finally` because a handler that raised partway
            # through may still have changed the draft, and losing that is
            # how a draft "disappears" after an error.
            if flask_session is not None:
                self._save_state_to_session(flask_session)
            self._persist_draft(actor)

    def clear_history(self, flask_session=None, actor=None):
        """Clear conversation history and all active states."""
        logger.info("Clearing conversation history and states")
        self.conversation_history = []
        self.draft_state.update(new_draft_state())
        self.workflow_state.update(default_workflow_state())
        if actor is not None:
            self._persist_draft(actor)

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

_assistants: dict[int, JidhrAssistant] = {}


def get_assistant(user_id: int) -> JidhrAssistant:
    """Get or create an assistant instance for the given user.

    Keyed by users.id, not by email address: the id is stable even if
    someone's address changes, and it keeps conversation history from being
    shared between two rows that differ only in the case of their email.

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
