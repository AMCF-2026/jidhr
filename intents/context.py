"""
Request Context
===============
What a handler is given instead of a reference to the assistant.

Handlers used to receive the whole `JidhrAssistant` and reach into it for
whatever they needed — clients, draft state, conversation history. That made
the assistant a service locator: every handler could touch everything, the
real dependencies of a handler were invisible from its signature, and there
was no point at which "who is asking" could be checked.

`RequestContext` is that same data, made explicit and passed down. It carries
three things a handler may need:

    ctx.actor      — who is asking (identity and role)
    ctx.services   — the API clients, shared for the process
    ctx.<state>    — the per-request mutable state the assistant loads from
                     and saves back to the Flask session

`draft_state`, `workflow_state` and `conversation_history` are the *same*
objects the assistant holds, not copies: handlers mutate them in place and the
assistant persists them afterwards, exactly as before.

This module holds no logic beyond the dataclasses and `is_allowed`. Nothing
here should import a handler, and no handler should import another handler
through it.
"""

from dataclasses import dataclass, field
from typing import Any

# The complete set of roles the users table allows. Kept here so handlers and
# tests have one place to agree on, matching the CHECK constraint in schema.sql.
VALID_ROLES = frozenset({"admin", "staff", "donor"})


# The shape of an empty draft. Defined here rather than in either of its two
# users: the assistant seeds a new request with it and intents/content.py
# resets to it, and when those were separate literals they had already drifted
# apart by one key (`created_at`). Copy it — never hand out the shared dict.
DEFAULT_DRAFT_STATE = {
    "active": False,
    "created_at": None,
    "type": None,
    "subject": None,
    "body": None,
    "platform": None,
    "template": None,
    "link_url": None,
    "photo_url": None,
}


def new_draft_state() -> dict:
    """A fresh empty draft. Always a copy, so callers cannot alias the default."""
    return dict(DEFAULT_DRAFT_STATE)


@dataclass(frozen=True)
class Actor:
    """Who is making this request.

    Frozen: a handler must not be able to change who is asking partway
    through, which is the whole point of passing identity down explicitly.
    """

    user_id: int
    email: str
    role: str
    csuite_profile_id: str | None = None

    def is_allowed(self, roles) -> bool:
        """True if this actor's role is in `roles`."""
        return self.role in roles


@dataclass
class Services:
    """The shared API clients.

    One instance per assistant, not per request — these are stateless HTTP
    wrappers and rebuilding them per request would throw away connection
    pooling for no benefit.
    """

    hubspot: Any
    csuite: Any
    claude: Any


@dataclass
class RequestContext:
    """Everything a handler is allowed to reach.

    The three state fields are live references to the assistant's own dicts
    and list. Handlers mutate them in place; the assistant writes them back to
    the session at the end of the request.
    """

    actor: Actor
    services: Services
    draft_state: dict = field(default_factory=dict)
    workflow_state: dict = field(default_factory=dict)
    conversation_history: list = field(default_factory=list)

    def is_allowed(self, roles) -> bool:
        """True if this request's actor may use a handler requiring `roles`.

        Args:
            roles: Any container of role names, normally a handler module's
                   ALLOWED_ROLES frozenset.
        """
        return self.actor.is_allowed(roles)
