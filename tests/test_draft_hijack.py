"""Step 1f: an open draft must not swallow unrelated messages.

The production failure this covers: with a draft active, content.can_handle
claimed every message, so "fund balance for END0026" was sent to the draft
refiner as feedback, and the model's "I can't do that" reply was written back
as the new draft body. The next refinement then ran against the refusal.

No network — Claude is a stub that returns whatever the test dictates.
"""

import logging
from datetime import datetime, timedelta

import pytest

from intents import content as C
from intents import route_intent
from intents.context import Actor, RequestContext, Services


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class StubClaude:
    def __init__(self, reply=""):
        self.reply = reply
        self.calls = 0

    def chat(self, messages=None, system_prompt=None, **kwargs):
        self.calls += 1
        return self.reply


def live_draft(**overrides):
    draft = {
        "active": True,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "type": "social",
        "subject": None,
        "body": "Join us for Iftar this Ramadan! Register at amuslimcf.org",
        "platform": "facebook",
        "template": None,
        "link_url": None,
        "photo_url": None,
    }
    draft.update(overrides)
    return draft


def make_ctx(draft_state=None, claude=None, workflow_state=None):
    return RequestContext(
        actor=Actor(user_id=1, email="staff@amuslimcf.org", role="staff"),
        services=Services(hubspot=None, csuite=None,
                          claude=claude or StubClaude()),
        draft_state=draft_state if draft_state is not None else {},
        workflow_state=workflow_state if workflow_state is not None else {},
        conversation_history=[],
    )


# ---------------------------------------------------------------------------
# 1. can_handle no longer claims everything
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("query", [
    "fund balance for END0026",
    "what is the balance of the Tanvir Family Fund",
    "who attended the gala?",
    "how many grants did we make last quarter",
    "show me recent donations",
    "pull up the profile for Ahmed",
    "uncashed checks report",
])
def test_unrelated_questions_are_not_claimed_by_an_open_draft(query):
    assert not C.can_handle(query, draft_state=live_draft())


@pytest.mark.parametrize("query", [
    "make it shorter",
    "shorter",
    "make it longer",
    "more formal",
    "add emojis",
    "punchier",
    "add a link",
    "switch to linkedin",
    "post it",
    "looks good",
    "schedule it for tuesday",
    "cancel draft",
])
def test_draft_feedback_is_still_claimed(query):
    assert C.can_handle(query, draft_state=live_draft())


def test_a_short_nudge_with_no_topic_words_is_claimed():
    assert C.can_handle("a bit warmer please", draft_state=live_draft())


def test_a_long_message_is_not_claimed_even_without_topic_words():
    query = ("could you please take another look at this and think about "
             "whether the overall framing really lands for our audience")
    assert not C.can_handle(query, draft_state=live_draft())


def test_new_content_commands_are_claimed_regardless_of_draft():
    assert C.can_handle("draft a linkedin post about EverWaqf", draft_state={})
    assert C.can_handle("create a task to call Ahmed", draft_state={})


def test_claims_by_draft_only_flags_the_weak_claim():
    assert C.claims_by_draft_only("add more emojis", draft_state=live_draft())
    # An explicit follow-up is a real claim, not a draft-only one.
    assert not C.claims_by_draft_only("post it", draft_state=live_draft())
    # So is a new draft request.
    assert not C.claims_by_draft_only("draft a facebook post",
                                      draft_state=live_draft())


# ---------------------------------------------------------------------------
# 2. Routing — the unrelated query reaches the fallback
# ---------------------------------------------------------------------------

def test_unrelated_query_falls_through_to_the_claude_fallback():
    ctx = make_ctx(draft_state=live_draft())
    assert route_intent("fund balance for END0026", ctx) is None


def test_draft_feedback_still_routes_to_content():
    ctx = make_ctx(draft_state=live_draft())
    match = route_intent("make it shorter", ctx)
    assert match is not None and match[0] == "content"


def test_another_handler_beats_a_draft_only_claim(monkeypatch):
    """The two-pass rule: a real match outranks 'I have a draft open'."""
    class Weak:
        ALLOWED_ROLES = frozenset({"admin", "staff"})

        def can_handle(self, query, **kwargs):
            return True

        def claims_by_draft_only(self, query, **kwargs):
            return True

        def handle(self, query, ctx):
            return "weak"

    class Strong:
        ALLOWED_ROLES = frozenset({"admin", "staff"})

        def can_handle(self, query, **kwargs):
            return True

        def handle(self, query, ctx):
            return "strong"

    monkeypatch.setattr("intents.HANDLER_CHAIN",
                        [("weak", Weak()), ("strong", Strong())])

    name, _ = route_intent("anything", make_ctx())
    assert name == "strong", "a full-strength match must beat a draft-only one"


def test_a_draft_only_claim_is_used_when_nothing_else_matches(monkeypatch):
    class Weak:
        ALLOWED_ROLES = frozenset({"admin", "staff"})

        def can_handle(self, query, **kwargs):
            return True

        def claims_by_draft_only(self, query, **kwargs):
            return True

        def handle(self, query, ctx):
            return "weak"

    monkeypatch.setattr("intents.HANDLER_CHAIN", [("weak", Weak())])

    name, _ = route_intent("anything", make_ctx())
    assert name == "weak"


def test_a_raising_claims_predicate_is_treated_as_a_full_match(monkeypatch,
                                                               caplog):
    class Broken:
        ALLOWED_ROLES = frozenset({"admin", "staff"})

        def can_handle(self, query, **kwargs):
            return True

        def claims_by_draft_only(self, query, **kwargs):
            raise RuntimeError("boom")

        def handle(self, query, ctx):
            return "broken"

    monkeypatch.setattr("intents.HANDLER_CHAIN", [("broken", Broken())])

    with caplog.at_level(logging.WARNING, logger="intents"):
        name, _ = route_intent("anything", make_ctx())

    assert name == "broken"


# ---------------------------------------------------------------------------
# 3. A refusal never becomes the draft body
# ---------------------------------------------------------------------------

REFUSALS = [
    "I'm sorry, but I can't help with that request.",
    "It looks like the post content wasn't included in your message.",
    "I'm not able to revise a post without seeing the original.",
    "Could you clarify what you'd like me to change?",
    "Unfortunately I don't have the draft you're referring to.",
    "",
    "ok",
]


@pytest.mark.parametrize("reply", REFUSALS)
def test_a_refusal_reply_leaves_the_body_untouched(reply):
    original = "Join us for Iftar this Ramadan! Register at amuslimcf.org"
    ctx = make_ctx(draft_state=live_draft(body=original),
                   claude=StubClaude(reply))

    response = C._refine_draft("make it shorter", ctx)

    assert ctx.draft_state["body"] == original
    assert "unchanged" in response.lower()


def test_a_real_revision_does_replace_the_body():
    revised = "Iftar this Ramadan — join us. Register at amuslimcf.org 🌙"
    ctx = make_ctx(draft_state=live_draft(), claude=StubClaude(revised))

    response = C._refine_draft("make it shorter", ctx)

    assert ctx.draft_state["body"] == revised
    assert "unchanged" not in response.lower()


def test_an_email_refusal_leaves_subject_and_body_untouched():
    ctx = make_ctx(
        draft_state=live_draft(type="email", subject="Ramadan Appeal",
                               body="<p>Original body</p>"),
        claude=StubClaude("I'm sorry, I don't have that draft."))

    C._refine_draft("more formal", ctx)

    assert ctx.draft_state["subject"] == "Ramadan Appeal"
    assert ctx.draft_state["body"] == "<p>Original body</p>"


def test_the_poisoning_loop_cannot_start():
    """One unrelated question must not change the draft at all."""
    original = "Join us for Iftar this Ramadan!"
    draft = live_draft(body=original)
    ctx = make_ctx(draft_state=draft)

    # Routing declines it...
    assert route_intent("fund balance for END0026", ctx) is None
    # ...and nothing touched the draft.
    assert draft["body"] == original
    assert draft["active"] is True


# ---------------------------------------------------------------------------
# 4. Cancel clears everything
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("query", [
    "cancel draft", "discard draft", "start over", "discard the draft",
])
def test_cancel_clears_the_draft(query):
    draft = live_draft()
    draft["pending_schedule"] = {"when": "tuesday"}
    draft["some_future_key"] = "debris"
    ctx = make_ctx(draft_state=draft)

    response = C.handle(query, ctx)

    assert draft["active"] is False
    assert draft["body"] is None
    assert "pending_schedule" not in draft, "stray keys must go too"
    assert "some_future_key" not in draft
    assert "discard" in response.lower() or "cancel" in response.lower()


def test_clear_drops_keys_the_default_shape_never_had():
    draft = live_draft()
    draft["pending_schedule"] = {"when": "tuesday"}
    draft["invented_later"] = 1
    ctx = make_ctx(draft_state=draft)

    C._clear_draft_state(ctx)

    assert set(draft) == set(C._EMPTY_DRAFT)


# ---------------------------------------------------------------------------
# 5. Logout clears the session
# ---------------------------------------------------------------------------

def test_logout_clears_draft_and_workflow_state(monkeypatch):
    import importlib
    import sys

    monkeypatch.setenv("SECRET_KEY", "test-secret-key")
    monkeypatch.setenv("DATABASE_URL", "")
    for name in ("app", "auth", "assistant", "config"):
        sys.modules.pop(name, None)
    app_module = importlib.import_module("app")

    client = app_module.app.test_client()
    with client.session_transaction() as session:
        session["draft_state"] = {"active": True, "body": "secret draft"}
        session["workflow_state"] = {"active": True}

    client.get("/logout")

    with client.session_transaction() as session:
        assert "draft_state" not in session
        assert "workflow_state" not in session


def test_auth_names_the_keys_it_clears():
    import auth

    assert "draft_state" in auth.SESSION_STATE_KEYS
    assert "workflow_state" in auth.SESSION_STATE_KEYS


# ---------------------------------------------------------------------------
# 6. Stale drafts
# ---------------------------------------------------------------------------

def _aged(hours):
    return (datetime.now() - timedelta(hours=hours)).isoformat(timespec="seconds")


def test_a_fresh_draft_is_active():
    assert C.draft_is_active(live_draft(created_at=_aged(1)))
    assert not C.draft_is_stale(live_draft(created_at=_aged(1)))


def test_a_draft_older_than_four_hours_is_inactive():
    old = live_draft(created_at=_aged(5))
    assert C.draft_is_stale(old)
    assert not C.draft_is_active(old)


def test_a_stale_draft_is_not_used_as_context_for_a_refinement():
    """The follow-up still reaches content — but as "no draft", not feedback.

    can_handle stays True so handle() can clear the aged-out draft; what
    matters is that the stale body is never treated as the thing being
    refined.
    """
    stale = live_draft(created_at=_aged(9))
    original = stale["body"]
    claude = StubClaude("SHOULD NEVER BE CALLED")
    ctx = make_ctx(draft_state=stale, claude=claude)

    response = C.handle("make it shorter", ctx)

    assert claude.calls == 0, "a stale draft must not be sent to the refiner"
    assert stale["active"] is False
    assert stale["body"] is None and original is not None
    assert "start fresh" in response.lower() or "don't have" in response.lower()


def test_a_stale_draft_does_not_claim_a_short_nudge():
    """A bare nudge with no live draft is not content's business."""
    assert not C.can_handle("a bit warmer please",
                            draft_state=live_draft(created_at=_aged(9)))


def test_a_stale_draft_is_cleared_by_the_next_content_command():
    draft = live_draft(created_at=_aged(9))
    ctx = make_ctx(draft_state=draft)

    C.handle("cancel draft", ctx)

    assert draft["active"] is False
    assert draft["body"] is None


def test_a_draft_without_created_at_is_left_active():
    """Drafts predating this change must not vanish mid-conversation."""
    draft = live_draft()
    draft.pop("created_at")

    assert C.draft_is_active(draft)
    assert not C.draft_is_stale(draft)


def test_an_unparseable_created_at_does_not_crash_routing():
    draft = live_draft(created_at="not a timestamp")

    assert C.draft_is_active(draft)
    assert not C.draft_is_stale(draft)


def test_new_drafts_record_created_at():
    ctx = make_ctx(claude=StubClaude("A brand new social post about EverWaqf!"))

    class HS:
        def get_available_social_platforms(self):
            return ["facebook"]

    ctx.services.hubspot = HS()
    C._initiate_social_post("draft a facebook post about EverWaqf", ctx)

    assert ctx.draft_state["created_at"]
    assert C.draft_is_active(ctx.draft_state)
