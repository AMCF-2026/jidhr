"""A draft survives the gap between two messages.

Production, 2026-09-24: a brief was drafted successfully, and the very
next message — "Save this to the AMCF template" — answered "I don't have
a recent draft to act on". The draft lived in the Flask session cookie,
which is client-side and capped near 4 KB. The 2026-09-22 Giving Circle
draft measured 3,851 signed bytes against a 4,096-byte cap, so the
largest and most valuable drafts were exactly the ones the browser
silently dropped.

No network. A fake store stands in for Postgres and interprets the SQL
clients/drafts.py generates, so a change to that SQL fails here rather
than in production.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest

from clients import drafts


# ---------------------------------------------------------------------------
# A pending_drafts table that can be queried
# ---------------------------------------------------------------------------

class FakeDrafts:
    """Stands in for clients.database.execute_query over pending_drafts."""

    def __init__(self, now=None):
        self.rows = {}          # (user_id, channel) -> {"draft", "expires_at"}
        self.now = now or datetime.now(timezone.utc)
        self.queries = []

    def __call__(self, sql, params=None, fetch=True):
        text = " ".join(str(sql).split())
        params = list(params or ())
        self.queries.append(text.split()[0])

        if text.startswith("INSERT INTO pending_drafts"):
            user_id, channel, payload, ttl_hours = params
            self.rows[(user_id, channel)] = {
                "draft": json.loads(payload),
                "expires_at": self.now + timedelta(hours=int(ttl_hours)),
            }
            return [{"id": len(self.rows)}]

        if text.startswith("SELECT draft"):
            row = self.rows.get((params[0], params[1]))
            if row is None or row["expires_at"] <= self.now:
                return []
            return [{"draft": row["draft"]}]

        if text.startswith("DELETE FROM pending_drafts WHERE user_id"):
            return 1 if self.rows.pop((params[0], params[1]), None) else 0

        if text.startswith("DELETE FROM pending_drafts WHERE expires_at"):
            gone = [k for k, v in self.rows.items()
                    if v["expires_at"] <= self.now]
            for k in gone:
                del self.rows[k]
            return len(gone)

        raise AssertionError(f"unrecognised pending_drafts query: {text}")


@pytest.fixture
def store(monkeypatch):
    fake = FakeDrafts()
    monkeypatch.setattr("clients.database.execute_query", fake)
    return fake


# A draft the size that actually broke. 5,084 characters of generated
# newsletter HTML, which is what the cookie could not hold.
BIG_BODY = ("<h2>Round one opens Saturday</h2>"
            "<p>Members choose three of the eight nominees.</p>") * 60


def a_draft(**over):
    draft = {
        "active": True,
        "created_at": "2026-09-24T10:00:00",
        "type": "email",
        "subject": "Voting Begins This Weekend",
        "body": BIG_BODY,
        "preview_text": "Round One voting opens Saturday",
        "button_label": "Cast your vote",
        "button_url": "https://www.grapevine.org/giving-circle/zjrhaGG",
        "template": None,
    }
    draft.update(over)
    return draft


# ---------------------------------------------------------------------------
# The bug: a draft made on one instance, saved on another
# ---------------------------------------------------------------------------

def test_a_draft_generated_on_one_instance_is_found_by_the_next(store):
    """Production runs eight gunicorn workers.

    The message that generates a draft and the message that saves it
    routinely land on different ones.
    """
    drafts.save(42, a_draft())

    restored = drafts.load(42)
    assert restored is not None, "the draft was lost between requests"
    assert restored["subject"] == "Voting Begins This Weekend"
    assert restored["body"] == BIG_BODY
    assert restored["button_url"] == \
        "https://www.grapevine.org/giving-circle/zjrhaGG"


def test_a_draft_far_too_big_for_a_cookie_survives(store):
    """The size is the whole point.

    Anything under about 4 KB survived a cookie too; the drafts that
    vanished were the long ones.
    """
    body = "x" * 200_000
    drafts.save(42, a_draft(body=body))
    assert drafts.load(42)["body"] == body


def test_one_requester_cannot_see_anothers_draft(store):
    drafts.save(42, a_draft(subject="Carl's draft"))
    drafts.save(43, a_draft(subject="Someone else's draft"))

    assert drafts.load(42)["subject"] == "Carl's draft"
    assert drafts.load(43)["subject"] == "Someone else's draft"


def test_channels_are_separate(store):
    drafts.save(42, a_draft(subject="web"), channel="web")
    drafts.save(42, a_draft(subject="slack"), channel="slack")

    assert drafts.load(42, channel="web")["subject"] == "web"
    assert drafts.load(42, channel="slack")["subject"] == "slack"


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def test_a_new_generation_replaces_the_old_draft(store):
    """A second draft means the person moved on from the first.

    Keeping both would only raise the question of which one "save this"
    meant.
    """
    drafts.save(42, a_draft(subject="First"))
    drafts.save(42, a_draft(subject="Second"))

    assert drafts.load(42)["subject"] == "Second"
    assert len(store.rows) == 1


def test_saving_deletes_the_pending_draft(store):
    drafts.save(42, a_draft())
    assert drafts.clear(42) is True
    assert drafts.load(42) is None


def test_clearing_a_draft_that_is_not_there_is_not_an_error(store):
    assert drafts.clear(42) is True
    assert drafts.load(42) is None


def test_an_expired_draft_is_never_returned(store):
    drafts.save(42, a_draft())
    assert drafts.load(42) is not None

    # Two hours and a minute later.
    store.now += timedelta(hours=drafts.TTL_HOURS, minutes=1)
    assert drafts.load(42) is None, "an aged-out draft came back"


def test_expiry_is_enforced_in_sql_not_by_the_sweeper(store):
    """An aged-out draft must not reappear just because nothing tidied up."""
    drafts.save(42, a_draft())
    store.now += timedelta(hours=drafts.TTL_HOURS, minutes=1)

    assert drafts.load(42) is None
    assert store.rows, "the row is still there — expiry came from the query"


def test_the_sweeper_removes_expired_rows(store):
    drafts.save(42, a_draft())
    drafts.save(43, a_draft())
    store.now += timedelta(hours=drafts.TTL_HOURS, minutes=1)
    drafts.save(44, a_draft())          # fresh, must survive

    assert drafts.sweep() == 2
    assert list(store.rows) == [(44, "web")]


def test_the_ttl_is_two_hours(store):
    drafts.save(42, a_draft())
    row = store.rows[(42, "web")]
    assert row["expires_at"] - store.now == timedelta(hours=2)


# ---------------------------------------------------------------------------
# Degrading, never guessing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("user_id", [None, "", "not-a-number"])
def test_no_usable_requester_means_no_draft_and_no_crash(store, user_id):
    assert drafts.save(user_id, a_draft()) is False
    assert drafts.load(user_id) is None
    assert store.rows == {}


def test_a_database_failure_reads_as_no_draft_rather_than_raising(
        monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("connection reset")

    monkeypatch.setattr("clients.database.execute_query", broken)

    assert drafts.load(42) is None
    assert drafts.save(42, a_draft()) is False
    assert drafts.clear(42) is False
    assert drafts.sweep() == 0


def test_there_is_no_in_memory_fallback():
    """A fallback that works on one worker of eight is worse than none.

    It reproduces once a day and never in testing.
    """
    import inspect
    source = inspect.getsource(drafts)
    assert "_CACHE" not in source
    assert "lru_cache" not in source
    for line in source.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or '"' in stripped:
            continue
        assert "= {}" not in stripped or "rows" in stripped


# ---------------------------------------------------------------------------
# End to end, across two assistant instances
# ---------------------------------------------------------------------------

class Actor:
    def __init__(self, user_id=42):
        self.user_id = user_id


def fresh_assistant():
    """A new JidhrAssistant, as a different gunicorn worker would build."""
    from assistant import JidhrAssistant
    return JidhrAssistant()


def test_generate_on_one_instance_then_save_on_a_fresh_one(store):
    """Carl's exact sequence: draft, then "Save this to the AMCF template".

    The two messages land on different workers; before this fix the
    second one answered "I don't have a recent draft to act on".
    """
    actor = Actor()

    generated = fresh_assistant()
    generated.draft_state.update(a_draft())
    generated._persist_draft(actor)

    saving = fresh_assistant()
    assert saving.draft_state.get("active") is not True, \
        "a fresh instance started with a draft already in it"

    saving._load_draft(actor)

    assert saving.draft_state["active"] is True
    assert saving.draft_state["subject"] == "Voting Begins This Weekend"
    assert saving.draft_state["body"] == BIG_BODY
    assert saving.draft_state["button_url"] == \
        "https://www.grapevine.org/giving-circle/zjrhaGG"


def test_saving_the_draft_removes_it_for_the_next_instance(store):
    actor = Actor()
    first = fresh_assistant()
    first.draft_state.update(a_draft())
    first._persist_draft(actor)

    # What _clear_draft_state does after a successful save.
    from intents.context import new_draft_state
    first.draft_state.update(new_draft_state())
    first._persist_draft(actor)

    later = fresh_assistant()
    later._load_draft(actor)
    assert later.draft_state.get("active") is not True
    assert store.rows == {}


def test_a_fresh_instance_with_no_stored_draft_starts_empty(store):
    later = fresh_assistant()
    later._load_draft(Actor(user_id=99))
    assert later.draft_state.get("active") is not True


def test_the_draft_no_longer_travels_in_the_session_cookie(store):
    """The cookie is what capped the draft near 4 KB."""
    class Session(dict):
        modified = False

    session = Session({"draft_state": {"active": True, "body": "stale"},
                       "workflow_state": {}})
    a = fresh_assistant()
    a._load_state_from_session(session)
    assert a.draft_state.get("body") != "stale", \
        "the cookie is still being read for drafts"

    a.draft_state.update(a_draft())
    a._save_state_to_session(session)
    assert "draft_state" not in session, \
        "the draft was written back into the cookie"


# ---------------------------------------------------------------------------
# General chat never looks like it produced a draft (2026-09-25)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("reply", [
    "📧 Email Formatted for HubSpot\n\nSubject: Voting begins\n\n…",
    "Email Formatted for HubSpot — here you go",
    "Here is your email, formatted and ready:",
    "**Subject:** Voting Begins This Weekend",
    "Draft email below:",
])
def test_a_chat_echo_dressed_as_a_draft_carries_the_warning(reply):
    """2026-09-25: general chat echoed a pasted brief back under
    "📧 Email Formatted for HubSpot". Nothing was generated and nothing
    was stored, so the save that followed found no draft — and the
    obvious next move is to paste the brief again."""
    from assistant import NOT_A_DRAFT_NOTICE, _mark_if_not_a_draft

    marked = _mark_if_not_a_draft(reply, draft_active=False)
    assert marked.startswith(NOT_A_DRAFT_NOTICE)
    assert "Format it for a HubSpot email" in marked
    assert reply in marked


def test_a_real_pending_draft_is_not_warned_about():
    from assistant import _mark_if_not_a_draft
    reply = "📧 Email Draft\n\n**Subject:** Voting begins"
    assert _mark_if_not_a_draft(reply, draft_active=True) == reply


@pytest.mark.parametrize("reply", [
    "The fund balance for END0026 is $52,400.",
    "I found 34 investment requests this month.",
    "",
])
def test_an_ordinary_answer_is_left_alone(reply):
    from assistant import _mark_if_not_a_draft
    assert _mark_if_not_a_draft(reply, draft_active=False) == reply
