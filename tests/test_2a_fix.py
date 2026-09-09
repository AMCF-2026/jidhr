"""Step 2a-fix: GC routing with a name in the middle, name punctuation,
and a guard message that does not over-promise.
"""

import logging

import pytest

from intents import notes, route_intent
from intents.context import Actor, RequestContext, Services, new_draft_state

ACTOR = Actor(user_id=1, email="staff@amuslimcf.org", role="staff")


def make_ctx(hubspot=None, workflow_state=None):
    return RequestContext(
        actor=ACTOR,
        services=Services(hubspot=hubspot, csuite=None, claude=None),
        draft_state=new_draft_state(),
        workflow_state=workflow_state if workflow_state is not None else {},
        conversation_history=[],
    )


def _contact(cid, first, last, email, codes=""):
    return {"id": cid, "properties": {
        "firstname": first, "lastname": last, "email": email,
        "constituent_codes": codes}}


class RecordingHubSpot:
    def __init__(self, contacts):
        self._contacts = contacts
        self.writes = []
        self.searches = []

    def search_contacts(self, query, limit=10):
        self.searches.append(query)
        return {"results": self._contacts}

    def update_giving_circle_status(self, contact_id, status):
        self.writes.append(("gc", contact_id, status))
        return {"id": contact_id}

    def create_call_note(self, body, contact_id):
        self.writes.append(("call", contact_id, body))
        return {"id": "n1"}

    def create_meeting_note(self, title, body, contact_id):
        self.writes.append(("meeting", contact_id, body))
        return {"id": "n1"}

    def create_note(self, body, contact_id):
        self.writes.append(("note", contact_id, body))
        return {"id": "n1"}

    @staticmethod
    def get_contact_url(contact_id):
        return f"https://hubspot.example/contact/{contact_id}"


# ===========================================================================
# 1. GC status commands route with the name in the middle
# ===========================================================================

class TestGCRouting:
    ROUTED = [
        "upgrade Sara to voting member",
        "Upgrade Sara Ahmad to voting member",
        "upgrade Sara to member",
        "make Lisa a voting member",
        "make voting member",
        "upgrade to voting member",
        "set gc status for Ahmed to member",
        "upgrade gc for Sara",
        "upgrade giving circle for Aaliyah",
        "giving circle status for Sara",
    ]

    @pytest.mark.parametrize("query", ROUTED)
    def test_can_handle_claims_the_command(self, query):
        """Substring matching missed these: the name sits between the words."""
        assert notes.can_handle(query), query

    @pytest.mark.parametrize("query", ROUTED)
    def test_it_is_recognised_as_a_gc_command(self, query):
        assert notes.is_gc_status_command(query.lower()), query

    def test_the_docstring_example_now_reaches_the_gc_handler(self):
        """_handle_gc_upgrade's own docstring advertises this phrasing."""
        hubspot = RecordingHubSpot(
            [_contact("1", "Sara", "Ali", "sara@example.org")])
        ctx = make_ctx(hubspot=hubspot)

        out = notes.handle("upgrade Sara to voting member", ctx)

        assert hubspot.writes == [("gc", "1", "GC Voting Member")]
        assert "Sara Ali" in out
        assert "What should I note" not in out, "must not fall to the note path"

    def test_the_name_is_extracted_from_the_middle(self):
        hubspot = RecordingHubSpot(
            [_contact("1", "Sara", "Ali", "sara@example.org")])
        notes.handle("upgrade Sara to voting member", make_ctx(hubspot=hubspot))

        assert hubspot.searches == ["sara"]

    def test_a_note_mentioning_an_upgrade_is_not_hijacked(self):
        """The verb forms are start-anchored precisely to protect this."""
        query = "log a call with Sara - we discussed her upgrade to voting member"

        assert not notes.is_gc_status_command(query.lower())

        hubspot = RecordingHubSpot(
            [_contact("1", "Sara", "Ali", "sara@example.org")])
        out = notes.handle(query, make_ctx(hubspot=hubspot))

        kinds = [w[0] for w in hubspot.writes]
        assert kinds == ["call"], "must log a note, not change GC status"
        assert "✅" in out

    @pytest.mark.parametrize("query", [
        "log call with Ahmed - discussed the DAF timeline",
        "just spoke with Lisa about the gala",
        "sync donations",
        "what is our fee structure",
    ])
    def test_non_gc_queries_are_not_claimed_as_gc(self, query):
        assert not notes.is_gc_status_command(query.lower())

    def test_voting_and_plain_member_map_to_different_statuses(self):
        hubspot = RecordingHubSpot(
            [_contact("1", "Sara", "Ali", "sara@example.org")])
        notes.handle("upgrade Sara to voting member", make_ctx(hubspot=hubspot))
        assert hubspot.writes[-1][2] == "GC Voting Member"

        hubspot2 = RecordingHubSpot(
            [_contact("1", "Sara", "Ali", "sara@example.org")])
        notes.handle("set gc status for Sara to member",
                     make_ctx(hubspot=hubspot2))
        assert hubspot2.writes[-1][2] == "American Muslim Women's Giving Circle"

    def test_an_ambiguous_gc_command_still_refuses_to_guess(self):
        hubspot = RecordingHubSpot([
            _contact("1", "Sara", "Ali", "sara.a@example.org"),
            _contact("2", "Sara", "Noor", "sara.n@example.org"),
        ])
        state = {}
        out = notes.handle("upgrade Sara to voting member",
                           make_ctx(hubspot=hubspot, workflow_state=state))

        assert hubspot.writes == []
        assert "1." in out and "2." in out
        assert state["pending_contact_pick"]["action"] == "gc_status"


# ===========================================================================
# 2. Extracted names lose trailing punctuation
# ===========================================================================

class TestNamePunctuation:
    @pytest.mark.parametrize("query,expected", [
        ("log call with Ahmed Khan.", "Ahmed Khan"),
        ("log call with Ahmed Khan!", "Ahmed Khan"),
        ("log call with Ahmed Khan?", "Ahmed Khan"),
        ("log call with Ahmed Khan,", "Ahmed Khan"),
        ("met with Lisa?", "Lisa"),
        ("log call with Ahmed Khan - discussed timeline", "Ahmed Khan"),
    ])
    def test_note_contact_name_is_clean(self, query, expected):
        assert notes._parse_note_query(query)["contact_name"] == expected

    @pytest.mark.parametrize("query,expected", [
        ("upgrade sara to voting member.", "sara"),
        ("set gc status for ahmed khan!", "ahmed khan"),
        ("upgrade giving circle for aaliyah?", "aaliyah"),
    ])
    def test_gc_name_is_clean(self, query, expected):
        assert notes._extract_gc_name(query) == expected

    def test_the_search_receives_the_cleaned_name(self):
        """"Khan." and "Khan" must not be two different HubSpot searches."""
        hubspot = RecordingHubSpot(
            [_contact("1", "Ahmed", "Khan", "a@example.org")])
        notes.handle("log call with Ahmed Khan. - discussed timeline",
                     make_ctx(hubspot=hubspot))

        assert hubspot.searches == ["Ahmed Khan"]

    def test_internal_punctuation_is_preserved(self):
        """Only the edges are trimmed — real names contain . ' and -."""
        assert notes._clean_name("O'Brien-Smith") == "O'Brien-Smith"
        assert notes._clean_name("  Dr. Amina  ") == "Dr. Amina"

    def test_clean_name_is_safe_on_empty_input(self):
        assert notes._clean_name("") == ""
        assert notes._clean_name(None) == ""
        assert notes._clean_name("...") == ""


# ===========================================================================
# 3. The guard message does not claim a rollback that never happened
# ===========================================================================

class TestGuardMessage:
    def _routed(self, monkeypatch, handler):
        class Module:
            ALLOWED_ROLES = frozenset({"admin", "staff"})

            def can_handle(self, query, **kwargs):
                return True

            handle = staticmethod(handler)

        monkeypatch.setattr("intents.HANDLER_CHAIN", [("boomer", Module())])
        return route_intent("anything", make_ctx())

    def test_the_message_warns_rather_than_reassures(self, monkeypatch, caplog):
        def handler(query, ctx):
            raise RuntimeError("hubspot exploded")

        _, wrapped = self._routed(monkeypatch, handler)

        with caplog.at_level(logging.WARNING, logger="intents"):
            out = wrapped("anything", make_ctx())

        assert "boomer hit an error" in out
        assert "hubspot exploded" in out
        assert "This action may not have completed — check before retrying." in out

    def test_it_no_longer_promises_nothing_changed(self, monkeypatch):
        """A handler can raise AFTER writing; claiming a rollback invites a
        retry that writes twice."""
        def handler(query, ctx):
            raise RuntimeError("failed after the write")

        _, wrapped = self._routed(monkeypatch, handler)
        out = wrapped("anything", make_ctx())

        assert "Nothing was changed" not in out

    def test_a_partial_write_is_not_contradicted(self, monkeypatch):
        written = []

        def handler(query, ctx):
            written.append("csuite profile")
            raise RuntimeError("fell over after creating the profile")

        _, wrapped = self._routed(monkeypatch, handler)
        out = wrapped("anything", make_ctx())

        assert written == ["csuite profile"]
        assert "may not have completed" in out

    def test_a_successful_handler_says_nothing_of_the_sort(self, monkeypatch):
        _, wrapped = self._routed(monkeypatch, lambda query, ctx: "done")
        assert wrapped("anything", make_ctx()) == "done"

    def test_the_router_source_no_longer_carries_the_old_claim(self):
        import pathlib

        source = (pathlib.Path(__file__).resolve().parent.parent
                  / "intents" / "__init__.py").read_text()
        assert "Nothing was changed" not in source
