"""Step 2a-polish: a resolved pick must not be re-asked, the assistant's
backstop must match the router, and a note's contact name must not keep the
connective that introduced it.
"""

import pytest

from intents import notes
from intents.queries import _gather_fund_context, gather_context
from intents.context import Actor, RequestContext, Services, new_draft_state

ACTOR = Actor(user_id=1, email="staff@amuslimcf.org", role="staff")

RAW1 = "Ramadan Relief Fund-(DAF0101)"
RAW2 = "Ramadan Iftar Fund-(DAF0102)"


class FundCSuite:
    def __init__(self):
        self.display_calls = []

    def search_funds(self, term):
        return {"success": True, "data": {"results": [
            {"id": 111, "name": RAW1},
            {"id": 222, "name": RAW2},
        ]}}

    def get_fund(self, fund_id):
        self.display_calls.append(fund_id)
        names = {111: RAW1, 222: RAW2}
        balances = {111: "1200.00", 222: "7500.00"}
        return {"success": True, "data": {
            "funit_id": fund_id, "fund_name": names[fund_id],
            "current_fundbalance": balances[fund_id]}}

    def get_funds(self, limit=20, offset=0):
        return {"success": True, "data": {"results": []}}

    def get_grants_by_fund(self, fund_id, limit=10):
        return {"success": True, "data": {"results": []}}


# ===========================================================================
# 1a. The fund pick tells the model what the digit meant
# ===========================================================================

class TestFundPickDirective:
    def _pick(self, digit="2"):
        csuite = FundCSuite()
        state = {}
        _gather_fund_context("balance for Ramadan", "balance for ramadan",
                             csuite, state)
        parts = _gather_fund_context(digit, digit, csuite, state)
        return csuite, "\n\n".join(parts)

    def test_the_context_begins_with_the_directive(self):
        _, context = self._pick("2")

        assert context.startswith("[User replied '2' to a numbered fund list")

    def test_the_directive_names_the_selected_fund_and_code(self):
        _, context = self._pick("2")

        assert "selected: Ramadan Iftar Fund (DAF0102)" in context

    def test_the_directive_forbids_asking_again(self):
        _, context = self._pick("1")

        assert "Present the fund details below directly." in context
        assert "Do not ask what they meant." in context

    def test_the_digit_in_the_directive_is_the_one_the_user_typed(self):
        _, context = self._pick("1")

        assert "User replied '1'" in context
        assert "Ramadan Relief Fund (DAF0101)" in context

    def test_the_fund_details_still_follow_the_directive(self):
        csuite, context = self._pick("2")

        assert csuite.display_calls == [222]
        assert "$7,500.00" in context
        assert context.index("[User replied") < context.index("Current balance")

    def test_a_fund_without_a_code_falls_back_to_its_id(self):
        class NoCode(FundCSuite):
            def search_funds(self, term):
                return {"success": True, "data": {"results": [
                    {"id": 111, "name": "Plain Fund One"},
                    {"id": 222, "name": "Plain Fund Two"},
                ]}}

            def get_fund(self, fund_id):
                self.display_calls.append(fund_id)
                return {"success": True, "data": {
                    "funit_id": fund_id, "fund_name": "Plain Fund Two",
                    "current_fundbalance": "1.00"}}

        csuite = NoCode()
        state = {}
        _gather_fund_context("balance for Plain", "balance for plain",
                             csuite, state)
        context = "\n\n".join(_gather_fund_context("2", "2", csuite, state))

        assert "selected: Plain Fund Two (id 222)" in context

    def test_the_directive_reaches_gather_context(self):
        csuite = FundCSuite()
        state = {}
        gather_context("balance for Ramadan", None, csuite, state)
        context = gather_context("2", None, csuite, state)

        assert context.startswith("[User replied '2'")

    def test_a_normal_fund_query_carries_no_directive(self):
        csuite = FundCSuite()
        context = "\n\n".join(
            _gather_fund_context("fund 222", "fund 222", csuite, {}))

        assert "User replied" not in context
        assert "Current balance" in context


# ===========================================================================
# 1b. Several matching contacts must not be silently narrowed to one
# ===========================================================================

class TestContactAmbiguityDirective:
    class HubSpot:
        def __init__(self, contacts):
            self._contacts = contacts

        def search_contacts(self, query, limit=10):
            return {"results": self._contacts}

        def get_contacts(self, limit=10):
            return {"results": []}

    @staticmethod
    def _contact(cid, first, last, email):
        return {"id": cid, "properties": {
            "firstname": first, "lastname": last, "email": email}}

    class Csuite:
        def search_profiles(self, name):
            return {"success": True, "data": {"results": []}}

    def test_two_matches_tell_the_model_not_to_choose(self):
        from intents.queries import _gather_contact_context

        hubspot = self.HubSpot([
            self._contact("1", "Ahmed", "Khan", "a.k@example.org"),
            self._contact("2", "Ahmed", "Siddiqui", "a.s@example.org"),
        ])
        parts = _gather_contact_context(
            "who is Ahmed", "who is ahmed", hubspot, self.Csuite())
        context = "\n\n".join(parts)

        assert "2 HubSpot contacts match" in context
        assert "Do not pick one" in context
        assert "a.k@example.org" in context and "a.s@example.org" in context

    def test_a_single_match_carries_no_directive(self):
        from intents.queries import _gather_contact_context

        hubspot = self.HubSpot(
            [self._contact("1", "Ahmed", "Khan", "a.k@example.org")])
        context = "\n\n".join(_gather_contact_context(
            "who is Ahmed", "who is ahmed", hubspot, self.Csuite()))

        assert "Do not pick one" not in context
        assert "a.k@example.org" in context


# ===========================================================================
# 1c. The contact pick in notes.py answers without the model
# ===========================================================================

class TestContactPickIsDeterministic:
    class HubSpot:
        def __init__(self, contacts):
            self._contacts = contacts
            self.writes = []

        def search_contacts(self, query, limit=10):
            return {"results": self._contacts}

        def create_call_note(self, body, contact_id):
            self.writes.append(contact_id)
            return {"id": "n1"}

        @staticmethod
        def get_contact_url(contact_id):
            return f"https://hubspot.example/{contact_id}"

    def test_the_pick_is_answered_by_the_handler_not_the_model(self):
        """No directive is needed: this never becomes Claude context."""
        contacts = [
            {"id": "1", "properties": {"firstname": "Ahmed", "lastname": "Khan",
                                       "email": "a.k@example.org"}},
            {"id": "2", "properties": {"firstname": "Ahmed",
                                       "lastname": "Siddiqui",
                                       "email": "a.s@example.org"}},
        ]
        hubspot = self.HubSpot(contacts)
        state = {}
        ctx = RequestContext(
            actor=ACTOR,
            services=Services(hubspot=hubspot, csuite=None, claude=None),
            draft_state=new_draft_state(), workflow_state=state,
            conversation_history=[])

        notes.handle("log call with Ahmed - discussed timeline", ctx)
        assert hubspot.writes == []

        out = notes.handle("2", ctx)

        assert hubspot.writes == ["2"]
        assert "✅" in out
        assert "Ahmed Siddiqui" in out


# ===========================================================================
# 2. The assistant backstop matches the router
# ===========================================================================

class TestBackstopMessage:
    def test_the_assistant_uses_the_same_wording_as_the_router(self, monkeypatch):
        from assistant import JidhrAssistant

        def handler(query, ctx):
            raise RuntimeError("hubspot exploded")

        class Module:
            ALLOWED_ROLES = frozenset({"admin", "staff"})

            def can_handle(self, query, **kwargs):
                return True

            handle = staticmethod(handler)

        monkeypatch.setattr("intents.HANDLER_CHAIN", [("boomer", Module())])

        a = JidhrAssistant.__new__(JidhrAssistant)
        a.claude = a.hubspot = a.csuite = None
        a.conversation_history = []
        a.services = Services(None, None, None)
        a.draft_state, a.workflow_state = new_draft_state(), {}

        out = a.process_query("anything", ACTOR)

        assert "This action may not have completed — check before retrying." in out
        assert "Nothing was changed" not in out

    def test_the_phrase_is_gone_from_the_codebase(self):
        import pathlib

        root = pathlib.Path(__file__).resolve().parent.parent
        for name in ("assistant.py", "intents/__init__.py"):
            assert "Nothing was changed" not in (root / name).read_text(), name


# ===========================================================================
# 3. A connective is not part of the contact's name
# ===========================================================================

class TestNameLead:
    @pytest.mark.parametrize("query,expected", [
        ("log a note about Sara!", "Sara"),
        ("log a note about Sara Ahmad - sent follow-up", "Sara Ahmad"),
        ("note about Lisa: sent email", "Lisa"),
        ("log a note on Ahmed", "Ahmed"),
        ("log a note re: Sara", "Sara"),
        ("log call with Ahmed Khan.", "Ahmed Khan"),
        ("log a note for Lisa - sent the deck", "Lisa"),
    ])
    def test_the_connective_is_stripped(self, query, expected):
        assert notes._parse_note_query(query)["contact_name"] == expected

    def test_a_colon_after_re_is_not_read_as_the_body_separator(self):
        """"re: Sara" used to split into contact "re", body "Sara"."""
        parsed = notes._parse_note_query("log a note re: Sara")

        assert parsed["contact_name"] == "Sara"
        assert parsed["contact_name"] != "re"

    @pytest.mark.parametrize("name,expected", [
        ("about Sara", "Sara"),
        ("with Ahmed", "Ahmed"),
        ("for Lisa", "Lisa"),
        ("on Ahmed", "Ahmed"),
        ("re: Sara", "Sara"),
    ])
    def test_clean_name_drops_the_lead(self, name, expected):
        assert notes._clean_name(name) == expected

    @pytest.mark.parametrize("name", [
        "Ron", "Onyx", "Forrest", "Withers", "Abou Bakr", "Reem",
    ])
    def test_names_that_merely_start_with_those_letters_survive(self, name):
        """The strip is word-bounded: "Ron" is not "on" with an R."""
        assert notes._clean_name(name) == name

    def test_internal_punctuation_is_still_preserved(self):
        assert notes._clean_name("O'Brien-Smith") == "O'Brien-Smith"
        assert notes._clean_name("Dr. Amina") == "Dr. Amina"

    def test_the_search_receives_the_stripped_name(self):
        searches = []

        class HubSpot:
            def search_contacts(self, query, limit=10):
                searches.append(query)
                return {"results": []}

        ctx = RequestContext(
            actor=ACTOR,
            services=Services(hubspot=HubSpot(), csuite=None, claude=None),
            draft_state=new_draft_state(), workflow_state={},
            conversation_history=[])

        notes.handle("log a note about Sara Ahmad - sent the deck", ctx)

        assert searches == ["Sara Ahmad"]

    def test_stacked_connectives_are_bounded(self):
        assert notes._clean_name("about with Sara") == "Sara"
        assert notes._clean_name("about about about about Sara").endswith("Sara")
