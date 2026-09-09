"""Step 1d: fund reference extraction, fund balance lookup, event matching.

No network. The CSuite client is a stub that records what it was asked for,
because most of these bugs were about calling the wrong endpoint with the
wrong id rather than about formatting the answer.
"""

import pytest

from intents.events import (
    _match_events,
    _split_event_date,
    take_pending_event_pick,
)
from intents.queries import _gather_fund_context, extract_fund_ref


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class StubCSuite:
    """Records calls; returns whatever the test hands it."""

    def __init__(self, search_results=None, fund=None, fund_error=None,
                 events=None):
        self._search_results = search_results if search_results is not None else []
        self._fund = fund or {}
        self._fund_error = fund_error
        self._events = events if events is not None else []
        self.search_calls = []
        self.display_calls = []
        self.event_list_calls = 0

    def search_funds(self, term):
        self.search_calls.append(term)
        return {"success": True, "data": {"results": self._search_results}}

    def get_fund(self, fund_id):
        self.display_calls.append(fund_id)
        if self._fund_error is not None:
            return {"success": False, "error": self._fund_error}
        return {"success": True, "data": self._fund}

    def get_funds(self, limit=20, offset=0):
        return {"success": True, "data": {"results": []}}

    def get_grants_by_fund(self, fund_id, limit=10):
        return {"success": True, "data": {"results": []}}

    def get_event_dates(self, limit=200):
        self.event_list_calls += 1
        return {"success": True, "data": {"results": self._events}}


def event(name, date=None, description=None):
    return {
        "event_date_id": abs(hash((name, date))) % 100000,
        "event_name": name,
        "event_description": description if description is not None else name,
        "event_date": date,
        "archived": 0,
    }


# ---------------------------------------------------------------------------
# 1. extract_fund_ref
# ---------------------------------------------------------------------------

def test_fund_code_is_recognised():
    assert extract_fund_ref("fund balance for END0026") == {"code": "END0026"}


def test_fund_code_is_case_insensitive_and_normalised():
    assert extract_fund_ref("balance for daf0123") == {"code": "DAF0123"}


def test_a_name_that_starts_with_a_number_is_not_a_fund_id():
    """The bug this step exists for: 200 is part of the fund's name."""
    assert extract_fund_ref("fund balance for 200 Muslim Women Who Care") is None


def test_a_keyword_introduced_number_is_a_fund_id():
    assert extract_fund_ref("fund 1046") == {"id": 1046}


def test_trailing_number_is_a_fund_id():
    assert extract_fund_ref("Calculate fees for fund 1234") == {"id": 1234}


def test_hash_prefixed_number_is_a_fund_id():
    assert extract_fund_ref("fund #1046") == {"id": 1046}


def test_number_followed_by_a_capitalised_word_is_treated_as_a_name():
    assert extract_fund_ref("fund 200 Muslim Women Who Care") is None


def test_plain_name_query_yields_no_reference():
    assert extract_fund_ref("what is the balance of the Tanvir Fund") is None


def test_number_spliced_out_of_a_longer_token_is_never_an_id():
    # The old \b(\d{2,})\b happily matched inside these.
    for query in ("fund END0026", "contact ABC1234XYZ", "fund 12ab34"):
        ref = extract_fund_ref(query)
        assert ref is None or "code" in ref


def test_empty_query_is_safe():
    assert extract_fund_ref("") is None
    assert extract_fund_ref(None) is None


# ---------------------------------------------------------------------------
# 2. Fund balance path
# ---------------------------------------------------------------------------

def test_exact_name_match_drives_the_display_call(caplog):
    """Two candidates, one exact — display must use THAT row's id."""
    csuite = StubCSuite(
        search_results=[
            {"id": 111, "name": "Tanvir Family Fund Endowment"},
            {"id": 222, "name": "Tanvir Family Fund"},
        ],
        fund={"funit_id": 222, "fund_name": "Tanvir Family Fund",
              "fgroup_id": 1002, "current_fundbalance": "125000.00",
              "short_name": "TAN0001", "fund_open_date": "2020-01-15"},
    )

    parts = _gather_fund_context(
        "balance for Tanvir Family Fund", "balance for tanvir family fund", csuite)

    assert csuite.display_calls == [222], "display must use the exact match's id"
    joined = "\n".join(parts)
    assert "Tanvir Family Fund" in joined
    assert "$125,000.00" in joined
    assert "1002" in joined and "DAF" in joined
    assert "TAN0001" in joined
    assert "fund_open_date: 2020-01-15" in joined


def test_ambiguous_search_lists_candidates_and_makes_no_display_call():
    csuite = StubCSuite(search_results=[
        {"id": 111, "name": "Ramadan Fund 2025"},
        {"id": 222, "name": "Ramadan Fund 2026"},
    ])

    parts = _gather_fund_context("balance for Ramadan Fund",
                                 "balance for ramadan fund", csuite)

    assert csuite.display_calls == [], "must not guess between two funds"
    joined = "\n".join(parts)
    assert "Ramadan Fund 2025" in joined
    assert "Ramadan Fund 2026" in joined
    assert "111" in joined and "222" in joined


def test_single_search_result_is_used_without_an_exact_match():
    csuite = StubCSuite(
        search_results=[{"id": 333, "name": "Some Long Fund Name"}],
        fund={"funit_id": 333, "fund_name": "Some Long Fund Name",
              "current_fundbalance": "10.00"},
    )
    _gather_fund_context("balance for Some Long", "balance for some long", csuite)
    assert csuite.display_calls == [333]


def test_numeric_id_goes_straight_to_display_with_no_search():
    csuite = StubCSuite(
        fund={"funit_id": 1046, "fund_name": "Direct Fund",
              "current_fundbalance": "5000.00"})

    _gather_fund_context("fund 1046", "fund 1046", csuite)

    assert csuite.search_calls == [], "a numeric id needs no search"
    assert csuite.display_calls == [1046]


def test_fund_code_searches_by_code():
    csuite = StubCSuite(
        search_results=[{"id": 777, "name": "END0026"}],
        fund={"funit_id": 777, "fund_name": "Endowment Fund",
              "current_fundbalance": "1.00"})

    _gather_fund_context("fund balance for END0026",
                         "fund balance for end0026", csuite)

    assert csuite.search_calls == ["END0026"]
    assert csuite.display_calls == [777]


def test_display_error_text_reaches_the_context_verbatim():
    csuite = StubCSuite(fund_error="Fund 9999 not found")

    parts = _gather_fund_context("fund 9999", "fund 9999", csuite)

    joined = "\n".join(parts)
    assert "Fund 9999 not found" in joined


def test_balance_is_never_read_from_a_field_called_balance():
    """funit/display has no `balance`; reading it always reported $0.00."""
    csuite = StubCSuite(
        fund={"funit_id": 1, "fund_name": "F", "balance": "999999.00",
              "current_fundbalance": "42.00"})

    parts = _gather_fund_context("fund 1", "fund 1", csuite)
    joined = "\n".join(parts)

    assert "$42.00" in joined
    assert "999,999" not in joined


def test_endowment_group_is_labelled():
    csuite = StubCSuite(
        fund={"funit_id": 1, "fund_name": "F", "fgroup_id": 1008,
              "current_fundbalance": "1.00"})
    joined = "\n".join(_gather_fund_context("fund 1", "fund 1", csuite))
    assert "1008" in joined and "Endowment" in joined


# ---------------------------------------------------------------------------
# 3. Event matching
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,expected", [
    ("Spring Gala — 2026-04-11", ("Spring Gala", "2026-04-11")),
    ("Spring Gala - 2026-04-11", ("Spring Gala", "2026-04-11")),
    ("Spring Gala – 2026-04-11", ("Spring Gala", "2026-04-11")),
    ("Spring Gala", ("Spring Gala", None)),
])
def test_trailing_date_is_split_off(text, expected):
    assert _split_event_date(text) == expected


def test_exact_title_beats_substring():
    events = [
        event("Gala Dinner 2026"),
        event("Gala"),
        event("Annual Gala Fundraiser"),
    ]
    matches = _match_events(events, "Gala", None)

    assert len(matches) == 1
    assert matches[0]["event_name"] == "Gala"


def test_prefix_beats_contains():
    events = [event("Annual Symposium 2026"), event("The Annual Symposium")]
    matches = _match_events(events, "Annual Symposium", None)

    assert [m["event_name"] for m in matches] == ["Annual Symposium 2026"]


def test_contains_tier_catches_a_whole_phrase():
    events = [event("The 2026 Spring Gala Dinner"), event("Winter Fundraiser")]
    matches = _match_events(events, "Spring Gala", None)

    assert [m["event_name"] for m in matches] == ["The 2026 Spring Gala Dinner"]


def test_word_level_tier_is_the_last_resort():
    events = [event("Symposium on Giving"), event("Winter Fundraiser")]
    matches = _match_events(events, "Giving Symposium", None)

    assert [m["event_name"] for m in matches] == ["Symposium on Giving"]


def test_date_filter_narrows_identical_titles():
    events = [
        event("Iftar", date="2026-03-20"),
        event("Iftar", date="2025-03-20"),
    ]
    matches = _match_events(events, "Iftar", "2026-03-20")

    assert len(matches) == 1
    assert matches[0]["event_date"] == "2026-03-20"


def test_date_filter_applies_at_the_word_level_tier_too():
    events = [
        event("Spring Community Iftar", date="2026-03-20"),
        event("Spring Community Iftar", date="2025-03-20"),
    ]
    matches = _match_events(events, "community gathering", "2026-03-20")

    assert all(m["event_date"] == "2026-03-20" for m in matches)


# ---------------------------------------------------------------------------
# The numbered pick
# ---------------------------------------------------------------------------

def _find(csuite, query, state, action="attendees"):
    from intents.events import _find_event
    return _find_event(query, query.lower(), csuite, state, action=action)


def test_two_matches_produce_a_numbered_list_and_a_pending_pick():
    csuite = StubCSuite(events=[
        event("Iftar 2025", date="2025-03-20"),
        event("Iftar 2026", date="2026-03-20"),
    ])
    state = {}

    result = _find(csuite, "who's registered for Iftar", state)

    assert isinstance(result, str)
    assert "1." in result and "2." in result
    pending = state["pending_event_pick"]
    assert pending["action"] == "attendees"
    assert len(pending["events"]) == 2


def test_a_bare_number_selects_the_matching_event():
    state = {"pending_event_pick": {
        "action": "attendees",
        "events": [event("First"), event("Second")],
    }}

    action, picked, extra = take_pending_event_pick("2", state)

    assert action == "attendees"
    assert picked["event_name"] == "Second"
    assert extra == {}
    assert "pending_event_pick" not in state, "a pick consumes the list"


def test_a_non_numeric_message_clears_the_pending_pick():
    state = {"pending_event_pick": {"action": "attendees",
                                    "events": [event("First")]}}

    action, picked, _ = take_pending_event_pick("show me donations", state)

    assert (action, picked) == (None, None)
    assert "pending_event_pick" not in state


def test_an_out_of_range_number_clears_without_picking():
    state = {"pending_event_pick": {"action": "attendees",
                                    "events": [event("Only")]}}

    action, picked, _ = take_pending_event_pick("7", state)

    assert (action, picked) == (None, None)
    assert "pending_event_pick" not in state


def test_pick_is_a_no_op_when_nothing_is_pending():
    state = {}
    assert take_pending_event_pick("2", state) == (None, None, {})


def test_more_than_nine_matches_reports_a_count_and_lists_nothing():
    csuite = StubCSuite(events=[
        event(f"Community Event {i}", date=f"2026-01-{i:02d}")
        for i in range(1, 12)
    ])
    state = {}

    result = _find(csuite, "who's registered for Community Event", state)

    assert isinstance(result, str)
    assert "11" in result
    assert "1." not in result, "must not print a long list"
    assert "pending_event_pick" not in state


def test_one_match_returns_the_event_and_sets_no_pending_pick():
    csuite = StubCSuite(events=[event("Only Event", date="2026-05-01")])
    state = {}

    result = _find(csuite, "who's registered for Only Event", state)

    assert isinstance(result, dict)
    assert result["event_name"] == "Only Event"
    assert "pending_event_pick" not in state


def test_query_with_a_trailing_date_resolves_to_one_event():
    csuite = StubCSuite(events=[
        event("Iftar", date="2025-03-20"),
        event("Iftar", date="2026-03-20"),
    ])
    state = {}

    result = _find(csuite, "who's registered for Iftar — 2026-03-20", state)

    assert isinstance(result, dict), result
    assert result["event_date"] == "2026-03-20"


def test_no_match_says_so_with_the_date_it_tried():
    csuite = StubCSuite(events=[event("Iftar", date="2026-03-20")])

    result = _find(csuite, "who's registered for Gala — 2026-03-20", {})

    assert isinstance(result, str)
    assert "2026-03-20" in result


# ---------------------------------------------------------------------------
# can_handle claims a pending pick only for digits
# ---------------------------------------------------------------------------

def test_can_handle_claims_a_bare_digit_while_a_pick_is_pending():
    from intents import events as events_module

    state = {"pending_event_pick": {"action": "attendees", "events": []}}
    assert events_module.can_handle("2", workflow_state=state)


def test_can_handle_ignores_unrelated_text_while_a_pick_is_pending():
    from intents import events as events_module

    state = {"pending_event_pick": {"action": "attendees", "events": []}}
    assert not events_module.can_handle("sync donations", workflow_state=state)


# ---------------------------------------------------------------------------
# Step 1e: profile id extraction shares the fund rule
# ---------------------------------------------------------------------------

from intents.queries import _extract_id, _gather_donation_context  # noqa: E402


def test_a_donation_query_naming_a_numeric_fund_extracts_no_id():
    """"200 Muslim Women Who Care" is a name, not profile 200."""
    assert _extract_id("donations for 200 Muslim Women Who Care") is None


def test_profile_keyword_introduces_an_id():
    assert _extract_id("donations for profile 19879") == "19879"


@pytest.mark.parametrize("query,expected", [
    ("donations for 19879", "19879"),          # trailing number
    ("profile #19879", "19879"),               # hash prefix
    ("donor 19879 giving history", "19879"),   # keyword-introduced
    ("show me recent donations", None),        # no number at all
    ("donations for 200 Cedar Street Fund", None),
])
def test_profile_id_rule_matches_the_fund_rule(query, expected):
    assert _extract_id(query) == expected


def test_profile_id_and_fund_ref_agree_on_the_same_query():
    """Both paths share _extract_bare_number, so they cannot drift."""
    for query in ("donations for 200 Muslim Women Who Care",
                  "totals for 1046"):
        fund = extract_fund_ref(query)
        profile = _extract_id(query)
        fund_id = fund.get("id") if fund else None
        assert (fund_id is None) == (profile is None)


def test_donation_gatherer_does_not_look_up_a_name_as_a_profile_id():
    """The whole point: no get_donations_by_profile(200) for a fund name."""
    class DonationStub:
        def __init__(self):
            self.by_profile = []

        def get_donations_by_profile(self, profile_id, limit=10):
            self.by_profile.append(profile_id)
            return {"success": True, "data": {"results": []}}

        def get_donations(self, limit=10, offset=0):
            return {"success": True, "data": {"results": []}}

    csuite = DonationStub()
    _gather_donation_context(
        "donations for 200 Muslim Women Who Care",
        "donations for 200 muslim women who care", csuite)

    assert csuite.by_profile == []


# ---------------------------------------------------------------------------
# Step 1e: _compare_events goes through _find_event
# ---------------------------------------------------------------------------

def _compare(csuite, query, state=None):
    from intents.events import _compare_events
    return _compare_events(query, query.lower(), csuite, state)


class CompareStub(StubCSuite):
    """Adds event-detail responses keyed by event_date_id."""

    def __init__(self, events, details=None):
        super().__init__(events=events)
        self._details = details or {}
        self.detail_calls = []

    def get_event_date(self, event_date_id):
        self.detail_calls.append(event_date_id)
        detail = self._details.get(event_date_id, {})
        return {"success": True, "data": detail}


def _detail(name, date, emails):
    return {
        "event_description": name,
        "event_date": date,
        "profiles": [
            {"event_profile_name": e.split("@")[0], "event_profile_email": e}
            for e in emails
        ],
    }


def test_compare_with_one_exact_and_one_ambiguous_side_lists_only_the_ambiguous():
    gala = event("Spring Gala", date="2026-04-11")
    iftar_a = event("Iftar 2025", date="2025-03-20")
    iftar_b = event("Iftar 2026", date="2026-03-20")
    csuite = CompareStub(events=[gala, iftar_a, iftar_b])
    state = {}

    result = _compare(csuite, "compare event Spring Gala vs Iftar", state)

    assert isinstance(result, str)
    # Only the ambiguous side is listed.
    assert "Iftar 2025" in result and "Iftar 2026" in result
    assert "Spring Gala" not in result
    assert "1." in result and "2." in result

    pending = state["pending_event_pick"]
    assert pending["action"] == "compare"
    assert pending["side"] == "prior"
    assert pending["resolved_other"]["event_name"] == "Spring Gala"
    # The exact side never triggered a detail fetch.
    assert csuite.detail_calls == []


def test_picking_the_ambiguous_side_resumes_the_comparison():
    gala = event("Spring Gala", date="2026-04-11")
    iftar_a = event("Iftar 2025", date="2025-03-20")
    iftar_b = event("Iftar 2026", date="2026-03-20")
    details = {
        gala["event_date_id"]: _detail("Spring Gala", "2026-04-11", ["a@x.org"]),
        iftar_a["event_date_id"]: _detail("Iftar 2025", "2025-03-20",
                                          ["a@x.org", "lapsed@x.org"]),
    }
    csuite = CompareStub(events=[gala, iftar_a, iftar_b], details=details)
    state = {}

    _compare(csuite, "compare event Spring Gala vs Iftar", state)
    action, picked, extra = take_pending_event_pick("1", state)

    assert action == "compare"
    assert picked["event_name"] == "Iftar 2025"

    from intents.events import _dispatch_event_action
    result = _dispatch_event_action(
        action, picked, "1", "1", state, None, csuite, extra)

    assert "Event Comparison" in result
    assert "lapsed@x.org" in result
    assert "Attended prior but NOT registered for current: 1" in result


def test_compare_uses_the_exact_tier_not_a_substring():
    """"Gala" must not also drag in "Gala Dinner"."""
    exact = event("Gala", date="2026-04-11")
    other = event("Gala Dinner", date="2026-05-11")
    prior = event("Symposium", date="2025-04-11")
    details = {
        exact["event_date_id"]: _detail("Gala", "2026-04-11", ["a@x.org"]),
        prior["event_date_id"]: _detail("Symposium", "2025-04-11", ["b@x.org"]),
    }
    csuite = CompareStub(events=[exact, other, prior], details=details)

    result = _compare(csuite, "compare event Gala vs Symposium", {})

    assert "Event Comparison" in result
    assert set(csuite.detail_calls) == {
        exact["event_date_id"], prior["event_date_id"]}


def test_compare_strips_a_trailing_date_on_a_side():
    a = event("Iftar", date="2026-03-20")
    b = event("Iftar", date="2025-03-20")
    details = {
        a["event_date_id"]: _detail("Iftar", "2026-03-20", ["a@x.org"]),
        b["event_date_id"]: _detail("Iftar", "2025-03-20", ["a@x.org", "c@x.org"]),
    }
    csuite = CompareStub(events=[a, b], details=details)

    result = _compare(
        csuite, "compare event Iftar — 2026-03-20 vs Iftar — 2025-03-20", {})

    assert "Event Comparison" in result
    assert "c@x.org" in result


def test_compare_orders_sides_so_the_later_event_is_current():
    older = event("Iftar 2025", date="2025-03-20")
    newer = event("Iftar 2026", date="2026-03-20")
    details = {
        older["event_date_id"]: _detail("Iftar 2025", "2025-03-20", ["gone@x.org"]),
        newer["event_date_id"]: _detail("Iftar 2026", "2026-03-20", []),
    }
    csuite = CompareStub(events=[older, newer], details=details)

    # Older named first — the renderer must still treat 2026 as "current".
    result = _compare(csuite, "compare event Iftar 2025 vs Iftar 2026", {})

    assert "Current: **Iftar 2026**" in result
    assert "Prior: **Iftar 2025**" in result
    assert "gone@x.org" in result


def test_one_sided_compare_still_works_across_years():
    older = event("Annual Symposium 2025", date="2025-04-11")
    newer = event("Annual Symposium 2026", date="2026-04-11")
    details = {
        older["event_date_id"]: _detail("Annual Symposium 2025", "2025-04-11",
                                        ["stayed@x.org", "gone@x.org"]),
        newer["event_date_id"]: _detail("Annual Symposium 2026", "2026-04-11",
                                        ["stayed@x.org"]),
    }
    csuite = CompareStub(events=[older, newer], details=details)

    result = _compare(csuite, "who attended the Annual Symposium last year "
                              "but hasn't registered this year", {})

    assert "Event Comparison" in result
    assert "gone@x.org" in result
    assert "stayed@x.org" not in result.split("NOT registered")[1]


def test_one_sided_compare_needs_two_events():
    only = event("Solo Event", date="2026-04-11")
    csuite = CompareStub(events=[only])

    result = _compare(csuite, "compare event Solo Event", {})

    assert "Only found one event" in result
    assert csuite.detail_calls == []


def test_compare_refuses_when_both_sides_are_the_same_event():
    same = event("Iftar", date="2026-03-20")
    csuite = CompareStub(events=[same])

    result = _compare(csuite, "compare event Iftar vs Iftar", {})

    assert "same event" in result.lower()
    assert csuite.detail_calls == []


def test_events_module_has_no_bespoke_matching_left():
    """All event matching must funnel through _match_events."""
    import pathlib

    source = (pathlib.Path(__file__).resolve().parent.parent
              / "intents" / "events.py").read_text()
    # The old hand-rolled comparison loop tested descriptions inline.
    assert 'in (e.get("event_description")' not in source
    assert source.count("def _match_events") == 1
