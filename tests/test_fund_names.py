"""Step 1e: CSuite carries the fund code inside the name.

    "200 Muslim Women Who Care Endowment Fund-(END0026)"

There is no separate code field, so every comparison and every display has to
go through split_fund_name. No network — the CSuite client is a stub that
records which endpoint it was asked for.
"""

import pytest

from intents.queries import (
    _choose_fund,
    _fund_row_code,
    _fund_row_names,
    _gather_fund_context,
    extract_fund_name_phrase,
    gather_context,
    resolve_fund_id,
    split_fund_name,
    take_pending_fund_pick,
)
from intents.reports import _format_fee_type, _report_fees

FULL_RAW = "200 Muslim Women Who Care Endowment Fund-(END0026)"
FULL_CLEAN = "200 Muslim Women Who Care Endowment Fund"


class StubCSuite:
    def __init__(self, search_results=None, fund=None, fee_types=None,
                 funds_by_id=None):
        self._search_results = search_results or []
        self._fund = fund or {}
        # Per-id details, so a test can tell WHICH fund was fetched rather
        # than only that a fetch happened.
        self._funds_by_id = funds_by_id or {}
        self._fee_types = fee_types if fee_types is not None else []
        self.search_calls = []
        self.display_calls = []

    def search_funds(self, term):
        self.search_calls.append(term)
        return {"success": True, "data": {"results": self._search_results}}

    def get_fund(self, fund_id):
        self.display_calls.append(fund_id)
        detail = self._funds_by_id.get(fund_id, self._fund)
        return {"success": True, "data": detail}

    def get_funds(self, limit=20, offset=0):
        return {"success": True, "data": {"results": []}}

    def get_grants_by_fund(self, fund_id, limit=10):
        return {"success": True, "data": {"results": []}}

    def get_fund_fee_types(self):
        return {"success": True, "data": {"results": self._fee_types}}


# ---------------------------------------------------------------------------
# 1. split_fund_name
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    (FULL_RAW, (FULL_CLEAN, "END0026")),
    ("Ali Family Fund (DAF0123)", ("Ali Family Fund", "DAF0123")),
    ("Women's Giving Circle-GC0002", ("Women's Giving Circle", "GC0002")),
    ("Tanvir Family Fund - (END0099)", ("Tanvir Family Fund", "END0099")),
    ("lowercase fund-(gc0002)", ("lowercase fund", "GC0002")),
    ("Plain Fund With No Code", ("Plain Fund With No Code", None)),
    ("Fund-(notacode)", ("Fund-(notacode)", None)),
    ("", ("", None)),
    (None, ("", None)),
])
def test_split_fund_name(raw, expected):
    assert split_fund_name(raw) == expected


def test_split_collapses_whitespace_so_spacing_never_breaks_a_match():
    clean, code = split_fund_name("  Ali   Family    Fund-(DAF0123) ")
    assert clean == "Ali Family Fund"
    assert code == "DAF0123"


def test_row_helpers_read_the_code_out_of_the_name():
    row = {"id": 1, "name": FULL_RAW}
    assert _fund_row_names(row) == [FULL_CLEAN]
    assert _fund_row_code(row) == "END0026"


def test_row_code_falls_back_to_short_name():
    row = {"id": 1, "name": "No Code Here", "short_name": "gc0002"}
    assert _fund_row_code(row) == "GC0002"


# ---------------------------------------------------------------------------
# The name phrase survives numbers and stop words
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("query,expected", [
    (f"fund balance for {FULL_CLEAN}", FULL_CLEAN),
    (f"what is the balance of the Tanvir Family Fund", "Tanvir Family Fund"),
    ("how much is in the Ali Family Fund", "Ali Family Fund"),
    ("fees for END0026", "END0026"),
    ("balance for END0026", "END0026"),
])
def test_fund_name_phrase_keeps_the_whole_name(query, expected):
    assert extract_fund_name_phrase(query) == expected


def test_phrase_keeps_a_leading_number_and_an_embedded_stop_word():
    """"200 ... Who ..." — the old extractor dropped both."""
    phrase = extract_fund_name_phrase(f"fund balance for {FULL_CLEAN}")
    assert phrase.startswith("200")
    assert "Who Care" in phrase


# ---------------------------------------------------------------------------
# The headline case: full name resolves with no follow-up
# ---------------------------------------------------------------------------

def test_full_name_query_resolves_to_one_fund_and_fetches_the_balance():
    csuite = StubCSuite(
        search_results=[
            {"id": 111, "name": "200 Muslim Women Who Care Fund-(GC0002)"},
            {"id": 222, "name": FULL_RAW},
        ],
        fund={"funit_id": 222, "fund_name": FULL_RAW, "fgroup_id": 1008,
              "current_fundbalance": "48250.00"},
    )
    workflow_state = {}

    parts = _gather_fund_context(
        f"fund balance for {FULL_CLEAN}",
        f"fund balance for {FULL_CLEAN}".lower(),
        csuite, workflow_state)

    assert csuite.display_calls == [222], "must resolve without asking"
    assert "pending_fund_pick" not in workflow_state
    joined = "\n".join(parts)
    assert FULL_CLEAN in joined
    assert "END0026" in joined
    assert "$48,250.00" in joined
    assert "Endowment" in joined


def test_pasting_the_raw_name_with_its_code_also_resolves():
    csuite = StubCSuite(
        search_results=[{"id": 111, "name": "Other Fund-(GC0002)"},
                        {"id": 222, "name": FULL_RAW}],
        fund={"funit_id": 222, "fund_name": FULL_RAW,
              "current_fundbalance": "1.00"})

    _gather_fund_context(f"balance for {FULL_RAW}",
                         f"balance for {FULL_RAW}".lower(), csuite, {})

    assert csuite.display_calls == [222]


def test_code_matches_against_the_parsed_suffix():
    rows = [{"id": 111, "name": "Something Else Fund-(GC0002)"},
            {"id": 222, "name": FULL_RAW}]
    assert _choose_fund(rows, "END0026")["id"] == 222


def test_code_query_searches_and_resolves():
    csuite = StubCSuite(
        search_results=[{"id": 111, "name": "Other-(GC0002)"},
                        {"id": 222, "name": FULL_RAW}],
        fund={"funit_id": 222, "fund_name": FULL_RAW,
              "current_fundbalance": "5.00"})

    _gather_fund_context("fund balance for END0026",
                         "fund balance for end0026", csuite, {})

    assert csuite.search_calls == ["END0026"]
    assert csuite.display_calls == [222]


def test_display_shows_the_clean_name_not_the_raw_suffix():
    csuite = StubCSuite(fund={"funit_id": 1, "fund_name": FULL_RAW,
                              "current_fundbalance": "10.00"})
    joined = "\n".join(_gather_fund_context("fund 1", "fund 1", csuite, {}))

    assert f"Fund name: {FULL_CLEAN}" in joined
    assert "Fund code: END0026" in joined
    assert "-(END0026)" not in joined


# ---------------------------------------------------------------------------
# 2. Fund pick by number
# ---------------------------------------------------------------------------

def _two_fund_stub():
    return StubCSuite(
        search_results=[
            {"id": 111, "name": "Ramadan Relief Fund-(DAF0101)"},
            {"id": 222, "name": "Ramadan Iftar Fund-(DAF0102)"},
        ],
        funds_by_id={
            111: {"funit_id": 111, "fund_name": "Ramadan Relief Fund-(DAF0101)",
                  "current_fundbalance": "1200.00"},
            222: {"funit_id": 222, "fund_name": "Ramadan Iftar Fund-(DAF0102)",
                  "current_fundbalance": "7500.00"},
        },
    )


def test_two_candidates_are_numbered_and_remembered():
    csuite = _two_fund_stub()
    workflow_state = {}

    parts = _gather_fund_context("balance for Ramadan", "balance for ramadan",
                                 csuite, workflow_state)

    assert csuite.display_calls == [], "no guessing between two funds"
    joined = "\n".join(parts)
    assert "1. Ramadan Relief Fund" in joined
    assert "2. Ramadan Iftar Fund" in joined
    assert "DAF0101" in joined and "DAF0102" in joined

    pending = workflow_state["pending_fund_pick"]
    assert len(pending["funds"]) == 2


def test_a_bare_digit_picks_the_fund_and_fetches_the_balance():
    csuite = _two_fund_stub()
    workflow_state = {}
    _gather_fund_context("balance for Ramadan", "balance for ramadan",
                         csuite, workflow_state)

    parts = _gather_fund_context("2", "2", csuite, workflow_state)

    assert csuite.display_calls == [222]
    assert "$7,500.00" in "\n".join(parts)
    assert "pending_fund_pick" not in workflow_state


def test_gather_context_routes_a_bare_digit_with_no_keywords():
    """"1" has no fund keyword, so keyword dispatch alone would miss it."""
    csuite = _two_fund_stub()
    workflow_state = {}
    gather_context("balance for Ramadan", None, csuite, workflow_state)
    assert "pending_fund_pick" in workflow_state

    context = gather_context("1", None, csuite, workflow_state)

    assert csuite.display_calls == [111]
    assert "Ramadan Relief Fund" in context


def test_a_non_digit_message_clears_the_pending_fund_pick():
    csuite = _two_fund_stub()
    workflow_state = {"pending_fund_pick": {"term": "x", "funds": [{"id": 1}]}}

    gather_context("who is Ahmed", None, csuite, workflow_state)

    assert "pending_fund_pick" not in workflow_state


def test_out_of_range_digit_clears_without_picking():
    workflow_state = {"pending_fund_pick": {"term": "x", "funds": [{"id": 1}]}}
    assert take_pending_fund_pick("8", workflow_state) is None
    assert "pending_fund_pick" not in workflow_state


def test_more_than_nine_candidates_are_not_listed():
    rows = [{"id": i, "name": f"Community Fund {i}-(DAF{i:04d})"}
            for i in range(1, 13)]
    csuite = StubCSuite(search_results=rows)
    workflow_state = {}

    parts = _gather_fund_context("balance for Community", "balance for community",
                                 csuite, workflow_state)

    joined = "\n".join(parts)
    assert "12" in joined
    assert "1. Community Fund 1" not in joined
    assert "pending_fund_pick" not in workflow_state


def test_pick_is_inert_without_a_workflow_state():
    """The gatherer still works when no state is threaded through."""
    csuite = _two_fund_stub()
    parts = _gather_fund_context("balance for Ramadan", "balance for ramadan",
                                 csuite, None)
    assert csuite.display_calls == []
    assert "Ramadan Relief Fund" in "\n".join(parts)


# ---------------------------------------------------------------------------
# resolve_fund_id is shared with reports.py
# ---------------------------------------------------------------------------

def test_resolve_fund_id_returns_an_id_for_an_exact_name():
    csuite = StubCSuite(search_results=[{"id": 222, "name": FULL_RAW}])
    fund_id, rows, error = resolve_fund_id(csuite, f"balance for {FULL_CLEAN}")
    assert (fund_id, error) == (222, None)


def test_resolve_fund_id_returns_rows_when_ambiguous():
    csuite = _two_fund_stub()
    fund_id, rows, error = resolve_fund_id(csuite, "balance for Ramadan")
    assert fund_id is None and error is None and len(rows) == 2


def test_resolve_fund_id_skips_the_search_for_a_numeric_id():
    csuite = StubCSuite()
    assert resolve_fund_id(csuite, "fund 1046")[0] == 1046
    assert csuite.search_calls == []


# ---------------------------------------------------------------------------
# 5. Fee report reads the real funit/feetype fields
# ---------------------------------------------------------------------------

REAL_FEE_TYPES = [
    {"fund_fee_type_id": 1000, "admin_fee_type_name": "Standard DAF",
     "admin_fee_type_type": "percent_range", "admin_fee_percent": "1.0",
     "admin_fee_min_fee": "250.00", "admin_fee_amount": None,
     "admin_fee_max_fee": None},
    {"fund_fee_type_id": 1001, "admin_fee_type_name": "Endowment",
     "admin_fee_type_type": "percent_range", "admin_fee_percent": "0.5",
     "admin_fee_min_fee": None, "admin_fee_amount": None,
     "admin_fee_max_fee": None},
]


def test_fee_lines_show_real_names_and_percentages():
    line = _format_fee_type(REAL_FEE_TYPES[0])
    assert "Standard DAF" in line
    assert "1%" in line
    assert "min $250.00" in line
    assert "Unknown" not in line and "?%" not in line


def test_fee_line_survives_a_type_with_no_rate():
    line = _format_fee_type({"admin_fee_type_name": "Placeholder"})
    assert "Placeholder" in line
    assert "no rate published" in line


def test_fee_report_lists_types_and_states_the_join_is_unknown():
    csuite = StubCSuite(fee_types=REAL_FEE_TYPES)
    out = _report_fees("what are our fees", csuite)

    assert "Standard DAF" in out and "Endowment" in out
    assert "1%" in out and "0.5%" in out
    assert "not exposed by CSuite" in out
    assert "Shazeen" in out


def test_fee_report_shows_the_balance_of_a_named_fund():
    csuite = StubCSuite(
        fee_types=REAL_FEE_TYPES,
        search_results=[{"id": 222, "name": FULL_RAW}],
        fund={"funit_id": 222, "fund_name": FULL_RAW,
              "current_fundbalance": "48250.00"})

    out = _report_fees(f"fees for {FULL_CLEAN}", csuite)

    assert "$48,250.00" in out
    assert FULL_CLEAN in out
    assert "END0026" in out
    assert "Standard DAF" in out


def test_fee_report_never_asserts_a_per_fund_fee():
    """No estimate is printed, because the join does not exist."""
    csuite = StubCSuite(
        fee_types=REAL_FEE_TYPES,
        search_results=[{"id": 222, "name": FULL_RAW}],
        fund={"funit_id": 222, "fund_name": FULL_RAW,
              "current_fundbalance": "48250.00"})

    out = _report_fees(f"fees for {FULL_CLEAN}", csuite).lower()

    assert "estimated quarterly fee" not in out
    assert "annualised" not in out


def test_the_invented_fund_to_fee_join_is_gone():
    import intents.reports as reports
    assert not hasattr(reports, "_calculate_fee")
