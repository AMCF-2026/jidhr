"""Step 3c: reports read the mirror, and never CSuite.

No network and no database. clients.database.execute_query is replaced by
MirrorDB, which holds mirror rows in memory and interprets the WHERE
clauses the read layer actually generates — so a report that changes its
SQL is caught here rather than in production.

Every fixture is invented. No live donor data is used as a test fixture.
"""

import re
from datetime import datetime, timedelta

import pytest

from clients import mirror_read
from config import Config
from intents import donor_prep, queries, reports


# ---------------------------------------------------------------------------
# A mirror that can be queried
# ---------------------------------------------------------------------------

# The shapes clients/mirror_read.py builds: `AND csuite_id = %s` and
# `AND data->>'field' <op> %s`. Parsed rather than pattern-matched whole, so
# a new filter in a report either works here or fails loudly.
_JSON_FILTER = re.compile(r"data->>'([a-z_]+)'\s*(>=|<=|=)\s*%s")
_ID_FILTER = re.compile(r"csuite_id\s*=\s*%s")

NOW = datetime(2026, 9, 10, 12, 0, 0)


class MirrorDB:
    """Stands in for clients.database.execute_query over csuite_mirror.

    Construct with {record_type: [row dicts]}. A row may carry the two
    column values under the reserved keys `_id` and `_group`; everything
    else is the jsonb payload.
    """

    def __init__(self, tables=None, synced=None):
        self.tables = {}
        self.synced = dict(synced or {})
        self.queries = []
        for record_type, rows in (tables or {}).items():
            self.tables[record_type] = [self._row(r) for r in rows]

    @staticmethod
    def _row(raw):
        data = {k: v for k, v in raw.items() if k not in ("_id", "_group")}
        csuite_id = raw.get("_id")
        if csuite_id is None:
            for key in ("funit_id", "grant_id", "profile_id", "check_id",
                        "event_date_id", "fund_fee_type_id"):
                if raw.get(key) is not None:
                    csuite_id = raw[key]
                    break
        return {
            "csuite_id": str(csuite_id) if csuite_id is not None else None,
            "fund_group_id": raw.get("_group"),
            "data": data,
        }

    def stamp(self, record_type):
        return self.synced.get(record_type, NOW)

    def __call__(self, sql, params=None, fetch=True):
        collapsed = " ".join(str(sql).split())
        params = list(params or ())
        self.queries.append((collapsed, tuple(params)))

        record_type = params[0]
        rows = self.tables.get(record_type, [])

        if collapsed.startswith("SELECT COUNT(*)"):
            return [{"n": len(rows)}]

        if collapsed.startswith("SELECT MAX(synced_at)"):
            return [{"synced_at": self.stamp(record_type) if rows else None}]

        assert collapsed.startswith("SELECT csuite_id, fund_group_id, data"), \
            f"unrecognised mirror query: {collapsed}"

        remaining = params[1:]
        where = collapsed.split("WHERE", 1)[1]

        for clause in where.split("AND")[1:]:
            clause = clause.strip()
            value = remaining.pop(0)
            if _ID_FILTER.search(clause):
                rows = [r for r in rows if r["csuite_id"] == str(value)]
                continue
            match = _JSON_FILTER.search(clause)
            assert match, f"unhandled WHERE clause: {clause!r}"
            field, op = match.group(1), match.group(2)
            rows = [r for r in rows
                    if _compare(r["data"].get(field), op, value)]

        return [dict(r, synced_at=self.stamp(record_type)) for r in rows]


def _compare(stored, op, wanted) -> bool:
    """jsonb ->> yields text, so every comparison here is a text one."""
    if stored is None:
        return False
    left, right = str(stored), str(wanted)
    if op == "=":
        return left == right
    if op == ">=":
        return left >= right
    return left <= right


class ExplodingClient:
    """Any attribute access is a live API call that must not happen."""

    def __getattr__(self, name):
        raise AssertionError(
            f"a mirror-backed report called CSuite: {name}()")


@pytest.fixture
def mirror(monkeypatch):
    """Install a MirrorDB; tests fill it via `mirror.load(...)`."""
    db = MirrorDB()

    def load(tables, synced=None):
        db.tables = {rt: [MirrorDB._row(r) for r in rows]
                     for rt, rows in tables.items()}
        db.synced = dict(synced or {})
        return db

    db.load = load
    monkeypatch.setattr("clients.database.execute_query", db)
    return db


# ---------------------------------------------------------------------------
# Fixtures — all invented
# ---------------------------------------------------------------------------

DAF = Config.FUND_GROUP_DAF          # 1002
ENDOW = Config.FUND_GROUP_ENDOWMENT  # 1008

RECENT = (NOW - timedelta(days=30)).strftime("%Y-%m-%d")
OLD = (NOW - timedelta(days=800)).strftime("%Y-%m-%d")

FUNDS = [
    {"_id": 1000, "_group": DAF, "funit_id": 1000,
     "fund_name": "Alpha Family Fund-(DAF0001)",
     "current_fundbalance": "50000.00", "fgroup_id": DAF},
    {"_id": 1001, "_group": DAF, "funit_id": 1001,
     "fund_name": "Beta Family Fund-(DAF0002)",
     "current_fundbalance": "1200.00", "fgroup_id": DAF},
    {"_id": 1002, "_group": ENDOW, "funit_id": 1002,
     "fund_name": "Gamma Endowment Fund-(END0003)",
     "current_fundbalance": "250000.00", "fgroup_id": ENDOW,
     "dist_start_date": "2026-01-01",
     "distribution_interval": "annual"},
    {"_id": 1003, "_group": ENDOW, "funit_id": 1003,
     "fund_name": "Delta Endowment Fund-(END0004)",
     "current_fundbalance": "9000.00", "fgroup_id": ENDOW,
     "fund_closed": True},
]

GRANTS = [
    # Alpha got a grant last month — not dormant.
    {"_id": 9001, "grant_id": 9001, "funit_id": 1000,
     "fund_name": "Alpha Family Fund-(DAF0001)", "grant_amount": "500.00",
     "grant_date": RECENT, "grant_status": "paid", "name": "Helping Hands"},
    # Beta's only grant is two years old — dormant.
    {"_id": 9002, "grant_id": 9002, "funit_id": 1001,
     "fund_name": "Beta Family Fund-(DAF0002)", "grant_amount": "250.00",
     "grant_date": OLD, "grant_status": "complete", "name": "Helping Hands"},
    {"_id": 9003, "grant_id": 9003, "funit_id": 1000,
     "fund_name": "Alpha Family Fund-(DAF0001)", "grant_amount": "125.00",
     "grant_date": OLD, "grant_status": "paid", "name": "Books For All"},
]

PROFILES = [
    {"_id": 7001, "profile_id": 7001, "ptype": "indiv",
     "name": "Testcase, Aisha", "primary_email": "aisha@example.invalid",
     "dead": 0},
    {"_id": 7002, "profile_id": 7002, "ptype": "indiv",
     "name": "Sample, Bilal", "primary_email": "bilal@example.invalid",
     "dead": 1},
    {"_id": 7003, "profile_id": 7003, "ptype": "org",
     "name": "Example Charity Inc", "primary_email": "info@example.invalid",
     "dead": 0},
    {"_id": 7004, "profile_id": 7004, "ptype": "indiv",
     "name": "Fixture, Dawud", "primary_email": "dawud@example.invalid",
     "dead": 0},
]

THIS_YEAR = NOW.year        # 2026
LAST_YEAR = THIS_YEAR - 1   # 2025

AGGS = [
    # Lapsed: gave last Ramadan, not this one. Living individual.
    {"_id": 7001, "profile_id": "7001", "lifetime_total": "2675.50",
     "count": 3, "ramadan_years": [LAST_YEAR],
     "first_date": "2023-01-15", "first_amount": "100.00",
     "first_fund": "Alpha Family Fund-(DAF0001)",
     "latest_date": "2025-03-09", "latest_amount": "75.50",
     "latest_fund": "Alpha Family Fund-(DAF0001)",
     "greatest_amount": "2500.00", "greatest_date": "2024-06-01"},
    # Lapsed by the year test, but deceased.
    {"_id": 7002, "profile_id": "7002", "lifetime_total": "40.00",
     "count": 1, "ramadan_years": [LAST_YEAR]},
    # Lapsed by the year test, but an organisation.
    {"_id": 7003, "profile_id": "7003", "lifetime_total": "5000.00",
     "count": 2, "ramadan_years": [LAST_YEAR]},
    # Gave in both — not lapsed.
    {"_id": 7004, "profile_id": "7004", "lifetime_total": "900.00",
     "count": 4, "ramadan_years": [LAST_YEAR, THIS_YEAR]},
]

QUARTER = (NOW.month - 1) // 3 + 1  # Q3 2026

FUND_QUARTERS = [
    {"_id": f"1000:{THIS_YEAR}Q{QUARTER}", "funit_id": "1000",
     "fund_name": "Alpha Family Fund-(DAF0001)", "year": THIS_YEAR,
     "quarter": QUARTER, "total": "10000.00", "count": 12},
    {"_id": f"1002:{THIS_YEAR}Q{QUARTER}", "funit_id": "1002",
     "fund_name": "Gamma Endowment Fund-(END0003)", "year": THIS_YEAR,
     "quarter": QUARTER, "total": "4000.00", "count": 2},
    # A different quarter — must not be counted.
    {"_id": f"1000:{THIS_YEAR - 1}Q1", "funit_id": "1000",
     "fund_name": "Alpha Family Fund-(DAF0001)", "year": THIS_YEAR - 1,
     "quarter": 1, "total": "999999.00", "count": 99},
]

FEE_TYPES = [
    {"_id": 1000, "fund_fee_type_id": 1000,
     "admin_fee_type_name": "Standard DAF", "admin_fee_type_type":
     "percent_range", "admin_fee_percent": "1.0",
     "admin_fee_min_fee": "250.00"},
]

FULL = {
    "fund": FUNDS,
    "grant": GRANTS,
    "profile": PROFILES,
    "donation_agg": AGGS,
    "donation_fund_quarter": FUND_QUARTERS,
    "fee_type": FEE_TYPES,
}


@pytest.fixture
def loaded(mirror):
    mirror.load(FULL)
    return mirror


@pytest.fixture(autouse=True)
def frozen_now(monkeypatch):
    """Pin 'now' so Ramadan years and 12-month cutoffs are deterministic."""
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW

    monkeypatch.setattr(reports, "datetime", FixedDatetime)


# ---------------------------------------------------------------------------
# The rule: an empty mirror stops the report
# ---------------------------------------------------------------------------

MIRROR_BACKED = [
    ("grant report this quarter", "grant"),
    ("ramadan lapsed donors", "donation_agg"),
    ("dormant funds", "fund"),
    ("admin fees", "fee_type"),
    ("uncashed checks", "grant"),
    ("quarterly summary", "donation_fund_quarter"),
    ("endowment distribution schedule", "fund"),
]


@pytest.fixture
def ctx():
    class Services:
        hubspot = ExplodingClient()
        csuite = ExplodingClient()
        claude = ExplodingClient()

    class Ctx:
        services = Services()

    return Ctx()


@pytest.mark.parametrize("query, needed", MIRROR_BACKED)
def test_an_empty_mirror_says_so_and_stops(mirror, ctx, query, needed):
    mirror.load({})

    out = reports.handle(query, ctx)

    assert out == (f"⚠️ CSuite mirror not loaded for {needed} — "
                   "run mirror_refresh.")


@pytest.mark.parametrize("query, needed", MIRROR_BACKED)
def test_no_mirror_backed_report_calls_csuite(loaded, ctx, query, needed):
    """ctx.services.csuite raises on any attribute access."""
    out = reports.handle(query, ctx)

    assert out and not out.startswith("❌")
    assert "CSuite mirror" in out, "every one of these ends with as_of_line"


@pytest.mark.parametrize("query, needed", MIRROR_BACKED)
def test_every_mirror_backed_report_ends_with_provenance(loaded, ctx,
                                                         query, needed):
    out = reports.handle(query, ctx)
    assert out.rstrip().endswith("(CSuite mirror)")
    assert "📅 Data as of" in out


def test_the_empty_check_costs_one_count_and_returns(mirror, ctx):
    mirror.load({})
    reports.handle("dormant funds", ctx)

    assert all(sql.startswith("SELECT COUNT(*)") for sql, _ in mirror.queries)


# ---------------------------------------------------------------------------
# Dormant funds
# ---------------------------------------------------------------------------

def test_dormant_excludes_funds_with_a_recent_grant(loaded, ctx):
    out = reports.handle("dormant funds", ctx)

    assert "Alpha Family Fund" not in out, "Alpha had a grant last month"
    assert "Beta Family Fund" in out, "Beta's only grant is two years old"
    assert "Gamma Endowment Fund" in out, "Gamma has never had a grant"
    assert "Delta Endowment Fund" in out


def test_dormant_counts_against_all_funds_not_a_page(loaded, ctx):
    out = reports.handle("dormant funds", ctx)
    assert "**3** of 4 funds" in out


def test_dormant_shows_the_fund_group_label(loaded, ctx):
    out = reports.handle("dormant funds", ctx)
    assert "**DAF** (1)" in out
    assert "**Endowment** (2)" in out


def test_dormant_reports_never_granted_funds_as_never(loaded, ctx):
    out = reports.handle("dormant funds", ctx)
    assert "last grant: Never" in out
    assert f"last grant: {OLD}" in out


def test_dormant_when_everything_is_active(mirror, ctx):
    mirror.load({
        "fund": [FUNDS[0]],
        "grant": [GRANTS[0]],
    })
    out = reports.handle("dormant funds", ctx)
    assert "All 1 funds have had grant activity" in out
    assert "(CSuite mirror)" in out


# ---------------------------------------------------------------------------
# Grants issued but not cleared (was: uncashed checks)
# ---------------------------------------------------------------------------

def test_uncleared_lists_paid_grants_only(loaded, ctx):
    out = reports.handle("uncashed checks", ctx)

    assert "status 'paid'" in out
    assert "Books For All" in out       # grant 9003, paid
    assert "**2** grants" in out        # 9001 and 9003
    # Grant 9002 is 'complete' — it has cleared.
    assert "$250.00" not in out


def test_uncleared_groups_by_grantee_oldest_first(loaded, ctx):
    out = reports.handle("uncashed checks", ctx)

    books = out.index("Books For All")
    hands = out.index("Helping Hands")
    assert books < hands, "the older outstanding grantee comes first"


def test_uncleared_totals_only_the_paid_grants(loaded, ctx):
    out = reports.handle("uncashed checks", ctx)
    assert "$625.00" in out  # 500.00 + 125.00, not the 250.00 complete one


def test_uncleared_says_it_is_grant_status_not_bank_clearance(loaded, ctx):
    out = reports.handle("uncashed checks", ctx)
    assert "no link from a grant to the check that paid it" in out


def test_uncleared_when_nothing_is_outstanding(mirror, ctx):
    mirror.load({"grant": [GRANTS[1]]})  # the 'complete' one only
    out = reports.handle("uncashed checks", ctx)
    assert "No grants are sitting at status 'paid'" in out


# ---------------------------------------------------------------------------
# Ramadan lapsed donors
# ---------------------------------------------------------------------------

def test_ramadan_lapsed_finds_the_lapsed_individual(loaded, ctx):
    out = reports.handle("ramadan lapsed", ctx)

    assert "Testcase, Aisha" in out
    assert f"Ramadan {LAST_YEAR}" in out
    assert f"Ramadan {THIS_YEAR}" in out


def test_ramadan_lapsed_excludes_the_deceased(loaded, ctx):
    out = reports.handle("ramadan lapsed", ctx)
    assert "Sample, Bilal" not in out


def test_ramadan_lapsed_excludes_organisations(loaded, ctx):
    out = reports.handle("ramadan lapsed", ctx)
    assert "Example Charity" not in out


def test_ramadan_lapsed_excludes_donors_who_gave_again(loaded, ctx):
    out = reports.handle("ramadan lapsed", ctx)
    assert "Fixture, Dawud" not in out


def test_ramadan_lapsed_states_the_total(loaded, ctx):
    out = reports.handle("ramadan lapsed", ctx)
    assert "**1** individual donors gave during Ramadan" in out
    assert "showing 1 of 1" in out


def test_ramadan_lapsed_says_the_total_when_the_list_is_capped(mirror, ctx):
    aggs = []
    profiles = []
    for i in range(40):
        pid = 8000 + i
        aggs.append({"_id": pid, "profile_id": str(pid),
                     "lifetime_total": "10.00", "count": 1,
                     "ramadan_years": [LAST_YEAR]})
        profiles.append({"_id": pid, "profile_id": pid, "ptype": "indiv",
                         "name": f"Donor {i:02d}", "dead": 0})
    mirror.load({"donation_agg": aggs, "profile": profiles})

    out = reports.handle("ramadan lapsed", ctx)

    assert "**40** individual donors" in out
    assert "showing 25 of 40" in out
    assert "and 15 more (40 in total)" in out


def test_ramadan_lapsed_when_nobody_lapsed(mirror, ctx):
    mirror.load({"donation_agg": [AGGS[3]], "profile": [PROFILES[3]]})
    out = reports.handle("ramadan lapsed", ctx)
    assert "No lapsed Ramadan donors" in out


def test_outreachable_rules_in_isolation():
    assert reports._is_outreachable({"ptype": "indiv", "dead": 0}) is True
    assert reports._is_outreachable({"ptype": "indiv", "dead": 1}) is False
    assert reports._is_outreachable({"ptype": "org", "dead": 0}) is False
    assert reports._is_outreachable({}) is False
    assert reports._is_outreachable(None) is False


# ---------------------------------------------------------------------------
# Quarterly summary
# ---------------------------------------------------------------------------

def test_quarterly_uses_only_the_requested_quarter(loaded, ctx):
    out = reports.handle("quarterly summary", ctx)

    assert "$999,999.00" not in out, "last year's quarter must not be counted"
    assert "$14,000.00" in out  # 10000 + 4000


def test_quarterly_splits_daf_from_endowment(loaded, ctx):
    out = reports.handle("quarterly summary", ctx)

    assert "**DAF** (1 funds): In $10,000.00" in out
    assert "**Endowment** (1 funds): In $4,000.00" in out


def test_quarterly_counts_grants_out_in_the_window(loaded, ctx):
    out = reports.handle("quarterly summary", ctx)
    # Only GRANTS[0] falls inside this quarter.
    assert "Total Grants Out:** $500.00" in out
    assert "Net:** $13,500.00" in out


def test_quarterly_tags_each_fund_with_its_group(loaded, ctx):
    out = reports.handle("quarterly summary", ctx)
    assert "[DAF]" in out
    assert "[Endowment]" in out


# ---------------------------------------------------------------------------
# Fee report
# ---------------------------------------------------------------------------

def test_fee_report_lists_mirrored_fee_types(loaded, ctx):
    out = reports.handle("admin fees", ctx)

    assert "Standard DAF" in out
    assert "1%" in out
    assert "min $250.00" in out


def test_fee_report_keeps_the_shazeen_disclaimer(loaded, ctx):
    out = reports.handle("admin fees", ctx)
    assert "confirming with Shazeen" in out


def test_fee_report_shows_a_named_funds_balance_from_the_mirror(loaded, ctx):
    out = reports.handle("admin fees for DAF0001", ctx)

    assert "$50,000.00" in out
    assert "Alpha Family Fund" in out


def test_fee_report_never_computes_a_per_fund_fee(loaded, ctx):
    out = reports.handle("admin fees for DAF0001", ctx)

    assert "estimated fee" not in out.lower()
    assert "$500.00" not in out, "1% of 50,000 must never be asserted"


def test_fee_report_refuses_to_pick_between_matching_funds(loaded, ctx):
    out = reports.handle("admin fees for Family Fund", ctx)
    assert "matched 2 funds" in out
    assert "$50,000.00" not in out, "never pick one of two matches"


def test_fee_report_resolves_a_fund_by_name(loaded, ctx):
    """handle() must pass the ORIGINAL query here: the name extractor keys
    on capitalisation, so the lowercased one only ever matched codes."""
    out = reports.handle("admin fees for Alpha Family Fund", ctx)

    assert "Alpha Family Fund" in out
    assert "$50,000.00" in out


# ---------------------------------------------------------------------------
# Endowment distributions
# ---------------------------------------------------------------------------

def test_endowment_report_covers_every_endowment(loaded, ctx):
    out = reports.handle("endowment distribution schedule", ctx)

    assert "Gamma Endowment Fund" in out
    assert "Alpha Family Fund" not in out, "DAFs are not endowments"


def test_endowment_report_excludes_closed_funds(loaded, ctx):
    out = reports.handle("endowment distribution schedule", ctx)
    assert "Delta Endowment Fund" not in out


def test_endowment_report_shows_the_distribution_fields(loaded, ctx):
    out = reports.handle("endowment distribution schedule", ctx)
    assert "annual" in out
    assert "2026-01-01" in out


def test_endowment_report_has_no_partial_disclaimer(loaded, ctx):
    out = reports.handle("endowment distribution schedule", ctx)
    assert "may not be shown" not in out
    assert "Partial data" not in out


def test_endowment_report_says_when_nothing_is_configured(mirror, ctx):
    bare = dict(FUNDS[2])
    bare.pop("dist_start_date")
    bare.pop("distribution_interval")
    mirror.load({"fund": [bare]})

    out = reports.handle("endowment distribution schedule", ctx)

    assert "Not configured" in out
    assert "no endowment fund has a distribution schedule set" in out


# ---------------------------------------------------------------------------
# Grant report
# ---------------------------------------------------------------------------

def test_grant_report_filters_to_the_window(loaded, ctx):
    out = reports.handle("grant report this quarter", ctx)

    assert "1 grants totalling **$500.00**" in out
    assert "$125.00" not in out, "the two-year-old grant is out of window"


def test_grant_report_when_the_window_is_empty(mirror, ctx):
    mirror.load({"grant": [GRANTS[1]]})  # the old one only
    out = reports.handle("grant report this quarter", ctx)
    assert "No grants found" in out
    assert "(CSuite mirror)" in out


# ---------------------------------------------------------------------------
# as_of_line
# ---------------------------------------------------------------------------

def test_as_of_line_uses_the_oldest_input(mirror):
    fresh = datetime(2026, 9, 10, 9, 0)
    stale = datetime(2026, 9, 3, 6, 30)
    mirror.load(FULL, synced={"fund": fresh, "grant": stale})

    assert mirror_read.as_of_line("fund") == \
        "📅 Data as of 2026-09-10 09:00 UTC (CSuite mirror)"
    assert mirror_read.as_of_line("fund", "grant") == \
        "📅 Data as of 2026-09-03 06:30 UTC (CSuite mirror)"
    assert mirror_read.as_of_line("grant", "fund") == \
        "📅 Data as of 2026-09-03 06:30 UTC (CSuite mirror)"


def test_as_of_line_with_nothing_loaded(mirror):
    mirror.load({})
    assert mirror_read.as_of_line("fund") == \
        "📅 Data as of unknown (CSuite mirror)"


def test_dormant_report_stamps_the_older_of_its_two_inputs(mirror, ctx):
    mirror.load(FULL, synced={"fund": datetime(2026, 9, 10, 9, 0),
                              "grant": datetime(2026, 9, 1, 8, 0)})
    out = reports.handle("dormant funds", ctx)
    assert "2026-09-01 08:00 UTC" in out


# ---------------------------------------------------------------------------
# mirror_read primitives
# ---------------------------------------------------------------------------

def test_rows_merges_columns_over_the_payload(loaded):
    rows = mirror_read.rows("fund")
    alpha = next(r for r in rows if r["csuite_id"] == "1000")

    assert alpha["fund_group_id"] == DAF
    assert alpha["fund_name"] == "Alpha Family Fund-(DAF0001)"
    assert alpha["synced_at"] == NOW


def test_get_returns_one_row_or_none(loaded):
    assert mirror_read.get("fund", 1000)["csuite_id"] == "1000"
    assert mirror_read.get("fund", "1000")["csuite_id"] == "1000"
    assert mirror_read.get("fund", 999999) is None
    assert mirror_read.get("fund", None) is None


def test_count_and_freshness(loaded):
    assert mirror_read.count("fund") == 4
    assert mirror_read.count("nothing_here") == 0
    assert mirror_read.freshness("fund") == NOW
    assert mirror_read.freshness("nothing_here") is None


def test_require_names_the_first_missing_type(mirror):
    mirror.load({"fund": FUNDS})
    assert mirror_read.require("fund") is None
    assert mirror_read.require("fund", "grant") == \
        "⚠️ CSuite mirror not loaded for grant — run mirror_refresh."


# ---------------------------------------------------------------------------
# donor_prep reads the aggregate
# ---------------------------------------------------------------------------

class StubCSuite:
    """Only the calls donor_prep still makes live."""

    def __init__(self, profile_id=7001):
        self.profile_id = profile_id
        self.calls = []

    def search_profiles(self, name):
        self.calls.append(("search_profiles", name))
        return {"success": True, "data": {"results": [
            {"profile_id": self.profile_id, "name": name}]}}

    def get_grants_by_profile(self, profile_id, limit=100, offset=0):
        self.calls.append(("get_grants_by_profile", profile_id))
        return {"success": True, "data": {"results": []}}

    def __getattr__(self, name):
        raise AssertionError(f"donor_prep called csuite.{name}()")


class StubHubSpot:
    def search_contacts(self, name):
        return {"results": []}

    def __getattr__(self, name):
        raise AssertionError(f"unexpected hubspot.{name}()")


def test_donor_prep_reads_lifetime_giving_from_the_aggregate(loaded):
    csuite = StubCSuite(7001)

    data = donor_prep._gather_csuite_data("Aisha", csuite)

    assert data["lifetime_giving"] == 2675.50
    assert data["donation_count"] == 3
    assert data["last_donation"] == "2025-03-09"
    assert data["first_donation"] == "2023-01-15"
    assert data["greatest_donation"] == "2500.00"
    assert data["giving_note"] is None


def test_donor_prep_never_calls_the_broken_donations_method(loaded):
    """get_donations_by_profile takes no `limit`; every call raised
    TypeError, was swallowed, and left lifetime giving at $0.00."""
    csuite = StubCSuite(7001)
    donor_prep._gather_csuite_data("Aisha", csuite)

    assert not any(c[0] == "get_donations_by_profile" for c in csuite.calls)


def test_donor_prep_says_so_when_there_is_no_aggregate(loaded):
    csuite = StubCSuite(999999)

    data = donor_prep._gather_csuite_data("Nobody", csuite)

    assert data["giving_note"] == "No recorded donations in CSuite mirror."
    assert data["lifetime_giving"] == 0


def test_donor_prep_brief_prints_the_note_not_a_zero(loaded):
    cs = dict(donor_prep._gather_csuite_data("Nobody", StubCSuite(999999)))
    hs = {"found": False, "tickets": [], "notes": [], "email": None,
          "last_activity": None, "hubspot_link": None}

    brief = donor_prep._format_brief("Nobody", hs, cs, "• point")

    assert "No recorded donations in CSuite mirror." in brief
    assert "$0.00" not in brief


def test_donor_prep_context_block_carries_the_real_total(loaded):
    cs = donor_prep._gather_csuite_data("Aisha", StubCSuite(7001))
    hs = {"found": False, "tickets": [], "notes": [], "emails": [],
          "engagements": []}

    block = donor_prep._build_context_block("Aisha", hs, cs)

    assert "$2,675.50" in block
    assert "across 3 donations" in block


def test_donor_prep_lists_no_tickets_without_an_association_lookup(loaded):
    """The old code printed five unrelated open tickets as this donor's."""
    class HubSpot(StubHubSpot):
        def search_contacts(self, name):
            return {"results": [{"id": "701", "properties": {}}]}

        def get_contact_notes(self, contact_id, limit=5):
            return {"results": []}

        def get_contact_emails(self, contact_id, limit=5):
            return {"results": []}

        def get_contact_engagements(self, contact_id, limit=5):
            return {"results": []}

        def get_open_tickets(self, limit=10):
            raise AssertionError("global open tickets must not be fetched")

    data = donor_prep._gather_hubspot_data("Aisha", HubSpot())
    assert data["tickets"] == []


# ---------------------------------------------------------------------------
# queries.py — giving context and the live-balance fallback
# ---------------------------------------------------------------------------

def test_profile_giving_context_reads_the_aggregate(loaded):
    parts = queries._profile_giving_context(7001)

    assert len(parts) == 1
    assert "$2,675.50" in parts[0]
    assert "Number of donations: 3" in parts[0]
    assert f"Gave during Ramadan in: {LAST_YEAR}" in parts[0]
    assert "(CSuite mirror)" in parts[0]


def test_profile_giving_context_is_explicit_about_nothing(loaded):
    parts = queries._profile_giving_context(999999)

    assert len(parts) == 1, "an empty list would fall through to any donor"
    assert "no recorded donations" in parts[0].lower()


def test_fund_balance_stays_live(loaded):
    class Live:
        def get_fund(self, fund_id):
            return {"success": True, "data": {
                "funit_id": fund_id,
                "fund_name": "Alpha Family Fund-(DAF0001)",
                "current_fundbalance": "51234.00"}}

    out = queries._fund_detail_context(Live(), 1000)

    assert "$51,234.00" in out, "the live balance, not the mirrored one"
    assert "mirror balance" not in out


def test_a_failed_live_lookup_falls_back_to_the_mirror(loaded):
    class Broken:
        def get_fund(self, fund_id):
            return {"success": False, "error": "CSuite timeout"}

    out = queries._fund_detail_context(Broken(), 1000)

    assert "CSuite timeout" in out
    assert "Live lookup failed; mirror balance as of" in out
    assert "$50,000.00" in out
    assert "do not present it as the current balance" in out


def test_the_fallback_names_the_date_it_was_taken(loaded, mirror):
    mirror.load(FULL, synced={"fund": datetime(2026, 9, 2, 7, 15)})

    class Broken:
        def get_fund(self, fund_id):
            raise RuntimeError("connection reset")

    out = queries._fund_detail_context(Broken(), 1000)
    assert "2026-09-02 07:15 UTC" in out


def test_no_fallback_when_the_mirror_has_no_such_fund(loaded):
    class Broken:
        def get_fund(self, fund_id):
            return {"success": False, "error": "no such fund"}

    out = queries._fund_detail_context(Broken(), 424242)

    assert out == "CSuite fund lookup for id 424242 failed: no such fund"
    assert "mirror balance" not in out


# ---------------------------------------------------------------------------
# 3c-fix: the check gatherer reads the mirror, not check/list
# ---------------------------------------------------------------------------

def test_check_context_lists_uncleared_grants_with_the_grantee(loaded):
    parts = queries._gather_check_context("any uncashed checks?")

    assert len(parts) == 1
    text = parts[0]
    assert "Books For All" in text
    assert "Helping Hands" in text
    assert "Unknown" not in text, "the grantee was never in the old data"
    assert "$625.00" in text
    assert "(CSuite mirror)" in text


def test_check_context_is_paid_only(loaded):
    text = queries._gather_check_context("uncashed")[0]

    assert "2 grants" in text
    assert "$250.00" not in text, "the 'complete' grant has cleared"


def test_check_context_is_oldest_first(loaded):
    text = queries._gather_check_context("uncashed")[0]
    assert text.index("Books For All") < text.index("Helping Hands")


def test_check_context_says_it_is_not_bank_clearance(loaded):
    text = queries._gather_check_context("uncashed")[0]
    assert "no link from a grant to the check that paid it" in text


def test_check_context_makes_no_live_call(loaded):
    """The signature no longer takes a client, so it cannot make one."""
    import inspect

    params = inspect.signature(queries._gather_check_context).parameters
    assert list(params) == ["query_lower"]


def test_check_context_on_an_empty_mirror_refuses_to_guess(mirror):
    mirror.load({})

    parts = queries._gather_check_context("uncashed")

    assert len(parts) == 1
    assert "CSuite mirror not loaded for grant" in parts[0]
    assert "Do not answer from memory" in parts[0]


def test_check_context_when_nothing_is_outstanding(mirror):
    mirror.load({"grant": [GRANTS[1]]})  # the 'complete' one only
    text = queries._gather_check_context("uncashed")[0]

    assert "No grants are at status 'paid'" in text
    assert "(CSuite mirror)" in text


def test_check_context_names_the_total_when_the_list_is_capped(mirror):
    grants = [
        {"_id": 5000 + i, "grant_id": 5000 + i, "funit_id": 1000,
         "fund_name": "Alpha", "grant_amount": "10.00",
         "grant_date": f"2026-01-{i + 1:02d}", "grant_status": "paid",
         "name": f"Grantee {i:02d}"}
        for i in range(20)
    ]
    mirror.load({"grant": grants})

    text = queries._gather_check_context("uncashed")[0]

    assert "20 grants totalling $200.00" in text
    assert "and 5 more (20 in total)" in text


def test_grants_by_fund_names_the_grantee_not_unknown():
    """grant/list has no vendor_name; the grantee is `name`."""
    class Csuite:
        def get_grants_by_fund(self, fund_id, limit=10):
            return {"success": True, "data": {"results": [
                {"grant_amount": "750.00", "name": "Helping Hands",
                 "grant_date": "2026-04-01"}]}}

        def get_fund(self, fund_id):
            return {"success": True, "data": {
                "funit_id": fund_id, "fund_name": "Alpha Fund-(DAF0001)"}}

        def __getattr__(self, name):
            raise AssertionError(f"unexpected csuite.{name}()")

    parts = queries._gather_fund_context(
        "grants for fund 1000", "grants for fund 1000", Csuite())
    text = "\n".join(parts)

    assert "Helping Hands" in text
    assert "$750.00" in text
    assert "to Unknown" not in text


# ---------------------------------------------------------------------------
# 3c-fix: contact-scoped tickets
# ---------------------------------------------------------------------------

TICKETS = {
    "301": {"id": "301", "properties": {
        "subject": "DAF paperwork question", "hs_pipeline_stage": "1",
        "createdate": "2026-08-01T10:00:00Z"}},
    "302": {"id": "302", "properties": {
        "subject": "Grant recommendation follow-up",
        "hs_pipeline_stage": "2", "createdate": "2026-09-01T10:00:00Z"}},
    "303": {"id": "303", "properties": {
        "subject": "Resolved months ago", "hs_pipeline_stage": "4",
        "createdate": "2026-01-01T10:00:00Z"}},
}


class TicketHubSpot:
    """A HubSpot client stubbed at the HTTP seam, not the method seam."""

    def __init__(self, associations=None, assoc_error=None,
                 batch_error=None, paging=None):
        from clients.hubspot import HubSpotClient

        self.client = HubSpotClient.__new__(HubSpotClient)
        self.client.access_token = "test-token"
        self.client.base_url = "https://api.example.invalid"
        self.client.headers = {}
        self.client._social_channels_cache = None

        self.associations = associations if associations is not None else \
            [{"toObjectId": 301}, {"toObjectId": 302}, {"toObjectId": 303}]
        self.assoc_error = assoc_error
        self.batch_error = batch_error
        self.paging = paging or {}
        self.gets = []
        self.posts = []

        self.client._get = self._get
        self.client._post = self._post

    def _get(self, endpoint, params=None):
        self.gets.append((endpoint, params))
        if self.assoc_error:
            return {"error": self.assoc_error}
        after = (params or {}).get("after")
        page = self.paging.get(after or "first")
        if page is not None:
            return page
        return {"results": list(self.associations)}

    def _post(self, endpoint, data=None):
        self.posts.append((endpoint, data))
        if self.batch_error:
            return {"error": self.batch_error}
        wanted = [i["id"] for i in data["inputs"]]
        return {"results": [TICKETS[i] for i in wanted if i in TICKETS]}


def test_get_contact_tickets_reads_associations_then_batch_read():
    hub = TicketHubSpot()

    tickets = hub.client.get_contact_tickets("701")

    assert [t["id"] for t in tickets] == ["301", "302", "303"]

    endpoint, params = hub.gets[0]
    assert endpoint == "crm/v4/objects/contacts/701/associations/tickets"
    assert params["limit"] == 500

    endpoint, data = hub.posts[0]
    assert endpoint == "crm/v3/objects/tickets/batch/read"
    assert data["inputs"] == [{"id": "301"}, {"id": "302"}, {"id": "303"}]
    assert data["properties"] == ["subject", "hs_pipeline_stage", "createdate"]


def test_batch_read_is_not_audited_as_a_write():
    from clients.hubspot import is_hubspot_write

    assert is_hubspot_write(
        "POST", "crm/v3/objects/tickets/batch/read") is False
    assert is_hubspot_write(
        "GET", "crm/v4/objects/contacts/701/associations/tickets") is False


def test_get_contact_tickets_is_empty_for_a_contact_with_none():
    hub = TicketHubSpot(associations=[])

    assert hub.client.get_contact_tickets("701") == []
    assert hub.posts == [], "no batch read when there is nothing to read"


def test_get_contact_tickets_never_falls_back_on_an_error():
    hub = TicketHubSpot(assoc_error="HubSpot returned 500")
    assert hub.client.get_contact_tickets("701") == []

    hub = TicketHubSpot(batch_error="HubSpot returned 500")
    assert hub.client.get_contact_tickets("701") == []


def test_get_contact_tickets_needs_a_contact_id():
    hub = TicketHubSpot()
    assert hub.client.get_contact_tickets(None) == []
    assert hub.client.get_contact_tickets("") == []
    assert hub.gets == []


def test_get_contact_tickets_follows_paging():
    hub = TicketHubSpot(paging={
        "first": {"results": [{"toObjectId": 301}],
                  "paging": {"next": {"after": "p2"}}},
        "p2": {"results": [{"toObjectId": 302}]},
    })

    tickets = hub.client.get_contact_tickets("701")

    assert [t["id"] for t in tickets] == ["301", "302"]
    assert len(hub.gets) == 2
    assert hub.gets[1][1]["after"] == "p2"


def test_get_contact_tickets_deduplicates_multi_label_associations():
    """One ticket associated under two labels comes back twice."""
    hub = TicketHubSpot(associations=[{"toObjectId": 301},
                                      {"toObjectId": 301},
                                      {"toObjectId": 302}])

    tickets = hub.client.get_contact_tickets("701")

    assert [t["id"] for t in tickets] == ["301", "302"]


def test_get_contact_tickets_accepts_a_v3_shaped_row():
    """`id` instead of `toObjectId` must not read as zero associations."""
    hub = TicketHubSpot(associations=[{"id": 301}])

    assert [t["id"] for t in hub.client.get_contact_tickets("701")] == ["301"]


def test_donor_prep_shows_only_this_contacts_open_tickets():
    hub = TicketHubSpot()

    tickets = donor_prep._contact_tickets("701", hub.client)

    subjects = [t["subject"] for t in tickets]
    assert "Grant recommendation follow-up" in subjects
    assert "DAF paperwork question" in subjects
    assert "Resolved months ago" not in subjects, "stage 4 is closed"


def test_donor_prep_orders_tickets_newest_first():
    hub = TicketHubSpot()

    tickets = donor_prep._contact_tickets("701", hub.client)

    assert tickets[0]["subject"] == "Grant recommendation follow-up"
    assert tickets[0]["created"] == "2026-09-01"


def test_donor_prep_open_items_section_is_back(loaded):
    class HubSpot(TicketHubSpot):
        def __init__(self):
            super().__init__()
            client = self.client
            client.search_contacts = lambda name: {
                "results": [{"id": "701", "properties": {
                    "email": "aisha@example.invalid"}}]}
            client.get_contact_notes = lambda cid, limit=5: {"results": []}
            client.get_contact_emails = lambda cid, limit=5: {"results": []}
            client.get_contact_engagements = lambda cid, limit=5: {
                "results": []}
            client.get_open_tickets = _explode_open_tickets

    hub = HubSpot()
    hs = donor_prep._gather_hubspot_data("Aisha", hub.client)
    cs = donor_prep._gather_csuite_data("Aisha", StubCSuite(7001))

    brief = donor_prep._format_brief("Aisha", hs, cs, "• point")

    assert "**Open Items:**" in brief
    assert "Grant recommendation follow-up" in brief
    assert "opened 2026-09-01" in brief


def _explode_open_tickets(*args, **kwargs):
    raise AssertionError("the portal-wide ticket list must not be fetched")


def test_donor_prep_never_fetches_portal_wide_tickets():
    hub = TicketHubSpot()
    hub.client.search_contacts = lambda name: {
        "results": [{"id": "701", "properties": {}}]}
    hub.client.get_contact_notes = lambda cid, limit=5: {"results": []}
    hub.client.get_contact_emails = lambda cid, limit=5: {"results": []}
    hub.client.get_contact_engagements = lambda cid, limit=5: {"results": []}
    hub.client.get_open_tickets = _explode_open_tickets

    data = donor_prep._gather_hubspot_data("Aisha", hub.client)

    assert len(data["tickets"]) == 2
