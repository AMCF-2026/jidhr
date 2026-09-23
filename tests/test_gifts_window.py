"""scripts/gifts_window.py — a window of gifts, from the mirror only.

No network, no database: the MirrorDB fake from test_mirror_reports
answers the date-range filters the script generates.

Every gift here is invented.
"""

import io
from decimal import Decimal

import pytest

from scripts import gifts_window
from tests.test_mirror_reports import (  # noqa: F401  (fixtures)
    ENDOW, DAF, MirrorDB, mirror,
)

FUNDS = [
    {"_id": 1000, "_group": DAF, "funit_id": 1000, "fgroup_id": DAF,
     "fund_name": "Alpha Family Fund-(DAF0001)"},
    {"_id": 1002, "_group": ENDOW, "funit_id": 1002, "fgroup_id": ENDOW,
     "fund_name": "Gamma Endowment Fund-(END0003)"},
    {"_id": 1900, "_group": DAF, "funit_id": 1900, "fgroup_id": DAF,
     "fund_name": "Testing Fund-(DAF9001)"},
]


def gift(donation_id, profile_id, funit_id, day, amount):
    return {"_id": donation_id, "donation_id": donation_id,
            "profile_id": profile_id, "funit_id": funit_id,
            "donation_date": day, "donation_amount": amount,
            "donation_status": "closed", "anonymous_donation": 0,
            "payment_method_id": 1003}


START, END = "2025-11-15", "2025-12-31"

GIFTS = [
    gift(1, 7001, 1000, "2025-11-15", "100.00"),   # first-time (first_date in window)
    gift(2, 7001, 1000, "2025-12-01", "50.00"),    # same donor, second gift
    gift(3, 7002, 1002, "2025-12-31", "1000.00"),  # repeat donor (first gift 2023)
    gift(4, 7003, 1000, "2025-12-10", "25.00"),    # first-time, lapsed after
    gift(5, 7004, 1900, "2025-12-12", "99999.00"), # to a TEST fund: excluded
    gift(6, 7002, 1002, "2026-01-05", "10.00"),    # outside the window
    gift(7, 7005, 1000, "2025-11-14", "5.00"),     # the day before: outside
    gift(8, 7006, 1002, "2025-12-20", "300.00"),   # no aggregate row: unknown
]

AGGS = [
    {"_id": 7001, "profile_id": "7001", "first_date": "2025-11-15",
     "latest_date": "2026-03-01", "count": 3, "lifetime_total": "175.00"},
    {"_id": 7002, "profile_id": "7002", "first_date": "2023-01-15",
     "latest_date": "2026-01-05", "count": 5, "lifetime_total": "2000.00"},
    {"_id": 7003, "profile_id": "7003", "first_date": "2025-12-10",
     "latest_date": "2025-12-10", "count": 1, "lifetime_total": "25.00"},
    {"_id": 7004, "profile_id": "7004", "first_date": "2025-12-12",
     "latest_date": "2025-12-12", "count": 1, "lifetime_total": "99999.00"},
    {"_id": 7005, "profile_id": "7005", "first_date": "2025-11-14",
     "latest_date": "2025-11-14", "count": 1, "lifetime_total": "5.00"},
]


@pytest.fixture
def loaded(mirror):
    mirror.load({"fund": FUNDS, "donation": GIFTS, "donation_agg": AGGS})
    return mirror


def run(argv, loaded):
    out, err = io.StringIO(), io.StringIO()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("sys.stdout", out)
        patch.setattr("sys.stderr", err)
        code = gifts_window.main(argv)
    return code, out.getvalue(), err.getvalue()


# ---------------------------------------------------------------------------
# The window
# ---------------------------------------------------------------------------

def test_window_is_inclusive_and_excludes_test_funds(loaded):
    gifts = gifts_window.gifts_between(START, END)
    ids = sorted(g["csuite_id"] for g in gifts)
    # 1,2,3,4,8 in; 5 test fund; 6 after; 7 before.
    assert ids == ["1", "2", "3", "4", "8"]


def test_summary_math(loaded):
    gifts = gifts_window.gifts_between(START, END)
    aggs = gifts_window.aggregates_for(g["profile_id"] for g in gifts)
    s = gifts_window.summarise(gifts, aggs, START, END)

    assert s["count"] == 5
    assert s["total"] == Decimal("1475.00")
    assert s["median"] == Decimal("100.00")
    assert s["donors"] == 4

    assert s["first_time"]["count"] == 3          # gifts 1, 2, 4
    assert s["first_time"]["total"] == Decimal("175.00")
    assert s["first_time"]["profiles"] == {"7001", "7003"}
    assert s["repeat"]["count"] == 1              # gift 3
    assert s["repeat"]["total"] == Decimal("1000.00")
    assert s["unknown"]["count"] == 1             # gift 8, no aggregate

    assert s["by_fund"]["1000"] == {"count": 3, "total": Decimal("175.00")}
    assert s["by_fund"]["1002"] == {"count": 2, "total": Decimal("1300.00")}
    assert s["by_day"]["2025-12-31"] == {"count": 1,
                                         "total": Decimal("1000.00")}
    assert len(s["by_day"]) == 5


def test_amounts_are_decimal_not_float():
    gifts = [gift(1, 1, 1, "2025-12-01", "0.10"),
             gift(2, 1, 1, "2025-12-01", "0.20")]
    s = gifts_window.summarise(gifts, {}, START, END)
    assert s["total"] == Decimal("0.30")
    assert isinstance(s["total"], Decimal)
    assert gifts_window.money("$1,234.56") == Decimal("1234.56")
    assert gifts_window.money("junk") == Decimal("0")


def test_median_of_an_even_count():
    gifts = [gift(i, 1, 1, "2025-12-01", a)
             for i, a in enumerate(["10.00", "20.00", "30.00", "40.00"], 1)]
    assert gifts_window.summarise(gifts, {}, START, END)["median"] == \
        Decimal("25.00")


def test_report_output(loaded):
    code, out, _ = run(["--from", START, "--to", END], loaded)

    assert code == 0
    assert "Gifts:    5" in out
    assert "Total:    $1,475.00" in out
    assert "Median:   $100.00" in out
    assert "first-time       3 gifts" in out
    assert "repeat           1 gifts" in out
    assert "unknown          1 gifts" in out
    assert "Gamma Endowment Fund (END0003)" in out
    assert "Alpha Family Fund (DAF0001)" in out
    assert "Testing Fund" not in out
    assert "2025-12-31      1" in out
    assert "(CSuite mirror)" in out


def test_report_never_prints_a_donor_name_or_id(loaded):
    _, out, _ = run(["--from", START, "--to", END], loaded)
    for profile_id in ("7001", "7002", "7003", "7006"):
        assert profile_id not in out


# ---------------------------------------------------------------------------
# --lapsed
# ---------------------------------------------------------------------------

def test_lapsed_is_gave_in_window_and_not_since(loaded):
    gifts = gifts_window.gifts_between(START, END)
    aggs = gifts_window.aggregates_for(g["profile_id"] for g in gifts)

    assert gifts_window.lapsed_profiles(gifts, aggs, END) == ["7003"]
    # 7001 gave again in March; 7002 gave on Jan 5; 7006 has no aggregate
    # so nothing is known about "since" — not a lapse.


def test_lapsed_output_is_ids_only(loaded):
    code, out, err = run(["--from", START, "--to", END, "--lapsed"], loaded)

    assert code == 0
    assert out == "7003\n"
    assert "# 1 lapsed of 4 donors" in err
    assert "(CSuite mirror)" in err


def test_lapsed_ignores_a_test_fund_donor(loaded):
    _, out, _ = run(["--from", START, "--to", END, "--lapsed"], loaded)
    assert "7004" not in out


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------

def test_empty_mirror_stops_with_exit_2(mirror):
    mirror.load({})
    code, out, err = run(["--from", START, "--to", END], mirror)
    assert code == 2
    assert "CSuite mirror not loaded for donation" in err
    assert out == ""


def test_bad_dates_exit_1(loaded):
    code, _, err = run(["--from", END, "--to", START], loaded)
    assert code == 1 and "is before" in err
    with pytest.raises(SystemExit):
        gifts_window.main(["--from", "yesterday", "--to", END])


def test_empty_window_is_a_clean_answer(loaded):
    code, out, _ = run(["--from", "2020-01-01", "--to", "2020-01-31"], loaded)
    assert code == 0
    assert "No gifts in the mirror for this window." in out


def test_script_makes_no_csuite_call():
    import inspect
    source = inspect.getsource(gifts_window)
    assert "CSuiteClient" not in source
    assert "_request(" not in source
    assert "fetch_all" not in source
