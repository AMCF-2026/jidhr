"""A filter is trusted when it proves it filters, not when it looks right.

2026-09-30: profile/list with q="HUBSYNC SENTINEL" returned 18,797 —
the exact total — with unrelated names. CSuite ignores an unrecognised
filter parameter and returns the whole table.

No network. `read` is injected.
"""

import pytest

from sync import filter_trust as ft
from sync.filter_trust import (EMPTY_TABLE, ERROR, FILTER_IGNORED,
                               FilterNotTrusted, TRUSTED, check_filter,
                               search_before_create)

ENDPOINT = "profile/list"
TOTAL = 18797


def reader(counts):
    """counts: (unfiltered, probe, [search]) -> a read function."""
    seq = list(counts)

    def read(endpoint, body):
        n = seq.pop(0)
        if isinstance(n, dict):
            return n
        return {"success": True, "http_status": 200, "outcome": "ok",
                "data": {"count": n, "results": []}}
    return read


# ---------------------------------------------------------------------------
# The three verdicts
# ---------------------------------------------------------------------------

def test_a_filter_that_filters_is_trusted():
    trust = check_filter(reader([TOTAL, 0]), ENDPOINT, "primary_email")
    assert trust.verdict == TRUSTED
    assert trust.trusted is True
    assert trust.unfiltered == TOTAL
    assert trust.probe_count == 0
    assert "filters" in trust.why()


def test_a_filter_that_returns_everything_is_ignored():
    """The exact shape of the 2026-09-30 failure."""
    trust = check_filter(reader([TOTAL, TOTAL]), ENDPOINT, "q")
    assert trust.verdict == FILTER_IGNORED
    assert trust.trusted is False
    assert "IGNORED" in trust.why()
    assert "must not be used for a duplicate decision" in trust.why()


def test_a_probe_that_matches_something_is_also_untrusted():
    """Neither 0 nor everything.

    The probe value is built from a fresh uuid, so anything matching it
    means the filter is doing something we do not understand — which is
    not a basis for deciding whether a record exists.
    """
    trust = check_filter(reader([TOTAL, 3]), ENDPOINT, "primary_email")
    assert trust.verdict == FILTER_IGNORED
    assert "neither 0 nor" in trust.why()
    assert "must not be used" in trust.why()


def test_an_empty_table_proves_nothing():
    """An empty table makes every filter look like it works."""
    trust = check_filter(reader([0, 0]), ENDPOINT, "primary_email")
    assert trust.verdict == EMPTY_TABLE
    assert trust.trusted is False
    assert "nothing can be concluded" in trust.why()


@pytest.mark.parametrize("responses", [
    [{"success": False, "http_status": 401, "outcome": "auth_rejected",
      "error": "need_auth"}, 0],
    [TOTAL, {"success": False, "http_status": 500,
             "outcome": "server_error", "error": "boom"}],
    [{"success": True, "http_status": 200, "outcome": "ok", "data": None}, 0],
])
def test_a_failed_read_is_an_error_not_a_pass(responses):
    trust = check_filter(reader(responses), ENDPOINT, "primary_email")
    assert trust.verdict == ERROR
    assert trust.trusted is False


# ---------------------------------------------------------------------------
# The probe value
# ---------------------------------------------------------------------------

def test_the_probe_value_is_random_not_fixed():
    """A hard-coded probe is one coincidence away from matching something
    real, and the whole check rests on it matching nothing."""
    assert ft.absent_value("primary_email") != ft.absent_value("primary_email")


def test_an_email_probe_looks_like_an_email():
    probe = ft.absent_value("primary_email")
    assert "@" in probe and probe.endswith(".invalid")


def test_a_non_email_probe_does_not_pretend_to_be_one():
    assert "@" not in ft.absent_value("last_name")


# ---------------------------------------------------------------------------
# search_before_create
# ---------------------------------------------------------------------------

def test_a_trusted_filter_returns_the_search_count():
    count, _response = search_before_create(
        reader([TOTAL, 0, 0]), ENDPOINT, "primary_email", "nobody@x.invalid")
    assert count == 0


def test_an_ignored_filter_refuses_rather_than_returning_a_count():
    """This is the whole point: no number is handed back to be misread."""
    with pytest.raises(FilterNotTrusted) as caught:
        search_before_create(reader([TOTAL, TOTAL, TOTAL]), ENDPOINT, "q",
                             "HUBSYNC SENTINEL")
    assert "IGNORED" in str(caught.value)


def test_the_trust_check_runs_before_every_search():
    """Not cached.

    Caching would mean trusting that CSuite has not changed since — the
    assumption that produced 18,797 in the first place.
    """
    calls = []

    def read(endpoint, body):
        calls.append(sorted(body.keys()))
        n = TOTAL if "primary_email" not in body else 0
        return {"success": True, "http_status": 200, "outcome": "ok",
                "data": {"count": n, "results": []}}

    search_before_create(read, ENDPOINT, "primary_email", "a@b.invalid")
    search_before_create(read, ENDPOINT, "primary_email", "c@d.invalid")
    assert len(calls) == 6, "the trust check was skipped on the second search"


def test_a_failed_search_refuses_rather_than_reading_as_zero():
    with pytest.raises(FilterNotTrusted) as caught:
        search_before_create(
            reader([TOTAL, 0, {"success": False, "http_status": 500,
                               "outcome": "server_error", "error": "boom"}]),
            ENDPOINT, "primary_email", "a@b.invalid")
    assert "failed" in str(caught.value)


def test_q_is_gone_from_the_search_path():
    import inspect
    source = inspect.getsource(ft)
    assert '"q"' not in source and "'q'" not in source
