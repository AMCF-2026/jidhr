"""A page that failed is not the end of the data, and searches are paced.

Both CSuite sweeps in the donation sync were hand-rolled `while True` loops
whose only failure branch was:

    if not result.get("success"):
        logger.error(...)
        break

So a page that FAILED was indistinguishable from the last page. A 429 ended
the sweep, the caller got a short list, and nothing anywhere said so — then
the V5.55 footer printed "the whole database, so these counts are totals"
over it. ~18,800 profiles is 189 unpaced calls and ~26,600 donations is 266
more, so being throttled was likely rather than hypothetical.

They now go through clients.csuite_fetch.fetch_all, which paces, waits out a
429 and reports `complete` — the same path sync/mirror.py uses and refuses to
write on.

The HubSpot side had the same shape in a different disguise: a throttled
contact search comes back as HubSpot's error JSON, which carries no "error"
key, so _read_contact saw no "results" and reported "no such contact". A
rate-limited search read as a donor who is not in HubSpot.

No network.
"""

import pytest

from intents import sync_commands
from sync.donations import (HUBSPOT_SEARCH_PER_SECOND, SEARCH_BACKOFFS,
                            DonationSync, PartialReadRefused, SearchPacer,
                            hubspot_error, is_rate_limited)


class Fetch:
    """Stands in for clients.csuite_fetch.fetch_all."""

    def __init__(self, records, complete, error=None, calls=1, waits=0):
        self.result = type("R", (), {
            "records": records, "complete": complete, "error": error,
            "calls": calls, "total_429s": waits, "pages": 1,
            "expected": None, "first_429_at": None})()
        self.asked = []

    def __call__(self, client, endpoint, pace_ms=None, **kwargs):
        self.asked.append((endpoint, kwargs))
        return self.result


def profiles(n, start=1):
    return [{"profile_id": start + i, "primary_email": f"d{start + i}@x.inv"}
            for i in range(n)]


def sync_with(monkeypatch, profile_fetch, donation_fetch=None, enabled=True):
    monkeypatch.setattr("config.Config.CSUITE_DONATION_SYNC_ENABLED", enabled)
    instance = DonationSync.__new__(DonationSync)
    instance.csuite = object()
    instance.hubspot = object()
    instance.pace_ms = None
    instance.progress = None
    instance.reset_counters()

    fetches = {"profile/list": profile_fetch,
               "donation/list": donation_fetch or Fetch([{"d": 1}], True)}

    def fake_fetch_all(client, endpoint, pace_ms=None, **kwargs):
        return fetches[endpoint](client, endpoint, pace_ms=pace_ms, **kwargs)

    monkeypatch.setattr("clients.csuite_fetch.fetch_all", fake_fetch_all)
    return instance


# ---------------------------------------------------------------------------
# A failed page is not the end of the data
# ---------------------------------------------------------------------------

def test_a_complete_profile_sweep_is_complete(monkeypatch):
    sync = sync_with(monkeypatch, Fetch(profiles(3), True))

    emails = sync.get_profile_emails()

    assert len(emails) == 3
    assert sync.profiles_complete is True
    assert sync.profiles_error is None


def test_a_429_mid_sweep_is_not_the_end_of_the_data(monkeypatch):
    """The bug. The old loop returned these two rows and said nothing."""
    sync = sync_with(monkeypatch,
                     Fetch(profiles(2), False, error="rate limited", waits=3))

    emails = sync.get_profile_emails()

    assert len(emails) == 2, "what it managed to read is still returned"
    assert sync.profiles_complete is False
    assert "rate limited" in sync.profiles_error
    assert sync.rate_limit_waits == 3


def test_a_capped_sweep_is_not_called_incomplete(monkeypatch):
    """A sample stops early on purpose. "Did not reach the end" is only a
    failure when nothing asked it to stop."""
    sync = sync_with(monkeypatch, Fetch(profiles(500), False))

    sync.get_profile_emails(limit=500)

    assert sync.profiles_complete is True


def test_a_limit_is_turned_into_a_page_cap(monkeypatch):
    fetch = Fetch(profiles(500), False)
    sync = sync_with(monkeypatch, fetch)

    sync.get_profile_emails(limit=500)

    assert fetch.asked[0][1]["max_pages"] == 5, "500 rows at 100 a page"


def test_an_uncapped_sweep_asks_for_no_page_cap(monkeypatch):
    fetch = Fetch(profiles(3), True)
    sync = sync_with(monkeypatch, fetch)

    sync.get_profile_emails()

    assert "max_pages" not in fetch.asked[0][1]


def test_a_partial_donation_sweep_is_recorded(monkeypatch):
    sync = sync_with(monkeypatch, Fetch(profiles(1), True),
                     Fetch([{"d": 1}], False, error="CSuite 500"))

    sync.get_profile_emails()
    sync.get_donations_with_limit()

    assert sync.donations_complete is False
    assert "CSuite 500" in sync.donations_error


# ---------------------------------------------------------------------------
# What sync() does about it
# ---------------------------------------------------------------------------

def finish(sync, dry_run):
    sync.aggregate_donations = lambda donations: {}
    return sync.sync(dry_run=dry_run)


def test_a_partial_read_marks_a_preview_partial(monkeypatch):
    sync = sync_with(monkeypatch, Fetch(profiles(2), True),
                     Fetch([{"d": 1}], False, error="rate limited"))

    results = finish(sync, dry_run=True)

    assert results["partial"] is True
    assert "rate limited" in results["partial_reason"]


def test_a_partial_read_refuses_a_live_run(monkeypatch):
    """A short donation list understates lifetime_giving, and writing that
    over the correct figure makes a donor look smaller than they are."""
    sync = sync_with(monkeypatch, Fetch(profiles(2), True),
                     Fetch([{"d": 1}], False, error="rate limited"))

    with pytest.raises(PartialReadRefused) as caught:
        finish(sync, dry_run=False)

    assert "understated" in str(caught.value)
    assert "Nothing was written" in str(caught.value)


def test_a_partial_profile_sweep_is_not_an_empty_csuite(monkeypatch):
    """"No profiles" and "we never finished asking" are different answers."""
    sync = sync_with(monkeypatch, Fetch([], False, error="rate limited"))
    assert finish(sync, dry_run=True)["partial"] is True

    sync = sync_with(monkeypatch, Fetch([], False, error="rate limited"))
    with pytest.raises(PartialReadRefused):
        finish(sync, dry_run=False)


def test_an_empty_csuite_is_not_partial(monkeypatch):
    sync = sync_with(monkeypatch, Fetch([], True))

    assert finish(sync, dry_run=True)["partial"] is False


def test_a_complete_read_is_not_marked_partial(monkeypatch):
    sync = sync_with(monkeypatch, Fetch(profiles(2), True),
                     Fetch([{"d": 1}], True))

    results = finish(sync, dry_run=True)

    assert results["partial"] is False
    assert results["profiles_read"] == 2
    assert results["donations_read"] == 1


def test_the_call_counts_reach_the_result(monkeypatch):
    sync = sync_with(monkeypatch,
                     Fetch(profiles(2), True, calls=189, waits=1),
                     Fetch([{"d": 1}], True, calls=266, waits=2))

    results = finish(sync, dry_run=True)

    assert results["csuite_calls"] == 455
    assert results["csuite_rate_limit_waits"] == 3


# ---------------------------------------------------------------------------
# The footer never claims totals over a partial read
# ---------------------------------------------------------------------------

def results_with(**overrides):
    base = {"updated": 1, "skipped_no_email": 0, "skipped_not_found": 0,
            "errors": 0, "details": [], "sampled": False, "partial": False,
            "partial_reason": None, "profiles_read": 18823,
            "donations_read": 26597}
    base.update(overrides)
    return base


def test_a_partial_full_read_is_never_called_a_total():
    reply = sync_commands._format_donation_sync_results(
        results_with(partial=True, partial_reason="rate limited",
                     profiles_read=4200), dry_run=True)

    assert "PARTIAL READ" in reply
    assert "NOT totals" in reply
    assert "rate limited" in reply
    assert "these counts are totals" not in reply
    assert "whole database" not in reply


def test_a_partial_sample_says_it_is_not_even_a_sample():
    reply = sync_commands._format_donation_sync_results(
        results_with(sampled=True, partial=True,
                     partial_reason="CSuite 500"), dry_run=True)

    assert "PARTIAL READ" in reply
    assert "Not even a sample" in reply
    assert "these counts are not totals" not in reply


def test_a_complete_full_read_still_claims_totals():
    """The claim has to mean something, so it cannot be always absent."""
    reply = sync_commands._format_donation_sync_results(
        results_with(partial=False), dry_run=True)

    assert "these counts are totals" in reply
    assert "PARTIAL" not in reply


def test_a_partial_read_with_no_reason_still_says_partial():
    reply = sync_commands._format_donation_sync_results(
        results_with(partial=True, partial_reason=None), dry_run=True)

    assert "PARTIAL READ" in reply
    assert "reason not recorded" in reply


# ---------------------------------------------------------------------------
# HubSpot: pacing, and a 429 that is not "not found"
# ---------------------------------------------------------------------------

RATE_LIMITED = {"status": "error", "errorType": "RATE_LIMIT",
                "message": "You have reached your secondly limit."}


def test_hubspots_rate_limit_body_is_recognised_as_an_error():
    """It carries no "error" key, which is why it used to read as a contact
    that does not exist."""
    assert "error" not in RATE_LIMITED
    assert is_rate_limited(RATE_LIMITED) is True
    assert "RATE_LIMIT" in hubspot_error(RATE_LIMITED)


@pytest.mark.parametrize("response,expected", [
    ({"results": []}, None),
    ({"error": "boom"}, "boom"),
    ({"status": "error", "message": "nope"}, "error: nope"),
    ({"status_code": 500}, "HTTP 500"),
    ("not a dict", "unreadable response: str"),
])
def test_hubspot_error_reads_each_shape(response, expected):
    found = hubspot_error(response)
    if expected is None:
        assert found is None
    else:
        assert expected in found


class PacedHubSpot:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def search_contact_by_email(self, email, properties=None):
        self.calls += 1
        return self.responses.pop(0) if self.responses else {"results": []}


def paced_sync(hubspot, sleeper):
    instance = DonationSync.__new__(DonationSync)
    instance.csuite = object()
    instance.hubspot = hubspot
    instance.pace_ms = None
    instance.progress = None
    instance.reset_counters()
    instance.searches = SearchPacer(per_second=4.0, sleeper=sleeper,
                                    clock=lambda: 0.0)
    return instance


def test_a_rate_limited_search_is_retried_not_read_as_absent():
    slept = []
    hubspot = PacedHubSpot([RATE_LIMITED,
                            {"results": [{"id": "70123", "properties": {}}]}])
    sync = paced_sync(hubspot, slept.append)

    row, error = sync._read_contact("d@x.inv")

    assert error is None
    assert row["id"] == "70123", "the retry found the contact"
    assert hubspot.calls == 2
    assert SEARCH_BACKOFFS[0] in slept


def test_a_search_rate_limited_throughout_is_an_error_not_absent():
    hubspot = PacedHubSpot([RATE_LIMITED] * (len(SEARCH_BACKOFFS) + 1))
    sync = paced_sync(hubspot, lambda _s: None)

    row, error = sync._read_contact("d@x.inv")

    assert row is None
    assert "rate limited" in error
    assert hubspot.calls == len(SEARCH_BACKOFFS) + 1, "capped retries"


def test_searches_are_paced_under_the_documented_limit():
    """HubSpot caps the Search API at 5 requests per second across all object
    types; this paces at 4 to leave room for anything else in the portal."""
    assert HUBSPOT_SEARCH_PER_SECOND <= 5.0

    slept = []
    now = {"t": 0.0}
    pacer = SearchPacer(per_second=4.0, sleeper=slept.append,
                        clock=lambda: now["t"])

    pacer.wait()
    assert slept == []
    pacer.wait()
    assert slept == [0.25]


def test_the_pacer_does_not_sleep_when_time_has_already_passed():
    slept = []
    now = {"t": 0.0}
    pacer = SearchPacer(per_second=4.0, sleeper=slept.append,
                        clock=lambda: now["t"])

    pacer.wait()
    now["t"] = 10.0
    pacer.wait()

    assert slept == [], "a slow caller needs no help"


def test_the_limit_is_cited_where_it_is_set():
    """A magic number nobody can check is a magic number."""
    source = open("sync/donations.py").read()

    assert "developers.hubspot.com" in source
    assert "5 requests per second" in source
