"""_save_map actually executes, with the real clock and the real module scope.

2026-10-06, production: "sync events apply ..." returned

    Event sync failed: name 'datetime' is not defined

_save_map stamps last_synced_at with datetime.now(timezone.utc). Both names
were imported at the top of scripts/event_sync.py and were not carried over
when _save_map moved into sync/event_apply.py. The function had no module-level
datetime at all.

**Why 2,622 tests missed it.** Every apply test stubs _save_map itself:

    @pytest.fixture(autouse=True)
    def no_db(monkeypatch):
        monkeypatch.setattr(ea, "_save_map", lambda *a, **k: None)

which is the right call for tests about HubSpot — _save_map writes to
Postgres. But it means the body of _save_map was never executed by anything,
so a NameError on its third line was invisible. The clock being pinned is NOT
the cause: `today` only reaches the withholding rules, never _save_map.

So these tests stub the DATABASE and leave _save_map real. They are the only
tests in the suite that execute it.

No network, no database.
"""

from datetime import date, datetime, timezone

import pytest

from sync import event_apply as ea
from sync import event_hubspot as eh

PINNED_TODAY = date(2026, 10, 6)


def row(event_date_id, event_date="2026-12-01", archived=0):
    return {"event_date_id": event_date_id, "event_id": 900,
            "event_name": "Event - Other", "event_description": "An event",
            "event_date": event_date, "start_time": "3 pm ET",
            "location": "Virtual", "archived": archived,
            "goal_amount": None, "available_seats": 10}


@pytest.fixture
def stored(monkeypatch):
    """The database stubbed, _save_map REAL. The opposite of the no_db
    fixture every other apply test uses."""
    rows = []

    def execute_query(sql, params=None, fetch=True):
        rows.append({"sql": " ".join(str(sql).split()), "params": params})
        return 1

    monkeypatch.setattr(ea.database, "execute_query", execute_query)
    return rows


class FakeHubSpot:
    def __init__(self, fail=False):
        self.fail = fail
        self.calls = []

    def _patch(self, endpoint, payload=None):
        self.calls.append(("PATCH", endpoint))
        return {"error": "nope"} if self.fail else {"objectId": "hs-1"}

    def _post(self, endpoint, payload=None):
        self.calls.append(("POST", endpoint))
        return {"error": "nope"} if self.fail else {"objectId": "hs-new"}

    def _send_with_status(self, method, endpoint, data=None):
        """apply_plan writes through this seam now, because the STATUS CODE
        is the only reliable success signal: HubSpot answers a 404 with a
        JSON body that carries no "error" key, so a 404 used to read as a
        success and be recorded as "synced"."""
        result = self._post(endpoint, data) if method in ("POST", "PUT") \
            else self._patch(endpoint, data)
        if isinstance(result, dict) and result.get("status") == "error":
            return result, 404          # a definite HTTP failure
        if isinstance(result, dict) and result.get("error"):
            return result, None         # a transport fault: AMBIGUOUS
        return result, 200


def plan_of(creates=(), updates=()):
    result = {"creates": [], "updates": [], "unchanged": [], "skipped": [],
              "review": []}
    for mapped in creates:
        result["creates"].append((mapped, "new"))
    for mapped in updates:
        result["updates"].append(
            (mapped, {"objectId": "hs-1",
                      "externalEventId": mapped.external_event_id},
             "content hash changed"))
    for mapped in list(creates) + list(updates):
        if mapped.review_reason:
            result["review"].append((mapped, mapped.review_reason))
    return result


def mapped_of(**kwargs):
    return eh.map_event_date(row(**kwargs), "AMCF")


# ---------------------------------------------------------------------------
# The regression itself
# ---------------------------------------------------------------------------

def test_save_map_executes_with_an_unpatched_clock(stored):
    """The exact production failure. Before the fix this raised NameError on
    sync/event_apply.py:163, AFTER the PATCH had already gone out."""
    update = mapped_of(event_date_id=1466)

    outcomes = ea.apply_plan(FakeHubSpot(), plan_of(updates=[update]),
                             today=PINNED_TODAY)

    assert [o["outcome"] for o in outcomes] == ["updated"]
    assert len(stored) == 1, "the mapping row was never written"


def test_the_stamp_is_a_real_aware_utc_datetime(stored):
    """datetime.now(timezone.utc), not a string and not naive."""
    before = datetime.now(timezone.utc)
    ea.apply_plan(FakeHubSpot(), plan_of(updates=[mapped_of(event_date_id=1466)]),
                  today=PINNED_TODAY)
    after = datetime.now(timezone.utc)

    stamp = stored[0]["params"][4]
    assert isinstance(stamp, datetime)
    assert stamp.tzinfo is not None, "a naive stamp is a wrong stamp"
    assert before <= stamp <= after


def test_a_create_also_writes_its_mapping_row(stored):
    """The create path calls _save_map on its own line; the fix has to cover
    both, and a create is the one where a missing row means a DUPLICATE on
    the next run."""
    create = mapped_of(event_date_id=1528, event_date="2026-12-15")

    outcomes = ea.apply_plan(FakeHubSpot(), plan_of(creates=[create]),
                             today=PINNED_TODAY)

    assert [o["outcome"] for o in outcomes] == ["created"]
    assert stored[0]["params"][0] == "1528"
    assert stored[0]["params"][1] == "hs-new"


def test_a_failed_write_still_records_its_row_then_stops(stored):
    """The failure branch calls _save_map too, so it had the same NameError.
    It also STOPS the run now: one failed write is evidence about the next."""
    with pytest.raises(ea.WriteFailed) as caught:
        ea.apply_plan(FakeHubSpot(fail=True),
                      plan_of(updates=[mapped_of(event_date_id=1466)]),
                      today=PINNED_TODAY)

    assert [o["outcome"] for o in caught.value.outcomes] == ["unknown"]
    assert len(stored) == 1
    assert stored[0]["params"][5] == "unknown"
    assert "ambiguous" in caught.value.reason


def test_an_ambiguous_create_records_unknown_before_stopping(stored):
    """_save_map on the unknown path is the one that matters most: it is what
    stops the next run retrying a create that may have landed."""
    class NoId(FakeHubSpot):
        def _post(self, endpoint, payload=None):
            self.calls.append(("POST", endpoint))
            return {}

    with pytest.raises(ea.FirstFailureStop):
        ea.apply_plan(NoId(), plan_of(
            creates=[mapped_of(event_date_id=1528, event_date="2026-12-15")]),
            today=PINNED_TODAY)

    assert len(stored) == 1
    assert stored[0]["params"][5] == "unknown"


def test_withholding_writes_no_mapping_row(stored):
    """A withheld record was not written, so there is nothing to record."""
    archived = mapped_of(event_date_id=1043, event_date="2026-12-10",
                         archived=1)
    hubspot = FakeHubSpot()

    outcomes = ea.apply_plan(hubspot, plan_of(creates=[archived]),
                             today=PINNED_TODAY)

    assert [o["outcome"] for o in outcomes] == ["withheld"]
    assert hubspot.calls == []
    assert stored == []


def test_an_unchanged_row_still_gets_its_timestamp_refreshed(stored):
    """"Last seen" and "last changed" are different questions. This call site
    runs on every apply, so it had the NameError on every apply even when
    nothing at all was written to HubSpot."""
    mapped = mapped_of(event_date_id=1462)
    plan = plan_of()
    plan["unchanged"].append((mapped, "hs-1"))

    outcomes = ea.apply_plan(FakeHubSpot(), plan, today=PINNED_TODAY)

    assert [o["outcome"] for o in outcomes] == ["unchanged"]
    assert stored[0]["params"][5] == "synced"


def test_a_not_syncable_row_is_recorded_for_review(stored):
    """Never written to HubSpot, recorded so a person can find it."""
    mapped = mapped_of(event_date_id=1200)
    mapped.syncable = False
    mapped.review_reason = "no event_date in CSuite"
    plan = plan_of()
    plan["skipped"].append((mapped, mapped.review_reason))

    ea.apply_plan(FakeHubSpot(), plan, today=PINNED_TODAY)

    assert stored[0]["params"][5] == "review"


def test_the_module_imports_datetime_at_module_scope():
    """Not inside a function: _save_map is called from four places and a
    local import in one of them would leave the others broken."""
    import inspect

    source = inspect.getsource(ea)
    header = source.split("def ")[0]
    assert "from datetime import datetime, timezone" in header


def test_every_save_map_call_site_is_covered_by_these_tests():
    """If a fifth call site appears, this fails and asks for a test."""
    import inspect

    sites = inspect.getsource(ea.apply_plan).count("_save_map(")
    assert sites == 8, (
        f"apply_plan has {sites} _save_map call sites; the tests above cover "
        "update-synced, update-ambiguous, update-failed, create-synced, "
        "create-failed, create-unknown, unchanged and skipped-review. A new "
        "one needs a test — this is the function whose body 2,622 tests "
        "never executed.")
