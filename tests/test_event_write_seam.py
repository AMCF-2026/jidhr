"""One URL for the read, the update and the create — the one the GETs confirm.

scripts/event_sync.py built its update url as
f"{CREATE_ENDPOINT}/{external_event_id}" — no `/events/` segment. Measured
against this portal on 2026-10-06 with GETs only:

    GET marketing/v3/marketing-events                    -> 200, the listing
    GET marketing/v3/marketing-events/events             -> 405 (exists, not GET)
    GET marketing/v3/marketing-events/events/upsert      -> 405 (exists, not GET)
    GET marketing/v3/marketing-events/{objectId}         -> 200, one event
    GET marketing/v3/marketing-events/events/{extId}     -> 200 with
                                                            externalAccountId,
                                                            validation error
                                                            without it

So the path the update used does not exist. **Every update it ever sent
404'd**, including the five attempted in production on 2026-10-06 — which is
why the portal was unchanged after that run, not because anything stopped in
time.

And a 404 would have been recorded as a SUCCESS. HubSpot answers one with a
JSON body — {"status": "error", "category": ..., "message": ...} — which
_parse_response returns verbatim, so it carries no "error" key, so
`result.get("error")` was None, so the row went in as "synced". The write
seam checks the STATUS CODE now.

No network.
"""

from datetime import date

import pytest

from clients.hubspot import hubspot_error, marketing_event_url
from sync import event_apply as ea
from sync import event_hubspot as eh

PINNED_TODAY = date(2026, 10, 6)

# HubSpot's real 404 body shape. No "error" key — that is the whole point.
NOT_FOUND = {"status": "error", "category": "OBJECT_NOT_FOUND",
             "message": "No marketing event found", "correlationId": "x"}


def row(event_date_id, event_date="2026-12-01"):
    return {"event_date_id": event_date_id, "event_id": 900,
            "event_name": "E", "event_description": "An event",
            "event_date": event_date, "start_time": "3 pm ET",
            "location": "Virtual", "archived": 0,
            "goal_amount": None, "available_seats": 1}


def mapped_of(**kwargs):
    return eh.map_event_date(row(**kwargs), "AMCF")


def plan_of(creates=(), updates=()):
    result = {"creates": [], "updates": [], "unchanged": [], "skipped": [],
              "review": []}
    for m in creates:
        result["creates"].append((m, "new"))
    for m in updates:
        result["updates"].append((m, {"objectId": "hs-1",
                                      "externalEventId": m.external_event_id},
                                  "content hash changed"))
    return result


class Seam:
    """Records what reached _send_with_status, and answers with (body, status)."""

    def __init__(self, answers=None):
        self.sent = []
        self.answers = list(answers or [])

    def _send_with_status(self, method, endpoint, data=None):
        self.sent.append((method, endpoint, data))
        if self.answers:
            return self.answers.pop(0)
        return {"objectId": "hs-new"}, 200


@pytest.fixture(autouse=True)
def no_db(monkeypatch):
    monkeypatch.setattr(ea, "_save_map", lambda *a, **k: None)


# ---------------------------------------------------------------------------
# One builder
# ---------------------------------------------------------------------------

def test_the_builder_matches_the_path_the_get_confirmed():
    assert marketing_event_url("csuite-1466") == \
        "marketing/v3/marketing-events/events/csuite-1466"
    assert marketing_event_url() == "marketing/v3/marketing-events"


def test_the_old_update_path_is_not_what_the_builder_produces():
    """The regression, stated as an inequality."""
    old = f"marketing/v3/marketing-events/csuite-1466"

    assert marketing_event_url("csuite-1466") != old
    assert "/events/" in marketing_event_url("csuite-1466")


def test_the_write_path_equals_the_read_path():
    """The GET that works and the PUT that writes are the same url."""
    read_url = marketing_event_url("csuite-1464")
    seam = Seam()
    ea.apply_plan(seam, plan_of(updates=[mapped_of(event_date_id=1464)]),
                  today=PINNED_TODAY)

    assert seam.sent[0][1] == read_url


def test_the_client_uses_the_builder_too():
    """create_marketing_event had its own copy of the path."""
    import inspect

    from clients.hubspot import HubSpotClient

    source = inspect.getsource(HubSpotClient.create_marketing_event)
    assert "marketing_event_url(" in source
    assert "f\"marketing/v3" not in source


def test_updates_and_creates_use_the_same_shape():
    """One proven write shape. PUT /events/{id} is an upsert, and it is the
    only shape with 200s on this portal — write_audit rows 46-52."""
    seam = Seam()
    ea.apply_plan(seam, plan_of(creates=[mapped_of(event_date_id=1528,
                                                   event_date="2026-12-15")],
                                updates=[mapped_of(event_date_id=1464)]),
                  today=PINNED_TODAY)

    methods = {m for m, _e, _d in seam.sent}
    assert methods == {"PUT"}
    assert all("/events/" in e for _m, e, _d in seam.sent)


def test_the_payload_carries_the_account_id():
    """The external-id path REQUIRES externalAccountId — the GET without it
    returns a validation error."""
    seam = Seam()
    ea.apply_plan(seam, plan_of(updates=[mapped_of(event_date_id=1464)]),
                  today=PINNED_TODAY)

    assert seam.sent[0][2]["externalAccountId"] == eh.EXTERNAL_ACCOUNT_ID


# ---------------------------------------------------------------------------
# A non-2xx can never be recorded as synced
# ---------------------------------------------------------------------------

def test_a_404_is_a_failure_not_a_success():
    """The exact body HubSpot sends, which has no "error" key."""
    assert "error" not in NOT_FOUND
    assert hubspot_error(NOT_FOUND) is not None

    seam = Seam([(NOT_FOUND, 404)])
    with pytest.raises(ea.WriteFailed) as caught:
        ea.apply_plan(seam, plan_of(updates=[mapped_of(event_date_id=1464)]),
                      today=PINNED_TODAY)

    assert [o["outcome"] for o in caught.value.outcomes] == ["failed"]
    assert "HTTP 404" in caught.value.outcomes[0]["error"]
    assert "OBJECT_NOT_FOUND" in caught.value.outcomes[0]["error"]


def test_a_404_never_reaches_save_map_as_synced(monkeypatch):
    """The assertion the brief asks for, made directly."""
    saved = []
    monkeypatch.setattr(ea, "_save_map",
                        lambda mapped, hid, status, error=None:
                        saved.append(status))

    seam = Seam([(NOT_FOUND, 404)])
    with pytest.raises(ea.WriteFailed):
        ea.apply_plan(seam, plan_of(updates=[mapped_of(event_date_id=1464)]),
                      today=PINNED_TODAY)

    assert saved == ["error"]
    assert "synced" not in saved


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 429, 500, 503])
def test_no_non_2xx_status_is_ever_synced(monkeypatch, status):
    saved = []
    monkeypatch.setattr(ea, "_save_map",
                        lambda mapped, hid, st, error=None: saved.append(st))

    seam = Seam([({"status": "error", "message": "no"}, status)])
    with pytest.raises(ea.WriteFailed):
        ea.apply_plan(seam, plan_of(updates=[mapped_of(event_date_id=1464)]),
                      today=PINNED_TODAY)

    assert "synced" not in saved


def test_a_2xx_whose_body_is_an_error_is_still_a_failure():
    seam = Seam([({"status": "error", "message": "nope"}, 200)])
    with pytest.raises(ea.WriteFailed) as caught:
        ea.apply_plan(seam, plan_of(updates=[mapped_of(event_date_id=1464)]),
                      today=PINNED_TODAY)

    assert "body is an error" in caught.value.outcomes[0]["error"]


def test_a_2xx_with_an_id_is_a_success():
    """Success has to be reachable, or the test above proves nothing."""
    seam = Seam([({"objectId": "hs-7"}, 200)])
    outcomes = ea.apply_plan(
        seam, plan_of(updates=[mapped_of(event_date_id=1464)]),
        today=PINNED_TODAY)

    assert [o["outcome"] for o in outcomes] == ["updated"]
    assert outcomes[0]["hubspot_id"] == "hs-7"


# ---------------------------------------------------------------------------
# Ambiguous stays ambiguous
# ---------------------------------------------------------------------------

def test_no_status_at_all_is_ambiguous_not_failed():
    """A transport fault may have landed. With no idempotency key, calling it
    a clean failure is how a retry duplicates an event."""
    seam = Seam([({"error": "connection reset"}, None)])
    with pytest.raises(ea.FirstFailureStop) as caught:
        ea.apply_plan(seam, plan_of(creates=[mapped_of(
            event_date_id=1528, event_date="2026-12-15")]),
            today=PINNED_TODAY)

    assert [o["outcome"] for o in caught.value.outcomes] == ["unknown"]
    assert "NOT retried" in caught.value.outcomes[0]["why"]


def test_a_2xx_with_no_id_is_ambiguous_too():
    seam = Seam([({}, 200)])
    with pytest.raises(ea.FirstFailureStop) as caught:
        ea.apply_plan(seam, plan_of(creates=[mapped_of(
            event_date_id=1528, event_date="2026-12-15")]),
            today=PINNED_TODAY)

    assert caught.value.outcomes[0]["outcome"] == "unknown"


def test_an_ambiguous_create_is_recorded_unknown(monkeypatch):
    saved = []
    monkeypatch.setattr(ea, "_save_map",
                        lambda mapped, hid, st, error=None: saved.append(st))
    seam = Seam([({"error": "timeout"}, None)])

    with pytest.raises(ea.FirstFailureStop):
        ea.apply_plan(seam, plan_of(creates=[mapped_of(
            event_date_id=1528, event_date="2026-12-15")]),
            today=PINNED_TODAY)

    assert saved == ["unknown"]


# ---------------------------------------------------------------------------
# The first failure stops the run
# ---------------------------------------------------------------------------

def test_one_404_stops_the_run_rather_than_sending_five():
    """Production attempted five updates against a url that cannot exist.
    One failure is evidence about the next call, not an isolated event."""
    seam = Seam([(NOT_FOUND, 404)])
    updates = [mapped_of(event_date_id=i) for i in
               (1466, 1464, 1463, 1462, 1430)]

    with pytest.raises(ea.WriteFailed) as caught:
        ea.apply_plan(seam, plan_of(updates=updates), today=PINNED_TODAY)

    assert len(seam.sent) == 1, "five 404s is four more than anyone needs"
    assert "Stopped before any further write" in caught.value.reason
    assert len(caught.value.outcomes) == 1


def test_a_failure_during_updates_stops_before_the_creates():
    seam = Seam([(NOT_FOUND, 404)])
    plan = plan_of(creates=[mapped_of(event_date_id=1528,
                                      event_date="2026-12-15")],
                   updates=[mapped_of(event_date_id=1464)])

    with pytest.raises(ea.WriteFailed):
        ea.apply_plan(seam, plan, today=PINNED_TODAY)

    assert len(seam.sent) == 1
    assert "/events/csuite-1464" in seam.sent[0][1], "the update, not the create"


def test_the_stop_carries_what_happened_before_it():
    seam = Seam([({"objectId": "hs-1"}, 200), (NOT_FOUND, 404)])
    updates = [mapped_of(event_date_id=1466), mapped_of(event_date_id=1464)]

    with pytest.raises(ea.WriteFailed) as caught:
        ea.apply_plan(seam, plan_of(updates=updates), today=PINNED_TODAY)

    kinds = [o["outcome"] for o in caught.value.outcomes]
    assert kinds == ["updated", "failed"]


def test_run_reports_a_stop_rather_than_raising(monkeypatch):
    """run() turns both stop types into `stopped`, so a caller gets a report
    instead of a traceback — which is what production got."""
    monkeypatch.setattr(ea, "migration_applied", lambda: True)
    monkeypatch.setattr(ea, "load_map", lambda: {})
    monkeypatch.setattr(ea, "open_run", lambda applied: 7)
    closed = {}
    monkeypatch.setattr(ea, "close_run",
                        lambda rid, status, counts, outcomes=None,
                        error_summary=None: closed.update(
                            id=rid, status=status, counts=dict(counts),
                            error=error_summary))
    monkeypatch.setattr(ea.eh, "fetch_event_dates",
                        lambda client, pace_ms=None: eh.Fetched(
                            rows=[row(1464)], calls=1, complete=True,
                            error=None, total_429s=0))
    monkeypatch.setattr(
        ea.eh, "hubspot_index",
        lambda hubspot: ({"csuite-1464": {"objectId": "hs-1",
                                          "externalEventId": "csuite-1464"}},
                         1, None))
    monkeypatch.setattr("clients.csuite.CSuiteClient", lambda: object())

    result = ea.run(hubspot=Seam([(NOT_FOUND, 404)]), dry_run=False,
                    today=PINNED_TODAY)

    assert result["stopped"] and "HTTP 404" in result["stopped"]
    assert closed["status"] == "failed", "a stopped run is not complete"


# ---------------------------------------------------------------------------
# An update must be for an event the portal actually has
# ---------------------------------------------------------------------------
#
# The write is a PUT to /events/{externalEventId}, and that is an UPSERT. So
# an "update" for an id the portal does not hold would quietly CREATE it —
# the one outcome the create/update split exists to decide deliberately.
# plan() only appends to `updates` when the id was found in hubspot_index, so
# this holds by construction; it is checked anyway because nothing else would
# catch a hand-built plan or a future change to plan().

def plan_with_update(existing):
    mapped = mapped_of(event_date_id=1464)
    return {"creates": [], "updates": [(mapped, existing, "hash changed")],
            "unchanged": [], "skipped": [], "review": []}, mapped


def test_an_update_not_in_the_portal_is_withheld():
    seam = Seam()
    plan, _mapped = plan_with_update({})          # nothing came back

    outcomes = ea.apply_plan(seam, plan, today=PINNED_TODAY)

    assert seam.sent == [], "an upsert would have created it"
    assert outcomes[0]["outcome"] == "withheld"
    assert "not in the HubSpot listing" in outcomes[0]["why"]
    assert "would create the event" in outcomes[0]["why"]


def test_an_update_with_no_objectid_is_withheld():
    seam = Seam()
    plan, _mapped = plan_with_update({"externalEventId": "csuite-1464"})

    outcomes = ea.apply_plan(seam, plan, today=PINNED_TODAY)

    assert seam.sent == []
    assert outcomes[0]["outcome"] == "withheld"


def test_an_update_whose_portal_row_is_a_different_event_is_withheld():
    """A mismatched pairing is worse than a missing one: it would write this
    event's payload under that event's id."""
    seam = Seam()
    plan, _mapped = plan_with_update({"objectId": "hs-9",
                                      "externalEventId": "csuite-9999"})

    outcomes = ea.apply_plan(seam, plan, today=PINNED_TODAY)

    assert seam.sent == []
    assert outcomes[0]["outcome"] == "withheld"


def test_an_update_that_IS_in_the_portal_is_sent():
    """The guard has to let the real case through."""
    seam = Seam()
    plan, mapped = plan_with_update({"objectId": "hs-1464",
                                     "externalEventId": "csuite-1464"})

    outcomes = ea.apply_plan(seam, plan, today=PINNED_TODAY)

    assert len(seam.sent) == 1
    assert outcomes[0]["outcome"] == "updated"
    assert outcomes[0]["hubspot_id"] in ("hs-1464", "hs-new")


def test_plan_never_produces_an_update_outside_the_listing():
    """The structural half of the same claim."""
    rows = [row(1464), row(1528, event_date="2026-12-15")]
    index = {"csuite-1464": {"objectId": "hs-1464",
                             "externalEventId": "csuite-1464"}}

    result = ea.plan(rows, {}, index, "AMCF")

    for _mapped, existing, _why in result["updates"]:
        assert existing.get("objectId"), "plan() produced a blind update"
    assert [str(m.csuite_eventdate_id)
            for m, _w in result["creates"]] == ["1528"]


# ---------------------------------------------------------------------------
# An 'unknown' outcome stops the run, on both paths
# ---------------------------------------------------------------------------

def test_an_unknown_create_stops_the_run():
    seam = Seam([({"error": "timeout"}, None),
                 ({"objectId": "hs-2"}, 200)])
    creates = [mapped_of(event_date_id=1528, event_date="2026-12-15"),
               mapped_of(event_date_id=1529, event_date="2026-12-16")]

    with pytest.raises(ea.FirstFailureStop) as caught:
        ea.apply_plan(seam, plan_of(creates=creates), today=PINNED_TODAY)

    assert len(seam.sent) == 1, "the second create must not be attempted"
    assert caught.value.outcomes[-1]["outcome"] == "unknown"


def test_an_unknown_update_stops_the_run():
    seam = Seam([({"error": "connection reset"}, None),
                 ({"objectId": "hs-2"}, 200)])
    updates = [mapped_of(event_date_id=1466), mapped_of(event_date_id=1464)]

    with pytest.raises(ea.WriteFailed) as caught:
        ea.apply_plan(seam, plan_of(updates=updates), today=PINNED_TODAY)

    assert len(seam.sent) == 1
    assert caught.value.outcomes[-1]["outcome"] == "unknown"
    assert "ambiguous" in caught.value.reason


def test_an_unknown_update_stops_before_the_creates():
    seam = Seam([({"error": "timeout"}, None)])
    plan = plan_of(creates=[mapped_of(event_date_id=1528,
                                      event_date="2026-12-15")],
                   updates=[mapped_of(event_date_id=1466)])

    with pytest.raises(ea.WriteFailed):
        ea.apply_plan(seam, plan, today=PINNED_TODAY)

    assert len(seam.sent) == 1
    assert "csuite-1466" in seam.sent[0][1], "the update, not the create"
