"""CSuite event dates -> HubSpot. One way, and it cannot go the other way.

No network. The CSuite refusal test is the one that matters most here:
CSuite v2 signs the request body, so every call is an HTTP POST and the
verb says nothing about whether a call changes anything. The only signal
is the endpoint name, so the check has to be by name and has to happen
before the request is built.

Every event in this file is invented.
"""

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from sync import event_apply as ea
from sync import event_hubspot as eh
from scripts import event_sync as cli


# ---------------------------------------------------------------------------
# CSuite is read-only, enforced
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("endpoint", [
    "event/edit/eventdate",
    "event/create/eventdate",
    "profile/create/individual",
    "profile/edit",
    "funit/create",
    "task/complete",
])
def test_a_csuite_write_endpoint_is_refused(endpoint):
    """The rule this whole job hangs on."""
    with pytest.raises(eh.CSuiteWriteRefused) as caught:
        eh.read_only_endpoint(endpoint)
    assert endpoint in str(caught.value)
    assert "read-only" in str(caught.value)
    assert "Nothing was sent" in str(caught.value)


@pytest.mark.parametrize("endpoint", [
    "event/list/dates", "event/display/eventdate", "profile/list",
    "donation/list", "funit/display",
])
def test_a_csuite_read_endpoint_is_allowed(endpoint):
    assert eh.read_only_endpoint(endpoint) == endpoint


def test_the_only_csuite_endpoint_this_job_names_is_a_read():
    assert eh.read_only_endpoint(eh.EVENT_DATES_ENDPOINT)


def test_the_refusal_happens_before_any_request():
    """A CSuite client that raises on any use proves nothing was sent."""
    class Exploding:
        def __getattr__(self, name):
            raise AssertionError(f"CSuite was called: {name}()")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(eh, "EVENT_DATES_ENDPOINT", "event/edit/eventdate")
        with pytest.raises(eh.CSuiteWriteRefused):
            eh.fetch_event_dates(Exploding())


def test_the_sync_module_never_calls_a_write_endpoint_by_name():
    import inspect
    from clients.csuite import is_csuite_write
    source = inspect.getsource(eh) + inspect.getsource(cli)
    for token in ("event/create", "event/edit", "profile/create",
                  "profile/edit", "funit/create", "task/complete"):
        assert is_csuite_write(token)
        # Only ever as a test-like mention, never as an endpoint constant.
        assert f'"{token}' not in source or "read_only_endpoint" in source


# ---------------------------------------------------------------------------
# Timezones — CSuite has none, so nothing may be invented
# ---------------------------------------------------------------------------

def row(**over):
    base = {"event_date_id": 1430, "event_id": 1000,
            "event_name": "Event - Other",
            "event_description": "AMCF x ISPU Webinar",
            "event_date": "2026-10-08",
            "start_time": "3 pm ET | 2 pm CT | 12 noon PT",
            "location": "Virtual", "archived": 0,
            "goal_amount": None, "available_seats": 40}
    base.update(over)
    return base


@pytest.mark.parametrize("text, hour, zone", [
    ("3 pm ET | 2 pm CT | 12 noon PT", 15, "America/New_York"),
    ("7:30 pm PST", 19, "America/Los_Angeles"),
    ("11 am PST", 11, "America/Los_Angeles"),
    ("2 pm EST | 11 am PST", 14, "America/New_York"),
    ("6 pm CST", 18, "America/Chicago"),
    ("12 noon ET", 12, "America/New_York"),
])
def test_a_named_zone_is_used_exactly(text, hour, zone):
    moment, reason = eh.start_moment(row(start_time=text))
    assert moment.hour == hour
    assert moment.tzinfo == ZoneInfo(zone)
    assert reason is None, "an exact conversion should need no review"


def test_a_time_with_no_zone_is_assumed_et_and_says_so():
    moment, reason = eh.start_moment(row(start_time="10:00 am"))
    assert moment.hour == 10
    assert moment.tzinfo == eh.DEFAULT_TZ
    assert "names no timezone" in reason


def test_no_usable_time_lands_at_midnight_and_says_so():
    """The existing sync put 10:00 on an event with no time at all.

    Midnight is visibly a placeholder in a way that 10:00 is not.
    """
    moment, reason = eh.start_moment(row(start_time=None))
    assert (moment.hour, moment.minute) == (0, 0)
    assert "no usable time" in reason


def test_a_date_in_start_time_is_not_read_as_a_time():
    """csuite-1157's start_time is "September 3rd".

    The existing sync parsed that as a date and used it as the event's
    start, overriding event_date — so a July event is in HubSpot as a
    September one.
    """
    assert eh.parse_time("September 3rd") is None
    moment, reason = eh.start_moment(row(event_date="2026-07-21",
                                         start_time="September 3rd"))
    assert moment.date().isoformat() == "2026-07-21", "event_date was overridden"
    assert "no usable time" in reason


def test_no_event_date_is_not_syncable():
    """98 of 179 CSuite event dates have no date at all."""
    moment, reason = eh.start_moment(row(event_date=None))
    assert moment is None
    assert "cannot be a marketing event" in reason

    mapped = eh.map_event_date(row(event_date=None), "AMCF")
    assert mapped.syncable is False
    assert mapped.payload is None
    assert mapped.status == "review"


def test_the_offset_is_explicit_never_a_bare_z():
    mapped = eh.map_event_date(row(), "AMCF")
    stamp = mapped.payload["startDateTime"]
    assert not stamp.endswith("Z")
    assert ("-04:00" in stamp) or ("-05:00" in stamp)


def test_a_wall_clock_hour_is_never_written_as_utc():
    """The bug in the existing sync: 7:30 pm PST stored as 19:30Z."""
    mapped = eh.map_event_date(row(event_date="2026-04-10",
                                   start_time="7:30 pm PST"), "AMCF")
    moment = datetime.fromisoformat(mapped.payload["startDateTime"])
    assert moment.astimezone(timezone.utc).hour != 19
    assert moment.astimezone(timezone.utc).isoformat().startswith("2026-04-11")


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------

def test_the_title_comes_from_the_description_not_the_series_name():
    """event_name is the parent series and has three values in total."""
    mapped = eh.map_event_date(row(), "AMCF")
    assert mapped.payload["eventName"] == "AMCF x ISPU Webinar"


def test_the_external_id_keeps_the_existing_convention():
    """Four events already in HubSpot use csuite-<id>."""
    mapped = eh.map_event_date(row(event_date_id=1155), "AMCF")
    assert mapped.external_event_id == "csuite-1155"
    assert mapped.payload["externalEventId"] == "csuite-1155"


def test_the_organizer_is_sent():
    mapped = eh.map_event_date(row(), "AMCF")
    assert mapped.payload["eventOrganizer"] == "AMCF"


# ---------------------------------------------------------------------------
# The content hash decides an update
# ---------------------------------------------------------------------------

def test_a_field_hubspot_never_sees_does_not_change_the_hash():
    """Otherwise a ticket sale pushes an update to HubSpot."""
    before = eh.content_hash(row(available_seats=40, goal_amount="100"))
    after = eh.content_hash(row(available_seats=12, goal_amount="500"))
    assert before == after


@pytest.mark.parametrize("change", [
    {"event_description": "A different title"},
    {"event_date": "2026-10-09"},
    {"start_time": "4 pm ET"},
    {"location": "Somewhere else"},
    {"archived": 1},
])
def test_a_mapped_field_does_change_the_hash(change):
    assert eh.content_hash(row()) != eh.content_hash(row(**change))


# ---------------------------------------------------------------------------
# Archived and vanished: recorded, never acted on
# ---------------------------------------------------------------------------

def test_an_archived_event_is_flagged_and_left_alone():
    mapped = eh.map_event_date(row(archived=1), "AMCF")
    assert "archived in CSuite" in mapped.review_reason
    assert "left untouched" in mapped.review_reason
    assert mapped.status == "review"
    # Still mapped: the sync does not cancel or delete it in HubSpot.
    assert mapped.payload is not None


def test_nothing_in_the_sync_deletes_from_hubspot():
    import inspect
    source = inspect.getsource(eh) + inspect.getsource(cli)
    assert "_delete(" not in source
    assert '"DELETE"' not in source
    assert "eventCancelled" not in source


# ---------------------------------------------------------------------------
# Planning: creates, updates, and the 'unknown' resolution
# ---------------------------------------------------------------------------

def hs(object_id, external):
    return {"objectId": object_id, "externalEventId": external}


def test_a_new_event_date_is_a_create():
    result = cli.plan([row()], {}, {}, "AMCF")
    assert len(result["creates"]) == 1
    assert not result["updates"]


def test_an_event_already_in_hubspot_but_not_in_the_map_is_adopted():
    """The four "Irritable-Needle" events arrive this way."""
    result = cli.plan([row(event_date_id=1155)], {},
                      {"csuite-1155": hs("749247088350", "csuite-1155")},
                      "AMCF")
    assert not result["creates"], "it would have been created twice"
    assert len(result["updates"]) == 1
    assert "adopting" in result["updates"][0][2]


def test_an_unchanged_hash_is_left_alone():
    mapped = eh.map_event_date(row(), "AMCF")
    existing = {"1430": {"csuite_eventdate_id": "1430",
                         "hubspot_event_id": "999",
                         "content_hash": mapped.content_hash,
                         "status": "synced"}}
    result = cli.plan([row()], existing,
                      {"csuite-1430": hs("999", "csuite-1430")}, "AMCF")
    assert len(result["unchanged"]) == 1
    assert not result["updates"] and not result["creates"]


def test_a_changed_hash_is_an_update():
    existing = {"1430": {"csuite_eventdate_id": "1430",
                         "hubspot_event_id": "999",
                         "content_hash": "stale", "status": "synced"}}
    result = cli.plan([row()], existing,
                      {"csuite-1430": hs("999", "csuite-1430")}, "AMCF")
    assert len(result["updates"]) == 1
    assert "content hash changed" in result["updates"][0][2]


def test_an_unknown_create_that_did_land_becomes_an_update_not_a_create():
    """The whole point of 'unknown': resolve by looking, never by retrying.

    A retried create with no idempotency key is how one event becomes
    two, and HubSpot offers none.
    """
    existing = {"1430": {"csuite_eventdate_id": "1430",
                         "hubspot_event_id": None,
                         "content_hash": "x", "status": "unknown"}}
    result = cli.plan([row()], existing,
                      {"csuite-1430": hs("999", "csuite-1430")}, "AMCF")
    assert not result["creates"], "an ambiguous create was retried"
    assert len(result["updates"]) == 1
    assert "did land" in result["updates"][0][2]


def test_an_unknown_create_that_did_not_land_is_created_once():
    existing = {"1430": {"csuite_eventdate_id": "1430",
                         "hubspot_event_id": None,
                         "content_hash": "x", "status": "unknown"}}
    result = cli.plan([row()], existing, {}, "AMCF")
    assert len(result["creates"]) == 1
    assert "did not land" in result["creates"][0][1]


def test_a_mapped_row_whose_hubspot_event_vanished_goes_to_review():
    existing = {"1430": {"csuite_eventdate_id": "1430",
                         "hubspot_event_id": "999",
                         "content_hash": "x", "status": "synced"}}
    result = cli.plan([row()], existing, {}, "AMCF")
    assert not result["creates"], "a vanished HubSpot event was recreated"
    assert any("needs a person" in reason for _m, reason in result["review"])


def test_an_undated_event_is_skipped_not_created():
    result = cli.plan([row(event_date=None)], {}, {}, "AMCF")
    assert not result["creates"]
    assert len(result["skipped"]) == 1


def test_a_review_reason_survives_into_the_review_list():
    result = cli.plan([row(start_time="10:00 am")], {}, {}, "AMCF")
    assert len(result["creates"]) == 1
    assert any("names no timezone" in reason for _m, reason in result["review"])


# ---------------------------------------------------------------------------
# The dry run writes nothing
# ---------------------------------------------------------------------------

def test_the_call_budget_is_set():
    assert eh.CALL_BUDGET >= 1


def test_the_dry_run_report_says_no_write_was_made():
    class Fetched:
        rows = [row()]
        calls = 1
        total_429s = 0
    result = cli.plan([row()], {}, {}, "AMCF")
    report = cli.render(result, Fetched(), 1, applied=False, organizer="AMCF")
    assert "DRY RUN" in report
    assert "No HubSpot write was made" in report
    assert "csuite-1430" in report


def test_the_script_refuses_to_create_its_own_tables():
    import inspect
    source = inspect.getsource(cli)
    assert "CREATE TABLE" not in source.upper()
    assert "EXIT_NO_MIGRATION" in source


# ---------------------------------------------------------------------------
# Safety options (2026-09-30)
# ---------------------------------------------------------------------------

class FakeHubSpot:
    """Records calls; answers creates from a scripted list of outcomes."""

    def __init__(self, create_results=None):
        self.calls = []
        self.create_results = list(create_results or [])

    def _post(self, endpoint, data=None):
        self.calls.append(("POST", endpoint, data))
        if self.create_results:
            return self.create_results.pop(0)
        return {"objectId": f"hs-{len(self.calls)}"}

    def _patch(self, endpoint, data=None):
        self.calls.append(("PATCH", endpoint, data))
        return {"objectId": "hs-patched"}

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

    @property
    def creates(self):
        return [c for c in self.calls if c[0] == "POST"]

    @property
    def updates(self):
        return [c for c in self.calls if c[0] == "PATCH"]


@pytest.fixture
def no_db(monkeypatch):
    """_save_map writes to Postgres; these tests are about HubSpot."""
    monkeypatch.setattr(ea, "_save_map", lambda *a, **k: None)


# The fixture rows are dated 2026-10-08. apply_plan withholds a create whose
# start is in the past, so the clock is pinned before that date — otherwise
# these tests quietly change meaning once it rolls past, which is the drift
# that has already broken this suite twice.
PINNED_TODAY = date(2026, 10, 1)


def plan_of(creates=0, updates=0):
    rows = [row(event_date_id=1000 + i) for i in range(creates)]
    result = cli.plan(rows, {}, {}, "AMCF")
    for i in range(updates):
        mapped = eh.map_event_date(row(event_date_id=2000 + i), "AMCF")
        result["updates"].append((mapped, hs(f"hs-{i}", mapped.external_event_id),
                                  "content hash changed"))
    return result


# --- externalAccountId -----------------------------------------------

def test_every_create_payload_carries_the_external_account_id():
    mapped = eh.map_event_date(row(), "AMCF")
    assert mapped.payload["externalAccountId"] == "amcf-csuite" or \
        mapped.payload["externalAccountId"] == eh.EXTERNAL_ACCOUNT_ID
    # VERIFIED 2026-10-06: the 11 csuite-* marketing events in the portal
    # resolve under this value and 404 under any other, so it is the pair
    # HubSpot keys them on. The module declared "amuslimcf-csuite" until then
    # while clients/hubspot.create_marketing_event quietly injected this one.
    assert eh.EXTERNAL_ACCOUNT_ID == "jidhr-amcf"


def test_the_external_account_id_is_on_the_wire(no_db):
    hubspot = FakeHubSpot()
    cli.apply_plan(hubspot, plan_of(creates=2), today=PINNED_TODAY)
    for _method, _endpoint, payload in hubspot.creates:
        assert payload["externalAccountId"] == "jidhr-amcf"


# --- stop on the first bad create ------------------------------------

def test_apply_stops_on_the_first_ambiguous_create(no_db):
    """No idempotency key, so an error says nothing about the next call.

    Running eighty more creates to find out produces eighty more things
    to clean up by hand.
    """
    hubspot = FakeHubSpot(create_results=[
        {"objectId": "hs-1"},
        {"error": "500 Internal Server Error"},   # ambiguous
        {"objectId": "hs-3"},
    ])
    with pytest.raises(cli.FirstFailureStop) as caught:
        cli.apply_plan(hubspot, plan_of(creates=5), today=PINNED_TODAY)

    assert len(hubspot.creates) == 2, "it kept creating after a failure"
    outcomes = caught.value.outcomes
    assert [o["outcome"] for o in outcomes] == ["created", "unknown"]
    assert "Stopped before any further create" in caught.value.reason


def test_the_stop_carries_what_happened_before_it(no_db):
    """A partial run that reports nothing is worse than one that says
    where it got to."""
    hubspot = FakeHubSpot(create_results=[{"objectId": "hs-1"},
                                          {"objectId": None}])
    with pytest.raises(cli.FirstFailureStop) as caught:
        cli.apply_plan(hubspot, plan_of(creates=4), today=PINNED_TODAY)
    assert caught.value.outcomes[0]["hubspot_id"] == "hs-1"


def test_a_create_that_succeeds_does_not_stop_the_run(no_db):
    hubspot = FakeHubSpot()
    outcomes = cli.apply_plan(hubspot, plan_of(creates=3), today=PINNED_TODAY)
    assert len(hubspot.creates) == 3
    assert all(o["outcome"] == "created" for o in outcomes)


def test_the_ambiguous_record_is_recorded_unknown_not_retried(no_db):
    saved = []
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ea, "_save_map",
                      lambda mapped, hid, status, error=None:
                      saved.append((mapped.csuite_eventdate_id, status)))
        hubspot = FakeHubSpot(create_results=[{"error": "timeout"}])
        with pytest.raises(cli.FirstFailureStop):
            cli.apply_plan(hubspot, plan_of(creates=3), today=PINNED_TODAY)
    assert saved[-1][1] == "unknown"


# --- --limit ----------------------------------------------------------

@pytest.mark.parametrize("limit, expected", [(1, 1), (2, 2), (5, 5)])
def test_limit_caps_the_number_of_creates(no_db, limit, expected):
    hubspot = FakeHubSpot()
    cli.apply_plan(hubspot, plan_of(creates=8), limit=limit, today=PINNED_TODAY)
    assert len(hubspot.creates) == expected


def test_updates_count_toward_the_limit_too(no_db):
    """The point of a limit is to bound the blast radius of a run, and an
    update to the wrong event is not free."""
    hubspot = FakeHubSpot()
    cli.apply_plan(hubspot, plan_of(creates=2, updates=3), limit=3, today=PINNED_TODAY)
    # Three PUTs. Which were updates is decided by apply_plan, not by the
    # verb: everything upserts through /events/{externalEventId} now.
    assert len(hubspot.calls) == 3
    outcomes = cli.apply_plan(FakeHubSpot(), plan_of(creates=2, updates=3),
                              limit=3, today=PINNED_TODAY)
    # UPDATES run first as of 2026-10-06: an update touches a record that
    # already exists and whose previous value HubSpot still holds, while a
    # create adds a row somebody has to delete by hand. A spent limit should
    # buy the reversible half.
    assert [o["outcome"] for o in outcomes].count("updated") == 3
    assert [o["outcome"] for o in outcomes].count("created") == 0


def test_records_beyond_the_limit_are_deferred_not_lost(no_db):
    hubspot = FakeHubSpot()
    outcomes = cli.apply_plan(hubspot, plan_of(creates=4), limit=1, today=PINNED_TODAY)
    deferred = [o for o in outcomes if o["outcome"] == "deferred"]
    assert len(deferred) == 3
    assert "--limit 1 reached" in deferred[0]["why"]


def test_no_limit_means_no_cap(no_db):
    hubspot = FakeHubSpot()
    cli.apply_plan(hubspot, plan_of(creates=6), limit=None, today=PINNED_TODAY)
    assert len(hubspot.creates) == 6


def test_a_limit_of_zero_writes_nothing(no_db):
    hubspot = FakeHubSpot()
    outcomes = cli.apply_plan(hubspot, plan_of(creates=3), limit=0, today=PINNED_TODAY)
    assert hubspot.calls == []
    assert all(o["outcome"] == "deferred" for o in outcomes)


def test_the_limit_flag_exists_and_is_documented():
    import io as _io
    import contextlib
    buf = _io.StringIO()
    with contextlib.redirect_stdout(buf):
        with pytest.raises(SystemExit):
            cli.main(["--help"])
    help_text = buf.getvalue()
    assert "--limit" in help_text
    assert "Updates count toward N" in help_text


# --- the undated list in the report ----------------------------------

def test_the_report_lists_every_undated_event_by_id_and_name():
    """98 of them in production. A count is not a work list."""
    class Fetched:
        rows = []
        calls = 1
        total_429s = 0

    undated = [row(event_date_id=3000 + i, event_date=None,
                   event_description=f"Undated event {i}") for i in range(4)]
    result = cli.plan(undated, {}, {}, "AMCF")
    report = cli.render(result, Fetched(), 1, applied=False, organizer="AMCF")

    assert "| csuite_eventdate_id | name | reason |" in report
    for i in range(4):
        assert f"`{3000 + i}`" in report
        assert f"Undated event {i}" in report


def test_an_undated_event_keeps_its_name_for_the_list():
    mapped = eh.map_event_date(
        row(event_date=None, event_description="AMCF Open House"), "AMCF")
    assert mapped.syncable is False
    assert mapped.source_name == "AMCF Open House"
