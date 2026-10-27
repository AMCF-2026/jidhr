"""V5.92: the one-off, create-only backfill of past CSuite events.

Every CSuite and HubSpot object here is a TEST DOUBLE; the rows are shaped
like event/list/dates and invented. No network, no database.
"""

import json
from datetime import date

import pytest

from clients import audit
from config import Config
from intents import sync_commands
from sync import event_apply as ea
from sync import event_backfill as backfill
from sync import event_hubspot as eh

TODAY = date(2026, 10, 27)
WINDOW = date(2026, 10, 26)       # today - 1 day
SINCE = date(2026, 1, 1)


def row(eid, day, start="4:00 pm EST", archived=1, name="Series",
        title=None, **extra):
    return dict({"event_date_id": eid, "event_date": day,
                 "start_time": start, "archived": archived,
                 "event_name": name,
                 "event_description": title or f"Event {eid}",
                 "location": "Zoom"}, **extra)


ARCHIVED = row("1150", "2026-02-04")                          # archived only
NO_TZ = row("1161", "2026-04-11", start="10:00 am")           # + no timezone
MIDNIGHT = row("1170", "2026-05-01", start="sometime")        # placed at 00:00
FUTURE = row("1600", "2026-11-20", archived=0)                # nightly's
OLD = row("1100", "2025-12-30")                               # before since
LISTED = row("1200", "2026-03-01")                            # in HubSpot


def index_with(*rows):
    return {eh.external_id(r["event_date_id"]): {"objectId": "77",
            "externalEventId": eh.external_id(r["event_date_id"])}
            for r in rows}


def plan(rows, index=None, existing=None, **flags):
    return backfill.plan(rows, existing or {}, index or {}, SINCE, WINDOW,
                         **flags)


# --- what releases what ----------------------------------------------------------

def test_without_flags_nothing_is_created():
    """The ordinary rules hold every archived event; the default must
    match them (0 of 23 on 2026-10-27)."""
    out = plan([ARCHIVED, NO_TZ])
    assert out["release"] == []
    assert {e["id"] for e in out["held"]} == {"1150", "1161"}


def test_include_archived_releases_only_the_archived_reason():
    out = plan([ARCHIVED, NO_TZ], include_archived=True)
    assert [e["id"] for e in out["release"]] == ["1150"]
    assert out["release"][0]["flags"] == ["include_archived"]
    [held] = out["held"]
    assert held["id"] == "1161" and held["needs"] == ["assume_eastern"]
    assert "needs --assume-eastern" in held["why"]


def test_both_flags_release_both_and_name_both():
    out = plan([ARCHIVED, NO_TZ], include_archived=True, assume_eastern=True)
    flags = {e["id"]: e["flags"] for e in out["release"]}
    assert flags == {"1150": ["include_archived"],
                     "1161": ["assume_eastern", "include_archived"]}


def test_a_midnight_guess_is_never_released():
    out = plan([MIDNIGHT], include_archived=True, assume_eastern=True)
    assert out["release"] == []
    assert "placed at midnight" in out["held"][0]["why"]


@pytest.mark.parametrize("reason,expected", [
    ("archived in CSuite — left untouched in HubSpot for review",
     (["include_archived"], [])),
    ("start_time '10:00 am' names no timezone — assumed America/New_York",
     (["assume_eastern"], [])),
    ("start_time '9:00 am' names no timezone — assumed America/New_York; "
     "archived in CSuite — left untouched in HubSpot for review",
     (["assume_eastern", "include_archived"], [])),
    ("no usable time in start_time ('x') — placed at midnight "
     "America/New_York", ([], ["no usable time in start_time ('x') — "
                              "placed at midnight America/New_York"])),
    (None, ([], [])),
])
def test_each_flag_covers_exactly_its_own_clause(reason, expected):
    assert backfill.release_flags(reason) == expected


# --- cancelled -------------------------------------------------------------------

@pytest.mark.parametrize("extra", [
    {"event_name": "CANCELLED - Spring Gala"},
    {"cancelled": 1},
    {"event_status": "Canceled"},
])
def test_a_cancelled_event_is_excluded_even_with_the_flags(extra):
    cancelled = row("1300", "2026-06-01", **extra)
    out = plan([cancelled], include_archived=True, assume_eastern=True)
    assert out["release"] == []
    [entry] = out["cancelled"]
    assert entry["id"] == "1300"


def test_csuite_has_no_cancelled_status_today():
    """2026-10-27: an event-date row carries only `archived` — nothing in
    the 23 reads as cancelled."""
    assert backfill.cancelled_reason(ARCHIVED) is None


# --- scope ------------------------------------------------------------------------

def test_scope_is_since_up_to_the_nightly_window():
    out = plan([OLD, ARCHIVED, FUTURE], include_archived=True)
    assert [e["id"] for e in out["release"]] == ["1150"]
    assert out["nightly"] == 1 and out["out_of_range"] == 1


def test_an_event_already_in_hubspot_is_left_alone():
    out = plan([LISTED], index=index_with(LISTED), include_archived=True)
    assert out["release"] == []
    assert [e["id"] for e in out["in_hubspot"]] == ["1200"]


@pytest.mark.parametrize("known", [{"status": "unknown"},
                                   {"status": "synced",
                                    "hubspot_event_id": "55"}])
def test_an_uncertain_map_row_needs_a_person(known):
    out = plan([ARCHIVED], existing={"1150": known}, include_archived=True)
    assert out["release"] == [] and len(out["needs_person"]) == 1


def test_a_sign_up_list_is_never_an_event(monkeypatch):
    monkeypatch.setattr(ea, "contacts_only_reason",
                        lambda r: "a sign-up list" if r is LISTED else "")
    out = plan([LISTED], include_archived=True)
    assert out["release"] == [] and out["skipped"][0]["why"] == "a sign-up list"


# --- apply: create-only ------------------------------------------------------------

class Hub:
    """_send_with_status double. present: externalEventIds HubSpot has."""

    def __init__(self, present=(), get_status=None, put=None):
        self.present = set(present)
        self.get_status = get_status
        self.put = put or (lambda ext: ({"objectId": f"9{ext[-4:]}"}, 200))
        self.gets, self.puts = [], []

    def _send_with_status(self, method, url, data=None, audit_meta=None):
        ext = url.split("/events/")[1].split("?")[0]
        if method == "GET":
            assert "?externalAccountId=jidhr-amcf" in url
            self.gets.append(ext)
            if self.get_status is not None:
                return {}, self.get_status
            return ({}, 200) if ext in self.present else ({}, 404)
        assert method == "PUT"
        self.puts.append({"ext": ext, "body": data, "audit_meta": audit_meta})
        return self.put(ext)


@pytest.fixture
def applying(monkeypatch):
    saved = []
    monkeypatch.setattr(ea, "_save_map",
                        lambda mapped, hid, status, error=None:
                        saved.append((mapped.csuite_eventdate_id, hid, status,
                                      mapped.content_hash)))
    return saved


def apply(entries, hub, limit=23, run_id=41):
    out = {"release": entries, "limit": limit, "created": 0, "failed": 0,
           "deferred": 0, "skipped_existing": 0, "outcomes": [],
           "hubspot_calls": 0, "stopped": None}
    backfill._apply(out, hub, run_id)
    return out


def released(*rows):
    return plan(list(rows), include_archived=True,
                assume_eastern=True)["release"]


def test_a_create_puts_the_mapped_payload_with_the_backfill_tag(applying):
    hub = Hub()
    out = apply(released(ARCHIVED), hub)

    [put] = hub.puts
    assert put["body"] == eh.map_event_date(ARCHIVED, ea.DEFAULT_ORGANIZER).payload
    assert put["audit_meta"] == {"event_backfill_run_id": 41}
    assert out["created"] == 1
    mapped = eh.map_event_date(ARCHIVED, ea.DEFAULT_ORGANIZER)
    assert applying == [("1150", "91150", "synced", mapped.content_hash)]


def test_an_event_that_appeared_meanwhile_is_never_overwritten(applying):
    hub = Hub(present={"csuite-1150"})
    out = apply(released(ARCHIVED), hub)
    assert hub.puts == [] and out["skipped_existing"] == 1
    assert applying == []


def test_an_unconfirmed_absence_stops_the_run(applying):
    hub = Hub(get_status=500)
    out = apply(released(ARCHIVED, NO_TZ), hub)
    assert hub.puts == [] and "could not confirm" in out["stopped"]


def test_the_first_failed_create_stops_the_run(applying):
    hub = Hub(put=lambda ext: ({"message": "bad"}, 400))
    out = apply(released(ARCHIVED, NO_TZ), hub)
    assert len(hub.puts) == 1 and out["failed"] == 1
    assert "Stopped before any further write" in out["stopped"]
    assert applying[0][2] == "error"


def test_a_create_with_no_id_is_unknown_not_retried(applying):
    hub = Hub(put=lambda ext: ({}, 200))
    out = apply(released(ARCHIVED, NO_TZ), hub)
    assert len(hub.puts) == 1 and "NOT retried" in out["stopped"]
    assert applying[0][2] == "unknown"


def test_the_limit_defers_the_rest(applying):
    hub = Hub()
    out = apply(released(ARCHIVED, NO_TZ), hub, limit=1)
    assert out["created"] == 1 and out["deferred"] == 1
    assert len(hub.puts) == 1


def test_apply_needs_the_event_map(monkeypatch):
    monkeypatch.setattr(ea, "migration_applied", lambda: False)
    with pytest.raises(backfill.Refused, match="duplicate guard"):
        backfill.run(SINCE, include_archived=True, apply=True,
                     hubspot=Hub(), csuite=object(), today=TODAY)


def test_the_default_limit_is_23():
    assert backfill.DEFAULT_LIMIT == 23


# --- the audit tag reaches the AUDIT ROW, never HubSpot ------------------------------------

def test_the_tag_is_on_the_audit_row_and_not_in_the_request(monkeypatch):
    import requests

    from clients import hubspot as hs_mod

    reserved, sent = [], []
    monkeypatch.setattr(hs_mod, "reserve_write",
                        lambda system, method, endpoint, payload=None, **k:
                        reserved.append(audit.payload_meta(payload)) or
                        {"id": 1})
    monkeypatch.setattr(hs_mod, "complete_write", lambda *a, **k: None)
    monkeypatch.setattr(hs_mod, "hubspot_writes_allowed", lambda: True)

    class R:
        status_code = 200
        text = '{"objectId": "9"}'

        def json(self):
            return {"objectId": "9"}

    monkeypatch.setattr(requests, "put",
                        lambda url, json=None, **k: sent.append(json) or R())
    client = hs_mod.HubSpotClient()
    mapped = eh.map_event_date(ARCHIVED, ea.DEFAULT_ORGANIZER)
    ea._write_event(client, mapped, audit_meta={"event_backfill_run_id": 41})

    assert reserved[0]["ids"]["event_backfill_run_id"] == 41
    assert "event_backfill_run_id" not in sent[0]
    assert sent[0] == mapped.payload


def test_the_run_log_job_is_event_backfill():
    assert "'event_backfill'" in backfill._RUN_OPEN_SQL


# --- the nightly event step afterwards ---------------------------------------------------

def test_the_nightly_plan_sees_a_backfilled_event_as_unchanged():
    """Once created and mapped with its content hash, the nightly plan —
    which covers the WHOLE calendar for updates — neither re-creates it nor
    updates it."""
    mapped = eh.map_event_date(ARCHIVED, ea.DEFAULT_ORGANIZER)
    existing = {"1150": {"status": "synced", "hubspot_event_id": "91150",
                         "content_hash": mapped.content_hash}}
    result = ea.plan([ARCHIVED], existing, index_with(ARCHIVED),
                     ea.DEFAULT_ORGANIZER)
    assert result["creates"] == [] and result["updates"] == []
    assert [m.csuite_eventdate_id for m, _ in result["unchanged"]] == ["1150"]


def test_a_changed_archived_event_is_withheld_by_the_nightly_step():
    """If CSuite edits one later, the update is still never sent: an
    archived event carries a review reason, and the event sync withholds
    every write for a flagged event."""
    edited = dict(ARCHIVED, location="Somewhere else")
    old = eh.map_event_date(ARCHIVED, ea.DEFAULT_ORGANIZER)
    existing = {"1150": {"status": "synced", "hubspot_event_id": "91150",
                         "content_hash": old.content_hash}}
    result = ea.plan([edited], existing, index_with(ARCHIVED),
                     ea.DEFAULT_ORGANIZER)
    assert len(result["updates"]) == 1

    class NoWrites:
        def _send_with_status(self, *a, **k):
            raise AssertionError("the nightly step wrote to a backfilled "
                                 "event")

    outcomes = ea.apply_plan(NoWrites(), result, include_ids=("1150",),
                             today=TODAY)
    assert [o["outcome"] for o in outcomes] == ["withheld"]


def test_the_nightly_step_never_creates_a_past_event():
    result = ea.plan([ARCHIVED], {}, {}, ea.DEFAULT_ORGANIZER)

    class NoWrites:
        def _send_with_status(self, *a, **k):
            raise AssertionError("wrote")

    outcomes = ea.apply_plan(NoWrites(), result, include_ids=(),
                             today=TODAY)
    assert all(o["outcome"] == "withheld" for o in outcomes)


# --- chat ------------------------------------------------------------------------------------

def test_the_chat_options():
    opts = sync_commands.backfill_options(
        "backfill events since 2026-01-01 include archived --assume-eastern "
        "apply limit 5")
    assert opts == {"since": "2026-01-01", "include_archived": True,
                    "assume_eastern": True, "apply": True, "limit": 5}
    preview = sync_commands.backfill_options(
        "backfill events since 2026-01-01")
    assert preview["apply"] is False and preview["limit"] == 23
    assert not preview["include_archived"] and not preview["assume_eastern"]


def test_the_chat_needs_a_date(monkeypatch):
    monkeypatch.setattr(backfill, "run", lambda **k: pytest.fail("ran"))
    assert "From when?" in sync_commands.handle("backfill events", None)


def test_the_chat_routes_it_and_sync_all_cannot_reach_it():
    assert sync_commands.can_handle("backfill events since 2026-01-01")
    for phrase in sync_commands.ALL_SYNC_PHRASES:
        assert not any(p in phrase
                       for p in sync_commands.EVENT_BACKFILL_PHRASES)


def test_the_preview_names_the_releasing_flag():
    out = {"since": "2026-01-01", "window_start": "2026-10-26",
           "apply": False, "limit": 23,
           "flags": ["--include-archived", "--assume-eastern"],
           "map_read": True, "created": 0, "csuite_calls": 1,
           "hubspot_calls": 1, "nightly": 6, "in_hubspot": [],
           "release": released(ARCHIVED, NO_TZ),
           "held": [], "cancelled": [], "needs_person": [], "skipped": []}
    text = backfill.report(out, show_payload_for="1161")
    assert "(csuite-1150) — released by --include-archived" in text
    assert ("(csuite-1161) — released by --assume-eastern, "
            "--include-archived") in text
    assert '"externalEventId": "csuite-1161"' in text
    assert "Add `apply` to create them." in text
