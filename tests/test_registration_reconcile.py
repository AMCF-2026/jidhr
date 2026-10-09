"""Reconcile: registration_map rows for writes HubSpot already accepted.

write_audit 80, production 2026-10-09. The POST to
attendance/csuite-1463/register/create returned 201. The read-back ran about
0.2s later and found nothing, so the run was recorded as a failure — the
participation record's own createdAt was 18:43:59.508, about a second after
the read-back had already given up. The registration was in HubSpot the
whole time.

The breakdown responses here are HubSpot's DOCUMENTED shape, copied from the
real response for csuite-1463.

No network, no database.
"""

import pytest

from intents import sync_commands
from sync import registration_reconcile as rc
from sync import registrations as reg
from tests.test_registrations_preview import (CSuite, HubSpot, breakdown,
                                              participation, registrant)

CONTACT = "543954422478"
EMAIL = "a@x.inv"


class ReconcileHubSpot(HubSpot):
    """batch/read for contacts, plus the participation breakdown GET."""

    def __init__(self, contacts=None, breakdowns=()):
        super().__init__(contacts)
        self.breakdowns = list(breakdowns) or [
            breakdown(participation(contact_id=CONTACT))]
        self.gets = []

    def _get(self, endpoint, params=None):
        self.gets.append((endpoint, params))
        i = min(len(self.gets) - 1, len(self.breakdowns) - 1)
        return self.breakdowns[i]


@pytest.fixture(autouse=True)
def no_db(monkeypatch):
    monkeypatch.setattr(rc.reg, "migration_applied", lambda: True)
    monkeypatch.setattr(rc.reg, "load_map", lambda: {})
    monkeypatch.setattr(rc, "accepted_writes", lambda: {"1463": [80]})


def doubles(breakdowns=()):
    return (CSuite({"1463": [registrant(EMAIL)]}, dates={"1463": "2025-01-01"}),
            ReconcileHubSpot({EMAIL: (CONTACT, True, [])}, breakdowns))


# ---------------------------------------------------------------------------
# Which events it looks at
# ---------------------------------------------------------------------------

def test_the_event_id_comes_out_of_the_audit_endpoint():
    assert rc._event_of("marketing/v3/marketing-events/attendance/"
                        "csuite-1463/register/create"
                        "?externalAccountId=jidhr-amcf") == "1463"
    assert rc._event_of("marketing/v3/marketing-events/attendance/"
                        "csuite-1153/register/create") == "1153"


def test_an_endpoint_that_is_not_an_attendance_write_yields_nothing():
    assert rc._event_of("marketing/v3/marketing-events/events/csuite-1463") == ""
    assert rc._event_of("") == ""
    assert rc._event_of(None) == ""


def test_a_non_csuite_external_id_is_not_guessed_at():
    """Only ids this sync owns. 'csuite-' is the prefix it writes."""
    assert rc._event_of("marketing/v3/marketing-events/attendance/"
                        "someone-elses-99/register/create") == ""


# ---------------------------------------------------------------------------
# The proposal
# ---------------------------------------------------------------------------

def test_a_confirmed_registration_is_proposed(monkeypatch):
    """audit 80's row: status 'review', HubSpot holds it, becomes synced."""
    monkeypatch.setattr(rc.reg, "load_map", lambda: {
        ("1463", EMAIL): {"status": "review", "write_audit_id": 80,
                          "last_state": "REGISTERED"}})
    csuite, hubspot = doubles()

    out = rc.run(csuite=csuite, hubspot=hubspot)

    assert out["error"] is None
    assert out["confirmed"] == 1
    assert len(out["proposals"]) == 1
    proposal = out["proposals"][0]
    assert proposal["event_date_id"] == "1463"
    assert proposal["hubspot_contact_id"] == CONTACT
    assert proposal["current_status"] == "review"
    assert proposal["new_status"] == "synced"
    assert proposal["write_audit_id"] == 80


def test_a_dry_run_writes_nothing(monkeypatch):
    written = []
    monkeypatch.setattr(rc.reg, "record_registration",
                        lambda *a, **k: written.append(a) or (True, None))
    monkeypatch.setattr(rc.reg, "load_map", lambda: {
        ("1463", EMAIL): {"status": "review", "write_audit_id": 80}})
    csuite, hubspot = doubles()

    out = rc.run(csuite=csuite, hubspot=hubspot, dry_run=True)

    assert out["proposals"], "it still says what it would do"
    assert written == [], "and writes none of it"
    assert out["rows_written"] == 0


def test_apply_writes_the_row(monkeypatch):
    written = []
    monkeypatch.setattr(
        rc.reg, "record_registration",
        lambda record, ext, audit, status, error=None, last_state=None:
        written.append((ext, audit, status, last_state)) or (True, None))
    monkeypatch.setattr(rc.reg, "load_map", lambda: {
        ("1463", EMAIL): {"status": "review", "write_audit_id": 80}})
    csuite, hubspot = doubles()

    out = rc.run(csuite=csuite, hubspot=hubspot, dry_run=False)

    assert written == [("csuite-1463", 80, "synced", "REGISTERED")]
    assert out["rows_written"] == 1


def test_a_registration_hubspot_does_not_hold_is_not_recorded(monkeypatch):
    """The whole point is to ask HubSpot. An empty breakdown means nothing
    landed, so the row must NOT be written — the next run should send it."""
    monkeypatch.setattr(rc.reg, "load_map", lambda: {
        ("1463", EMAIL): {"status": "review", "write_audit_id": 80}})
    written = []
    monkeypatch.setattr(rc.reg, "record_registration",
                        lambda *a, **k: written.append(a) or (True, None))
    csuite, hubspot = doubles(breakdowns=[breakdown()])

    out = rc.run(csuite=csuite, hubspot=hubspot, dry_run=False)

    assert out["confirmed"] == 0
    assert out["not_in_hubspot"] == 1
    assert out["proposals"] == []
    assert written == []


def test_a_row_already_synced_is_left_alone(monkeypatch):
    monkeypatch.setattr(rc.reg, "load_map", lambda: {
        ("1463", EMAIL): {"status": "synced", "write_audit_id": 80}})
    csuite, hubspot = doubles()

    out = rc.run(csuite=csuite, hubspot=hubspot)

    assert out["already_synced"] == 1
    assert out["proposals"] == []
    assert hubspot.gets == [], "and HubSpot is not even asked"


def test_a_missing_row_is_proposed_too(monkeypatch):
    """A 2xx that left no row at all — the case where the map is empty."""
    csuite, hubspot = doubles()

    out = rc.run(csuite=csuite, hubspot=hubspot)

    assert out["confirmed"] == 1
    assert out["proposals"][0]["current_status"] == "(no row)"
    assert out["proposals"][0]["write_audit_id"] == 80


def test_a_failed_participation_read_is_not_treated_as_absence(monkeypatch):
    """A 403 is not evidence the registration is missing, and must not cause
    a row to be skipped silently."""
    monkeypatch.setattr(rc.reg, "load_map", lambda: {
        ("1463", EMAIL): {"status": "review", "write_audit_id": 80}})
    csuite, hubspot = doubles(
        breakdowns=[{"status": "error", "message": "403"}])

    out = rc.run(csuite=csuite, hubspot=hubspot, dry_run=False)

    assert out["proposals"] == []
    assert out["not_in_hubspot"] == 0, "a failed read is not an absence"
    assert out["events_skipped"], "and it is reported"
    assert "participation read failed" in out["events_skipped"][0][1]


def test_it_reads_the_documented_participation_endpoint(monkeypatch):
    csuite, hubspot = doubles()

    rc.run(csuite=csuite, hubspot=hubspot)

    assert hubspot.gets[0][0] == ("marketing/v3/marketing-events/"
                                  "participations/jidhr-amcf/csuite-1463/"
                                  "breakdown")
    assert hubspot.gets[0][1] == {"contactIdentifier": CONTACT,
                                  "limit": 100}, \
        "no state filter — csuite-1155's registration is NO_SHOW"


def test_no_registration_map_table_is_refused(monkeypatch):
    monkeypatch.setattr(rc.reg, "migration_applied", lambda: False)

    out = rc.run(csuite=CSuite(), hubspot=HubSpot())

    assert out["error"] and "registration_map does not exist" in out["error"]
    assert out["proposals"] == []


def test_an_unreadable_csuite_event_is_skipped_not_assumed(monkeypatch):
    csuite = CSuite({"1463": [registrant(EMAIL)]}, fail_on=("1463",))
    hubspot = ReconcileHubSpot({EMAIL: (CONTACT, True, [])})

    out = rc.run(csuite=csuite, hubspot=hubspot, dry_run=False)

    assert out["proposals"] == []
    assert out["events_skipped"]
    assert "CSuite registrant read failed" in out["events_skipped"][0][1]


def test_reconcile_never_writes_to_hubspot():
    """It repairs a local table. The HubSpot write already happened."""
    import inspect

    source = inspect.getsource(rc)
    for forbidden in ("_send_with_status", "_post(", "write_registration",
                      "attendance_request"):
        assert forbidden not in source, forbidden


def test_held_events_are_still_reconciled(monkeypatch):
    """A held event is not SENT. A row HubSpot already holds is still a row
    this table should carry, or the hold would hide a real registration."""
    monkeypatch.setattr("config.Config.REGISTRATION_HELD_EVENT_IDS",
                        ("1463",))
    csuite, hubspot = doubles()

    out = rc.run(csuite=csuite, hubspot=hubspot)

    assert out["confirmed"] == 1


# ---------------------------------------------------------------------------
# The command
# ---------------------------------------------------------------------------

def test_chat_can_ask_for_the_reconcile():
    assert sync_commands.can_handle("reconcile registrations")
    assert sync_commands.can_handle("reconcile registrations apply")


def test_sync_all_cannot_reach_the_reconcile():
    for phrase in sync_commands.ALL_SYNC_PHRASES:
        assert not any(p in phrase for p in
                       sync_commands.REGISTRATION_RECONCILE_PHRASES)


def test_the_reconcile_is_a_dry_run_unless_apply_is_said(monkeypatch):
    seen = {}
    monkeypatch.setattr(rc, "run", lambda **k: seen.update(k) or
                        {"dry_run": k.get("dry_run"), "proposals": []})

    sync_commands._reconcile_registrations("reconcile registrations")
    assert seen["dry_run"] is True

    sync_commands._reconcile_registrations("reconcile registrations apply")
    assert seen["dry_run"] is False


def test_the_reconcile_can_be_scoped_to_one_event(monkeypatch):
    seen = {}
    monkeypatch.setattr(rc, "run", lambda **k: seen.update(k) or
                        {"proposals": []})

    sync_commands._reconcile_registrations("reconcile registrations event 1463")

    assert seen["event_ids"] == ("1463",)


def test_the_reconcile_report_shows_what_it_would_insert():
    reply = sync_commands._format_reconcile_results({
        "dry_run": True, "error": None, "events_scanned": 1, "confirmed": 1,
        "already_synced": 0, "not_in_hubspot": 0, "csuite_calls": 1,
        "hubspot_calls": 2, "rows_written": 0, "events_skipped": [],
        "proposals": [{"event_date_id": "1463",
                       "hubspot_contact_id": CONTACT,
                       "email_sha1": "f6f994303f85",
                       "current_status": "review", "new_status": "synced",
                       "write_audit_id": 80}]})

    assert "PREVIEW" in reply
    assert "`1463`" in reply and CONTACT in reply
    assert "review" in reply and "synced" in reply
    assert "80" in reply
    assert "0 HubSpot writes" in reply


def test_the_reconcile_report_never_prints_an_address():
    reply = sync_commands._format_reconcile_results({
        "dry_run": True, "error": None, "events_scanned": 1, "confirmed": 1,
        "already_synced": 0, "not_in_hubspot": 0, "csuite_calls": 1,
        "hubspot_calls": 2, "rows_written": 0, "events_skipped": [],
        "proposals": [{"event_date_id": "1463",
                       "hubspot_contact_id": CONTACT,
                       "email_sha1": "f6f994303f85",
                       "current_status": "review", "new_status": "synced",
                       "write_audit_id": 80}]})

    assert "@" not in reply


# ---------------------------------------------------------------------------
# The shared predicate, and a row that does not exist yet
# ---------------------------------------------------------------------------
#
# write_audit 121: a register POST on csuite-1155 (ended 2026-04-10) got a
# 201 and HubSpot stored the participation as NO_SHOW. "reconcile
# registrations event 1155" reported 0 confirmed, because the reconcile
# carried its own REGISTERED-only check while the participation sat there.


def test_a_no_show_participation_is_confirmed(monkeypatch):
    csuite, hubspot = doubles(
        breakdowns=[breakdown(participation(contact_id=CONTACT,
                                            state="NO_SHOW"))])

    out = rc.run(csuite=csuite, hubspot=hubspot)

    assert out["confirmed"] == 1
    assert out["landed_states"] == {"NO_SHOW": 1}
    assert out["proposals"][0]["last_state"] == "NO_SHOW"


def test_a_missing_row_is_INSERTED_when_hubspot_confirms(monkeypatch):
    """csuite-1155 had NO row at all — its unverified row failed to write.
    The upsert inserts rather than only updating."""
    written = []
    monkeypatch.setattr(
        rc.reg, "record_registration",
        lambda record, ext, audit, status, error=None, last_state=None:
        written.append({"event": record["event_date_id"],
                        "status": status, "last_state": last_state,
                        "contact": record["hubspot_contact_id"]})
        or (True, None))
    monkeypatch.setattr(rc.reg, "load_map", lambda: {})      # no rows at all
    csuite, hubspot = doubles(
        breakdowns=[breakdown(participation(contact_id=CONTACT,
                                            state="NO_SHOW"))])

    out = rc.run(csuite=csuite, hubspot=hubspot, dry_run=False)

    assert out["proposals"][0]["inserted"] is True
    assert out["proposals"][0]["current_status"] == "(no row)"
    assert written == [{"event": "1463", "status": "synced",
                        "last_state": "NO_SHOW", "contact": CONTACT}]
    assert out["rows_written"] == 1


def test_a_cancelled_participation_is_not_recorded_as_registered(monkeypatch):
    monkeypatch.setattr(rc.reg, "load_map", lambda: {
        ("1463", EMAIL): {"status": "unverified", "write_audit_id": 80}})
    written = []
    monkeypatch.setattr(rc.reg, "record_registration",
                        lambda *a, **k: written.append(a) or (True, None))
    csuite, hubspot = doubles(
        breakdowns=[breakdown(participation(contact_id=CONTACT,
                                            state="CANCELLED"))])

    out = rc.run(csuite=csuite, hubspot=hubspot, dry_run=False)

    assert out["cancelled"] == 1
    assert out["confirmed"] == 0
    assert out["proposals"] == []
    assert written == []


def test_the_reconcile_uses_the_shared_predicate_not_a_copy():
    """One function, not two. The 1155 miss was two copies drifting."""
    import inspect

    source = inspect.getsource(rc)
    assert "reg.participation_state(" in source
    assert "state\": \"REGISTERED\"" not in source
    assert "'state': 'REGISTERED'" not in source
    assert "LANDED_STATES" not in source, \
        "the predicate belongs to registrations, not here"


def test_a_map_write_failure_names_the_cause(monkeypatch):
    monkeypatch.setattr(
        rc.reg, "record_registration",
        lambda *a, **k: (False, "CheckViolation: registration_map_status_check"))
    monkeypatch.setattr(rc.reg, "load_map", lambda: {})
    csuite, hubspot = doubles()

    out = rc.run(csuite=csuite, hubspot=hubspot, dry_run=False)

    assert out["failed_writes"] == 1
    assert "CheckViolation" in out["proposals"][0]["new_status"]


# ---------------------------------------------------------------------------
# Unresolved rows are named even when HubSpot cannot confirm them
# ---------------------------------------------------------------------------

def test_unresolved_rows_are_listed(monkeypatch):
    rows = [{"csuite_eventdate_id": "1155", "hubspot_contact_id": CONTACT,
             "email_sha1": "abc123", "status": "unverified",
             "last_state": None, "write_audit_id": 121,
             "age_seconds": 7200.0}]
    monkeypatch.setattr(rc, "unresolved_rows", lambda: rows)
    csuite, hubspot = doubles()

    out = rc.run(csuite=csuite, hubspot=hubspot)

    assert out["unresolved"] == rows

    reply = sync_commands._format_reconcile_results(out)
    assert "still unresolved" in reply
    assert "`1155`" in reply
    assert CONTACT in reply
    assert "unverified" in reply
    assert "121" in reply
    assert "2h" in reply, "the age, so a stuck row is visibly stuck"


def test_unresolved_rows_are_listed_even_when_nothing_is_confirmed(
        monkeypatch):
    """The row HubSpot cannot confirm is the one most worth printing."""
    rows = [{"csuite_eventdate_id": "1155", "hubspot_contact_id": CONTACT,
             "email_sha1": "abc", "status": "unverified", "last_state": None,
             "write_audit_id": 121, "age_seconds": 90.0}]
    monkeypatch.setattr(rc, "unresolved_rows", lambda: rows)
    csuite, hubspot = doubles(breakdowns=[breakdown()])

    out = rc.run(csuite=csuite, hubspot=hubspot)
    reply = sync_commands._format_reconcile_results(out)

    assert out["confirmed"] == 0
    assert "still unresolved" in reply
    assert "1m" in reply


def test_the_unresolved_query_covers_every_repairable_status():
    assert set(rc.REPAIRABLE_STATUSES) >= {"unverified", "unknown", "review",
                                           "error"}
    assert "status = ANY(%s)" in rc._UNRESOLVED_SQL


def test_the_unresolved_list_never_prints_an_address(monkeypatch):
    rows = [{"csuite_eventdate_id": "1155", "hubspot_contact_id": CONTACT,
             "email_sha1": "abc", "status": "unverified", "last_state": None,
             "write_audit_id": 121, "age_seconds": 1.0}]
    monkeypatch.setattr(rc, "unresolved_rows", lambda: rows)
    csuite, hubspot = doubles()

    reply = sync_commands._format_reconcile_results(
        rc.run(csuite=csuite, hubspot=hubspot))

    assert "@" not in reply
    assert "contact_email" not in rc._UNRESOLVED_SQL
