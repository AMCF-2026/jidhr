"""An update preserves every field the sync does not own.

2026-10-07, csuite-1466. The update built its body from scratch and PUT it.
PUT to /events/{externalEventId} is an upsert, so every omitted field was
cleared. Measured before and after, from a GET snapshot taken minutes earlier:

    startDateTime    2026-12-01T00:00:00Z -> 18:00:00Z          FIXED
    endDateTime      2026-12-01T02:00:00Z -> null               LOST
    eventType        "Conference"         -> null               LOST
    eventOrganizer   "159996166"          -> "American Muslim   LOST
                                             Community Foundation"
    eventDescription "AMCF Nonprofit Directory Office Hours on  LOST
                      December 1st | Virtual - Zoom link..."
                                          -> "Location: Virtual - Zoom..."

One field corrected, four lost. The sync owns the dates and nothing else, so
an update is now read-merge-write: the body starts from the portal record the
plan already holds.

CSuite has no end time to map. Its event-date record carries exactly:
archived, available_seats, event_date, event_date_id, event_description,
event_id, event_name, event_type_code, funit_id, goal_amount, location,
newsletter, online_ticket_sales, private_event, start_time. So endDateTime
reapplies HubSpot's OWN duration to the corrected start, and the report says
it is an assumption.

No network.
"""

from datetime import date

import pytest

from intents import sync_commands
from sync import event_apply as ea
from sync import event_hubspot as eh

PINNED_TODAY = date(2026, 10, 6)

# csuite-1466 as the portal held it before the 2026-10-07 write.
PORTAL = {
    "attendees": 0, "cancellations": 0,
    "createdAt": "2026-10-02T18:31:07.032Z",
    "endDateTime": "2026-12-01T02:00:00Z",
    "eventCancelled": False, "eventCompleted": False,
    "eventDescription": "AMCF Nonprofit Directory Office Hours on December "
                        "1st | Virtual - Zoom link provided upon RSVP.",
    "eventName": "AMCF Nonprofit Directory Office Hours on December 1st",
    "eventOrganizer": "159996166",
    "eventType": "Conference",
    "eventUrl": None,
    "externalEventId": "csuite-1466", "id": "csuite-1466",
    "noShows": 0, "objectId": "864004668137", "registrants": 0,
    "startDateTime": "2026-12-01T00:00:00Z",
    "updatedAt": "2026-10-02T18:31:07.794Z",
    "customProperties": [{"name": "hs_event_status", "value": "UPCOMING",
                          "sourceVid": []}],
}


def row(event_date_id=1466, event_date="2026-12-01",
        start_time="Start Time: 1 pm ET", archived=0):
    return {"event_date_id": event_date_id, "event_id": 900,
            "event_name": "Event - Other",
            "event_description": "AMCF Nonprofit Directory Office Hours",
            "event_date": event_date, "start_time": start_time,
            "location": "Virtual - Zoom link provided upon RSVP.",
            "archived": archived, "goal_amount": None, "available_seats": 1}


def mapped_of(**kwargs):
    return eh.map_event_date(row(**kwargs), "American Muslim Community "
                                            "Foundation")


class Seam:
    def __init__(self, answers=None):
        self.sent = []
        self.answers = list(answers or [])

    def _send_with_status(self, method, endpoint, data=None):
        self.sent.append((method, endpoint, data))
        if self.answers:
            return self.answers.pop(0)
        return {"objectId": "864004668137"}, 200


@pytest.fixture(autouse=True)
def no_db(monkeypatch):
    monkeypatch.setattr(ea, "_save_map", lambda *a, **k: None)
    monkeypatch.setattr(ea, "_write_audit_ids", lambda since: [])


def plan_update(mapped, existing=None):
    return {"creates": [], "updates": [
        (mapped, PORTAL if existing is None else existing, "hash changed")],
        "unchanged": [], "skipped": [], "review": []}


def sent_body(seam):
    return seam.sent[0][2]


# ---------------------------------------------------------------------------
# Every field the sync does not own survives byte for byte
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field", [
    "eventName", "eventDescription", "eventOrganizer", "eventType",
])
def test_a_field_the_sync_does_not_own_survives_byte_for_byte(field):
    """The four that were lost on 2026-10-07."""
    seam = Seam()
    ea.apply_plan(seam, plan_update(mapped_of()), today=PINNED_TODAY)

    assert sent_body(seam)[field] == PORTAL[field]


def test_the_organizer_is_not_replaced_with_an_organisation_name():
    """HubSpot holds an owner ID. The mapped payload holds a name, and that
    name overwrote the id."""
    seam = Seam()
    mapped = mapped_of()
    assert mapped.payload["eventOrganizer"] == ("American Muslim Community "
                                                "Foundation")

    ea.apply_plan(seam, plan_update(mapped), today=PINNED_TODAY)

    assert sent_body(seam)["eventOrganizer"] == "159996166"


def test_the_description_is_not_degraded_to_a_location():
    seam = Seam()
    ea.apply_plan(seam, plan_update(mapped_of()), today=PINNED_TODAY)

    body = sent_body(seam)
    assert body["eventDescription"] == PORTAL["eventDescription"]
    assert not body["eventDescription"].startswith("Location:")


def test_read_only_fields_are_never_sent_back():
    seam = Seam()
    ea.apply_plan(seam, plan_update(mapped_of()), today=PINNED_TODAY)

    for derived in ("objectId", "id", "createdAt", "updatedAt", "attendees",
                    "registrants", "noShows", "cancellations",
                    "eventCancelled", "eventCompleted", "customProperties"):
        assert derived not in sent_body(seam), derived


def test_only_the_two_date_fields_differ_from_the_portal_record():
    """The whole claim, stated as a diff."""
    seam = Seam()
    ea.apply_plan(seam, plan_update(mapped_of()), today=PINNED_TODAY)
    body = sent_body(seam)

    differing = {k for k in body
                 if k in PORTAL and body[k] != PORTAL[k]}
    assert differing == {"startDateTime", "endDateTime"}


def test_the_required_identifiers_are_always_present():
    seam = Seam()
    ea.apply_plan(seam, plan_update(mapped_of()), today=PINNED_TODAY)

    body = sent_body(seam)
    assert body["externalEventId"] == "csuite-1466"
    assert body["externalAccountId"] == eh.EXTERNAL_ACCOUNT_ID


# ---------------------------------------------------------------------------
# The dates the sync does own
# ---------------------------------------------------------------------------

def test_the_start_time_is_the_corrected_one():
    seam = Seam()
    mapped = mapped_of()
    ea.apply_plan(seam, plan_update(mapped), today=PINNED_TODAY)

    assert sent_body(seam)["startDateTime"] == \
        mapped.payload["startDateTime"]
    assert sent_body(seam)["startDateTime"] != PORTAL["startDateTime"]


def test_the_end_time_keeps_the_portals_duration():
    """CSuite has no end time, so the only defensible end is the one HubSpot
    already implies."""
    from datetime import datetime

    seam = Seam()
    ea.apply_plan(seam, plan_update(mapped_of()), today=PINNED_TODAY)
    body = sent_body(seam)

    began = datetime.fromisoformat(body["startDateTime"])
    ended = datetime.fromisoformat(body["endDateTime"])
    assert (ended - began).total_seconds() == 2 * 3600


def test_the_assumed_end_time_is_flagged():
    seam = Seam()
    outcomes = ea.apply_plan(seam, plan_update(mapped_of()),
                             today=PINNED_TODAY)

    note = outcomes[0]["note"]
    assert "ASSUMED" in note
    assert "CSuite has no end time" in note
    assert "2h" in note


def test_a_portal_record_with_no_end_time_sends_none():
    """Nothing to carry over, and nothing invented."""
    portal = {k: v for k, v in PORTAL.items() if k != "endDateTime"}
    seam = Seam()
    ea.apply_plan(seam, plan_update(mapped_of(), existing=portal),
                  today=PINNED_TODAY)

    assert "endDateTime" not in sent_body(seam)


def test_csuite_really_has_no_end_time_field():
    """If CSuite ever grows one, this fails and the assumption can go."""
    fields = set(row())
    assert not any("end" in f for f in fields)
    assert [f for f in fields if "time" in f] == ["start_time"]


# ---------------------------------------------------------------------------
# An update with no portal record is withheld (unchanged behaviour, re-pinned)
# ---------------------------------------------------------------------------

def test_an_update_with_no_portal_record_is_withheld():
    seam = Seam()
    outcomes = ea.apply_plan(seam, plan_update(mapped_of(), existing={}),
                             today=PINNED_TODAY)

    assert seam.sent == []
    assert outcomes[0]["outcome"] == "withheld"


# ---------------------------------------------------------------------------
# content_hash: a synced record is unchanged next time
# ---------------------------------------------------------------------------

def test_a_record_synced_with_its_hash_is_unchanged_next_run():
    """Item 3. After 1466 was written, event_map holds its content_hash; the
    next plan must call it unchanged rather than planning the same write
    again."""
    source = row()
    mapped = mapped_of()
    existing_map = {"1466": {"csuite_eventdate_id": "1466",
                             "hubspot_event_id": "864004668137",
                             "external_event_id": "csuite-1466",
                             "content_hash": mapped.content_hash,
                             "status": "synced"}}
    index = {"csuite-1466": PORTAL}

    result = ea.plan([source], existing_map, index, "AMCF")

    assert len(result["unchanged"]) == 1
    assert result["updates"] == []
    assert result["creates"] == []


def test_a_restored_record_is_still_unchanged():
    """Restoring 1466 changed only HubSpot fields. The hash is over CSUITE
    data, so the restore must not make it look like an update again."""
    mapped = mapped_of()
    restored = dict(PORTAL, startDateTime="2026-12-01T18:00:00Z",
                    endDateTime="2026-12-01T20:00:00Z")
    existing_map = {"1466": {"content_hash": mapped.content_hash,
                             "hubspot_event_id": "864004668137",
                             "status": "synced"}}

    result = ea.plan([row()], existing_map, {"csuite-1466": restored}, "AMCF")

    assert len(result["unchanged"]) == 1


def test_a_changed_csuite_record_is_an_update_again():
    """Unchanged has to mean something."""
    mapped = mapped_of()
    existing_map = {"1466": {"content_hash": mapped.content_hash,
                             "hubspot_event_id": "864004668137",
                             "status": "synced"}}
    moved = row(start_time="Start Time: 4 pm ET")

    result = ea.plan([moved], existing_map, {"csuite-1466": PORTAL}, "AMCF")

    assert len(result["updates"]) == 1
    assert not result["unchanged"]


# ---------------------------------------------------------------------------
# The write count in the report
# ---------------------------------------------------------------------------

def report(**overrides):
    base = {"dry_run": False, "created": 0, "updated": 1, "unchanged": 0,
            "deferred": 0, "unknown": 0, "failed": 0, "skipped": 98,
            "review": 79, "review_rows": [], "withheld": 79,
            "withheld_rows": [], "csuite_calls": 1, "hubspot_calls": 1,
            "event_dates_read": 186, "run_logged": True,
            "migration_applied": True, "error": None, "stopped": None,
            "limit": None, "updates_only": False, "future_only": True,
            "include_ids": [], "writes_attempted": 1, "writes_succeeded": 1,
            "write_audit_ids": [70], "duration_notes": []}
    base.update(overrides)
    return base


def test_the_report_states_the_write_count_and_the_audit_ids():
    reply = sync_commands._format_event_sync_results(report())

    assert "✍️ **1 HubSpot write(s) attempted, 1 succeeded**" in reply
    assert "(write_audit 70)" in reply


def test_a_dry_run_says_nothing_was_sent():
    reply = sync_commands._format_event_sync_results(report(dry_run=True))

    assert "✍️ **0 HubSpot writes — nothing was sent.**" in reply
    assert "write_audit" not in reply


def test_a_partial_run_names_what_did_not_succeed():
    reply = sync_commands._format_event_sync_results(
        report(writes_attempted=3, writes_succeeded=1,
               write_audit_ids=[70, 71, 72]))

    assert "3 HubSpot write(s) attempted, 1 succeeded" in reply
    assert "2 did not succeed" in reply
    assert "write_audit 70, 71, 72" in reply


def test_an_unreadable_audit_says_so_rather_than_claiming_none():
    reply = sync_commands._format_event_sync_results(
        report(write_audit_ids=[]))

    assert "no write_audit ids" in reply
    assert "could not be read back" in reply


def test_the_assumed_end_time_reaches_the_report():
    reply = sync_commands._format_event_sync_results(
        report(duration_notes=[("1466", "end time is ASSUMED: CSuite has no "
                                        "end time, so HubSpot's own 2h "
                                        "duration was applied")]))

    assert "End times are assumed" in reply
    assert "`1466`" in reply
    assert "2h" in reply


def test_the_counts_come_from_the_write_seam_not_the_outcomes(monkeypatch):
    """A run that attempted five and landed one must not read as one."""
    seam = Seam([({"status": "error", "message": "no"}, 404)])
    stats = {}

    with pytest.raises(ea.WriteFailed):
        ea.apply_plan(seam, plan_update(mapped_of()), today=PINNED_TODAY,
                      stats=stats)

    assert stats["writes_attempted"] == 1
    assert stats["writes_succeeded"] == 0, "a 404 is not a success"
