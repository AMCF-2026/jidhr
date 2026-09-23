"""Event registrations in the mirror (2026-09-23).

Fixtures only — no live CSuite call anywhere in this file. The CSuite
client is the same StubClient the other mirror tests use; the database is
the same FakeDB.

Every registrant here is invented.
"""

import json

import pytest

from sync import mirror
from tests.test_mirror import (  # noqa: F401  (fixtures + doubles)
    FakeDB, StubClient, db, fail, no_pacing, ok, ok_object,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def event(event_date_id, archived=0, event_id=1000, date="2026-10-10",
          name="Nonprofit Summit"):
    return {"event_date_id": event_date_id, "event_id": event_id,
            "event_date": date, "event_name": name, "archived": archived,
            "event_type_code": "event", "available_seats": None,
            "event_description": "A description", "location": "Somewhere"}


def registrant(profile_id, email="donor@example.invalid",
               name="Testcase, Aisha", rsvp=None, attended=None, guests=None):
    row = {"profile_id": profile_id, "event_profile_email": email,
           "event_profile_name": name, "rsvp": rsvp, "attended": attended}
    if guests is not None:
        row["guests"] = guests
    return row


def display(event_date_id, registrants, archived=0, event_id=1000,
            source=None):
    """The event/display/eventdate payload: the event, plus profiles[]."""
    payload = dict(source) if source else event(
        event_date_id, archived=archived, event_id=event_id)
    payload["profiles"] = list(registrants)
    payload["tickets"] = []
    return ok_object(payload)


EVENTS = [
    event(1168, archived=0, date="2026-10-10"),
    event(1429, archived=0, date="2026-10-20"),
    event(1022, archived=1, date="2025-01-15"),   # frozen
    event(1033, archived=1, date="2025-01-30"),   # frozen
    event(1500, archived=0, date=None),           # undated but live
]

REGISTRANTS = {
    1168: [registrant(21004), registrant(21007, rsvp=1),
           registrant(21016, email=None, name="No Address")],
    1429: [registrant(31001, guests=[{"contact_name": "Guest One",
                                      "contact_email": "g@example.invalid"},
                                     {"contact_name": "Guest Two"}])],
    1022: [registrant(41001), registrant(41002)],
    1033: [registrant(42001)],
    1500: [],
}


def client_for(events=None, registrants=None, fail_on=None):
    events = EVENTS if events is None else events
    registrants = REGISTRANTS if registrants is None else registrants

    by_id = {e["event_date_id"]: e for e in events}

    def eventdate(data):
        event_date_id = int(data["event_date_id"])
        if fail_on is not None and event_date_id == fail_on:
            return fail("HTTP 500 Internal Server Error")
        return display(event_date_id, registrants.get(event_date_id, []),
                       source=by_id.get(event_date_id))

    return StubClient({
        "event/list/dates": lambda data: ok(events, count=len(events)),
        "event/display/eventdate": eventdate,
    })


def rows_of(db):
    return [r for r in db.upserted_rows()
            if r["record_type"] == "event_registration"]


# ---------------------------------------------------------------------------
# 1. The synthetic key
# ---------------------------------------------------------------------------

def test_key_is_event_date_id_colon_profile_id():
    assert mirror.registration_key(1168, 21004) == "1168:21004"
    assert mirror.registration_key("1168", "21004") == "1168:21004"


def test_key_is_stable_across_types_and_whitespace():
    assert mirror.registration_key(1168, 21004) == \
        mirror.registration_key(" 1168 ", 21004) == \
        mirror.registration_key("1168", " 21004")


def test_key_needs_both_halves():
    assert mirror.registration_key(None, 21004) is None
    assert mirror.registration_key(1168, None) is None
    assert mirror.registration_key("", "") is None


def test_the_same_profile_at_two_events_is_two_rows(db):
    events = [event(1168), event(1429)]
    same_person = {1168: [registrant(7001)], 1429: [registrant(7001)]}
    mirror.refresh_type("event_registration",
                        client=client_for(events, same_person), pace_ms=0)

    assert sorted(r["csuite_id"] for r in rows_of(db)) == \
        ["1168:7001", "1429:7001"]


def test_keys_are_stable_between_runs(db):
    mirror.refresh_type("event_registration", client=client_for(), pace_ms=0)
    first = sorted(r["csuite_id"] for r in rows_of(db))

    second_db = FakeDB()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("clients.database.execute_query", second_db)
        mirror.refresh_type("event_registration", client=client_for(),
                            pace_ms=0)
    second = sorted(r["csuite_id"] for r in second_db.upserted_rows()
                    if r["record_type"] == "event_registration")
    assert first == second


# ---------------------------------------------------------------------------
# 2. The whitelist
# ---------------------------------------------------------------------------

FORBIDDEN = ("guests", "registration_id", "first_name", "last_name",
             "ticket_type", "registered_at", "tickets", "event_description",
             "location", "cf_event_1000")


def test_whitelist_is_exactly_the_nine_agreed_fields():
    assert mirror.EVENT_REGISTRATION_FIELDS == (
        "event_date_id", "event_id", "profile_id", "event_profile_email",
        "event_profile_name", "rsvp", "attended", "guest_count", "pulled_at")


def test_extra_source_fields_are_dropped(db):
    noisy = registrant(21004)
    noisy.update({"cf_event_1000": "custom", "ticket_type": "VIP",
                  "registered_at": "2026-01-01", "dedc_id": 99,
                  "guests": [{"contact_name": "Guest",
                              "contact_email": "g@example.invalid"}]})
    mirror.refresh_type(
        "event_registration",
        client=client_for([event(1168)], {1168: [noisy]}), pace_ms=0)

    stored = rows_of(db)[0]["data"]
    assert set(stored) == set(mirror.EVENT_REGISTRATION_FIELDS)
    for field in ("cf_event_1000", "ticket_type", "registered_at", "dedc_id"):
        assert field not in stored


def test_fields_csuite_lacks_are_absent_not_stubbed_null(db):
    """registration_id, first_name, last_name, ticket_type, registered_at
    do not exist in CSuite — they must not appear as null columns."""
    mirror.refresh_type("event_registration",
                        client=client_for([event(1168)],
                                          {1168: [registrant(21004)]}),
                        pace_ms=0)
    stored = rows_of(db)[0]["data"]
    for absent in ("registration_id", "first_name", "last_name",
                   "ticket_type", "registered_at"):
        assert absent not in stored, f"{absent} was stubbed"


def test_guests_become_a_count_and_never_a_name(db):
    mirror.refresh_type(
        "event_registration",
        client=client_for([event(1429)], {1429: REGISTRANTS[1429]}),
        pace_ms=0)

    stored = rows_of(db)[0]["data"]
    assert stored["guest_count"] == 2
    assert "guests" not in stored

    raw = db.upserts[0][1][3]
    assert "Guest One" not in raw and "g@example.invalid" not in raw


def test_no_guests_is_a_zero_count(db):
    mirror.refresh_type("event_registration",
                        client=client_for([event(1168)],
                                          {1168: [registrant(21004)]}),
                        pace_ms=0)
    assert rows_of(db)[0]["data"]["guest_count"] == 0


def test_attended_is_stored_as_given_and_never_inferred_from_rsvp():
    record = mirror.registration_record(
        registrant(1, rsvp=1, attended=None), event(1168), "2026-09-23T00:00:00+00:00")
    assert record["rsvp"] == 1
    assert record["attended"] is None, "an rsvp is an intention, not a check-in"

    record = mirror.registration_record(
        registrant(1, attended=1), event(1168), "2026-09-23T00:00:00+00:00")
    assert record["attended"] == 1, "a real value is kept if CSuite ever sets one"


def test_the_row_carries_its_event_context(db):
    mirror.refresh_type(
        "event_registration",
        client=client_for([event(1168, event_id=1002)],
                          {1168: [registrant(21004)]}), pace_ms=0)
    stored = rows_of(db)[0]["data"]
    assert stored["event_date_id"] == "1168"
    assert stored["event_id"] == 1002
    assert stored["profile_id"] == 21004


# ---------------------------------------------------------------------------
# 3. Missing email
# ---------------------------------------------------------------------------

def test_a_registrant_without_an_email_is_kept_and_logged(db, caplog):
    import logging

    with caplog.at_level(logging.INFO, logger="sync.mirror"):
        result = mirror.refresh_type(
            "event_registration",
            client=client_for([event(1168)], {1168: REGISTRANTS[1168]}),
            pace_ms=0)

    stored = {r["csuite_id"]: r["data"] for r in rows_of(db)}
    assert "1168:21016" in stored, "the row must be kept"
    assert stored["1168:21016"]["event_profile_email"] is None
    assert stored["1168:21016"]["profile_id"] == 21016

    assert result.notes["registrants_without_email"] == 1
    assert any("has no email" in r.getMessage() for r in caplog.records)


def test_registrants_with_emails_are_not_logged_as_missing(db, caplog):
    import logging

    with caplog.at_level(logging.INFO, logger="sync.mirror"):
        mirror.refresh_type(
            "event_registration",
            client=client_for([event(1168)], {1168: [registrant(21004)]}),
            pace_ms=0)
    assert not [r for r in caplog.records if "has no email" in r.getMessage()]


# ---------------------------------------------------------------------------
# 4. Nightly excludes archived dates
# ---------------------------------------------------------------------------

def test_nightly_reads_only_non_archived_event_dates(db):
    client = client_for()
    result = mirror.refresh_type("event_registration", client=client,
                                 pace_ms=0)

    read = [d["event_date_id"] for e, d in client.calls
            if e == "event/display/eventdate"]
    assert sorted(read) == [1168, 1429, 1500], "archived 1022/1033 skipped"
    assert result.notes["skipped_archived"] == 2
    assert result.notes["mode"] == "nightly"


def test_nightly_never_stores_a_registrant_from_an_archived_event(db):
    mirror.refresh_type("event_registration", client=client_for(), pace_ms=0)
    keys = {r["csuite_id"] for r in rows_of(db)}
    assert not any(k.startswith(("1022:", "1033:")) for k in keys)


def test_selection_rules_in_isolation():
    chosen, skipped = mirror.select_event_dates(EVENTS, backfill=False)
    assert [e["event_date_id"] for e in chosen] == [1168, 1429, 1500]
    assert skipped["archived"] == 2

    chosen, skipped = mirror.select_event_dates(EVENTS, backfill=True)
    assert len(chosen) == 5, "backfill takes archived dates too"
    assert skipped["archived"] == 0


def test_backfill_has_no_date_filter():
    """98 of 179 real event dates carry no event_date; a date window would
    silently drop more than half the catalogue."""
    undated = [event(1, date=None), event(2, date=None),
               event(3, date="2019-01-01")]
    chosen, _ = mirror.select_event_dates(undated, backfill=True)
    assert len(chosen) == 3


def test_an_event_date_with_no_id_is_skipped_not_guessed():
    chosen, skipped = mirror.select_event_dates(
        [{"archived": 0}, event(1168)], backfill=True)
    assert [e["event_date_id"] for e in chosen] == [1168]
    assert skipped["unkeyed"] == 1


# ---------------------------------------------------------------------------
# 5. A nightly run must not delete the backfill
# ---------------------------------------------------------------------------

def test_nightly_does_not_delete_archived_events_registrants(db):
    """The whole backfill lives under event dates nightly never reads. If
    delete-scoping were missing, night one would wipe it."""
    for key in ("1022:41001", "1022:41002", "1033:42001"):
        db.stored[("event_registration", key)] = "a-hash-from-the-backfill"

    result = mirror.refresh_type("event_registration", client=client_for(),
                                 pace_ms=0)

    assert db.deletes == [], "archived registrants must survive"
    assert result.deleted == 0
    assert result.notes["protected_from_delete"] == 3


def test_a_registrant_removed_from_a_live_event_is_deleted(db):
    """Scoping protects untouched events, not untouched rows."""
    events = [event(1168)]
    db.stored[("event_registration", "1168:99999")] = "gone-from-csuite"
    db.stored[("event_registration", "1022:41001")] = "archived-backfill"

    result = mirror.refresh_type(
        "event_registration",
        client=client_for(events, {1168: [registrant(21004)]}), pace_ms=0)

    assert result.deleted == 1
    _, params = db.deletes[0]
    assert params[1] == ("1168:99999",)
    assert result.notes["protected_from_delete"] == 1


def test_a_failed_sweep_writes_nothing_and_keeps_the_backfill(db):
    db.stored[("event_registration", "1022:41001")] = "archived-backfill"
    result = mirror.refresh_type(
        "event_registration", client=client_for(fail_on=1429), pace_ms=0)

    assert result.status == "failed"
    assert db.upserts == [] and db.deletes == []
    assert "event/display/eventdate failed for event date 1429" in result.error


# ---------------------------------------------------------------------------
# 6. Backfill resumability
# ---------------------------------------------------------------------------

def test_backfill_skips_event_dates_that_already_have_rows(db):
    db.registered_event_dates = {"1168", "1022"}
    client = client_for()

    result = mirror.refresh_type("event_registration", client=client,
                                 pace_ms=0, options={"backfill": True})

    read = sorted(d["event_date_id"] for e, d in client.calls
                  if e == "event/display/eventdate")
    assert read == [1033, 1429, 1500]
    assert result.notes["skipped_already_backfilled"] == 2
    assert result.notes["mode"] == "backfill"


def test_force_re_reads_everything(db):
    db.registered_event_dates = {"1168", "1022", "1033", "1429", "1500"}
    client = client_for()

    result = mirror.refresh_type(
        "event_registration", client=client, pace_ms=0,
        options={"backfill": True, "force": True})

    read = [d["event_date_id"] for e, d in client.calls
            if e == "event/display/eventdate"]
    assert len(read) == 5
    assert result.notes["skipped_already_backfilled"] == 0


def test_max_events_caps_one_invocation(db):
    client = client_for()
    result = mirror.refresh_type(
        "event_registration", client=client, pace_ms=0,
        options={"backfill": True, "max_events": 2})

    read = [d["event_date_id"] for e, d in client.calls
            if e == "event/display/eventdate"]
    assert read == [1168, 1429]
    assert result.notes["event_dates_selected"] == 2


def test_two_capped_invocations_cover_the_catalogue(db):
    """Backfill may span invocations: each run picks up where the last
    left off, because 'done' is read from the rows themselves."""
    client = client_for()
    mirror.refresh_type("event_registration", client=client, pace_ms=0,
                        options={"backfill": True, "max_events": 2})
    done_after_first = {r["csuite_id"].split(":")[0] for r in rows_of(db)}
    assert done_after_first == {"1168", "1429"}

    db.registered_event_dates = done_after_first
    client2 = client_for()
    mirror.refresh_type("event_registration", client=client2, pace_ms=0,
                        options={"backfill": True, "max_events": 2})

    read = [d["event_date_id"] for e, d in client2.calls
            if e == "event/display/eventdate"]
    assert read == [1022, 1033], "the next two, not the first two again"


def test_backfill_reads_progress_from_the_mirror(db):
    mirror.refresh_type("event_registration", client=client_for(), pace_ms=0,
                        options={"backfill": True})
    progress = db.matching("SELECT DISTINCT split_part(csuite_id")
    assert progress, "resumability must consult the existing rows"
    sql, _ = progress[0]
    assert "record_type = 'event_registration'" in sql


def test_nightly_does_not_query_backfill_progress(db):
    mirror.refresh_type("event_registration", client=client_for(), pace_ms=0)
    assert db.matching("SELECT DISTINCT split_part(csuite_id") == []


# ---------------------------------------------------------------------------
# 7. pulled_at, hashing and cost
# ---------------------------------------------------------------------------

def test_pulled_at_is_recorded_but_excluded_from_the_change_hash():
    """Hashing pulled_at would make every row 'changed' every night and
    defeat the unchanged-skip the mirror is built on."""
    a = mirror.registration_record(registrant(1), event(1168),
                                   "2026-09-23T06:00:00+00:00")
    b = mirror.registration_record(registrant(1), event(1168),
                                   "2026-09-24T06:00:00+00:00")

    assert a["pulled_at"] != b["pulled_at"]
    assert mirror._hash(a, "event_registration") == \
        mirror._hash(b, "event_registration")
    # Other types keep hashing everything.
    assert mirror._hash(a) != mirror._hash(b)


def test_a_genuine_change_still_registers():
    a = mirror.registration_record(registrant(1, rsvp=None), event(1168), "t")
    b = mirror.registration_record(registrant(1, rsvp=1), event(1168), "t")
    assert mirror._hash(a, "event_registration") != \
        mirror._hash(b, "event_registration")


def test_unchanged_registrants_are_not_rewritten(db):
    client = client_for([event(1168)], {1168: [registrant(21004)]})
    record = mirror.registration_record(registrant(21004), event(1168), "x")
    db.stored[("event_registration", "1168:21004")] = \
        mirror._hash(record, "event_registration")

    result = mirror.refresh_type("event_registration", client=client,
                                 pace_ms=0)

    assert result.unchanged == 1 and result.written == 0


def test_nightly_costs_one_call_per_live_event_date_plus_the_listing(db):
    client = client_for()
    result = mirror.refresh_type("event_registration", client=client,
                                 pace_ms=0)
    # event/list/dates takes no view_offset (probe #3 C11), so the
    # listing is ONE call — then one per non-archived date.
    assert result.calls == 4
    assert len([c for c in client.calls
                if c[0] == "event/display/eventdate"]) == 3


def test_event_registration_is_a_record_type():
    assert "event_registration" in mirror.RECORD_TYPES
    assert "event_registration" in mirror.GATHERERS
    assert "event_registration" not in mirror.TTL_HOURS, (
        "an archived registrant list is final, not stale — expiring it "
        "would mark deliberately frozen data as untrustworthy")


def test_event_rows_are_not_duplicated_by_this_type(db):
    """record_type 'event' already carries all event dates."""
    mirror.refresh_type("event_registration", client=client_for(), pace_ms=0)
    assert {r["record_type"] for r in db.upserted_rows()} == \
        {"event_registration"}
