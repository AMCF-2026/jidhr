"""Registrations: phase 1 previews and writes nothing.

Measured against production 2026-10-07 before any of this was written:

* `event/display/eventdate` is the only endpoint carrying registrants — one
  call per event date.
* A registrant row is {attended, event_profile_email, event_profile_name,
  guests, profile_id, rsvp}. No ticket, order or payment reference, so no
  money field can be synced from here even by accident.
* **No cancelled state exists.** A cancellation is a row that stops being
  returned, indistinguishable from a short read.
* `attended` is set on NONE of the 113 rows across the eleven mapped events,
  so ATTENDED has nothing to send.
* 113 rows dedupe to 109 (person, event) registrations from 100 distinct
  people: 83 resolve to one HubSpot contact, 17 to none, 0 to more than one.
* `hs_marketable_status` is read-only to the API, so this sync cannot make
  anyone marketing. 7 of the matched contacts are deliberately
  non-marketing and this never touches the field.

The 83 + 20 = 103 that prompted the reconciliation was an arithmetic error
of mine, not duplicate data: "new" had been computed as inputs minus
CANONICAL emails, which counted 3 secondary-address matches as new. Keyed on
canonical plus hs_additional_emails it is 83 + 17 = 100.

No network, no database.
"""

import json
from datetime import datetime, timezone

import pytest

from intents import sync_commands
from sync import registrations as reg


def registrant(email, profile_id=1, rsvp=None, attended=None, guests=None):
    row = {"event_profile_email": email, "event_profile_name": "A Person",
           "profile_id": profile_id, "rsvp": rsvp, "attended": attended}
    if guests is not None:
        row["guests"] = guests
    return row


class CSuite:
    """event/display/eventdate, and nothing else."""

    def __init__(self, by_event=None, fail_on=()):
        self.by_event = by_event or {}
        self.fail_on = set(str(e) for e in fail_on)
        self.asked = []

    def _request(self, endpoint, data=None):
        assert endpoint == "event/display/eventdate", endpoint
        rid = str((data or {}).get("event_date_id"))
        self.asked.append(rid)
        if rid in self.fail_on:
            return {"success": False, "error": "CSuite returned 500"}
        return {"success": True,
                "data": {"event_date_id": rid,
                         "profiles": list(self.by_event.get(rid, []))}}


class HubSpot:
    """batch/read by email, answering from a canonical/alias map."""

    def __init__(self, contacts=None, error=None):
        # {canonical_email: (contact_id, marketing, [aliases])}
        self.contacts = contacts or {}
        self.error = error
        self.calls = 0

    def _post(self, endpoint, data=None):
        assert endpoint == "crm/v3/objects/contacts/batch/read", endpoint
        self.calls += 1
        if self.error:
            return {"status": "error", "message": self.error}
        wanted = {i["id"] for i in data["inputs"]}
        results = []
        for canonical, (cid, marketing, aliases) in self.contacts.items():
            if wanted & ({canonical} | set(aliases)):
                results.append({"id": cid, "properties": {
                    "email": canonical,
                    "hs_additional_emails": ";".join(aliases),
                    "hs_marketable_status": "true" if marketing else "false"}})
        return {"results": results, "numErrors": len(wanted) - len(results)}


@pytest.fixture(autouse=True)
def no_db(monkeypatch):
    """No registration_map, no run_log. Those have their own tests."""
    monkeypatch.setattr(reg, "migration_applied", lambda: False)
    monkeypatch.setattr(reg, "load_map", lambda: {})
    monkeypatch.setattr(reg, "open_run", lambda applied: None)
    monkeypatch.setattr(reg, "close_run", lambda *a, **k: None)


def arm(monkeypatch, enabled):
    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", enabled)


# ---------------------------------------------------------------------------
# The flag, and the fact there is no write path
# ---------------------------------------------------------------------------

def test_the_flag_is_off_by_default():
    import config

    assert config.Config.REGISTRATIONS_SYNC_ENABLED is False


@pytest.mark.parametrize("raw,expected", [
    ("true", True), ("TRUE", True), (" true ", True),
    ("false", False), ("1", False), ("yes", False), ("", False),
])
def test_only_the_exact_word_true_enables_it(monkeypatch, raw, expected):
    monkeypatch.setenv("REGISTRATIONS_SYNC_ENABLED", raw)
    import importlib

    import config
    importlib.reload(config)
    try:
        assert config.Config.REGISTRATIONS_SYNC_ENABLED is expected
    finally:
        monkeypatch.delenv("REGISTRATIONS_SYNC_ENABLED", raising=False)
        importlib.reload(config)


def test_a_live_run_is_refused_while_the_flag_is_off(monkeypatch):
    arm(monkeypatch, False)

    with pytest.raises(reg.RegistrationsSyncDisabled) as caught:
        reg.run(csuite=object(), hubspot=object(), dry_run=False)

    assert "REGISTRATIONS_SYNC_ENABLED" in str(caught.value)
    assert "nothing was read" in str(caught.value)


def test_a_live_run_needs_a_limit(monkeypatch):
    """There is no "all of them". 92 registrations are planned; a run that
    writes all 92 because nobody typed a number is one whose blast radius
    was set by omission."""
    arm(monkeypatch, True)

    with pytest.raises(reg.LimitRequired) as caught:
        reg.run(csuite=object(), hubspot=object(), dry_run=False, limit=None)

    assert "needs a limit" in str(caught.value)
    assert "no phrase" in str(caught.value)


def test_a_live_run_needs_the_registration_map(monkeypatch):
    """Without it a successful write could not be recorded, and the next run
    would send it again."""
    arm(monkeypatch, True)
    monkeypatch.setattr(reg, "migration_applied", lambda: False)

    with pytest.raises(reg.RegistrationWriteStopped) as caught:
        reg.run(csuite=CSuite(), hubspot=HubSpot(), dry_run=False, limit=1)

    assert "005_registration_map.sql" in str(caught.value)
    assert "Nothing was written" in str(caught.value)


def test_a_live_run_reads_nothing_before_refusing_on_the_flag(monkeypatch):
    arm(monkeypatch, False)
    csuite = CSuite()

    with pytest.raises(reg.RegistrationsSyncDisabled):
        reg.run(csuite=csuite, hubspot=HubSpot(), dry_run=False, limit=1)

    assert csuite.asked == []


def test_writes_happen_only_through_the_gated_path():
    """Item 4's replacement for "no write verbs". There is exactly one place
    a HubSpot write can be issued, it is reached only from _apply, and
    _apply is reached only from a non-dry run."""
    import inspect

    module = inspect.getsource(reg)
    assert module.count("_send_with_status(") == 1, \
        "more than one place can issue a write"
    assert "_send_with_status(" in inspect.getsource(reg.write_registration)

    apply_source = inspect.getsource(reg._apply)
    assert "write_registration(" in apply_source

    body = inspect.getsource(reg._run_body)
    assert "if dry_run:" in body and "return out" in body
    assert "_apply(" in body


def test_the_only_state_ever_written_is_REGISTERED():
    """Never ATTENDED — `attended` is null on all 113 rows. Never CANCELLED
    — CSuite has no such state."""
    import inspect

    writer = inspect.getsource(reg.write_registration)
    assert "REGISTERED" in writer
    assert "ATTENDED" not in writer
    assert "CANCELLED" not in writer

    applier = inspect.getsource(reg._apply)
    for forbidden in ("ATTENDED", "CANCELLED", "NO_SHOW"):
        assert forbidden not in applier, forbidden


def test_the_path_segment_is_the_verb_not_the_state():
    """write_audit 78, production: POST .../REGISTERED/create returned
    HTTP 400 "Unknown state for 'REGISTERED'. Correct are 'register',
    'attend' or 'cancel'." The 405 probes could not catch it — the path
    does not validate the segment until POST, so every spelling looked
    equally real, including 'BOGUS'."""
    assert reg._ATTENDANCE_PATH == (
        "marketing/v3/marketing-events/attendance/"
        "{external_event_id}/{verb}/create")
    assert reg._PATH_VERBS == {"REGISTERED": "register"}
    assert reg.REGISTERED == "REGISTERED", "the internal name is unchanged"


def test_only_register_can_be_reached():
    """An allowlist of one, not a mapping of three: a dict with 'attend' and
    'cancel' in it is an invitation to pass a variable."""
    assert list(reg._PATH_VERBS.values()) == ["register"]
    for forbidden in ("attend", "cancel", "no_show"):
        assert forbidden not in reg._PATH_VERBS.values()

    import inspect
    # The verb is chosen in attendance_request, which builds the whole
    # request; write_registration only sends what it is handed.
    builder = inspect.getsource(reg.attendance_request)
    assert "_PATH_VERBS" in builder
    writer = inspect.getsource(reg.write_registration)
    assert "attendance_request(" in writer

    # Docstrings excluded: the builder's docstring QUOTES the documented
    # values of subscriberState, which is the point of having it there.
    for func in (reg.write_registration, reg.attendance_request):
        code = inspect.getsource(func).replace(func.__doc__ or "\0", "")
        for forbidden in ('"attend"', "'attend'", '"cancel"', "'cancel'"):
            assert forbidden not in code, f"{func.__name__} can reach it"


# HubSpot's documented shape for this endpoint, written out here as
# LITERALS. The point of these tests is to check the request against the
# documentation, not against sync.registrations' own template — a test that
# formats _ATTENDANCE_PATH and compares it to _ATTENDANCE_PATH only proves
# the module agrees with itself, which is exactly what passed while
# write_audit 78 and 79 were both being rejected in production.
#
# POST /marketing/v3/marketing-events/attendance/{externalEventId}/
#      {subscriberState}/create
#
#   externalEventId     path    required
#   subscriberState     path    required
#   externalAccountId   query   "in: query", "style: form"
#   inputs              body    required, array of MarketingEventSubscriber
#     vid                       required   int64
#     interactionDateTime       required   int64, unix milliseconds
#     properties                required   object of string -> string
#
# The spec marks externalAccountId "required: false"; production answers a
# 400 "externalAccountId is required" without it (write_audit 79). The API
# is taken over the spec.
DOCUMENTED_PATH = ("marketing/v3/marketing-events/attendance/"
                   "csuite-1462/register/create")
DOCUMENTED_QUERY_KEYS = {"externalAccountId"}
DOCUMENTED_SUBSCRIBER_KEYS = {"vid", "interactionDateTime", "properties"}


def _captured_request():
    """The complete request write_registration actually issues."""
    sent = {}

    class Seam:
        def _send_with_status(self, method, endpoint, data=None):
            sent.update(method=method, endpoint=endpoint, body=data)
            return {}, 200

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(reg, "_latest_audit_id", lambda endpoint: 99)
        reg.write_registration(Seam(), "csuite-1462", "543954422478",
                               1796000000000)
    return sent


def test_the_complete_request_matches_the_documented_shape():
    """Path, query string and body keys, all three, against the docs."""
    from urllib.parse import urlsplit, parse_qs

    sent = _captured_request()
    assert sent["method"] == "POST"

    parts = urlsplit(sent["endpoint"])
    assert parts.path == DOCUMENTED_PATH
    assert set(parse_qs(parts.query)) == DOCUMENTED_QUERY_KEYS
    assert set(sent["body"]) == {"inputs"}
    assert len(sent["body"]["inputs"]) == 1
    assert set(sent["body"]["inputs"][0]) == DOCUMENTED_SUBSCRIBER_KEYS


def test_the_query_string_carries_the_external_account_id():
    """write_audit 79, production: HTTP 400 "externalAccountId is required".
    The audit row's endpoint held no query string at all, because
    _send_with_status was called with a bare path and has no params
    argument."""
    from urllib.parse import urlsplit, parse_qs
    from sync.event_hubspot import EXTERNAL_ACCOUNT_ID

    sent = _captured_request()
    query = parse_qs(urlsplit(sent["endpoint"]).query)
    assert query["externalAccountId"] == ["jidhr-amcf"]
    assert query["externalAccountId"] == [EXTERNAL_ACCOUNT_ID], \
        "one externalAccountId for the whole repo, not a second literal"
    assert "externalAccountId" not in sent["body"], "query, not body"
    assert "externalAccountId" not in sent["body"]["inputs"][0]


def test_every_documented_required_field_is_sent():
    """Item 4 of the brief, as a test: the required list from the spec's
    MarketingEventSubscriber is interactionDateTime, properties and vid.
    `properties` was NOT sent by write_audit 79 — the 400 never reached it,
    because the query string is validated first."""
    sent = _captured_request()
    record = sent["body"]["inputs"][0]
    for field in ("vid", "interactionDateTime", "properties"):
        assert field in record, f"the docs require {field}"
    assert record["vid"] == 543954422478, "int64, not a string"
    assert isinstance(record["vid"], int)
    assert record["interactionDateTime"] == 1796000000000
    assert isinstance(record["interactionDateTime"], int)


def test_properties_is_sent_empty_and_sets_no_field_values():
    """The schema requires the key; this sync asserts attendance, not field
    values, so the map is empty."""
    sent = _captured_request()
    assert sent["body"]["inputs"][0]["properties"] == {}


def test_the_audit_row_records_the_query_string_that_was_sent():
    """79 could not be diagnosed from its own audit row: the endpoint column
    held the path only. The endpoint handed to _send_with_status is the
    string the audit stores, so the query string has to be in it."""
    sent = _captured_request()
    assert "?" in sent["endpoint"]
    assert "externalAccountId=jidhr-amcf" in sent["endpoint"]


def test_the_audit_lookup_matches_the_endpoint_including_the_query():
    """_latest_audit_id matches `endpoint = %s` exactly, so the string used
    for the lookup must be the one that was sent — query string and all."""
    looked_up = []
    sent = {}

    class Seam:
        def _send_with_status(self, method, endpoint, data=None):
            sent["endpoint"] = endpoint
            return {}, 200

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(reg, "_latest_audit_id",
                      lambda endpoint: looked_up.append(endpoint) or 99)
        reg.write_registration(Seam(), "csuite-1462", "1", 1796000000000)

    assert looked_up == [sent["endpoint"]]


def test_the_attendance_post_is_an_audited_write():
    """It must not be classified as a read-shaped POST — and adding the
    query string must not change that classification."""
    from clients.hubspot import is_hubspot_write

    endpoint, _body = reg.attendance_request("csuite-1462", "1", 1)
    assert "?" in endpoint
    assert is_hubspot_write("POST", endpoint) is True


# ---------------------------------------------------------------------------
# Dedupe, guests, and the units
# ---------------------------------------------------------------------------

def test_dedupe_is_by_normalised_email():
    rows = [registrant("A@X.Inv"), registrant("a@x.inv"),
            registrant(" a@x.inv ")]

    deduped, dropped = reg.dedupe(rows)

    assert list(deduped) == ["a@x.inv"]
    assert dropped == 2


def test_a_registrant_with_no_email_is_not_a_registration():
    deduped, dropped = reg.dedupe([registrant(None), registrant("")])

    assert deduped == {}
    assert dropped == 0


def test_guests_are_excluded():
    """A guest is a different person on someone else's row, with a different
    field name, who has not given this address to AMCF."""
    rows = [registrant("host@x.inv", guests=[
        {"contact_email": "guest@x.inv", "contact_name": "G", "rsvp": 1}])]

    deduped, _dropped = reg.dedupe(rows)

    assert list(deduped) == ["host@x.inv"]
    assert "guest@x.inv" not in deduped


def test_the_same_person_at_two_events_is_two_registrations(monkeypatch):
    """The unit is (person, event). Reporting one number for both units is
    how 83 + 20 came to be read against 100."""
    csuite = CSuite({"1": [registrant("a@x.inv")],
                     "2": [registrant("a@x.inv")]})
    hubspot = HubSpot({"a@x.inv": ("701", True, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1", "2"))

    assert out["unique_emails"] == 2, "two registrations, one person"
    assert out["would_register"] == 2
    assert hubspot.calls == 1, "one batch/read for both events"


# ---------------------------------------------------------------------------
# Contact resolution, including the alias case
# ---------------------------------------------------------------------------

def test_a_registrant_with_a_contact_would_be_registered():
    csuite = CSuite({"1": [registrant("a@x.inv", profile_id=99, rsvp=1)]})
    hubspot = HubSpot({"a@x.inv": ("701", True, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",))

    assert out["would_register"] == 1
    record = out["events"][0]["would_register"][0]
    assert record["hubspot_contact_id"] == "701"
    assert record["csuite_profile_id"] == "99"
    assert record["rsvp"] == "1"
    assert record["marketing"] is True


def test_a_secondary_address_still_resolves():
    """3 of the 83 matches are this case. Keyed on canonical only they were
    counted as new, which is where 83 + 20 = 103 came from."""
    csuite = CSuite({"1": [registrant("old@x.inv")]})
    hubspot = HubSpot({"new@x.inv": ("701", True, ["old@x.inv"])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",))

    assert out["would_register"] == 1
    assert out["withheld"] == 0


def test_a_registrant_with_no_contact_is_withheld_not_created():
    csuite = CSuite({"1": [registrant("nobody@x.inv")]})

    out = reg.run(csuite=csuite, hubspot=HubSpot({}), event_ids=("1",))

    assert out["withheld"] == 1
    assert out["would_register"] == 0
    why = out["events"][0]["withheld"][0]["why"]
    assert "no HubSpot contact" in why
    assert "not created" in why


def test_a_non_marketing_contact_is_registered_and_counted(monkeypatch):
    """Registered, and hs_marketable_status never touched — it is read-only
    to the API, so this sync could not change it even if asked."""
    csuite = CSuite({"1": [registrant("a@x.inv")]})
    hubspot = HubSpot({"a@x.inv": ("701", False, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",))

    assert out["would_register"] == 1
    assert out["non_marketing"] == 1
    assert out["events"][0]["would_register"][0]["marketing"] is False


def test_an_unreadable_contact_index_refuses_to_plan():
    """Without it every registrant looks new, and every new registrant looks
    like a contact to create."""
    csuite = CSuite({"1": [registrant("a@x.inv")]})

    out = reg.run(csuite=csuite, hubspot=HubSpot(error="403 forbidden"),
                  event_ids=("1",))

    assert "could not be read" in out["error"]
    assert out["would_register"] == 0
    assert out["events"] == []


# ---------------------------------------------------------------------------
# One bad event does not become an event with no registrants
# ---------------------------------------------------------------------------

def test_a_failed_event_read_is_review_not_zero_registrants():
    csuite = CSuite({"1": [registrant("a@x.inv")], "2": []}, fail_on=["2"])
    hubspot = HubSpot({"a@x.inv": ("701", True, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1", "2"))

    assert out["events_read"] == 1, "only one event was actually read"
    assert out["review"] == 1
    assert "the registrant read failed" in out["review_rows"][0][1]
    assert out["would_register"] == 1, "the good event still planned"


# ---------------------------------------------------------------------------
# The cancellation guard: refuses, never infers
# ---------------------------------------------------------------------------

def test_an_empty_list_against_known_rows_is_refused():
    """Zero registrants and a failed read are the same response from
    CSuite, so nothing is inferred from zero."""
    why = reg.shrink_guard("1", [], known_count=12)

    assert why and "came back EMPTY" in why
    assert "nothing is inferred" in why


def test_an_empty_list_with_no_known_rows_is_fine():
    """An event nobody has registered for is not a failure."""
    assert reg.shrink_guard("1", [], known_count=0) is None


def test_a_sharp_drop_is_refused():
    why = reg.shrink_guard("1", [registrant(f"{i}@x.inv") for i in range(2)],
                           known_count=12)

    assert why and "fell from 12 to 2" in why
    assert "not as 10 cancellations" in why


def test_a_small_drop_is_allowed_through():
    """A drop from 12 to 11 is a cancellation; the guard is for collapses."""
    rows = [registrant(f"{i}@x.inv") for i in range(11)]

    assert reg.shrink_guard("1", rows, known_count=12) is None


def test_growth_is_never_refused():
    rows = [registrant(f"{i}@x.inv") for i in range(20)]

    assert reg.shrink_guard("1", rows, known_count=12) is None


def test_a_guarded_event_plans_nothing_at_all(monkeypatch):
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {
        ("1", f"{i}@x.inv"): {"last_state": "REGISTERED"} for i in range(12)})
    csuite = CSuite({"1": []})

    out = reg.run(csuite=csuite, hubspot=HubSpot({}), event_ids=("1",))

    assert out["review"] == 1
    assert out["would_register"] == 0
    assert out["withheld"] == 0


def test_phase_1_asserts_no_cancellations_at_all():
    """The guard exists from the start, but nothing uses it to cancel: there
    is no CANCELLED anywhere in the module."""
    import inspect

    source = inspect.getsource(reg)
    assert "CANCELLED" in source, "the state is named in the docstring"
    assert source.count("'CANCELLED'") == 0
    assert '"CANCELLED"' not in source.replace(
        "('REGISTERED', 'ATTENDED', 'CANCELLED')", "")


def test_only_REGISTERED_is_ever_planned():
    assert reg.REGISTERED == "REGISTERED"
    import inspect
    source = inspect.getsource(reg.plan_event)
    assert "ATTENDED" not in source


# ---------------------------------------------------------------------------
# A record already registered is not registered again
# ---------------------------------------------------------------------------

def test_a_known_registration_is_not_sent_again(monkeypatch):
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {
        ("1", "a@x.inv"): {"last_state": "REGISTERED", "status": "synced"}})
    csuite = CSuite({"1": [registrant("a@x.inv")]})
    hubspot = HubSpot({"a@x.inv": ("701", True, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",))

    assert out["already"] == 1
    assert out["would_register"] == 0


# ---------------------------------------------------------------------------
# run_log.outcomes carries the inputs, hashed
# ---------------------------------------------------------------------------

def test_the_outcome_rows_carry_the_per_record_inputs():
    csuite = CSuite({"1": [registrant("a@x.inv", profile_id=99, rsvp=1)],
                     "2": [registrant("nobody@x.inv")]})
    hubspot = HubSpot({"a@x.inv": ("701", True, [])})
    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1", "2"))

    rows = reg._outcome_rows(out)
    assert len(rows) == 2
    kinds = {r["outcome"] for r in rows}
    assert kinds == {"would_register", "withheld"}
    for row in rows:
        assert row["event_date_id"] in ("1", "2")
        assert row["email_sha1"]


def test_no_email_address_reaches_the_run_log():
    """clients/audit.payload_meta draws the same line: an id is the point of
    an audit trail, a value is not."""
    csuite = CSuite({"1": [registrant("secret@donor.invalid")]})
    hubspot = HubSpot({"secret@donor.invalid": ("701", True, [])})
    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",))

    blob = json.dumps(reg._outcome_rows(out))
    assert "secret@donor.invalid" not in blob
    assert "donor.invalid" not in blob
    assert "secret" not in blob


def test_the_fingerprint_is_stable_and_normalised():
    assert reg.email_fingerprint("A@X.Inv") == reg.email_fingerprint("a@x.inv")
    assert reg.email_fingerprint(" a@x.inv ") == reg.email_fingerprint("a@x.inv")
    assert reg.email_fingerprint("a@x.inv") != reg.email_fingerprint("b@x.inv")
    assert len(reg.email_fingerprint("a@x.inv")) == 12


def test_a_review_event_appears_in_the_outcome_rows(monkeypatch):
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {
        ("1", f"{i}@x.inv"): {"last_state": "REGISTERED"} for i in range(12)})
    out = reg.run(csuite=CSuite({"1": []}), hubspot=HubSpot({}),
                  event_ids=("1",))

    rows = reg._outcome_rows(out)
    assert [r["outcome"] for r in rows] == ["review"]
    assert "EMPTY" in rows[0]["why"]


def test_the_run_log_row_is_closed_even_when_the_run_dies(monkeypatch):
    closed = {}
    monkeypatch.setattr(reg, "open_run", lambda applied: 7)
    monkeypatch.setattr(reg, "close_run",
                        lambda rid, status, counts, outcomes=None,
                        error_summary=None: closed.update(
                            id=rid, status=status))
    monkeypatch.setattr(reg, "read_registrants",
                        lambda c, e: (_ for _ in ()).throw(
                            RuntimeError("nobody predicted this")))

    with pytest.raises(RuntimeError):
        reg.run(csuite=CSuite(), hubspot=HubSpot(), event_ids=("1",))

    assert closed["id"] == 7


# ---------------------------------------------------------------------------
# Scope and the chat surface
# ---------------------------------------------------------------------------

def test_phase_one_is_the_eleven_mapped_events():
    assert reg.PHASE_ONE_EVENT_IDS == (
        "1153", "1155", "1157", "1159", "1168", "1429", "1430", "1462",
        "1463", "1464", "1466")
    assert len(reg.PHASE_ONE_EVENT_IDS) == 11


def test_the_scope_is_not_discovered_at_runtime():
    """A sync that decides for itself which events to touch is one whose
    blast radius changes without anyone editing it."""
    import inspect

    signature = inspect.signature(reg.run)
    assert signature.parameters["event_ids"].default is reg.PHASE_ONE_EVENT_IDS


def test_chat_can_ask_for_the_preview():
    assert sync_commands.can_handle("sync registrations dry run")
    assert sync_commands.can_handle("sync registrations")


def test_sync_all_cannot_reach_the_registrations_sync():
    """Phase 1 writes nothing, but the day it does, three words should not
    be what starts it."""
    for phrase in sync_commands.ALL_SYNC_PHRASES:
        assert "registration" not in phrase
    for phrase in sync_commands.REGISTRATION_SYNC_PHRASES:
        assert phrase not in sync_commands.ALL_SYNC_PHRASES


def test_a_plain_request_is_refused_not_run(monkeypatch):
    """"sync registrations" without "dry run" asks for a live run."""
    monkeypatch.setattr(
        "sync.registrations.run",
        lambda **kw: (_ for _ in ()).throw(
            reg.RegistrationsSyncDisabled("the flag is off")))

    reply = sync_commands.handle("sync registrations", None)

    assert "⏸️" in reply
    assert "turned off" in reply
    assert "sync registrations dry run" in reply


def test_the_preview_reports_in_the_right_units():
    reply = sync_commands._format_registration_results({
        "dry_run": True,
        "events_read": 11, "registrant_rows": 113, "unique_emails": 109,
        "duplicates_dropped": 3, "would_register": 92, "withheld": 17,
        "already": 0, "review": 0, "review_rows": [], "non_marketing": 7,
        "csuite_calls": 11, "hubspot_calls": 1, "migration_applied": False,
        "run_logged": True, "run_id": 7, "error": None})

    assert "**113** registrant rows" in reply
    assert "**109** registrations after dedupe by email within each event" in reply
    assert "**92** would be sent as REGISTERED" in reply
    assert "**17** withheld" in reply
    assert "**7** of those contacts are deliberately NON-marketing" in reply
    assert "0 HubSpot writes — nothing was sent" in reply
    assert "005_registration_map.sql" in reply
    assert "hashed addresses, never addresses" in reply


def test_the_preview_reports_a_refusal_as_the_whole_reply():
    reply = sync_commands._format_registration_results(
        {"error": "HubSpot contacts could not be read (403)"})

    assert reply.startswith("❌ **Registrations preview stopped.**")
    assert "registrant rows" not in reply


# ---------------------------------------------------------------------------
# The migration
# ---------------------------------------------------------------------------

def test_the_migration_is_additive_and_not_executed():
    sql = open("migrations/005_registration_map.sql").read()

    assert "CREATE TABLE IF NOT EXISTS hubsync.registration_map" in sql
    assert "UNIQUE (csuite_eventdate_id, contact_email)" in sql
    assert "NOT EXECUTED" in sql
    for forbidden in ("DROP TABLE", "DELETE FROM", "ALTER TABLE public"):
        assert forbidden not in sql


def test_nothing_executes_the_migration():
    import subprocess

    found = subprocess.run(
        ["grep", "-rn", "005_registration_map", "--include=*.py", "."],
        capture_output=True, text=True).stdout
    for line in found.strip().split("\n"):
        if line:
            assert "execute_query" not in line, line


# ---------------------------------------------------------------------------
# The applied report
# ---------------------------------------------------------------------------

def applied_report(**overrides):
    base = {"dry_run": False, "limit": 1, "events_read": 11,
            "registrant_rows": 113, "unique_emails": 109,
            "duplicates_dropped": 3, "would_register": 92, "withheld": 17,
            "already": 0, "review": 0, "review_rows": [], "non_marketing": 7,
            "csuite_calls": 12, "hubspot_calls": 3, "migration_applied": True,
            "run_logged": True, "run_id": 8, "error": None, "stopped": None,
            "registered": 1, "failed": 0, "deferred": 91,
            "writes_attempted": 1, "write_audit_ids": [71],
            "interaction_assumed": True}
    base.update(overrides)
    return base


def test_the_applied_report_states_writes_and_audit_ids():
    reply = sync_commands._format_registration_results(applied_report())

    assert "✅ **Registrations — APPLIED**" in reply
    assert "**1** registered and verified in HubSpot" in reply
    assert "**91** deferred — the limit of 1 was reached" in reply
    assert "1 HubSpot write(s) attempted, 1 verified" in reply
    assert "(write_audit 71)" in reply


def test_the_applied_report_flags_the_assumed_timestamp():
    reply = sync_commands._format_registration_results(applied_report())

    assert "`interactionDateTime` is the EVENT START" in reply
    assert "an assumption, not a measurement" in reply


def test_a_stopped_run_says_where_it_got_to():
    reply = sync_commands._format_registration_results(applied_report(
        registered=0, writes_attempted=1, write_audit_ids=[71],
        # The real reason _apply raises, which already ends with what was
        # and was not written — the duplication came from the report adding
        # a second sentence saying the same thing.
        stopped="the write for event 1462 returned 2xx but could not be "
                "verified: HubSpot reports no participation for this event "
                "after the write. Nothing further was written."))

    assert "🛑 **Registrations — STOPPED**" in reply, "not ✅ APPLIED"
    assert "🛑 **Stopped:**" in reply
    assert "could not be verified" in reply
    assert reply.count("Nothing further was written") == 1, \
        "it used to be printed twice"
    assert "1 HubSpot write(s) attempted, 0 verified" in reply


def test_chat_defaults_a_live_run_to_a_limit_of_one():
    assert sync_commands._registration_limit("sync registrations apply") == 1


@pytest.mark.parametrize("phrase,expected", [
    ("sync registrations apply", 1),
    ("sync registrations apply limit 5", 5),
    ("sync registrations apply limit 0", 0),
    ("sync registrations apply no limit", None),
    ("sync registrations apply unlimited", None),
])
def test_chat_reads_the_limit(phrase, expected):
    assert sync_commands._registration_limit(phrase) == expected


def test_no_limit_is_read_but_then_refused(monkeypatch):
    """"no limit" parses to None, and None is what the sync refuses. The
    phrase exists so the refusal can name it."""
    monkeypatch.setattr(
        "sync.registrations.run",
        lambda **kw: (_ for _ in ()).throw(reg.LimitRequired("needs a limit")))

    reply = sync_commands.handle("sync registrations apply no limit", None)

    assert "🛑 **No limit, no run.**" in reply


def test_a_plain_preview_passes_no_limit_at_all(monkeypatch):
    seen = {}
    monkeypatch.setattr("sync.registrations.run",
                        lambda **kw: seen.update(kw) or {"dry_run": True,
                                                         "error": None})
    sync_commands.handle("sync registrations dry run", None)

    assert seen["dry_run"] is True
    assert seen["limit"] is None, "a preview is not capped"


def test_apply_is_the_word_that_writes(monkeypatch):
    seen = {}
    monkeypatch.setattr("sync.registrations.run",
                        lambda **kw: seen.update(kw) or {"dry_run": False,
                                                         "error": None})
    sync_commands.handle("sync registrations apply", None)

    assert seen["dry_run"] is False
    assert seen["limit"] == 1


# ---------------------------------------------------------------------------
# The read-back
# ---------------------------------------------------------------------------

class Breakdown:
    def __init__(self, response):
        self.response = response
        self.asked = []

    def _get(self, endpoint, params=None):
        self.asked.append(endpoint)
        return self.response


def test_a_confirmed_registration_returns_none():
    hub = Breakdown({"total": 1, "results": [
        {"contactId": "701", "state": "REGISTERED"}]})

    assert reg.confirm_registered(hub, "864022788822", "701") is None


def test_an_empty_breakdown_is_a_mismatch():
    """A 2xx says HubSpot accepted the request, not that it recorded it."""
    why = reg.confirm_registered(Breakdown({"total": 0, "results": []}),
                                 "864022788822", "701")

    assert why and "no participation" in why
    assert "accepted but not recorded" in why


def test_another_contact_is_not_this_contact():
    why = reg.confirm_registered(Breakdown({"total": 1, "results": [
        {"contactId": "999", "state": "REGISTERED"}]}),
        "864022788822", "701")

    assert why and "none for contact 701" in why


def test_the_wrong_state_is_a_mismatch():
    why = reg.confirm_registered(Breakdown({"total": 1, "results": [
        {"contactId": "701", "state": "CANCELLED"}]}),
        "864022788822", "701")

    assert why and "not as REGISTERED" in why


def test_an_unrecognised_shape_is_unverified_not_confirmed():
    """The populated shape has never been observed — every event had total=0
    when this was written. An unknown shape must not read as success."""
    why = reg.confirm_registered(Breakdown({"total": 1}),
                                 "864022788822", "701")

    assert why and "no results list" in why


def test_a_failed_read_back_is_a_mismatch():
    why = reg.confirm_registered(
        Breakdown({"status": "error", "message": "403"}),
        "864022788822", "701")

    assert why and "read-back failed" in why


def test_the_read_back_is_by_object_id():
    hub = Breakdown({"total": 1, "results": [{"contactId": "701",
                                              "state": "REGISTERED"}]})
    reg.confirm_registered(hub, "864022788822", "701")

    assert hub.asked == ["marketing/v3/marketing-events/participations/"
                         "864022788822/breakdown"]


# ---------------------------------------------------------------------------
# A definite failure records nothing, so the next run retries
# ---------------------------------------------------------------------------

def test_a_definite_failure_writes_no_registration_map_row(monkeypatch):
    """write_audit 78 wrote one anyway — status 'error' AND last_state
    'REGISTERED' — so the record read as already registered and the next run
    would have skipped it. A 400 means nothing landed."""
    arm(monkeypatch, True)
    stored = []
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {})
    monkeypatch.setattr(reg, "open_run", lambda applied: None)
    monkeypatch.setattr(reg, "close_run", lambda *a, **k: None)
    monkeypatch.setattr(reg, "record_registration",
                        lambda record, ext, audit, status, error=None:
                        stored.append(status) or True)
    monkeypatch.setattr(reg, "event_object_ids",
                        lambda h, ids: ({"1": "hs-ev"}, 1, None))
    monkeypatch.setattr(reg, "interaction_timestamp",
                        lambda c, e, cache: (1796000000000, True))
    monkeypatch.setattr(reg, "write_registration",
                        lambda h, ext, cid, when: (
                            78, "HTTP 400: Unknown state", None))

    out = reg.run(csuite=CSuite({"1": [registrant("a@x.inv")]}),
                  hubspot=HubSpot({"a@x.inv": ("701", True, [])}),
                  dry_run=False, limit=1, event_ids=("1",))

    assert stored == [], "a failed write must leave no row"
    assert out["stopped"] and "400" in out["stopped"]
    assert out["registered"] == 0
    assert out["failed"] == 1


def test_an_ambiguous_write_DOES_record_unknown(monkeypatch):
    """The opposite case: it may have landed, and HubSpot has no idempotency
    key here, so a retry is how one registration becomes two."""
    arm(monkeypatch, True)
    stored = []
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {})
    monkeypatch.setattr(reg, "open_run", lambda applied: None)
    monkeypatch.setattr(reg, "close_run", lambda *a, **k: None)
    monkeypatch.setattr(reg, "record_registration",
                        lambda record, ext, audit, status, error=None:
                        stored.append(status) or True)
    monkeypatch.setattr(reg, "event_object_ids",
                        lambda h, ids: ({"1": "hs-ev"}, 1, None))
    monkeypatch.setattr(reg, "interaction_timestamp",
                        lambda c, e, cache: (1796000000000, True))
    monkeypatch.setattr(reg, "write_registration",
                        lambda h, ext, cid, when: (
                            78, None, "no HTTP status came back"))

    out = reg.run(csuite=CSuite({"1": [registrant("a@x.inv")]}),
                  hubspot=HubSpot({"a@x.inv": ("701", True, [])}),
                  dry_run=False, limit=1, event_ids=("1",))

    assert stored == ["unknown"]
    assert out["stopped"] and "ambiguous" in out["stopped"]


def test_a_failed_record_is_not_treated_as_already_registered(monkeypatch):
    """The second half of write_audit 78: 'already' now needs status synced
    as well as last_state REGISTERED."""
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {
        ("1", "a@x.inv"): {"last_state": "REGISTERED", "status": "error"}})

    out = reg.run(csuite=CSuite({"1": [registrant("a@x.inv")]}),
                  hubspot=HubSpot({"a@x.inv": ("701", True, [])}),
                  event_ids=("1",))

    assert out["already"] == 0, "a failed write is not a completed one"
    assert out["would_register"] == 1


@pytest.mark.parametrize("status,already", [
    ("synced", 1), ("error", 0), ("review", 0), ("unknown", 0),
    ("pending", 0), (None, 0),
])
def test_only_a_synced_row_counts_as_already_registered(monkeypatch, status,
                                                        already):
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {
        ("1", "a@x.inv"): {"last_state": "REGISTERED", "status": status}})

    out = reg.run(csuite=CSuite({"1": [registrant("a@x.inv")]}),
                  hubspot=HubSpot({"a@x.inv": ("701", True, [])}),
                  event_ids=("1",))

    assert out["already"] == already


# ---------------------------------------------------------------------------
# Deterministic send order
# ---------------------------------------------------------------------------

def test_events_are_sent_in_numeric_event_order():
    """1153 before 1462, not after it as strings would sort."""
    csuite = CSuite({"1462": [registrant("a@x.inv")],
                     "1153": [registrant("b@x.inv")],
                     "999": [registrant("c@x.inv")]})
    hubspot = HubSpot({"a@x.inv": ("1", True, []), "b@x.inv": ("2", True, []),
                       "c@x.inv": ("3", True, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot,
                  event_ids=("1462", "1153", "999"))

    assert [e["event_date_id"] for e in out["events"]] == \
        ["999", "1153", "1462"]


def test_contacts_are_sent_in_numeric_contact_order():
    csuite = CSuite({"1": [registrant("a@x.inv"), registrant("b@x.inv"),
                           registrant("c@x.inv")]})
    hubspot = HubSpot({"a@x.inv": ("900", True, []),
                       "b@x.inv": ("80", True, []),
                       "c@x.inv": ("1000", True, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",))

    sent = [r["hubspot_contact_id"]
            for r in out["events"][0]["would_register"]]
    assert sent == ["80", "900", "1000"]


def test_the_order_is_stable_across_runs():
    """A limit of 1 has to buy the SAME record every time, or "apply limit 1"
    is a different experiment on each run."""
    def build():
        csuite = CSuite({"2": [registrant("x@x.inv")],
                         "1": [registrant("a@x.inv"), registrant("b@x.inv")]})
        hubspot = HubSpot({"a@x.inv": ("7", True, []),
                           "b@x.inv": ("3", True, []),
                           "x@x.inv": ("9", True, [])})
        return reg.first_sends(reg.run(csuite=csuite, hubspot=hubspot,
                                       event_ids=("2", "1")), count=3)

    assert build() == build()
    assert build()[0] == {"event_date_id": "1", "hubspot_contact_id": "3",
                          "marketing": True}


def test_the_dry_run_names_the_first_three_it_would_send():
    csuite = CSuite({"1": [registrant(f"{i}@x.inv") for i in range(5)]})
    hubspot = HubSpot({f"{i}@x.inv": (str(10 + i), i != 2, [])
                       for i in range(5)})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",))

    assert len(out["first_sends"]) == 3
    for entry in out["first_sends"]:
        assert set(entry) == {"event_date_id", "hubspot_contact_id",
                              "marketing"}
    assert out["first_sends"][0]["hubspot_contact_id"] == "10"


def test_the_preview_prints_the_queue():
    reply = sync_commands._format_registration_results({
        "dry_run": True, "events_read": 1, "registrant_rows": 3,
        "unique_emails": 3, "duplicates_dropped": 0, "would_register": 3,
        "withheld": 0, "already": 0, "review": 0, "review_rows": [],
        "non_marketing": 1, "csuite_calls": 1, "hubspot_calls": 1,
        "migration_applied": True, "run_logged": False, "error": None,
        "first_sends": [
            {"event_date_id": "1430", "hubspot_contact_id": "701",
             "marketing": True},
            {"event_date_id": "1430", "hubspot_contact_id": "702",
             "marketing": False}]})

    assert "The next 2 to be sent" in reply
    assert "| `1430` | `701` | yes |" in reply
    assert "| `1430` | `702` | NO |" in reply
    assert "A limit of 1 sends the first row." in reply


def test_a_live_report_does_not_print_the_queue():
    """It is a preview device. After a run, what was sent is the record."""
    reply = sync_commands._format_registration_results(applied_report(
        first_sends=[{"event_date_id": "1", "hubspot_contact_id": "2",
                      "marketing": True}]))

    assert "to be sent" not in reply


# ---------------------------------------------------------------------------
# The header tells the truth
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("overrides,expected", [
    ({}, "✅ **Registrations — APPLIED**"),
    ({"failed": 1, "registered": 0}, "🛑 **Registrations — STOPPED**"),
    ({"stopped": "it stopped"}, "🛑 **Registrations — STOPPED**"),
    ({"writes_attempted": 2, "registered": 1}, "🛑 **Registrations — STOPPED**"),
])
def test_applied_only_when_every_write_verified(overrides, expected):
    reply = sync_commands._format_registration_results(
        applied_report(**overrides))

    assert expected in reply
    if "STOPPED" in expected:
        assert "APPLIED" not in reply


def test_a_clean_run_says_applied():
    reply = sync_commands._format_registration_results(applied_report(
        writes_attempted=1, registered=1, failed=0, stopped=None))

    assert "✅ **Registrations — APPLIED**" in reply
    assert "STOPPED" not in reply


def test_zero_deferred_never_claims_the_limit_was_reached():
    """"0 deferred — the limit of 1 was reached" said two contradictory
    things: nothing was held back, and the cap stopped it."""
    reply = sync_commands._format_registration_results(applied_report(
        deferred=0, stopped="it stopped", registered=0, failed=1))

    assert "stopped before the limit" in reply
    assert "0** deferred — the limit of 1 was reached" not in reply


def test_a_clean_run_with_nothing_deferred_omits_the_line():
    reply = sync_commands._format_registration_results(applied_report(
        deferred=0, stopped=None, registered=1, writes_attempted=1))

    assert "deferred" not in reply
