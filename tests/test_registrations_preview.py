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

    def __init__(self, by_event=None, fail_on=(), dates=None, names=None):
        self.by_event = by_event or {}
        self.fail_on = set(str(e) for e in fail_on)
        # event_date is what start_moment parses; event_description is what
        # event_title reads. Absent by default, so a test that says nothing
        # about dates gets an event with no usable start, exactly as the 98
        # of 179 production rows with no event_date do.
        self.dates = {str(k): v for k, v in (dates or {}).items()}
        self.names = {str(k): v for k, v in (names or {}).items()}
        self.asked = []

    def _request(self, endpoint, data=None):
        assert endpoint == "event/display/eventdate", endpoint
        rid = str((data or {}).get("event_date_id"))
        self.asked.append(rid)
        if rid in self.fail_on:
            return {"success": False, "error": "CSuite returned 500"}
        row = {"event_date_id": rid,
               "profiles": list(self.by_event.get(rid, []))}
        if rid in self.dates:
            row["event_date"] = self.dates[rid]
        if rid in self.names:
            row["event_description"] = self.names[rid]
        return {"success": True, "data": row}


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


def test_the_only_state_ever_sent_is_a_registration():
    """Registration only. ATTENDED, CANCELLED and NO_SHOW are now states
    this sync READS back — HubSpot records an ended event's registration as
    NO_SHOW by itself — but none of them is ever SENT.

    So the guard is on the request builder, which is the only thing that
    can send anything, rather than on text appearing anywhere in a
    function. Carl's instruction stands: do not start sending attend or
    no-show data.
    """
    import inspect
    from urllib.parse import urlsplit

    url, body = reg.attendance_request("csuite-1155", "701", 1)
    assert urlsplit(url).path.endswith("/register/create")
    assert list(reg._PATH_VERBS.values()) == ["register"]
    # Nothing in the body names a state at all.
    assert set(body["inputs"][0]) == {"vid", "interactionDateTime",
                                      "properties"}

    builder = inspect.getsource(reg.attendance_request)
    code = builder.replace(reg.attendance_request.__doc__ or "\0", "")
    # Quoted forms: "attend" is a substring of attendance_request itself.
    for forbidden in ("ATTENDED", "CANCELLED", "NO_SHOW", '"attend"',
                      "'attend'", '"cancel"', "'cancel'", "/attend/",
                      "/cancel/"):
        assert forbidden not in code, forbidden


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


def test_no_cancellation_is_ever_asserted_to_hubspot():
    """CANCELLED is read, never written. CSuite has no cancelled state — a
    cancellation there is a row that stops being returned — so there is
    nothing to send, and the shrink guard exists instead."""
    import inspect

    # The one place a state could be sent.
    assert reg._PATH_VERBS == {"REGISTERED": "register"}

    # And the only state CANCELLED reaches is a LOCAL one: 'review'.
    applier = inspect.getsource(reg._apply)
    assert "CANCELLED_STATE" in applier
    assert '"review"' in applier
    assert "cancel/create" not in inspect.getsource(reg)
    assert "attend/create" not in inspect.getsource(reg)


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
            "interaction_rules": {"event_start": 1, "run_time": 0}}
    base.update(overrides)
    return base


def test_the_applied_report_states_writes_and_audit_ids():
    reply = sync_commands._format_registration_results(applied_report())

    assert "✅ **Registrations — APPLIED**" in reply
    assert "**1** registered and verified in HubSpot" in reply
    assert "**91** deferred — the limit of 1 was reached" in reply
    assert "1 HubSpot write(s) attempted, 1 verified" in reply
    assert "(write_audit 71)" in reply


def test_the_applied_report_says_which_timestamp_rule_was_used():
    """Item 2: the old note said "is the EVENT START" unconditionally, which
    stopped being true when the clamp went in."""
    reply = sync_commands._format_registration_results(applied_report())

    assert "min(event start, run time)" in reply
    assert "**1** used the EVENT START" in reply
    assert "**0** used the RUN TIME" in reply
    assert "is the EVENT START." not in reply, "the old note is gone"


def test_the_applied_report_counts_each_rule_separately():
    reply = sync_commands._format_registration_results(applied_report(
        interaction_rules={"event_start": 4, "run_time": 7}))

    assert "**4** used the EVENT START" in reply
    assert "**7** used the RUN TIME" in reply


def test_no_rule_note_when_nothing_was_stamped():
    """A run that stopped before any timestamp was chosen has no rule to
    report, and must not print "0 and 0"."""
    reply = sync_commands._format_registration_results(applied_report(
        interaction_rules={"event_start": 0, "run_time": 0}))

    assert "interactionDateTime" not in reply


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
    arm(monkeypatch, True)
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    started = []
    monkeypatch.setattr("sync.registration_jobs.reg.open_run",
                        lambda applied: started.append(applied) or 99)

    reply = sync_commands.handle("sync registrations apply no limit", None)

    assert "🛑 **No limit, no run.**" in reply
    assert started == [], "a refusal starts nothing"


def test_a_plain_preview_passes_no_limit_at_all(monkeypatch):
    seen = {}
    monkeypatch.setattr("sync.registrations.run",
                        lambda **kw: seen.update(kw) or {"dry_run": True,
                                                         "error": None})
    sync_commands.handle("sync registrations dry run", None)

    assert seen["dry_run"] is True
    assert seen["limit"] is None, "a preview is not capped"


def test_apply_is_the_word_that_starts_a_background_run(monkeypatch):
    """The apply no longer runs inside the HTTP request — run_log 30 took
    318s and Railway discards a response after 300."""
    seen = {}
    monkeypatch.setattr("sync.registration_jobs.start_apply",
                        lambda **kw: seen.update(kw) or 42)

    reply = sync_commands.handle("sync registrations apply", None)

    assert seen["limit"] == 1
    assert "run_log 42" in reply
    assert "background" in reply
    assert "Do not send the apply again" in reply


def test_the_background_thread_is_the_thing_that_writes(monkeypatch):
    """And it runs with dry_run False, on the row chat already reported."""
    from sync import registration_jobs as jobs

    seen = {}
    monkeypatch.setattr(jobs.reg, "run", lambda **kw: seen.update(kw) or {})

    jobs._run_apply(42, 3, ("1463",))

    assert seen["dry_run"] is False
    assert seen["limit"] == 3
    assert seen["run_id"] == 42
    assert seen["event_ids"] == ("1463",)


# ---------------------------------------------------------------------------
# The read-back
# ---------------------------------------------------------------------------
#
# The fakes below return HubSpot's DOCUMENTED participation shape, copied
# from a real response for csuite-1463 read on 2026-10-09:
#
#   {"total": 1, "results": [{"id": "869277318854",
#     "properties": {"attendanceState": "REGISTERED",
#                    "occurredAt": 1791571437115,
#                    "attendanceDurationSeconds": null,
#                    "attendancePercentage": null},
#     "associations": {
#       "contact": {"contactId": "543954422478", "email": "...",
#                   "firstname": "...", "lastname": "..."},
#       "marketingEvent": {"marketingEventId": "863952588483",
#                          "name": "...", "externalEventId": "csuite-1463",
#                          "externalAccountId": "jidhr-amcf"}},
#     "createdAt": "2026-10-09T18:43:59.508Z"}]}
#
# The previous fakes answered {"contactId": ..., "state": ...}, which HubSpot
# has never returned. They passed against a code path that searched a JSON
# blob for the id, and they would have passed whatever the real shape was.

EXT = "csuite-1463"


def participation(contact_id="701", state="REGISTERED", external=EXT):
    """One result entry, in the documented shape."""
    return {
        "id": "869277318854",
        "properties": {"attendanceState": state,
                       "occurredAt": 1791571437115,
                       "attendanceDurationSeconds": None,
                       "attendancePercentage": None},
        "associations": {
            "contact": {"contactId": contact_id, "email": "a@x.inv",
                        "firstname": "A", "lastname": "Person"},
            "marketingEvent": {"marketingEventId": "863952588483",
                               "name": "An Event",
                               "externalEventId": external,
                               "externalAccountId": "jidhr-amcf"}},
        "createdAt": "2026-10-09T18:43:59.508Z"}


def breakdown(*entries):
    return {"total": len(entries), "results": list(entries)}


class Breakdown:
    """Answers one response, or a different one per attempt."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.asked = []
        self.params = []

    def _get(self, endpoint, params=None):
        self.asked.append(endpoint)
        self.params.append(params)
        i = min(len(self.asked) - 1, len(self.responses) - 1)
        return self.responses[i]


def confirm(hub, contact_id="701", external=EXT):
    """confirm_registered with the clock stubbed out. The backoff is real
    seconds in production; a test that actually waited ten of them would be
    a test nobody runs."""
    slept = []
    why, state, calls = reg.confirm_registered(hub, external, contact_id,
                                               sleep=slept.append)
    return why, calls, slept, state


def test_a_confirmed_registration_returns_none():
    why, calls, slept, _state = confirm(Breakdown(breakdown(participation())))

    assert why is None
    assert calls == 1, "a confirmed read needs no retry"
    assert slept == [], "and no waiting"


def test_the_read_back_asks_the_documented_endpoint_for_every_state():
    """Keyed on (externalAccountId, externalEventId), filtered server-side
    to this CONTACT but NOT to a state.

    Filtering to state=REGISTERED is the bug this replaces: write_audit
    121's registration landed as NO_SHOW, the filtered read returned
    total=0, and a landed write was reported as a failure.

    limit=100 because `limit` defaults to 10 and a contact can hold more
    than one state on one event — csuite-1157 holds REGISTERED and NO_SHOW
    for each of its 29 contacts."""
    hub = Breakdown(breakdown(participation()))
    confirm(hub)

    assert hub.asked == ["marketing/v3/marketing-events/participations/"
                         "jidhr-amcf/csuite-1463/breakdown"]
    assert hub.params == [{"contactIdentifier": "701", "limit": 100}]
    assert "state" not in hub.params[0]


def test_an_empty_breakdown_is_not_confirmed():
    """A 2xx says HubSpot accepted the request, not that it recorded it."""
    why, calls, slept, _state = confirm(Breakdown(breakdown()))

    assert why and "holds no participation" in why
    assert calls == 3, "three attempts before deciding"
    assert slept == list(reg.VERIFY_BACKOFFS)
    assert "over ~10s" in why


def test_an_empty_first_read_then_present_is_verified():
    """write_audit 80's case: the participation record's createdAt was about
    a second AFTER the read-back had already given up."""
    hub = Breakdown(breakdown(), breakdown(participation()))

    why, calls, slept, _state = confirm(hub)

    assert why is None, "a late participation is still a registration"
    assert calls == 2
    assert slept == [reg.VERIFY_BACKOFFS[0]], "waited once, then found it"


def test_present_only_on_the_third_read_is_verified():
    hub = Breakdown(breakdown(), breakdown(), breakdown(participation()))

    why, calls, _slept, _state = confirm(hub)

    assert why is None
    assert calls == 3


def test_another_contact_is_not_this_contact():
    why, _calls, _slept, _state = confirm(
        Breakdown(breakdown(participation(contact_id="999"))))

    assert why and "contact 701" in why
    assert "holds no participation" in why


def test_a_participation_on_another_event_does_not_count():
    """The filter is server-side, but the association is checked anyway —
    an endpoint that stopped filtering must not read as success."""
    why, _calls, _slept, _state = confirm(
        Breakdown(breakdown(participation(external="csuite-9999"))))

    assert why and "holds no participation" in why


def test_cancelled_is_not_confirmed_and_is_not_retried():
    """Carl's rule: CANCELLED is NOT verified. It is also not retried —
    absence might change in a second, a state HubSpot has definitely
    recorded will not."""
    hub = Breakdown(breakdown(participation(state="CANCELLED")))

    why, calls, slept, state = confirm(hub)

    assert why and "CANCELLED" in why
    assert "was cancelled" in why
    assert state == reg.CANCELLED_STATE
    assert calls == 1, "no point waiting for a decided state"
    assert slept == []


@pytest.mark.parametrize("state", ["REGISTERED", "ATTENDED", "NO_SHOW"])
def test_every_landed_state_is_verified(state):
    """write_audit 121: a register POST on csuite-1155, an event that ended
    2026-04-10, got a 201 and HubSpot stored the participation as NO_SHOW.
    All three landed states mean the registration exists."""
    why, calls, _slept, seen = confirm(
        Breakdown(breakdown(participation(state=state))))

    assert why is None, f"{state} means the registration landed"
    assert seen == state, "and the state seen is what gets stored"
    assert calls == 1


def test_the_most_recent_landed_state_wins():
    """csuite-1157 holds REGISTERED and NO_SHOW for the same contact — 58
    participations for 29 people. The portal shows the later one."""
    early = participation(state="REGISTERED")
    early["createdAt"] = "2026-10-09T19:20:11.000Z"
    late = participation(state="NO_SHOW")
    late["createdAt"] = "2026-10-09T21:00:00.000Z"

    why, _calls, _slept, state = confirm(Breakdown(breakdown(early, late)))

    assert why is None
    assert state == "NO_SHOW"


def test_a_landed_state_outweighs_a_cancellation():
    """A stale CANCELLED alongside a live registration must not block it —
    the registration demonstrably exists."""
    why, _calls, _slept, state = confirm(Breakdown(breakdown(
        participation(state="CANCELLED"), participation(state="NO_SHOW"))))

    assert why is None
    assert state == "NO_SHOW"


def test_an_undocumented_state_is_never_treated_as_landed():
    """The four documented values and nothing else. A state this sync has
    not seen must not be silently accepted as a registration."""
    why, _calls, _slept, state = confirm(
        Breakdown(breakdown(participation(state="WAITLISTED"))))

    assert why and "does not recognise" in why
    assert "WAITLISTED" in why
    assert state is None


def test_the_documented_states_are_the_four_hubspot_names():
    """HubSpot enumerates them itself when given a bad one: "State value
    should be one of REGISTERED, CANCELLED, ATTENDED, NO_SHOW" (measured
    2026-10-09)."""
    assert set(reg.DOCUMENTED_STATES) == {"REGISTERED", "CANCELLED",
                                          "ATTENDED", "NO_SHOW"}
    assert set(reg.LANDED_STATES) == {"REGISTERED", "ATTENDED", "NO_SHOW"}
    assert reg.CANCELLED_STATE == "CANCELLED"
    assert reg.CANCELLED_STATE not in reg.LANDED_STATES


def test_an_unrecognised_shape_is_unverified_not_confirmed():
    """An unknown shape must not read as success."""
    why, calls, _slept, _state = confirm(Breakdown({"total": 1}))

    assert why and "no results list" in why
    assert calls == 3, "retried, in case it was a transient shape"


def test_a_failed_read_back_is_not_confirmed():
    why, _calls, _slept, _state = confirm(
        Breakdown({"status": "error", "message": "403"}))

    assert why and "read-back failed" in why


def test_a_read_error_then_a_hit_is_verified():
    """A 429 on the first read is not evidence the write failed."""
    hub = Breakdown({"status": "error", "category": "RATE_LIMIT"},
                    breakdown(participation()))

    why, calls, _slept, _state = confirm(hub)

    assert why is None
    assert calls == 2


def test_registered_in_portal_separates_absence_from_a_failed_read():
    """None is "no evidence", False is "evidence of absence". Collapsing
    them is how a 403 became "the write did not land"."""
    found, _why = reg.registered_in_portal(
        Breakdown(breakdown(participation())), EXT, "701")
    assert found is True

    found, _why = reg.registered_in_portal(Breakdown(breakdown()), EXT, "701")
    assert found is False

    found, _why = reg.registered_in_portal(
        Breakdown({"status": "error", "message": "403"}), EXT, "701")
    assert found is None


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
                        lambda record, ext, audit, status, error=None,
                        last_state=None:
                        stored.append(status) or (True, None))
    monkeypatch.setattr(reg, "event_object_ids",
                        lambda h, ids: ({"1": "hs-ev"}, 1, None))
    monkeypatch.setattr(reg, "interaction_timestamp",
                        lambda c, e, cache, now: (
                            1796000000000, reg.INTERACTION_EVENT_START))
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
                        lambda record, ext, audit, status, error=None,
                        last_state=None:
                        stored.append(status) or (True, None))
    monkeypatch.setattr(reg, "event_object_ids",
                        lambda h, ids: ({"1": "hs-ev"}, 1, None))
    monkeypatch.setattr(reg, "interaction_timestamp",
                        lambda c, e, cache, now: (
                            1796000000000, reg.INTERACTION_EVENT_START))
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
    first = build()[0]
    assert first["event_date_id"] == "1"
    assert first["hubspot_contact_id"] == "3"
    assert first["marketing"] is True
    # No csuite handed to first_sends, so the event detail stays blank
    # rather than being guessed at.
    assert first["event_name"] is None
    assert first["interaction_at"] is None


def test_the_dry_run_names_the_first_three_it_would_send():
    csuite = CSuite({"1": [registrant(f"{i}@x.inv") for i in range(5)]})
    hubspot = HubSpot({f"{i}@x.inv": (str(10 + i), i != 2, [])
                       for i in range(5)})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",))

    assert len(out["first_sends"]) == 3
    for entry in out["first_sends"]:
        assert set(entry) == {"event_date_id", "hubspot_contact_id",
                              "marketing", "event_name", "event_start_ms",
                              "interaction_at", "interaction_rule"}
    assert out["first_sends"][0]["hubspot_contact_id"] == "10"


def test_the_preview_prints_the_queue():
    reply = sync_commands._format_registration_results({
        "dry_run": True, "events_read": 1, "registrant_rows": 3,
        "unique_emails": 3, "duplicates_dropped": 0, "would_register": 3,
        "withheld": 0, "already": 0, "review": 0, "review_rows": [],
        "non_marketing": 1, "csuite_calls": 1, "hubspot_calls": 1,
        "migration_applied": True, "run_logged": False, "error": None,
        "interaction_rules": {"event_start": 1, "run_time": 1},
        "first_sends": [
            {"event_date_id": "1430", "hubspot_contact_id": "701",
             "marketing": True, "event_name": "Community Iftar",
             "event_start_ms": 1760000000000,
             "interaction_at": 1760000000000,
             "interaction_rule": "event_start"},
            {"event_date_id": "1466", "hubspot_contact_id": "702",
             "marketing": False, "event_name": "Winter Fundraiser",
             "event_start_ms": 1796000000000,
             "interaction_at": 1760500000000,
             "interaction_rule": "run_time"}]})

    assert "The next 2 to be sent" in reply
    # Item 3: name, start and the interactionDateTime the write would use.
    assert "| `1430` | Community Iftar | 2025-10-09 08:53 UTC | `701` | " \
           "yes | 2025-10-09 08:53 UTC | event start |" in reply
    assert "| `1466` | Winter Fundraiser | 2026-11-30 00:53 UTC | `702` | " \
           "NO | 2025-10-15 03:46 UTC | run time (event is in the " \
           "future) |" in reply
    assert "A limit of 1 sends the first row." in reply
    assert "of the 2 shown" in reply


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


# ---------------------------------------------------------------------------
# interactionDateTime = min(event start, run time)
# ---------------------------------------------------------------------------
#
# Six of the eleven phase-1 events start in the FUTURE (measured 2026-10-08:
# 1153 is 2026-12-31, and 1168, 1429, 1463, 1464 and 1466 follow). The
# event's own start would therefore have asserted a registration that has
# not happened yet — on 1153, the very first record a limit of 1 sends.
#
# The clock is pinned in every one of these. This suite has broken twice on
# clock rollover, and a test for "never later than the run time" that reads
# the real clock is a test that changes meaning every second.

NOW_MS = 1760000000000          # 2025-10-09 08:53 UTC


def test_a_past_event_keeps_its_own_start():
    """Unchanged behaviour: the start is already in the past, so it IS the
    min, and it stays the nearest available fact."""
    csuite = CSuite(dates={"1": "2025-01-01"})

    when, rule = reg.interaction_timestamp(csuite, "1", {}, NOW_MS)

    assert rule == reg.INTERACTION_EVENT_START
    assert when < NOW_MS
    # Midnight ET, because CSuite carries no start_time for this one and
    # start_moment will not invent a clock time.
    assert datetime.fromtimestamp(when / 1000, timezone.utc) == \
        datetime(2025, 1, 1, 5, 0, tzinfo=timezone.utc)


def test_a_future_event_gets_the_run_time():
    """Event 1153's case: 2026-12-31, read on a day in 2025."""
    csuite = CSuite(dates={"1": "2026-12-31"})

    when, rule = reg.interaction_timestamp(csuite, "1", {}, NOW_MS)

    assert rule == reg.INTERACTION_RUN_TIME
    assert when == NOW_MS


@pytest.mark.parametrize("day", ["2020-02-29", "2024-01-01", "2025-10-09",
                                 "2025-10-10", "2026-12-31", "2030-06-15"])
def test_the_value_is_never_later_than_the_run_time(day):
    csuite = CSuite(dates={"1": day})

    when, rule = reg.interaction_timestamp(csuite, "1", {}, NOW_MS)

    assert when <= NOW_MS, "a registration cannot have happened in the future"
    assert rule in (reg.INTERACTION_EVENT_START, reg.INTERACTION_RUN_TIME)


def test_an_event_starting_exactly_now_counts_as_past():
    """The boundary belongs to the start: min(x, x) is x, and calling it the
    run time would report a rule that changed nothing."""
    csuite = CSuite(dates={"1": "2025-01-01"})
    start = reg.event_detail(csuite, "1", {})["start_ms"]

    when, rule = reg.interaction_timestamp(csuite, "1", {}, start)

    assert when == start
    assert rule == reg.INTERACTION_EVENT_START


def test_an_event_with_no_date_has_no_timestamp_and_no_rule():
    """98 of 179 production rows have no event_date. There is no min to
    take, and _apply stops rather than guessing."""
    csuite = CSuite(dates={})

    when, rule = reg.interaction_timestamp(csuite, "1", {}, NOW_MS)

    assert when is None
    assert rule is None


def test_the_cache_holds_the_event_start_not_the_clamped_value():
    """The clamp depends on when the run happened; the event does not."""
    csuite = CSuite(dates={"1": "2026-12-31"})
    cache = {}

    reg.interaction_timestamp(csuite, "1", cache, NOW_MS)
    assert cache["1"]["start_ms"] > NOW_MS, "the start, not the run time"

    # Same cache, a clock past the event: now the start is the min.
    when, rule = reg.interaction_timestamp(csuite, "1", cache, 1830000000000)
    assert rule == reg.INTERACTION_EVENT_START
    assert when == cache["1"]["start_ms"]
    assert csuite.asked == ["1"], "one CSuite call per event, not per record"


def test_the_event_name_comes_from_the_same_call_as_the_start():
    csuite = CSuite(dates={"1": "2026-12-31"},
                    names={"1": "New Year Community Dinner"})

    detail = reg.event_detail(csuite, "1", {})

    assert detail["name"] == "New Year Community Dinner"
    assert detail["start_ms"]
    assert csuite.asked == ["1"]


def test_the_preview_table_carries_the_name_start_and_timestamp():
    """Item 3, end to end: the dry run reads the events in the head of the
    queue so the value a write WOULD use is inspectable first."""
    csuite = CSuite({"1": [registrant("a@x.inv")]},
                    dates={"1": "2026-12-31"}, names={"1": "Winter Dinner"})
    hubspot = HubSpot({"a@x.inv": ("701", True, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",),
                  now_ms=NOW_MS)

    row = out["first_sends"][0]
    assert row["event_name"] == "Winter Dinner"
    assert row["event_start_ms"] > NOW_MS
    assert row["interaction_at"] == NOW_MS
    assert row["interaction_rule"] == reg.INTERACTION_RUN_TIME
    assert out["interaction_rules"] == {"event_start": 0, "run_time": 1}


def test_the_run_time_is_one_moment_for_the_whole_run():
    """Read once in run(), not per record: two records stamped a second
    apart would be two different claims about the same sync."""
    import inspect

    assert "now_ms" in inspect.signature(reg.run).parameters
    body = inspect.getsource(reg._apply)
    assert "datetime.now" not in body, "the clock is not read per record"
    assert "now_ms" in body


def live_run(monkeypatch, csuite, hubspot, writer=None, **kwargs):
    """A successful applied run, with the write seam and the read-back
    faked. Everything these doubles stand in for has its own test."""
    arm(monkeypatch, True)
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {})
    monkeypatch.setattr(reg, "open_run", lambda applied: None)
    monkeypatch.setattr(reg, "close_run", lambda *a, **k: None)
    monkeypatch.setattr(reg, "record_registration",
                        lambda *a, **k: (True, None))
    monkeypatch.setattr(reg, "event_object_ids",
                        lambda h, ids: ({"1": "hs-ev"}, 1, None))
    monkeypatch.setattr(reg, "write_registration",
                        writer or (lambda h, ext, cid, when: (80, None, None)))
    # (reason, read calls) — None means verified.
    monkeypatch.setattr(reg, "confirm_registered",
                        lambda h, ext, cid, sleep=None: (None, "REGISTERED", 1))
    return reg.run(csuite=csuite, hubspot=hubspot, dry_run=False, limit=1,
                   event_ids=("1",), **kwargs)


def test_the_outcome_rows_say_which_rule_each_record_used(monkeypatch):
    """Item 1: "min(event start, run time)" read back from a log does not
    say which half of the min a given write used."""
    out = live_run(monkeypatch,
                   CSuite({"1": [registrant("a@x.inv")]},
                          dates={"1": "2026-12-31"}),
                   HubSpot({"a@x.inv": ("701", True, [])}),
                   now_ms=NOW_MS)

    assert out["registered"] == 1
    rows = [r for r in reg._outcome_rows(out) if r["outcome"] == "registered"]
    assert len(rows) == 1
    assert rows[0]["interaction_rule"] == reg.INTERACTION_RUN_TIME
    assert rows[0]["interaction_at"] == NOW_MS


def test_a_past_event_is_logged_as_the_event_start(monkeypatch):
    out = live_run(monkeypatch,
                   CSuite({"1": [registrant("a@x.inv")]},
                          dates={"1": "2025-01-01"}),
                   HubSpot({"a@x.inv": ("701", True, [])}),
                   now_ms=NOW_MS)

    rows = [r for r in reg._outcome_rows(out) if r["outcome"] == "registered"]
    assert rows[0]["interaction_rule"] == reg.INTERACTION_EVENT_START
    assert rows[0]["interaction_at"] < NOW_MS
    assert out["interaction_rules"] == {"event_start": 1, "run_time": 0}


def test_the_rule_is_in_the_logged_whitelist_not_leaking_the_email():
    """The whitelist gained two fields; it must not have gained a third."""
    assert "interaction_rule" in reg._LOGGED_FIELDS
    assert "interaction_at" in reg._LOGGED_FIELDS
    assert "contact_email" not in reg._LOGGED_FIELDS


def test_the_value_sent_to_hubspot_is_the_clamped_one(monkeypatch):
    """The clamp is worthless if _apply computes it and then sends the raw
    start anyway."""
    sent = []
    out = live_run(monkeypatch,
                   CSuite({"1": [registrant("a@x.inv")]},
                          dates={"1": "2026-12-31"}),
                   HubSpot({"a@x.inv": ("701", True, [])}),
                   writer=lambda h, ext, cid, when:
                       sent.append(when) or (80, None, None),
                   now_ms=NOW_MS)

    assert out["registered"] == 1
    assert sent == [NOW_MS], "the future start must not reach HubSpot"


# ---------------------------------------------------------------------------
# The hold list, and the event filter
# ---------------------------------------------------------------------------
#
# 1153 is held. Read from CSuite 2026-10-08, it is the only one of the eleven
# mapped events with event_name 'Newsletters' and event_type_code 'marketing'
# (the other ten are 'Event - Other' / 'event'); it has no start_time, no
# location, no tickets and no fund, and event_date 2026-12-31. It also sorts
# first, so it was what "apply limit 1" would have sent.
#
# The hold is a LIST IN CONFIG, not a branch in here: these tests set the
# list, they never patch a code path.


def hold(monkeypatch, *event_ids):
    monkeypatch.setattr("config.Config.REGISTRATION_HELD_EVENT_IDS",
                        tuple(event_ids))


def test_the_hold_list_comes_from_config_and_holds_1153_by_default():
    import config

    assert "1153" in config.Config.REGISTRATION_HELD_EVENT_IDS
    assert "1153" in reg.held_event_ids()


def test_a_held_event_is_never_sent(monkeypatch):
    hold(monkeypatch, "1")
    csuite = CSuite({"1": [registrant("a@x.inv"), registrant("b@x.inv")]},
                    dates={"1": "2025-01-01"})
    hubspot = HubSpot({"a@x.inv": ("701", True, []),
                       "b@x.inv": ("702", True, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",),
                  now_ms=NOW_MS)

    assert out["would_register"] == 0, "held records are not sendable"
    assert out["held"] == 2
    assert out["held_events"] == ["1"]
    assert out["first_sends"] == [], "nothing is queued for a held event"


def test_held_records_are_not_counted_in_to_register(monkeypatch):
    """The whole point: a held event must not inflate the number a human
    reads as "about to be written"."""
    hold(monkeypatch, "2")
    csuite = CSuite({"1": [registrant("a@x.inv")],
                     "2": [registrant("b@x.inv")]},
                    dates={"1": "2025-01-01", "2": "2025-01-01"})
    hubspot = HubSpot({"a@x.inv": ("701", True, []),
                       "b@x.inv": ("702", True, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1", "2"),
                  now_ms=NOW_MS)

    assert out["would_register"] == 1
    assert out["held"] == 1
    assert [r["event_date_id"] for r in out["first_sends"]] == ["1"]


def test_releasing_an_event_makes_it_sendable(monkeypatch):
    """Removing it from the list is the whole release procedure — no code
    change, which is what a config list buys."""
    csuite = CSuite({"1": [registrant("a@x.inv")]}, dates={"1": "2025-01-01"})
    hubspot = HubSpot({"a@x.inv": ("701", True, [])})

    hold(monkeypatch, "1")
    held_run = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",),
                       now_ms=NOW_MS)
    assert held_run["would_register"] == 0 and held_run["held"] == 1

    hold(monkeypatch)                 # the list is now empty
    free_run = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",),
                       now_ms=NOW_MS)
    assert free_run["would_register"] == 1
    assert free_run["held"] == 0
    assert free_run["held_events"] == []


def test_a_held_event_still_reports_its_registrant_rows(monkeypatch):
    """Held is not hidden. The rows are worth seeing — that is how anyone
    decides whether to release it."""
    hold(monkeypatch, "1")
    out = reg.run(csuite=CSuite({"1": [registrant("a@x.inv")]},
                                dates={"1": "2025-01-01"}),
                  hubspot=HubSpot({"a@x.inv": ("701", True, [])}),
                  event_ids=("1",), now_ms=NOW_MS)

    assert out["registrant_rows"] == 1
    assert out["events_read"] == 1
    rows = [r for r in reg._outcome_rows(out) if r["outcome"] == "held"]
    assert len(rows) == 1
    assert rows[0]["why"] == reg.HELD_REASON
    assert "contact_email" not in rows[0]


def test_a_held_event_is_not_written_even_on_a_live_run(monkeypatch):
    """Defence at the sync layer, not only at the command layer: a caller
    that hands run() a held id directly still sends nothing."""
    hold(monkeypatch, "1")
    sent = []
    out = live_run(monkeypatch,
                   CSuite({"1": [registrant("a@x.inv")]},
                          dates={"1": "2025-01-01"}),
                   HubSpot({"a@x.inv": ("701", True, [])}),
                   writer=lambda h, ext, cid, when:
                       sent.append(cid) or (80, None, None),
                   now_ms=NOW_MS)

    assert sent == [], "a held event reached the write seam"
    assert out["registered"] == 0
    assert out["writes_attempted"] == 0
    assert out["held"] == 1


# --- the event filter -------------------------------------------------------

def test_the_command_parses_the_event_id():
    for text, expected in (
            ("sync registrations dry run event 1463", "1463"),
            ("sync registrations apply event 1463", "1463"),
            ("sync registrations apply event 1463 limit 1", "1463"),
            ("sync registrations dry run", None)):
        assert sync_commands._registration_event(text) == expected


def test_the_event_filter_runs_only_that_event(monkeypatch):
    seen = {}
    monkeypatch.setattr(sync_commands, "_format_registration_results",
                        lambda results: "ok")
    monkeypatch.setattr(reg, "run",
                        lambda **kwargs: seen.update(kwargs) or {})

    assert sync_commands._sync_registrations(
        "sync registrations dry run event 1463") == "ok"
    assert seen["event_ids"] == ("1463",)
    assert seen["dry_run"] is True


def test_the_event_filter_reads_only_that_event():
    """End to end at the sync layer: one CSuite registrant call, for one
    event, not eleven."""
    csuite = CSuite({"1463": [registrant("a@x.inv")]},
                    dates={"1463": "2025-01-01"})
    out = reg.run(csuite=csuite, hubspot=HubSpot({"a@x.inv": ("7", True, [])}),
                  event_ids=("1463",), now_ms=NOW_MS)

    assert out["events_read"] == 1
    assert set(csuite.asked) == {"1463"}
    assert [e["event_date_id"] for e in out["events"]] == ["1463"]


def test_an_unmapped_event_id_is_refused_by_name():
    with pytest.raises(reg.EventRefused) as refused:
        reg.resolve_requested_event("9999")

    assert "9999" in str(refused.value)
    assert "not one of the 11 mapped events" in str(refused.value)


def test_a_held_event_id_is_refused_by_name(monkeypatch):
    hold(monkeypatch, "1463")

    with pytest.raises(reg.EventRefused) as refused:
        reg.resolve_requested_event("1463")

    assert "1463" in str(refused.value)
    assert reg.HELD_REASON in str(refused.value)
    assert "REGISTRATION_HELD_EVENT_IDS" in str(refused.value)


def test_a_mapped_released_event_resolves(monkeypatch):
    hold(monkeypatch)
    assert reg.resolve_requested_event("1463") == "1463"
    assert reg.resolve_requested_event(" 1463 ") == "1463"


def test_a_refused_event_reads_nothing_and_says_so(monkeypatch):
    """The refusal must come BEFORE any call — "nothing was read" has to be
    true, not reassuring."""
    called = []
    monkeypatch.setattr(reg, "run", lambda **k: called.append(k) or {})

    reply = sync_commands._sync_registrations(
        "sync registrations apply event 1153 limit 1")

    assert called == [], "a refused event must not start a run"
    assert "1153" in reply
    assert reg.HELD_REASON in reply


def test_an_unknown_event_is_refused_at_the_command_layer(monkeypatch):
    called = []
    monkeypatch.setattr(reg, "run", lambda **k: called.append(k) or {})

    reply = sync_commands._sync_registrations(
        "sync registrations dry run event 9999")

    assert called == []
    assert "9999" in reply


def test_a_scoped_apply_still_needs_a_limit():
    """Item 3: narrowing to one event does not lift the cap. 1157 alone has
    41 registrant rows."""
    assert sync_commands._registration_limit(
        "sync registrations apply event 1463") == 1
    assert sync_commands._registration_limit(
        "sync registrations apply event 1463 no limit") is None
    assert sync_commands._registration_limit(
        "sync registrations apply event 1463 unlimited") is None


def test_no_limit_is_still_refused_by_the_sync(monkeypatch):
    arm(monkeypatch, True)
    monkeypatch.setattr(reg, "migration_applied", lambda: True)

    with pytest.raises(reg.LimitRequired):
        reg.run(csuite=CSuite(), hubspot=HubSpot(), dry_run=False, limit=None,
                event_ids=("1463",))


# --- the report -------------------------------------------------------------

def test_the_report_names_the_held_events():
    reply = sync_commands._format_registration_results({
        "dry_run": True, "events_read": 11, "registrant_rows": 115,
        "unique_emails": 111, "duplicates_dropped": 3, "would_register": 81,
        "withheld": 17, "already": 0, "review": 0, "review_rows": [],
        "non_marketing": 7, "csuite_calls": 12, "hubspot_calls": 2,
        "migration_applied": True, "run_logged": False, "error": None,
        "held": 13, "held_events": ["1153"], "first_sends": []})

    assert "**13** held for CSuite setup review" in reply
    assert "`1153`" in reply
    assert "not in the count above" in reply
    assert "**81** would be sent" in reply


def test_the_report_says_nothing_about_holds_when_there_are_none():
    reply = sync_commands._format_registration_results({
        "dry_run": True, "events_read": 1, "registrant_rows": 1,
        "unique_emails": 1, "duplicates_dropped": 0, "would_register": 1,
        "withheld": 0, "already": 0, "review": 0, "review_rows": [],
        "non_marketing": 0, "csuite_calls": 1, "hubspot_calls": 1,
        "migration_applied": True, "run_logged": False, "error": None,
        "held": 0, "held_events": [], "first_sends": []})

    assert "held for CSuite setup review" not in reply


def test_the_report_names_the_scoped_event():
    reply = sync_commands._format_registration_results({
        "dry_run": True, "events_read": 1, "registrant_rows": 1,
        "unique_emails": 1, "duplicates_dropped": 0, "would_register": 1,
        "withheld": 0, "already": 0, "review": 0, "review_rows": [],
        "non_marketing": 0, "csuite_calls": 2, "hubspot_calls": 1,
        "migration_applied": True, "run_logged": False, "error": None,
        "held": 0, "held_events": [], "first_sends": [],
        "scoped_event": "1463"})

    assert "Scoped to event `1463` only" in reply


def test_the_held_wording_is_shared_not_copied():
    """One wording, so the report and the run log can be reconciled."""
    assert sync_commands.reg_held_reason() == reg.HELD_REASON


# ---------------------------------------------------------------------------
# The three write outcomes, end to end through the real read-back
# ---------------------------------------------------------------------------
#
# These drive sync.registrations._apply with the REAL confirm_registered, so
# the retry logic is exercised rather than stubbed. The backoff is set to
# zero seconds — the attempt COUNT is what matters, and a test that waited
# the production ten seconds is a test nobody runs.


class WritingHubSpot(HubSpot):
    """batch/read for contacts, the attendance POST, and the breakdown GET.

    `breakdowns` is answered one per GET, the last one repeating — so a test
    can say "empty, then present".
    """

    def __init__(self, contacts=None, write=(None, 201), breakdowns=()):
        super().__init__(contacts)
        self.write = write
        self.breakdowns = list(breakdowns) or [breakdown()]
        self.gets = []
        self.posted = []

    def _send_with_status(self, method, endpoint, data=None):
        self.posted.append((method, endpoint, data))
        body, status = self.write
        return (body if body is not None else {}), status

    def _get(self, endpoint, params=None):
        self.gets.append((endpoint, params))
        i = min(len(self.gets) - 1, len(self.breakdowns) - 1)
        return self.breakdowns[i]


def outcome_run(monkeypatch, hubspot, **kwargs):
    """A live run with the DB faked and the backoff zeroed."""
    arm(monkeypatch, True)
    stored = []
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {})
    monkeypatch.setattr(reg, "open_run", lambda applied: None)
    monkeypatch.setattr(reg, "close_run", lambda *a, **k: None)
    monkeypatch.setattr(reg, "record_registration",
                        lambda record, ext, audit, status, error=None,
                        last_state=None:
                        stored.append({"status": status, "error": error,
                                       "last_state": last_state,
                                       "contact": record.get(
                                           "hubspot_contact_id")})
                        or (True, None))
    monkeypatch.setattr(reg, "event_object_ids",
                        lambda h, ids: ({"1463": "863952588483"}, 1, None))
    monkeypatch.setattr(reg, "_latest_audit_id", lambda endpoint: 80)
    monkeypatch.setattr(reg, "VERIFY_BACKOFFS", (0.0, 0.0))
    out = reg.run(csuite=CSuite({"1463": [registrant("a@x.inv")]},
                                dates={"1463": "2025-01-01"}),
                  hubspot=hubspot, dry_run=False, limit=1,
                  event_ids=("1463",), now_ms=NOW_MS, **kwargs)
    return out, stored


def test_2xx_then_present_on_retry_is_VERIFIED(monkeypatch):
    """write_audit 80's exact case. The first read is empty because the
    participation index had not caught up; the retry finds it."""
    hub = WritingHubSpot({"a@x.inv": ("543954422478", True, [])},
                         write=(None, 201),
                         breakdowns=[breakdown(),
                                     breakdown(participation(
                                         contact_id="543954422478"))])

    out, stored = outcome_run(monkeypatch, hub)

    assert out["registered"] == 1
    assert out["unverified"] == 0
    assert out["failed"] == 0
    assert out["stopped"] is None
    assert [r["status"] for r in stored] == ["synced"], \
        "a verified write writes a synced map row"
    assert len(hub.gets) == 2, "one empty read, then one that found it"


def test_2xx_never_present_is_UNVERIFIED(monkeypatch):
    """Not APPLIED and not failed. The write got a 2xx, so a resend is how
    one registration becomes two."""
    hub = WritingHubSpot({"a@x.inv": ("543954422478", True, [])},
                         write=(None, 201), breakdowns=[breakdown()])

    out, stored = outcome_run(monkeypatch, hub)

    assert out["registered"] == 0
    assert out["unverified"] == 1
    assert out["failed"] == 0, "a 2xx is not a failure"
    assert out["stopped"] and "could not be verified" in out["stopped"]
    assert "Reconcile it" in out["stopped"]
    assert [r["status"] for r in stored] == ["unverified"], \
        "an unverified write DOES write a row, so it is not resent"
    assert len(hub.gets) == 3, "three attempts before deciding"


def test_a_non_2xx_is_FAILED_with_no_map_row(monkeypatch):
    """Existing behaviour, unchanged: nothing landed, so nothing is
    recorded and the next run sends it again."""
    hub = WritingHubSpot({"a@x.inv": ("543954422478", True, [])},
                         write=({"status": "error",
                                 "message": "externalAccountId is required"},
                                400))

    out, stored = outcome_run(monkeypatch, hub)

    assert out["registered"] == 0
    assert out["unverified"] == 0
    assert out["failed"] == 1
    assert out["stopped"] and "400" in out["stopped"]
    assert stored == [], "a definite failure must leave no row"
    assert hub.gets == [], "and must not even read back"


def test_the_unverified_report_is_neither_applied_nor_failed(monkeypatch):
    hub = WritingHubSpot({"a@x.inv": ("543954422478", True, [])},
                         write=(None, 201), breakdowns=[breakdown()])
    out, _stored = outcome_run(monkeypatch, hub)

    reply = sync_commands._format_registration_results(out)

    assert "UNVERIFIED" in reply
    assert "APPLIED" not in reply
    assert "could NOT be verified" in reply
    assert "reconcile registrations" in reply


# --- a row nobody confirmed is never resent ---------------------------------

@pytest.mark.parametrize("status", ["unverified", "unknown", "review"])
def test_an_unconfirmed_row_is_not_resent(status):
    """'review' is in the list because that is what the code wrote before
    this hotfix — write_audit 80's row. plan_event only treated 'synced' as
    already-registered, so the record WOULD have been sent a second time."""
    known = {("1463", "a@x.inv"): {"last_state": "REGISTERED",
                                   "status": status, "write_audit_id": 80}}

    plan = reg.plan_event("1463", [registrant("a@x.inv")],
                          {"a@x.inv": {"id": "543954422478",
                                       "marketing": True}}, known)

    assert plan["would_register"] == [], f"{status} must not be resent"
    assert len(plan["unverified"]) == 1
    assert "not resent" in plan["unverified"][0]["why"]
    assert "80" in plan["unverified"][0]["why"]


def test_a_synced_row_is_already_registered_and_not_resent():
    """The acceptance condition: once reconcile records the row, the preview
    shows it as already registered and queues nothing."""
    known = {("1463", "a@x.inv"): {"last_state": "REGISTERED",
                                   "status": "synced", "write_audit_id": 80}}

    plan = reg.plan_event("1463", [registrant("a@x.inv")],
                          {"a@x.inv": {"id": "543954422478",
                                       "marketing": True}}, known)

    assert plan["would_register"] == []
    assert len(plan["already"]) == 1
    assert plan["unverified"] == []


def test_an_unverified_row_is_reported_before_any_write(monkeypatch):
    out = reg.run(csuite=CSuite({"1463": [registrant("a@x.inv")]},
                                dates={"1463": "2025-01-01"}),
                  hubspot=HubSpot({"a@x.inv": ("543954422478", True, [])}),
                  event_ids=("1463",), now_ms=NOW_MS)
    assert out["would_register"] == 1

    # The map is only consulted when the table exists.
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {
        ("1463", "a@x.inv"): {"last_state": "REGISTERED",
                              "status": "unverified", "write_audit_id": 80}})
    held_back = reg.run(csuite=CSuite({"1463": [registrant("a@x.inv")]},
                                      dates={"1463": "2025-01-01"}),
                        hubspot=HubSpot({"a@x.inv": ("543954422478", True,
                                                     [])}),
                        event_ids=("1463",), now_ms=NOW_MS)

    assert held_back["would_register"] == 0
    assert held_back["unverified_prior"] == 1
    assert held_back["first_sends"] == []

    reply = sync_commands._format_registration_results(held_back)
    assert "held back from an earlier run" in reply
    assert "reconcile registrations" in reply


# ---------------------------------------------------------------------------
# The landed states, end to end through the real read-back
# ---------------------------------------------------------------------------
#
# write_audit 121, production 2026-10-09: POST register for contact
# 269277574890 on csuite-1155 — an event that ENDED 2026-04-10 — returned
# 201 in 219ms, and HubSpot stored the participation as NO_SHOW. The
# verifier asked for state=REGISTERED, got total=0, and stopped the run as
# a failure. The write had landed.
#
# Past events are synced anyway (Carl's decision): which events and who
# registered matter more than attendance, and no attendance data is sent.


def outcome_run_with(monkeypatch, breakdowns, write=(None, 201),
                     recorder=None, event_date="2025-01-01"):
    """A live run on one event, with the REAL read-back and zero backoff."""
    arm(monkeypatch, True)
    stored = []

    def record(record, ext, audit, status, error=None, last_state=None):
        stored.append({"status": status, "last_state": last_state,
                       "error": error})
        return (True, None) if recorder is None else recorder()

    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {})
    monkeypatch.setattr(reg, "open_run", lambda applied: None)
    monkeypatch.setattr(reg, "close_run", lambda *a, **k: None)
    monkeypatch.setattr(reg, "record_registration", record)
    monkeypatch.setattr(reg, "event_object_ids",
                        lambda h, ids: ({"1155": "749247088350"}, 1, None))
    monkeypatch.setattr(reg, "_latest_audit_id", lambda endpoint: 121)
    monkeypatch.setattr(reg, "VERIFY_BACKOFFS", (0.0, 0.0))

    hub = WritingHubSpot({"a@x.inv": ("269277574890", True, [])},
                         write=write, breakdowns=breakdowns)
    out = reg.run(csuite=CSuite({"1155": [registrant("a@x.inv")]},
                                dates={"1155": event_date}),
                  hubspot=hub, dry_run=False, limit=1,
                  event_ids=("1155",), now_ms=NOW_MS)
    return out, stored, hub


def test_a_no_show_participation_is_VERIFIED_and_stored(monkeypatch):
    """The regression. 2xx + NO_SHOW -> synced, last_state NO_SHOW."""
    out, stored, _hub = outcome_run_with(
        monkeypatch,
        [breakdown(participation(contact_id="269277574890",
                                 state="NO_SHOW", external="csuite-1155"))])

    assert out["registered"] == 1
    assert out["failed"] == 0
    assert out["unverified"] == 0
    assert out["stopped"] is None
    assert stored == [{"status": "synced", "last_state": "NO_SHOW",
                       "error": None}]
    assert out["landed_states"] == {"NO_SHOW": 1}


def test_an_attended_participation_is_VERIFIED(monkeypatch):
    out, stored, _hub = outcome_run_with(
        monkeypatch,
        [breakdown(participation(contact_id="269277574890",
                                 state="ATTENDED", external="csuite-1155"))])

    assert out["registered"] == 1
    assert stored[0]["status"] == "synced"
    assert stored[0]["last_state"] == "ATTENDED"


def test_a_cancelled_participation_is_review_and_stops(monkeypatch):
    """Carl's rule: CANCELLED is NOT verified. The POST landed, but nobody
    is registered — so it is neither a success nor a failure."""
    out, stored, _hub = outcome_run_with(
        monkeypatch,
        [breakdown(participation(contact_id="269277574890",
                                 state="CANCELLED",
                                 external="csuite-1155"))])

    assert out["registered"] == 0
    assert out["cancelled"] == 1
    assert out["failed"] == 0, "the POST did land"
    assert out["stopped"] and "CANCELLED" in out["stopped"]
    assert stored == [{"status": "review", "last_state": "CANCELLED",
                       "error": stored[0]["error"]}]
    assert "cancelled" in stored[0]["error"]


def test_nothing_ever_present_is_UNVERIFIED_and_stops(monkeypatch):
    out, stored, hub = outcome_run_with(monkeypatch, [breakdown()])

    assert out["registered"] == 0
    assert out["unverified"] == 1
    assert out["failed"] == 0
    assert out["stopped"] and "could not be verified" in out["stopped"]
    assert stored[0]["status"] == "unverified"
    assert len(hub.gets) == 3, "three attempts before deciding"


def test_a_map_write_that_raises_stops_the_run_with_the_real_error(
        monkeypatch):
    """run_log 34 said only "registration_map could NOT be updated". The
    real cause was a CheckViolation on registration_map_status_check —
    hotfix-50 began writing status 'unverified' without widening the
    constraint. A report that cannot name its own failure costs a day."""
    out, stored, _hub = outcome_run_with(
        monkeypatch,
        [breakdown(participation(contact_id="269277574890",
                                 state="NO_SHOW", external="csuite-1155"))],
        recorder=lambda: (False, 'CheckViolation: new row for relation '
                                 '"registration_map" violates check '
                                 'constraint '
                                 '"registration_map_status_check"'))

    assert out["stopped"], "the run must still stop"
    assert "CheckViolation" in out["stopped"]
    assert "registration_map_status_check" in out["stopped"]
    assert stored[0]["status"] == "synced"


def test_the_real_exception_is_returned_not_swallowed(monkeypatch):
    """record_registration returns the cause, not just False."""
    class Boom:
        @staticmethod
        def execute_query(sql, params=None, fetch=True):
            raise RuntimeError("relation does not exist")

    monkeypatch.setattr(reg, "database", Boom)

    ok, why = reg.record_registration(
        {"event_date_id": "1155", "contact_email": "a@x.inv",
         "email_sha1": "abc", "hubspot_contact_id": "701"},
        "csuite-1155", 121, "unverified")

    assert ok is False
    assert "RuntimeError" in why
    assert "relation does not exist" in why


def test_the_state_stored_is_the_one_hubspot_holds(monkeypatch):
    """Not the one we asked for. record_registration defaults to REGISTERED
    only when no state is given."""
    captured = {}

    class Capture:
        @staticmethod
        def execute_query(sql, params=None, fetch=True):
            captured["last_state"] = params[6]
            return [{"id": 1}]

    monkeypatch.setattr(reg, "database", Capture)
    record = {"event_date_id": "1155", "contact_email": "a@x.inv",
              "email_sha1": "abc", "hubspot_contact_id": "701"}

    reg.record_registration(record, "csuite-1155", 121, "synced",
                            last_state="NO_SHOW")
    assert captured["last_state"] == "NO_SHOW"

    reg.record_registration(record, "csuite-1155", 121, "synced")
    assert captured["last_state"] == "REGISTERED", "the default is unchanged"


# --- the ended-event flag ---------------------------------------------------

def test_an_ended_event_is_flagged_in_the_preview():
    """Step 4: say it before the run, not after somebody finds no-shows in
    the portal."""
    out = reg.run(csuite=CSuite({"1155": [registrant("a@x.inv")]},
                                dates={"1155": "2025-01-01"}),
                  hubspot=HubSpot({"a@x.inv": ("701", True, [])}),
                  event_ids=("1155",), now_ms=NOW_MS)

    assert out["ended_events"] == [("1155", 1)]
    assert out["ended_records"] == 1

    reply = sync_commands._format_registration_results(out)
    assert "already ENDED" in reply
    assert "no-show" in reply
    assert "`1155` — **1** record(s) will appear as no-shows" in reply


def test_a_future_event_is_not_flagged_as_ended():
    out = reg.run(csuite=CSuite({"1463": [registrant("a@x.inv")]},
                                dates={"1463": "2026-12-31"}),
                  hubspot=HubSpot({"a@x.inv": ("701", True, [])}),
                  event_ids=("1463",), now_ms=NOW_MS)

    assert out["ended_events"] == []
    assert out["ended_records"] == 0
    assert "already ENDED" not in \
        sync_commands._format_registration_results(out)


def test_an_event_with_nothing_to_send_is_not_flagged():
    """The flag is about records that WILL be sent, not about the calendar."""
    out = reg.run(csuite=CSuite({"1155": [registrant("nobody@x.inv")]},
                                dates={"1155": "2025-01-01"}),
                  hubspot=HubSpot({}), event_ids=("1155",), now_ms=NOW_MS)

    assert out["withheld"] == 1
    assert out["ended_events"] == []


def test_the_ended_flag_agrees_with_the_interaction_rule():
    """Both read CSuite's event_date, so a record on an ended event is
    exactly a record whose interactionDateTime rule is event_start."""
    out = reg.run(csuite=CSuite({"1155": [registrant("a@x.inv")]},
                                dates={"1155": "2025-01-01"}),
                  hubspot=HubSpot({"a@x.inv": ("701", True, [])}),
                  event_ids=("1155",), now_ms=NOW_MS)

    assert out["ended_events"] == [("1155", 1)]
    assert out["first_sends"][0]["interaction_rule"] == \
        reg.INTERACTION_EVENT_START


def test_an_event_with_no_date_is_not_called_ended():
    """98 of 179 production rows have no event_date. "Unknown" is not
    "past"."""
    assert reg.event_has_ended({}, NOW_MS) is False
    assert reg.event_has_ended({"event_date": "not a date"}, NOW_MS) is False
    assert reg.event_has_ended({"event_date": "2025-01-01"}, NOW_MS) is True
    assert reg.event_has_ended({"event_date": "2030-01-01"}, NOW_MS) is False
