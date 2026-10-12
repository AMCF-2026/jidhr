"""Creating HubSpot contacts for registrants who do not have one.

Phase 1 withheld a registrant with no HubSpot contact rather than creating
one, because creating a contact is a decision about who AMCF may email.
Carl made that decision; this is the write path for it.

Measured against production 2026-10-12 before any of this was written:

* 17 registrants are withheld across the eleven mapped events; 5 of them
  are on event 1430.
* The registrant row carries only `event_profile_name` — one combined
  field. `profile/display` carries first_name and last_name separately,
  populated on all 5 of 1430's withheld registrants. `organization` and
  `work_name` are empty on all 5, and every one is ptype 'indiv'.
* `hs_marketable_status` carries modificationMetadata.readOnlyValue = true
  in this portal, so the API cannot set marketing status at all. The
  allowlist and the body check exist anyway — a property that cannot be
  set today is not a guarantee about tomorrow.
* HubSpot answers 409 when the email is already on a contact's email or
  additional-email field, and names the existing id in the message.

No network, no database.
"""

import pytest

from intents import sync_commands
from sync import contact_create as cc
from sync import contact_jobs as jobs
from sync import registrations as reg

EMAIL = "new.person@example.invalid"


class CSuite:
    """event/display/eventdate and profile/display."""

    def __init__(self, by_event=None, profiles=None, fail_on=()):
        self.by_event = by_event or {}
        self.profiles = {str(k): v for k, v in (profiles or {}).items()}
        self.fail_on = set(str(e) for e in fail_on)
        self.asked = []

    def _request(self, endpoint, data=None):
        self.asked.append(endpoint)
        if endpoint == "event/display/eventdate":
            rid = str((data or {}).get("event_date_id"))
            if rid in self.fail_on:
                return {"success": False, "error": "CSuite returned 500"}
            return {"success": True,
                    "data": {"event_date_id": rid, "event_date": "2025-01-01",
                             "profiles": list(self.by_event.get(rid, []))}}
        if endpoint == "profile/display":
            pid = str((data or {}).get("profile_id"))
            return {"success": True, "data": self.profiles.get(pid, {})}
        raise AssertionError(f"unexpected endpoint {endpoint}")


class HubSpot:
    """batch/read for the duplicate check, and the contact create."""

    def __init__(self, contacts=None, create=(None, 201), read_error=None,
                 appears_after=None, read_error_after=None):
        self.contacts = dict(contacts or {})
        self.create = create
        self.read_error = read_error
        # The plan reads HubSpot, and so does the check immediately before
        # each create. These let the two answers DIFFER, which is the whole
        # point of re-reading: the plan may be minutes old.
        self.appears_after = appears_after or {}
        self.read_error_after = read_error_after
        self.posted = []
        self.reads = 0

    def _post(self, endpoint, data=None):
        assert endpoint == "crm/v3/objects/contacts/batch/read", endpoint
        self.reads += 1
        if self.read_error:
            return {"status": "error", "message": self.read_error}
        if self.read_error_after is not None and \
                self.reads > self.read_error_after:
            return {"status": "error", "message": "HTTP 429"}
        for email, (cid, after) in self.appears_after.items():
            if self.reads > after:
                self.contacts[email] = cid
        wanted = {i["id"] for i in data["inputs"]}
        results = [{"id": cid, "properties": {"email": email,
                                              "hs_additional_emails": "",
                                              "hs_marketable_status": "true"}}
                   for email, cid in self.contacts.items() if email in wanted]
        return {"results": results, "numErrors": len(wanted) - len(results)}

    def _send_with_status(self, method, endpoint, data=None):
        self.posted.append((method, endpoint, data))
        body, status = self.create
        if body is None and status and 200 <= status < 300:
            body = {"id": "900001"}
        return (body or {}), status


def registrant(email, profile_id=1):
    return {"event_profile_email": email, "event_profile_name": "A Person",
            "profile_id": profile_id, "rsvp": None, "attended": None}


def profile(first="New", last="Person", org=None):
    row = {"first_name": first, "last_name": last,
           "primary_email": EMAIL, "ptype": "indiv"}
    if org:
        row["organization"] = org
    return row


def doubles(contacts=None, create=(None, 201)):
    return (CSuite({"1430": [registrant(EMAIL, 21676)]},
                   profiles={21676: profile()}),
            HubSpot(contacts, create))


@pytest.fixture(autouse=True)
def no_db(monkeypatch):
    monkeypatch.setattr(cc, "open_run", lambda applied: None)
    monkeypatch.setattr(cc, "close_run", lambda *a, **k: None)
    monkeypatch.setattr(reg, "load_map", lambda: {})
    monkeypatch.setattr(reg, "_latest_audit_id", lambda endpoint: 200)


def arm(monkeypatch, on=True):
    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", on)


def applied(monkeypatch, csuite, hubspot, limit=5, **kwargs):
    arm(monkeypatch)
    return cc.run(csuite=csuite, hubspot=hubspot, dry_run=False, limit=limit,
                  event_ids=("1430",), **kwargs)


# ---------------------------------------------------------------------------
# The preview
# ---------------------------------------------------------------------------

def test_a_withheld_registrant_is_planned_for_creation():
    csuite, hubspot = doubles()

    out = cc.run(csuite=csuite, hubspot=hubspot, event_ids=("1430",))

    assert out["to_create"] == 1
    assert out["with_name"] == 1
    assert out["with_company"] == 0
    assert hubspot.posted == [], "a preview creates nothing"


def test_a_registrant_who_already_has_a_contact_is_not_planned():
    csuite, hubspot = doubles(contacts={EMAIL: "701"})

    out = cc.run(csuite=csuite, hubspot=hubspot, event_ids=("1430",))

    assert out["to_create"] == 0


def test_the_names_come_from_the_profile_not_from_splitting_a_string():
    """The registrant row has one combined `event_profile_name`."""
    csuite = CSuite({"1430": [registrant(EMAIL, 21676)]},
                    profiles={21676: profile(first="Aisha", last="Rahman")})

    out = cc.run(csuite=csuite, hubspot=HubSpot(), event_ids=("1430",))

    props = out["events"][0]["to_create"][0]["properties"]
    assert props["firstname"] == "Aisha"
    assert props["lastname"] == "Rahman"
    assert "profile/display" in csuite.asked


def test_a_company_is_sent_only_when_csuite_has_one():
    with_org = CSuite({"1430": [registrant(EMAIL, 21676)]},
                      profiles={21676: profile(org="Example Nonprofit")})
    out = cc.run(csuite=with_org, hubspot=HubSpot(), event_ids=("1430",))
    assert out["events"][0]["to_create"][0]["properties"]["company"] == \
        "Example Nonprofit"
    assert out["with_company"] == 1

    csuite, hubspot = doubles()
    out = cc.run(csuite=csuite, hubspot=hubspot, event_ids=("1430",))
    assert "company" not in out["events"][0]["to_create"][0]["properties"]


def test_a_profile_with_no_name_still_gets_a_contact():
    """An address with nothing else is still a person who registered."""
    csuite = CSuite({"1430": [registrant(EMAIL, 21676)]},
                    profiles={21676: {"ptype": "indiv"}})

    out = cc.run(csuite=csuite, hubspot=HubSpot(), event_ids=("1430",))

    props = out["events"][0]["to_create"][0]["properties"]
    assert props == {"email": EMAIL}
    assert out["with_name"] == 0


# ---------------------------------------------------------------------------
# Never a duplicate
# ---------------------------------------------------------------------------

def test_an_existing_email_found_by_the_pre_create_read_is_not_created(
        monkeypatch):
    """The plan may be minutes old; somebody may have been added since.

    The double answers "not there" on the plan's read and "there" on the
    pre-create read, which is the only way to exercise the second check.
    """
    csuite = CSuite({"1430": [registrant(EMAIL, 21676)]},
                    profiles={21676: profile()})
    hubspot = HubSpot(appears_after={EMAIL: ("701", 1)})

    out = applied(monkeypatch, csuite, hubspot)

    assert hubspot.reads >= 2, "it read again before creating"
    assert hubspot.posted == [], "no create was issued"
    assert out["created"] == 0
    assert out["existing"] == 1
    assert out["writes_attempted"] == 0
    assert out["stopped"] is None, "and the run carries on"
    assert out["events"][0]["existing"][0]["hubspot_contact_id"] == "701"


def test_the_pre_create_read_uses_batch_read_so_aliases_count():
    """A search on `email` alone would miss a registrant whose address is a
    SECONDARY address on an existing contact, and duplicate that person."""
    import inspect

    source = inspect.getsource(cc.find_contact)
    assert "resolve_contacts" in source
    assert "hs_additional_emails" in inspect.getsource(reg.resolve_contacts)


def test_a_409_is_treated_as_found_and_the_run_continues(monkeypatch):
    """HubSpot answers 409 when the address is already on a contact, and
    names the existing id."""
    csuite = CSuite({"1430": [registrant(EMAIL, 21676),
                              registrant("second@example.invalid", 21691)]},
                    profiles={21676: profile(), 21691: profile()})
    hubspot = HubSpot(create=(
        {"status": "error", "category": "CONFLICT",
         "message": "A contact with the email 'x@y.invalid' already exists. "
                    "Existing ID: 64732954744"}, 409))

    out = applied(monkeypatch, csuite, hubspot)

    assert out["existing"] == 2, "both 409s read as already-there"
    assert out["created"] == 0
    assert out["failed"] == 0, "a 409 is not a failure"
    assert out["stopped"] is None, "and does not stop the run"
    ids = {r["hubspot_contact_id"]
           for r in out["events"][0]["existing"]}
    assert ids == {"64732954744"}


def test_the_existing_id_is_read_from_either_documented_shape():
    assert cc.existing_id_from(
        {"message": "Contact already exists. Existing ID: 2926551"}) \
        == "2926551"
    assert cc.existing_id_from(
        {"errors": [{"message": "A contact with email a@b.c already exists. "
                                "Existing ID: 777"}]}) == "777"
    assert cc.existing_id_from({"message": "nope"}) == ""
    assert cc.existing_id_from(None) == ""


def test_a_409_with_no_id_is_a_failure_not_a_silent_skip(monkeypatch):
    """If the id cannot be read, the person's state is unknown — that must
    stop the run rather than be recorded as handled."""
    csuite, hubspot = doubles(create=(
        {"status": "error", "message": "already exists"}, 409))

    out = applied(monkeypatch, csuite, hubspot)

    assert out["existing"] == 0
    assert out["failed"] == 1
    assert out["stopped"]


def test_an_unreadable_pre_create_check_stops_rather_than_creating(
        monkeypatch):
    """A duplicate that cannot be ruled out is not a duplicate to risk."""
    csuite = CSuite({"1430": [registrant(EMAIL, 21676)]},
                    profiles={21676: profile()})
    # The plan's read succeeds; the pre-create read does not.
    hubspot = HubSpot(read_error_after=1)

    out = applied(monkeypatch, csuite, hubspot)

    assert hubspot.posted == []
    assert out["stopped"] and "duplicate could not be ruled out" in \
        out["stopped"]


# ---------------------------------------------------------------------------
# What may be in the body, and what may never be
# ---------------------------------------------------------------------------

def test_the_body_carries_only_the_allowed_properties(monkeypatch):
    csuite, hubspot = doubles()

    applied(monkeypatch, csuite, hubspot)

    method, endpoint, body = hubspot.posted[0]
    assert method == "POST"
    assert endpoint == "crm/v3/objects/contacts"
    assert set(body) == {"properties"}
    assert set(body["properties"]) <= set(cc.ALLOWED_PROPERTIES)
    assert set(body["properties"]) == {"email", "firstname", "lastname"}


@pytest.mark.parametrize("prop", [
    "hs_marketable_status", "hs_marketable_until_renewal",
    "hs_marketable_reason_type", "hs_email_optout", "subscription_status",
    "hs_legal_basis", "lifecyclestage", "hs_lead_status",
    "marketing_contact", "newsletter_list",
])
def test_a_consent_or_audience_property_is_refused(prop):
    """Newsletter consent is a separate decision. The allowlist is what the
    mapper builds from; this is what the writer refuses to send."""
    with pytest.raises(cc.ContactCreateStopped):
        cc.assert_safe_properties({"email": EMAIL, prop: "true"})


def test_no_marketing_property_appears_anywhere_in_a_real_run(monkeypatch):
    import json

    csuite, hubspot = doubles()
    applied(monkeypatch, csuite, hubspot)

    sent = json.dumps(hubspot.posted, default=str).lower()
    for forbidden in ("marketable", "marketing", "subscri", "optout",
                      "optin", "consent", "lifecycle", "list"):
        assert forbidden not in sent, forbidden


def test_the_allowlist_is_exactly_four_properties():
    assert cc.ALLOWED_PROPERTIES == ("email", "firstname", "lastname",
                                     "company")


# ---------------------------------------------------------------------------
# Addresses never reach chat, logs or the audit
# ---------------------------------------------------------------------------

def test_the_preview_masks_every_address():
    csuite, hubspot = doubles()
    out = cc.run(csuite=csuite, hubspot=hubspot, event_ids=("1430",))

    reply = sync_commands._format_contact_results(out)

    assert EMAIL not in reply
    assert "n***@example.invalid" in reply
    assert "21676" in reply, "the CSuite profile id is there to look up"


def test_masking_keeps_the_domain_and_hides_the_person():
    assert cc.mask_email("r.mian@icccschool.org") == "r***@icccschool.org"
    assert cc.mask_email("a@b.co") == "a***@b.co"
    assert cc.mask_email("") == "***"
    assert cc.mask_email(None) == "***"
    assert cc.mask_email("not-an-address") == "***"


def test_the_409_message_is_redacted_before_it_is_stored(monkeypatch):
    """write_audit stores the error text, and HubSpot's 409 names the
    ADDRESS. An audit table that must not hold addresses cannot be handed
    one by an error path."""
    assert cc.redact(
        "A contact with the email 'someone@donor.invalid' already exists."
    ) == "A contact with the email '<address redacted>' already exists."
    assert "@" not in cc.redact("a@b.co and c.d+e@f.g.h failed")


def test_no_address_reaches_the_run_log():
    csuite, hubspot = doubles()
    out = cc.run(csuite=csuite, hubspot=hubspot, event_ids=("1430",))

    import json
    stored = json.dumps({"summary": cc.summary_of(out),
                         "records": cc.outcome_rows(out)}, default=str)

    assert EMAIL not in stored
    assert "@example.invalid" in stored, "the masked form is there"
    assert "contact_email" not in cc.LOGGED_FIELDS
    assert "properties" not in cc.LOGGED_FIELDS, \
        "the properties dict holds the address"


# ---------------------------------------------------------------------------
# The gates
# ---------------------------------------------------------------------------

def test_a_live_run_needs_the_flag(monkeypatch):
    arm(monkeypatch, False)
    csuite, hubspot = doubles()

    with pytest.raises(reg.RegistrationsSyncDisabled):
        cc.run(csuite=csuite, hubspot=hubspot, dry_run=False, limit=1,
               event_ids=("1430",))

    assert hubspot.posted == []


def test_a_live_run_needs_a_limit(monkeypatch):
    arm(monkeypatch)
    csuite, hubspot = doubles()

    with pytest.raises(cc.LimitRequired):
        cc.run(csuite=csuite, hubspot=hubspot, dry_run=False, limit=None,
               event_ids=("1430",))

    assert hubspot.posted == []


def test_no_limit_parses_to_none_and_is_refused():
    assert sync_commands._registration_limit(
        "create contacts apply no limit") is None
    assert sync_commands._registration_limit(
        "create contacts apply limit 3") == 3
    assert sync_commands._registration_limit(
        "create contacts apply") == 1


def test_the_limit_caps_the_creates(monkeypatch):
    csuite = CSuite({"1430": [registrant(EMAIL, 21676),
                              registrant("b@example.invalid", 21691),
                              registrant("c@example.invalid", 21693)]},
                    profiles={21676: profile(), 21691: profile(),
                              21693: profile()})

    out = applied(monkeypatch, csuite, HubSpot(), limit=2)

    assert out["created"] == 2
    assert out["deferred"] == 1


def test_a_held_event_is_skipped_entirely(monkeypatch):
    monkeypatch.setattr("config.Config.REGISTRATION_HELD_EVENT_IDS",
                        ("1430",))
    csuite, hubspot = doubles()

    out = cc.run(csuite=csuite, hubspot=hubspot, event_ids=("1430",))

    assert out["to_create"] == 0
    assert out["held_events"] == ["1430"]
    assert csuite.asked == [], "a held event is not even read"


def test_1153_is_still_held_by_default():
    import config

    assert "1153" in config.Config.REGISTRATION_HELD_EVENT_IDS


def test_an_unknown_event_is_refused_by_name(monkeypatch):
    called = []
    monkeypatch.setattr(cc, "run", lambda **k: called.append(k) or {})

    reply = sync_commands.handle("create contacts dry run event 9999", None)

    assert called == []
    assert "9999" in reply


def test_a_held_event_id_is_refused_by_name(monkeypatch):
    called = []
    monkeypatch.setattr(cc, "run", lambda **k: called.append(k) or {})

    reply = sync_commands.handle("create contacts dry run event 1153", None)

    assert called == []
    assert "1153" in reply
    assert reg.HELD_REASON in reply


# ---------------------------------------------------------------------------
# Registering is somebody else's job
# ---------------------------------------------------------------------------

def test_creating_a_contact_never_registers_anybody(monkeypatch):
    """The registrations sync has the verified write path, the retrying
    read-back and the registration_map row. Duplicating any of that here
    would be a second thing to keep correct."""
    import inspect

    # Docstring excluded: it EXPLAINS that registering is somebody else's
    # job, and naming the things it does not do is the point of it.
    source = inspect.getsource(cc).replace(cc.__doc__ or "\0", "")
    for forbidden in ("attendance/", "write_registration", "register/create",
                      "attendance_request", "registration_map"):
        assert forbidden not in source, forbidden

    csuite, hubspot = doubles()
    applied(monkeypatch, csuite, hubspot)

    for _method, endpoint, _body in hubspot.posted:
        assert "marketing-events" not in endpoint
        assert endpoint == "crm/v3/objects/contacts"


def test_the_reply_says_the_next_step_is_the_registrations_sync(monkeypatch):
    csuite, hubspot = doubles()
    out = cc.run(csuite=csuite, hubspot=hubspot, event_ids=("1430",))

    reply = sync_commands._format_contact_results(out)

    assert "the next registrations sync does the registering" in reply
    assert "Nobody is registered by that" in reply
    assert "create contacts apply limit N" in reply


# ---------------------------------------------------------------------------
# One at a time, in the background
# ---------------------------------------------------------------------------

class Ledger:
    def __init__(self, rows=None, writes=0):
        self.rows = list(rows or [])
        self.writes = writes
        self.opened = []
        self.closed = []
        self.next_id = 300

    def execute_query(self, sql, params=None, fetch=True):
        if "COUNT(*)" in sql:
            return [{"n": self.writes}]
        if "status = 'running'" in sql:
            return [r for r in self.rows if r["status"] == "running"]
        if "ORDER BY id DESC" in sql:
            return [r for r in self.rows if r["applied"]][-1:]
        if "AND id = %s" in sql:
            wanted = int((params or (0, 0))[1])
            return [r for r in self.rows if r["id"] == wanted]
        return []

    def open_run(self, applied):
        run_id = self.next_id
        self.next_id += 1
        self.opened.append(applied)
        self.rows.append(row(run_id, status="running"))
        return run_id

    def close_run(self, run_id, status, out, error_summary=None):
        self.closed.append((run_id, status, error_summary))


def row(run_id, status="complete", applied=True, outcomes=None):
    from datetime import datetime, timezone
    started = datetime(2026, 10, 12, 18, 0, 0, tzinfo=timezone.utc)
    return {"id": run_id, "applied": applied, "status": status,
            "started_at": started,
            "finished_at": None if status == "running" else started,
            "seconds": 42.0, "age_seconds": 9.0, "idle_seconds": 9.0,
            "error_summary": None, "outcomes": outcomes}


@pytest.fixture
def ledger(monkeypatch):
    led = Ledger()
    monkeypatch.setattr(jobs.database, "execute_query", led.execute_query)
    monkeypatch.setattr(jobs.cc, "open_run", led.open_run)
    monkeypatch.setattr(jobs.cc, "close_run", led.close_run)
    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", True)
    return led


def test_the_apply_runs_in_the_background(ledger):
    ran = []

    run_id = jobs.start_apply(limit=5, event_ids=("1430",),
                              runner=lambda *a: ran.append(a))

    assert run_id == 300
    assert ledger.opened == [True]
    for _ in range(200):
        if ran:
            break
    assert ran and ran[0] == (300, 5, ("1430",))


def test_a_second_run_is_refused_while_one_is_running(ledger):
    ledger.rows.append(row(250, status="running"))

    with pytest.raises(jobs.ApplyAlreadyRunning) as refused:
        jobs.start_apply(limit=1, runner=lambda *a: None)

    assert "run_log 250" in str(refused.value)
    assert "one person becomes two contacts" in str(refused.value)
    assert ledger.opened == []


def test_the_refusal_reaches_chat(ledger):
    ledger.rows.append(row(250, status="running"))

    reply = sync_commands.handle("create contacts apply limit 1", None)

    assert "One run at a time" in reply
    assert "run_log 250" in reply


def test_abandonment_is_judged_by_create_activity_not_age(ledger):
    assert jobs.IDLE_ABANDONED_SECONDS == 300
    sql = jobs._RUNNING_SQL
    assert "MAX(created_at) AS last_write" in sql
    assert "crm/v3/objects/contacts" in sql
    assert "GREATEST(" in sql
    assert "INTERVAL" not in sql, "age must not decide abandonment"


def test_the_flag_and_limit_are_checked_before_the_thread(ledger,
                                                          monkeypatch):
    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", False)
    with pytest.raises(reg.RegistrationsSyncDisabled):
        jobs.start_apply(limit=1, runner=lambda *a: None)

    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", True)
    with pytest.raises(cc.LimitRequired):
        jobs.start_apply(limit=None, runner=lambda *a: None)

    assert ledger.opened == []


def test_the_background_thread_writes_with_dry_run_false(monkeypatch):
    seen = {}
    monkeypatch.setattr(jobs.cc, "run", lambda **kw: seen.update(kw) or {})

    jobs._run_apply(300, 5, ("1430",))

    assert seen["dry_run"] is False
    assert seen["limit"] == 5
    assert seen["run_id"] == 300
    assert seen["event_ids"] == ("1430",)


def test_the_apply_command_starts_a_background_run(monkeypatch):
    seen = {}
    monkeypatch.setattr("sync.contact_jobs.start_apply",
                        lambda **kw: seen.update(kw) or 301)

    reply = sync_commands.handle("create contacts apply event 1430 limit 5",
                                 None)

    assert seen["limit"] == 5
    assert seen["event_ids"] == ("1430",)
    assert "run_log 301" in reply
    assert "Do not send it again" in reply


def test_status_returns_the_full_report(ledger):
    summary = {"dry_run": False, "limit": 5, "events_read": 1,
               "to_create": 5, "created": 5, "existing": 0, "failed": 0,
               "unknown": 0, "deferred": 0, "review": 0, "review_rows": [],
               "held_events": [], "csuite_calls": 6, "hubspot_calls": 6,
               "writes_attempted": 5, "write_audit_ids": [201, 202],
               "stopped": None, "with_name": 5, "with_company": 0,
               "error": None}
    ledger.rows.append(row(300, outcomes={"summary": summary,
                                          "records": [{"a": 1}] * 5}))

    reply = sync_commands.handle("contacts status run 300", None)

    assert "run_log 300" in reply
    assert "APPLIED" in reply
    assert "**5** contact(s) created" in reply
    assert "write_audit 201, 202" in reply


def test_status_of_a_running_run_shows_progress(ledger):
    ledger.rows.append(row(300, status="running"))
    ledger.writes = 3

    reply = sync_commands.handle("contacts status", None)

    assert "still running" in reply
    assert "**3** so far" in reply
    assert "Do not start another run" in reply


def test_status_before_any_run_says_so(ledger):
    reply = sync_commands.handle("contacts status", None)

    assert "No contact creation has been run yet" in reply


def test_the_contact_status_does_not_answer_for_registrations(ledger):
    """Two jobs, two run_log job names, two status commands."""
    assert jobs.cc.JOB == "contact_create"
    assert "job = %s" in jobs._RUN_SQL
    assert "registrations_sync" not in jobs._RUNNING_SQL


def test_sync_all_cannot_reach_contact_creation():
    for phrase in sync_commands.ALL_SYNC_PHRASES:
        assert not any(p in phrase
                       for p in sync_commands.CONTACT_CREATE_PHRASES)


def test_chat_recognises_both_commands():
    for text in ("create contacts dry run", "create contacts apply limit 1",
                 "create contacts dry run event 1430", "contacts status",
                 "contacts status run 300"):
        assert sync_commands.can_handle(text), text
