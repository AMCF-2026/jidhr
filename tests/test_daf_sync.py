"""V5.90: the nightly DAF intake.

Every client here is a TEST DOUBLE apart from CSuiteClient._request, which is
real over a faked transport, as in tests/daf_snapshot.py. Every name and
address is invented. No network, no database.
"""

import json
import logging
from datetime import datetime, timezone

import pytest

from clients import audit
from clients.csuite import CSuiteClient
from config import Config
from sync import daf_sync
from sync.sandbox_writes import WriteBudget
from tests import daf_snapshot

FORM = Config.DAF_INQUIRY_FORM_ID
AFTER = 1800000000000          # 2027-01-15, after the cutoff
BEFORE = 1760000000000         # 2025-10-09, before it


def sub(conv, first, last, email, ms=AFTER, **extra):
    return daf_snapshot.submission(conv, first, last, email, ms, **extra)


class Hub:
    """HubSpot double. contacts: email -> (contact id, csuite_profile_id)."""

    def __init__(self, subs, contacts=None, unreadable=()):
        self.subs = subs
        self.contacts = contacts if contacts is not None else {
            s["values"][2]["value"]: ("70" + s["conversionId"][:3], "")
            for s in subs}
        self.unreadable = set(unreadable)
        self.patched = []

    def get_form_submissions(self, form_id, limit=50, after=None):
        return {"results": list(self.subs) if form_id == FORM else []}

    def search_contact_by_email(self, email, properties=None):
        if email in self.unreadable:
            return {"error": "HTTP 503"}
        found = self.contacts.get(email)
        if not found:
            return {"results": []}
        cid, stored = found
        return {"results": [{"id": cid,
                             "properties": {"csuite_profile_id": stored}}]}

    def update_contact_by_email(self, email, properties):
        if email not in self.contacts:
            return {"error": "Contact not found"}
        self.patched.append((self.contacts[email][0], dict(properties)))
        return {"id": self.contacts[email][0]}

    def create_contact(self, properties):
        raise AssertionError("the nightly step must never create a contact")

    def __getattr__(self, name):
        raise AssertionError(f"unexpected HubSpot call: {name}()")


class Client(CSuiteClient):
    """The real _request. existing: email -> [profile ids]."""

    def __init__(self, existing=None, missing=(), unreadable=(), budget=10,
                 fail_profile=False, task_rows=(), tasks_unreadable=False):
        self.api_key, self.api_secret = "k", "s"
        self.base_url = "https://amuslimcf.fcsuite.com/api/v2"
        self.env = "live"
        self.verify_writes = False
        self.write_budget = WriteBudget(budget)
        self.sent = []
        self.task_rows = tuple(task_rows)
        self.tasks_unreadable = tasks_unreadable
        outer, existing = self, existing or {}
        missing = {str(m) for m in missing}
        unreadable = set(unreadable)

        class Session:
            def post(_self, url, data=None, **kwargs):
                endpoint = url.split("/api/v2/", 1)[-1]
                body = json.loads(data)
                outer.sent.append({"endpoint": endpoint, "body": body})
                if endpoint == "profile/list":
                    email = body.get("primary_email")
                    if email in unreadable:
                        payload = {"success": 0, "errors": ["boom"]}
                    elif email is None:
                        payload = {"success": 1,
                                   "data": {"count": 18797, "results": []}}
                    elif str(email).startswith("filter-probe-"):
                        payload = {"success": 1,
                                   "data": {"count": 0, "results": []}}
                    else:
                        ids = existing.get(email, [])
                        payload = {"success": 1, "data": {
                            "count": len(ids),
                            "results": [{"profile_id": i} for i in ids]}}
                elif endpoint == "profile/display":
                    pid = str(body.get("profile_id"))
                    payload = ({"success": 0, "errors": ["Profile not found"]}
                               if pid in missing else
                               {"success": 1, "data": {"profile_id": int(pid),
                                                       "ptype": "indiv"}})
                elif endpoint == "profile/create/individual":
                    payload = ({"success": 0, "errors": ["server error"]}
                               if fail_profile else
                               {"success": 1, "data": {"profile_id": 31337}})
                elif endpoint == "task/create":
                    payload = {"success": 1, "data": {"task_id": 1099}}
                elif endpoint == "task/list":
                    if outer.tasks_unreadable:
                        payload = {"success": 0, "errors": ["boom"]}
                    else:
                        rows = [dict(r) for r in outer.task_rows]
                        payload = {"success": 1, "data": {
                            "count": len(rows), "results": rows}}
                else:
                    raise AssertionError(f"unexpected CSuite {endpoint}")

                class R:
                    status_code = 200
                    text = json.dumps(payload)

                    def json(self):
                        return payload
                return R()

        self.session = Session()

    def writes(self):
        return [s["endpoint"] for s in self.sent
                if "create" in s["endpoint"]]


@pytest.fixture
def world(monkeypatch):
    import clients.csuite as mod

    state = {"processed": {}, "held": {}, "holds": [], "audit": []}
    for name, value in (("JIDHR_DAF_SINCE", "2026-10-22"),
                        ("JIDHR_DAF_MODE", "live"),
                        ("JIDHR_DAF_REPORT_TO", ("carl@example.invalid",)),
                        ("CSUITE_ENV", "live"),
                        ("CSUITE_WRITE_BUDGET", 10),
                        ("CSUITE_DAF_CREATE_ENABLED", True),
                        ("CSUITE_DAF_TASK_CREATE_ENABLED", True),
                        ("CSUITE_DAF_FUND_CREATE_ENABLED", False),
                        ("CSUITE_HUBSPOT_BACKFILL_ENABLED", False),
                        ("CSUITE_TICKET_CLOSE_ENABLED", False),
                        ("CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", 1012),
                        ("CSUITE_TASK_TYPE_ID", 1065)):
        monkeypatch.setattr(Config, name, value)

    def reserve(system, method, endpoint, target_id=None, payload=None,
                sync_run_id=None):
        state["audit"].append({"endpoint": endpoint,
                               "meta": audit.payload_meta(payload)})
        ids = (payload or {})
        if endpoint in ("profile/create/individual", "task/create") and \
                ids.get("hubspot_submission_id"):
            state["processed"][ids["hubspot_submission_id"]] = "31337"
        return {"id": len(state["audit"])}

    def record(system, method, endpoint, target_id=None, payload=None,
               status="success", **kwargs):
        meta = audit.payload_meta(payload)
        state["holds"].append({"system": system, "method": method,
                               "endpoint": endpoint, "meta": meta})
        if endpoint.startswith("submission/held/"):
            state["held"][payload["hubspot_submission_id"]] = \
                endpoint.rsplit("/", 1)[-1]
        return True

    monkeypatch.setattr(mod, "reserve_write", reserve)
    monkeypatch.setattr(mod, "complete_write", lambda *a, **kw: None)
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: dict(state["processed"]))
    monkeypatch.setattr(audit, "held_submissions",
                        lambda form_id: dict(state["held"]))
    monkeypatch.setattr(audit, "record_write", record)
    monkeypatch.setattr("sync.daf_reconcile.link_rows_allowed", lambda: True)
    import uuid
    import sync.filter_trust as trust
    monkeypatch.setattr(trust.uuid, "uuid4", lambda: uuid.UUID(int=0x5EED))
    return state


def run(hub, client, mode="live"):
    return daf_sync.run(hubspot=hub, csuite=client, run_mode=mode)


# The golden fixture's own fresh donor, submitted at the golden time, so the
# requests can be compared with what the chat sent for the same person.
FRESH = sub("fresh001-bbbb", "Fresh", "Donor", "fresh.donor@example.invalid",
            1899990000000)
BACK = sub("back0001-cccc", "Returning", "Donor",
           "returning.donor@example.invalid")


# --- live: the chat's own plan ---------------------------------------------------

def test_a_new_donor_gets_the_chat_profile_task_and_link(world):
    hub, client = Hub([FRESH]), Client()
    out = run(hub, client)

    assert client.writes() == ["profile/create/individual", "task/create"]
    golden = {s["endpoint"]: s["body"]
              for s in daf_snapshot.GOLDEN_NEW_DONOR_SENT()}
    for endpoint in ("profile/create/individual", "task/create"):
        sent = next(s["body"] for s in client.sent
                    if s["endpoint"] == endpoint)
        assert {k: v for k, v in sent.items() if k != "epoch"} == \
            {k: v for k, v in golden[endpoint].items() if k != "epoch"}, \
            f"{endpoint} body differs from what the chat sends"
    assert hub.patched == [("70fre", {"csuite_profile_id": "31337"})]
    [added] = out["added"]
    assert added["plan"] == "new" and added["task_id"] == 1099
    assert added["profile_created"] and not added.get("problems")
    assert out["held"] == []


def test_a_rejected_hubspot_link_is_a_problem_not_a_success(world):
    """V5.93: HubSpot answers a rejected PATCH with its own JSON, which has
    no "error" key. It was reported as "HubSpot contact updated"."""
    class Rejecting(Hub):
        def update_contact_by_email(self, email, properties):
            self.patched.append(("rejected", dict(properties)))
            return {"status": "error", "category": "VALIDATION_ERROR",
                    "message": "Property values were not valid"}

    hub, client = Rejecting([FRESH]), Client()
    out = run(hub, client)

    [added] = out["added"]
    assert added["profile_created"] and added["task_id"] == 1099
    assert added["problems"] == [
        "HubSpot update: VALIDATION_ERROR: Property values were not valid"]
    text = daf_sync.email_text([out])
    assert "Needs Carl (1)" in text and "Added to CSuite (1)" not in text


def test_a_returning_donor_gets_the_task_only(world):
    hub = Hub([BACK])
    client = Client(existing={"returning.donor@example.invalid": [21999]})
    out = run(hub, client)

    assert client.writes() == ["task/create"]
    task = next(s["body"] for s in client.sent
                if s["endpoint"] == "task/create")
    assert task["id"] == 21999
    assert out["added"][0]["plan"] == "returning"
    assert hub.patched == [], "a returning donor's HubSpot link is untouched"


# --- holds: nothing written, a hold row recorded -----------------------------------

@pytest.mark.parametrize("make,reason", [
    (lambda: (Hub([sub("m1", "", "Donor", "m@example.invalid")]),
              Client()), "missing_fields"),
    (lambda: (Hub([FRESH], contacts={}), Client()), "no_hubspot_contact"),
    (lambda: (Hub([FRESH]),
              Client(existing={"fresh.donor@example.invalid": [1, 2]})),
     "multiple_profiles"),
    (lambda: (Hub([FRESH], contacts={"fresh.donor@example.invalid":
                                     ("70fre", "777")}),
              Client(missing={777})), "stale_link"),
    (lambda: (Hub([FRESH], contacts={"fresh.donor@example.invalid":
                                     ("70fre", "777")}),
              Client(existing={"fresh.donor@example.invalid": [888]})),
     "conflicting_link"),
])
def test_anything_uncertain_is_held_and_nothing_written(world, make, reason):
    hub, client = make()
    out = run(hub, client)

    assert client.writes() == []
    assert hub.patched == []
    [held] = out["held"]
    assert held["reason"] == reason and held["recorded"] is True
    [row] = world["holds"]
    assert row["system"] == "jidhr"
    assert row["endpoint"] == f"submission/held/{reason}"
    assert set(row["meta"]["ids"]) == {"hubspot_form_id",
                                       "hubspot_submission_id"}


# --- deferred: about tonight, not the submission (V5.93) -----------------------------

@pytest.mark.parametrize("make,reason", [
    (lambda: (Hub([FRESH], unreadable={"fresh.donor@example.invalid"}),
              Client()), "hubspot_unreadable"),
    (lambda: (Hub([FRESH]),
              Client(unreadable={"fresh.donor@example.invalid"})),
     "check_failed"),
])
def test_a_bad_night_is_deferred_not_held(world, make, reason):
    hub, client = make()
    out = run(hub, client)

    assert client.writes() == [] and hub.patched == []
    assert out["held"] == []
    [deferred] = out["deferred"]
    assert deferred["reason"] == reason and "recorded" not in deferred
    assert "try again tomorrow night" in deferred["why"]
    assert world["holds"] == [], "no hold row: tomorrow sees it as new"


def test_a_deferred_submission_is_tried_again_the_next_night(world):
    hub = Hub([FRESH], unreadable={"fresh.donor@example.invalid"})
    first = run(hub, Client())
    assert first["deferred"] and world["holds"] == []

    second = run(Hub([FRESH]), Client())
    assert [e["submission_id"] for e in second["added"]] == \
        [first["deferred"][0]["submission_id"]]


def test_the_deferred_reasons_are_exactly_these():
    assert daf_sync.DEFERRED_REASONS == {"nightly_limit",
                                         "hubspot_unreadable", "check_failed",
                                         "task_check_failed"}
    assert daf_sync.DEFERRED_REASONS <= set(daf_sync.HOLD_REASONS)


def test_a_csuite_error_while_writing_is_held(world):
    hub, client = Hub([FRESH]), Client(fail_profile=True)
    out = run(hub, client)

    assert out["added"] == []
    assert out["held"][0]["reason"] == "csuite_error"
    assert "task/create" not in client.writes()


def test_no_contact_is_never_papered_over(world):
    hub = Hub([FRESH], contacts={})
    out = run(hub, Client())
    assert "Every form submission should create one" in out["held"][0]["why"]


# --- the budget ----------------------------------------------------------------------

def test_the_budget_runs_out_and_the_rest_are_deferred_not_held(world):
    a = sub("aaaa0001", "Ann", "One", "a@example.invalid", AFTER)
    b = sub("bbbb0002", "Ben", "Two", "b@example.invalid", AFTER + 1000)
    hub, client = Hub([b, a]), Client(budget=3)
    out = run(hub, client)

    assert [e["submission_id"] for e in out["added"]] == ["aaaa0001"], \
        "oldest first"
    assert out["held"] == []
    [deferred] = out["deferred"]
    assert deferred["submission_id"] == "bbbb0002"
    assert deferred["reason"] == "nightly_limit"
    assert "Nightly limit reached" in deferred["why"]
    assert client.write_budget.used == 2
    assert world["holds"] == [], \
        "a spent budget must never hold a submission for good"


def test_the_deferred_one_goes_through_the_next_night(world):
    b = sub("bbbb0002", "Ben", "Two", "b@example.invalid", AFTER + 1000)
    run(Hub([b]), Client(budget=1))
    out = run(Hub([b]), Client(budget=10))
    assert [e["submission_id"] for e in out["added"]] == ["bbbb0002"]


def test_preview_reports_what_the_budget_would_have_allowed(world,
                                                            monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_WRITE_BUDGET", 3)
    a = sub("aaaa0001", "Ann", "One", "a@example.invalid", AFTER)
    b = sub("bbbb0002", "Ben", "Two", "b@example.invalid", AFTER + 1000)
    out = run(Hub([a, b]), Client(), mode="preview")

    assert out["budget"] == 3
    assert out["budget_needed"] == 4 and out["budget_allowed"] == 2
    assert out["held"] == []
    assert out["deferred"][0]["reason"] == "nightly_limit"


# --- preview writes nothing at all -------------------------------------------------

def test_preview_writes_nothing_and_shows_the_exact_title(world):
    hub = Hub([FRESH, sub("m1", "", "X", "m@example.invalid")])
    client = Client()
    out = run(hub, client, mode="preview")

    assert client.writes() == [] and hub.patched == []
    assert world["holds"] == [] and world["audit"] == []
    [would] = out["added"]
    assert would["task_title"] == \
        "DAF inquiry follow-up: Fresh Donor — fresh.donor@example.invalid"
    assert out["held"][0]["reason"] == "missing_fields"
    assert "recorded" not in out["held"][0]


# --- no double-creates ---------------------------------------------------------------

def test_a_rerun_creates_nothing_twice(world):
    hub, client = Hub([FRESH, BACK]), Client(
        existing={"returning.donor@example.invalid": [21999]})
    run(hub, client)
    first = list(client.writes())

    out = run(hub, client)

    assert client.writes() == first, "the second run wrote nothing"
    assert out["skipped_handled"] == 2 and out["added"] == []


def test_held_ones_are_not_retried(world):
    hub = Hub([FRESH], contacts={})
    run(hub, Client())
    hub.contacts = {"fresh.donor@example.invalid": ("70fre", "")}
    client = Client()

    out = run(hub, client)

    assert client.writes() == []
    assert out["skipped_held"] == 1
    assert len(world["holds"]) == 1, "no second hold row"


def test_handled_in_chat_meanwhile_is_not_written(world, monkeypatch):
    """The history is re-read immediately before writing."""
    calls = {"n": 0}

    def processed(form_id, **k):
        calls["n"] += 1
        return {} if calls["n"] == 1 else {"fresh001-bbbb": "21700"}

    monkeypatch.setattr(audit, "processed_submissions", processed)
    hub, client = Hub([FRESH]), Client()
    out = run(hub, client)

    assert client.writes() == []
    assert out["skipped_handled"] == 1


def test_only_submissions_on_or_after_the_cutoff(world):
    old = sub("old00001", "Old", "Donor", "old@example.invalid", BEFORE)
    out = run(Hub([old, FRESH]), Client())
    assert out["candidates"] == 1
    assert [e["submission_id"] for e in out["added"]] == ["fresh001-bbbb"]


# --- refusals ------------------------------------------------------------------------------

@pytest.mark.parametrize("since", ["", "22/10/2026", "soon"])
def test_no_cutoff_no_run(world, monkeypatch, since):
    monkeypatch.setattr(Config, "JIDHR_DAF_SINCE", since)
    with pytest.raises(daf_sync.Refused):
        run(Hub([FRESH]), Client())


@pytest.mark.parametrize("flag", ["CSUITE_DAF_CREATE_ENABLED",
                                  "CSUITE_DAF_TASK_CREATE_ENABLED"])
def test_live_needs_the_daf_switches(world, monkeypatch, flag):
    monkeypatch.setattr(Config, flag, False)
    client = Client()
    with pytest.raises(daf_sync.Refused, match=flag):
        run(Hub([FRESH]), client)
    assert client.sent == []


def test_preview_runs_without_the_daf_switches(world, monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", False)
    out = run(Hub([FRESH]), Client(), mode="preview")
    assert len(out["added"]) == 1


@pytest.mark.parametrize("raw,expected", [("live", "live"), (" LIVE ", "live"),
                                          ("", "preview"), ("true", "preview"),
                                          ("preview", "preview")])
def test_only_the_word_live_writes(monkeypatch, raw, expected):
    monkeypatch.setattr(Config, "JIDHR_DAF_MODE", raw)
    assert daf_sync.mode() == expected


def test_the_mode_defaults_to_preview():
    import importlib
    import os

    import config
    saved = os.environ.pop("JIDHR_DAF_MODE", None)
    try:
        importlib.reload(config)
        assert config.Config.JIDHR_DAF_MODE == "preview"
    finally:
        if saved is not None:
            os.environ["JIDHR_DAF_MODE"] = saved
        importlib.reload(config)


# --- logs and audit: ids only ----------------------------------------------------------------

def test_no_donor_name_or_email_in_any_log_line(world, caplog):
    subs = [FRESH, BACK, sub("m1", "Missy", "", "missy@example.invalid")]
    hub = Hub(subs, contacts={
        "fresh.donor@example.invalid": ("70fre", ""),
        "returning.donor@example.invalid": ("70bac", "")})
    hub.contacts["missy@example.invalid"] = ("70mis", "")
    with caplog.at_level(logging.DEBUG):
        run(hub, Client(existing={"returning.donor@example.invalid":
                                  [21999]}))
    logged = "\n".join(r.getMessage() for r in caplog.records)
    for private in ("Fresh", "Returning", "Missy", "@example.invalid",
                    "Fresh Family Fund", "1 Test Way"):
        assert private not in logged, f"{private!r} reached a log line"


def test_the_audit_rows_carry_ids_only(world):
    run(Hub([FRESH]), Client())
    dumped = json.dumps(world["audit"] + world["holds"])
    for private in ("Fresh", "fresh.donor", "Fresh Family Fund"):
        assert private not in dumped


def test_hold_rows_never_count_as_handled():
    """A held submission must still be offered in chat."""
    assert "submission/link/%" in str(
        (audit.LINK_TARGET_SYSTEM, audit.SUBMISSION_LINK_ENDPOINT + "/%"))
    assert not "submission/held/nightly_limit".startswith(
        audit.SUBMISSION_LINK_ENDPOINT + "/")
    assert "LIKE %s" in audit._PROCESSED_SQL


# --- the email --------------------------------------------------------------------------------

WHEN = datetime(2026, 10, 22, 11, 0, tzinfo=timezone.utc)   # Thu Oct 22 ET


def section(added=(), held=(), mode="live", **kw):
    return dict({"form": "daf", "label": "DAF", "mode": mode,
                 "added": list(added), "held": list(held), "budget": 10,
                 "budget_needed": 0, "budget_allowed": 0}, **kw)


ADDED = {"submission_id": "s1", "submitted_at": AFTER, "name": "Ann One",
         "email": "a@example.invalid", "fund_name": "One Family Fund",
         "contribution": "$10,001-$25,000", "hubspot_link": "https://hs/1",
         "plan": "new", "profile_id": 31337, "profile_created": True,
         "task_id": 1099, "task_due": "2027-01-19",
         "assignee": "Ahmed, Munira",
         "task_title": "DAF inquiry follow-up: Ann One — a@example.invalid"}
HELD = {"submission_id": "s2", "submitted_at": AFTER, "name": "Ben Two",
        "email": "b@example.invalid", "fund_name": "", "contribution": "",
        "hubspot_link": "https://hs/2", "reason": "multiple_profiles",
        "why": daf_sync.HOLD_REASONS["multiple_profiles"]}


@pytest.mark.parametrize("added,held,expected", [
    ([ADDED], [HELD], "New donor inquiries added to CSuite — Thu Oct 22 "
                      "(1 added, 1 needs Carl)"),
    ([ADDED], [], "New donor inquiries added to CSuite — Thu Oct 22 "
                  "(1 added)"),
    ([], [HELD], "New donor inquiries added to CSuite — Thu Oct 22 "
                 "(1 needs Carl)"),
    ([], [HELD, dict(HELD, submission_id="s3")],
     "New donor inquiries added to CSuite — Thu Oct 22 (2 need Carl)"),
])
def test_the_subject_drops_a_zero_part(added, held, expected):
    assert daf_sync.subject([section(added, held)], WHEN) == expected


def test_a_preview_subject_says_so():
    assert daf_sync.subject([section([ADDED], [], mode="preview")], WHEN) == \
        "[Preview] New donor inquiries for CSuite — Thu Oct 22 " \
        "(1 would be added)"


def test_the_body_is_plain_and_complete():
    text = daf_sync.email_text([section([ADDED], [HELD])])

    for part in ("DAF INQUIRIES", "Added to CSuite (1)", "Ann One",
                 "Fund name: One Family Fund · Contribution: $10,001-$25,000",
                 "New CSuite profile 31337; follow-up task 1099 for Ahmed, "
                 "Munira, due Tue Jan 19.", "HubSpot: https://hs/1",
                 "Needs Carl (1)", "Ben Two",
                 "Fund name: not given · Contribution: not given",
                 "Why: More than one CSuite profile already uses this email "
                 "address", "HubSpot: https://hs/2",
                 "Jidhr won't try these again by itself.",
                 "Nothing was sent to any donor."):
        assert part in text, part
    for jargon in ("API", "endpoint", "write_audit", "submission/held",
                   "multiple_profiles"):
        assert jargon not in text, jargon


def test_the_preview_body_shows_titles_and_the_budget():
    s = section([dict(ADDED, task_id=None)], [], mode="preview",
                budget_needed=4, budget_allowed=2)
    text = daf_sync.email_text([s])
    assert "PREVIEW: nothing was added" in text
    assert "Would be added (1)" in text
    assert "Task title: DAF inquiry follow-up: Ann One — a@example.invalid" \
        in text
    assert "would have allowed 2 of the 4 needed" in text


def test_no_email_when_nothing_was_added_or_held(monkeypatch):
    sent = []
    monkeypatch.setattr("clients.notify.send_report",
                        lambda *a: sent.append(a) or {"sent": a[0]})
    out = daf_sync.send_email([section()], WHEN)
    assert out["skipped"] == "nothing to report" and sent == []


def test_the_email_goes_to_the_report_list_only(monkeypatch):
    sent = []
    monkeypatch.setattr(Config, "JIDHR_DAF_REPORT_TO",
                        ("carl@example.invalid",))
    monkeypatch.setattr("clients.notify.send_report",
                        lambda to, subj, text: sent.append((to, subj)) or
                        {"sent": list(to), "failed": []})
    daf_sync.send_email([section([ADDED])], WHEN)
    assert sent[0][0] == ("carl@example.invalid",)
    assert not sent[0][1].startswith("[Jidhr")


def test_an_added_one_with_a_problem_is_listed_for_carl():
    rough = dict(ADDED, problems=["HubSpot has no contact for this address"])
    text = daf_sync.email_text([section([rough])])
    assert daf_sync.counts([section([rough])]) == (0, 1)
    assert "Needs Carl (1)" in text and "but: HubSpot has no contact" in text


# --- the job ------------------------------------------------------------------------------------

def _setting(monkeypatch, name, value):
    """On every Config a reader might hold: sync.scheduled reads the CURRENT
    config.Config, sync.daf_sync the one it imported; a reload elsewhere in
    the suite makes those two different classes."""
    import config

    for holder in {id(c): c for c in (config.Config, daf_sync.Config,
                                      Config)}.values():
        monkeypatch.setattr(holder, name, value)


def test_the_job_does_nothing_when_the_schedule_is_disarmed(monkeypatch):
    from jobs.handlers import daf_sync as job

    _setting(monkeypatch, "SCHEDULED_SYNC_ENABLED", False)
    monkeypatch.setattr(daf_sync, "run",
                        lambda *a, **k: pytest.fail("it ran"))
    assert job.handle({}) == {"enabled": False}


def test_a_refused_step_alerts_and_fails_the_job(monkeypatch):
    from jobs.handlers import daf_sync as job
    from jobs.runner import JobFailed

    alerts = []
    _setting(monkeypatch, "SCHEDULED_SYNC_ENABLED", True)
    _setting(monkeypatch, "CSUITE_WRITE_BUDGET", 10)
    _setting(monkeypatch, "CSUITE_WRITE_BUDGET_RAW", "10")
    _setting(monkeypatch, "JIDHR_DAF_SINCE", "")
    monkeypatch.setattr("clients.notify.safe_notify",
                        lambda *a, **k: alerts.append(a) or {})
    with pytest.raises(JobFailed):
        job.handle({})
    assert alerts and "did not run" in alerts[0][0]


def test_the_job_row_keeps_ids_not_names(world, monkeypatch):
    from jobs.handlers import daf_sync as job

    _setting(monkeypatch, "SCHEDULED_SYNC_ENABLED", True)
    monkeypatch.setattr(daf_sync, "run", lambda: {
        "mode": "live", "candidates": 2, "added": [ADDED], "held": [HELD],
        "skipped_handled": 0, "skipped_held": 0})
    monkeypatch.setattr(daf_sync, "send_email",
                        lambda sections: {"sent": ["x"]})
    result = job.handle({})
    dumped = json.dumps(result)
    assert "Ann One" not in dumped and "a@example.invalid" not in dumped
    assert result["added_ids"] == ["s1"]
    assert result["held_ids"] == {"s2": "multiple_profiles"}


def test_the_job_is_seeded_after_registrations_and_before_the_mirror():
    import scripts.jobs_run as jr

    assert jr.REGISTRATIONS_SEED["priority"] < jr.DAF_SEED["priority"] < \
        jr.MIRROR_SEED["priority"]
    assert jr.DAF_SEED["max_attempts"] == 1


# --- hotfix-71: CSUITE_WRITE_BUDGET must be a whole number ---------------------------

@pytest.mark.parametrize("raw", ["true", "1.5", "-3", "10abc", "ten", "1e3"])
def test_a_budget_that_is_not_a_whole_number_is_refused(world, monkeypatch,
                                                        raw):
    """2026-10-23: the jobs service had CSUITE_WRITE_BUDGET=true. Read as 0,
    a live run would have held every submission for good."""
    import config

    monkeypatch.setenv("CSUITE_WRITE_BUDGET", raw)
    importlib_reload_config()
    try:
        _setting(monkeypatch, "CSUITE_WRITE_BUDGET",
                 config.Config.CSUITE_WRITE_BUDGET)
        _setting(monkeypatch, "CSUITE_WRITE_BUDGET_RAW", raw)
        hub, client = Hub([FRESH]), Client()
        with pytest.raises(daf_sync.BadSetting, match="CSUITE_WRITE_BUDGET"):
            run(hub, client)
        assert client.sent == [] and world["holds"] == []
    finally:
        monkeypatch.delenv("CSUITE_WRITE_BUDGET", raising=False)
        importlib_reload_config()


def importlib_reload_config():
    import importlib

    import config
    importlib.reload(config)


@pytest.mark.parametrize("raw,value", [("", 0), ("0", 0)])
@pytest.mark.parametrize("mode", ["live", "preview"])
def test_an_unset_or_zero_budget_is_refused_not_held(world, monkeypatch, raw,
                                                    value, mode):
    _setting(monkeypatch, "CSUITE_WRITE_BUDGET", value)
    _setting(monkeypatch, "CSUITE_WRITE_BUDGET_RAW", raw)
    hub, client = Hub([FRESH]), Client()
    with pytest.raises(daf_sync.BadSetting, match="not set"):
        run(hub, client, mode=mode)
    assert client.sent == [] and world["holds"] == []


@pytest.mark.parametrize("raw", ["10", " 10 ", "3"])
def test_a_whole_number_is_accepted(world, monkeypatch, raw):
    _setting(monkeypatch, "CSUITE_WRITE_BUDGET", int(raw))
    _setting(monkeypatch, "CSUITE_WRITE_BUDGET_RAW", raw)
    assert daf_sync.budget_problem() is None
    assert len(run(Hub([FRESH]), Client())["added"]) == 1


def test_a_bad_budget_alerts_by_name_and_the_job_does_not_fail(monkeypatch):
    from jobs.handlers import daf_sync as job

    alerts = []
    _setting(monkeypatch, "SCHEDULED_SYNC_ENABLED", True)
    _setting(monkeypatch, "CSUITE_WRITE_BUDGET", 0)
    _setting(monkeypatch, "CSUITE_WRITE_BUDGET_RAW", "true")
    monkeypatch.setattr("clients.notify.safe_notify",
                        lambda subject, body, **k: alerts.append(
                            (subject, body, k)) or {})

    result = job.handle({})            # no JobFailed: nothing crashed

    assert result["stood_down"] is True
    [(subject, body, kwargs)] = alerts
    assert kwargs["kind"] == "alert"
    assert "CSUITE_WRITE_BUDGET" in body and "'true'" in body
    assert "not a whole number" in body


def test_a_bad_budget_run_exits_zero(monkeypatch):
    """Through the REAL runner.run and jobs_run --once: the job completes,
    so the run reports success to Railway, not a crash. Only the database
    the runner records into is stubbed."""
    import scripts.jobs_run as jr
    from jobs import runner

    job = {"id": 9, "job_type": "daf_sync", "payload": {}, "attempts": 0,
           "max_attempts": 1, "priority": 45}
    summaries = []
    _setting(monkeypatch, "SCHEDULED_SYNC_ENABLED", True)
    _setting(monkeypatch, "CSUITE_WRITE_BUDGET", 0)
    _setting(monkeypatch, "CSUITE_WRITE_BUDGET_RAW", "true")
    monkeypatch.setattr("clients.notify.safe_notify", lambda *a, **k: {})
    monkeypatch.setattr(jr.database, "is_configured", lambda: True)
    monkeypatch.setattr(jr, "ensure_registrations_sync", lambda: "ok")
    monkeypatch.setattr(jr, "ensure_daf_sync", lambda: "ok")
    monkeypatch.setattr(jr, "ensure_forms_sync", lambda: "ok")
    monkeypatch.setattr(runner.database, "execute_query",
                        lambda *a, **k: [])
    monkeypatch.setattr(runner, "queue_next_occurrence", lambda job: None)
    monkeypatch.setattr(runner, "run_until_empty",
                        lambda worker: summaries.append(runner.run(job))
                        or summaries)

    code = jr.main(["--once"])

    assert summaries[0]["status"] == runner.STATUS_COMPLETE
    assert code == jr.EXIT_OK


# --- V5.93: the email's deferred part, and no chat commands in the body ---------------

DEFERRED = dict(HELD, submission_id="s4", name="Cy Four", reason="nightly_limit",
                why=daf_sync.HOLD_REASONS["nightly_limit"])


def test_chat_commands_are_only_in_the_footer_for_carl():
    text = daf_sync.email_text([section([ADDED], [HELD])])
    body, footer = text.split("— Jidhr")
    for command in ("new daf", "mark inquiry handled", "Jidhr chat"):
        assert command not in body, command
    assert footer.lstrip("\n").startswith("-----\n(for Carl)")
    assert '"mark inquiry handled daf <id>"' in footer
    assert "Ben Two: s2" in footer


def test_no_footer_when_nothing_is_held():
    text = daf_sync.email_text([section([ADDED], [])])
    assert "(for Carl)" not in text and "Jidhr chat" not in text


def test_the_deferred_are_listed_and_counted():
    s = section([], [], deferred=[DEFERRED])
    text = daf_sync.email_text([s])
    assert "Deferred to tomorrow night (1)" in text
    assert "Cy Four" in text
    assert "Jidhr will try them again tomorrow night by itself." in text
    assert "(for Carl)" not in text, "nothing for Carl to do about these"
    assert daf_sync.subject([s], WHEN) == \
        "New donor inquiries added to CSuite — Thu Oct 22 (1 deferred)"


def test_an_all_deferred_night_still_sends_the_email(monkeypatch):
    sent = []
    monkeypatch.setattr(Config, "JIDHR_DAF_REPORT_TO", ("r@example.invalid",),
                        raising=False)
    import clients.notify as notify
    monkeypatch.setattr(notify, "send_report",
                        lambda to, subj, body: sent.append(subj) or
                        {"sent": list(to), "failed": []})
    daf_sync.send_email([section([], [], deferred=[DEFERRED])], WHEN)
    assert sent == ["New donor inquiries added to CSuite — Thu Oct 22 "
                    "(1 deferred)"]


# --- V5.95: an open task already there --------------------------------------------------

OPEN_1099 = {"task_id": 1099, "o": "profile", "id": 21999,
             "created_date": "2026-10-23 10:00:00", "complete_date": None}
BACK_EMAIL = "returning.donor@example.invalid"


@pytest.mark.parametrize("mode", ["preview", "live"])
def test_an_open_task_on_the_profile_means_no_second_one(world, mode):
    client = Client(existing={BACK_EMAIL: [21999]}, task_rows=[OPEN_1099])
    out = run(Hub([BACK]), client, mode=mode)

    assert client.writes() == [] and out["added"] == []
    assert out["held"] == [] and out["deferred"] == []
    [found] = out["existing_task"]
    assert found["open_task"] == {"task_id": 1099, "created": "2026-10-23"}
    assert found["existing_profile"] == 21999
    assert out["budget_needed"] == 0, "no write proposed, no budget spent"
    assert world["holds"] == [], "nothing recorded: shown again tomorrow"


def test_a_completed_or_unlinked_task_does_not_count(world):
    done = dict(OPEN_1099, complete_date="2026-10-24 09:00:00")
    other = dict(OPEN_1099, task_id=1100, id=55555)
    unlinked = dict(OPEN_1099, task_id=1101, o=None)
    client = Client(existing={BACK_EMAIL: [21999]},
                    task_rows=[done, other, unlinked])
    out = run(Hub([BACK]), client)
    assert out["existing_task"] == []
    assert client.writes() == ["task/create"]


def test_an_unreadable_task_list_defers(world):
    client = Client(existing={BACK_EMAIL: [21999]}, tasks_unreadable=True)
    out = run(Hub([BACK]), client)
    assert client.writes() == []
    assert [e["reason"] for e in out["deferred"]] == ["task_check_failed"]
    assert world["holds"] == []


def test_a_short_task_list_is_not_trusted():
    from sync import open_tasks

    class Short:
        def _request(self, endpoint, data=None):
            return {"success": 1, "data": {"count": 1500, "results": []}}

    found, problem = open_tasks.open_tasks_on_profile(Short(), 21999)
    assert found == [] and "0 of 1500" in problem


def test_one_task_list_read_serves_the_whole_night(world):
    b2 = sub("back0002-dddd", "Second", "Return", "second@example.invalid",
             AFTER + 1000)
    client = Client(existing={BACK_EMAIL: [21999],
                              "second@example.invalid": [22000]})
    run(Hub([BACK, b2]), client, mode="preview")
    assert [s["endpoint"] for s in client.sent].count("task/list") == 1


def test_a_new_donor_is_never_asked_about_tasks(world):
    client = Client()
    run(Hub([FRESH]), client)
    assert "task/list" not in [s["endpoint"] for s in client.sent]


def test_the_email_shows_the_open_task_and_counts_it():
    entry = dict(ADDED, submission_id="s5", name="Dee Five", plan="returning",
                 existing_profile=21999,
                 open_task={"task_id": 1099, "created": "2026-10-23"})
    s = section([], [], mode="preview", existing_task=[entry])
    text = daf_sync.email_text([s])
    assert "Already has an open CSuite task (1)" in text
    assert ("Existing open task 1099 (created Fri Oct 23) on CSuite profile "
            "21999, so no new task was proposed.") in text
    body = text.split("— Jidhr")[0]
    assert "mark inquiry" not in body
    assert "Dee Five: s5" in text.split("(for Carl)")[1]
    assert daf_sync.subject([s], WHEN) == \
        "[Preview] New donor inquiries for CSuite — Thu Oct 22 " \
        "(1 has an open task)"


def test_a_returning_donor_is_a_follow_up_task_not_added():
    back = dict(ADDED, submission_id="s6", plan="returning",
                existing_profile=21999)
    s = section([ADDED, back, dict(back, submission_id="s7")], [],
                mode="preview")
    assert daf_sync.subject([s], WHEN) == \
        "[Preview] New donor inquiries for CSuite — Thu Oct 22 " \
        "(1 would be added, 2 follow-up tasks)"
    text = daf_sync.email_text([s])
    assert "Would be added (1)" in text
    assert "Would get a follow-up task (2)" in text


def test_the_subject_counts_every_part():
    back = dict(ADDED, submission_id="s6", plan="returning")
    opened = dict(back, submission_id="s8",
                  open_task={"task_id": 1, "created": None})
    s = section([ADDED, back], [HELD, dict(HELD, submission_id="s9")],
                existing_task=[opened, dict(opened, submission_id="s10")],
                deferred=[DEFERRED])
    assert daf_sync.subject([s], WHEN) == (
        "New donor inquiries added to CSuite — Thu Oct 22 (1 added, 1 "
        "follow-up task, 2 need Carl, 2 have an open task, 1 deferred)")


def test_the_task_title_capitalises_the_name_but_the_body_keeps_it(world):
    lower = sub("lower001-eeee", "mustafa", "al-hassan van dyke",
                "lower@example.invalid", AFTER)
    out = run(Hub([lower]), Client(), mode="preview")
    [entry] = out["added"]
    assert entry["task_title"] == ("DAF inquiry follow-up: Mustafa Al-hassan "
                                   "Van Dyke — lower@example.invalid")
    assert entry["name"] == "mustafa al-hassan van dyke"
    assert "1. mustafa al-hassan van dyke —" in daf_sync.email_text([out])


def test_an_open_task_seen_only_at_the_write_still_stops_it(world):
    class Racing(Client):
        """No open task at the plan; one at the write-time re-check."""
        def _request(self, endpoint, data=None, **kwargs):
            if endpoint == "task/list":
                self.task_rows = () if not getattr(self, "_seen", 0) \
                    else (OPEN_1099,)
                self._seen = getattr(self, "_seen", 0) + 1
            return super()._request(endpoint, data, **kwargs)

    client = Racing(existing={BACK_EMAIL: [21999]})
    out = run(Hub([BACK]), client)
    assert client.writes() == []
    assert [e["open_task"]["task_id"] for e in out["existing_task"]] == [1099]
    assert out["held"] == []
