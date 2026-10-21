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
                 fail_profile=False):
        self.api_key, self.api_secret = "k", "s"
        self.base_url = "https://amuslimcf.fcsuite.com/api/v2"
        self.env = "live"
        self.verify_writes = False
        self.write_budget = WriteBudget(budget)
        self.sent = []
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
    (lambda: (Hub([FRESH], unreadable={"fresh.donor@example.invalid"}),
              Client()), "hubspot_unreadable"),
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
    (lambda: (Hub([FRESH]),
              Client(unreadable={"fresh.donor@example.invalid"})),
     "check_failed"),
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

def test_the_budget_runs_out_and_the_rest_are_held_not_dropped(world):
    a = sub("aaaa0001", "Ann", "One", "a@example.invalid", AFTER)
    b = sub("bbbb0002", "Ben", "Two", "b@example.invalid", AFTER + 1000)
    hub, client = Hub([b, a]), Client(budget=3)
    out = run(hub, client)

    assert [e["submission_id"] for e in out["added"]] == ["aaaa0001"], \
        "oldest first"
    [held] = out["held"]
    assert held["submission_id"] == "bbbb0002"
    assert held["reason"] == "nightly_limit"
    assert "Nightly limit reached" in held["why"]
    assert client.write_budget.used == 2


def test_preview_reports_what_the_budget_would_have_allowed(world,
                                                            monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_WRITE_BUDGET", 3)
    a = sub("aaaa0001", "Ann", "One", "a@example.invalid", AFTER)
    b = sub("bbbb0002", "Ben", "Two", "b@example.invalid", AFTER + 1000)
    out = run(Hub([a, b]), Client(), mode="preview")

    assert out["budget"] == 3
    assert out["budget_needed"] == 4 and out["budget_allowed"] == 2
    assert out["held"][0]["reason"] == "nightly_limit"


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
                 'To handle it in Jidhr chat: "new daf", or "mark inquiry '
                 'handled daf s2" if it has already been dealt with.',
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
