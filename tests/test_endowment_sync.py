"""The nightly endowment step (sync/endowment_sync.py) and its block in the
DAF morning email.

CSuite is the REAL CSuiteClient._request over a faked transport (the
doubles in tests/test_endowment_org.py); HubSpot, the mirror and the audit
store are test doubles. Every value is invented; the EIN, goal and essays
are marker strings so the email and logs can be searched for them. No
network, no database.
"""

import ast
import json
import logging
import pathlib

import pytest

from clients import audit
from config import Config
from sync import daf_sync, endowment_sync, org_match
from sync.sandbox_writes import WriteBudget
from tests.test_endowment_org import (DAF, EIN, FORM, MIRROR_SYNCED, SECRETS,
                                      Client, Hub, endowment_submission)

WRITE_ENDPOINTS = ("profile/create/org", "note/create", "task/create",
                   "profile/edit")
EXACT = {"profile_id": "18075", "organization": "The Test Peace Academy, Inc.",
         "ptype": "org"}
CLOSE = {"profile_id": "18076",
         "organization": "Test Peace Academy of Virginia", "ptype": "org"}


class NClient(Client):
    """Client with failures on chosen endpoints, a budget, and profile/edit
    forbidden outright."""

    def __init__(self, fail=(), budget=10, **kwargs):
        super().__init__(**kwargs)
        self.write_budget = WriteBudget(budget)
        inner, outer = self.session, self

        class Session:
            def post(_self, url, data=None, **kw):
                endpoint = url.split("/api/v2/", 1)[-1]
                if endpoint == "profile/edit":
                    raise AssertionError("the nightly step called "
                                         "profile/edit")
                if endpoint in fail:
                    outer.sent.append({"endpoint": endpoint,
                                       "body": json.loads(data), "raw": data})
                    payload = {"success": 0, "errors": ["server error"]}

                    class R:
                        status_code = 200
                        text = json.dumps(payload)

                        def json(self):
                            return payload
                    return R()
                return inner.post(url, data=data, **kw)

        self.session = Session()

    def writes(self):
        return [e for e in self.endpoints() if e in WRITE_ENDPOINTS]


@pytest.fixture
def world(monkeypatch):
    import uuid

    import clients.csuite as mod
    import sync.filter_trust as trust

    state = {"processed": {}, "held": {}, "records": [], "audit": [],
             "mirror": [{"profile_id": "9648", "organization":
                         "Unrelated Charity Trust", "ptype": "org"}]}
    for name, value in (("JIDHR_ENDOWMENT_SINCE", "2030-01-01"),
                        ("JIDHR_ENDOWMENT_MODE", "live"),
                        ("JIDHR_DAF_REPORT_TO", ("carl@example.invalid",)),
                        ("CSUITE_ENV", "live"),
                        ("CSUITE_WRITE_BUDGET", 10),
                        ("CSUITE_WRITE_BUDGET_RAW", "10"),
                        ("CSUITE_ENDOWMENT_CREATE_RAW", "true"),
                        ("CSUITE_ENDOWMENT_CREATE_ENABLED", True),
                        ("CSUITE_ENDOWMENT_ASSIGNEE", 1002),
                        ("CSUITE_TICKET_CLOSE_ENABLED", False)):
        monkeypatch.setattr(Config, name, value)

    def reserve(system, method, endpoint, target_id=None, payload=None,
                sync_run_id=None):
        state["audit"].append({"endpoint": endpoint,
                               "meta": audit.payload_meta(payload)})
        return {"id": len(state["audit"]), "endpoint": endpoint,
                "sid": (payload or {}).get("hubspot_submission_id")}

    def complete(reservation, status="success", **kwargs):
        # Handled = a SUCCESSFUL profile or task create, as
        # clients/audit.processed_submissions reads it.
        if status == "success" and reservation.get("sid") and \
                reservation["endpoint"] in ("profile/create/org",
                                            "task/create"):
            state["processed"][reservation["sid"]] = "x"
        return True

    def record(system, method, endpoint, target_id=None, payload=None,
               status="success", **kwargs):
        state["records"].append({"system": system, "endpoint": endpoint,
                                 "meta": audit.payload_meta(payload)})
        sid = payload["hubspot_submission_id"]
        if endpoint.startswith("submission/held/"):
            state["held"][sid] = endpoint.rsplit("/", 1)[-1]
        if endpoint.startswith("submission/link/"):
            state["processed"][sid] = str(payload.get("linked_profile_id"))
        return True

    monkeypatch.setattr(mod, "reserve_write", reserve)
    monkeypatch.setattr(mod, "complete_write", complete)
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, endpoints=None, **k:
                        dict(state["processed"]))
    monkeypatch.setattr(audit, "held_submissions",
                        lambda form_id: dict(state["held"]))
    monkeypatch.setattr(audit, "record_write", record)
    monkeypatch.setattr(org_match, "mirror_org_rows",
                        lambda: (list(state["mirror"]), MIRROR_SYNCED))
    monkeypatch.setattr("sync.daf_reconcile.link_rows_allowed", lambda: True)
    monkeypatch.setattr(trust.uuid, "uuid4", lambda: uuid.UUID(int=0x5EED))
    return state


def hub(*subs):
    return Hub(by_form={FORM: list(subs or [endowment_submission()]),
                        DAF: []})


def run(h=None, client=None, mode="live", budget_left=None):
    return endowment_sync.run(hubspot=h or hub(), csuite=client or NClient(),
                              run_mode=mode, budget_left=budget_left)


def email(*sections):
    return daf_sync.email_text(list(sections))


# --- off, wrong, refused ---------------------------------------------------------

def test_an_unset_since_does_nothing_and_reads_nothing(world, monkeypatch):
    monkeypatch.setattr(Config, "JIDHR_ENDOWMENT_SINCE", "")

    class NoCalls:
        def __getattr__(self, name):
            raise AssertionError(f"read {name}")
    assert endowment_sync.run(hubspot=NoCalls(), csuite=NoCalls()) is None


def test_a_since_that_is_not_a_date_is_a_bad_setting(world, monkeypatch):
    monkeypatch.setattr(Config, "JIDHR_ENDOWMENT_SINCE", "Nov 14")
    with pytest.raises(daf_sync.BadSetting):
        run()


def test_the_mode_defaults_to_preview(monkeypatch):
    for raw in ("", "Preview", "lve", None):
        monkeypatch.setattr(Config, "JIDHR_ENDOWMENT_MODE", raw)
        assert endowment_sync.mode() == "preview"
    monkeypatch.setattr(Config, "JIDHR_ENDOWMENT_MODE", " LIVE ")
    assert endowment_sync.mode() == "live"


@pytest.mark.parametrize("name,value,words", [
    ("CSUITE_ENDOWMENT_CREATE_ENABLED", False,
     "CSUITE_ENDOWMENT_CREATE_ENABLED is off"),
    ("CSUITE_ENDOWMENT_ASSIGNEE", None,
     "CSUITE_ENDOWMENT_ASSIGNEE is not set")])
def test_live_needs_the_switch_and_the_assignee(world, monkeypatch, name,
                                                value, words):
    monkeypatch.setattr(Config, name, value)
    client = NClient()
    with pytest.raises(daf_sync.Refused) as caught:
        run(client=client)
    assert words in str(caught.value)
    assert client.writes() == []


def test_preview_needs_neither_and_writes_nothing(world, monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_ASSIGNEE", None)
    client = NClient()
    out = run(client=client, mode="preview")
    assert client.writes() == [] and world["records"] == []
    [entry] = out["added"]
    assert entry["plan"] == "new" and entry["writes"] == 3
    assert out["budget_needed"] == 3 and out["budget_allowed"] == 3


# --- new and matched organisations -------------------------------------------------

def test_a_new_organisation_is_three_writes_and_never_again(world):
    client = NClient()
    out = run(client=client)
    assert client.writes() == ["profile/create/org", "note/create",
                               "task/create"]
    [entry] = out["added"]
    assert (entry["profile_id"], entry["note_id"], entry["task_id"]) == \
        (40001, 303200, 2001)
    assert entry["profile_created"] and not entry.get("problems")

    again = NClient()
    second = run(client=again)
    assert again.writes() == [] and second["skipped_handled"] == 1
    assert second["added"] == []


def test_a_matched_organisation_gets_a_note_and_a_task_only(world):
    world["mirror"].append(EXACT)
    client = NClient()               # profile 18075 has no fedid
    out = run(client=client)
    assert client.writes() == ["note/create", "task/create"]
    assert client.body("task/create")["id"] == 18075
    [entry] = out["added"]
    assert entry["plan"] == "returning" and entry["profile_id"] == 18075
    assert entry["ein"] == endowment_sync.EIN_TEXT["set"]
    text = email(out)
    assert "the form has one and the CSuite profile has none" in text
    for secret in SECRETS:
        assert secret not in text


def test_a_matched_organisation_with_the_same_ein_says_nothing_of_it(world):
    world["mirror"].append(EXACT)
    client = NClient(fedids={"18075": EIN})
    out = run(client=client)
    assert client.writes() == ["note/create", "task/create"]
    assert out["added"][0]["ein"] is None
    assert "EIN" not in email(out)


def test_a_different_ein_on_the_profile_is_reported_never_written(world):
    world["mirror"].append(EXACT)
    client = NClient(fedids={"18075": "99-9999999"})
    out = run(client=client)
    assert client.writes() == ["note/create", "task/create"]
    text = email(out)
    assert endowment_sync.EIN_TEXT["mismatch"] in text
    assert "99-9999999" not in text and EIN not in text


# --- hold, defer ---------------------------------------------------------------

def test_an_ambiguous_match_is_held_and_recorded_in_live(world):
    world["mirror"].append(CLOSE)
    client = NClient()
    out = run(client=client)
    assert client.writes() == []
    [held] = out["held"]
    assert held["reason"] == "ambiguous_match" and held["recorded"]
    assert world["held"] == {"endw0001-aaaa-bbbb": "ambiguous_match"}
    assert "Test Peace Academy of Virginia" in held["why"]


def test_an_ambiguous_match_in_preview_is_shown_not_recorded(world):
    world["mirror"].append(CLOSE)
    out = run(mode="preview")
    assert out["held"][0]["reason"] == "ambiguous_match"
    assert world["records"] == [] and world["held"] == {}


def test_an_unreadable_mirror_is_deferred_not_held(world, monkeypatch):
    def down():
        raise org_match.MirrorUnavailable("no database")
    monkeypatch.setattr(org_match, "mirror_org_rows", down)
    client = NClient()
    out = run(client=client)
    assert out["deferred"][0]["reason"] == "check_failed"
    assert out["held"] == [] and world["records"] == []
    assert client.writes() == []


def test_a_missing_organisation_name_is_held(world):
    out = run(hub(endowment_submission(name="")))
    assert out["held"][0]["reason"] == "missing_fields"
    assert "organisation name" in out["held"][0]["why"]


def test_a_matched_org_with_a_recent_open_task_gets_nothing(world):
    world["mirror"].append(EXACT)
    client = NClient()
    client.task_rows = ({"task_id": 777, "o": "profile", "id": 18075,
                         "created_date": "2030-03-16"},)
    out = run(client=client)
    assert client.writes() == []
    [found] = out["existing_task"]
    assert found["open_task"]["task_id"] == 777 and found["recorded"]


@pytest.mark.parametrize("left,plan_mirror,added", [
    (2, False, False),      # new org needs 3
    (2, True, True),        # matched org needs 2
    (3, False, True)])
def test_the_full_cost_is_checked_before_each_submission(world, left,
                                                         plan_mirror, added):
    if plan_mirror:
        world["mirror"].append(EXACT)
    client = NClient(budget=left)
    out = run(client=client, budget_left=left)
    if added:
        assert len(out["added"]) == 1
    else:
        assert client.writes() == []
        assert out["deferred"][0]["reason"] == "nightly_limit"
        assert world["records"] == []


def test_the_allowance_is_what_daf_left(world):
    out = run(mode="preview", budget_left=2)
    assert out["budget"] == 2
    assert out["deferred"][0]["reason"] == "nightly_limit"


# --- partial writes ------------------------------------------------------------

@pytest.mark.parametrize("fail", ["task/create", "note/create"])
def test_a_new_profile_with_a_failed_note_or_task_needs_carl(world, fail):
    client = NClient(fail=(fail,))
    out = run(client=client)
    [entry] = out["added"]
    assert entry["profile_id"] == 40001 and entry["problems"]
    text = email(out)
    assert "Needs Carl (1)" in text
    assert "CSuite profile 40001 (new)" in text
    assert "Next night: The created profile marks this submission " \
           "handled, so Jidhr won't try it again by itself." in text

    again = NClient()
    second = run(client=again)
    assert again.writes() == [] and second["skipped_handled"] == 1


def test_a_matched_org_whose_task_fails_is_held_so_no_second_note(world):
    world["mirror"].append(EXACT)
    client = NClient(fail=("task/create",))
    out = run(client=client)
    assert client.writes() == ["note/create", "task/create"]
    [held] = out["held"]
    assert held["reason"] == "partial_write" and held["recorded"]
    assert "CSuite profile 18075" in held["why"]

    again = NClient()
    second = run(client=again)
    assert again.writes() == [] and second["skipped_held"] == 1


def test_a_new_profile_that_fails_writes_nothing_more_and_is_held(world):
    client = NClient(fail=("profile/create/org",))
    out = run(client=client)
    assert client.writes() == ["profile/create/org"]
    assert out["held"][0]["reason"] == "csuite_error"


# --- never a ticket, never profile/edit, no secrets ---------------------------------

def test_nothing_on_this_path_reads_or_touches_tickets():
    source = pathlib.Path("sync/endowment_sync.py").read_text()
    tree = ast.parse(source)
    body = tree.body[1:]       # the module docstring may say "never"
    found = [n for node in body for n in ast.walk(node)
             if (isinstance(n, ast.Name) and "ticket" in n.id.lower())
             or (isinstance(n, ast.Attribute) and "ticket" in n.attr.lower())
             or (isinstance(n, ast.Constant) and isinstance(n.value, str)
                 and "ticket" in n.value.lower())]
    assert found == []


def test_the_block_has_no_ticket_line_with_close_off(world):
    out = run()
    assert "ticket" not in "\n".join(endowment_sync.section_text(out)).lower()


def test_secrets_never_reach_the_email_or_the_logs(world, caplog):
    world["mirror"].append(CLOSE)
    sub_new = endowment_submission(conv="endw0002-cccc", org="Other Org One",
                                   ms=1900000100000)
    with caplog.at_level(logging.DEBUG):
        out = run(hub(endowment_submission(), sub_new))
    text = email(out) + daf_sync.subject([out])
    logged = " ".join(r.getMessage() for r in caplog.records)
    for secret in SECRETS + ("director@example.invalid",):
        assert secret not in text, secret
        assert secret not in logged, secret
    assert "d***@example.invalid" in text


# --- the email with DAF -----------------------------------------------------------

DAF_ADDED = {"submission_id": "s1", "submitted_at": "1800000000000",
             "name": "Ann One", "email": "a@example.invalid",
             "fund_name": "", "contribution": "", "plan": "new",
             "profile_id": 31337, "task_id": 1099, "task_due": "2027-01-19",
             "profile_created": True, "assignee": "Ahmed, Munira",
             "hubspot_link": "https://hub/x",
             "ticket": "HubSpot ticket left open (close disabled)."}


def daf_section(mode="live", **kw):
    return {"form": "daf", "label": "DAF", "mode": mode, "added": [DAF_ADDED],
            "held": [], "deferred": [], "existing_task": [],
            "budget": 10, "budget_needed": 2, "budget_allowed": 2, **kw}


def test_the_daf_only_email_is_unchanged_by_this_step():
    """email_text and subject for DAF alone, against 6d0f817's daf_sync.py
    loaded beside this one."""
    import importlib.util
    import subprocess
    import tempfile
    from datetime import datetime, timezone

    old = subprocess.run(["git", "show", "6d0f817:sync/daf_sync.py"],
                         capture_output=True, text=True, check=True).stdout
    with tempfile.TemporaryDirectory() as tmp:
        path = pathlib.Path(tmp) / "old_daf_sync.py"
        path.write_text(old)
        spec = importlib.util.spec_from_file_location("old_daf_sync", path)
        then = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(then)
    when = datetime(2027, 1, 16, 12, tzinfo=timezone.utc)
    held = {"submission_id": "s2", "submitted_at": "1800000000000",
            "name": "Bo Two", "email": "b@example.invalid", "fund_name": "",
            "contribution": "", "hubspot_link": "https://hub/y",
            "reason": "multiple_profiles", "why": "Two profiles."}
    for mode in ("live", "preview"):
        sections = [daf_section(mode, held=[held])]
        assert daf_sync.email_text(sections) == then.email_text(sections)
        assert daf_sync.subject(sections, when) == \
            then.subject(sections, when)
        assert daf_sync.carl_footer(sections) == then.carl_footer(sections)


def test_the_endowment_block_follows_the_daf_block(world):
    world["mirror"].append(CLOSE)
    endow = run()
    text = email(daf_section(), endow)
    assert text.index("DAF INQUIRIES") < text.index("ENDOWMENT INQUIRIES")
    assert "(PREVIEW)" not in text
    # Carl's footer names the endowment commands for the endowment hold.
    assert 'mark inquiry handled endowment <id>' in text
    assert "Test Peace Academy: endw0001" in text
    subject = daf_sync.subject([daf_section(), endow])
    assert "1 added" in subject and "1 needs Carl" in subject


def test_a_preview_endowment_beside_a_live_daf_is_marked(world):
    endow = run(mode="preview")
    text = email(daf_section(), endow)
    assert "ENDOWMENT INQUIRIES (PREVIEW)" in text
    assert "DAF INQUIRIES\n" in text
    assert "Part of this is a PREVIEW" in text
    subject = daf_sync.subject([daf_section(), endow])
    assert not subject.startswith("[Preview]")
    assert "1 added" in subject and "1 more in preview" in subject


# --- the job --------------------------------------------------------------------

def _job_world(monkeypatch, daf=None, daf_error=None, endowment=None,
               endowment_error=None):
    from jobs.handlers import daf_sync as job

    sent, alerts, seen = [], [], {}
    for holder in {Config, __import__("config").Config}:
        monkeypatch.setattr(holder, "SCHEDULED_SYNC_ENABLED", True,
                            raising=False)
        monkeypatch.setattr(holder, "CSUITE_WRITE_BUDGET", 10, raising=False)

    def daf_run():
        if daf_error:
            raise daf_error
        return daf

    def endow_run(budget_left=None, **k):
        seen["budget_left"] = budget_left
        if endowment_error:
            raise endowment_error
        return endowment

    monkeypatch.setattr(daf_sync, "run", daf_run)
    monkeypatch.setattr(endowment_sync, "run", endow_run)
    monkeypatch.setattr(daf_sync, "send_email",
                        lambda sections: sent.append(sections) or
                        {"sent": ["carl@example.invalid"]})
    monkeypatch.setattr("clients.notify.safe_notify",
                        lambda *a, **k: alerts.append(a) or {})
    return job, sent, alerts, seen


DAF_OUT = {"mode": "live", "candidates": 1, "added": [], "held": [],
           "deferred": [], "existing_task": [], "skipped_handled": 0,
           "skipped_held": 0, "budget": 10, "budget_needed": 8,
           "budget_allowed": 8}


def test_the_job_without_endowment_is_as_before(monkeypatch):
    job, sent, alerts, seen = _job_world(monkeypatch, daf=dict(DAF_OUT))
    result = job.handle({})
    assert "endowment" not in result and alerts == []
    assert len(sent[0]) == 1
    assert seen["budget_left"] == 2       # what DAF left of 10


def test_the_job_puts_both_in_one_email_and_keeps_ids_only(world,
                                                           monkeypatch):
    endow = run()
    job, sent, alerts, _seen = _job_world(monkeypatch, daf=dict(DAF_OUT),
                                          endowment=endow)
    result = job.handle({})
    assert [s.get("form") for s in sent[0]] == [None, "endowment"]
    assert result["endowment"]["added_ids"] == ["endw0001-aaaa-bbbb"]
    assert "Test Peace Academy" not in json.dumps(result)


def test_an_endowment_refusal_alerts_and_the_daf_email_still_goes(
        monkeypatch):
    job, sent, alerts, _seen = _job_world(
        monkeypatch, daf=dict(DAF_OUT),
        endowment_error=daf_sync.Refused("switch off"))
    result = job.handle({})
    assert "endowment" not in result and len(sent[0]) == 1
    assert alerts and alerts[0][0] == "nightly endowment step did not run"


def test_an_endowment_crash_does_not_stop_the_daf_email(monkeypatch):
    job, sent, alerts, _seen = _job_world(
        monkeypatch, daf=dict(DAF_OUT), endowment_error=KeyError("boom"))
    job.handle({})
    assert len(sent[0]) == 1
    assert alerts[0][0] == "nightly endowment step failed"


def test_a_refused_daf_still_reports_endowment_then_fails(world,
                                                          monkeypatch):
    from jobs.runner import JobFailed

    endow = run()
    job, sent, alerts, seen = _job_world(
        monkeypatch, daf_error=daf_sync.Refused("no since"),
        endowment=endow)
    with pytest.raises(JobFailed) as caught:
        job.handle({})
    assert [s["form"] for s in sent[0]] == ["endowment"]
    assert caught.value.result["endowment"]["added_ids"]
    assert seen["budget_left"] is None
    assert alerts[0][0] == "nightly DAF step did not run"
