"""The endowment nightly names its assignee, and stops for the night when
CSuite refuses that assignee.

* The block's name line comes from task/list rows the night has ALREADY
  read (employee_name beside employee_name_link_id). No read of its own.
* A task/create refused with CSuite's "is not a valid employee" (HTTP 400,
  recorded in the sandbox 2026-11-19) stops the step after that submission:
  it is reported as any failed task is, the rest get no writes and stay
  unhandled, and an alert goes out.
* DAF is unchanged, pinned against c603832.

Test doubles only; no network, no database.
"""

import json

import pytest

from config import Config
from sync import daf_sync, endowment_sync
from tests.test_endowment_org import Hub, endowment_submission
from tests.test_endowment_sync import (EXACT, FORM, DAF, NClient, email, run,
                                       world)  # noqa: F401  (world: fixture)

PIN = "c603832"
REFUSED_BODY = {"alert": {"employee_id":
                          "employee_id 999999 is not a valid employee"},
                "errors": ["employee_id: employee_id 999999 is not a valid "
                           "employee"],
                "success": 0}


def named_row(task_id, profile, employee, name, created="2030-03-16"):
    return {"task_id": task_id, "o": "profile", "id": profile,
            "created_date": created, "employee_name_link_id": employee,
            "employee_name": name}


class Refusing(NClient):
    """task/create answers HTTP 400 exactly as the sandbox did for an
    employee id that doesn't exist."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        inner, outer = self.session, self

        class Session:
            def post(_self, url, data=None, **kw):
                endpoint = url.split("/api/v2/", 1)[-1]
                if endpoint != "task/create":
                    return inner.post(url, data=data, **kw)
                outer.sent.append({"endpoint": endpoint,
                                   "body": json.loads(data), "raw": data})

                class R:
                    status_code = 400
                    text = json.dumps(REFUSED_BODY)

                    def json(self):
                        return REFUSED_BODY
                return R()

        self.session = Session()


def subs(n, start=1900000000000):
    return [endowment_submission(conv=f"endw000{i}-aaaa", org=f"New Org {i}",
                                 email=f"contact{i}@example.invalid",
                                 ms=start + i * 1000)
            for i in range(1, n + 1)]


# --- the name line --------------------------------------------------------------

def test_found_names_the_assignee_from_rows_already_read(world):
    world["mirror"].append(EXACT)              # a matched org: task/list read
    client = NClient()
    client.task_rows = (named_row(500, 4444, 1002, "Mohamed, Ola"),)
    out = run(client=client)
    reads = client.endpoints().count("task/list")
    assert "Follow-up tasks go to Mohamed, Ola (employee 1002)." in \
        email(out)
    assert out["assignee"] == {"id": 1002, "name": "Mohamed, Ola",
                               "read": True}
    # No read of its own: one plan-time list, one write-time recheck.
    assert reads == 2


def test_preview_says_would_go(world):
    world["mirror"].append(EXACT)
    client = NClient()
    client.task_rows = (named_row(500, 4444, 1002, "Mohamed, Ola"),)
    out = run(client=client, mode="preview")
    assert "Follow-up tasks would go to Mohamed, Ola (employee 1002)." in \
        email(out)
    assert client.endpoints().count("task/list") == 1


def test_not_found_says_no_open_task_is_assigned_to_the_id(world):
    world["mirror"].append(EXACT)
    client = NClient()
    client.task_rows = (named_row(500, 4444, 1007, "Zouita, Kods"),)
    out = run(client=client)
    assert ("Follow-up tasks go to employee 1002. No open CSuite task is "
            "assigned to employee 1002, so Jidhr can't show the name or "
            "confirm the id is valid.") in email(out)
    assert "Zouita" not in email(out)


def test_a_night_that_read_no_task_list_says_so(world):
    """A new organisation alone: the night never reads task/list."""
    client = NClient()
    out = run(client=client)
    assert "task/list" not in client.endpoints()
    assert ("Follow-up tasks go to employee 1002. Tonight's run didn't read "
            "CSuite's task list") in email(out)
    assert "No open CSuite task is assigned" not in email(out)


def test_preview_without_an_assignee_says_nobody_yet(world, monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_ASSIGNEE", None)
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_ASSIGNEE_RAW", "")
    out = run(mode="preview")
    assert ("Follow-up tasks would go to nobody yet: "
            "CSUITE_ENDOWMENT_ASSIGNEE isn't set.") in email(out)


# --- a refused assignee stops the night -------------------------------------------

def _hub(submissions):
    return Hub(by_form={FORM: submissions, DAF: []})


def test_a_refusal_on_the_first_of_three_leaves_the_others_untouched(world):
    client = Refusing(budget=10)
    out = run(_hub(subs(3)), client)
    # First: profile, note, then the refused task. Nothing after it.
    assert client.writes() == ["profile/create/org", "note/create",
                               "task/create"]
    assert out["stopped"]["submission_id"] == "endw0001-aaaa"
    assert "is not a valid employee" in out["stopped"]["error"]
    [first] = out["added"]
    assert first["problems"] and first["profile_id"] == 40001
    assert [e["submission_id"] for e in out["deferred"]] == \
        ["endw0002-aaaa", "endw0003-aaaa"]
    assert {e["reason"] for e in out["deferred"]} == {"assignee_refused"}
    # The later two are not recorded: tomorrow night takes them up.
    assert set(world["held"]) == set()
    assert "endw0002-aaaa" not in world["processed"]
    text = email(out)
    assert "Needs Carl (1)" in text
    assert "Tonight's run stopped before this one" in text


def test_a_refused_matched_org_is_held_as_today_and_stops_the_night(world):
    world["mirror"].append(EXACT)
    first = endowment_submission()                   # matches 18075
    later = subs(2, start=1900000500000)
    client = Refusing(budget=10)
    out = run(_hub([first] + later), client)
    assert client.writes() == ["note/create", "task/create"]
    [held] = out["held"]
    assert held["reason"] == "partial_write" and held["recorded"]
    assert [e["reason"] for e in out["deferred"]] == ["assignee_refused"] * 2
    assert out["stopped"]["submission_id"] == "endw0001-aaaa-bbbb"


def test_any_other_task_failure_does_not_stop_the_night(world):
    client = NClient(fail=("task/create",))
    out = run(_hub(subs(2)), client)
    assert "stopped" not in out
    assert client.writes().count("profile/create/org") == 2


def test_preview_never_writes_so_never_stops(world):
    client = Refusing(budget=10)
    out = run(_hub(subs(3)), client, mode="preview")
    assert client.writes() == [] and "stopped" not in out
    assert len(out["added"]) == 3


def test_the_job_alerts_when_the_step_stops(world, monkeypatch):
    from jobs.handlers import daf_sync as job

    out = run(_hub(subs(3)), Refusing(budget=10))
    alerts, sent = [], []
    for holder in {Config, __import__("config").Config}:
        monkeypatch.setattr(holder, "SCHEDULED_SYNC_ENABLED", True,
                            raising=False)
    monkeypatch.setattr(daf_sync, "run", lambda: (_ for _ in ()).throw(
        daf_sync.Refused("no since")))
    monkeypatch.setattr(endowment_sync, "run", lambda **k: out)
    monkeypatch.setattr(daf_sync, "send_email",
                        lambda sections: sent.append(sections) or {})
    monkeypatch.setattr("clients.notify.safe_notify",
                        lambda *a, **k: alerts.append(a) or {})
    from jobs.runner import JobFailed
    with pytest.raises(JobFailed) as caught:
        job.handle({})
    stopped = [a for a in alerts if "assignee isn't a valid" in a[0]]
    assert len(stopped) == 1
    assert "employee 1002" in stopped[0][1]
    assert "2 later submission(s) got no writes" in stopped[0][1]
    assert caught.value.result["endowment"]["stopped"] == "endw0001-aaaa"


# --- DAF unchanged (pinned against c603832) ----------------------------------------

@pytest.mark.parametrize("spec", [
    [("2027-01-10", 1099, 1002, "Mohamed, Ola")],
    [("2026-09-01", 1099, 1007, "Zouita, Kods"),
     ("2027-01-10", 1100, 1012, "Ahmed, Munira")],
    [("2026-08-01", 1099, 1002, "Mohamed, Ola"),
     ("2026-10-02", 1100, 1012, "Ahmed, Munira")]])
@pytest.mark.parametrize("mode", ["live", "preview"])
def test_daf_nights_are_unchanged(spec, mode, monkeypatch):
    from tests import test_daf_sync as t
    from tests.test_open_task_rules import old_module, use_modules

    rows = tuple(dict(t.open_task(created, task_id),
                      employee_name_link_id=employee, employee_name=name)
                 for created, task_id, employee, name in spec)

    def night(module):
        client = t.Client(existing={t.BACK_EMAIL: [21999]}, task_rows=rows)
        out = module.run(hubspot=t.Hub([t.BACK]), csuite=client,
                         run_mode=mode)
        return {"out": json.loads(json.dumps(out, default=str)),
                "sent": [s["endpoint"] for s in client.sent],
                "email": module.email_text([out])}

    from tests.test_endowment_chat_filter import _daf_world
    with monkeypatch.context() as m:
        _daf_world(m)
        m.setattr(t.Config, "JIDHR_DAF_SINCE", "2026-10-22")
        m.setattr(t.Config, "JIDHR_DAF_MODE", mode)
        m.setattr(t.Config, "CSUITE_WRITE_BUDGET_RAW", "10")
        m.setattr(t.Config, "CSUITE_DAF_OPPORTUNITY_CREATE_ENABLED", False)
        now = night(daf_sync)
        old = {name: old_module(name, path, PIN) for name, path in (
            ("sync.open_tasks", "sync/open_tasks.py"),
            ("sync.daf_sync", "sync/daf_sync.py"),
            ("intents.daf_workflow", "intents/daf_workflow.py"))}
        for module in old.values():
            if hasattr(module, "Config"):
                module.Config = t.Config
        use_modules(m, old)
        then = night(old["sync.daf_sync"])
    assert now == then
    assert now["sent"]            # a real night, not two empty ones
    for entry in now["out"].get("existing_task") or []:
        assert "employee_name" not in entry["open_task"]
