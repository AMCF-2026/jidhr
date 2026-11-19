"""Chat "new endowment" counts only the endowment step's OWN open tasks
(intents/endowment_inquiry.own_open_tasks), as the nightly step does, and
writes nothing when that check cannot be completed. The DAF chat is
unchanged, pinned against b0be280.

Test doubles only (tests/test_endowment_org.py, tests/test_daf_sync.py);
no network, no database.
"""

import json

import pytest

from intents import daf_workflow
from tests.test_endowment_org import (WRITES, Client, Hub, _existing_org,
                                      offer_and_create, open_task, world)
# noqa above: `world` is the endowment chat fixture.

PIN = "b0be280"


def writes(client):
    return [e for e in client.endpoints() if e in WRITES]


def displayed(client):
    return [s["body"]["task_id"] for s in client.sent
            if s["endpoint"] == "task/display"]


# --- a check that can't be completed writes nothing -------------------------------

def test_a_failed_display_read_at_the_plan_stops_and_writes_nothing(world):
    _existing_org(world)
    client = Client()
    client.task_rows = (open_task("2030-03-16"),)
    client.task_types = {1067: "fail"}
    _review, reply, state, client = offer_and_create(client=client)
    assert "couldn't be checked" in reply and "Nothing was written" in reply
    assert writes(client) == []
    assert state.get("step") == "done"
    assert world["records"] == []


def test_a_failed_display_read_at_the_write_writes_nothing(world):
    """Not even the EIN or the note: the check runs before any write."""
    _existing_org(world)
    client = Client()                 # profile 18075 has no EIN: EIN_SET
    hub = Hub()
    state = daf_workflow.default_workflow_state()
    daf_workflow._initiate_workflow("new endowment", state, hub)
    plan = daf_workflow._handle_active_workflow("yes", state, hub, client)
    assert "`task/create`" in plan and "profile/edit" in plan
    client.task_rows = (open_task("2030-03-16"),)    # appears, unreadable
    client.task_types = {1067: "fail"}
    reply = daf_workflow._handle_active_workflow("yes", state, hub, client)
    assert "couldn't be checked" in reply and "Nothing was written" in reply
    assert writes(client) == []
    assert world["records"] == []


def test_an_unreadable_task_list_also_writes_nothing(world):
    """The same rule as the nightly: any incomplete check stops it."""
    _existing_org(world)

    class NoList(Client):
        def _request(self, endpoint, data=None, **kwargs):
            if endpoint == "task/list":
                return {"success": False, "error": "HTTP 503"}
            return super()._request(endpoint, data, **kwargs)

    _review, reply, _state, client = offer_and_create(client=NoList())
    assert "couldn't be checked" in reply
    assert writes(client) == []


# --- whose open tasks count ----------------------------------------------------

def test_the_assignees_endowment_task_still_covers(world):
    _existing_org(world)
    client = Client()
    client.task_rows = (open_task("2030-03-16"),)
    _review, reply, _state, client = offer_and_create(client=client)
    assert "task/create" not in client.endpoints()
    assert "open task 1067" in reply
    assert displayed(client) == [1067, 1067]   # the plan, then the write


def test_another_assignees_task_is_never_displayed_and_never_counts(world):
    _existing_org(world)
    client = Client()
    client.task_rows = (open_task("2030-03-16", 1068, employee=1012),)
    _review, _reply, _state, client = offer_and_create(client=client)
    assert "task/create" in client.endpoints()
    assert displayed(client) == []


def test_the_assignees_task_of_another_type_does_not_count(world):
    _existing_org(world)
    client = Client()
    client.task_rows = (open_task("2030-03-16", 1069),)
    client.task_types = {1069: 1000}
    _review, _reply, _state, client = offer_and_create(client=client)
    assert "task/create" in client.endpoints()


# --- the DAF chat is unchanged (pinned against b0be280) ----------------------------

def test_the_daf_chat_modules_are_byte_identical_to_the_pin():
    """intents/daf_workflow.py byte for byte. sync/open_tasks.py left this
    list when the endowment step began naming its assignee (V5.104): it
    gained a field that only the endowment step asks for, and DAF's use of
    it is pinned by behaviour instead (tests/test_endowment_assignee.py)."""
    import subprocess

    found = subprocess.run(
        ["git", "diff", "--stat", PIN, "--", "intents/daf_workflow.py"],
        capture_output=True, text=True)
    assert found.returncode == 0, found.stderr
    assert found.stdout.strip() == "", found.stdout


@pytest.mark.parametrize("spec", [
    [("2027-01-10", 1099, 1002)],
    [("2026-09-01", 1099, 1007), ("2027-01-10", 1100, 1012)],
    [("2026-08-01", 1099, 1002), ("2026-10-02", 1100, 1012)]])
def test_the_daf_chat_write_sequence_matches_the_pin(spec, monkeypatch):
    """run_daf_writes (what the DAF chat runs after its yes) with open tasks
    on a returning donor: the same results through b0be280's modules."""
    from tests import test_daf_sync as t
    from tests.test_open_task_rules import old_module, use_modules

    def once():
        from intents import daf_workflow as wf

        state = {"processed": {}, "held": {}, "holds": [], "audit": []}
        rows = tuple(dict(t.open_task(created, task_id),
                          employee_name_link_id=employee)
                     for created, task_id, employee in spec)
        client = t.Client(existing={t.BACK_EMAIL: [21999]}, task_rows=rows)
        row = wf.inquiry_form("daf")
        data = wf._parse_submission(t.BACK, row["field_map"])
        wf_state = {"active": True, "workflow_type": "daf", "type": "daf",
                    "step": "confirm", "form_id": row["form_id"],
                    "submission_data": data, "profile_id": None,
                    "funit_id": None, "ticket_id": None}
        results = wf.run_daf_writes(wf_state, t.Hub([t.BACK]), client,
                                    allow_contact_create=False)
        return {"results": json.loads(json.dumps(results, default=str)),
                "sent": [s["endpoint"] for s in client.sent]}

    with monkeypatch.context() as m:
        _daf_world(m)
        now = once()
    old = {"sync.open_tasks": old_module("sync.open_tasks",
                                         "sync/open_tasks.py", PIN),
           "intents.daf_workflow": old_module("intents.daf_workflow",
                                              "intents/daf_workflow.py",
                                              PIN)}
    with monkeypatch.context() as m:
        _daf_world(m)
        for module in old.values():
            if hasattr(module, "Config"):
                module.Config = t.Config
        use_modules(m, old)
        then = once()
    assert now == then
    # Not two empty answers: the open task was found and judged.
    assert now["results"].get("task_existing"), now["results"]
    assert "task/list" in now["sent"]


def _daf_world(m):
    """The DAF test settings and audit doubles, on a monkeypatch context."""
    import uuid

    import clients.csuite as mod
    import sync.filter_trust as trust
    from clients import audit
    from tests import test_daf_sync as t

    for name, value in (("CSUITE_ENV", "live"), ("CSUITE_WRITE_BUDGET", 10),
                        ("CSUITE_DAF_CREATE_ENABLED", True),
                        ("CSUITE_DAF_TASK_CREATE_ENABLED", True),
                        ("CSUITE_DAF_FUND_CREATE_ENABLED", False),
                        ("CSUITE_HUBSPOT_BACKFILL_ENABLED", False),
                        ("CSUITE_TICKET_CLOSE_ENABLED", False),
                        ("CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", 1012),
                        ("CSUITE_TASK_TYPE_ID", 1065)):
        m.setattr(t.Config, name, value)
    m.setattr(mod, "reserve_write", lambda *a, **k: {"id": 1})
    m.setattr(mod, "complete_write", lambda *a, **k: None)
    m.setattr(audit, "record_write", lambda *a, **k: True)
    m.setattr(audit, "processed_submissions", lambda form_id, **k: {})
    m.setattr(audit, "held_submissions", lambda form_id: {})
    m.setattr("sync.daf_reconcile.link_rows_allowed", lambda: True)
    m.setattr(trust.uuid, "uuid4", lambda: uuid.UUID(int=0x5EED))


# --- several open tasks, both chats -----------------------------------------------

def _daf_chat(monkeypatch, rows):
    """The DAF chat's write sequence for a returning donor with `rows`."""
    from intents import daf_workflow as wf
    from tests import test_daf_sync as t

    recorded = []
    with monkeypatch.context() as m:
        _daf_world(m)
        from clients import audit
        m.setattr(audit, "record_write",
                  lambda system, method, endpoint, **k:
                  recorded.append(endpoint) or True)
        client = t.Client(existing={t.BACK_EMAIL: [21999]}, task_rows=rows)
        row = wf.inquiry_form("daf")
        data = wf._parse_submission(t.BACK, row["field_map"])
        state = {"active": True, "workflow_type": "daf", "type": "daf",
                 "step": "confirm", "form_id": row["form_id"],
                 "submission_data": data, "profile_id": None,
                 "funit_id": None, "ticket_id": None}
        results = wf.run_daf_writes(state, t.Hub([t.BACK]), client,
                                    allow_contact_create=False)
    return results, client, recorded


def test_daf_chat_a_recent_task_beside_an_old_one_covers(monkeypatch):
    from tests.test_daf_sync import open_task as daf_task

    results, client, recorded = _daf_chat(
        monkeypatch, (daf_task("2026-09-01", 1099),
                      daf_task("2027-01-10", 1100)))
    found = results["task_existing"]
    assert found["task_id"] == 1100 and found["verdict"] == "within"
    assert "task/create" not in client.writes()
    assert recorded == ["submission/link/existing_open_task"]


def test_daf_chat_all_tasks_old_is_held_naming_the_newest(monkeypatch):
    from tests.test_daf_sync import open_task as daf_task

    results, client, recorded = _daf_chat(
        monkeypatch, (daf_task("2026-08-01", 1099),
                      daf_task("2026-11-20", 1100),
                      daf_task("2026-10-02", 1101)))
    found = results["task_existing"]
    assert found["verdict"] == "stale" and found["task_id"] == 1100
    assert found["text"].startswith("The newest of the profile's 3 open "
                                    "tasks is open task 1100")
    assert "task/create" not in client.writes()
    assert recorded == ["submission/held/stale_open_task"]


def test_endowment_chat_a_recent_task_beside_an_old_one_covers(world):
    _existing_org(world)
    client = Client()
    client.task_rows = (open_task("2029-06-01", 1066),
                        open_task("2030-03-16", 1067))
    _review, reply, _state, client = offer_and_create(client=client)
    assert "task/create" not in client.endpoints()
    assert "open task 1067" in reply and "recorded as handled" in reply
    assert "1066" not in reply


def test_endowment_chat_all_tasks_old_is_held_naming_the_newest(world):
    _existing_org(world)
    client = Client()
    client.task_rows = (open_task("2029-06-01", 1066),
                        open_task("2029-09-01", 1067))
    _review, reply, _state, client = offer_and_create(client=client)
    assert "task/create" not in client.endpoints()
    assert "The newest of the profile's 2 open tasks is open task 1067" \
        in reply
    assert [r["endpoint"] for r in world["records"]] == \
        ["submission/held/stale_open_task"]


def test_endowment_chat_another_assignees_daf_follow_up_does_not_cover(
        world):
    """Munira's recent DAF follow-up (type 1065, the type DAF also uses):
    not the endowment assignee's, so it covers nothing and Ola gets the
    task."""
    _existing_org(world)
    client = Client()
    client.task_rows = (open_task("2030-03-16", 1070, employee=1012),)
    _review, reply, _state, client = offer_and_create(client=client)
    assert "task/create" in client.endpoints()
    task = client.body("task/create")
    assert task["employee_id"] == 1002 and task["id"] == 18075
    assert displayed(client) == []
    assert not [r for r in world["records"]
                if "existing_open_task" in r["endpoint"]]


# --- an assignee that isn't a whole-number employee id is named as such -----------

def _set(monkeypatch, name, value):
    """On every Config the modules hold: another test may reload config."""
    import config
    from intents import endowment_inquiry
    from sync import daf_sync, endowment_sync
    for holder in {config.Config, endowment_inquiry.Config,
                   endowment_sync.Config, daf_sync.Config}:
        monkeypatch.setattr(holder, name, value, raising=False)


UNSET_REFUSAL = ("CSUITE_ENDOWMENT_ASSIGNEE is not set. It waits on Carl "
                 "confirming in the CSuite UI who employee 1002 is; no id is "
                 "guessed. Nothing was written.")


@pytest.mark.parametrize("raw", ["Ola", "1002.0", "Mohamed, Ola", "0"])
def test_a_value_that_is_not_an_id_is_named_in_every_message(world,
                                                              monkeypatch,
                                                              raw):
    from config import Config
    from intents import endowment_inquiry as ei
    from sync import endowment_sync
    from sync.daf_sync import Refused

    _set(monkeypatch, "CSUITE_ENDOWMENT_ASSIGNEE", None)
    _set(monkeypatch, "CSUITE_ENDOWMENT_ASSIGNEE_RAW", raw)
    words = f"is set to {raw!r}, which isn't a whole-number employee id"

    review, reply, _state, client = offer_and_create()
    assert words in review and "is not set" not in review
    assert words in reply and UNSET_REFUSAL not in reply
    assert writes(client) == []

    from clients import audit
    monkeypatch.setattr(audit, "held_submissions", lambda form_id: {})
    _set(monkeypatch, "JIDHR_ENDOWMENT_SINCE", "2030-01-01")
    _set(monkeypatch, "CSUITE_WRITE_BUDGET", 10)
    _set(monkeypatch, "CSUITE_WRITE_BUDGET_RAW", "10")
    with pytest.raises(Refused) as caught:
        endowment_sync.run(hubspot=Hub(), csuite=Client(), run_mode="live")
    assert words in str(caught.value)

    _existing_org(world)
    out = endowment_sync.run(hubspot=Hub(), csuite=Client(),
                             run_mode="preview")
    block = "\n".join(endowment_sync.section_text(out))
    assert f"Open-task check skipped: CSUITE_ENDOWMENT_ASSIGNEE {words}" \
        in block
    assert ei.assignee() is None          # text only: still no assignee


def test_unset_keeps_every_message_as_it_was(world, monkeypatch):
    from config import Config
    from sync import endowment_sync
    from sync.daf_sync import Refused

    _set(monkeypatch, "CSUITE_ENDOWMENT_ASSIGNEE", None)
    _set(monkeypatch, "CSUITE_ENDOWMENT_ASSIGNEE_RAW", "")
    review, reply, _state, _client = offer_and_create()
    assert ("Assignee not confirmed: CSUITE_ENDOWMENT_ASSIGNEE is not set, "
            "so create will refuse.") in review
    assert UNSET_REFUSAL in reply

    from clients import audit
    monkeypatch.setattr(audit, "held_submissions", lambda form_id: {})
    _set(monkeypatch, "JIDHR_ENDOWMENT_SINCE", "2030-01-01")
    _set(monkeypatch, "CSUITE_WRITE_BUDGET", 10)
    _set(monkeypatch, "CSUITE_WRITE_BUDGET_RAW", "10")
    with pytest.raises(Refused) as caught:
        endowment_sync.run(hubspot=Hub(), csuite=Client(), run_mode="live")
    assert str(caught.value) == (
        "JIDHR_ENDOWMENT_MODE is live but CSUITE_ENDOWMENT_ASSIGNEE is not "
        "set on this service, so the nightly endowment step wrote nothing.")
    _existing_org(world)
    out = endowment_sync.run(hubspot=Hub(), csuite=Client(),
                             run_mode="preview")
    assert ("Open-task check skipped: CSUITE_ENDOWMENT_ASSIGNEE isn't set, "
            "so whose open tasks count can't be told. Live would check "
            "first.") in "\n".join(endowment_sync.section_text(out))
