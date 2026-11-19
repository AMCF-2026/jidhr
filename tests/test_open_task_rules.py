"""Open-task rules shared by DAF and endowment: every open task on the
profile is judged (sync/open_tasks.decide).

  * Any task created within 30 days of the inquiry, or after it, covers it;
    the NEWEST such task is the one recorded.
  * Held only when every open task is older; the line names the newest and
    how many there are.

Single-task results are pinned against e4e8b87, where the first task (the
oldest) was the only one judged: with one task the two rules agree, and the
old modules are loaded from git and run side by side with the new ones.
Test doubles only; no network, no database.
"""

import importlib.util
import json
import subprocess
import sys
import tempfile
import types
from pathlib import Path

import pytest

from sync import open_tasks as ot
from tests.test_daf_sync import (BACK, BACK_EMAIL, Client, Hub, open_task,
                                 run, world)  # noqa: F401  (world: fixture)

PIN = "e4e8b87"
BACK_SUBMITTED = "1800000000000"          # 2027-01-15


def old_module(name, path, sha=PIN):
    """`path` at `sha`, loaded as a module called `name`."""
    source = subprocess.run(["git", "show", f"{sha}:{path}"],
                            capture_output=True, text=True,
                            check=True).stdout
    with tempfile.TemporaryDirectory() as tmp:
        file = Path(tmp) / Path(path).name
        file.write_text(source)
        spec = importlib.util.spec_from_file_location(name, file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


def use_modules(monkeypatch, modules):
    """Put `modules` ({dotted name: module}) in place, packages included."""
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
        parent, _, leaf = name.rpartition(".")
        monkeypatch.setattr(sys.modules[parent], leaf, module)


# --- decide() against e4e8b87's judge() on the first task -------------------------

def task(created, task_id=1099):
    return {"task_id": task_id, "created": created}


SINGLE = [task("2027-01-10"), task("2026-12-16"), task("2026-12-15"),
          task("2027-01-20"), task(None), task("not a date"),
          task("2025-01-01")]


@pytest.mark.parametrize("one", SINGLE)
@pytest.mark.parametrize("recorded,preview", [(True, False), (False, False),
                                              (True, True)])
def test_one_task_decides_exactly_as_before(one, recorded, preview):
    old = old_module("old_open_tasks", "sync/open_tasks.py")
    verdict, days = old.judge(one, BACK_SUBMITTED)
    then = old.outcome_text(verdict, one, BACK_SUBMITTED, recorded=recorded,
                            preview=preview)

    decided = ot.decide([one], BACK_SUBMITTED)
    assert (decided["verdict"], decided["days"], decided["task"]) == \
        (verdict, days, one)
    assert decided["count"] == 1
    assert ot.outcome_text(decided["verdict"], decided["task"],
                           BACK_SUBMITTED, recorded=recorded,
                           preview=preview,
                           count=decided["count"]) == then


def test_no_tasks_is_none():
    assert ot.decide([], BACK_SUBMITTED) is None


# --- DAF end to end, one task: identical to e4e8b87 --------------------------------

def _night(created, mode):
    client = Client(existing={BACK_EMAIL: [21999]},
                    task_rows=(open_task(created),))
    out = run(Hub([BACK]), client, mode=mode)
    from sync import daf_sync
    return {"out": json.loads(json.dumps(out, default=str)),
            "sent": [s["endpoint"] for s in client.sent],
            "email": daf_sync.email_text([out])}


@pytest.mark.parametrize("created", ["2027-01-10", "2026-12-16",
                                     "2026-12-15", "2025-06-01"])
@pytest.mark.parametrize("mode", ["live", "preview"])
def test_a_daf_night_with_one_open_task_is_unchanged(world, monkeypatch,
                                                     created, mode):
    now = _night(created, mode)
    holds_now = list(world["holds"])
    world["holds"].clear()
    world["held"].clear()
    world["processed"].clear()

    old = {"sync.open_tasks": old_module("sync.open_tasks",
                                         "sync/open_tasks.py"),
           "sync.daf_sync": old_module("sync.daf_sync", "sync/daf_sync.py"),
           "intents.daf_workflow": old_module("intents.daf_workflow",
                                              "intents/daf_workflow.py")}
    from tests.test_daf_sync import Config as patched
    for module in old.values():
        # Another test may have reloaded `config`: use the Config the
        # fixture set up, as the current modules do.
        if hasattr(module, "Config"):
            module.Config = patched
    with monkeypatch.context() as m:
        use_modules(m, old)
        from tests import test_daf_sync as t
        m.setattr(t, "daf_sync", old["sync.daf_sync"])
        client = Client(existing={BACK_EMAIL: [21999]},
                        task_rows=(open_task(created),))
        out = old["sync.daf_sync"].run(hubspot=Hub([BACK]), csuite=client,
                                       run_mode=mode)
        then = {"out": json.loads(json.dumps(out, default=str)),
                "sent": [s["endpoint"] for s in client.sent],
                "email": old["sync.daf_sync"].email_text([out])}
    assert now == then
    assert holds_now == world["holds"]


# --- several open tasks -----------------------------------------------------------

def test_a_recent_task_covers_even_beside_an_old_one(world):
    """e4e8b87 judged only the oldest (1099, stale) and HELD this."""
    client = Client(existing={BACK_EMAIL: [21999]},
                    task_rows=(open_task("2026-09-01", 1099),
                               open_task("2027-01-10", 1100)))
    out = run(Hub([BACK]), client)
    assert out["held"] == []
    [found] = out["existing_task"]
    assert found["open_task"]["task_id"] == 1100 and found["recorded"]
    assert client.writes() == []


def test_the_newest_covering_task_is_the_one_recorded(world):
    client = Client(existing={BACK_EMAIL: [21999]},
                    task_rows=(open_task("2027-01-02", 1100),
                               open_task("2027-01-12", 1101),
                               open_task("2027-01-08", 1102)))
    out = run(Hub([BACK]), client)
    assert out["existing_task"][0]["open_task"]["task_id"] == 1101
    [link] = [h for h in world["holds"]
              if h["endpoint"].startswith("submission/link/")]
    assert str(link["meta"]["ids"]["linked_task_id"]) == "1101"


def test_all_old_is_held_naming_the_newest_and_the_count(world):
    client = Client(existing={BACK_EMAIL: [21999]},
                    task_rows=(open_task("2026-08-01", 1099),
                               open_task("2026-11-20", 1100),
                               open_task("2026-10-02", 1101)))
    out = run(Hub([BACK]), client)
    [held] = out["held"]
    assert held["reason"] == "stale_open_task"
    assert held["why"].startswith(
        "The newest of the profile's 3 open tasks is open task 1100 "
        "(created Fri Nov 20, 2026)")
    assert "only open task" not in held["why"]


def test_an_unreadable_date_never_covers_but_a_recent_task_does():
    decided = ot.decide([task(None, 1), task("2027-01-14", 2)],
                        BACK_SUBMITTED)
    assert (decided["verdict"], decided["task"]["task_id"]) == (ot.WITHIN, 2)
    decided = ot.decide([task(None, 1), task("2025-01-01", 2)],
                        BACK_SUBMITTED)
    assert decided["verdict"] == ot.STALE
    assert decided["task"]["task_id"] == 2       # newest readable date


def test_a_task_created_after_the_inquiry_covers_it():
    decided = ot.decide([task("2025-01-01", 1), task("2027-02-01", 2)],
                        BACK_SUBMITTED)
    assert (decided["verdict"], decided["task"]["task_id"]) == (ot.WITHIN, 2)


def test_same_day_tasks_break_ties_by_the_higher_task_id():
    decided = ot.decide([task("2027-01-10", 7), task("2027-01-10", 9)],
                        BACK_SUBMITTED)
    assert decided["task"]["task_id"] == 9
