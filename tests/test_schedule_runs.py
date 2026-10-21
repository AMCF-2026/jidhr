"""HOTFIX-60: the schedule actually runs, and the event step is bounded.

Measured 2026-10-13, before any of this was written:

* The 06:15 cron runs only QUEUED jobs, and no registrations_sync job had
  ever been queued — so SCHEDULED_SYNC_ENABLED=true would have done nothing.
* Worse: mirror_refresh job 1 has sat 'queued' with run_after 2026-09-16
  06:00 for nearly a month. If the jidhr-jobs service were firing, it would
  have run then. Seeding cannot make an email arrive until that service
  exists and fires.
* max_attempts defaults to 3, and the runner's next-occurrence insert did
  not carry it — so a recurring job seeded with 1 reverted to 3 the next
  day, and a stopped chain would have been RETRIED.
* The event step would create 3 marketing events (1529, 1530, 1531) with
  no ceiling and no names in the email.
* 98 of 187 CSuite dates have no event_date: ALL event_name 'Unassigned',
  ALL archived = 1, ids 1052-1149 — one legacy block.

No network, no database.
"""

from datetime import datetime, timezone

import pytest

from clients import notify as notifier
from jobs.handlers import registrations_sync as handler
from jobs.runner import JobFailed
from sync import scheduled

NOW = datetime(2026, 10, 14, 6, 15, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# CHANGE 1: one recurring row, maintained without anybody typing
# ---------------------------------------------------------------------------

class Queue:
    """Stands in for the jobs table, for ensure_registrations_sync."""

    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.inserted = []

    def execute_query(self, sql, params=None, fetch=True):
        if "status IN ('queued', 'claimed', 'running')" in sql:
            return [r for r in self.rows
                    if r["job_type"] == params[0]
                    and r["status"] in ("queued", "claimed", "running")][:1]
        if sql.strip().startswith("INSERT"):
            new_id = 900 + len(self.inserted)
            self.inserted.append(params)
            self.rows.append({"id": new_id, "job_type": params[0],
                              "status": "queued", "run_after": params[3]})
            return [{"id": new_id}]
        return []


@pytest.fixture
def queue(monkeypatch):
    import scripts.jobs_run as jr

    q = Queue()
    monkeypatch.setattr(jr.database, "execute_query", q.execute_query)
    return q


def test_seeding_with_none_queues_exactly_one(queue):
    import scripts.jobs_run as jr

    message = jr.ensure_registrations_sync(now=NOW)

    assert len(queue.inserted) == 1
    job_type, payload, priority, run_after, max_attempts = queue.inserted[0]
    assert job_type == "registrations_sync"
    assert '"recurring": "daily"' in payload and '"at": "06:00"' in payload
    assert priority == 40, "before the mirror's 50 — the heartbeat goes first"
    assert max_attempts == 1, "a stopped chain is never retried"
    assert run_after == NOW, "due NOW, so the same --once claims it"
    assert "due now" in message


@pytest.mark.parametrize("status", ["queued", "claimed", "running"])
def test_seeding_is_idempotent_against_any_live_row(queue, status):
    """The runner's own dedupe looks at 'queued' only, so a seed that ran
    while the day's job was claimed or running would have added a second."""
    import scripts.jobs_run as jr

    queue.rows.append({"id": 7, "job_type": "registrations_sync",
                       "status": status, "run_after": NOW})

    message = jr.ensure_registrations_sync(now=NOW)

    assert queue.inserted == []
    assert "nothing added" in message
    assert status in message


def test_seeding_twice_still_leaves_one_row(queue):
    import scripts.jobs_run as jr

    jr.ensure_registrations_sync(now=NOW)
    jr.ensure_registrations_sync(now=NOW)
    jr.ensure_registrations_sync(now=NOW)

    assert len(queue.inserted) == 1


def test_a_finished_row_does_not_block_seeding(queue):
    """complete and failed rows are history, not a schedule."""
    import scripts.jobs_run as jr

    queue.rows += [{"id": 1, "job_type": "registrations_sync",
                    "status": "complete", "run_after": NOW},
                   {"id": 2, "job_type": "registrations_sync",
                    "status": "failed", "run_after": NOW}]

    jr.ensure_registrations_sync(now=NOW)

    assert len(queue.inserted) == 1


def test_the_mirror_row_does_not_count(queue):
    import scripts.jobs_run as jr

    queue.rows.append({"id": 1, "job_type": "mirror_refresh",
                       "status": "queued", "run_after": NOW})

    jr.ensure_registrations_sync(now=NOW)

    assert len(queue.inserted) == 1


def test_every_once_maintains_the_schedule(monkeypatch):
    """Nobody has to remember to seed it."""
    import scripts.jobs_run as jr

    called = []
    monkeypatch.setattr(jr, "ensure_registrations_sync",
                        lambda now=None: called.append(1) or "ok")
    monkeypatch.setattr(jr.runner, "run_until_empty", lambda worker: [])

    jr.run_once()

    assert called == [1]


def test_a_seed_failure_does_not_stop_the_run(monkeypatch):
    """The mirror must not be held hostage to a seed that could not write."""
    import scripts.jobs_run as jr

    ran = []
    monkeypatch.setattr(jr, "ensure_registrations_sync",
                        lambda now=None: (_ for _ in ()).throw(
                            RuntimeError("db down")))
    monkeypatch.setattr(jr.runner, "run_until_empty",
                        lambda worker: ran.append(worker) or [])

    assert jr.run_once() == jr.EXIT_OK
    assert ran, "the queued jobs still ran"


def test_seed_also_ensures_the_registrations_job(monkeypatch, capsys):
    import scripts.jobs_run as jr

    monkeypatch.setattr(jr.database, "is_configured", lambda: True)
    monkeypatch.setattr(jr, "seed", lambda now=None: "mirror ok")
    monkeypatch.setattr(jr, "ensure_registrations_sync",
                        lambda now=None: "registrations ok")
    monkeypatch.setattr(jr, "ensure_daf_sync", lambda now=None: "daf ok")

    assert jr.main(["--seed"]) == jr.EXIT_OK
    out = capsys.readouterr().out
    assert "mirror ok" in out and "registrations ok" in out
    assert "daf ok" in out


# ---------------------------------------------------------------------------
# Disabled is still silent
# ---------------------------------------------------------------------------

def test_disabled_is_still_silent(monkeypatch):
    sent = []
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_ENABLED", False)
    monkeypatch.setattr("clients.notify.notify",
                        lambda *a, **k: sent.append(a) or {})
    ran = []
    monkeypatch.setattr(scheduled, "run_chain", lambda **k: ran.append(k))

    result = handler.handle({"payload": {}})

    assert result["enabled"] is False
    assert ran == [] and sent == []


# ---------------------------------------------------------------------------
# CHANGE 2: the event-create ceiling, and names
# ---------------------------------------------------------------------------

class Mapped:
    def __init__(self, event_id):
        self.csuite_eventdate_id = event_id


ROWS = [{"event_date_id": "1529", "event_description": "AMCF Webinar A",
         "event_date": "2026-11-10", "event_name": "Event - Other"},
        {"event_date_id": "1530", "event_description": "AMCF Webinar B",
         "event_date": "2026-11-18", "event_name": "Event - Other"},
        {"event_date_id": "1531", "event_description": "AMCF Webinar C",
         "event_date": "2026-10-28", "event_name": "Event - Other"},
        {"event_date_id": "1532", "event_description": "AMCF Webinar D",
         "event_date": "2026-12-02", "event_name": "Event - Other"}]


def planned_for(ids, withheld=()):
    return {"created": len(ids) - len(withheld), "updated": 0,
            "plan": {"creates": [(Mapped(i), "new") for i in ids]},
            "withheld_rows": [(i, "past") for i in withheld], "error": None,
            "stopped": None, "run_id": 70, "outcomes": []}


@pytest.fixture
def events(monkeypatch):
    """ea.run, recording every call and whether it was a write."""
    calls = []
    state = {"ids": ["1529", "1530", "1531"]}

    def run(**kwargs):
        calls.append(kwargs)
        if kwargs.get("dry_run"):
            return planned_for(state["ids"])
        made = state["ids"][:kwargs.get("limit") or len(state["ids"])]
        return {"created": len(made), "updated": 0, "error": None,
                "stopped": None, "run_id": 71,
                "outcomes": [{"id": i, "outcome": "created"} for i in made]}

    monkeypatch.setattr("sync.event_apply.run", run)
    return calls, state


def chain_out(dry_run=True, rows=ROWS):
    return {"event_ids": [r["event_date_id"] for r in rows],
            "calendar_rows": rows, "dry_run": dry_run, "steps": {},
            "run_log_ids": [], "max_event_creates": 3}


def test_three_creates_are_within_the_ceiling(events):
    calls, _ = events
    out = chain_out()

    scheduled._step_events(out, object(), True, None)

    assert [e["id"] for e in out["event_creates"]] == ["1529", "1530",
                                                       "1531"]


def test_four_creates_refuse_and_write_nothing(events):
    """N + 1. Not "the first three and a deferral" — nothing."""
    calls, state = events
    state["ids"] = ["1529", "1530", "1531", "1532"]
    out = chain_out(dry_run=False)

    with pytest.raises(scheduled.ChainStopped) as stopped:
        scheduled._step_events(out, object(), False, None)

    assert all(c.get("dry_run") for c in calls), "no write call was made"
    assert "would create 4" in str(stopped.value)
    assert "MAX_EVENT_CREATE_PER_RUN = 3" in str(stopped.value)
    assert "NOTHING was created" in str(stopped.value)


def test_the_ceiling_refuses_in_preview_too(events):
    """A preview must predict what the live run would refuse."""
    _calls, state = events
    state["ids"] = ["1529", "1530", "1531", "1532"]

    with pytest.raises(scheduled.ChainStopped):
        scheduled._step_events(chain_out(dry_run=True), object(), True, None)


def test_a_live_write_is_capped_at_what_was_checked(events):
    """The calendar could grow between the plan and the write."""
    calls, _ = events
    out = chain_out(dry_run=False)

    scheduled._step_events(out, object(), False, None)

    writes = [c for c in calls if not c.get("dry_run")]
    assert len(writes) == 1
    assert writes[0]["limit"] == 3, "capped at the checked plan"


def test_live_reports_what_was_actually_created(events):
    calls, _ = events
    out = chain_out(dry_run=False)

    scheduled._step_events(out, object(), False, None)

    assert [e["id"] for e in out["event_creates"]] == ["1529", "1530",
                                                       "1531"]


@pytest.mark.parametrize("raw", ["", "0", "-2", "three", "  ", "3.5"])
def test_an_unusable_ceiling_is_three_never_off(monkeypatch, raw):
    monkeypatch.setenv("MAX_EVENT_CREATE_PER_RUN", raw)
    assert scheduled.max_event_creates() == 3


def test_a_valid_ceiling_is_honoured(monkeypatch):
    monkeypatch.setenv("MAX_EVENT_CREATE_PER_RUN", "5")
    assert scheduled.max_event_creates() == 5


def test_withheld_creates_are_not_counted_against_the_ceiling():
    planned = planned_for(["1529", "1530", "1531", "1155"],
                          withheld=("1155",))
    assert scheduled.would_create_ids(planned) == ["1529", "1530", "1531"]


def test_describe_names_id_date_category():
    described = scheduled.describe_events(["1529"], ROWS)

    assert described == [{"id": "1529", "name": "AMCF Webinar A",
                          "date": "2026-11-10",
                          "category": "Event - Other"}]


def test_an_id_with_no_row_is_still_listed():
    """A created event must never vanish from the email for want of a
    name."""
    described = scheduled.describe_events(["9999"], ROWS)
    assert described[0]["id"] == "9999"
    assert "no name" in described[0]["name"]


def test_names_appear_in_the_preview_body():
    out = {"mode": "preview", "dry_run": True, "max_event_creates": 3,
           "event_creates": scheduled.describe_events(["1529", "1531"],
                                                      ROWS),
           "steps": {}}

    body = "\n".join(scheduled.summary_lines(out))

    assert "marketing events would create: 2 (ceiling 3)" in body
    assert "1529  2026-11-10  AMCF Webinar A  [Event - Other]" in body
    assert "1531  2026-10-28  AMCF Webinar C  [Event - Other]" in body


def test_names_appear_in_the_live_body():
    out = {"mode": "live", "dry_run": False, "max_event_creates": 3,
           "event_creates": scheduled.describe_events(["1530"], ROWS),
           "steps": {}}

    body = "\n".join(scheduled.summary_lines(out))

    assert "marketing events created: 1" in body
    assert "1530  2026-11-18  AMCF Webinar B" in body


def test_a_refusal_names_the_events_it_would_have_created(monkeypatch,
                                                          events):
    """The ALERT has to say WHAT, or "raise the ceiling" is a blind call."""
    sent = []
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_ENABLED", True)
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_MODE", "preview")
    monkeypatch.setattr("clients.notify.notify",
                        lambda subject, body, **k: sent.append(
                            (notifier.subject_line(
                                subject, k.get("kind")), body))
                        or {"sent": ["email"], "failed": []})
    _calls, state = events
    state["ids"] = ["1529", "1530", "1531", "1532"]
    monkeypatch.setattr(scheduled, "read_calendar",
                        lambda csuite, today=None: {
                            "inside": ROWS, "undated": [], "all": ROWS,
                            "total": 187, "error": None})
    monkeypatch.setattr(scheduled, "previous_undated_ids", lambda: None)

    with pytest.raises(JobFailed):
        handler.handle({"payload": {}})

    assert len(sent) == 1
    subject, body = sent[0]
    assert subject.startswith("[Jidhr ALERT]")
    assert "1532  2026-12-02  AMCF Webinar D" in body
    assert "[contact_create] NOT REACHED" in body


# ---------------------------------------------------------------------------
# CHANGE 3: undated events
# ---------------------------------------------------------------------------

UNDATED = [{"event_date_id": str(i), "event_name": "Unassigned",
            "event_description": f"Legacy {i}", "archived": 1,
            "event_date": None} for i in (1052, 1053, 1054)]


def test_the_count_and_archived_split_are_recorded(monkeypatch):
    monkeypatch.setattr(scheduled, "previous_undated_ids",
                        lambda: ["1052", "1053", "1054"])
    out = {}

    scheduled._note_undated(out, UNDATED)

    assert out["undated_count"] == 3
    assert out["undated_archived"] == 3
    assert out["new_undated"] == []


def test_a_new_undated_event_is_flagged(monkeypatch):
    """Somebody created an event and left the date blank: the window can
    never see it."""
    monkeypatch.setattr(scheduled, "previous_undated_ids",
                        lambda: ["1052", "1053", "1054"])
    fresh = UNDATED + [{"event_date_id": "1540", "event_name": "Event - Other",
                        "event_description": "Fall Gala", "archived": 0,
                        "event_date": None}]
    out = {}

    scheduled._note_undated(out, fresh)

    assert out["new_undated"] == [{"id": "1540", "name": "Fall Gala",
                                   "category": "Event - Other"}]

    body = "\n".join(scheduled.summary_lines(
        dict(out, mode="preview", dry_run=True, steps={})))
    assert "NOTE new undated event: 1540, Fall Gala [Event - Other]" in body


def test_an_event_that_lost_its_date_is_flagged_too(monkeypatch):
    """Not undated last time = new, whether it is new or merely dateless
    now."""
    monkeypatch.setattr(scheduled, "previous_undated_ids", lambda: ["1052"])
    out = {}

    scheduled._note_undated(out, UNDATED[:2])

    assert [e["id"] for e in out["new_undated"]] == ["1053"]


def test_the_first_run_records_a_baseline_rather_than_flagging_all(
        monkeypatch):
    monkeypatch.setattr(scheduled, "previous_undated_ids", lambda: None)
    out = {}

    scheduled._note_undated(out, UNDATED)

    assert out["new_undated"] == [], "98 legacy rows are not 98 surprises"
    body = "\n".join(scheduled.summary_lines(
        dict(out, mode="preview", dry_run=True, steps={})))
    assert "first scheduled run" in body
    assert "baseline" in body


def test_a_new_undated_event_is_a_note_not_an_alert(monkeypatch):
    """The brief: a note in the OK email, not an ALERT."""
    sent = []
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_ENABLED", True)
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_MODE", "preview")
    monkeypatch.setattr("clients.notify.notify",
                        lambda subject, body, **k: sent.append(
                            (notifier.subject_line(
                                subject, k.get("kind")), body))
                        or {"sent": ["email"], "failed": []})
    monkeypatch.setattr(scheduled, "previous_undated_ids",
                        lambda: ["1052", "1053", "1054"])
    fresh = UNDATED + [{"event_date_id": "1540", "event_name": "Event - Other",
                        "event_description": "Fall Gala", "archived": 0,
                        "event_date": None}]
    monkeypatch.setattr(scheduled, "read_calendar",
                        lambda csuite, today=None: {
                            "inside": [], "undated": fresh, "all": fresh,
                            "total": 187, "error": None})
    for step in ("_step_events", "_step_contacts"):
        monkeypatch.setattr(scheduled, step, lambda *a: None)

    handler.handle({"payload": {}})

    assert len(sent) == 1
    subject, body = sent[0]
    assert subject.startswith("[Jidhr OK]"), "a note, not an alert"
    assert "new undated event: 1540" in body


def test_the_undated_ids_are_stored_for_tomorrows_comparison():
    """jobs.result is the dict the email was built from; the next run reads
    its baseline from there."""
    assert "result ? 'undated_ids'" in scheduled._PREVIOUS_UNDATED_SQL
    assert "job_type = 'registrations_sync'" in \
        scheduled._PREVIOUS_UNDATED_SQL


def test_an_unreadable_baseline_is_none_not_a_crash(monkeypatch):
    monkeypatch.setattr("clients.database.execute_query",
                        lambda *a, **k: (_ for _ in ()).throw(
                            RuntimeError("no db")))
    assert scheduled.previous_undated_ids() is None


def test_no_address_reaches_the_undated_note(monkeypatch):
    monkeypatch.setattr(scheduled, "previous_undated_ids", lambda: [])
    out = {}
    scheduled._note_undated(out, UNDATED)
    body = "\n".join(scheduled.summary_lines(
        dict(out, mode="preview", dry_run=True, steps={})))
    assert "@" not in body


# ---------------------------------------------------------------------------
# The write paths did not move
# ---------------------------------------------------------------------------

def test_the_registration_and_contact_write_paths_have_zero_diff():
    """The request bodies are byte-identical BY CONSTRUCTION: the two files
    that build them are untouched since V5.77."""
    import subprocess

    found = subprocess.run(
        ["git", "diff", "--stat", "65b728b", "--",
         "sync/registrations.py", "sync/contact_create.py"],
        capture_output=True, text=True)
    assert found.returncode == 0, found.stderr
    assert found.stdout.strip() == "", found.stdout
