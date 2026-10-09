"""Registrations applies run in the background, not inside the HTTP request.

Production, 2026-10-09: "sync registrations apply event 1157 limit 29".
run_log 30 recorded status 'complete', 19:20:08 to 19:25:27 — 318 seconds.
All 29 writes landed and verified (registration_map holds 29 'synced' rows
for 1157 and no unverified ones). The chat UI showed "Network error. Please
try again."

Railway's public proxy closes a response after 5 minutes with no data
transferred. 318 > 300. Nothing in the app timed out: gunicorn kills a
silent worker, and a killed worker could not have written the finished_at
and 'complete' status that run_log 30 carries.

No network, no database — the run_log is a fake ledger.
"""

import pytest

from intents import sync_commands
from sync import registration_jobs as jobs
from sync import registrations as reg


class Ledger:
    """Stands in for hubsync.run_log and write_audit."""

    def __init__(self, rows=None, writes=0):
        self.rows = list(rows or [])
        self.writes = writes
        self.opened = []
        self.closed = []
        self.next_id = 100

    # --- the seams jobs.py uses -------------------------------------------
    def execute_query(self, sql, params=None, fetch=True):
        if "COUNT(*)" in sql and "write_audit" in sql:
            return [{"n": self.writes}]
        if "status = 'running'" in sql:
            return [r for r in self.rows if r["status"] == "running"]
        if "ORDER BY id DESC" in sql:
            applied = [r for r in self.rows if r["applied"]]
            return applied[-1:][::-1]
        if "AND id = %s" in sql:
            wanted = int((params or (0,))[0])
            return [r for r in self.rows if r["id"] == wanted]
        return []

    def open_run(self, applied):
        run_id = self.next_id
        self.next_id += 1
        self.opened.append(applied)
        self.rows.append(row(run_id, status="running", applied=applied))
        return run_id

    def close_run(self, run_id, status, counts, outcomes=None,
                  error_summary=None):
        self.closed.append((run_id, status, error_summary))
        for r in self.rows:
            if r["id"] == run_id:
                r["status"] = status
                r["error_summary"] = error_summary
                r["outcomes"] = outcomes


def row(run_id, status="complete", applied=True, outcomes=None,
        seconds=318.0, age=5.0, error_summary=None):
    from datetime import datetime, timezone
    started = datetime(2026, 10, 9, 19, 20, 8, tzinfo=timezone.utc)
    return {"id": run_id, "applied": applied, "status": status,
            "started_at": started,
            "finished_at": None if status == "running" else started,
            "seconds": seconds, "age_seconds": age,
            "error_summary": error_summary, "outcomes": outcomes}


@pytest.fixture
def ledger(monkeypatch):
    led = Ledger()
    monkeypatch.setattr(jobs.database, "execute_query", led.execute_query)
    monkeypatch.setattr(jobs.reg, "open_run", led.open_run)
    monkeypatch.setattr(jobs.reg, "close_run", led.close_run)
    monkeypatch.setattr(jobs.reg, "migration_applied", lambda: True)
    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", True)
    return led


def summary(**overrides):
    base = {"dry_run": False, "limit": 29, "events_read": 1,
            "registrant_rows": 41, "unique_emails": 29,
            "duplicates_dropped": 0, "would_register": 29, "withheld": 12,
            "already": 0, "review": 0, "review_rows": [], "non_marketing": 0,
            "csuite_calls": 2, "hubspot_calls": 26, "migration_applied": True,
            "run_logged": True, "run_id": 30, "error": None, "stopped": None,
            "registered": 29, "failed": 0, "deferred": 0, "unverified": 0,
            "unverified_prior": 0, "held": 0, "held_events": [],
            "writes_attempted": 29, "write_audit_ids": [81, 82],
            "interaction_rules": {"event_start": 29, "run_time": 0}}
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Starting
# ---------------------------------------------------------------------------

def test_the_apply_starts_in_the_background_and_returns_its_run_id(ledger):
    ran = []

    run_id = jobs.start_apply(limit=29, event_ids=("1157",),
                              runner=lambda *a: ran.append(a))

    assert run_id == 100
    assert ledger.opened == [True], "one applied run_log row"
    # The thread is real; give it a moment to have been handed the args.
    for _ in range(200):
        if ran:
            break
    assert ran and ran[0] == (100, 29, ("1157",))


def test_the_caller_does_not_wait_for_the_run(ledger):
    """The whole point: start_apply returns before the work is done."""
    import threading

    gate = threading.Event()
    jobs.start_apply(limit=1, runner=lambda *a: gate.wait(5))

    assert not gate.is_set(), "start_apply waited for the run"
    gate.set()


def test_the_background_run_closes_its_row_even_when_it_dies(ledger,
                                                             monkeypatch):
    """A thread that raises leaves a row on 'running' forever, which also
    blocks every later apply."""
    monkeypatch.setattr(jobs.reg, "run",
                        lambda **kw: (_ for _ in ()).throw(
                            RuntimeError("boom")))
    ledger.open_run(True)                      # run 100, status running

    jobs._run_apply(100, 1, None)

    assert ledger.closed and ledger.closed[-1][0] == 100
    assert ledger.closed[-1][1] == "failed"
    assert "boom" in ledger.closed[-1][2]


def test_a_run_that_closed_itself_is_not_closed_twice(ledger, monkeypatch):
    """reg.run closes the row in its own finally. _run_apply must not stamp
    a second, contradictory outcome over it."""
    def finishes(**kw):
        ledger.close_run(kw["run_id"], "complete", {})
        raise reg.RegistrationWriteStopped([], "stopped after writing")

    monkeypatch.setattr(jobs.reg, "run", finishes)
    ledger.open_run(True)

    jobs._run_apply(100, 1, None)

    statuses = [c[1] for c in ledger.closed if c[0] == 100]
    assert statuses == ["complete"], statuses


# ---------------------------------------------------------------------------
# One at a time
# ---------------------------------------------------------------------------

def test_a_second_apply_is_refused_while_one_is_running(ledger):
    ledger.rows.append(row(30, status="running"))

    with pytest.raises(jobs.ApplyAlreadyRunning) as refused:
        jobs.start_apply(limit=1, runner=lambda *a: None)

    assert "run_log 30" in str(refused.value)
    assert "one registration becomes two" in str(refused.value)
    assert ledger.opened == [], "and no second row is opened"


def test_the_refusal_names_the_running_run_in_chat(ledger):
    ledger.rows.append(row(30, status="running"))

    reply = sync_commands.handle("sync registrations apply limit 1", None)

    assert "One apply at a time" in reply
    assert "run_log 30" in reply
    assert "registrations status" in reply


def test_abandonment_is_judged_by_write_activity_not_by_age(ledger):
    """A 29-record apply legitimately runs for 318 seconds, so judging a run
    by its total age would have called it abandoned while it was still
    writing. The test is on the statement itself: the fake ledger cannot
    evaluate an interval or a lateral join.

    The idle clock is measured from the newest attendance row in
    write_audit, falling back to the run's own start before its first
    write — so a run that is still writing can never look idle."""
    assert jobs.IDLE_ABANDONED_SECONDS == 300
    sql = jobs._RUNNING_SQL
    assert "MAX(created_at) AS last_write" in sql
    assert "marketing-events/attendance/" in sql
    assert "COALESCE(w.last_write, r.started_at)" in sql
    assert "GREATEST(" in sql
    assert "idle_seconds" in sql
    # Not the old wall-clock rule.
    assert "INTERVAL '1 minute'" not in sql


def test_a_run_still_writing_is_never_treated_as_abandoned(ledger):
    """The guard must keep refusing a second apply while the first is
    making progress, however long it has been going."""
    import re

    sql = jobs._RUNNING_SQL
    # The WHERE clause compares the IDLE time to the threshold, not the age.
    where = sql.split("WHERE", 1)[1]
    assert "idle" in where.lower() or "GREATEST(" in where
    assert re.search(r"NOW\(\) - GREATEST", where)
    assert "age_seconds" not in where, \
        "age must not decide abandonment — run_log 30 ran 318s"


def test_a_run_that_lost_the_race_stands_down_and_writes_nothing(ledger):
    """Two requests can both pass the pre-check. The one with the higher id
    closes its own row rather than running alongside the other."""
    started = []

    def claim_then_collide(applied):
        run_id = ledger.open_run(applied)
        # Another worker's apply appears, with a lower id.
        ledger.rows.append(row(5, status="running"))
        return run_id

    import sync.registration_jobs as mod
    original = mod.reg.open_run
    mod.reg.open_run = claim_then_collide
    try:
        with pytest.raises(jobs.ApplyAlreadyRunning) as refused:
            jobs.start_apply(limit=1, runner=lambda *a: started.append(a))
    finally:
        mod.reg.open_run = original

    assert "run_log 5" in str(refused.value)
    assert "stood down" in str(refused.value)
    assert started == [], "the loser never runs"
    assert (100, "failed") == ledger.closed[-1][:2]


# ---------------------------------------------------------------------------
# Refusals happen before the thread
# ---------------------------------------------------------------------------

def test_the_flag_is_checked_before_anything_starts(ledger, monkeypatch):
    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", False)

    with pytest.raises(reg.RegistrationsSyncDisabled):
        jobs.start_apply(limit=1, runner=lambda *a: None)

    assert ledger.opened == []


def test_a_missing_limit_is_refused_before_anything_starts(ledger):
    with pytest.raises(reg.LimitRequired):
        jobs.start_apply(limit=None, runner=lambda *a: None)

    assert ledger.opened == []


def test_a_missing_registration_map_is_refused_before_anything_starts(
        ledger, monkeypatch):
    monkeypatch.setattr(jobs.reg, "migration_applied", lambda: False)

    with pytest.raises(reg.RegistrationWriteStopped):
        jobs.start_apply(limit=1, runner=lambda *a: None)

    assert ledger.opened == []


def test_a_row_that_cannot_be_opened_stops_the_run(ledger, monkeypatch):
    monkeypatch.setattr(jobs.reg, "open_run", lambda applied: None)
    started = []

    with pytest.raises(reg.RegistrationWriteStopped):
        jobs.start_apply(limit=1, runner=lambda *a: started.append(a))

    assert started == [], "an unreportable run is not worth starting"


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def test_status_of_a_finished_run_returns_the_full_report(ledger):
    ledger.rows.append(row(30, outcomes={"summary": summary(),
                                         "records": [{"a": 1}] * 29}))

    reply = sync_commands.handle("status run 30", None)

    assert "run_log 30" in reply
    assert "took **318s**" in reply
    # The same report the apply itself prints.
    assert "✅ **Registrations — APPLIED**" in reply
    assert "**29** would be sent as REGISTERED" in reply
    assert "**29** registered and verified in HubSpot" in reply
    assert "write_audit 81, 82" in reply


def test_status_of_a_running_run_shows_progress(ledger):
    ledger.rows.append(row(30, status="running"))
    ledger.writes = 8

    reply = sync_commands.handle("status run 30", None)

    assert "still running" in reply
    assert "**8** so far" in reply
    assert "Do not start another apply" in reply


def test_status_with_no_id_takes_the_latest_apply(ledger):
    ledger.rows.append(row(28, applied=False))          # a preview
    ledger.rows.append(row(30, outcomes={"summary": summary(),
                                         "records": []}))

    reply = sync_commands.handle("registrations status", None)

    assert "run_log 30" in reply, "a status request is about the write"


def test_status_of_an_unknown_run_says_so(ledger):
    reply = sync_commands.handle("status run 999", None)

    assert "No such run" in reply
    assert "999" in reply


def test_status_before_any_apply_says_so(ledger):
    reply = sync_commands.handle("registrations status", None)

    assert "No registrations apply has been run yet" in reply


def test_a_stopped_run_reports_its_stop_reason(ledger):
    ledger.rows.append(row(
        30, status="failed",
        error_summary="the write for event 1463 failed (HTTP 400)",
        outcomes={"summary": summary(
            registered=0, failed=1, writes_attempted=1,
            stopped="the write for event 1463 failed (HTTP 400)"),
            "records": []}))

    reply = sync_commands.handle("status run 30", None)

    assert "🛑 **Registrations — STOPPED**" in reply
    assert "HTTP 400" in reply


def test_an_old_row_without_a_summary_degrades_instead_of_crashing(ledger):
    """run_log rows written before this hotfix carried the per-record list
    only."""
    ledger.rows.append(row(30, outcomes=[{"event_date_id": "1157"}] * 29))

    reply = sync_commands.handle("status run 30", None)

    assert "run_log 30" in reply
    assert "**29** per-record row(s)" in reply
    assert "stored no full report" in reply


# ---------------------------------------------------------------------------
# What the stored report may contain
# ---------------------------------------------------------------------------

def test_the_stored_summary_holds_no_email_address():
    """run_log.outcomes already whitelists addresses out of the per-record
    rows. The summary must not reintroduce them through out["events"]."""
    import json

    out = {"would_register": 1, "registered": 1,
           "events": [{"event_date_id": "1157",
                       "would_register": [{"contact_email": "a@donor.invalid",
                                           "email_sha1": "abc"}]}],
           "first_sends": [{"event_date_id": "1157",
                            "hubspot_contact_id": "701"}]}

    stored = reg.summary_of(out)

    assert "events" not in stored
    assert "@" not in json.dumps(stored, default=str)
    assert stored["registered"] == 1, "the counts survive"


def test_the_summary_keeps_everything_the_report_reads():
    out = summary()
    out["events"] = [{"event_date_id": "1157"}]

    stored = reg.summary_of(out)

    for key in ("registered", "failed", "unverified", "writes_attempted",
                "write_audit_ids", "stopped", "limit", "dry_run",
                "interaction_rules", "held", "held_events"):
        assert key in stored, key


# ---------------------------------------------------------------------------
# A lost response must not become a duplicate send
# ---------------------------------------------------------------------------

def test_a_lost_response_does_not_resend(ledger, monkeypatch):
    """The actual regression. run_log 30's writes all landed; the response
    was discarded. If chat had been retried, 1157 would have been sent
    twice — so the rows it wrote must read as already-registered."""
    known = {("1157", "a@x.inv"): {"last_state": "REGISTERED",
                                   "status": "synced", "write_audit_id": 81}}
    from tests.test_registrations_preview import registrant

    plan = reg.plan_event("1157", [registrant("a@x.inv")],
                          {"a@x.inv": {"id": "701", "marketing": True}},
                          known)

    assert plan["would_register"] == [], "a landed write is never resent"
    assert len(plan["already"]) == 1


def test_the_lost_response_message_does_not_say_try_again():
    """"Please try again" is the wrong advice for a write."""
    page = open("templates/chat.html").read()

    assert "Network error. Please try again." not in page
    assert "do not send it again" in page.lower()
    assert "registrations status" in page
    assert "status run N" in page


def test_the_worker_timeout_exceeds_railways_ceiling():
    """A worker killed mid-run is worse than a lost response: the writes
    stop half way. Railway gives up at 15 minutes, so gunicorn must not."""
    config = open("railway.toml").read()

    assert "--timeout 900" in config
    assert "--timeout 180" not in config
    assert "--graceful-timeout 120" in config
