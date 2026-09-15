"""Step 3b: the jobs runner, the mirror_refresh handler, and the CLI.

No database. clients.database.execute_query is replaced by a fake that
records every statement and answers the few SELECT/RETURNING shapes the
runner uses, so each test can assert on exactly what would have been
written to `jobs`.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest

from jobs import runner
from jobs.handlers import mirror_refresh
from scripts import jobs_run

NOW = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# A fake `jobs` table
# ---------------------------------------------------------------------------

class FakeJobs:
    """Records statements; answers the claim, count and insert shapes."""

    def __init__(self, claimable=None, queued_recurring=0):
        self.statements = []
        self.claimable = list(claimable or [])
        self.queued_recurring = queued_recurring
        self.next_id = 500

    def __call__(self, sql, params=None, fetch=True):
        collapsed = " ".join(str(sql).split())
        self.statements.append((collapsed, tuple(params or ())))

        if collapsed.startswith("UPDATE jobs SET status = 'running'"):
            if not self.claimable:
                return []
            return [self.claimable.pop(0)]

        if collapsed.startswith("SELECT COUNT(*) AS n FROM jobs"):
            return [{"n": self.queued_recurring}]

        if collapsed.startswith("SELECT id, run_after FROM jobs"):
            if self.queued_recurring:
                return [{"id": 77, "run_after": "2026-09-16T06:00:00+00:00"}]
            return []

        if collapsed.startswith("INSERT INTO jobs"):
            self.next_id += 1
            # Anything inserted as queued+recurring is now queued.
            payload = json.loads(params[1]) if len(params) > 1 else {}
            if payload.get("recurring"):
                self.queued_recurring += 1
            return [{"id": self.next_id}]

        return 1

    # -- views ---------------------------------------------------------------

    def matching(self, prefix):
        return [(s, p) for s, p in self.statements if s.startswith(prefix)]

    @property
    def claims(self):
        return self.matching("UPDATE jobs SET status = 'running'")

    @property
    def completes(self):
        return self.matching("UPDATE jobs SET status = 'complete'")

    @property
    def fails(self):
        return self.matching("UPDATE jobs SET status = 'failed'")

    @property
    def inserts(self):
        return self.matching("INSERT INTO jobs")

    @property
    def retries(self):
        return [(s, p) for s, p in self.inserts if "interval" in s]

    @property
    def occurrences(self):
        return [(s, p) for s, p in self.inserts if "interval" not in s]


def job(job_id=1, job_type="demo", payload=None, attempts=0, max_attempts=3,
        priority=100, requested_by=None):
    return {
        "id": job_id, "job_type": job_type,
        "payload": dict(payload or {}), "status": "running",
        "priority": priority, "run_after": NOW, "attempts": attempts,
        "max_attempts": max_attempts, "requested_by": requested_by,
        "sync_run_id": None, "claimed_by": "host:1", "claimed_at": NOW,
        "started_at": NOW, "finished_at": None, "result": None,
        "error": None, "created_at": NOW,
    }


DAILY = {"recurring": "daily", "at": "06:00", "pace_ms": 400}


@pytest.fixture
def db(monkeypatch):
    fake = FakeJobs()
    monkeypatch.setattr("clients.database.execute_query", fake)
    return fake


@pytest.fixture
def handlers(monkeypatch):
    """A clean registry with two demo handlers."""
    registry = {}
    monkeypatch.setattr(runner, "HANDLERS", registry)

    @runner.register("demo")
    def demo(job):
        return {"ok": True, "saw": job["payload"], "sync_run_id": 42}

    @runner.register("boom")
    def boom(job):
        raise ValueError("kaboom")

    return registry


@pytest.fixture(autouse=True)
def frozen_now(monkeypatch):
    monkeypatch.setattr(runner, "datetime", _FixedDatetime)


class _FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz else NOW.replace(tzinfo=None)


# ---------------------------------------------------------------------------
# claim_next
# ---------------------------------------------------------------------------

def test_claim_marks_running_with_worker_and_timestamps(db):
    db.claimable = [job(job_id=9, job_type="demo", payload={"a": 1})]

    claimed = runner.claim_next("host:123")

    assert claimed["id"] == 9
    assert claimed["payload"] == {"a": 1}
    sql, params = db.claims[0]
    assert "SET status = 'running'" in sql
    assert "claimed_by = %s" in sql and params == ("host:123",)
    assert "claimed_at = NOW()" in sql and "started_at = NOW()" in sql


def test_claim_is_one_statement_with_skip_locked(db):
    db.claimable = [job()]
    runner.claim_next("w")

    sql, _ = db.claims[0]
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "LIMIT 1" in sql
    assert "status = 'queued'" in sql and "run_after <= NOW()" in sql
    assert "ORDER BY priority, run_after" in sql
    assert "RETURNING id, job_type, payload" in sql
    assert len(db.statements) == 1, "claim must be a single round trip"


def test_claim_returns_none_when_nothing_is_claimable(db):
    assert runner.claim_next("w") is None


def test_claim_parses_a_text_payload(db):
    db.claimable = [dict(job(), payload='{"recurring": "daily"}')]
    assert runner.claim_next("w")["payload"] == {"recurring": "daily"}


def test_claim_accepts_a_tuple_row(db):
    row = tuple(job(job_id=3, job_type="demo")[c] for c in runner.JOB_COLUMNS)
    db.claimable = [row]
    claimed = runner.claim_next("w")
    assert claimed["id"] == 3 and claimed["job_type"] == "demo"


def test_worker_id_is_host_and_pid():
    host, pid = runner.worker_id().rsplit(":", 1)
    assert host and pid.isdigit()


# ---------------------------------------------------------------------------
# run: success
# ---------------------------------------------------------------------------

def test_success_marks_complete_with_result_and_sync_run_id(db, handlers):
    summary = runner.run(job(job_id=5, job_type="demo", payload={"x": 1}))

    assert summary["status"] == "complete"
    assert summary["error"] is None
    sql, params = db.completes[0]
    assert "attempts = attempts + 1" in sql and "finished_at = NOW()" in sql
    assert "error = NULL" in sql
    result, sync_run_id, job_id = params
    assert json.loads(result)["saw"] == {"x": 1}
    assert json.loads(result)["_run"]["status"] == "complete"
    assert sync_run_id == 42
    assert job_id == 5
    assert db.fails == [] and db.retries == []


def test_success_on_a_daily_job_queues_the_next_occurrence_once(db, handlers):
    summary = runner.run(job(job_type="demo", payload=DAILY, priority=50,
                             requested_by=7))

    assert len(db.occurrences) == 1
    sql, params = db.occurrences[0]
    job_type, payload, priority, run_after, requested_by = params
    assert job_type == "demo"
    assert json.loads(payload) == DAILY, "payload copied verbatim"
    assert priority == 50 and requested_by == 7
    assert run_after == datetime(2026, 9, 16, 6, 0, tzinfo=timezone.utc)
    assert summary["next_occurrence_queued"] == 501


def test_second_finish_does_not_double_queue(db, handlers):
    runner.run(job(job_id=1, job_type="demo", payload=DAILY))
    runner.run(job(job_id=2, job_type="demo", payload=DAILY))

    assert len(db.occurrences) == 1, "one tomorrow, not two"
    assert len(db.completes) == 2


def test_an_already_queued_recurrence_is_left_alone(db, handlers):
    db.queued_recurring = 1
    summary = runner.run(job(job_type="demo", payload=DAILY))
    assert db.occurrences == []
    assert summary["next_occurrence_queued"] is None


def test_non_recurring_job_queues_nothing(db, handlers):
    runner.run(job(job_type="demo", payload={"pace_ms": 400}))
    assert db.inserts == []


def test_dedupe_checks_job_type_and_recurrence(db, handlers):
    runner.run(job(job_type="demo", payload=DAILY))
    sql, params = db.matching("SELECT COUNT(*) AS n FROM jobs")[0]
    assert "status = 'queued'" in sql
    assert "payload->>'recurring' = %s" in sql
    assert params == ("demo", "daily")


# ---------------------------------------------------------------------------
# run: failure and retry
# ---------------------------------------------------------------------------

def test_failure_marks_failed_and_queues_a_backed_off_retry(db, handlers):
    summary = runner.run(job(job_id=8, job_type="boom", payload={"k": 1},
                             attempts=0, max_attempts=3, priority=60,
                             requested_by=3))

    assert summary["status"] == "failed"
    assert summary["error"] == "kaboom"
    assert summary["attempt"] == 1

    sql, params = db.fails[0]
    assert "attempts = attempts + 1" in sql
    error, result, job_id = params
    assert error == "kaboom" and job_id == 8
    assert "traceback" in json.loads(result)

    assert len(db.retries) == 1
    sql, params = db.retries[0]
    job_type, payload, priority, delay, requested_by, attempts, max_att = params
    assert job_type == "boom" and json.loads(payload) == {"k": 1}
    assert priority == 60 and requested_by == 3
    assert delay == "15 minutes"
    assert attempts == 1 and max_att == 3
    assert "NOW() + %s::interval" in sql
    assert db.completes == []


def test_backoff_grows_with_attempts(db, handlers):
    runner.run(job(job_type="boom", attempts=1, max_attempts=3))
    _, params = db.retries[0]
    assert params[3] == "30 minutes"
    assert params[5] == 2


def test_last_attempt_does_not_retry_but_still_queues_recurrence(db, handlers):
    summary = runner.run(job(job_type="boom", payload=DAILY,
                             attempts=2, max_attempts=3))

    assert summary["status"] == "failed"
    assert summary["attempt"] == 3
    assert db.retries == [], "attempts exhausted"
    assert len(db.occurrences) == 1, "one bad night must not stop the mirror"
    assert summary["retry_queued"] is None
    assert summary["next_occurrence_queued"] is not None


def test_a_non_final_failure_does_not_queue_recurrence(db, handlers):
    """The retry row carries the recurring payload; it queues tomorrow
    when it finishes. Queuing it now too would be two tomorrows."""
    runner.run(job(job_type="boom", payload=DAILY, attempts=0,
                   max_attempts=3))
    assert len(db.retries) == 1
    assert db.occurrences == []


def test_unknown_job_type_fails_cleanly(db, handlers):
    summary = runner.run(job(job_type="nope"))
    assert summary["status"] == "failed"
    assert "no handler registered" in summary["error"]
    assert db.fails


def test_error_text_is_capped(db, monkeypatch):
    registry = {}
    monkeypatch.setattr(runner, "HANDLERS", registry)

    @runner.register("long")
    def long_error(job):
        raise RuntimeError("x" * 5000)

    summary = runner.run(job(job_type="long"))
    assert len(summary["error"]) == 2000
    _, params = db.fails[0]
    assert len(params[0]) == 2000


def test_job_failed_keeps_its_result(db, monkeypatch):
    registry = {}
    monkeypatch.setattr(runner, "HANDLERS", registry)

    @runner.register("partial")
    def partial(job):
        raise runner.JobFailed("2 of 8 failed", result={"per_type": [1, 2]})

    runner.run(job(job_type="partial"))
    _, params = db.fails[0]
    stored = json.loads(params[1])
    assert stored["per_type"] == [1, 2]
    assert stored["_run"]["status"] == "failed"


def test_handler_returning_a_non_dict_is_wrapped(db, monkeypatch):
    registry = {}
    monkeypatch.setattr(runner, "HANDLERS", registry)
    runner.register("scalar")(lambda job: 7)

    runner.run(job(job_type="scalar"))
    _, params = db.completes[0]
    assert json.loads(params[0])["returned"] == 7


def test_registering_a_type_twice_with_a_different_function_is_refused(
        handlers):
    with pytest.raises(ValueError, match="already registered"):
        runner.register("demo")(lambda job: None)


# ---------------------------------------------------------------------------
# Recurrence helpers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("payload, expected", [
    (DAILY, ("daily", "06:00")),
    ({"recurring": "daily", "at": "6:5"}, ("daily", "06:05")),
    ({"recurring": "daily"}, None),
    ({"recurring": "daily", "at": "25:00"}, None),
    ({"recurring": "weekly", "at": "06:00"}, None),
    ({"pace_ms": 400}, None),
    ({}, None),
    (None, None),
])
def test_recurrence_of(payload, expected):
    assert runner.recurrence_of(payload) == expected


def test_next_occurrence_is_strictly_after_now():
    at_noon = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)
    assert runner.next_occurrence("06:00", at_noon) == \
        datetime(2026, 9, 16, 6, 0, tzinfo=timezone.utc)
    assert runner.next_occurrence("18:00", at_noon) == \
        datetime(2026, 9, 15, 18, 0, tzinfo=timezone.utc)
    # Exactly now counts as passed.
    assert runner.next_occurrence("12:00", at_noon) == \
        datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# run_until_empty
# ---------------------------------------------------------------------------

def test_run_until_empty_drains_the_queue(db, handlers):
    db.claimable = [job(job_id=1, job_type="demo"),
                    job(job_id=2, job_type="boom"),
                    job(job_id=3, job_type="demo")]

    summaries = runner.run_until_empty("w")

    assert [s["status"] for s in summaries] == ["complete", "failed",
                                                 "complete"]
    assert len(db.claims) == 4, "three claims plus the empty one"


def test_run_until_empty_has_a_ceiling(db, handlers, monkeypatch):
    db.claimable = [job(job_type="demo") for _ in range(20)]
    summaries = runner.run_until_empty("w", limit=5)
    assert len(summaries) == 5


# ---------------------------------------------------------------------------
# mirror_refresh handler
# ---------------------------------------------------------------------------

class TypeResult:
    def __init__(self, record_type, status="complete", run_id=None, **kw):
        self.record_type = record_type
        self.status = status
        self.run_id = run_id
        self.error = kw.get("error")
        self.expected = kw.get("expected", 10)
        self.fetched = kw.get("fetched", 10)
        self.complete = status == "complete"
        self.written = kw.get("written", 1)
        self.unchanged = kw.get("unchanged", 9)
        self.deleted = kw.get("deleted", 0)
        self.calls = kw.get("calls", 2)
        self.total_429s = kw.get("total_429s", 0)
        self.seconds = kw.get("seconds", 1.5)


def test_mirror_handler_calls_refresh_with_job_settings(monkeypatch):
    seen = {}

    def fake_refresh(**kwargs):
        seen.update(kwargs)
        return [TypeResult("fund", run_id=11), TypeResult("grant", run_id=12)]

    monkeypatch.setattr(mirror_refresh.mirror, "refresh", fake_refresh)

    result = mirror_refresh.handle(job(job_type="mirror_refresh",
                                       payload={"pace_ms": 650},
                                       requested_by=4))

    assert seen["record_types"] is None, "every type"
    assert seen["pace_ms"] == 650
    assert seen["triggered_by"] == "job:mirror_refresh"
    assert seen["triggered_by_user_id"] == 4
    assert seen["dry_run"] is False

    assert result["sync_run_ids"] == [11, 12]
    assert result["sync_run_id"] == 11
    assert result["calls"] == 4
    assert [r["type"] for r in result["per_type"]] == ["fund", "grant"]
    assert set(result["per_type"][0]) == set(mirror_refresh.RESULT_COLUMNS)


def test_mirror_handler_pace_falls_back_to_the_environment(monkeypatch):
    monkeypatch.setenv("CSUITE_PACE_MS", "700")
    assert mirror_refresh.pace_from({}) == 700
    assert mirror_refresh.pace_from({"pace_ms": "not-a-number"}) == 700
    assert mirror_refresh.pace_from({"pace_ms": -5}) == 700
    assert mirror_refresh.pace_from({"pace_ms": 300}) == 300
    monkeypatch.delenv("CSUITE_PACE_MS")
    assert mirror_refresh.pace_from({}) == 400


def test_mirror_handler_raises_when_a_type_failed(monkeypatch):
    monkeypatch.setattr(mirror_refresh.mirror, "refresh", lambda **kw: [
        TypeResult("fund", run_id=11),
        TypeResult("profile", status="failed", run_id=12,
                   error="rate limited"),
        TypeResult("donation_agg", status="skipped"),
    ])

    with pytest.raises(runner.JobFailed) as raised:
        mirror_refresh.handle(job(job_type="mirror_refresh"))

    assert "2 of 3 record types did not complete" in str(raised.value)
    assert "profile: rate limited" in str(raised.value)
    assert "donation_agg: skipped" in str(raised.value)
    # The per-type table survives the failure.
    assert [r["type"] for r in raised.value.result["per_type"]] == \
        ["fund", "profile", "donation_agg"]
    assert raised.value.result["sync_run_ids"] == [11, 12]


def test_a_failed_mirror_job_is_recorded_as_failed_with_its_table(
        db, monkeypatch):
    monkeypatch.setattr(mirror_refresh.mirror, "refresh", lambda **kw: [
        TypeResult("fund", run_id=11),
        TypeResult("grant", status="failed", error="budget reached"),
    ])
    registry = {"mirror_refresh": mirror_refresh.handle}
    monkeypatch.setattr(runner, "HANDLERS", registry)

    summary = runner.run(job(job_type="mirror_refresh", payload=DAILY))

    assert summary["status"] == "failed"
    _, params = db.fails[0]
    stored = json.loads(params[1])
    assert [r["type"] for r in stored["per_type"]] == ["fund", "grant"]
    assert len(db.retries) == 1


def test_mirror_handler_is_registered():
    import jobs.handlers  # noqa: F401
    assert "mirror_refresh" in runner.HANDLERS


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setattr("clients.database.is_configured", lambda: True)


def test_seed_queues_the_daily_job_for_the_next_0600(db, configured):
    message = jobs_run.seed(now=NOW)

    assert message.startswith("queued daily mirror_refresh as job 501")
    sql, params = db.inserts[0]
    job_type, payload, priority, run_after = params
    assert job_type == "mirror_refresh"
    assert json.loads(payload) == {"recurring": "daily", "at": "06:00",
                                   "pace_ms": 400}
    assert priority == 50
    assert run_after == datetime(2026, 9, 16, 6, 0, tzinfo=timezone.utc)


def test_seed_is_idempotent(db, configured):
    first = jobs_run.seed(now=NOW)
    second = jobs_run.seed(now=NOW)

    assert first.startswith("queued daily")
    assert second.startswith("daily mirror_refresh already queued")
    assert len(db.inserts) == 1


def test_seed_checks_for_a_queued_daily_row_only(db, configured):
    jobs_run.seed(now=NOW)
    sql, params = db.matching("SELECT id, run_after FROM jobs")[0]
    assert "status = 'queued'" in sql
    assert "payload->>'recurring' = 'daily'" in sql
    assert params == ("mirror_refresh",)


def test_now_queues_a_one_off_without_recurrence(db, configured):
    message = jobs_run.queue_now(now=NOW)

    assert message.startswith("queued one-off mirror_refresh as job 501")
    _, params = db.inserts[0]
    payload = json.loads(params[1])
    assert "recurring" not in payload and "at" not in payload
    assert payload == {"pace_ms": 400}
    assert params[3] == NOW


def test_cli_refuses_without_database_url(monkeypatch, capsys):
    monkeypatch.setattr("clients.database.is_configured", lambda: False)
    assert jobs_run.main(["--once"]) == 2
    assert "DATABASE_URL is not set" in capsys.readouterr().err


def test_cli_once_exit_code_reflects_failures(db, handlers, configured,
                                              capsys):
    db.claimable = [job(job_id=1, job_type="demo")]
    assert jobs_run.main(["--once"]) == 0
    assert "1 complete, 0 failed" in capsys.readouterr().out

    db.claimable = [job(job_id=2, job_type="boom")]
    assert jobs_run.main(["--once"]) == 1
    assert "0 complete, 1 failed" in capsys.readouterr().out


def test_cli_once_logs_one_line_per_job(db, handlers, configured, caplog):
    import logging

    db.claimable = [job(job_id=1, job_type="demo"),
                    job(job_id=2, job_type="boom")]
    with caplog.at_level(logging.INFO, logger="jobs_run"):
        jobs_run.main(["--once"])

    lines = [r.getMessage() for r in caplog.records if r.name == "jobs_run"]
    assert any("job 1 | demo | complete |" in l for l in lines)
    assert any("job 2 | boom | failed |" in l and "kaboom" in l
               for l in lines)


def test_cli_turns_an_unreachable_database_into_exit_2(monkeypatch, configured,
                                                       capsys):
    def broken(sql, params=None, fetch=True):
        raise RuntimeError('OperationalError: could not translate host name')

    monkeypatch.setattr("clients.database.execute_query", broken)
    assert jobs_run.main(["--once"]) == 2
    assert "jobs runner stopped" in capsys.readouterr().err


def test_cli_requires_exactly_one_mode(configured):
    with pytest.raises(SystemExit):
        jobs_run.main([])
    with pytest.raises(SystemExit):
        jobs_run.main(["--once", "--seed"])
