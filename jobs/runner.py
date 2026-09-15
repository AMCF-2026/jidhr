"""
Job Runner
==========
Claim one queued row from `jobs`, run its handler, record what happened,
and queue whatever should follow: a retry, the next daily occurrence, or
nothing.

The table (exists; never created or altered from here)
-----------------------------------------------------
    id, job_type, payload jsonb, status, priority, run_after, attempts,
    max_attempts, requested_by, sync_run_id, claimed_by, claimed_at,
    started_at, finished_at, result jsonb, error, created_at

Status walks queued -> running -> complete | failed. There is no schedule
column: recurrence lives in the payload as {"recurring": "daily",
"at": "HH:MM"} (UTC), and the runner queues the next occurrence itself.

Why one UPDATE claims a job
---------------------------
The claim is a single statement — an UPDATE whose WHERE picks the row via
a `FOR UPDATE SKIP LOCKED` subselect — rather than a SELECT followed by
an UPDATE. One statement is one transaction by construction, so two
workers starting at the same moment cannot both read the same queued row
before either has marked it running. It also means the claim goes
through clients.database.execute_query like every other write in this
codebase, which is what makes it testable without a database.

Stale jobs and the per-type mutex
---------------------------------
On 2026-09-15 two mirror_refresh jobs were killed when the Railway shell
that started them closed. Both sat at 'running' forever — nothing ever
writes a finish for a process that no longer exists — and for a while
they ran at once and drew 429s from CSuite.

So every --once begins by reaping: any row 'running' (or 'claimed') whose
started_at is older than its payload.max_runtime (minutes, default 90) is
marked failed with error 'stale: no finish recorded', and the ordinary
retry / recurrence rules apply as if it had just failed. And the claim
refuses to start a job while another row of the same job_type is
genuinely running: one mirror at a time.

What a failure looks like
-------------------------
A handler that raises marks its row 'failed' with the exception text and
the attempt count. If attempts remain, a FRESH row is queued for the
retry — the failed row keeps its error, so the history is readable — with
run_after pushed out 15 minutes per attempt already made. When the last
attempt fails, no retry is queued, but a daily job still gets its next
occurrence: one bad night must not silence the mirror for good.
"""

import json
import logging
import os
import socket
import time
import traceback
from datetime import datetime, timedelta, timezone

from clients import database

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

# job_type -> callable(job) -> dict. Handlers register themselves at import
# time (see jobs/handlers/__init__.py); the runner does not import them.
HANDLERS = {}


def register(job_type: str):
    """Decorator: `@register("mirror_refresh")` above a handle(job)."""
    def decorate(func):
        if job_type in HANDLERS and HANDLERS[job_type] is not func:
            raise ValueError(f"job_type {job_type!r} is already registered")
        HANDLERS[job_type] = func
        return func
    return decorate


class JobFailed(RuntimeError):
    """A handler's way of failing WITH a result to keep.

    A plain exception loses whatever the handler learned before it gave
    up. Raising this with `result=` puts that dict into the job's result
    column beside the error, so a mirror refresh that completed six types
    and was rate limited on the seventh still records the six.
    """

    def __init__(self, message: str, result: dict = None):
        super().__init__(message)
        self.result = result if isinstance(result, dict) else {}


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_COMPLETE = "complete"
STATUS_FAILED = "failed"

RETRY_STEP_MINUTES = 15
MAX_ERROR_CHARS = 2000

# 'claimed' is not a status this runner writes (a claim goes straight to
# 'running'), but a row could carry it from elsewhere; it is treated as
# in-flight everywhere 'running' is.
IN_FLIGHT_STATUSES = ("claimed", "running")

# Minutes a job may run before the reaper treats it as dead. Overridable
# per job via payload.max_runtime. 90 covers a full 960-call mirror at
# the 400ms pace (11-14 min) six times over.
DEFAULT_MAX_RUNTIME_MINUTES = 90

STALE_ERROR = "stale: no finish recorded"

# Every column a handler might want to see, in the order RETURNING lists
# them. Kept explicit so a tuple-shaped row can be mapped as well as a
# dict-shaped one.
JOB_COLUMNS = (
    "id", "job_type", "payload", "status", "priority", "run_after",
    "attempts", "max_attempts", "requested_by", "sync_run_id", "claimed_by",
    "claimed_at", "started_at", "finished_at", "result", "error",
    "created_at",
)


def _utcnow() -> datetime:
    """The runner's clock. One function so tests can pin it."""
    return datetime.now(timezone.utc)


def worker_id() -> str:
    """hostname:pid — enough to tell two Railway replicas apart in the log."""
    return f"{socket.gethostname()}:{os.getpid()}"


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

# A row is "genuinely running" if it is in flight AND younger than its own
# max_runtime. The regex guard keeps a non-numeric max_runtime from
# turning the whole claim into a cast error.
_IN_FLIGHT_STATUS_LIST = ", ".join(f"'{s}'" for s in IN_FLIGHT_STATUSES)

_LIVE_SIBLING_SQL = f"""
    SELECT 1
      FROM jobs r
     WHERE r.job_type = q.job_type
       AND r.status IN ({_IN_FLIGHT_STATUS_LIST})
       AND r.started_at > NOW() - (
           CASE WHEN r.payload->>'max_runtime' ~ '^[0-9]+$'
                THEN (r.payload->>'max_runtime')::int
                ELSE {DEFAULT_MAX_RUNTIME_MINUTES} END
           * INTERVAL '1 minute')
"""

_CLAIM_SQL = f"""
    UPDATE jobs
       SET status = '{STATUS_RUNNING}',
           claimed_by = %s,
           claimed_at = NOW(),
           started_at = NOW()
     WHERE id = (
           SELECT q.id
             FROM jobs q
            WHERE q.status = '{STATUS_QUEUED}'
              AND q.run_after <= NOW()
              AND NOT EXISTS ({_LIVE_SIBLING_SQL})
            ORDER BY q.priority, q.run_after
            FOR UPDATE SKIP LOCKED
            LIMIT 1
     )
    RETURNING {", ".join(JOB_COLUMNS)}
"""

# Runnable rows that the mutex is holding back, for one INFO line.
_BLOCKED_TYPES_SQL = f"""
    SELECT DISTINCT q.job_type
      FROM jobs q
     WHERE q.status = '{STATUS_QUEUED}'
       AND q.run_after <= NOW()
       AND EXISTS ({_LIVE_SIBLING_SQL})
"""

_IN_FLIGHT_SQL = f"""
    SELECT {", ".join(JOB_COLUMNS)}
      FROM jobs
     WHERE status IN ({_IN_FLIGHT_STATUS_LIST})
"""

_COMPLETE_SQL = f"""
    UPDATE jobs
       SET status = '{STATUS_COMPLETE}',
           finished_at = NOW(),
           attempts = attempts + 1,
           result = %s::jsonb,
           sync_run_id = COALESCE(%s, sync_run_id),
           error = NULL
     WHERE id = %s
"""

_FAIL_SQL = f"""
    UPDATE jobs
       SET status = '{STATUS_FAILED}',
           finished_at = NOW(),
           attempts = attempts + 1,
           error = %s,
           result = %s::jsonb
     WHERE id = %s
"""

# A retry is a NEW row so the failed one keeps its error text.
_RETRY_SQL = f"""
    INSERT INTO jobs (job_type, payload, status, priority, run_after,
                      requested_by, attempts, max_attempts)
    VALUES (%s, %s::jsonb, '{STATUS_QUEUED}', %s, NOW() + %s::interval,
            %s, %s, %s)
    RETURNING id
"""

_NEXT_OCCURRENCE_SQL = f"""
    INSERT INTO jobs (job_type, payload, status, priority, run_after,
                      requested_by)
    VALUES (%s, %s::jsonb, '{STATUS_QUEUED}', %s, %s, %s)
    RETURNING id
"""

_QUEUED_RECURRING_SQL = f"""
    SELECT COUNT(*) AS n
      FROM jobs
     WHERE status = '{STATUS_QUEUED}'
       AND job_type = %s
       AND payload->>'recurring' = %s
"""


# ---------------------------------------------------------------------------
# Row helpers
# ---------------------------------------------------------------------------

def _as_job(row) -> dict | None:
    """One returned row as a dict with a dict-shaped payload."""
    if row is None:
        return None
    job = dict(row) if isinstance(row, dict) else dict(zip(JOB_COLUMNS, row))
    job["payload"] = _as_payload(job.get("payload"))
    return job


def _as_payload(value) -> dict:
    """payload jsonb as a dict, whether the driver hands back dict or text."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def _first(found):
    if not isinstance(found, (list, tuple)) or not found:
        return None
    return found[0]


def _dump(value) -> str:
    return json.dumps(value, default=str, sort_keys=True)


# ---------------------------------------------------------------------------
# Recurrence
# ---------------------------------------------------------------------------

def recurrence_of(payload: dict):
    """("daily", "HH:MM") if the payload asks for it, else None.

    Only "daily" at a clock time is supported; anything else in the
    `recurring` key is logged and ignored rather than silently treated as
    daily.
    """
    if not isinstance(payload, dict):
        return None
    recurring = payload.get("recurring")
    if not recurring:
        return None
    if recurring != "daily":
        logger.warning("unsupported recurrence %r — not re-queued", recurring)
        return None
    at = str(payload.get("at") or "").strip()
    try:
        hour, minute = at.split(":")
        hour, minute = int(hour), int(minute)
        if not (0 <= hour < 24 and 0 <= minute < 60):
            raise ValueError
    except ValueError:
        logger.warning("recurring daily job has no usable 'at' (%r) — "
                       "not re-queued", at)
        return None
    return "daily", f"{hour:02d}:{minute:02d}"


def next_occurrence(at: str, now=None) -> datetime:
    """The next time HH:MM UTC comes round, strictly after `now`."""
    now = now or _utcnow()
    hour, minute = (int(part) for part in at.split(":"))
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def queue_next_occurrence(job: dict, now=None) -> int | None:
    """Queue the next daily run of a recurring job. Returns the new id.

    Returns None (and queues nothing) if the job is not recurring or a
    queued row for the same job_type and recurrence already exists —
    which is how two workers finishing the same job cannot queue two
    tomorrows.
    """
    recurrence = recurrence_of(job.get("payload"))
    if recurrence is None:
        return None
    kind, at = recurrence

    found = _first(database.execute_query(
        _QUEUED_RECURRING_SQL, (job["job_type"], kind), fetch=True))
    already = int(found.get("n") if isinstance(found, dict) else found[0]) \
        if found is not None else 0
    if already:
        logger.info("next %s %s already queued — not adding another",
                    kind, job["job_type"])
        return None

    when = next_occurrence(at, now)
    inserted = _first(database.execute_query(
        _NEXT_OCCURRENCE_SQL,
        (job["job_type"], _dump(job.get("payload") or {}),
         job.get("priority"), when, job.get("requested_by")),
        fetch=True))
    new_id = (inserted.get("id") if isinstance(inserted, dict)
              else inserted[0]) if inserted is not None else None
    logger.info("queued next %s %s as job %s for %s",
                kind, job["job_type"], new_id, when.isoformat())
    return new_id


# ---------------------------------------------------------------------------
# Claim / run / finish
# ---------------------------------------------------------------------------

def claim_next(worker: str = None) -> dict | None:
    """Mark the next runnable job as running and return it, or None.

    Skips any queued row whose job_type has another row genuinely in
    flight — see _LIVE_SIBLING_SQL. Two mirror refreshes at once is how
    the 2026-09-15 429s happened.
    """
    worker = worker or worker_id()
    found = database.execute_query(_CLAIM_SQL, (worker,), fetch=True)
    job = _as_job(_first(found))
    if job is not None:
        logger.info("claimed job %s (%s) as %s",
                    job["id"], job["job_type"], worker)
    return job


def blocked_job_types() -> list:
    """job_types with runnable rows held back by a running sibling."""
    found = database.execute_query(_BLOCKED_TYPES_SQL, (), fetch=True)
    if not isinstance(found, (list, tuple)):
        return []
    types = []
    for row in found:
        value = row.get("job_type") if isinstance(row, dict) else row[0]
        if value:
            types.append(str(value))
    return sorted(set(types))


def max_runtime_minutes(payload) -> int:
    """payload.max_runtime as minutes, or the default when unusable."""
    raw = (payload or {}).get("max_runtime") if isinstance(payload, dict) \
        else None
    try:
        minutes = int(raw)
        if minutes > 0:
            return minutes
    except (TypeError, ValueError):
        pass
    return DEFAULT_MAX_RUNTIME_MINUTES


def _as_datetime(value):
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def is_stale(job: dict, now=None) -> bool:
    """True if an in-flight job has outlived its max_runtime."""
    if job.get("status") not in IN_FLIGHT_STATUSES:
        return False
    started = _as_datetime(job.get("started_at") or job.get("claimed_at"))
    if started is None:
        # In flight with no start time at all: nothing will ever finish it.
        return True
    now = now or _utcnow()
    limit = timedelta(minutes=max_runtime_minutes(job.get("payload")))
    return now - started > limit


def reap_stale(now=None) -> list:
    """Fail every in-flight job that has outlived its max_runtime.

    Returns one summary per reaped job. Each is failed exactly the way a
    handler exception fails it — same attempt count, same retry backoff,
    same recurrence — so a killed process and a crashed one leave the same
    trail and the queue recovers the same way.
    """
    found = database.execute_query(_IN_FLIGHT_SQL, (), fetch=True)
    if not isinstance(found, (list, tuple)):
        return []

    reaped = []
    for row in found:
        job = _as_job(row)
        if job is None or not is_stale(job, now):
            continue
        started = job.get("started_at")
        logger.warning(
            "job %s (%s) has been %s since %s with no finish — reaping",
            job["id"], job["job_type"], job.get("status"), started)
        reaped.append(_fail(job, STALE_ERROR, {
            "reaped_at": (now or _utcnow()).isoformat(),
            "started_at": str(started),
            "claimed_by": job.get("claimed_by"),
            "max_runtime_minutes": max_runtime_minutes(job.get("payload")),
        }, seconds=None))
    if reaped:
        logger.warning("reaped %d stale job(s): %s", len(reaped),
                       ", ".join(str(r["job_id"]) for r in reaped))
    return reaped


def _fail(job: dict, error: str, extra_result: dict, seconds) -> dict:
    """Record a failure and queue what follows. Shared by run() and the
    reaper so the two paths cannot drift apart."""
    job_type = job.get("job_type")
    summary = {
        "job_id": job.get("id"),
        "job_type": job_type,
        "status": STATUS_FAILED,
        "seconds": seconds,
        "attempt": int(job.get("attempts") or 0) + 1,
        "max_attempts": int(job.get("max_attempts") or 1),
        "retry_queued": None,
        "next_occurrence_queued": None,
        "error": str(error)[:MAX_ERROR_CHARS],
    }

    database.execute_query(
        _FAIL_SQL,
        (summary["error"], _dump({**extra_result, "_run": summary}),
         job["id"]),
        fetch=False)

    attempts_made = summary["attempt"]
    if attempts_made < summary["max_attempts"]:
        summary["retry_queued"] = queue_retry(job, attempts_made)
    else:
        logger.error("job %s (%s) exhausted %s attempts — not retried",
                     job.get("id"), job_type, summary["max_attempts"])
        summary["next_occurrence_queued"] = queue_next_occurrence(job)
    return summary


def queue_retry(job: dict, attempts_made: int) -> int | None:
    """A fresh queued row for another go, backed off by attempts made."""
    delay = f"{RETRY_STEP_MINUTES * attempts_made} minutes"
    inserted = _first(database.execute_query(
        _RETRY_SQL,
        (job["job_type"], _dump(job.get("payload") or {}),
         job.get("priority"), delay, job.get("requested_by"),
         attempts_made, job.get("max_attempts")),
        fetch=True))
    new_id = (inserted.get("id") if isinstance(inserted, dict)
              else inserted[0]) if inserted is not None else None
    logger.info("job %s failed on attempt %s — retry queued as job %s in %s",
                job["id"], attempts_made, new_id, delay)
    return new_id


def run(job: dict) -> dict:
    """Run one claimed job to its final status. Returns a summary.

    Never raises for a handler failure — that becomes status='failed'.
    A database failure while recording the outcome does propagate: a
    runner that cannot write its ledger should stop, not carry on
    claiming.
    """
    started = time.perf_counter()
    job_type = job.get("job_type")
    handler = HANDLERS.get(job_type)

    summary = {
        "job_id": job.get("id"),
        "job_type": job_type,
        "status": None,
        "seconds": None,
        "attempt": int(job.get("attempts") or 0) + 1,
        "max_attempts": int(job.get("max_attempts") or 1),
        "retry_queued": None,
        "next_occurrence_queued": None,
        "error": None,
    }

    try:
        if handler is None:
            raise LookupError(
                f"no handler registered for job_type {job_type!r} "
                f"(known: {', '.join(sorted(HANDLERS)) or 'none'})")
        result = handler(job)
        if not isinstance(result, dict):
            result = {"returned": result}
        summary["seconds"] = round(time.perf_counter() - started, 2)
        summary["status"] = STATUS_COMPLETE

        database.execute_query(
            _COMPLETE_SQL,
            (_dump({**result, "_run": summary}),
             result.get("sync_run_id"), job["id"]),
            fetch=False)
        logger.info("job %s (%s) complete in %ss",
                    job["id"], job_type, summary["seconds"])

        summary["next_occurrence_queued"] = queue_next_occurrence(job)
        return summary

    except Exception as exc:
        logger.error("job %s (%s) failed on attempt %s/%s: %s",
                     job.get("id"), job_type, summary["attempt"],
                     summary["max_attempts"], exc, exc_info=True)
        return _fail(
            job, str(exc),
            {**(exc.result if isinstance(exc, JobFailed) else {}),
             "traceback": traceback.format_exc()[-MAX_ERROR_CHARS:]},
            seconds=round(time.perf_counter() - started, 2))


def run_until_empty(worker: str = None, limit: int = 1000,
                    reap: bool = True) -> list:
    """Reap, then claim and run until nothing is claimable.

    Returns every summary — reaped jobs first, then the ones this call
    ran. `limit` is a guard, not a quota: a queue that keeps producing
    claimable rows faster than they finish would otherwise hold the cron
    service open forever.
    """
    worker = worker or worker_id()
    summaries = list(reap_stale()) if reap else []

    for _ in range(limit):
        job = claim_next(worker)
        if job is None:
            break
        # Logged BEFORE the handler runs, so a process killed mid-job
        # still leaves this line — the 2026-09-15 runs left nothing.
        logger.info("job %s: started (%s) as %s",
                    job["id"], job["job_type"], worker)
        summaries.append(run(job))
    else:
        logger.warning("stopped after %d jobs with work still claimable",
                       limit)

    blocked = blocked_job_types()
    if blocked:
        logger.info(
            "runnable %s job(s) held back — another of the same type is "
            "running; they will go on the next --once",
            ", ".join(blocked))
    return summaries
