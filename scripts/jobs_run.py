"""
Jobs Runner CLI
===============
What Railway's cron service runs, and what you run by hand to seed or
test the queue.

    python scripts/jobs_run.py --once   # claim and run until the queue
                                        # is empty; exit 1 if any failed
    python scripts/jobs_run.py --seed   # queue the daily mirror_refresh
                                        # at 06:00 UTC, once
    python scripts/jobs_run.py --now    # queue a one-off mirror_refresh
                                        # to run on the next --once

Refuses to start (exit 2) without DATABASE_URL: a runner with no queue
to read has nothing to do, and pretending otherwise would let a
misconfigured cron service exit 0 every night while the mirror aged.

Exit codes: 0 ok · 1 a job failed this run · 2 not configured.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

# Make the repo root importable when run as `python scripts/jobs_run.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clients import database  # noqa: E402
from jobs import runner  # noqa: E402
import jobs.handlers  # noqa: E402,F401  (registers every handler)

logger = logging.getLogger("jobs_run")

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_NOT_CONFIGURED = 2

# The one recurring job this repo ships with.
MIRROR_JOB_TYPE = "mirror_refresh"
MIRROR_SEED = {
    "job_type": MIRROR_JOB_TYPE,
    "payload": {"recurring": "daily", "at": "06:00", "pace_ms": 400},
    "priority": 50,
}

# The scheduled registrations chain (jobs/handlers/registrations_sync.py).
#
# priority 40, BEFORE the mirror's 50: claims go `ORDER BY priority,
# run_after`, and the daily email this job sends is the dead-man's switch.
# It should not wait behind — or be hostage to — a seven-minute mirror.
#
# max_attempts 1: a chain that stopped is NOT retried. Every write inside
# it is individually guarded, but "never retry a write" is the rule, and a
# guard that refused did so for a reason a human should read. Tomorrow's
# 06:00 run is a fresh daily run, not a retry.
REGISTRATIONS_JOB_TYPE = "registrations_sync"
REGISTRATIONS_SEED = {
    "job_type": REGISTRATIONS_JOB_TYPE,
    "payload": {"recurring": "daily", "at": "06:00"},
    "priority": 40,
    "max_attempts": 1,
}

_INSERT_SQL = """
    INSERT INTO jobs (job_type, payload, status, priority, run_after)
    VALUES (%s, %s::jsonb, 'queued', %s, %s)
    RETURNING id
"""

_INSERT_WITH_ATTEMPTS_SQL = """
    INSERT INTO jobs (job_type, payload, status, priority, run_after,
                      max_attempts)
    VALUES (%s, %s::jsonb, 'queued', %s, %s, %s)
    RETURNING id
"""

# ANY live row counts, not just queued ones. The runner's own dedupe looks
# at status = 'queued' only, so a seed that ran while the day's job was
# claimed or running would have added a second one.
_ACTIVE_RECURRING_SQL = """
    SELECT id, status, run_after
      FROM jobs
     WHERE job_type = %s
       AND payload->>'recurring' = 'daily'
       AND status IN ('queued', 'claimed', 'running')
     ORDER BY run_after
     LIMIT 1
"""

# A mirror that is already due or already running makes --now a second
# concurrent mirror, which is exactly what drew 429s on 2026-09-15.
_MIRROR_BUSY_SQL = """
    SELECT id, status, run_after
      FROM jobs
     WHERE job_type = %s
       AND (
             status IN ('claimed', 'running')
          OR (status = 'queued' AND run_after <= NOW())
       )
     ORDER BY status, run_after
     LIMIT 1
"""

_QUEUED_DAILY_SQL = """
    SELECT id, run_after
      FROM jobs
     WHERE status = 'queued'
       AND job_type = %s
       AND payload->>'recurring' = 'daily'
     ORDER BY run_after
     LIMIT 1
"""


def _first(found):
    if not isinstance(found, (list, tuple)) or not found:
        return None
    return found[0]


def _get(row, key, index=0):
    if isinstance(row, dict):
        return row.get(key)
    try:
        return row[index]
    except (IndexError, TypeError):
        return None


def seed(now=None) -> str:
    """Queue the daily mirror_refresh unless one is already queued."""
    existing = _first(database.execute_query(
        _QUEUED_DAILY_SQL, (MIRROR_JOB_TYPE,), fetch=True))
    if existing is not None:
        return (f"daily {MIRROR_JOB_TYPE} already queued as job "
                f"{_get(existing, 'id', 0)} for {_get(existing, 'run_after', 1)}"
                " — nothing added")

    when = runner.next_occurrence(MIRROR_SEED["payload"]["at"], now)
    inserted = _first(database.execute_query(
        _INSERT_SQL,
        (MIRROR_JOB_TYPE, json.dumps(MIRROR_SEED["payload"], sort_keys=True),
         MIRROR_SEED["priority"], when),
        fetch=True))
    return (f"queued daily {MIRROR_JOB_TYPE} as job {_get(inserted, 'id')} "
            f"for {when.isoformat()}")


def ensure_registrations_sync(now=None) -> str:
    """Keep exactly one live recurring registrations_sync row.

    Called by --seed AND at the start of every --once, so the schedule
    maintains itself: nobody has to remember to seed it, and nobody has to
    type anything for the first 06:15 email after a deploy.

    Idempotent. If a queued, claimed or running row exists, nothing is
    added. Otherwise one is queued DUE NOW, so the --once that called this
    claims it in the same invocation; the runner then queues tomorrow's
    06:00 itself after the run, carrying max_attempts = 1 forward.

    Seeded whether or not SCHEDULED_SYNC_ENABLED is set. The handler
    decides — disabled logs and exits with no email — so arming the
    schedule stays a single flag, with no seed step to forget.
    """
    existing = _first(database.execute_query(
        _ACTIVE_RECURRING_SQL, (REGISTRATIONS_JOB_TYPE,), fetch=True))
    if existing is not None:
        return (f"daily {REGISTRATIONS_JOB_TYPE} already "
                f"{_get(existing, 'status', 1)} as job "
                f"{_get(existing, 'id', 0)} for "
                f"{_get(existing, 'run_after', 2)} — nothing added")

    when = now or datetime.now(timezone.utc)
    inserted = _first(database.execute_query(
        _INSERT_WITH_ATTEMPTS_SQL,
        (REGISTRATIONS_JOB_TYPE,
         json.dumps(REGISTRATIONS_SEED["payload"], sort_keys=True),
         REGISTRATIONS_SEED["priority"], when,
         REGISTRATIONS_SEED["max_attempts"]),
        fetch=True))
    return (f"queued daily {REGISTRATIONS_JOB_TYPE} as job "
            f"{_get(inserted, 'id')}, due now ({when.isoformat()}); the "
            f"runner re-queues 06:00 UTC daily from here")


class Refused(Exception):
    """--now would start a second mirror; the message says which one."""


def queue_now(now=None) -> str:
    """Queue a one-off mirror_refresh to run immediately. No recurrence.

    Raises Refused if a mirror_refresh is already due (queued with
    run_after in the past) or in flight.
    """
    busy = _first(database.execute_query(
        _MIRROR_BUSY_SQL, (MIRROR_JOB_TYPE,), fetch=True))
    if busy is not None:
        status = _get(busy, "status", 1)
        raise Refused(
            f"refused: {MIRROR_JOB_TYPE} job {_get(busy, 'id', 0)} is already "
            f"{'running' if status in ('claimed', 'running') else 'due'} "
            f"(status {status}, run_after {_get(busy, 'run_after', 2)}) — "
            "one mirror at a time")

    payload = {k: v for k, v in MIRROR_SEED["payload"].items()
               if k not in ("recurring", "at")}
    when = now or datetime.now(timezone.utc)
    inserted = _first(database.execute_query(
        _INSERT_SQL,
        (MIRROR_JOB_TYPE, json.dumps(payload, sort_keys=True),
         MIRROR_SEED["priority"], when),
        fetch=True))
    return (f"queued one-off {MIRROR_JOB_TYPE} as job {_get(inserted, 'id')} "
            "— run `--once` to execute it")


def run_once() -> int:
    worker = runner.worker_id()
    # Self-maintaining schedule. A failure here is logged and the run goes
    # on: the jobs already queued — the mirror among them — must not be
    # held hostage to a seed that could not write.
    try:
        logger.info(ensure_registrations_sync())
    except Exception as exc:
        logger.error("could not ensure the %s job: %s: %s",
                     REGISTRATIONS_JOB_TYPE, type(exc).__name__, exc)
    try:
        summaries = runner.run_until_empty(worker)
    except Exception as exc:
        # The runner lets ledger failures propagate on purpose (a runner
        # that cannot write its own status must not keep claiming). Here
        # that becomes an exit code and one line, not a traceback that
        # Railway's cron view truncates to its first frame.
        logger.error("jobs runner stopped: %s: %s",
                     type(exc).__name__, exc, exc_info=True)
        print(f"jobs runner stopped: {type(exc).__name__}: {exc}",
              file=sys.stderr)
        return EXIT_NOT_CONFIGURED if _looks_like_connection_failure(exc) \
            else EXIT_FAILED

    for summary in summaries:
        logger.info("job %s | %s | %s | %ss%s",
                    summary["job_id"], summary["job_type"], summary["status"],
                    summary["seconds"],
                    f" | {summary['error']}" if summary.get("error") else "")

    failed = [s for s in summaries if s["status"] != runner.STATUS_COMPLETE]
    print(f"{len(summaries)} job(s) run as {worker}: "
          f"{len(summaries) - len(failed)} complete, {len(failed)} failed")
    return EXIT_FAILED if failed else EXIT_OK


def _looks_like_connection_failure(exc) -> bool:
    """A database that cannot be reached is a configuration problem."""
    text = f"{type(exc).__name__}: {exc}".lower()
    return any(token in text for token in (
        "operationalerror", "could not translate host", "could not connect",
        "connection refused", "timeout expired", "database_url"))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Run queued jobs, or queue the nightly mirror refresh.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--once", action="store_true",
                       help="Claim and run jobs until nothing is claimable.")
    group.add_argument("--seed", action="store_true",
                       help="Queue the daily 06:00 UTC mirror_refresh if it "
                            "is not already queued.")
    group.add_argument("--now", action="store_true",
                       help="Queue a one-off mirror_refresh for the next "
                            "--once. For testing.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stderr,
    )

    if not database.is_configured():
        print("DATABASE_URL is not set — the jobs runner has no queue to "
              "read. Not started.", file=sys.stderr)
        return EXIT_NOT_CONFIGURED

    if args.seed:
        print(seed())
        print(ensure_registrations_sync())
        return EXIT_OK
    if args.now:
        try:
            print(queue_now())
        except Refused as why:
            print(str(why), file=sys.stderr)
            return EXIT_FAILED
        return EXIT_OK
    return run_once()


if __name__ == "__main__":
    sys.exit(main())
