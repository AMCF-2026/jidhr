"""
Job: mirror_refresh
===================
The nightly CSuite mirror fill, run by the jobs runner instead of by
someone at a terminal.

payload
    pace_ms   optional; milliseconds between CSuite calls. Falls back to
              CSUITE_PACE_MS in the environment, then 400 — the pace that
              ran 960 calls clean on 2026-09-10.
    recurring / at
              read by the runner, not here.

result
    per_type        one row per record type: type, expected, fetched,
                    complete, written, unchanged, deleted, calls,
                    total_429s, seconds, status
    sync_run_ids    every sync_runs row this job opened
    sync_run_id     the first of them, so the job row's sync_run_id column
                    points at something — the full list is in the result

A job is 'failed' if ANY record type did not complete. The mirror module
already refuses to write a partial type; this refuses to call the night
a success when one of them was refused.
"""

import logging

from clients.csuite_fetch import configured_pace_ms
from jobs.runner import JobFailed, register
from sync import mirror

logger = logging.getLogger(__name__)

JOB_TYPE = "mirror_refresh"
TRIGGER_LABEL = "job:mirror_refresh"

# sync_runs.trigger_source is CHECK-constrained and 'cli' is the one value
# confirmed allowed (Step 3a-fix). The job is named in notes.trigger, so
# the ledger still says who ran it; if the constraint admits 'job', change
# this to that.
TRIGGER_SOURCE = "cli"

RESULT_COLUMNS = ("type", "expected", "fetched", "complete", "written",
                  "unchanged", "deleted", "calls", "total_429s", "seconds",
                  "status")


def pace_from(payload: dict) -> int:
    """payload.pace_ms if it is a usable integer, else the configured pace."""
    raw = (payload or {}).get("pace_ms")
    if raw is not None:
        try:
            pace = int(raw)
            if pace >= 0:
                return pace
        except (TypeError, ValueError):
            pass
        logger.warning("payload.pace_ms=%r is not usable — using the "
                       "configured pace", raw)
    return configured_pace_ms()


def result_row(type_result) -> dict:
    return {
        "type": type_result.record_type,
        "expected": type_result.expected,
        "fetched": type_result.fetched,
        "complete": type_result.complete,
        "written": type_result.written,
        "unchanged": type_result.unchanged,
        "deleted": type_result.deleted,
        "calls": type_result.calls,
        "total_429s": type_result.total_429s,
        "seconds": type_result.seconds,
        "status": type_result.status,
    }


@register(JOB_TYPE)
def handle(job: dict) -> dict:
    payload = job.get("payload") or {}
    pace_ms = pace_from(payload)

    logger.info("mirror_refresh job %s: every record type at %dms pace",
                job.get("id"), pace_ms)

    results = mirror.refresh(
        record_types=None,
        pace_ms=pace_ms,
        dry_run=False,
        triggered_by=TRIGGER_LABEL,
        trigger_source=TRIGGER_SOURCE,
        triggered_by_user_id=job.get("requested_by"),
    )

    per_type = [result_row(r) for r in results]
    run_ids = [r.run_id for r in results if r.run_id is not None]

    result = {
        "per_type": per_type,
        "sync_run_ids": run_ids,
        "sync_run_id": run_ids[0] if run_ids else None,
        "calls": sum(r.calls for r in results),
        "total_429s": sum(r.total_429s for r in results),
    }

    failed = [r for r in results if r.status != "complete"]
    if failed:
        # JobFailed carries the per-type table with it, so the job row
        # shows which six types completed and which one was refused —
        # not just "1 of 8 failed".
        detail = "; ".join(
            f"{r.record_type}: {r.error or r.status}" for r in failed)
        raise JobFailed(
            f"{len(failed)} of {len(results)} record types did not "
            f"complete — {detail}", result=result)

    return result
