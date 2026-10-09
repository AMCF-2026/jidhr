"""Registrations applies run in the background, not inside the HTTP request.

Why this exists
---------------
Production, 2026-10-09: "sync registrations apply event 1157 limit 29".
run_log 30 recorded status **complete**, 19:20:08 to 19:25:27 — **318
seconds**. All 29 writes landed and verified; registration_map holds 29
'synced' rows for 1157 and no unverified ones. The chat UI showed
"Network error. Please try again."

Railway's public proxy closes a request after 5 minutes with no data
transferred. 318 > 300, so the response was discarded by the platform after
the work had already succeeded. Nothing in the app timed out: gunicorn's
worker was not killed (the run_log row has a finished_at and a 'complete'
status, which a SIGKILLed worker could not have written), and the chat
page's fetch() sets no timeout at all.

The run time is not going to come down. The verification retries are there
on purpose — 8 writes took 26 HubSpot reads — so a 90-record apply would be
further over the line, not closer to it. "Raise the timeout" is not
available either: the 5 minutes is Railway's, not ours.

So the request stops owning the run. Chat starts the work, answers with the
run_log id, and the report is read back from run_log afterwards. A lost
response now costs nothing, because the response was never carrying the
result.

ONE AT A TIME
-------------
The attendance endpoint has no idempotency key, so two concurrent applies
over the same event are how one registration becomes two. The guard is the
run_log table, not a process variable: there are eight gunicorn workers and
a module-level flag in one of them tells the other seven nothing.
"""

import logging
import threading

from clients import database
from sync import registrations as reg

logger = logging.getLogger(__name__)

# A 'running' row older than this is treated as abandoned rather than as a
# reason to refuse forever. A worker killed mid-run leaves its row 'running'
# with no finished_at, and nothing else would ever clear it.
STALE_RUNNING_MINUTES = 30

_RUNNING_SQL = """
    SELECT id, started_at,
           EXTRACT(EPOCH FROM (NOW() - started_at)) AS age_seconds
      FROM hubsync.run_log
     WHERE job = 'registrations_sync'
       AND applied IS TRUE
       AND status = 'running'
       AND started_at > NOW() - (%s * INTERVAL '1 minute')
     ORDER BY id
"""

_RUN_SQL = """
    SELECT id, applied, status, started_at, finished_at, error_summary,
           outcomes,
           EXTRACT(EPOCH FROM (COALESCE(finished_at, NOW()) - started_at))
               AS seconds
      FROM hubsync.run_log
     WHERE job = 'registrations_sync' AND id = %s
"""

_LATEST_SQL = """
    SELECT id, applied, status, started_at, finished_at, error_summary,
           outcomes,
           EXTRACT(EPOCH FROM (COALESCE(finished_at, NOW()) - started_at))
               AS seconds
      FROM hubsync.run_log
     WHERE job = 'registrations_sync' AND applied IS TRUE
     ORDER BY id DESC
     LIMIT 1
"""

# Progress for a run still in flight. Counted from write_audit, which every
# write passes through anyway — no second progress table to keep in step
# with the first.
_WRITES_SINCE_SQL = """
    SELECT COUNT(*) AS n
      FROM write_audit
     WHERE target_system = 'hubspot'
       AND http_method = 'POST'
       AND endpoint LIKE '%%marketing-events/attendance/%%'
       AND created_at >= %s
"""

# Narrows the window in which two requests could both pass the pre-check.
# Across workers the run_log row is the real guard; this closes the
# same-worker race, which is the likely one when somebody double-clicks.
_START_LOCK = threading.Lock()


class ApplyAlreadyRunning(RuntimeError):
    """A registrations apply is already in flight."""


class RunNotFound(RuntimeError):
    """No registrations run with that id."""


def running_applies() -> list:
    """Live 'running' apply rows, oldest first. Stale rows are excluded."""
    try:
        rows = database.execute_query(_RUNNING_SQL, (STALE_RUNNING_MINUTES,),
                                      fetch=True)
    except Exception as e:
        # Refusing because the guard cannot be read would make the feature
        # unusable whenever the database hiccups; reporting it and letting
        # the caller's own refusals stand is the lesser risk, and reg.run
        # re-checks the flag, the limit and the table itself.
        logger.warning("could not read running applies: %s", e)
        return []
    return rows or []


def start_apply(limit, event_ids=None, runner=None) -> int:
    """Open a run_log row, start the apply in a thread, return the row id.

    Raises before starting anything if the run would be refused: the flag,
    the limit and the registration_map table are all checked HERE as well as
    inside reg.run, because a refusal raised inside the thread is a refusal
    nobody sees.
    """
    if not reg.registrations_sync_allowed():
        raise reg.RegistrationsSyncDisabled(
            "REGISTRATIONS_SYNC_ENABLED is off, so nothing was started and "
            "nothing was written. Say \"sync registrations dry run\" to "
            "preview it.")
    if limit is None:
        raise reg.LimitRequired(
            "a live registrations run needs a limit, and there is no phrase "
            'for "all of them". Say "sync registrations apply limit 1" and '
            "raise it deliberately.")
    if not reg.migration_applied():
        raise reg.RegistrationWriteStopped(
            [], "hubsync.registration_map does not exist, so a successful "
                "write could not be recorded and the next run would send it "
                "again. Run migrations/005_registration_map.sql first. "
                "Nothing was started.")

    with _START_LOCK:
        busy = running_applies()
        if busy:
            raise ApplyAlreadyRunning(
                f"registrations apply run_log {busy[0]['id']} is still "
                f"running (started "
                f"{busy[0]['started_at']:%H:%M:%S} UTC, "
                f"{int(busy[0]['age_seconds'])}s ago). One at a time: the "
                f"attendance endpoint has no idempotency key, so two "
                f"applies over the same event is how one registration "
                f"becomes two. Say \"registrations status\" to see it.")

        run_id = reg.open_run(applied=True)
        if run_id is None:
            raise reg.RegistrationWriteStopped(
                [], "a run_log row could not be opened, so the run would "
                    "have been unreportable. Nothing was started.")

        # Re-check AFTER claiming the row. If another request claimed one
        # first, its id is lower, and this one stands down rather than
        # running alongside it.
        others = [r for r in running_applies() if r["id"] < run_id]
        if others:
            reg.close_run(run_id, "failed", {},
                          error_summary=f"stood down: run_log "
                                        f"{others[0]['id']} was already "
                                        f"running")
            raise ApplyAlreadyRunning(
                f"registrations apply run_log {others[0]['id']} claimed the "
                f"slot first, so this one stood down and wrote nothing. Say "
                f"\"registrations status\" to see it.")

    target = runner or _run_apply
    thread = threading.Thread(
        target=target, args=(run_id, limit, event_ids),
        name=f"registrations-apply-{run_id}", daemon=True)
    thread.start()
    logger.info("registrations apply started in the background as run_log %s "
                "(limit=%s, events=%s)", run_id, limit,
                event_ids or "all mapped")
    return run_id


def _run_apply(run_id, limit, event_ids):
    """Thread body. Nothing escapes: a thread that raises is a run_log row
    stuck on 'running' forever, which also blocks the next apply."""
    scope = {"event_ids": event_ids} if event_ids else {}
    try:
        reg.run(dry_run=False, limit=limit, run_id=run_id, **scope)
    except reg.RegistrationWriteStopped as stop:
        # reg.run closes the row itself in its own finally; this is only for
        # a refusal raised before that point.
        logger.warning("registrations apply run_log %s stopped: %s",
                       run_id, stop)
        _close_if_open(run_id, f"stopped: {stop}")
    except Exception as e:
        logger.error("registrations apply run_log %s died: %s", run_id, e,
                     exc_info=True)
        _close_if_open(run_id, f"{type(e).__name__}: {e}")


def _close_if_open(run_id, error_summary):
    try:
        row = _row(run_id)
    except Exception:
        row = None
    if row and row.get("status") == "running":
        reg.close_run(run_id, "failed", {}, error_summary=error_summary[:500])


def _row(run_id):
    rows = database.execute_query(_RUN_SQL, (int(run_id),), fetch=True)
    if not rows:
        raise RunNotFound(f"there is no registrations run with id {run_id}.")
    return rows[0]


def status(run_id=None) -> dict:
    """What a run is doing, or what it did.

    With no id, the newest APPLY — a status request after a lost response is
    asking about the write, not about the last preview.
    """
    if run_id is None:
        rows = database.execute_query(_LATEST_SQL, (), fetch=True)
        if not rows:
            return {"found": False}
        row = rows[0]
    else:
        row = _row(run_id)

    stored = row.get("outcomes")
    summary, records = None, None
    if isinstance(stored, dict):
        summary = stored.get("summary")
        records = stored.get("records")
    elif isinstance(stored, list):
        # Rows written before this hotfix carried the per-record list only.
        records = stored

    out = {"found": True, "run_id": row["id"], "applied": row["applied"],
           "status": row["status"], "started_at": row["started_at"],
           "finished_at": row["finished_at"], "seconds": row["seconds"],
           "error_summary": row["error_summary"], "summary": summary,
           "record_count": len(records or []), "writes_so_far": None}

    if row["status"] == "running":
        out["writes_so_far"] = _writes_since(row["started_at"])
    return out


def _writes_since(started_at):
    try:
        rows = database.execute_query(_WRITES_SINCE_SQL, (started_at,),
                                      fetch=True)
    except Exception as e:
        logger.warning("could not count writes in flight: %s", e)
        return None
    return (rows or [{}])[0].get("n")
