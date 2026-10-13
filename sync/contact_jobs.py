"""Contact creation runs in the background, like the registrations apply.

The same reasoning as sync/registration_jobs.py: Railway's proxy discards a
response after 5 minutes with no data transferred, and a create run is one
HubSpot read plus one write per person, so a 17-record run is minutes of
work. The request must not own it.

This is a sibling of registration_jobs rather than a generalisation of it.
registration_jobs is explicitly protected from change by the briefs that
built it, and sharing a guard between two jobs would mean one apply could
block the other — which is wrong here, because creating contacts and
registering them are independent writes to different objects.

The one-at-a-time guard is per JOB, and it lives in run_log rather than in
a process variable: there are eight gunicorn workers, and a flag in one
tells the other seven nothing.
"""

import logging
import threading

from clients import database
from sync import contact_create as cc

logger = logging.getLogger(__name__)

# A run with no write_audit contact create for this long is abandoned, not
# running — the same activity-based rule hotfix-52 put on registrations,
# for the same reason: a long run that is still writing must never be
# called abandoned.
IDLE_ABANDONED_SECONDS = 300

_RUNNING_SQL = """
    SELECT r.id,
           r.started_at,
           EXTRACT(EPOCH FROM (NOW() - r.started_at)) AS age_seconds,
           EXTRACT(EPOCH FROM (NOW() - GREATEST(
               r.started_at,
               COALESCE(w.last_write, r.started_at)))) AS idle_seconds
      FROM hubsync.run_log r
      LEFT JOIN LATERAL (
           SELECT MAX(created_at) AS last_write
             FROM write_audit
            WHERE target_system = 'hubspot'
              AND http_method = 'POST'
              AND endpoint = 'crm/v3/objects/contacts'
              AND created_at >= r.started_at
      ) w ON TRUE
     WHERE r.job = %s
       AND r.applied IS TRUE
       AND r.status = 'running'
       AND EXTRACT(EPOCH FROM (NOW() - GREATEST(
               r.started_at,
               COALESCE(w.last_write, r.started_at)))) < %s
     ORDER BY r.id
"""

_RUN_SQL = """
    SELECT id, applied, status, started_at, finished_at, error_summary,
           outcomes,
           EXTRACT(EPOCH FROM (COALESCE(finished_at, NOW()) - started_at))
               AS seconds
      FROM hubsync.run_log
     WHERE job = %s AND id = %s
"""

_LATEST_SQL = """
    SELECT id, applied, status, started_at, finished_at, error_summary,
           outcomes,
           EXTRACT(EPOCH FROM (COALESCE(finished_at, NOW()) - started_at))
               AS seconds
      FROM hubsync.run_log
     WHERE job = %s AND applied IS TRUE
     ORDER BY id DESC
     LIMIT 1
"""

_WRITES_SINCE_SQL = """
    SELECT COUNT(*) AS n
      FROM write_audit
     WHERE target_system = 'hubspot'
       AND http_method = 'POST'
       AND endpoint = 'crm/v3/objects/contacts'
       AND created_at >= %s
"""

_START_LOCK = threading.Lock()


class ApplyAlreadyRunning(RuntimeError):
    """A contact-create run is already in flight."""


class RunNotFound(RuntimeError):
    """No contact-create run with that id."""


def running_applies() -> list:
    try:
        rows = database.execute_query(
            _RUNNING_SQL, (cc.JOB, IDLE_ABANDONED_SECONDS), fetch=True)
    except Exception as e:
        logger.warning("could not read running contact-create runs: %s", e)
        return []
    return rows or []


def start_apply(limit, event_ids=None, runner=None) -> int:
    """Open a run_log row, start the run in a thread, return the row id.

    Every refusal is raised HERE, before the thread exists: a refusal
    raised inside a thread is a refusal nobody sees.
    """
    from sync import registrations as reg

    if not reg.registrations_sync_allowed():
        raise reg.RegistrationsSyncDisabled(
            "REGISTRATIONS_SYNC_ENABLED is off, so nothing was started and "
            "no contact was created. Say \"create contacts dry run\" to "
            "preview it.")
    if limit is None:
        raise cc.LimitRequired(
            "a live contact-create run needs a limit, and there is no phrase "
            'for "all of them". Say "create contacts apply limit 1" and '
            "raise it deliberately.")

    with _START_LOCK:
        busy = running_applies()
        if busy:
            raise ApplyAlreadyRunning(
                f"contact-create run_log {busy[0]['id']} is still running "
                f"(started {busy[0]['started_at']:%H:%M:%S} UTC, "
                f"{int(busy[0]['age_seconds'])}s ago; last create "
                f"{int(busy[0].get('idle_seconds') or 0)}s ago). One at a "
                f"time: contact creation has no idempotency key either, so "
                f"two runs over the same event is how one person becomes "
                f"two contacts. Say \"contacts status\" to see it.")

        run_id = cc.open_run(applied=True)
        if run_id is None:
            raise cc.ContactCreateStopped(
                "a run_log row could not be opened, so the run would have "
                "been unreportable. Nothing was started.")

        others = [r for r in running_applies() if r["id"] < run_id]
        if others:
            cc.close_run(run_id, "failed", {},
                         error_summary=f"stood down: run_log "
                                       f"{others[0]['id']} was already "
                                       f"running")
            raise ApplyAlreadyRunning(
                f"contact-create run_log {others[0]['id']} claimed the slot "
                f"first, so this one stood down and created nothing.")

    target = runner or _run_apply
    thread = threading.Thread(target=target,
                              args=(run_id, limit, event_ids),
                              name=f"contact-create-{run_id}", daemon=True)
    thread.start()
    logger.info("contact create started in the background as run_log %s "
                "(limit=%s, events=%s)", run_id, limit,
                event_ids or "all mapped")
    return run_id


def _run_apply(run_id, limit, event_ids):
    """Thread body. Nothing escapes: a thread that raises leaves a row on
    'running' forever, which also blocks the next run."""
    scope = {"event_ids": event_ids} if event_ids else {}
    try:
        cc.run(dry_run=False, limit=limit, run_id=run_id, **scope)
    except Exception as e:
        logger.error("contact create run_log %s died: %s", run_id, e,
                     exc_info=True)
        detail = f"{type(e).__name__}: {e}"
        _close_if_open(run_id, detail)
        _notify_died(run_id, detail)


def _notify_died(run_id, detail):
    """A thread that died is the case nobody can see: there is no reply to
    read, and the run_log row is the only trace."""
    from clients.notify import safe_notify

    safe_notify(
        "Jidhr: contact creation DIED",
        f"The background thread raised and the run was closed as failed: "
        f"{detail}",
        path="create contacts apply",
        run_log=run_id,
    )


def _close_if_open(run_id, error_summary):
    try:
        row = _row(run_id)
    except Exception:
        row = None
    if row and row.get("status") == "running":
        cc.close_run(run_id, "failed", {}, error_summary=error_summary)


def _row(run_id):
    rows = database.execute_query(_RUN_SQL, (cc.JOB, int(run_id)), fetch=True)
    if not rows:
        raise RunNotFound(f"there is no contact-create run with id {run_id}.")
    return rows[0]


def status(run_id=None) -> dict:
    if run_id is None:
        rows = database.execute_query(_LATEST_SQL, (cc.JOB,), fetch=True)
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
        records = stored

    out = {"found": True, "run_id": row["id"], "status": row["status"],
           "started_at": row["started_at"], "seconds": row["seconds"],
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
        logger.warning("could not count creates in flight: %s", e)
        return None
    return (rows or [{}])[0].get("n")
