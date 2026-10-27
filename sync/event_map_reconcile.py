"""
Reconcile event_map rows that say pending for events HubSpot has (V5.96)
=======================================================================
"reconcile event map" in chat, or scripts/event_map_reconcile.py. On
2026-10-27 csuite-1155, 1157 and 1159 were in HubSpot while their event_map
rows still said 'pending' with no HubSpot id, and no write_audit row of
Jidhr's ever touched them.

READ, THEN RECONCILE. MAP-ONLY.
For each event_map row that is 'pending' with no hubspot_event_id:

  1. Read: is the event in HubSpot's listing under this portal's
     externalAccountId (jidhr-amcf)? Not there: left exactly as it is.
  2. Compare HubSpot's eventName and startDateTime with what Jidhr would
     send from CSuite today.
  3. Preview (default) says what it would do. Apply:
       match   -> status 'synced', the HubSpot id, the current content hash
                  (the nightly then sees it as unchanged)
       differ  -> status 'synced', the HubSpot id, content hash EMPTY, so
                  the nightly sees a difference and plans an update — which
                  its review rules hold for a person if the event is archived
                  (all three are). Nothing is overwritten here.

Never writes to HubSpot or CSuite. Each change is one guarded UPDATE that
only applies while the row is still 'pending' with no HubSpot id, so a
nightly run in between cannot be overwritten. One hubsync.run_log row,
job 'event_map_reconcile'.
"""

import json
import logging
from datetime import datetime

from clients import database

logger = logging.getLogger(__name__)

JOB = "event_map_reconcile"

_RUN_OPEN_SQL = """
    INSERT INTO hubsync.run_log (job, applied, status)
    VALUES ('event_map_reconcile', %s, 'running')
    RETURNING id
"""

_RUN_CLOSE_SQL = """
    UPDATE hubsync.run_log
       SET finished_at = NOW(), status = %s, error_summary = %s,
           csuite_calls = %s, hubspot_calls = %s, event_dates_read = %s,
           updated_count = %s, review_count = %s, outcomes = %s::jsonb
     WHERE id = %s
"""

_RECONCILE_SQL = """
    UPDATE hubsync.event_map
       SET hubspot_event_id = %s, status = 'synced', content_hash = %s,
           last_synced_at = NOW(), last_error = NULL, updated_at = NOW()
     WHERE csuite_eventdate_id = %s
       AND status = 'pending'
       AND hubspot_event_id IS NULL
    RETURNING csuite_eventdate_id
"""


class Refused(RuntimeError):
    """The reconcile cannot run at all. Nothing was written."""


def _instant(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def differences(mapped, record) -> list:
    """Which of name and start differ between CSuite's mapping and HubSpot."""
    payload = mapped.payload or {}
    found = []
    if str(payload.get("eventName") or "").strip() != \
            str(record.get("eventName") or "").strip():
        found.append("name")
    ours, theirs = (_instant(payload.get("startDateTime")),
                    _instant(record.get("startDateTime")))
    if ours is None or theirs is None or ours != theirs:
        found.append("start time")
    return found


def plan(existing_map, rows, index) -> dict:
    """Sort every pending, unlinked event_map row. No calls."""
    from sync import event_hubspot as eh
    from sync.event_apply import DEFAULT_ORGANIZER

    by_id = {str(r.get("event_date_id")): r for r in rows or []}
    out = {"match": [], "differ": [], "not_in_hubspot": [],
           "not_in_csuite": []}
    for event_id, known in sorted((existing_map or {}).items(),
                                  key=lambda kv: (len(kv[0]), kv[0])):
        if str(known.get("status")) != "pending" or \
                known.get("hubspot_event_id"):
            continue
        record = index.get(eh.external_id(event_id))
        entry = {"id": event_id, "external": eh.external_id(event_id)}
        if not record:
            out["not_in_hubspot"].append(entry)
            continue
        entry["hubspot_id"] = str(record.get("objectId") or "")
        entry["name"] = record.get("eventName") or ""
        row = by_id.get(event_id)
        if row is None:
            out["not_in_csuite"].append(entry)
            continue
        mapped = eh.map_event_date(row, DEFAULT_ORGANIZER)
        diff = differences(mapped, record)
        if not entry["hubspot_id"]:
            out["not_in_csuite"].append(dict(entry, why="no HubSpot id"))
        elif diff:
            out["differ"].append(dict(entry, differs=diff))
        else:
            out["match"].append(dict(entry, content_hash=mapped.content_hash))
    return out


def _open_run(applied):
    try:
        found = database.execute_query(_RUN_OPEN_SQL, (applied,), fetch=True)
    except Exception as e:
        logger.error("could not open the reconcile run_log row: %s",
                     type(e).__name__)
        return None
    row = (found or [{}])[0]
    return row.get("id") if isinstance(row, dict) else None


def _close_run(run_id, out):
    if run_id is None:
        return
    try:
        database.execute_query(_RUN_CLOSE_SQL, (
            "failed" if out.get("error") or out.get("stopped") else "complete",
            out.get("error") or out.get("stopped"),
            out.get("csuite_calls", 0), out.get("hubspot_calls", 0),
            out.get("event_dates_read", 0), out.get("written", 0),
            len(out.get("differ") or []),
            json.dumps(out.get("outcomes") or [], default=str), run_id),
            fetch=False)
    except Exception as e:
        logger.error("could not close reconcile run_log %s: %s", run_id,
                     type(e).__name__)


def run(apply=False, hubspot=None, csuite=None) -> dict:
    """Preview, or (apply=True) reconcile. Raises Refused when it cannot."""
    from clients.csuite import CSuiteClient
    from clients.hubspot import HubSpotClient
    from sync import event_apply as ea
    from sync import event_hubspot as eh

    if not ea.migration_applied():
        raise Refused("hubsync.event_map is not reachable, so there is "
                      "nothing to reconcile. Nothing was read or written.")
    out = {"apply": apply, "error": None, "stopped": None, "written": 0,
           "outcomes": [], "csuite_calls": 0, "hubspot_calls": 0,
           "event_dates_read": 0}
    run_id = _open_run(apply) if apply else None
    out["run_id"] = run_id
    try:
        existing = ea.load_map()
        csuite = csuite or CSuiteClient()
        hubspot = hubspot or HubSpotClient()
        fetched = eh.fetch_event_dates(csuite)
        out["csuite_calls"] = fetched.calls
        out["event_dates_read"] = len(fetched.rows)
        if fetched.error or not fetched.complete:
            raise Refused("the CSuite event-date read did not complete. "
                          "Nothing was written.")
        index, calls, error = eh.hubspot_index(hubspot)
        out["hubspot_calls"] = calls
        if error:
            raise Refused(f"HubSpot's marketing events could not be listed "
                          f"({error}). Nothing was written.")
        out.update(plan(existing, fetched.rows, index))
        if apply:
            _apply(out)
        return out
    except Refused as refused:
        out["error"] = str(refused)
        raise
    finally:
        _close_run(run_id, out)


def _apply(out):
    for entry in out["match"] + out["differ"]:
        content = entry.get("content_hash")          # None for a difference
        try:
            done = database.execute_query(_RECONCILE_SQL, (
                entry["hubspot_id"], content, entry["id"]), fetch=True)
        except Exception as e:
            out["stopped"] = (f"the update for csuite-{entry['id']} failed "
                              f"({type(e).__name__}); stopped, nothing retried")
            break
        if done:
            out["written"] += 1
            entry["written"] = True
            out["outcomes"].append({"id": entry["id"],
                                    "hubspot_id": entry["hubspot_id"],
                                    "hash": bool(content)})
        else:
            entry["written"] = False      # no longer pending: left alone


def report(out) -> str:
    applied = out.get("apply")
    lines = ["✅ **Event map reconcile — APPLIED**" if applied else
             "🧪 **Event map reconcile — PREVIEW** (nothing was written)", "",
             "event_map rows that say 'pending' with no HubSpot id, checked "
             "against HubSpot's listing. Map-only: nothing is written to "
             "HubSpot or CSuite."]
    verb = "Marked synced" if applied else "Would mark synced"
    for title, key, tail in (
            (f"{verb} — HubSpot matches CSuite", "match",
             "fingerprint set; the nightly sees it as unchanged"),
            (f"{verb} — HubSpot differs from CSuite", "differ",
             "fingerprint left empty, so the nightly plans an update and its "
             "review rules decide")):
        rows = out.get(key) or []
        if rows:
            lines += ["", f"**{title} ({len(rows)})**"]
            for e in rows:
                diff = (f" — differs in {', '.join(e['differs'])}"
                        if e.get("differs") else "")
                left = (" (no longer pending — left alone)"
                        if applied and e.get("written") is False else "")
                lines.append(f"• {e['external']} → HubSpot {e['hubspot_id']}"
                             f" — {e['name']}{diff}{left}")
            lines.append(f"  ({tail})")
    for title, key in (("Not in HubSpot — left as pending",
                        "not_in_hubspot"),
                       ("In HubSpot but not in CSuite's list — needs a person",
                        "not_in_csuite")):
        rows = out.get(key) or []
        if rows:
            lines += ["", f"**{title} ({len(rows)})**"]
            lines += [f"• {e['external']}" for e in rows]
    if not any(out.get(k) for k in ("match", "differ", "not_in_hubspot",
                                    "not_in_csuite")):
        lines += ["", "No pending, unlinked rows. Nothing to do."]
    if out.get("stopped"):
        lines += ["", f"🛑 {out['stopped']}"]
    if applied and out.get("run_id"):
        lines.append(f"\nrun_log {out['run_id']}: {out['written']} row(s) "
                     "updated.")
    lines.append(f"\n📞 {out['csuite_calls']} CSuite call(s), "
                 f"{out['hubspot_calls']} HubSpot call(s).")
    if not applied and (out.get("match") or out.get("differ")):
        lines.append('Say *"reconcile event map apply"* to write it.')
    return "\n".join(lines)
