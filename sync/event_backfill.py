"""
One-off event backfill: past CSuite event dates into HubSpot (V5.92)
====================================================================
"backfill events since 2026-01-01" in chat, or scripts/event_backfill.py.
For a report (Lisa, 2026-10-27): every 2026 CSuite event as a HubSpot
marketing event. Events only — no registrations.

CREATE-ONLY
-----------
It creates marketing events that HubSpot does not have, and nothing else:
it never updates an event, never adopts one HubSpot already has, never
deletes. The write is a PUT to /events/{externalEventId}, which is an
UPSERT — so create-only is enforced twice: the event must be absent from
HubSpot's listing when the plan is made, AND a GET of that one event must
answer 404 immediately before the PUT. Anything else, and it is skipped.

SCOPE
-----
CSuite event dates from `since` up to, but not including, the nightly
window (today minus SCHEDULED_SYNC_WINDOW_DAYS). The nightly job owns the
window; this owns what is before it, so the two can never both create the
same event. Sign-up lists (contacts only) are never events here either.

THE TWO FLAGS
-------------
The ordinary rules hold every one of these back for review. Two explicit
flags release exactly one kind of review reason each; without them the
backfill creates what the ordinary rules would create, which on
2026-10-27 was none of the 23:

  include_archived   "archived in CSuite" — all 23 carry it
  assume_eastern     "start_time '...' names no timezone — assumed
                     America/New_York" — 6 of the 23

Any other reason — a start time with no usable clock placed at midnight,
no event date — is never released. An event is released only when EVERY
reason it carries is covered by a flag, and the preview names the flags.

CANCELLED
---------
Checked 2026-10-27: CSuite has no cancelled status. event/list/dates and
event/display/eventdate carry only `archived` (all 23 are 1), and no name
or description says cancelled, postponed or rescheduled. A cancelled event
is still excluded, even with include_archived, if CSuite ever marks one:
a cancelled/canceled status field, or "cancel" in the event's name. The
preview lists those separately.

EXCLUDE (V5.93)
---------------
--exclude csuite-1161,csuite-1264 (chat: "exclude csuite-1161, csuite-1264")
leaves those event dates out whatever the flags say. The preview lists them
separately, and names any excluded id that is not in the range at all, so a
typo shows instead of silently excluding nothing.

AFTERWARDS
----------
Each created event gets its event_map row at once, with the mapped content
hash, exactly as the event sync records its own creates. The nightly step
plans over the WHOLE calendar for updates, not only its window, so it is
NOT the window that keeps it off these events — it is that each one is in
HubSpot (never re-created), its hash is recorded (unchanged = no write),
and every one is archived, which the event sync holds back from any update
as needing a person.

Audit rows carry event_backfill_run_id (this run's hubsync.run_log id, job
'event_backfill'), so they can be told apart from the event sync's.
"""

import json
import logging
import re
from datetime import date

from clients import database

logger = logging.getLogger(__name__)

JOB = "event_backfill"
DEFAULT_LIMIT = 23

FLAG_ARCHIVED = "include_archived"
FLAG_EASTERN = "assume_eastern"
FLAG_LABELS = {FLAG_ARCHIVED: "--include-archived",
               FLAG_EASTERN: "--assume-eastern"}

_ARCHIVED_CLAUSE = "archived in CSuite — left untouched in HubSpot for review"
_EASTERN_CLAUSE = re.compile(
    r"^start_time .* names no timezone — assumed America/New_York$")

_RUN_OPEN_SQL = """
    INSERT INTO hubsync.run_log (job, applied, status)
    VALUES ('event_backfill', %s, 'running')
    RETURNING id
"""

_RUN_CLOSE_SQL = """
    UPDATE hubsync.run_log
       SET finished_at = NOW(), status = %s, error_summary = %s,
           csuite_calls = %s, hubspot_calls = %s, event_dates_read = %s,
           created_count = %s, skipped_count = %s, review_count = %s,
           failed_count = %s, outcomes = %s::jsonb
     WHERE id = %s
"""


class Refused(RuntimeError):
    """The backfill cannot run at all. Nothing was written."""


def parse_since(text) -> date:
    try:
        return date.fromisoformat(str(text or "").strip())
    except ValueError:
        raise Refused(f"{text!r} is not a date (YYYY-MM-DD).")


def parse_exclude(text) -> tuple:
    """'csuite-1161, 1264' -> ('1161', '1264'). Refuses anything else."""
    if isinstance(text, (list, tuple, set, frozenset)):
        text = ",".join(str(t) for t in text)
    ids = []
    for part in re.split(r"[\s,]+", str(text or "").strip()):
        if not part:
            continue
        match = re.fullmatch(r"(?:csuite-)?(\d+)", part.lower())
        if not match:
            raise Refused(f"{part!r} is not an event id to exclude (use "
                          "csuite-1161 or 1161).")
        if match.group(1) not in ids:
            ids.append(match.group(1))
    return tuple(ids)


def clause_flag(clause: str):
    """The flag that releases one review clause, or None."""
    clause = clause.strip()
    if clause == _ARCHIVED_CLAUSE:
        return FLAG_ARCHIVED
    if _EASTERN_CLAUSE.match(clause):
        return FLAG_EASTERN
    return None


def release_flags(review_reason) -> tuple:
    """(flags needed, clauses no flag releases) for a review reason."""
    needed, blocking = [], []
    for clause in [c for c in str(review_reason or "").split("; ")
                   if c.strip()]:
        flag = clause_flag(clause)
        if flag:
            if flag not in needed:
                needed.append(flag)
        else:
            blocking.append(clause.strip())
    return needed, blocking


def cancelled_reason(row) -> str | None:
    """Why CSuite says this event was cancelled, or None."""
    row = row or {}
    for key in ("cancelled", "canceled", "is_cancelled", "is_canceled"):
        if row.get(key) in (1, "1", True, "true"):
            return f"CSuite marks it cancelled ({key})"
    for key in ("event_status", "status"):
        if "cancel" in str(row.get(key) or "").lower():
            return f"CSuite status is {row.get(key)!r}"
    if "cancel" in str(row.get("event_name") or "").lower():
        return "its CSuite name says cancelled"
    return None


def plan(rows, existing_map, index, since, window_start,
         include_archived=False, assume_eastern=False, exclude=()) -> dict:
    """Every event date in [since, window_start), sorted out. No calls."""
    from sync import event_hubspot as eh
    from sync.event_apply import DEFAULT_ORGANIZER, contacts_only_reason

    allowed = {FLAG_ARCHIVED: include_archived, FLAG_EASTERN: assume_eastern}
    excluded_ids = set(parse_exclude(exclude))
    out = {"release": [], "held": [], "cancelled": [], "in_hubspot": [],
           "needs_person": [], "skipped": [], "excluded": [],
           "exclude_not_found": [], "nightly": 0, "out_of_range": 0}
    for row in rows or []:
        day_text = str((row or {}).get("event_date") or "")[:10]
        try:
            day = date.fromisoformat(day_text)
        except ValueError:
            out["out_of_range"] += 1
            continue
        if day < since:
            out["out_of_range"] += 1
            continue
        if day >= window_start:
            out["nightly"] += 1           # the nightly window's, not ours
            continue
        mapped = eh.map_event_date(row, DEFAULT_ORGANIZER)
        entry = {"id": mapped.csuite_eventdate_id, "name": mapped.source_name,
                 "date": day_text, "mapped": mapped}
        if str(mapped.csuite_eventdate_id) in excluded_ids:
            out["excluded"].append(dict(entry, why="excluded by --exclude"))
            continue
        if not mapped.csuite_eventdate_id or not mapped.syncable:
            out["skipped"].append(dict(entry, why=mapped.review_reason
                                       or "not syncable"))
            continue
        lists = contacts_only_reason(row)
        if lists:
            out["skipped"].append(dict(entry, why=lists))
            continue
        cancelled = cancelled_reason(row)
        if cancelled:
            out["cancelled"].append(dict(entry, why=cancelled))
            continue
        if index.get(mapped.external_event_id):
            out["in_hubspot"].append(entry)       # untouched: create-only
            continue
        known = (existing_map or {}).get(mapped.csuite_eventdate_id) or {}
        if str(known.get("status")) == "unknown":
            out["needs_person"].append(dict(
                entry, why="an earlier create's outcome is unknown — resolve "
                           "it with \"sync events\" first"))
            continue
        if known.get("hubspot_event_id"):
            out["needs_person"].append(dict(
                entry, why="the event map has a HubSpot id that HubSpot does "
                           "not list"))
            continue
        needed, blocking = release_flags(mapped.review_reason)
        missing = [f for f in needed if not allowed[f]]
        if blocking or missing:
            out["held"].append(dict(
                entry, why="; ".join(blocking + [
                    f"needs {FLAG_LABELS[f]}" for f in missing]),
                needs=missing, blocking=blocking))
            continue
        out["release"].append(dict(entry, flags=needed))
    out["release"].sort(key=lambda e: (e["date"], e["id"]))
    seen = {str(e["id"]) for e in out["excluded"]}
    out["exclude_not_found"] = [f"csuite-{i}" for i in
                                parse_exclude(exclude) if i not in seen]
    return out


def _open_run(applied: bool):
    try:
        found = database.execute_query(_RUN_OPEN_SQL, (applied,), fetch=True)
    except Exception as e:
        logger.error("could not open the backfill run_log row: %s", e)
        return None
    row = (found or [{}])[0]
    return row.get("id") if isinstance(row, dict) else None


def _close_run(run_id, out):
    if run_id is None:
        return
    status = "failed" if (out.get("error") or out.get("stopped")) \
        else "complete"
    try:
        database.execute_query(_RUN_CLOSE_SQL, (
            status, (out.get("error") or out.get("stopped") or None),
            out.get("csuite_calls", 0), out.get("hubspot_calls", 0),
            out.get("event_dates_read", 0), out.get("created", 0),
            out.get("skipped_existing", 0), len(out.get("held") or []),
            out.get("failed", 0),
            json.dumps(out.get("outcomes") or [], default=str), run_id),
            fetch=False)
    except Exception as e:
        logger.error("could not close backfill run_log %s: %s", run_id, e)


def _absent_in_hubspot(hubspot, external_event_id):
    """True when HubSpot answers 404 for this event; False when it has it;
    None when it could not be told."""
    from clients.hubspot import marketing_event_url
    from sync.event_hubspot import EXTERNAL_ACCOUNT_ID

    # externalAccountId goes in the query string on a read (see
    # sync/event_hubspot: with it, csuite-1464 is FOUND; with any other
    # account id, 404). A GET is not a write, so it is not audited.
    _result, status = hubspot._send_with_status(
        "GET", f"{marketing_event_url(external_event_id)}"
               f"?externalAccountId={EXTERNAL_ACCOUNT_ID}")
    if status == 404:
        return True
    if status is not None and 200 <= int(status) < 300:
        return False
    return None


def run(since, include_archived=False, assume_eastern=False, apply=False,
        limit=DEFAULT_LIMIT, hubspot=None, csuite=None, today=None,
        exclude=()) -> dict:
    """Preview, or (apply=True) create. Raises Refused when it cannot run."""
    from clients.csuite import CSuiteClient
    from clients.hubspot import HubSpotClient
    from sync import event_apply as ea
    from sync import event_hubspot as eh
    from sync.scheduled import window_floor

    since = since if isinstance(since, date) else parse_since(since)
    if limit is None or int(limit) < 0:
        raise Refused("a backfill needs a limit (0 or more).")
    limit = int(limit)
    exclude = parse_exclude(exclude)          # a typo refuses before any read
    window_start = window_floor(today)
    have_tables = ea.migration_applied()
    if apply and not have_tables:
        raise Refused("hubsync.event_map is not reachable, so a create would "
                      "have no duplicate guard. Nothing was written.")

    out = {"since": since.isoformat(), "window_start": window_start.isoformat(),
           "apply": apply, "limit": limit,
           "flags": [FLAG_LABELS[f] for f, on in
                     ((FLAG_ARCHIVED, include_archived),
                      (FLAG_EASTERN, assume_eastern)) if on],
           "exclude": [f"csuite-{i}" for i in exclude],
           "map_read": have_tables, "error": None, "stopped": None,
           "outcomes": [], "created": 0, "failed": 0, "deferred": 0,
           "skipped_existing": 0, "csuite_calls": 0, "hubspot_calls": 0,
           "event_dates_read": 0, "run_id": None}
    run_id = _open_run(apply) if have_tables else None
    out["run_id"] = run_id
    try:
        csuite = csuite or CSuiteClient()
        hubspot = hubspot or HubSpotClient()
        fetched = eh.fetch_event_dates(csuite)
        out["csuite_calls"] = fetched.calls
        out["event_dates_read"] = len(fetched.rows)
        if fetched.error or not fetched.complete:
            raise Refused(f"the CSuite event-date read did not complete "
                          f"({fetched.error or 'partial'}). Nothing was "
                          "planned.")
        ceiling = ea._date_read_ceiling()
        if ceiling and len(fetched.rows) >= ceiling:
            raise Refused("CSuite returned the maximum number of event "
                          "dates, so the read may be cut off. Nothing was "
                          "planned.")
        index, hs_calls, hs_error = eh.hubspot_index(hubspot)
        out["hubspot_calls"] = hs_calls
        if hs_error:
            raise Refused(f"HubSpot's marketing events could not be listed "
                          f"({hs_error}). Nothing was planned.")
        result = plan(fetched.rows, ea.load_map() if have_tables else {},
                      index, since, window_start, include_archived,
                      assume_eastern, exclude)
        out.update({k: result[k] for k in (
            "release", "held", "cancelled", "in_hubspot", "needs_person",
            "skipped", "excluded", "exclude_not_found", "nightly",
            "out_of_range")})
        if not apply:
            return out
        _apply(out, hubspot, run_id)
        return out
    except Refused as refused:
        out["error"] = str(refused)
        raise
    finally:
        _close_run(run_id, out)


def _apply(out, hubspot, run_id):
    from sync.event_apply import _save_map, _write_event

    tag = {"event_backfill_run_id": run_id} if run_id else None
    for entry in out["release"]:
        mapped = entry["mapped"]
        if out["created"] >= out["limit"]:
            out["deferred"] += 1
            out["outcomes"].append({"id": entry["id"], "outcome": "deferred"})
            continue
        absent = _absent_in_hubspot(hubspot, mapped.external_event_id)
        out["hubspot_calls"] += 1
        if absent is False:
            out["skipped_existing"] += 1
            out["outcomes"].append({"id": entry["id"],
                                    "outcome": "already in HubSpot — "
                                               "untouched"})
            continue
        if absent is None:
            out["stopped"] = (f"HubSpot could not confirm that "
                              f"{mapped.external_event_id} is absent, so "
                              "nothing further was created.")
            out["outcomes"].append({"id": entry["id"],
                                    "outcome": "not attempted"})
            return
        event_id, error, ambiguous = _write_event(hubspot, mapped,
                                                  audit_meta=tag)
        out["hubspot_calls"] += 1
        if error:
            _save_map(mapped, None, "error", error)
            out["failed"] += 1
            out["outcomes"].append({"id": entry["id"], "outcome": "failed",
                                    "error": str(error)[:200]})
            out["stopped"] = (f"create of {mapped.external_event_id} failed "
                              f"({error}). Stopped before any further write.")
            return
        if not event_id:
            _save_map(mapped, None, "unknown",
                      ambiguous or "no objectId in response")
            out["outcomes"].append({"id": entry["id"], "outcome": "unknown",
                                    "error": str(ambiguous)[:200]})
            out["stopped"] = (f"create of {mapped.external_event_id} came "
                              f"back with no id ({ambiguous}). NOT retried; "
                              "stopped before any further write.")
            return
        _save_map(mapped, event_id, "synced")
        out["created"] += 1
        entry["hubspot_id"] = str(event_id)
        out["outcomes"].append({"id": entry["id"], "outcome": "created",
                                "hubspot_id": str(event_id),
                                "flags": entry["flags"]})
        logger.info("backfill created HubSpot event %s for CSuite event "
                    "date %s", event_id, entry["id"])


def report(out, show_payload_for=None) -> str:
    """Plain text for chat and the command line."""
    head = ("🧪 **Event backfill — PREVIEW** (nothing was written)"
            if not out.get("apply") else "✅ **Event backfill — APPLIED**")
    lines = [head, "",
             f"CSuite event dates from {out['since']} up to {out['window_start']}"
             f" (the nightly job covers {out['window_start']} onward). "
             f"Flags: {', '.join(out['flags']) or 'none'}."
             + (f" Excluding: {', '.join(out['exclude'])}."
                if out.get("exclude") else ""),
             f"Create-only: events already in HubSpot are never touched. "
             f"Limit {out['limit']}."]
    if not out.get("map_read"):
        lines.append("⚠️ The event map could not be read, so this preview "
                     "used HubSpot's own listing only. Applying needs it.")
    release = out.get("release") or []
    verb = "Created" if out.get("apply") else "Would create"
    lines += ["", f"**{verb} ({out['created'] if out.get('apply') else len(release)})**"]
    for e in release:
        if out.get("apply") and not e.get("hubspot_id"):
            continue
        flags = ", ".join(FLAG_LABELS[f] for f in e["flags"]) or "no flag needed"
        made = f" → HubSpot {e['hubspot_id']}" if e.get("hubspot_id") else ""
        lines.append(f"• {e['date']} — {e['name'] or '(no name)'} "
                     f"(csuite-{e['id']}) — released by {flags}{made}")
    for title, key in (("Excluded — never created", "excluded"),
                       ("Held back", "held"), ("Cancelled — never created",
                                                "cancelled"),
                       ("Needs a person", "needs_person"),
                       ("Skipped", "skipped")):
        rows = out.get(key) or []
        if rows:
            lines += ["", f"**{title} ({len(rows)})**"]
            for e in rows:
                lines.append(f"• {e['date']} — {e['name'] or '(no name)'} "
                             f"(csuite-{e['id']}) — {e['why']}")
    if out.get("exclude_not_found"):
        lines += ["", f"⚠️ Asked to exclude, but not in this range: "
                      f"{', '.join(out['exclude_not_found'])}."]
    existing = out.get("in_hubspot") or []
    lines += ["", f"Already in HubSpot, left untouched: {len(existing)}. "
                  f"In the nightly window (not this backfill's): "
                  f"{out.get('nightly', 0)}."]
    if out.get("apply"):
        lines.append(f"Skipped because they appeared in HubSpot meanwhile: "
                     f"{out.get('skipped_existing', 0)}. Deferred by the "
                     f"limit: {out.get('deferred', 0)}. Failed: "
                     f"{out.get('failed', 0)}.")
        if out.get("run_id"):
            lines.append(f"run_log {out['run_id']}; audit rows carry "
                         f"event_backfill_run_id={out['run_id']}.")
    if out.get("stopped"):
        lines += ["", f"🛑 {out['stopped']}"]
    if show_payload_for and release:
        sample = next((e for e in release if e["id"] == str(show_payload_for)),
                      release[0])
        lines += ["", f"HubSpot payload for csuite-{sample['id']} (PUT "
                      f"marketing/v3/marketing-events/events/"
                      f"{sample['mapped'].external_event_id}):",
                  json.dumps(sample["mapped"].payload, indent=2,
                             ensure_ascii=False)]
    lines += ["", f"📞 {out['csuite_calls']} CSuite call(s), "
                  f"{out['hubspot_calls']} HubSpot call(s)."]
    if not out.get("apply") and release:
        lines.append("Add `apply` to create them.")
    return "\n".join(lines)
