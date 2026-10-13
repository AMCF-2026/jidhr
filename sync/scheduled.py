"""The scheduled sync chain: events, then contacts, then registrations.

SHIPS DISARMED. SCHEDULED_SYNC_ENABLED defaults to False and is set
nowhere but its own default, so a deploy changes nothing.

Why a chain, and why that order
-------------------------------
Measured 2026-10-13. Of 187 CSuite event dates, 8 fall inside today - 1,
and THREE of those eight — 1529, 1530 and 1531 — have no HubSpot marketing
event at all. A registrations run on one of them stops at
`event_object_ids` with "event N has no HubSpot objectId. Nothing further
was written", because attendance is written against a marketing event that
has to exist first.

So:

    1. event sync      creates the marketing event
    2. contact create  gives each registrant a HubSpot contact
    3. registrations   registers the contacts against the event

Each step stops the chain. There is no point registering against an event
that was not created, and no point registering a contact that does not
exist — and a step that failed is a step whose counts the next one would be
reading wrong.

The window
----------
`event_date >= today - SCHEDULED_SYNC_WINDOW_DAYS`. Without it a scheduled
run would keep registering people for events that finished months ago: 81
of the 187 dates are already past, and 98 carry no usable event_date at
all. The day of slack covers an event whose date has just turned over in a
timezone this sync does not model.

Phase 1's hardcoded PHASE_ONE_EVENT_IDS is NOT used here. That list was a
deliberately fixed scope for a human-triggered run; a schedule needs to
pick up an event nobody has added to a tuple, which is the whole point of
running it unattended.

No typed limit
--------------
A human apply takes `limit N` because a person is choosing how much to
risk. There is nobody to type one here, so the ceilings are the guards:
MAX_CREATE_BATCH, first_run_guard, shrink_guard and the event sync's own
withholding. Any guard refusing is a stop and an ALERT, never a smaller
run that proceeds quietly.
"""

import logging
from datetime import date, timedelta

logger = logging.getLogger(__name__)

# What the handler reports having done, per step.
STEP_EVENTS = "event_sync"
STEP_CONTACTS = "contact_create"
STEP_REGISTRATIONS = "registrations_sync"
STEPS = (STEP_EVENTS, STEP_CONTACTS, STEP_REGISTRATIONS)

MODE_PREVIEW = "preview"
MODE_LIVE = "live"

# The most HubSpot marketing events one unattended run may create.
#
# The event step creates marketing events with no human reading a plan
# first. Measured 2026-10-13: the window holds 8 events and 3 of them need
# creating (1529, 1530, 1531), so 3 is today's real workload and the
# default. Anything above it stops the chain, sends one ALERT and creates
# NOTHING — not "the first three and a deferral", which would leave a
# partial calendar that nobody chose.
#
# Never off. 0, a negative, a typo or an empty value all mean the default:
# a ceiling switched off by a typo is worse than no ceiling, because it
# looks present.
DEFAULT_MAX_EVENT_CREATE = 3


def max_event_creates() -> int:
    import os

    raw = (os.environ.get("MAX_EVENT_CREATE_PER_RUN") or "").strip()
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_MAX_EVENT_CREATE
    return value if value > 0 else DEFAULT_MAX_EVENT_CREATE


class ChainStopped(RuntimeError):
    """A step refused, so the steps after it did not run."""

    def __init__(self, step, reason):
        super().__init__(f"{step}: {reason}")
        self.step = step
        self.reason = reason


def scheduled_enabled() -> bool:
    import config

    return bool(getattr(config.Config, "SCHEDULED_SYNC_ENABLED", False))


def scheduled_mode() -> str:
    """'preview' or 'live'. Anything unreadable is preview."""
    import config

    mode = str(getattr(config.Config, "SCHEDULED_SYNC_MODE", MODE_PREVIEW))
    return MODE_LIVE if mode.strip().lower() == MODE_LIVE else MODE_PREVIEW


def window_floor(today=None) -> date:
    import config

    days = getattr(config.Config, "SCHEDULED_SYNC_WINDOW_DAYS", 1)
    try:
        days = int(days)
    except (TypeError, ValueError):
        days = 1
    if days < 0:
        days = 1
    return (today or date.today()) - timedelta(days=days)


def read_calendar(csuite, today=None) -> dict:
    """ONE read of event/list/dates, split three ways.

    {"inside": rows in the window, "undated": rows with no usable
     event_date, "all": every row, "total": n, "error": str | None}

    One fetch rather than one per question, so the window, the undated
    count and the created-event names are all read off the same snapshot
    and cannot disagree with each other.

    Refuses on a partial read rather than syncing a slice of the calendar
    as though it were all of it — the same line sync/mirror draws.
    """
    from clients.csuite_fetch import fetch_all
    from sync import event_hubspot as eh

    out = {"inside": [], "undated": [], "all": [], "total": 0, "error": None}
    try:
        result = fetch_all(csuite, eh.EVENT_DATES_ENDPOINT, {})
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
        return out
    rows = list(result.records or [])
    out["all"] = rows
    out["total"] = len(rows)
    if not result.complete:
        out["error"] = (
            f"the CSuite event-date read came back incomplete "
            f"({len(rows)} row(s), {result.pages} page(s)), so the window "
            f"cannot be trusted and nothing was planned")
        return out

    floor = window_floor(today)
    for row in rows:
        text = str((row or {}).get("event_date") or "")[:10]
        try:
            when = date.fromisoformat(text)
        except ValueError:
            # Measured 2026-10-13: 98 of 187 rows have no event_date, and
            # every one of them is event_name 'Unassigned', archived = 1,
            # ids 1052-1149 — one legacy block that predates every real
            # event. Not an error, and not in the window.
            out["undated"].append(row)
            continue
        if when >= floor:
            out["inside"].append(row)
    return out


def events_in_window(csuite, today=None) -> tuple:
    """(event_rows_inside_the_window, total_read, error). A thin wrapper
    over read_calendar, kept for callers that want only the window."""
    calendar = read_calendar(csuite, today)
    return calendar["inside"], calendar["total"], calendar["error"]


def event_ids_of(rows) -> list:
    out = []
    for row in rows or []:
        found = str((row or {}).get("event_date_id") or "").strip()
        if found:
            out.append(found)
    return out


def registrable_ids(rows) -> list:
    """Window event ids MINUS the sign-up lists and the held ones.

    A Newsletters date gets contacts and is never registered, and a held id
    is held. Both are already enforced inside the two syncs; filtering here
    as well keeps them out of the summary's registration section, so the
    email does not imply something was attempted that never could be.
    """
    from sync import contact_create as cc
    from sync import registrations as reg

    held = set(reg.held_event_ids())
    contacts_only = set(cc.contacts_only_event_ids())
    out = []
    for row in rows or []:
        event_id = str((row or {}).get("event_date_id") or "").strip()
        if not event_id:
            continue
        if event_id in held or event_id in contacts_only:
            continue
        if cc.is_contacts_only_row(row):
            continue
        out.append(event_id)
    return out


# ---------------------------------------------------------------------------
# The chain
# ---------------------------------------------------------------------------

def _no_manual_apply_running():
    """Raise ChainStopped if a human-triggered apply is in flight.

    The reverse direction needs no code: a live scheduled run's own
    reg.run / cc.run open run_log rows with applied = true and status
    'running', which is exactly what registration_jobs.running_applies and
    contact_jobs.running_applies look for — so a manual apply started
    mid-chain is refused by the guard that already exists.
    """
    from sync import contact_jobs, registration_jobs

    for module, label in ((registration_jobs, "registrations apply"),
                          (contact_jobs, "contact-create run")):
        try:
            busy = module.running_applies()
        except Exception as e:
            logger.warning("could not check for a running %s: %s", label, e)
            continue
        if busy:
            raise ChainStopped(
                "lock",
                f"a manual {label} is in flight (run_log {busy[0]['id']}), "
                f"so the scheduled run stood down and did nothing. Two "
                f"writers over the same events is how one registration "
                f"becomes two.")


def run_chain(csuite=None, hubspot=None, today=None, mode=None) -> dict:
    """Run events -> contacts -> registrations once. Never raises.

    Returns the summary the email is built from. `stopped` is set when a
    step refused; the steps after it are absent from `steps`.
    """
    from clients.csuite import CSuiteClient
    from clients.hubspot import HubSpotClient

    mode = mode or scheduled_mode()
    dry_run = mode != MODE_LIVE
    out = {"mode": mode, "dry_run": dry_run, "stopped": None,
           "stopped_step": None, "steps": {}, "run_log_ids": [],
           "window_floor": str(window_floor(today)),
           "events_in_window": 0, "events_read": 0, "event_ids": [],
           "registrable_ids": [], "contacts_only_ids": [], "error": None,
           "event_creates": [], "max_event_creates": max_event_creates(),
           "undated_count": 0, "undated_archived": 0, "undated_ids": [],
           "new_undated": [], "undated_baseline": None}

    csuite = csuite or CSuiteClient()
    hubspot = hubspot or HubSpotClient()

    try:
        if mode == MODE_LIVE:
            # A live scheduled run needs the manual-apply flag too. Two
            # switches for a write, and NEVER a silent fall back to
            # preview: a run that quietly previewed when it was told to
            # write looks exactly like a run with nothing to do.
            from sync import registrations as reg

            if not reg.registrations_sync_allowed():
                raise ChainStopped(
                    "flags",
                    "SCHEDULED_SYNC_MODE is 'live' but "
                    "REGISTRATIONS_SYNC_ENABLED is off. Refusing rather "
                    "than falling back to preview, because a run that "
                    "silently previewed would be indistinguishable from a "
                    "run with nothing to do. Nothing was read and nothing "
                    "was written.")
            _no_manual_apply_running()

        calendar = read_calendar(csuite, today)
        rows, total, error = (calendar["inside"], calendar["total"],
                              calendar["error"])
        out["events_read"] = total
        if error:
            raise ChainStopped("window", error)
        _note_undated(out, calendar["undated"])
        out["calendar_rows"] = calendar["all"]
        out["events_in_window"] = len(rows)
        out["event_ids"] = event_ids_of(rows)
        out["registrable_ids"] = registrable_ids(rows)
        out["contacts_only_ids"] = [
            e for e in out["event_ids"] if e not in out["registrable_ids"]]
        out["event_rows"] = rows

        _step_events(out, hubspot, dry_run, today)
        _step_contacts(out, csuite, hubspot, dry_run)
        _step_registrations(out, csuite, hubspot, dry_run)
    except ChainStopped as stop:
        out["stopped"] = stop.reason
        out["stopped_step"] = stop.step
    except Exception as e:
        # A handler that raises leaves the cron with a traceback and the
        # reader with nothing. Recorded as a stop so exactly one email
        # still goes out.
        logger.error("the scheduled chain died: %s", e, exc_info=True)
        out["stopped"] = f"{type(e).__name__}: {e}"
        out["stopped_step"] = out.get("stopped_step") or "chain"
    out.pop("event_rows", None)
    out.pop("calendar_rows", None)
    return out


def _record(out, step, result, run_id=None):
    out["steps"][step] = result
    if run_id:
        out["run_log_ids"].append(f"{step}:{run_id}")
    elif result.get("run_id"):
        out["run_log_ids"].append(f"{step}:{result['run_id']}")


def _step_events(out, hubspot, dry_run, today):
    """Create the marketing events the later steps need.

    ALWAYS planned as a dry run first, in both modes, so the ceiling is
    checked against a plan before anything is written — and so a preview
    predicts exactly what the live run would refuse. Only if the plan is
    within MAX_EVENT_CREATE_PER_RUN does a live run write, and then capped
    at the number of writes that plan contained, so a calendar that grew
    in the seconds between the two calls cannot slip extra creates past a
    ceiling that was checked against the smaller one.
    """
    from sync import event_apply as ea

    scope = dict(include_ids=tuple(out["event_ids"]), future_only=True,
                 today=today)
    planned = ea.run(hubspot=hubspot, dry_run=True, **scope)
    if planned.get("error"):
        _record(out, STEP_EVENTS, planned)
        raise ChainStopped(STEP_EVENTS, str(planned["error"]))

    creates = would_create_ids(planned)
    out["event_creates"] = describe_events(creates,
                                           out.get("calendar_rows") or [])
    ceiling = out.get("max_event_creates") or max_event_creates()
    if len(creates) > ceiling:
        _record(out, STEP_EVENTS, planned)
        raise ChainStopped(
            STEP_EVENTS,
            f"this run would create {len(creates)} HubSpot marketing "
            f"event(s), more than MAX_EVENT_CREATE_PER_RUN = {ceiling}. "
            f"NOTHING was created, and contacts and registrations did not "
            f"run. The events it would have created are listed above — if "
            f"they are all real, raise the ceiling deliberately or create "
            f"them by hand.")

    if dry_run:
        _record(out, STEP_EVENTS, planned)
        return

    approved = int(planned.get("created") or 0) + \
        int(planned.get("updated") or 0)
    result = ea.run(hubspot=hubspot, dry_run=False, limit=approved, **scope)
    _record(out, STEP_EVENTS, result)
    # Live: report what was ACTUALLY created, not what was planned.
    made = [str(o.get("id")) for o in (result.get("outcomes") or [])
            if o.get("outcome") == "created"]
    out["event_creates"] = describe_events(made,
                                           out.get("calendar_rows") or [])
    if result.get("error"):
        raise ChainStopped(STEP_EVENTS, str(result["error"]))
    if result.get("stopped"):
        raise ChainStopped(STEP_EVENTS, str(result["stopped"]))


def would_create_ids(planned) -> list:
    """The event ids a dry run would create.

    event_apply's predictor returns COUNTS, not ids, so this is the plan's
    creates minus the ones the predictor withheld — the same subtraction
    it does, read back by id.
    """
    plan = (planned or {}).get("plan") or {}
    withheld = {str(i) for i, _why in ((planned or {}).get("withheld_rows")
                                      or [])}
    out = []
    for entry in plan.get("creates") or []:
        mapped = entry[0] if isinstance(entry, (list, tuple)) else entry
        event_id = str(getattr(mapped, "csuite_eventdate_id", "") or "")
        if event_id and event_id not in withheld:
            out.append(event_id)
    return sorted(out, key=lambda e: (len(e), e))


def describe_events(event_ids, calendar_rows) -> list:
    """[{id, name, date, category}] from the CSuite rows already read.

    The name is event_description (the event's title); the category is
    event_name (the parent series). An id with no row is still listed, so
    a created event can never vanish from the email for want of a name.
    """
    by_id = {str((r or {}).get("event_date_id")): r
             for r in calendar_rows or []}
    out = []
    for event_id in event_ids or []:
        row = by_id.get(str(event_id)) or {}
        out.append({
            "id": str(event_id),
            "name": " ".join(str(row.get("event_description")
                                 or "(no name in CSuite)").split()),
            "date": str(row.get("event_date") or "(no date)")[:10],
            "category": str(row.get("event_name") or "(none)"),
        })
    return out


# ---------------------------------------------------------------------------
# Undated events
# ---------------------------------------------------------------------------
#
# Measured 2026-10-13: 98 of 187 CSuite dates have no event_date, and ALL
# 98 are event_name 'Unassigned', event_type_code 'event', archived = 1,
# ids 1052-1149 — one legacy block that predates every real event. So the
# count itself is not news. A NEW undated event is: somebody created a
# live event and left the date blank, and the window can never see it.

_PREVIOUS_UNDATED_SQL = """
    SELECT result->'undated_ids' AS undated_ids
      FROM jobs
     WHERE job_type = 'registrations_sync'
       AND status IN ('complete', 'failed')
       AND result ? 'undated_ids'
     ORDER BY finished_at DESC NULLS LAST, id DESC
     LIMIT 1
"""


def previous_undated_ids():
    """The undated ids the last SCHEDULED run saw, or None.

    Read from that run's jobs.result, which is the dict its email was built
    from. None means no baseline yet — the first run records one rather
    than announcing all 98 as new. A manual "run scheduled sync now"
    compares against this too but writes no jobs row, so the baseline only
    moves when the cron runs.
    """
    from clients import database

    try:
        rows = database.execute_query(_PREVIOUS_UNDATED_SQL, (), fetch=True)
    except Exception as e:
        logger.warning("could not read the previous undated baseline: %s", e)
        return None
    if not rows:
        return None
    found = (rows[0] or {}).get("undated_ids")
    if isinstance(found, str):
        import json
        try:
            found = json.loads(found)
        except ValueError:
            return None
    return [str(i) for i in found] if isinstance(found, list) else None


def _note_undated(out, undated_rows):
    ids = sorted({str((r or {}).get("event_date_id"))
                  for r in undated_rows or []
                  if (r or {}).get("event_date_id")},
                 key=lambda e: (len(e), e))
    out["undated_ids"] = ids
    out["undated_count"] = len(ids)
    out["undated_archived"] = sum(
        1 for r in undated_rows or []
        if str((r or {}).get("archived")) == "1")

    baseline = previous_undated_ids()
    out["undated_baseline"] = baseline is not None
    # Set before the early return: the function must not rely on its caller
    # having initialised the key.
    out["new_undated"] = []
    if baseline is None:
        return
    new = [i for i in ids if i not in set(baseline)]
    by_id = {str((r or {}).get("event_date_id")): r
             for r in undated_rows or []}
    out["new_undated"] = [
        {"id": i,
         "name": " ".join(str((by_id.get(i) or {}).get("event_description")
                              or "(no name in CSuite)").split()),
         "category": str((by_id.get(i) or {}).get("event_name")
                         or "(none)")}
        for i in new]


def _step_contacts(out, csuite, hubspot, dry_run):
    from sync import contact_create as cc

    result = cc.run(csuite=csuite, hubspot=hubspot, dry_run=dry_run,
                    limit=None if dry_run else cc.MAX_CREATE_BATCH,
                    event_ids=tuple(out["event_ids"]))
    _record(out, STEP_CONTACTS, result)
    if result.get("error"):
        raise ChainStopped(STEP_CONTACTS, str(result["error"]))
    if result.get("stopped"):
        raise ChainStopped(STEP_CONTACTS, str(result["stopped"]))


def _step_registrations(out, csuite, hubspot, dry_run):
    from sync import registrations as reg

    ids = tuple(out["registrable_ids"])
    if not ids:
        _record(out, STEP_REGISTRATIONS,
                {"note": "no registrable event in the window — every one "
                         "of them is a sign-up list or is held",
                 "would_register": 0, "registered": 0, "already": 0,
                 "held": 0, "withheld": 0, "events_read": 0})
        return
    result = reg.run(csuite=csuite, hubspot=hubspot, dry_run=dry_run,
                     limit=None if dry_run else reg.MAX_FIRST_RUN_BATCH,
                     event_ids=ids)
    _record(out, STEP_REGISTRATIONS, result)
    if result.get("error"):
        raise ChainStopped(STEP_REGISTRATIONS, str(result["error"]))
    if result.get("stopped"):
        raise ChainStopped(STEP_REGISTRATIONS, str(result["stopped"]))


# ---------------------------------------------------------------------------
# Exactly one email per enabled run
# ---------------------------------------------------------------------------
#
# OK on success, ALERT on any stop. Never both, never neither — a missing
# email is how Carl knows the schedule died, so a run with nothing to do
# still sends one. Masked addresses only, and no address reaches it at all:
# the body is built from counts and event names.

def event_name_of(result, event_id) -> str:
    """The human name for an event, from whatever step has it."""
    for event in (result or {}).get("events") or []:
        if str(event.get("event_date_id")) == str(event_id):
            for key in ("event_name", "name"):
                if event.get(key):
                    return str(event[key])
    return ""


def summary_lines(out) -> list:
    """The email body, as plain-text lines."""
    mode = out.get("mode", MODE_PREVIEW)
    lines = [
        f"mode: {mode}"
        + ("  (nothing is written in preview)" if mode != MODE_LIVE else ""),
        f"window: event_date >= {out.get('window_floor')}",
        f"events in window: {out.get('events_in_window', 0)} "
        f"of {out.get('events_read', 0)} CSuite event date(s)",
    ]
    if out.get("event_ids"):
        lines.append("event ids: " + ", ".join(out["event_ids"]))
    if out.get("contacts_only_ids"):
        lines.append("contacts-only / held (never registered): "
                     + ", ".join(out["contacts_only_ids"]))
    if out.get("run_log_ids"):
        lines.append("run_log: " + ", ".join(out["run_log_ids"]))

    # Every marketing event created — or, in preview, that WOULD be — by
    # CSuite id, name, date and category. A created event is the one thing
    # this run does that a person did not choose, so it is named in full.
    creates = out.get("event_creates") or []
    verb = ("would create" if out.get("dry_run") else "created")
    ceiling = out.get("max_event_creates")
    lines.append("")
    lines.append(f"marketing events {verb}: {len(creates)}"
                 + (f" (ceiling {ceiling})" if ceiling else ""))
    for event in creates:
        lines.append(f"  {event['id']}  {event['date']}  "
                     f"{event['name'][:60]}  [{event['category']}]")

    # Undated events. A NOTE, not an alert: the 98 known ones are an
    # archived legacy block, and what matters is a new one appearing.
    lines.append("")
    lines.append(f"undated CSuite events: {out.get('undated_count', 0)}"
                 f" ({out.get('undated_archived', 0)} archived)")
    if out.get("new_undated"):
        for event in out["new_undated"]:
            lines.append(f"  NOTE new undated event: {event['id']}, "
                         f"{event['name'][:60]} [{event['category']}] — "
                         f"it has no event_date, so the window can never "
                         f"pick it up")
    elif out.get("undated_baseline") is False:
        lines.append("  (first scheduled run — this count is the baseline; "
                     "a new undated event will be named from tomorrow)")
    else:
        lines.append("  no new undated events since the last scheduled run")

    for step in STEPS:
        result = out.get("steps", {}).get(step)
        lines.append("")
        if result is None:
            lines.append(f"[{step}] NOT REACHED")
            continue
        lines.append(f"[{step}]")
        lines.extend(_step_lines(step, result, out))

    if out.get("stopped"):
        lines += ["", f"STOPPED at {out.get('stopped_step')}:",
                  str(out["stopped"])]
    return lines


def _step_lines(step, result, out) -> list:
    # `note`, not `skipped`: event_apply returns `skipped` as an INT count,
    # and reading it as a flag swallowed that whole step's report.
    if result.get("note"):
        return [f"  {result['note']}"]
    lines = []
    if step == STEP_EVENTS:
        # This step discovers its own scope from event/list/dates rather
        # than taking the window, so its withheld and review counts are
        # across the WHOLE calendar — 81 of 187 dates are already past and
        # future_only withholds every one of them. `created` is the number
        # that matters here: those are the window's events that had no
        # marketing event yet.
        lines += [
            f"  created: {result.get('created', 0)}"
            + ("  (would create)" if out.get("dry_run") else ""),
            f"  updated: {result.get('updated', 0)}",
            f"  unchanged: {result.get('unchanged', 0)}",
            f"  withheld (past-dated, whole calendar): "
            f"{result.get('withheld', 0)}",
            f"  skipped (whole calendar): {result.get('skipped', 0)}",
            f"  needs review: {result.get('review', 0)}",
            f"  CSuite event dates read: "
            f"{result.get('event_dates_read', 0)}",
        ]
    elif step == STEP_CONTACTS:
        reasons = result.get("excluded_reasons") or {}
        lines += [
            f"  rows: {result.get('registrant_rows', 0)}"
            f" = {result.get('excluded', 0)} excluded"
            + (" (" + ", ".join(f"{n} {why}"
                                for why, n in sorted(reasons.items())) + ")"
               if reasons else "")
            + f" + {result.get('duplicates_dropped', 0)} duplicate(s)"
            f" + {result.get('unique_emails', 0)} unique",
            f"  already a contact: {result.get('already_contacts', 0)}",
            f"  created: {result.get('created', 0)}"
            + ("  (would create "
               f"{result.get('to_create', 0)})" if out.get("dry_run")
               else f"  (already existed: {result.get('existing', 0)})"),
        ]
        # The marketing breakdown is of NEW contacts only — the ones this
        # run is responsible for. The already-a-contact split is reported
        # by "create contacts dry run" and is not this run's doing.
        new_count = (result.get("to_create", 0) if out.get("dry_run")
                     else result.get("created", 0))
        lines.append(
            f"  marketing status of those {new_count} NEW contact(s): "
            + ("taken from the portal's connected-app setting; this sync "
               "never sets it" if new_count else "n/a — none"))
    elif step == STEP_REGISTRATIONS:
        lines += [
            f"  events read: {result.get('events_read', 0)}",
            f"  already registered: {result.get('already', 0)}",
            f"  registered and VERIFIED: {result.get('registered', 0)}"
            + (f"  (would send {result.get('would_register', 0)})"
               if out.get("dry_run") else ""),
            f"  held: {result.get('held', 0)}",
            f"  withheld (no contact): {result.get('withheld', 0)}",
            f"  unverified: {result.get('unverified', 0)}",
            f"  failed: {result.get('failed', 0)}",
        ]
    for event in (result.get("events") or [])[:20]:
        event_id = event.get("event_date_id")
        name = event_name_of(result, event_id)
        detail = _per_event(step, event)
        if detail:
            lines.append(f"    {event_id}"
                         + (f" {name[:40]}" if name else "")
                         + f": {detail}")
    return lines


def _per_event(step, event) -> str:
    if step == STEP_CONTACTS:
        return (f"{len(event.get('to_create') or [])} to create, "
                f"{len(event.get('created') or [])} created, "
                f"{len(event.get('existing') or [])} already there")
    if step == STEP_REGISTRATIONS:
        return (f"{len(event.get('would_register') or [])} to send, "
                f"{len(event.get('registered') or [])} verified, "
                f"{len(event.get('already') or [])} already, "
                f"{len(event.get('held') or [])} held")
    return ""


def send_summary(out) -> dict:
    """Exactly one email. OK on success, ALERT on a stop."""
    from clients.notify import KIND_ALERT, KIND_OK, safe_notify

    mode = out.get("mode", MODE_PREVIEW)
    body = "\n".join(summary_lines(out))
    if out.get("stopped"):
        return safe_notify(f"daily sync ({mode}) STOPPED",
                           body, kind=KIND_ALERT,
                           path="scheduled sync",
                           run_log=", ".join(out.get("run_log_ids") or [])
                           or "none")
    nothing = _nothing_happened(out)
    return safe_notify(
        f"daily sync ({mode})" + (" — nothing new" if nothing else ""),
        body, kind=KIND_OK, path="scheduled sync",
        run_log=", ".join(out.get("run_log_ids") or []) or "none")


def _nothing_happened(out) -> bool:
    """Whether the run found nothing to do, for the subject line."""
    for step in STEPS:
        result = out.get("steps", {}).get(step) or {}
        for key in ("created", "updated", "to_create", "would_register",
                    "registered"):
            if result.get(key):
                return False
    return True
