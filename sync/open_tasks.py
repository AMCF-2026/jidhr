"""
Open CSuite tasks on a profile (V5.95)
======================================
Before Jidhr proposes or creates a follow-up task on an EXISTING profile, it
asks CSuite whether that profile already has an open task. If it does, no
second task: the person following up already has one.

CSuite cannot be asked about one profile. task/list refuses every filter
that would do it ("Cannot filter by id because its not a column we are
displaying", 2026-10-27, for id, profile_id and o+id), and its view_offset
paging does not page (view_limit 2, offset 2 answered 16 rows). So the
whole list is read in ONE call and matched here — and if CSuite answers
with fewer rows than its own count, the answer is "could not check", never
"no open task".

An open task is one linked to the profile (o == "profile", id == the
profile id) with no complete_date. task 1003's complete_date (2026-05-06)
shows a completed task carries one.

Read-only: task/list is never a write.
"""

LIST_LIMIT = 1000


def load_open_tasks(csuite) -> tuple:
    """({profile_id: [{"task_id", "created"}]}, error). One CSuite read."""
    try:
        response = csuite._request("task/list", {"view_limit": LIST_LIMIT})
    except Exception as e:
        return {}, f"{type(e).__name__}"
    if not isinstance(response, dict) or not response.get("success"):
        return {}, str((response or {}).get("error")
                       or "task/list failed")[:160]
    data = response.get("data") or {}
    rows = data.get("results") or []
    try:
        count = int(data.get("count"))
    except (TypeError, ValueError):
        return {}, "task/list gave no count, so a complete read can't be shown"
    if len(rows) < count:
        return {}, (f"task/list returned {len(rows)} of {count} tasks, so an "
                    "open task could be missed")
    found = {}
    for row in rows:
        if str(row.get("o") or "") != "profile" or row.get("complete_date"):
            continue
        profile_id = str(row.get("id") or "").strip()
        if not profile_id:
            continue
        found.setdefault(profile_id, []).append({
            "task_id": row.get("task_id"),
            "created": str(row.get("created_date") or "")[:10] or None})
    for tasks in found.values():
        tasks.sort(key=lambda t: int(t["task_id"] or 0))
    return found, None


def open_tasks_on_profile(csuite, profile_id, cache=None) -> tuple:
    """([{"task_id", "created"}], error) for one profile. `cache` (a dict)
    lets one run read task/list once for every profile it checks."""
    if cache is not None and "tasks" in cache:
        found, error = cache["tasks"]
    else:
        found, error = load_open_tasks(csuite)
        if cache is not None:
            cache["tasks"] = (found, error)
    if error:
        return [], error
    return list(found.get(str(profile_id).strip(), [])), None


def describe(task) -> str:
    """'existing open task 1099 (created Fri Oct 23)'."""
    from datetime import date

    created = task.get("created")
    try:
        d = date.fromisoformat(created)
        when = f"created {d:%a %b} {d.day}"
    except (TypeError, ValueError):
        when = "created date unknown"
    return f"existing open task {task.get('task_id')} ({when})"


# ---------------------------------------------------------------------------
# How old is the open task? (V5.95.1)
# ---------------------------------------------------------------------------
# An open task created within 30 days of the inquiry is somebody already
# following it up: the submission is recorded as handled, shown once, and
# not again. Older than that, the task is about something else — a stale task
# is not handling — so the submission is held for Carl with both dates. A task
# created AFTER the inquiry counts as within. Exactly 30 days is within.

STALE_AFTER_DAYS = 30
WITHIN, STALE = "within", "stale"
HANDLED_LINK = "existing_open_task"           # submission/link/<this>
STALE_HOLD = "stale_open_task"                # submission/held/<this>


def _day(value):
    from datetime import date, datetime

    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    from intents.daf_workflow import _submitted_moment

    moment = _submitted_moment(value)
    if moment is not None:
        return moment.date()
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def judge(task, submitted_at) -> tuple:
    """(WITHIN or STALE, days from the task's creation to the inquiry).

    A date that cannot be read is STALE: nothing then shows the task is about
    this inquiry, and the cost of holding is a line in Carl's email."""
    created, submitted = _day(task.get("created")), _day(submitted_at)
    if created is None or submitted is None:
        return STALE, None
    days = (submitted - created).days
    return (WITHIN if days <= STALE_AFTER_DAYS else STALE), days


def reason(task) -> str:
    """'existing open task 1099 (2026-10-23)' — the recorded reason."""
    return f"existing open task {task.get('task_id')} ({task.get('created')})"


def record(verdict, form_id, submission_id, profile_id, task) -> bool:
    """One jidhr write_audit row: a handled LINK (within) or a HOLD (stale).

    Ids only, like every other link and hold row: the task's id rides in
    linked_task_id, the reason in the endpoint. Returns False when the row
    could not be stored, so the caller can say so."""
    from clients.audit import (LINK_TARGET_SYSTEM, SUBMISSION_HOLD_ENDPOINT,
                               SUBMISSION_LINK_ENDPOINT, AuditUnavailable,
                               record_write)
    import logging

    if verdict == WITHIN:
        method, endpoint = "LINK", f"{SUBMISSION_LINK_ENDPOINT}/{HANDLED_LINK}"
    else:
        method, endpoint = "HOLD", f"{SUBMISSION_HOLD_ENDPOINT}/{STALE_HOLD}"
    try:
        record_write(LINK_TARGET_SYSTEM, method, endpoint, target_id=None,
                     payload={"hubspot_form_id": form_id,
                              "hubspot_submission_id": submission_id,
                              "linked_profile_id": profile_id,
                              "linked_task_id": task.get("task_id")},
                     status="success")
        return True
    except AuditUnavailable as e:
        logging.getLogger(__name__).error(
            "open-task %s NOT recorded for submission %s: %s", verdict,
            submission_id, e)
        return False


def outcome_text(verdict, task, submitted_at, recorded=True,
                 preview=False) -> str:
    """The one sentence chat and the email both use."""
    def pretty(value):
        d = _day(value)
        return f"{d:%a %b} {d.day}, {d.year}" if d else "date unknown"

    task_part = (f"open task {task.get('task_id')} (created "
                 f"{pretty(task.get('created'))})")
    inquiry = f"this inquiry (submitted {pretty(submitted_at)})"
    if verdict == WITHIN:
        if preview:
            return (f"Existing {task_part}, within {STALE_AFTER_DAYS} days of "
                    f"{inquiry}: would be recorded as handled, no new task.")
        return (f"Existing {task_part}, within {STALE_AFTER_DAYS} days of "
                f"{inquiry}: "
                + ("recorded as handled, no new task. It won't be shown "
                   "again." if recorded else
                   "no new task, but recording it as handled FAILED, so it "
                   "will be shown again."))
    return (f"The profile's only open task is {task_part} — more than "
            f"{STALE_AFTER_DAYS} days before {inquiry}. An old task isn't "
            "follow-up on this inquiry, so nothing was added; held for Carl.")
