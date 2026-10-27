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
