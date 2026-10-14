"""Tie DAF submissions to the follow-up work already done for them.

Why this exists
---------------
A DAF submission reads as HANDLED when a write_audit row carries its id
(clients.audit.processed_submissions). Until V5.79 only a profile create
carried one. The returning-donor path finds an existing CSuite profile,
makes a follow-up task and returns — no profile create — so the task was
the only write, it carried no submission id, and the donor was offered
again in "new daf" forever. V5.79 makes new tasks carry the id. This
reconciles the tasks made BEFORE that.

Measured 2026-10-14, before any of this was written:

* The six live DAF follow-up tasks are 1033-1038, each linked to a CSuite
  profile by `o = 'profile'` + `id` (21696, 21697, 21698, 21699, 21700,
  21713). None carries a submission id.
* Four are already explained by a processed submission (audit profile_id
  and email agree): 1034, 1035, 1037, 1038.
* 1033 belongs to submission 62ed3c5d (2026-09-29, unprocessed): its email
  resolves to exactly task 1033's profile.
* 1036 matched TWO submissions: 6a31606e (2026-09-15, processed — it made
  profile 21699 and task 1036) and 6fff2fa0 (2026-09-03, unprocessed), one
  donor who submitted twice. Carl decided: link 6fff2fa0 as
  earlier_submission_same_donor, referencing 6a31606e, 21699 and 1036.

What it writes
--------------
ONLY write_audit rows, target_system 'jidhr', endpoint
"submission/link/<reason>". ZERO requests to CSuite or HubSpot: it reads a
page of form submissions, reads tasks, searches profiles, and records what
it concluded. migrations/007 must be run first; the live run checks.

Page 1 only — the newest 50 submissions. HubSpot returns them newest first
(proven 2026-10-14 across all 90), so every new submission is on page 1;
the 40 older ones are out of scope by decision.

Nothing ambiguous is ever linked. It is reported, and a person decides.
"""

import logging
import time
from collections import defaultdict

logger = logging.getLogger(__name__)

# Why a submission was linked. The reason is part of the ENDPOINT, not the
# payload: clients.audit.payload_meta keeps key names and id values only,
# so a reason held as a payload value would be thrown away.
LINK_TASK_FOR_PROFILE = "task_for_profile"
LINK_EARLIER_SUBMISSION = "earlier_submission_same_donor"
LINK_REASONS = (LINK_TASK_FOR_PROFILE, LINK_EARLIER_SUBMISSION)

# Why one was not.
NO_PROFILE = "no CSuite profile for this address"
MANY_PROFILES = "more than one CSuite profile for this address"
SEARCH_FAILED = "CSuite could not be searched"
NO_TASK = "a profile, but no follow-up task and no handled submission"
LATER_SUBMISSION = ("submitted AFTER the donor's handled submission — may be "
                    "a new request, so it is not suppressed")
NOT_ORDERABLE = ("the donor's handled submission is not on page 1, so which "
                 "came first cannot be told")
AMBIGUOUS_TASK = "the task matches more than one submission"
NO_ADDRESS = "the submission carries no email address"

# Live follow-up tasks made before tasks carried a submission id, for ONE
# form.
#
# Until V5.81 this was not filtered by form at all, which was harmless while
# DAF was the only form with tasks and wrong the moment a second one had any.
# A row that names its form counts only for that form. A row that names NO
# form is a pre-V5.79 task, and those are attributed to DAF: all six (1033-
# 1038, write_audit 54-76) are DAF follow-ups assigned to 1012, read
# 2026-10-14, and no other form had a task path then. See
# LEGACY_UNTAGGED_FORM_ID.
_LEGACY_TASKS_SQL = """
    SELECT id AS audit_id, target_id AS task_id
      FROM write_audit
     WHERE target_system = 'csuite'
       AND endpoint = 'task/create'
       AND status = 'success'
       AND csuite_env = 'live'
       AND payload_meta->'ids'->>'hubspot_submission_id' IS NULL
       AND COALESCE(payload_meta->'ids'->>'hubspot_form_id', %s) = %s
       AND target_id IS NOT NULL
     ORDER BY id
"""

# Tasks made SINCE V5.79, which carry their submission — and their form.
_TASKED_SUBMISSIONS_SQL = """
    SELECT DISTINCT payload_meta->'ids'->>'hubspot_submission_id' AS sid
      FROM write_audit
     WHERE target_system = 'csuite'
       AND endpoint = 'task/create'
       AND status = 'success'
       AND csuite_env = 'live'
       AND payload_meta->'ids'->>'hubspot_submission_id' IS NOT NULL
       AND payload_meta->'ids'->>'hubspot_form_id' = %s
"""


def legacy_untagged_form_id():
    """The form a task row with no form id belongs to: DAF. See above."""
    from config import Config

    return Config.DAF_INQUIRY_FORM_ID

# Is migrations/007 in? Checked BEFORE writing anything, rather than
# discovered as a CheckViolation part way through a run.
_TARGET_CHECK_SQL = """
    SELECT pg_get_constraintdef(oid) AS def
      FROM pg_constraint
     WHERE conname = 'write_audit_target_system_check'
"""


class ReconcileRefused(RuntimeError):
    """The live run cannot proceed; nothing was written."""


def _pace():
    from clients.csuite_fetch import configured_pace_ms

    time.sleep(configured_pace_ms() / 1000.0)


def link_rows_allowed() -> bool:
    """Whether write_audit's CHECK admits target_system 'jidhr'."""
    from clients import database

    rows = database.execute_query(_TARGET_CHECK_SQL, (), fetch=True)
    definition = str(((rows or [{}])[0] or {}).get("def") or "")
    return "'jidhr'" in definition


def legacy_tasks(form_id) -> list:
    from clients import database

    rows = database.execute_query(
        _LEGACY_TASKS_SQL, (legacy_untagged_form_id(), str(form_id)),
        fetch=True)
    return [str(r["task_id"]) for r in rows or [] if r.get("task_id")]


def tasked_submissions(form_id) -> set:
    from clients import database

    rows = database.execute_query(_TASKED_SUBMISSIONS_SQL, (str(form_id),),
                                  fetch=True)
    return {str(r["sid"]) for r in rows or [] if r.get("sid")}


def task_profile(csuite, task_id):
    """The profile a task is linked to, or None. One CSuite READ.

    The link is `o` + `id`, not profile_id (clients.csuite.create_task).
    Read back rather than taken from the audit row, so a task re-linked by
    hand in CSuite is seen as it is now.
    """
    response = csuite._request("task/display", {"task_id": int(task_id)})
    data = (response or {}).get("data")
    if isinstance(data, list):
        data = data[0] if data else {}
    data = data or {}
    if str(data.get("o") or "").lower() != "profile" or not data.get("id"):
        return None
    return str(data["id"])


def profiles_for(csuite, email) -> tuple:
    """([profile ids], error). The duplicate guard's own trusted search."""
    from sync.filter_trust import search_before_create

    try:
        _count, response = search_before_create(
            lambda endpoint, body: csuite._request(endpoint, body),
            "profile/list", "primary_email", email)
    except Exception as e:
        return [], f"{type(e).__name__}: {e}"
    rows = ((response or {}).get("data") or {}).get("results") or []
    return [str(r.get("profile_id")) for r in rows if r.get("profile_id")], None


def page_one(hubspot, form_id) -> list:
    """The newest 50 submissions, parsed. No paging, by decision.

    Parsed with the form's own field map, so an organisation form's company
    address is not read as a person's.
    """
    from intents.daf_workflow import _parse_submission, inquiry_form_for_id

    field_map = (inquiry_form_for_id(form_id) or {}).get("field_map")
    raw = hubspot.get_form_submissions(form_id, limit=50) or {}
    return [_parse_submission(s, field_map) for s in raw.get("results") or []]


def _moment(value):
    from intents.daf_workflow import _submitted_moment

    return _submitted_moment(value)


def plan(hubspot, csuite, form_id) -> dict:
    """Every proposed link and every leftover. Reads only."""
    from clients.audit import processed_submissions
    from sync.readback import normalise_email

    out = {"form_id": form_id, "fetched": 0, "processed_before": 0,
           "unprocessed": 0, "links": [], "unlinked": [], "tasks": {},
           "csuite_reads": 0}

    from intents.daf_workflow import handled_endpoints, inquiry_form_for_id

    submissions = page_one(hubspot, form_id)
    out["fetched"] = len(submissions)
    # Kept so "processed after" is the SAME measurement as "processed
    # before" — page 1, handled — and the two can be compared directly.
    out["fetched_ids"] = [s["submission_id"] for s in submissions]
    by_id = {s["submission_id"]: s for s in submissions}
    endpoints = handled_endpoints(inquiry_form_for_id(form_id))
    processed = (processed_submissions(form_id) if endpoints is None
                 else processed_submissions(form_id, endpoints=endpoints))
    out["processed_before"] = sum(1 for s in submissions
                                  if s["submission_id"] in processed)

    # Each legacy task's profile, read back from CSuite.
    tasks_by_profile = defaultdict(list)
    for task_id in legacy_tasks(form_id):
        profile = task_profile(csuite, task_id)
        out["csuite_reads"] += 1
        _pace()
        out["tasks"][task_id] = profile
        if profile:
            tasks_by_profile[profile].append(task_id)

    # A processed submission's donor, and whether that donor has a task.
    tasked = tasked_submissions(form_id)
    handled_by_profile = defaultdict(list)
    for sid, profile in processed.items():
        if profile:
            handled_by_profile[str(profile)].append(sid)

    claims = defaultdict(list)            # task_id -> [submission_id]
    for sub in submissions:
        sid = sub["submission_id"]
        if sid in processed:
            continue
        out["unprocessed"] += 1
        email = normalise_email(sub.get("email"))
        base = {"submission_id": sid, "submitted_at": sub.get("submitted_at"),
                "email": email}
        if not email:
            out["unlinked"].append(dict(base, why=NO_ADDRESS))
            continue

        profiles, error = profiles_for(csuite, email)
        out["csuite_reads"] += 1
        _pace()
        if error:
            out["unlinked"].append(dict(base, why=f"{SEARCH_FAILED}: {error}"))
            continue
        if not profiles:
            out["unlinked"].append(dict(base, why=NO_PROFILE))
            continue
        if len(profiles) > 1:
            out["unlinked"].append(dict(base, why=MANY_PROFILES,
                                        profiles=profiles))
            continue
        profile = profiles[0]

        handled = [h for h in handled_by_profile.get(profile, [])
                   if h in tasked or tasks_by_profile.get(profile)]
        if handled:
            # Rule B: the same donor has a PROCESSED submission with a task,
            # and resolves to exactly one profile. Only an EARLIER
            # submission is linked — a later one may be a new request, and
            # suppressing it would hide real work.
            dated = [(h, _moment(by_id[h].get("submitted_at")))
                     for h in handled if h in by_id]
            mine = _moment(sub.get("submitted_at"))
            if not dated or mine is None or any(d is None for _h, d in dated):
                out["unlinked"].append(dict(base, why=NOT_ORDERABLE,
                                            profile=profile))
                continue
            first_handled, first_at = min(dated, key=lambda x: x[1])
            if mine >= first_at:
                out["unlinked"].append(dict(base, why=LATER_SUBMISSION,
                                            profile=profile))
                continue
            task = (tasks_by_profile.get(profile) or [None])[0]
            out["links"].append(dict(
                base, reason=LINK_EARLIER_SUBMISSION, profile=profile,
                task=task, linked_from=first_handled))
            continue

        free = tasks_by_profile.get(profile) or []
        if len(free) == 1:
            # Rule A: exactly one legacy task on this donor's profile, and no
            # processed submission explains it.
            claims[free[0]].append(sid)
            out["links"].append(dict(base, reason=LINK_TASK_FOR_PROFILE,
                                     profile=profile, task=free[0]))
            continue
        out["unlinked"].append(dict(base, why=NO_TASK, profile=profile))

    # A task claimed by more than one submission links NONE of them.
    ambiguous = {t for t, subs in claims.items() if len(subs) > 1}
    if ambiguous:
        kept = []
        for link in out["links"]:
            if link["reason"] == LINK_TASK_FOR_PROFILE and \
                    link["task"] in ambiguous:
                out["unlinked"].append(dict(
                    {k: link[k] for k in ("submission_id", "submitted_at",
                                          "email")},
                    why=AMBIGUOUS_TASK, task=link["task"]))
            else:
                kept.append(link)
        out["links"] = kept
    return out


def run(hubspot=None, csuite=None, dry_run=True, form_id=None) -> dict:
    """Plan, and unless dry_run, record the links. Never writes elsewhere."""
    from clients.csuite import CSuiteClient
    from clients.hubspot import HubSpotClient
    from config import Config

    form_id = form_id or Config.DAF_INQUIRY_FORM_ID
    hubspot = hubspot or HubSpotClient()
    csuite = csuite or CSuiteClient()

    if not dry_run and not link_rows_allowed():
        raise ReconcileRefused(
            "write_audit's target_system CHECK does not admit 'jidhr' yet, so "
            "a link row would fail with a CheckViolation. Run "
            "migrations/007_write_audit_jidhr_target.sql first. Nothing was "
            "read and nothing was written.")

    out = plan(hubspot, csuite, form_id)
    out["dry_run"] = dry_run
    out["written"] = []
    if dry_run:
        return out

    from clients.audit import (LINK_TARGET_SYSTEM, SUBMISSION_LINK_ENDPOINT,
                               record_write)

    for link in out["links"]:
        payload = {
            "hubspot_form_id": form_id,
            "hubspot_submission_id": link["submission_id"],
            "linked_profile_id": link.get("profile"),
            "linked_task_id": link.get("task"),
            "linked_from_submission_id": link.get("linked_from"),
        }
        record_write(LINK_TARGET_SYSTEM, "LINK",
                     f"{SUBMISSION_LINK_ENDPOINT}/{link['reason']}",
                     target_id=link["submission_id"],
                     payload={k: v for k, v in payload.items()
                              if v not in (None, "")},
                     status="success")
        out["written"].append(link["submission_id"])
    return out
