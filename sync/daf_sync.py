"""
Nightly DAF intake (V5.90)
==========================
Runs the "new daf" plan, unattended, for DAF Inquiry submissions made on or
after JIDHR_DAF_SINCE that are neither handled nor held. Reached only from
jobs/handlers/daf_sync.py, after registrations_sync.

Same plan as the chat, not a copy of it
---------------------------------------
For each submission it runs the chat's own pieces: existing_hubspot_link and
already_in_csuite (the duplicate guard), then intents.daf_workflow.
run_daf_writes — the exact write sequence "new daf" runs after a yes, which
re-runs the duplicate guard immediately before writing. A new donor gets an
individual profile, a follow-up task and csuite_profile_id on their HubSpot
contact (2 CSuite writes). A returning donor — exactly one email match —
gets the task only, on their existing profile (1 write).

Hold, don't guess
-----------------
Any of these HOLDS the submission and writes nothing for it:

  missing_fields          no first name, last name or email
  no_hubspot_contact      no HubSpot contact behind the form submission (an
                          anomaly to report; creating one is never done here)
  multiple_profiles       more than one CSuite profile carries the email
  stale_link              HubSpot points at a CSuite profile that is gone
  stale_link_other_match  ...and a different profile carries the email
  conflicting_link        HubSpot and CSuite point at different profiles
  csuite_error            CSuite refused or did not answer while writing

A hold is a write_audit row (target_system 'jidhr', endpoint
submission/held/<reason>, ids only). It is never retried automatically: the
next night skips it, and it stays held until it is handled in chat ("new
daf", or "mark inquiry handled daf <id>"), which makes it processed.

Defer, don't hold, what tomorrow can fix (V5.93)
------------------------------------------------
These say nothing about the submission, only about tonight, so they are
DEFERRED: nothing is written for it, no hold row is recorded, and the next
night tries it again. Recording them is how one bad night — or a budget of
0 — would have held every submission for good.

  nightly_limit           CSUITE_WRITE_BUDGET ran out before this one
  hubspot_unreadable      HubSpot did not answer the contact lookup
  check_failed            the duplicate check could not be completed
  task_check_failed       CSuite's task list could not be read completely

Open task already there (V5.95)
-------------------------------
A returning donor whose CSuite profile already has an OPEN task gets no
second one: nothing is proposed or written, and the email shows "existing
open task N (created ...)" in its own section and its own count. Nothing is
recorded either, so it is shown again each night until it is handled in
chat ("mark inquiry handled daf <id>") — an open task is a person's to close.

No double-creates
-----------------
Processed (profile, task or link row) and held submissions are skipped, and
the processed history is re-read immediately before each write, so a rerun
— or a chat create in the meantime — cannot make a second task.

Modes
-----
JIDHR_DAF_MODE=preview (default) reads only and writes nothing, not even a
hold row: it reports what it WOULD add, with the exact task title, and what
CSUITE_WRITE_BUDGET would have allowed. live writes.

Logs and the audit table carry submission ids and counts only — never a
donor's name, email or form answers. The email to JIDHR_DAF_REPORT_TO does
carry them: it is for the staff who act on it.
"""

import logging
from datetime import date, datetime, timezone

from config import Config

logger = logging.getLogger(__name__)

MODE_PREVIEW = "preview"
MODE_LIVE = "live"
FORM_KEY = "daf"
REQUIRED = (("first_name", "first name"), ("last_name", "last name"),
            ("email", "email address"))

WRITES_NEW = 2          # profile + task
WRITES_RETURNING = 1    # task on the existing profile

# Hold reasons, as the email reads them. Written for someone who does not
# know what an API is.
HOLD_REASONS = {
    "missing_fields": "The form is missing the donor's {fields}.",
    "hubspot_unreadable": ("HubSpot didn't answer when Jidhr looked this "
                           "donor up, so nothing was added. Jidhr will try "
                           "again tomorrow night."),
    "no_hubspot_contact": ("There's no HubSpot contact for this email "
                           "address. Every form submission should create "
                           "one, so something went wrong on the HubSpot "
                           "side. Nothing was added."),
    "multiple_profiles": ("More than one CSuite profile already uses this "
                          "email address, so Jidhr didn't pick one."),
    "check_failed": ("Jidhr couldn't finish checking CSuite for an existing "
                     "profile, so nothing was added. Jidhr will try again "
                     "tomorrow night."),
    "stale_link": ("HubSpot links this person to a CSuite profile that no "
                   "longer exists."),
    "stale_link_other_match": ("HubSpot links this person to a CSuite "
                               "profile that no longer exists, and a "
                               "different profile has their email address."),
    "conflicting_link": ("HubSpot and CSuite point to two different "
                         "profiles for this person."),
    "task_check_failed": ("Jidhr couldn't read CSuite's task list to see "
                          "whether this donor already has an open task, so "
                          "nothing was added. Jidhr will try again tomorrow "
                          "night."),
    "nightly_limit": ("Nightly limit reached: tonight's allowance of CSuite "
                      "changes ran out before this one. Jidhr will try "
                      "again tomorrow night."),
    "csuite_error": ("CSuite returned an error or didn't answer while "
                     "adding this one. Check CSuite before trying again — "
                     "part of it may already be there."),
}

# Reasons about tonight, not about the submission: deferred to the next
# night, never recorded as a hold. See "Defer, don't hold" above.
DEFERRED_REASONS = frozenset({"nightly_limit", "hubspot_unreadable",
                              "check_failed", "task_check_failed"})

# DuplicateProfile.kind -> hold reason.
_DUPLICATE_HOLDS = {
    "ambiguous": "multiple_profiles",
    "unverifiable": "check_failed",
    "stale_no_match": "stale_link",
    "stale_with_match": "stale_link_other_match",
    "conflict": "conflicting_link",
}


class Refused(RuntimeError):
    """The whole step cannot run. Nothing was written, nobody was held."""


class BadSetting(Refused):
    """A setting on the service is wrong. The step stands down with an
    [Jidhr ALERT] naming it; the job itself does NOT fail, because nothing
    crashed — the run exits 0 and tomorrow's runs again."""


def budget_problem():
    """Why CSUITE_WRITE_BUDGET cannot be used, or None.

    Anything but a whole number is refused, never read as 0: read as 0, a
    live run holds every submission for good as "nightly limit reached".
    An unset or 0 budget is refused too — it lets nothing be written.
    """
    import re

    raw = str(getattr(Config, "CSUITE_WRITE_BUDGET_RAW", "") or "").strip()
    if raw and not re.fullmatch(r"\d+", raw):
        shown = raw if len(raw) <= 20 else raw[:20] + "…"
        return (f"CSUITE_WRITE_BUDGET is set to {shown!r}, which is not a "
                "whole number. It must be the number of CSuite changes "
                "allowed per night, e.g. 10. The nightly DAF step did "
                "nothing and held nothing; fix the variable on this "
                "service.")
    if int(getattr(Config, "CSUITE_WRITE_BUDGET", 0) or 0) < 1:
        return ("CSUITE_WRITE_BUDGET is not set (or is 0), so the nightly "
                "DAF step could not add anything. It did nothing and held "
                "nothing; set it to the number of CSuite changes allowed "
                "per night, e.g. 10.")
    return None


def mode() -> str:
    raw = str(getattr(Config, "JIDHR_DAF_MODE", MODE_PREVIEW) or "")
    return MODE_LIVE if raw.strip().lower() == MODE_LIVE else MODE_PREVIEW


def since() -> date:
    raw = str(getattr(Config, "JIDHR_DAF_SINCE", "") or "").strip()
    if not raw:
        raise Refused("JIDHR_DAF_SINCE is not set, so the nightly DAF step "
                      "did nothing (without it, it would take on every "
                      "historic submission).")
    try:
        return date.fromisoformat(raw)
    except ValueError:
        raise Refused("JIDHR_DAF_SINCE is not a date (YYYY-MM-DD), so the "
                      "nightly DAF step did nothing.")


def _live_refusal():
    """Why a LIVE run cannot write at all, or None."""
    from sync.daf_reconcile import link_rows_allowed

    off = [name for name in ("CSUITE_DAF_CREATE_ENABLED",
                             "CSUITE_DAF_TASK_CREATE_ENABLED")
           if not getattr(Config, name, False)]
    if off:
        return (f"JIDHR_DAF_MODE is live but {' and '.join(off)} "
                f"{'is' if len(off) == 1 else 'are'} off on this service, so "
                "the nightly DAF step wrote nothing.")
    if not getattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", None):
        return ("JIDHR_DAF_MODE is live but CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY "
                "is not set on this service, so the nightly DAF step wrote "
                "nothing.")
    try:
        if not link_rows_allowed():
            return ("write_audit cannot hold a hold row yet (migration 007), "
                    "so the nightly DAF step wrote nothing.")
    except Exception as e:
        return f"the audit table could not be checked ({e}); nothing written."
    return None


def _moment(value):
    from intents.daf_workflow import _submitted_moment

    return _submitted_moment(value)


def _hubspot_link(contact_id, submission_id) -> str:
    if contact_id:
        return Config.HUBSPOT_CONTACT_URL.format(contact_id=contact_id)
    form = Config.HUBSPOT_FORM_SUBMISSIONS_URL.format(
        form_id=Config.DAF_INQUIRY_FORM_ID)
    return f"{form} (submission {submission_id})"


def _entry(data, contact_id=None) -> dict:
    """What the email shows for one submission."""
    name = f"{data.get('first_name', '')} {data.get('last_name', '')}".strip()
    return {"submission_id": data.get("submission_id"),
            "submitted_at": data.get("submitted_at"),
            "name": name or "(no name given)",
            "email": data.get("email") or "",
            "fund_name": data.get("fund_name") or "",
            "contribution": data.get("initial_contribution") or "",
            "hubspot_link": _hubspot_link(contact_id,
                                          data.get("submission_id"))}


def _record_hold(form_id, submission_id, reason) -> bool:
    from clients.audit import (LINK_TARGET_SYSTEM, SUBMISSION_HOLD_ENDPOINT,
                               AuditUnavailable, record_write)
    try:
        record_write(LINK_TARGET_SYSTEM, "HOLD",
                     f"{SUBMISSION_HOLD_ENDPOINT}/{reason}", target_id=None,
                     payload={"hubspot_form_id": form_id,
                              "hubspot_submission_id": submission_id},
                     status="success")
        return True
    except AuditUnavailable as e:
        logger.error("DAF hold NOT recorded for submission %s (%s): %s",
                     submission_id, reason, e)
        return False


def run(hubspot=None, csuite=None, run_mode=None) -> dict:
    """One nightly pass. Never raises for a submission; raises Refused when
    the step cannot run at all."""
    from clients.audit import held_submissions, processed_submissions
    from intents.daf_workflow import (DuplicateProfile, _parse_submission,
                                      already_in_csuite, existing_hubspot_link,
                                      fetch_all_submissions,
                                      followup_task_title, inquiry_form)

    run_mode = run_mode or mode()
    problem = budget_problem()
    if problem:
        raise BadSetting(problem)
    floor = since()
    if run_mode == MODE_LIVE:
        refusal = _live_refusal()
        if refusal:
            raise Refused(refusal)

    if hubspot is None:
        from clients.hubspot import HubSpotClient
        hubspot = HubSpotClient()
    if csuite is None:
        from clients.csuite import CSuiteClient
        csuite = CSuiteClient()

    row = inquiry_form(FORM_KEY)
    form_id = row["form_id"]
    budget_total = int(getattr(Config, "CSUITE_WRITE_BUDGET", 0) or 0)
    out = {"form": FORM_KEY, "label": row["label"], "mode": run_mode,
           "since": floor.isoformat(), "candidates": 0, "added": [],
           "held": [], "deferred": [], "existing_task": [],
           "skipped_handled": 0, "skipped_held": 0,
           "budget": budget_total, "budget_needed": 0,
           "budget_allowed": 0}

    raw, error, complete = fetch_all_submissions(hubspot, form_id)
    if error:
        raise Refused(f"HubSpot's DAF submissions could not be read "
                      f"({error}); nothing was done.")
    if not complete:
        raise Refused("Not every DAF submission could be read, so the "
                      "nightly step did nothing rather than act on a part.")
    try:
        processed = processed_submissions(form_id)
        held = held_submissions(form_id)
    except Exception as e:
        raise Refused(f"the processed/held history could not be read ({e}); "
                      "nothing was done.")

    subs = []
    for sub in raw:
        data = _parse_submission(sub, row["field_map"])
        moment = _moment(data.get("submitted_at"))
        if moment is not None and moment.date() >= floor:
            subs.append((moment, data))
    subs.sort(key=lambda pair: pair[0])           # oldest first
    out["candidates"] = len(subs)

    if run_mode == MODE_LIVE:
        remaining = (csuite.write_budget.remaining
                     if getattr(csuite, "write_budget", None) else 0)
    else:
        remaining = budget_total

    task_cache = {}                 # one task/list read for the whole run
    for _moment_, data in subs:
        sid = data.get("submission_id")
        if sid in processed:
            out["skipped_handled"] += 1
            continue
        if sid in held:
            out["skipped_held"] += 1
            continue

        def hold(reason, contact_id=None, **details):
            entry = dict(_entry(data, contact_id), reason=reason,
                         why=HOLD_REASONS[reason].format(**details))
            if reason in DEFERRED_REASONS:
                # Nothing recorded: tomorrow night sees it as new again.
                out["deferred"].append(entry)
                logger.warning("DAF submission %s deferred: %s", sid, reason)
                return
            if run_mode == MODE_LIVE:
                entry["recorded"] = _record_hold(form_id, sid, reason)
            out["held"].append(entry)
            logger.warning("DAF submission %s held: %s", sid, reason)

        missing = [label for key, label in REQUIRED if not data.get(key)]
        if missing:
            hold("missing_fields", fields=" and ".join(missing))
            continue

        contact_id, _stored, ok = existing_hubspot_link(data["email"],
                                                        hubspot)
        if not ok:
            hold("hubspot_unreadable")
            continue
        if not contact_id:
            hold("no_hubspot_contact")
            continue

        try:
            already_in_csuite(data, hubspot, csuite)
            plan, existing_profile = "new", None
        except DuplicateProfile as duplicate:
            if duplicate.kind == "duplicate" and duplicate.profile_id:
                plan, existing_profile = "returning", duplicate.profile_id
            else:
                hold(_DUPLICATE_HOLDS.get(duplicate.kind, "check_failed"),
                     contact_id)
                continue

        if plan == "returning":
            from sync.open_tasks import open_tasks_on_profile

            open_tasks, problem = open_tasks_on_profile(
                csuite, existing_profile, task_cache)
            if problem:
                hold("task_check_failed", contact_id)
                continue
            if open_tasks:
                out["existing_task"].append(dict(
                    _entry(data, contact_id), plan=plan,
                    existing_profile=existing_profile,
                    open_task=open_tasks[0]))
                logger.info("DAF submission %s: profile %s already has open "
                            "task %s; none proposed", sid, existing_profile,
                            open_tasks[0]["task_id"])
                continue

        needed = WRITES_NEW if plan == "new" else WRITES_RETURNING
        out["budget_needed"] += needed
        if remaining < needed:
            hold("nightly_limit", contact_id)
            continue

        entry = dict(_entry(data, contact_id), plan=plan,
                     existing_profile=existing_profile,
                     task_title=followup_task_title(data, "DAF"),
                     writes=needed)
        if run_mode == MODE_PREVIEW:
            remaining -= needed
            out["budget_allowed"] += needed
            out["added"].append(entry)
            continue

        # LIVE. Re-read the history right before writing: a chat create or
        # an earlier attempt since this run started must not be repeated.
        try:
            if sid in processed_submissions(form_id) or \
                    sid in held_submissions(form_id):
                out["skipped_handled"] += 1
                continue
        except Exception:
            hold("check_failed", contact_id)
            continue

        before = csuite.write_budget.used if getattr(
            csuite, "write_budget", None) else 0
        state = {"active": True, "workflow_type": "daf", "type": FORM_KEY,
                 "step": "confirm", "form_id": form_id,
                 "submission_data": data, "profile_id": None,
                 "funit_id": None, "ticket_id": None}
        try:
            from intents.daf_workflow import run_daf_writes
            results = run_daf_writes(state, hubspot, csuite,
                                     allow_contact_create=False)
        except Exception as e:
            logger.error("DAF submission %s: write raised %s", sid,
                         type(e).__name__)
            hold("csuite_error", contact_id)
            continue
        used = (csuite.write_budget.used - before) if getattr(
            csuite, "write_budget", None) else needed
        remaining -= used
        out["budget_allowed"] += used
        _classify(out, entry, results, state, hold, contact_id, sid)
    return out


def _classify(out, entry, results, state, hold, contact_id, sid):
    """Turn run_daf_writes' results into added / needs-a-person."""
    task_id = results.get("task_id")
    if results.get("task_existing") and not task_id:
        # The write-time re-check found an open task the plan did not see.
        out["existing_task"].append(dict(
            entry, existing_profile=state.get("profile_id"),
            open_task=results["task_existing"]))
        return
    if results.get("task_check_failed") and not task_id and \
            not results.get("profile_created"):
        hold("task_check_failed", contact_id)
        return
    problems = list(results.get("errors") or [])
    if results.get("task_failed"):
        problems.append(f"the follow-up task wasn't created "
                        f"({results['task_failed']}) — add it by hand")
    if results.get("duplicate_reason") and not task_id:
        # Either the re-check found something new (a hold reason of its
        # own), or a returning donor's task did not go through.
        hold(_DUPLICATE_HOLDS.get(results.get("duplicate_kind"),
                                  "csuite_error"), contact_id)
        return
    if not results.get("profile_created") and not results.get(
            "duplicate_reason"):
        hold("csuite_error", contact_id)
        return
    entry.update(profile_id=state.get("profile_id"), task_id=task_id,
                 task_due=results.get("task_due"),
                 assignee=results.get("task_assignee"),
                 profile_created=bool(results.get("profile_created")))
    if problems:
        entry["problems"] = problems
        logger.warning("DAF submission %s added with %d problem(s)", sid,
                       len(problems))
    else:
        logger.info("DAF submission %s added: profile %s, task %s", sid,
                    entry["profile_id"], task_id)
    out["added"].append(entry)


# ---------------------------------------------------------------------------
# The email
# ---------------------------------------------------------------------------

def counts(sections) -> tuple:
    """(added cleanly, needs Carl) across every form section."""
    added = sum(1 for s in sections for e in s.get("added") or []
                if not e.get("problems"))
    needs = sum(len(s.get("held") or []) for s in sections) + sum(
        1 for s in sections for e in s.get("added") or []
        if e.get("problems"))
    return added, needs


def _clean(sections, returning):
    return [e for s in sections for e in s.get("added") or []
            if not e.get("problems")
            and (e.get("plan") == "returning") is returning]


def existing_task_count(sections) -> int:
    """Returning donors whose profile already has an open task (V5.95)."""
    return sum(len(s.get("existing_task") or []) for s in sections)


def deferred_count(sections) -> int:
    """Submissions put off to the next night (DEFERRED_REASONS)."""
    return sum(len(s.get("deferred") or []) for s in sections)


def subject(sections, when=None) -> str:
    """New donor inquiries added to CSuite — Wed Oct 22 (2 added, 1 follow-up
    task, 1 needs Carl, 1 has an open task, 1 deferred). A zero part is
    dropped; a preview says so. A returning donor gets a follow-up task, not
    "added" (V5.95)."""
    from zoneinfo import ZoneInfo

    when = when or datetime.now(timezone.utc)
    local = when.astimezone(ZoneInfo("America/New_York"))
    day = f"{local:%a %b} {local.day}"
    preview = any(s.get("mode") == MODE_PREVIEW for s in sections)
    _added, needs = counts(sections)
    new = len(_clean(sections, returning=False))
    followups = len(_clean(sections, returning=True))
    parts = []
    if new:
        parts.append(f"{new} {'would be added' if preview else 'added'}")
    if followups:
        parts.append(f"{followups} follow-up task"
                     f"{'' if followups == 1 else 's'}")
    if needs:
        parts.append(f"{needs} {'needs' if needs == 1 else 'need'} Carl")
    open_tasks = existing_task_count(sections)
    if open_tasks:
        parts.append(f"{open_tasks} {'has' if open_tasks == 1 else 'have'} "
                     "an open task")
    deferred = deferred_count(sections)
    if deferred:
        parts.append(f"{deferred} deferred")
    tail = f" ({', '.join(parts)})" if parts else ""
    if preview:
        return f"[Preview] New donor inquiries for CSuite — {day}{tail}"
    return f"New donor inquiries added to CSuite — {day}{tail}"


def _when(value) -> str:
    moment = _moment(value)
    if moment is None:
        return "date unknown"
    from zoneinfo import ZoneInfo
    local = moment.astimezone(ZoneInfo("America/New_York"))
    return f"{local:%a %b} {local.day}"


def _day(iso) -> str:
    """'2027-01-19' -> 'Tue Jan 19'. Anything else comes back as it was."""
    try:
        d = date.fromisoformat(str(iso))
    except ValueError:
        return str(iso or "")
    return f"{d:%a %b} {d.day}"


def _short_id(entry) -> str:
    return str(entry.get("submission_id") or "")[:8]


def carl_footer(sections) -> list:
    """The chat commands, for Carl only, after the signature. Staff read
    the body; nothing in it asks them to type anything into Jidhr."""
    held = [e for s in sections for e in s.get("held") or []]
    open_task = [e for s in sections for e in s.get("existing_task") or []]
    if not (held or open_task):
        return []
    lines = ["", "-----", "(for Carl)"]
    if held:
        lines.append('Held inquiries, in Jidhr chat: "new daf" to add one, or '
                     '"mark inquiry handled daf <id>" if it was dealt with '
                     'already.')
        lines += [f"  {e.get('name') or '(no name given)'}: {_short_id(e)}"
                  for e in held]
    if open_task:
        lines.append('Open task already there — these show every night until '
                     '"mark inquiry handled daf <id>":')
        lines += [f"  {e.get('name') or '(no name given)'}: {_short_id(e)}"
                  for e in open_task]
    return lines


def _person_lines(index, entry) -> list:
    lines = [f"{index}. {entry['name']} — {entry['email'] or 'no email'} "
             f"(submitted {_when(entry.get('submitted_at'))})"]
    fund = entry.get("fund_name") or "not given"
    contribution = entry.get("contribution") or "not given"
    lines.append(f"   Fund name: {fund} · Contribution: {contribution}")
    return lines


def section_text(section) -> list:
    """One form's part of the email. Endowment can add its own later."""
    preview = section.get("mode") == MODE_PREVIEW
    label = section.get("label", "DAF")
    clean = [e for e in section.get("added") or [] if not e.get("problems")]
    rough = [e for e in section.get("added") or [] if e.get("problems")]
    held = section.get("held") or []
    deferred = section.get("deferred") or []
    open_task = section.get("existing_task") or []
    if not (clean or rough or held or deferred or open_task):
        return []
    lines = [f"{label} INQUIRIES", ""]
    new = [e for e in clean if e.get("plan") != "returning"]
    returning = [e for e in clean if e.get("plan") == "returning"]
    groups = [(new, "Would be added" if preview else "Added to CSuite"),
              (returning, "Would get a follow-up task" if preview
               else "Follow-up task added")]
    for group, heading in groups:
        if not group:
            continue
        lines.append(f"{heading} ({len(group)})")
        for i, e in enumerate(group, 1):
            lines += _person_lines(i, e)
            if preview:
                what = ("a new CSuite profile and a follow-up task"
                        if e.get("plan") == "new" else
                        f"a follow-up task on their existing CSuite profile "
                        f"{e.get('existing_profile')}")
                lines.append(f"   Would create {what}.")
                lines.append(f"   Task title: {e.get('task_title')}")
            else:
                made = (f"New CSuite profile {e.get('profile_id')}"
                        if e.get("profile_created") else
                        f"Already in CSuite (profile {e.get('profile_id')})")
                lines.append(f"   {made}; follow-up task {e.get('task_id')} "
                             f"for {e.get('assignee') or 'the DAF assignee'}"
                             f", due {_day(e.get('task_due'))}.")
            lines.append(f"   HubSpot: {e['hubspot_link']}")
        lines.append("")
    if held or rough:
        lines.append(f"Needs Carl ({len(held) + len(rough)})")
        i = 0
        for e in held:
            i += 1
            lines += _person_lines(i, e)
            lines.append(f"   Why: {e['why']}")
            lines.append(f"   HubSpot: {e['hubspot_link']}")
        for e in rough:
            i += 1
            lines += _person_lines(i, e)
            lines.append(f"   Added to CSuite (profile {e.get('profile_id')}"
                         f"{', task ' + str(e['task_id']) if e.get('task_id') else ''}"
                         "), but: " + "; ".join(e["problems"]))
            lines.append(f"   HubSpot: {e['hubspot_link']}")
        lines.append("")
        lines.append("Jidhr won't try these again by itself.")
        lines.append("")
    if open_task:
        from sync.open_tasks import describe

        lines.append(f"Already has an open CSuite task ({len(open_task)})")
        for i, e in enumerate(open_task, 1):
            lines += _person_lines(i, e)
            text = describe(e["open_task"])
            lines.append(f"   {text[:1].upper()}{text[1:]} on CSuite profile "
                         f"{e.get('existing_profile')}, so no new task was "
                         f"{'proposed' if preview else 'added'}.")
            lines.append(f"   HubSpot: {e['hubspot_link']}")
        lines.append("")
    if deferred:
        lines.append(f"Deferred to tomorrow night ({len(deferred)})")
        for i, e in enumerate(deferred, 1):
            lines += _person_lines(i, e)
            lines.append(f"   Why: {e['why']}")
        lines.append("")
        lines.append("Nothing was added for these. Jidhr will try them again "
                     "tomorrow night by itself.")
        lines.append("")
    if preview:
        lines.append(f"Tonight's limit of {section.get('budget')} CSuite "
                     f"changes would have allowed {section.get('budget_allowed')}"
                     f" of the {section.get('budget_needed')} needed.")
        lines.append("")
    return lines


def email_text(sections) -> str:
    preview = any(s.get("mode") == MODE_PREVIEW for s in sections)
    lines = ["Good morning,", "",
             "Jidhr checked the website's donor inquiry forms overnight."]
    if preview:
        lines += ["", "This is a PREVIEW: nothing was added to CSuite. It "
                      "shows what Jidhr would have done."]
    lines.append("")
    for section in sections:
        lines += section_text(section)
    lines += ["Nothing was sent to any donor.", "", "— Jidhr"]
    lines += carl_footer(sections)
    return "\n".join(lines)


def send_email(sections, when=None) -> dict:
    """Send only when at least one submission was added, held, deferred or
    found with an open task."""
    from clients.notify import send_report

    added, needs = counts(sections)
    recipients = tuple(getattr(Config, "JIDHR_DAF_REPORT_TO", ()) or ())
    if not (added or needs or deferred_count(sections)
            or existing_task_count(sections)):
        return {"sent": [], "failed": [], "skipped": "nothing to report"}
    if not recipients:
        logger.warning("DAF email not sent: JIDHR_DAF_REPORT_TO is not set")
        return {"sent": [], "failed": [], "skipped": "no recipients"}
    return send_report(recipients, subject(sections, when),
                       email_text(sections))
