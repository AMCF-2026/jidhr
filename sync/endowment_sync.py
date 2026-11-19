"""
Nightly endowment intake
========================
Runs the "new endowment" checks and writes, unattended, for Endowment
Inquiry submissions made on or after JIDHR_ENDOWMENT_SINCE that are neither
handled nor held. Reached only from jobs/handlers/daf_sync.py, after the DAF
step, and reported as a block in the same morning email (JIDHR_DAF_REPORT_TO).

Same pieces as the chat, not a copy of them
-------------------------------------------
intents/endowment_inquiry.py's duplicate_check (contact email, then the
organisation's name against the mirror), ein_plan (one profile read),
create_org, _create_note and _create_task. The chat's two "yes"es exist
because a person pauses between them; here the check runs immediately
before the writes, after the processed/held history is read again.

  new organisation      profile/create/org + note/create + task/create = 3
  matched organisation  note/create + task/create = 2

The nightly NEVER calls profile/edit. A matched organisation whose profile
has no EIN while the form has one is reported ("the form has an EIN and the
profile has none"), never written, and the EIN itself is never shown. It
never touches HubSpot tickets, never writes to HubSpot at all, and never
opens a fund or a pipeline record.

Hold, defer, or ask Carl
------------------------
HELD (a write_audit hold row in live; never retried; "new endowment" or
"mark inquiry handled endowment <id>" in chat clears it):
  missing_fields      no organisation name or no contact email
  ambiguous_match     the email is on an individual profile, a close or
                      double name match, or the email and name disagree
  stale_open_task     the matched profile has an open task over 30 days old
  csuite_error        the profile create failed or did not answer
  partial_write       a matched organisation got its note but not its task
                      (held so that the next night cannot add a second note)

DEFERRED (nothing recorded; the next night tries again):
  check_failed        the duplicate check could not be completed
  task_check_failed   CSuite's task list could not be read
  nightly_limit       CSUITE_WRITE_BUDGET (shared with DAF) ran out

NEEDS CARL with a profile id: a NEW profile was created and the note or the
task then failed. The created profile makes the submission handled
(processed history), so the next night skips it; the missing piece is added
by hand.

Modes
-----
JIDHR_ENDOWMENT_MODE=preview (default) reads only: CSuite, the mirror and
HubSpot's submissions and contact. No hold row, no write. live writes, and
also needs CSUITE_ENDOWMENT_CREATE_ENABLED and CSUITE_ENDOWMENT_ASSIGNEE.
JIDHR_ENDOWMENT_SINCE unset: the step does nothing and reads nothing.

Logs and audit rows carry submission and profile ids and counts only. The
email names the organisation and the contact, with the email masked; the
EIN, the starting goal and the essays are never in it.
"""

import logging
from datetime import date

from config import Config
from sync.daf_sync import (MODE_LIVE, MODE_PREVIEW, BadSetting, Refused,
                           budget_problem)

logger = logging.getLogger(__name__)

FORM_KEY = "endowment"
LABEL = "Endowment"
WRITES_NEW = 3          # profile + note + task
WRITES_MATCHED = 2      # note + task on the existing organisation profile
REQUIRED = (("organization", "organisation name"),
            ("email", "contact email address"))

HOLD_REASONS = {
    "missing_fields": "The form is missing the {fields}.",
    "ambiguous_match": ("Jidhr couldn't tell whether this organization is "
                        "already in CSuite, so nothing was added. {detail}"),
    "stale_open_task": "{text}",
    "csuite_error": ("CSuite returned an error or didn't answer while "
                     "creating the organization profile. Check CSuite "
                     "before trying again — it may already be there."),
    "partial_write": ("CSuite profile {profile_id} is already in CSuite, "
                      "but not everything was added to it: {problem}. Add "
                      "what's missing by hand. Jidhr won't try this one "
                      "again (a second try could add a second note)."),
    "check_failed": ("Jidhr couldn't finish checking CSuite for this "
                     "organization, so nothing was added. Jidhr will try "
                     "again tomorrow night."),
    "task_check_failed": ("Jidhr couldn't read CSuite's tasks to see "
                          "whether this organization already has an open "
                          "endowment task, so nothing was added. Jidhr will "
                          "try again tomorrow night."),
    "nightly_limit": ("Nightly limit reached: tonight's allowance of CSuite "
                      "changes ran out before this one. Jidhr will try "
                      "again tomorrow night."),
}
DEFERRED_REASONS = frozenset({"check_failed", "task_check_failed",
                              "nightly_limit"})

EIN_TEXT = {
    "set": ("EIN: the form has one and the CSuite profile has none. Jidhr "
            "doesn't change existing profiles; add it in CSuite by hand."),
    "mismatch": ("EIN: the CSuite profile's EIN is different from the "
                 "form's. Nothing was changed; check which is right."),
    "unreadable": ("EIN: the CSuite profile couldn't be read, so whether "
                   "its EIN matches the form's is unknown."),
}


def mode() -> str:
    raw = str(getattr(Config, "JIDHR_ENDOWMENT_MODE", MODE_PREVIEW) or "")
    return MODE_LIVE if raw.strip().lower() == MODE_LIVE else MODE_PREVIEW


def since():
    """The date, or None when unset (the step does nothing)."""
    raw = str(getattr(Config, "JIDHR_ENDOWMENT_SINCE", "") or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        raise BadSetting("JIDHR_ENDOWMENT_SINCE is not a date (YYYY-MM-DD), "
                         "so the nightly endowment step did nothing.")


def writes_needed(plan) -> int:
    return WRITES_NEW if plan == "new" else WRITES_MATCHED


def _live_refusal():
    """Why a LIVE run cannot write at all, or None."""
    from intents.daf_workflow import endowment_switch
    from intents.endowment_inquiry import assignee
    from sync.daf_reconcile import link_rows_allowed

    enabled, _note = endowment_switch()
    if not enabled:
        return ("JIDHR_ENDOWMENT_MODE is live but "
                "CSUITE_ENDOWMENT_CREATE_ENABLED is off on this service, so "
                "the nightly endowment step wrote nothing.")
    if not assignee():
        from intents.endowment_inquiry import assignee_not_an_id

        return ("JIDHR_ENDOWMENT_MODE is live but CSUITE_ENDOWMENT_ASSIGNEE "
                f"{assignee_not_an_id() or 'is not set'} on this service, so "
                "the nightly endowment step wrote nothing.")
    try:
        if not link_rows_allowed():
            return ("write_audit cannot hold a hold row yet (migration 007), "
                    "so the nightly endowment step wrote nothing.")
    except Exception as e:
        return f"the audit table could not be checked ({e}); nothing written."
    return None


def _entry(data) -> dict:
    """What the email shows for one submission. Never the EIN, the goal or
    the essays; the contact's email masked."""
    from intents.endowment_inquiry import contact_name, submission_link
    from sync.contact_create import mask_email

    return {"submission_id": data.get("submission_id"),
            "submitted_at": data.get("submitted_at"),
            "organization": data.get("organization") or "(no name given)",
            "name": contact_name(data) or "(no contact name)",
            "email": mask_email(data.get("email")),
            "hubspot_link": submission_link(data.get("submission_id"))}


def _record_hold(form_id, submission_id, reason) -> bool:
    from sync.daf_sync import _record_hold as record

    return record(form_id, submission_id, reason)


def _plain(lines) -> str:
    """The duplicate check's chat lines as one plain sentence group."""
    out = []
    for line in lines or ():
        text = str(line).strip().lstrip("•").strip()
        for mark in ("⚠️", "🏛️", "↔️"):
            text = text.replace(mark, "").strip()
        if text and not text.lower().startswith(("mirror data",
                                                  "mirror synced")):
            out.append(text)
    return " ".join(out)


def run(hubspot=None, csuite=None, run_mode=None, budget_left=None):
    """One nightly pass, or None when JIDHR_ENDOWMENT_SINCE is unset.

    `budget_left` is what the DAF step left of CSUITE_WRITE_BUDGET tonight;
    None means the whole budget. Never raises for a submission; raises
    Refused/BadSetting when the step cannot run at all."""
    from clients.audit import held_submissions, processed_submissions
    from intents.daf_workflow import (_parse_submission, _submitted_moment,
                                      fetch_all_submissions,
                                      handled_endpoints, inquiry_form)
    from intents import endowment_inquiry as ei
    from sync import open_tasks as ot

    floor = since()
    if floor is None:
        return None
    run_mode = run_mode or mode()
    problem = budget_problem()
    if problem:
        raise BadSetting(problem.replace("nightly DAF step",
                                         "nightly endowment step"))
    if run_mode == MODE_LIVE:
        refusal = _live_refusal()
        if refusal:
            raise Refused(refusal)

    budget_total = int(getattr(Config, "CSUITE_WRITE_BUDGET", 0) or 0)
    allowance = (budget_total if budget_left is None
                 else max(0, min(budget_total, int(budget_left))))
    if hubspot is None:
        from clients.hubspot import HubSpotClient
        hubspot = HubSpotClient()
    if csuite is None:
        from clients.csuite import CSuiteClient
        from sync.sandbox_writes import WriteBudget
        csuite = CSuiteClient()
        # One nightly allowance across DAF and endowment.
        csuite.write_budget = WriteBudget(allowance)

    row = inquiry_form(FORM_KEY)
    form_id = row["form_id"]
    endpoints = handled_endpoints(row)
    out = {"form": FORM_KEY, "label": LABEL, "mode": run_mode,
           "since": floor.isoformat(), "candidates": 0, "added": [],
           "held": [], "deferred": [], "existing_task": [],
           "skipped_handled": 0, "skipped_held": 0,
           "budget": allowance, "budget_needed": 0, "budget_allowed": 0}

    raw, error, complete = fetch_all_submissions(hubspot, form_id)
    if error:
        raise Refused(f"HubSpot's endowment submissions could not be read "
                      f"({error}); nothing was done.")
    if not complete:
        raise Refused("Not every endowment submission could be read, so the "
                      "nightly step did nothing rather than act on a part.")
    try:
        processed = processed_submissions(form_id, endpoints=endpoints)
        held = held_submissions(form_id)
    except Exception as e:
        raise Refused(f"the processed/held history could not be read ({e}); "
                      "nothing was done.")

    subs = []
    for sub in raw:
        data = _parse_submission(sub, row["field_map"])
        moment = _submitted_moment(data.get("submitted_at"))
        if moment is not None and moment.date() >= floor:
            subs.append((moment, data))
    subs.sort(key=lambda pair: pair[0])           # oldest first
    out["candidates"] = len(subs)

    if run_mode == MODE_LIVE:
        remaining = (csuite.write_budget.remaining
                     if getattr(csuite, "write_budget", None) else 0)
    else:
        remaining = allowance
    employee = ei.assignee()
    task_cache = {}
    displays = {}                   # task/display reads, once per task

    for _moment_, data in subs:
        sid = data.get("submission_id")
        if sid in processed:
            out["skipped_handled"] += 1
            continue
        if sid in held:
            out["skipped_held"] += 1
            continue

        def hold(reason, **details):
            entry = dict(_entry(data), reason=reason,
                         why=HOLD_REASONS[reason].format(**details))
            if reason in DEFERRED_REASONS:
                out["deferred"].append(entry)
                logger.warning("endowment submission %s deferred: %s", sid,
                               reason)
                return entry
            if run_mode == MODE_LIVE:
                entry["recorded"] = _record_hold(form_id, sid, reason)
            out["held"].append(entry)
            logger.warning("endowment submission %s held: %s", sid, reason)
            return entry

        missing = [label for key, label in REQUIRED if not data.get(key)]
        if missing:
            hold("missing_fields", fields=" and ".join(missing))
            continue

        try:
            duplicate = ei.duplicate_check(data, hubspot, csuite)
        except Exception as e:
            logger.error("endowment submission %s: check raised %s", sid,
                         type(e).__name__)
            hold("check_failed")
            continue
        outcome = duplicate["outcome"]
        if outcome == ei.UNVERIFIABLE:
            hold("check_failed")
            continue
        if outcome == ei.AMBIGUOUS:
            hold("ambiguous_match", detail=_plain(duplicate.get("lines")))
            continue

        plan = "new" if outcome == ei.NEW else "returning"
        profile_id = ei._existing_id(duplicate) if plan == "returning" \
            else None
        ein_action = None
        check_skipped = False
        if plan == "returning":
            ein_action = ei.ein_plan(data, duplicate, csuite).get("action")
        if plan == "returning" and not employee:
            # Preview only (live refuses without the assignee): whose open
            # tasks count can't be told, so the check is skipped and the
            # block says so.
            check_skipped = True
        elif plan == "returning":
            found, problem = ei.own_open_tasks(csuite, profile_id,
                                               task_cache, displays)
            if problem:
                hold("task_check_failed")
                continue
            if found:
                decided = ot.decide(found, data.get("submitted_at"))
                task, verdict, days = (decided["task"], decided["verdict"],
                                       decided["days"])
                if verdict == ot.STALE:
                    hold("stale_open_task",
                         text=ot.outcome_text(verdict, task,
                                              data.get("submitted_at"),
                                              count=decided["count"]))
                    continue
                recorded = (ot.record(verdict, form_id, sid, profile_id,
                                      task)
                            if run_mode == MODE_LIVE else False)
                out["existing_task"].append(dict(
                    _entry(data), plan=plan, existing_profile=profile_id,
                    open_task=task, recorded=recorded, days=days))
                continue

        needed = writes_needed(plan)
        out["budget_needed"] += needed
        if remaining < needed:
            hold("nightly_limit")
            continue

        entry = dict(_entry(data), plan=plan, existing_profile=profile_id,
                     writes=needed, ein=EIN_TEXT.get(ein_action),
                     task_title_length=len(ei.task_title(data)),
                     open_task_check_skipped=check_skipped)
        if run_mode == MODE_PREVIEW:
            remaining -= needed
            out["budget_allowed"] += needed
            out["added"].append(entry)
            continue

        # LIVE. The history again, right before writing.
        try:
            if sid in processed_submissions(form_id, endpoints=endpoints) \
                    or sid in held_submissions(form_id):
                out["skipped_handled"] += 1
                continue
        except Exception:
            hold("check_failed")
            continue

        before = csuite.write_budget.used if getattr(
            csuite, "write_budget", None) else 0
        _write(out, entry, data, plan, profile_id, csuite, employee,
               form_id, hold, displays)
        used = (csuite.write_budget.used - before) if getattr(
            csuite, "write_budget", None) else needed
        remaining -= used
        out["budget_allowed"] += used
    return out


def _write(out, entry, data, plan, profile_id, csuite, employee, form_id,
           hold, displays=None):
    """The writes for one submission, then where it goes in the report."""
    from intents import endowment_inquiry as ei

    meta = {"hubspot_form_id": form_id,
            "hubspot_submission_id": data.get("submission_id")}
    results = {"profile_created": False, "errors": [], "warnings": [],
               "task_id": None, "task_due": None, "task_failed": None,
               "task_skipped": None, "task_assignee": None}
    state = {"profile_id": profile_id}
    sid = data.get("submission_id")
    try:
        status = ei.run_endowment_writes(data, state, results, csuite,
                                         employee, meta,
                                         new_org=plan == "new",
                                         displays=displays)
        if status == "profile_failed":
            hold("csuite_error")
            return
        if status == "deferred":
            # The write-time open-task recheck could not be read. Nothing
            # was sent; the next night tries again.
            hold("task_check_failed")
            return
    except Exception as e:
        logger.error("endowment submission %s: write raised %s", sid,
                     type(e).__name__)
        if not state.get("profile_id"):
            hold("csuite_error")
            return
        results["task_failed"] = results.get("task_failed") or (
            f"the write stopped ({type(e).__name__})")

    pid = state.get("profile_id")
    existing = results.get("task_existing")
    if existing and not results.get("task_id"):
        # The write-time recheck (before the note) found an own open task
        # the plan did not see; run_endowment_writes judged and recorded it.
        if existing.get("verdict") == "stale":
            out["held"].append(dict(entry, reason="stale_open_task",
                                    why=existing.get("text") or "",
                                    recorded=existing.get("recorded"),
                                    note_id=results.get("note_id")))
        else:
            out["existing_task"].append(dict(
                entry, existing_profile=pid, open_task=existing,
                recorded=existing.get("recorded"),
                note_id=results.get("note_id")))
        return

    problems = list(results.get("errors") or [])
    if results.get("note_failed"):
        problems.append(f"the note wasn't added ({results['note_failed']})")
    task_missing = not results.get("task_id")
    if task_missing:
        why = (results.get("task_failed") or results.get("task_skipped")
               or "no task id came back")
        problems.append(f"the follow-up task wasn't created ({why})")

    if plan == "returning" and task_missing:
        # Note added (or not), no task: without a hold row the next night
        # would see this submission as new and send a second note.
        hold("partial_write", profile_id=pid,
             problem="; ".join(problems) or "unknown")
        if out["held"]:
            out["held"][-1]["note_id"] = results.get("note_id")
        return

    entry.update(profile_id=pid, profile_created=bool(
                     results.get("profile_created")),
                 note_id=results.get("note_id"),
                 task_id=results.get("task_id"),
                 task_due=results.get("task_due"),
                 assignee=results.get("task_assignee"))
    if problems:
        entry["problems"] = problems
        entry["next_night"] = (
            "The created profile marks this submission handled, so Jidhr "
            "won't try it again by itself."
            if results.get("profile_created") else
            "The follow-up task marks this submission handled, so Jidhr "
            "won't try it again by itself.")
        logger.warning("endowment submission %s added with %d problem(s)",
                       sid, len(problems))
    else:
        logger.info("endowment submission %s added: profile %s, task %s",
                    sid, pid, results.get("task_id"))
    out["added"].append(entry)


# ---------------------------------------------------------------------------
# The block in the morning email (sync/daf_sync.section_text calls this)
# ---------------------------------------------------------------------------

def _person_lines(index, entry) -> list:
    from sync.daf_sync import _when

    return [f"{index}. {entry['organization']} — contact {entry['name']}, "
            f"{entry['email']} (submitted {_when(entry.get('submitted_at'))})"]


def section_text(section, mark_preview=False) -> list:
    from sync.daf_sync import _day

    preview = section.get("mode") == MODE_PREVIEW
    clean = [e for e in section.get("added") or [] if not e.get("problems")]
    rough = [e for e in section.get("added") or [] if e.get("problems")]
    held = section.get("held") or []
    deferred = section.get("deferred") or []
    open_task = section.get("existing_task") or []
    if not (clean or rough or held or deferred or open_task):
        return []
    lines = ["ENDOWMENT INQUIRIES"
             + (" (PREVIEW)" if preview and mark_preview else ""), ""]
    new = [e for e in clean if e.get("plan") != "returning"]
    matched = [e for e in clean if e.get("plan") == "returning"]
    groups = [(new, "Would be added" if preview else "Added to CSuite"),
              (matched, "Already in CSuite: would get a note and a "
                        "follow-up task" if preview else
                        "Already in CSuite: note and follow-up task added")]
    for group, heading in groups:
        if not group:
            continue
        lines.append(f"{heading} ({len(group)})")
        for i, e in enumerate(group, 1):
            lines += _person_lines(i, e)
            if preview:
                what = ("a new CSuite organization profile, a note with the "
                        "essays and a follow-up task"
                        if e.get("plan") == "new" else
                        f"a note with the essays and a follow-up task on "
                        f"CSuite profile {e.get('existing_profile')}")
                lines.append(f"   Would create {what} "
                             f"({e.get('writes')} CSuite changes).")
            else:
                made = (f"New CSuite organization profile {e.get('profile_id')}"
                        if e.get("profile_created") else
                        f"CSuite profile {e.get('profile_id')}")
                lines.append(f"   {made}; note {e.get('note_id')}; "
                             f"follow-up task {e.get('task_id')} for "
                             f"{e.get('assignee') or 'the endowment assignee'}"
                             f", due {_day(e.get('task_due'))}.")
            if e.get("ein"):
                lines.append(f"   {e['ein']}")
            if e.get("open_task_check_skipped"):
                from intents.endowment_inquiry import assignee_not_an_id

                why = assignee_not_an_id() or "isn't set"
                lines.append("   Open-task check skipped: "
                             f"CSUITE_ENDOWMENT_ASSIGNEE {why}, so whose "
                             "open tasks count can't be told. Live would "
                             "check first.")
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
            lines.append(f"   CSuite profile {e.get('profile_id')}"
                         f"{' (new)' if e.get('profile_created') else ''}"
                         ", but: " + "; ".join(e["problems"]) + ".")
            lines.append(f"   Next night: {e.get('next_night')}")
            lines.append(f"   HubSpot: {e['hubspot_link']}")
        lines.append("")
        lines.append("Jidhr won't try these again by itself.")
        lines.append("")
    if open_task:
        from sync.open_tasks import WITHIN, outcome_text

        lines.append(f"Already has an open CSuite task ({len(open_task)})")
        for i, e in enumerate(open_task, 1):
            lines += _person_lines(i, e)
            lines.append(f"   CSuite profile {e.get('existing_profile')}. "
                         + outcome_text(WITHIN, e["open_task"],
                                        e.get("submitted_at"),
                                        recorded=e.get("recorded", True),
                                        preview=preview))
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
                     f"changes left after DAF would have allowed "
                     f"{section.get('budget_allowed')} of the "
                     f"{section.get('budget_needed')} needed.")
        lines.append("")
    return lines
