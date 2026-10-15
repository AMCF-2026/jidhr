"""
Endowment inquiries: an ORGANISATION profile and a follow-up task (V5.81)
========================================================================
Row 2 of Config.INQUIRY_FORMS. Reached only through intents/daf_workflow,
and only when CSUITE_ENDOWMENT_CREATE_ENABLED is exactly "true".

Why it is not the DAF path
--------------------------
Step 0 discovery, 2026-10-14: the Endowment Inquiry Form is an organisation
form. The DAF path makes an INDIVIDUAL profile from the contact person, so
it would have put the organisation's address on the person and dropped the
organisation's name and EIN. This path makes an ORG profile, from the
company fields (objectTypeId 0-2) and never from the contact's.

What it writes, and what it never writes
----------------------------------------
* One `profile/create/org`, only when neither duplicate check finds the
  organisation. Organisation name, contact email, contact phone, the
  organisation's address, website.
* One `task/create`, assigned to CSUITE_ENDOWMENT_ASSIGNEE, task type 1065,
  linked to the new or existing org profile. Its description carries the
  EIN, the starting goal and the three essays, because no profile field
  can: an org profile has no EIN field, and none of the others has a
  confirmed input name.
* Never a fund (deferred, as for DAF). Never the HubSpot contact: its
  csuite_profile_id is the PERSON's link, and an org id there would be
  wrong. Never a ticket: the HubSpot workflow creates none (Carl,
  2026-10-14).

The EIN, the goal and the essays are never logged, never put in write_audit
(payload_meta keeps key names and ids only) and never kept in the session
cookie: the create step re-reads the submission from HubSpot.

Two confirmations (V5.85)
-------------------------
The review needs no CSuite call, so it cannot say whether the organisation is
already in CSuite. The first "yes" therefore runs the duplicate check — reads
only — and shows the outcome and exactly what would be written. Nothing is
written until a SECOND "yes". At that second "yes" the check runs again, and
if its answer is no longer the one that was approved (someone created the
organisation in between, say), nothing is written and the new answer is
shown instead.

Refusals, each before anything is written
-----------------------------------------
* CSUITE_ENDOWMENT_ASSIGNEE unset: "assignee not confirmed". Employee 1002
  is believed to be Ola; until Carl confirms it in the CSuite UI, nothing is
  guessed and nothing is created.
* A duplicate that cannot be ruled out: an email match on an INDIVIDUAL
  profile (the person exists; whether the organisation does is unknown), a
  close name match, more than one exact match, an email match and a name
  match that disagree, or a check that could not run.
"""

import logging

from clients.csuite import mark_id, ui_url
from config import Config

logger = logging.getLogger(__name__)

ROW_KEY = "endowment"

# What the session cookie may hold. The EIN, the goal, the essays and "how
# they heard" are re-read from HubSpot when they are needed.
SESSION_KEYS = ("submission_id", "submitted_at", "first_name", "last_name",
                "email", "phone", "organization", "website",
                "org_address_street", "org_address_city", "org_address_state",
                "org_address_zip")

ESSAYS = (("essay_mission", "Mission and core services/programs"),
          ("essay_interest", "What prompted the interest in an endowment"),
          ("essay_goals", "Main endowment goals"))

# CSuite's task_description limit is NOT KNOWN. No endpoint describes task
# fields, no write has ever tested one, and the 14 tasks in production carry
# one-line descriptions. 4,000 characters is a conservative guess: the
# read-back compares task_description, so a cut CSuite makes is reported as
# "did not store: task_description" rather than missed.
TASK_DESCRIPTION_MAX = 4000
TRUNCATED = "[truncated, see HubSpot]"
REVIEW_ESSAY_PREVIEW = 200

# How the four outcomes of the duplicate check are named.
NEW, EXISTING, AMBIGUOUS, UNVERIFIABLE = ("new", "existing", "ambiguous",
                                          "unverifiable")


def _row():
    from intents.daf_workflow import inquiry_form

    return inquiry_form(ROW_KEY)


def assignee():
    """The confirmed assignee's employee id, or None."""
    row = _row() or {}
    return getattr(Config, row.get("assignee_env") or "", None) or None


def session_copy(parsed: dict) -> dict:
    """The part of a parsed submission the session cookie may hold."""
    return {k: (parsed or {}).get(k, "") for k in SESSION_KEYS}


def contact_name(data: dict) -> str:
    return f"{data.get('first_name', '')} {data.get('last_name', '')}".strip()


def org_address(data: dict) -> str:
    street = (data.get("org_address_street") or "").strip()
    city = (data.get("org_address_city") or "").strip()
    tail = " ".join(p for p in ((data.get("org_address_state") or "").strip(),
                                (data.get("org_address_zip") or "").strip())
                    if p)
    return ", ".join(p for p in (street, city, tail) if p)


def submission_link(submission_id) -> str:
    url = Config.HUBSPOT_FORM_SUBMISSIONS_URL.format(
        form_id=Config.ENDOWMENT_INQUIRY_FORM_ID)
    return f"{url} (submission {submission_id})"


# ---------------------------------------------------------------------------
# Checks shown in the review: the name against the mirror, and other forms
# ---------------------------------------------------------------------------

def name_check(organization) -> dict:
    """{"exact", "close", "age", "error"}. Never raises."""
    from sync import org_match

    try:
        rows, synced = org_match.mirror_org_rows()
    except org_match.MirrorUnavailable as e:
        return {"exact": [], "close": [], "age": None, "error": str(e)}
    found = org_match.match_org_name(organization, rows)
    found["age"] = org_match.mirror_age_line(synced)
    found["error"] = None
    return found


def other_handled(email, submission_id, hubspot) -> tuple:
    """([handled submissions with this email on ANY row], [warnings]).

    "Every row's handled set": DAF and endowment, this submission excluded.
    A match is reported, never a reason to stop.
    """
    from clients.audit import handled_tasks
    from intents import daf_workflow as wf
    from sync.readback import normalise_email

    wanted = normalise_email(email)
    found, warnings = [], []
    if not wanted:
        return found, warnings
    for row in Config.INQUIRY_FORMS:
        try:
            raw, error, _complete = wf.fetch_all_submissions(hubspot,
                                                             row["form_id"])
            if error:
                raise RuntimeError(error)
            processed, warning = wf._processed_map(
                row["form_id"], wf.handled_endpoints(row))
            if warning:
                raise RuntimeError("its processed history could not be read")
            tasks = handled_tasks(row["form_id"])
        except Exception as e:
            warnings.append(f"the {row['label']} inquiries could not be "
                            f"checked ({e})")
            continue
        for sub in raw:
            parsed = wf._parse_submission(sub, row["field_map"])
            sid = parsed["submission_id"]
            if sid == str(submission_id) or sid not in processed:
                continue
            if normalise_email(parsed.get("email")) != wanted:
                continue
            found.append({"label": row["label"], "submission_id": sid,
                          "submitted_at": parsed.get("submitted_at"),
                          "task": tasks.get(sid),
                          "profile": processed.get(sid)})
    return found, warnings


def other_handled_lines(found, warnings) -> list:
    from intents.daf_workflow import format_submitted_at

    lines = []
    for hit in found:
        what = (f"task {hit['task']}" if hit.get("task")
                else f"profile {hit['profile']}" if hit.get("profile")
                else "handled with no task")
        lines.append(f"• ↔️ Also a {hit['label']} inquiry "
                     f"({format_submitted_at(hit['submitted_at'])}), {what}.")
    for warning in warnings:
        lines.append(f"• ⚠️ Cross-form check incomplete: {warning}.")
    return lines


def _name_lines(check) -> list:
    if check.get("error"):
        return [f"• ⚠️ Name check could not run: {check['error']}."]
    lines = []
    exact, close = check.get("exact") or [], check.get("close") or []
    if exact:
        lines.append("• 🏛️ Already in CSuite by name: " + ", ".join(
            f"{r['organization']} (profile {r['profile_id']})"
            for r in exact))
    if close:
        from sync.org_match import MAX_CLOSE_SHOWN

        shown = close[:MAX_CLOSE_SHOWN]
        more = (f" and {len(close) - len(shown)} more"
                if len(close) > len(shown) else "")
        lines.append("• ⚠️ Close name match — AMBIGUOUS, never linked "
                     "automatically: " + ", ".join(
                         f"{r['organization']} (profile {r['profile_id']})"
                         for r in shown) + more)
    if not exact and not close:
        lines.append("• No organisation profile with this name in CSuite.")
    lines.append(f"  {check.get('age') or 'Mirror data age unknown'}.")
    return lines


# ---------------------------------------------------------------------------
# The review
# ---------------------------------------------------------------------------

def _preview(text) -> str:
    text = " ".join(str(text or "").split())
    if len(text) <= REVIEW_ESSAY_PREVIEW:
        return text or "—"
    return text[:REVIEW_ESSAY_PREVIEW].rstrip() + "…"


def render_review(parsed: dict, hubspot, note: str = None) -> str:
    """The review block for one endowment submission."""
    from intents.daf_workflow import format_submitted_at

    lines = ["📋 **New Endowment Inquiry**", ""]
    if note:
        lines += [note, ""]
    lines += [
        f"🏛️ **Organization:** {parsed.get('organization') or 'Not given'}",
        f"🧾 **EIN:** {parsed.get('ein') or 'Not given'}",
        f"🌐 **Website:** {parsed.get('website') or 'N/A'}",
        f"🏠 **Organization address:** {org_address(parsed) or 'Not given'}",
        f"👤 **Contact:** {contact_name(parsed) or 'Not given'}",
        f"📧 **Email:** {parsed.get('email') or 'N/A'}",
        f"📱 **Phone:** {parsed.get('phone') or 'N/A'}",
        f"🎯 **Starting goal:** {parsed.get('starting_goal') or 'Not given'}",
    ]
    for key, label in ESSAYS:
        lines.append(f"📝 **{label}:** {_preview(parsed.get(key))}")
    lines += [
        f"📅 **Submitted:** "
        f"{format_submitted_at(parsed.get('submitted_at'))}",
        f"🆔 **Submission:** `{parsed.get('submission_id')}`",
        "",
        "**Checks**",
    ]
    lines += _name_lines(name_check(parsed.get("organization")))
    found, warnings = other_handled(parsed.get("email"),
                                    parsed.get("submission_id"), hubspot)
    lines += other_handled_lines(found, warnings)
    if not assignee():
        lines.append("• 🛑 Assignee not confirmed: CSUITE_ENDOWMENT_ASSIGNEE "
                     "is not set, so create will refuse.")
    lines += [
        "",
        "---",
        "**Shall I check CSuite for this organisation?** \"Yes\" runs the "
        "duplicate check and shows exactly what would be written — nothing "
        "is written until you say yes a second time. (No fund — that waits "
        "for a commitment.)",
        '• Say *"Yes"* to run the check',
        '• Say *"Skip"* to move to the next submission',
        '• Say *"Cancel"* to stop',
        f'• Handled by email already? *"mark inquiry handled endowment '
        f'{parsed.get("submission_id")}"*',
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# The task description
# ---------------------------------------------------------------------------

def task_description(data: dict, limit: int = TASK_DESCRIPTION_MAX) -> str:
    """Everything a reviewer acts on, in the order the brief sets.

    Too long: the essays are cut first, evenly, each ending in TRUNCATED.
    The organisation, EIN and contact lines are never cut.
    """
    from intents.daf_workflow import format_submitted_at

    head = [f"Organization: {data.get('organization') or 'Not given'}",
            f"EIN: {data.get('ein') or 'Not given'}",
            f"Website: {data.get('website') or 'N/A'}",
            f"Contact: {contact_name(data) or 'Not given'}",
            f"Email: {data.get('email') or 'N/A'}",
            f"Phone: {data.get('phone') or 'N/A'}",
            f"Starting goal: {data.get('starting_goal') or 'Not given'}"]
    tail = [f"How they heard: {data.get('heard_about') or 'Not given'}",
            f"Submitted: {format_submitted_at(data.get('submitted_at'))}",
            f"HubSpot: {submission_link(data.get('submission_id'))}"]
    essays = [(label, " ".join(str(data.get(key) or "").split()) or "—")
              for key, label in ESSAYS]

    def build(texts):
        body = [f"{label}: {text}" for (label, _), text in zip(essays, texts)]
        return "\n".join(head + body + tail)

    texts = [text for _label, text in essays]
    if len(build(texts)) <= limit:
        return build(texts)

    # Share what is left between the essays, the longest cut first.
    fixed = len(build(["" for _ in texts]))
    budget = max(limit - fixed, 0)
    allowances = [0] * len(texts)
    remaining, open_ = budget, list(range(len(texts)))
    while open_:
        share = remaining // len(open_)
        short = [i for i in open_ if len(texts[i]) <= share]
        if not short:
            for i in open_:
                allowances[i] = share
            break
        for i in short:
            allowances[i] = len(texts[i])
            remaining -= len(texts[i])
            open_.remove(i)

    cut = []
    for text, allowance in zip(texts, allowances):
        if len(text) <= allowance:
            cut.append(text)
            continue
        keep = max(allowance - len(TRUNCATED) - 1, 0)
        cut.append((text[:keep].rstrip() + " " + TRUNCATED).strip())
    return build(cut)


# ---------------------------------------------------------------------------
# The duplicate check, both ways
# ---------------------------------------------------------------------------

def duplicate_check(data: dict, hubspot, csuite) -> dict:
    """{"outcome", "profile_id", "lines"}. Reads only.

    a) By contact email, exactly as the DAF guard does (already_in_csuite):
       the HubSpot contact's csuite_profile_id and a trusted CSuite
       primary_email search. A match on an INDIVIDUAL profile is the person,
       not the organisation, so it is AMBIGUOUS, not existing.
    b) By organisation name against the mirror. Exact = existing. Close =
       AMBIGUOUS.
    """
    from intents.daf_workflow import DuplicateProfile, already_in_csuite
    from sync.profile_state import PROFILE_EXISTS, csuite_profile_state

    lines = []
    email_org = None
    try:
        already_in_csuite(data, hubspot, csuite)
        lines.append("• Email: no CSuite profile carries the contact's email.")
    except DuplicateProfile as duplicate:
        if duplicate.kind != "duplicate" or not duplicate.profile_id:
            lines.append(f"• Email: {duplicate.source}")
            outcome = (UNVERIFIABLE if duplicate.kind == "unverifiable"
                       else AMBIGUOUS)
            return {"outcome": outcome, "profile_id": None, "lines": lines,
                    "name": None}
        state, record = csuite_profile_state(csuite, duplicate.profile_id)
        ptype = str((record or {}).get("ptype") or "").lower()
        if state != PROFILE_EXISTS:
            lines.append(f"• Email: profile {mark_id(duplicate.profile_id)} "
                         "could not be read back.")
            return {"outcome": UNVERIFIABLE, "profile_id": None,
                    "lines": lines, "name": None}
        if ptype != "org":
            lines.append(
                f"• Email: the contact's email is on profile "
                f"{mark_id(duplicate.profile_id)}, which is "
                f"{'an INDIVIDUAL' if ptype == 'indiv' else 'not an org'} "
                "profile — the person is in CSuite; whether the "
                "organisation is cannot be told from the email.")
            return {"outcome": AMBIGUOUS, "profile_id": None, "lines": lines,
                    "name": None}
        email_org = str(duplicate.profile_id)
        lines.append(f"• Email: organisation profile {mark_id(email_org)} "
                     f"({duplicate.source}).")

    check = name_check(data.get("organization"))
    lines += _name_lines(check)
    if check.get("error"):
        return {"outcome": UNVERIFIABLE, "profile_id": None, "lines": lines,
                "name": check}
    exact = sorted({r["profile_id"] for r in check["exact"]})

    if email_org:
        if exact and email_org not in exact:
            lines.append("• The email and the name point at different "
                         "profiles — a person decides.")
            return {"outcome": AMBIGUOUS, "profile_id": None, "lines": lines,
                    "name": check}
        return {"outcome": EXISTING, "profile_id": email_org, "lines": lines,
                "name": check}
    if len(exact) == 1:
        return {"outcome": EXISTING, "profile_id": exact[0], "lines": lines,
                "name": check}
    if len(exact) > 1 or check["close"]:
        return {"outcome": AMBIGUOUS, "profile_id": None, "lines": lines,
                "name": check}
    return {"outcome": NEW, "profile_id": None, "lines": lines,
            "name": check}


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------

def _reread(state, hubspot):
    """The full submission, re-read from HubSpot, or (None, reason)."""
    from intents.daf_workflow import _parse_submission, fetch_all_submissions

    row = _row()
    wanted = str((state.get("submission_data") or {}).get("submission_id")
                 or "")
    try:
        raw, error, _complete = fetch_all_submissions(hubspot,
                                                      row["form_id"])
    except Exception as e:
        return None, str(e)
    if error:
        return None, error
    for sub in raw:
        parsed = _parse_submission(sub, row["field_map"])
        if wanted and parsed["submission_id"] == wanted:
            return parsed, None
    return None, f"submission {wanted} is no longer in HubSpot"


PLAN_KEY = "endowment_plan"


def _refuse_unassigned(state):
    from intents.daf_workflow import _reset_state

    state.pop(PLAN_KEY, None)
    _reset_state(state)
    return ("🛑 **Endowment create refused: assignee not confirmed.**\n\n"
            "CSUITE_ENDOWMENT_ASSIGNEE is not set. It waits on Carl "
            "confirming in the CSuite UI who employee 1002 is; no id is "
            "guessed. Nothing was written.")


def _check(state, hubspot, csuite):
    """(data, results, duplicate) or (None, reply, None). Reads only."""
    from intents.daf_workflow import _reset_state

    data, error = _reread(state, hubspot)
    if data is None:
        state.pop(PLAN_KEY, None)
        _reset_state(state)
        return None, (f"❌ The submission could not be re-read from HubSpot "
                      f"({error}). Nothing was written."), None

    state["csuite_api_base"] = getattr(csuite, "base_url", None)
    results = {"profile_created": False, "profile_id": None, "errors": [],
               "warnings": [], "task_id": None, "task_due": None,
               "task_failed": None, "task_skipped": None,
               "task_warning": None, "task_assignee": None,
               "task_donor": None, "csuite_api_base": state["csuite_api_base"]}
    duplicate = duplicate_check(data, hubspot, csuite)
    results["checks"] = duplicate["lines"]
    results["outcome"] = duplicate["outcome"]
    found, warnings = other_handled(data.get("email"),
                                    data.get("submission_id"), hubspot)
    results["cross_form"] = other_handled_lines(found, warnings)
    return data, results, duplicate


def _existing_id(duplicate):
    """The existing profile id as CSuite sends ids: a number."""
    found = str(duplicate.get("profile_id") or "").strip()
    return int(found) if found.isdigit() else (found or None)


def step_create(state: dict, hubspot, csuite) -> str:
    """The FIRST "yes": run the duplicate check and show the plan.

    Reads only. A stopped outcome ends here with nothing written. A new or
    existing organisation is shown with exactly what would be written, and
    the workflow waits for a second "yes" (step_write).
    """
    if not assignee():
        return _refuse_unassigned(state)

    data, results, duplicate = _check(state, hubspot, csuite)
    if data is None:
        return results

    if duplicate["outcome"] in (AMBIGUOUS, UNVERIFIABLE):
        logger.warning("endowment create STOPPED for submission %s: %s",
                       data.get("submission_id"), duplicate["outcome"])
        state.pop(PLAN_KEY, None)
        state["step"] = "done"
        return format_confirmation(data, state, results)

    # Ids and the outcome only: this lives in the session cookie.
    state[PLAN_KEY] = {"submission_id": data.get("submission_id"),
                       "outcome": duplicate["outcome"],
                       "profile_id": _existing_id(duplicate)
                       if duplicate["outcome"] == EXISTING else None}
    state["step"] = "confirm_write"
    return render_plan(data, results, duplicate)


def render_plan(data: dict, results: dict, duplicate: dict) -> str:
    """What the second "yes" would write, and nothing more."""
    from intents.daf_workflow import form_task_type, task_due_date

    org = data.get("organization") or "the organisation"
    existing = duplicate["outcome"] == EXISTING
    profile_id = _existing_id(duplicate) if existing else None
    email = (data.get("email") or "").strip()
    description = task_description(data)
    writes = 1 if existing else 2

    lines = ["🔎 **Duplicate check done — nothing has been written yet.**",
             "", f"🏛️ {org}"]
    lines += results.get("checks") or []
    lines += results.get("cross_form") or []
    lines += ["",
              (f"**Outcome: EXISTING organisation — profile "
               f"{mark_id(profile_id)}.** No new profile.") if existing else
              "**Outcome: NEW organisation.** It is not in CSuite by email "
              "or by name.",
              "", f"**A second \"yes\" will make {writes} CSuite "
                  f"write{'' if writes == 1 else 's'}:**"]
    step = 1
    if not existing:
        sent = ["organisation name", "contact email", "contact phone",
                "the organisation's address (street, city, state, zip)"]
        if data.get("website"):
            sent.append("website")
        lines += [f"{step}. `profile/create/org` — " + ", ".join(sent) + ".",
                  "   Not sent (no CSuite field takes them): the contact's "
                  "name, the EIN, the starting goal, the essays — they go in "
                  "the task."]
        step += 1
    target = (f"profile {mark_id(profile_id)}" if existing
              else "the new organisation profile")
    truncated = (", essays shortened to fit" if TRUNCATED in description
                 else "")
    lines += [f"{step}. `task/create` — *Endowment inquiry follow-up: "
              f"{org}" + (f" — {email}" if email else "") + "*",
              f"   Assigned to employee {assignee()}, task type "
              f"{form_task_type(_row())}, due "
              f"{task_due_date(data.get('submitted_at'))}, linked to "
              f"{target}.",
              "   Description: organisation, EIN, website, contact, starting "
              "goal, the three essays, how they heard, submitted date, "
              f"HubSpot link ({len(description)} characters{truncated}).",
              "",
              "No fund, no HubSpot change, no ticket.",
              "",
              "---",
              '• Say *"Yes"* to write it',
              '• Say *"Cancel"* to stop — nothing has been written',
              "The check runs again just before writing. If its answer has "
              "changed by then, nothing is written."]
    return "\n".join(lines)


def step_write(query: str, state: dict, hubspot, csuite) -> str:
    """The SECOND "yes": re-check, and write only what was approved."""
    from intents.daf_workflow import (_reset_state, endowment_refusal,
                                      says_yes)

    if not says_yes(query):
        return ('❓ Nothing has been written. Say *"Yes"* to write what is '
                'shown above, or *"Cancel"* to stop.')

    refusal = endowment_refusal()
    if refusal:
        state.pop(PLAN_KEY, None)
        _reset_state(state)
        return refusal
    employee = assignee()
    if not employee:
        return _refuse_unassigned(state)

    plan = state.get(PLAN_KEY) or {}
    shown = str((state.get("submission_data") or {}).get("submission_id")
                or "")
    if not plan or str(plan.get("submission_id") or "") != shown:
        state.pop(PLAN_KEY, None)
        _reset_state(state)
        return ("⚠️ There is no approved plan for this submission, so nothing "
                "was written. Start again with *\"new endowment\"*.")

    state["step"] = "processing"
    data, results, duplicate = _check(state, hubspot, csuite)
    if data is None:
        return results
    now_existing = (_existing_id(duplicate)
                    if duplicate["outcome"] == EXISTING else None)
    if duplicate["outcome"] != plan.get("outcome") or \
            str(now_existing) != str(plan.get("profile_id")):
        logger.warning("endowment write STOPPED for submission %s: the check "
                       "changed from %s to %s", data.get("submission_id"),
                       plan.get("outcome"), duplicate["outcome"])
        state.pop(PLAN_KEY, None)
        state["step"] = "done"
        lines = ["🛑 **Nothing was written — the duplicate check changed "
                 "since you approved it.**", "",
                 f"Approved: {plan.get('outcome')}"
                 + (f" (profile {plan.get('profile_id')})"
                    if plan.get("profile_id") else ""),
                 f"Now: {duplicate['outcome']}"
                 + (f" (profile {now_existing})" if now_existing else ""),
                 ""]
        lines += results.get("checks") or []
        lines += ["", 'Say *"new endowment"* to look at it again.']
        return "\n".join(lines)

    state.pop(PLAN_KEY, None)
    return _write(data, state, results, duplicate, csuite, employee)


def _write(data, state, results, duplicate, csuite, employee) -> str:
    """The writes, exactly as approved. Called only from step_write."""
    row = _row()
    meta = {"hubspot_form_id": row["form_id"],
            "hubspot_submission_id": data.get("submission_id")}

    if duplicate["outcome"] == EXISTING:
        state["profile_id"] = _existing_id(duplicate)
    else:
        try:
            response = csuite.create_org_profile(
                organization=data.get("organization"),
                email=data.get("email"),
                phone=data.get("phone"),
                address_line=data.get("org_address_street"),
                city=data.get("org_address_city"),
                state=data.get("org_address_state"),
                zipcode=data.get("org_address_zip"),
                audit_meta=meta,
                **({"website": data["website"]}
                   if data.get("website") else {}))
        except Exception as e:
            response = {"success": False, "error": str(e)}
        if response.get("success") and isinstance(response.get("data"), dict):
            state["profile_id"] = response["data"].get("profile_id")
            results["profile_created"] = True
            for key in ("phone_warning", "address_warning", "verify_warning"):
                if response.get(key):
                    results["warnings"].append(response[key])
            if response.get("nothing_stored"):
                results["errors"].append(
                    f"CSuite reported success on {state['profile_id']} but "
                    "stored NOTHING. Treat this profile as empty.")
            elif response.get("fields_dropped"):
                results["errors"].append(
                    f"Profile {state['profile_id']} created but CSuite did "
                    f"not store: {', '.join(sorted(response['fields_dropped']))}")
            logger.info("CSuite org profile %s created for endowment "
                        "submission %s", state["profile_id"],
                        data.get("submission_id"))
        else:
            results["errors"].append(
                f"Profile creation: {response.get('error') or 'Unknown error'}")
            logger.error("CSuite org profile NOT created for endowment "
                         "submission %s", data.get("submission_id"))

    if state.get("profile_id"):
        _create_task(data, state, results, csuite, employee, meta)
    else:
        results["task_skipped"] = "no CSuite profile to attach it to"

    state["step"] = "done"
    return format_confirmation(data, state, results)


def _create_task(data, state, results, csuite, employee, meta):
    from intents.daf_workflow import form_task_type, task_due_date

    row = _row()
    org = data.get("organization") or "unnamed organisation"
    email = (data.get("email") or "").strip()
    subject = (f"Endowment inquiry follow-up: {org}"
               + (f" — {email}" if email else ""))
    due = task_due_date(data.get("submitted_at"))
    try:
        response = csuite.create_task(
            name=subject, employee_id=employee, due_date=due,
            description=task_description(data),
            linked_profile_id=state["profile_id"],
            task_type_id=form_task_type(row),
            audit_meta=dict(meta, linked_profile_id=state["profile_id"]))
    except Exception as e:
        results["task_failed"] = str(e)
        logger.error("CSuite endowment task NOT created on profile %s",
                     state["profile_id"])
        return
    if response.get("success") and isinstance(response.get("data"), dict):
        results["task_id"] = response["data"].get("task_id")
        results["task_due"] = due
        results["task_warning"] = response.get("task_warning")
        results["task_assignee"] = (response.get("assignee_name")
                                    or str(employee))
        results["task_donor"] = org
        logger.info("CSuite endowment task %s created on profile %s, due %s",
                    results["task_id"], state["profile_id"], due)
    else:
        results["task_failed"] = (response.get("error")
                                  or "CSuite returned no task id")
        logger.error("CSuite endowment task NOT created on profile %s",
                     state["profile_id"])


NOT_ON_THE_PROFILE = ("Not on the CSuite profile, because no confirmed input "
                      "takes them: the contact's name, the EIN, the starting "
                      "goal and the essays. They are in the task "
                      "description.")


def format_confirmation(data: dict, state: dict, results: dict) -> str:
    from intents.daf_workflow import _task_lines

    org = data.get("organization") or "the organisation"
    outcome = results.get("outcome")
    profile_id = state.get("profile_id")
    lines = []
    if outcome in (AMBIGUOUS, UNVERIFIABLE):
        lines += [f"🛑 **No profile and no task created — "
                  f"{'a duplicate could not be ruled out' if outcome == UNVERIFIABLE else 'ambiguous'}**",
                  "", f"🏛️ {org}"]
        lines += results.get("checks") or []
        lines += results.get("cross_form") or []
        lines += ["", "Nothing was written. Check CSuite by hand, then create "
                      "it there — or, if it was handled by email, say "
                      f'*"mark inquiry handled endowment '
                      f'{data.get("submission_id")}"*.',
                  "", "Say anything to continue, or start a new workflow."]
        return "\n".join(lines)

    link = (ui_url(Config.CSUITE_PROFILE_URL, state.get("csuite_api_base"),
                   profile_id=profile_id) if profile_id else None)
    if outcome == EXISTING:
        lines += ["♻️ **Already in CSuite — no new profile created**", "",
                  f"🏛️ {org} — profile {mark_id(profile_id)} — "
                  f"[CSuite]({link})"]
    elif results.get("profile_created"):
        heading = ("✅ **Endowment Inquiry — Organisation Profile Created**"
                   + (" (with warnings)" if results["errors"] else ""))
        lines += [heading, "",
                  f"🏛️ {org} — [CSuite]({link})"]
    else:
        lines += ["❌ **Endowment NOT Created**", "",
                  "❌ Profile: Failed to create. The submission is still "
                  "unhandled, so it will be offered again."]
    for warning in results.get("warnings") or []:
        lines.append(warning)
    if results.get("profile_created"):
        lines.append(f"ℹ️ {NOT_ON_THE_PROFILE}")
    lines += results.get("checks") or []
    lines += results.get("cross_form") or []
    lines.append("💰 Fund: not opened — a fund is created when the "
                 "organisation commits, not at inquiry.")
    lines.append("🎯 HubSpot: not changed (the contact's CSuite link is the "
                 "person's, not the organisation's).")
    lines.extend(_task_lines(data, results))
    if results["errors"]:
        lines += ["", "**Issues:**"] + [f"• ⚠️ {e}" for e in results["errors"]]
    lines += ["", "Say anything to continue, or start a new workflow."]
    return "\n".join(lines)
