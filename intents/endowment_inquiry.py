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
  organisation's address, website, and the EIN as `fedid` (V5.86).
* For an EXISTING organisation, at most one `profile/edit` that sets ONLY
  `fedid`, and only when the profile has none and the form has one. A
  different EIN already on the profile is never overwritten: nothing is
  written to the profile and the mismatch is shown. (V5.86)
* One `task/create`, assigned to CSUITE_ENDOWMENT_ASSIGNEE, task type 1065,
  linked to the new or existing org profile. A CSuite task holds ONE text,
  at most 255 characters (sandbox, 2026-10-15: `task_description` is ignored
  when `name` is sent, and a 256-character name is refused), so the task is
  a title — "Endowment inquiry: {org} — EIN {ein} — goal {goal} — {contact}
  ({email})" — sent as both name and task_description, as DAF does. The
  contact's name and the nonprofit flag cannot be set on the profile through
  the API at all (see clients.csuite.ORG_DISPLAY_ONLY).
* One `note/create` on the org profile (V5.87): the three essays, the full
  starting goal, how they heard, and the HubSpot link, first line "Added by
  Jidhr from HubSpot submission {id}" because CSuite shows the API user as
  the author. Sent once and never retried; CSuite answers success: 0 with a
  note_id for a note it did create, and notes cannot be read back by this
  API user, so the result says so.
* Never a fund (deferred, as for DAF). Never the HubSpot contact: its
  csuite_profile_id is the PERSON's link, and an org id there would be
  wrong. Never a ticket: the HubSpot workflow creates none (Carl,
  2026-10-14).

The EIN, the goal and the essays are never logged, never put in write_audit
(payload_meta keeps key names and ids only, and `fedid` is not an id name)
and never kept in the session cookie: the create step re-reads the
submission from HubSpot. (create_task logs the title's length, not its
text, since the title now carries the EIN and the goal.)

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

# A CSuite task's only text. MEASURED in the sandbox 2026-10-15: 255
# characters stored identically (263 bytes, em dashes included); 256 refused
# with "name: must not be longer than 255". Characters, not bytes.
TASK_TITLE_MAX = 255
GOAL_TRIMMED_TO = 40
ELLIPSIS = "…"
REVIEW_ESSAY_PREVIEW = 200

# How the four outcomes of the duplicate check are named.
NEW, EXISTING, AMBIGUOUS, UNVERIFIABLE = ("new", "existing", "ambiguous",
                                          "unverifiable")

# What happens to an EXISTING organisation's EIN (fedid). Never overwrite.
EIN_SET = "set"                # profile has none, form has one: one edit
EIN_SAME = "same"              # profile already holds the form's EIN
EIN_MISMATCH = "mismatch"      # profile holds a DIFFERENT EIN: no write
EIN_NO_FORM_VALUE = "no_form_value"   # form has no EIN: nothing to do
EIN_UNREADABLE = "unreadable"  # the profile could not be read: no write


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
# The task title — the task's only text
# ---------------------------------------------------------------------------

def _one_line(text) -> str:
    return " ".join(str(text or "").split())


def _title_safe(text) -> str:
    """One line, with no < or >.

    CSuite's task page renders the text as HTML, so anything in angle
    brackets disappears there. Production task 1067 (2026-10-15) stored
    "... — Aftab Banth <abanth@...>" in full, and the page showed it ending
    at the name: the email was read as a tag.
    """
    return _one_line(str(text or "").replace("<", "").replace(">", ""))


def _shorten(text: str, length: int) -> str:
    """`text` cut to `length` characters, ending in an ellipsis if cut."""
    if len(text) <= length:
        return text
    return text[:max(length - 1, 0)].rstrip() + ELLIPSIS


def note_text(data: dict) -> str:
    """The note on the org profile: what the 255-character task cannot hold.

    Each section starts with a bracketed heading, so it still reads as
    sections if CSuite ever collapses the line breaks.
    """
    from intents.daf_workflow import format_submitted_at

    sid = data.get("submission_id")
    lines = [f"Added by Jidhr from HubSpot submission {sid}",
             f"Endowment inquiry — submitted "
             f"{format_submitted_at(data.get('submitted_at'))}",
             f"HubSpot: {submission_link(sid)}",
             "",
             f"[Starting goal] {_one_line(data.get('starting_goal')) or 'Not given'}"]
    for key, label in ESSAYS:
        lines += ["", f"[{label}]",
                  str(data.get(key) or "").strip() or "Not given"]
    lines += ["", f"[How they heard] "
                  f"{_one_line(data.get('heard_about')) or 'Not given'}"]
    return "\n".join(lines)


def task_title(data: dict, limit: int = TASK_TITLE_MAX) -> str:
    """Endowment inquiry: {org} — EIN {ein} — goal {goal} — {name} ({email})

    Over `limit`: the goal is cut to GOAL_TRIMMED_TO, then the organisation
    name, then the contact's name — each only as far as needed. The EIN and
    the email are never cut. (If those two alone exceed the limit, CSuite
    refuses the task and the reply says so; nothing is invented to fit.)
    """
    # No < or > in any field: the task page renders HTML (see _title_safe).
    org = _title_safe(data.get("organization")) or "unnamed organisation"
    ein = _title_safe(data.get("ein")) or "not given"
    goal = _title_safe(data.get("starting_goal")) or "not given"
    who = _title_safe(contact_name(data)) or "no name given"
    email = _title_safe(data.get("email")) or "no email"

    def build():
        return (f"Endowment inquiry: {org} — EIN {ein} — goal {goal} — "
                f"{who} ({email})")

    title = build()
    if len(title) > limit:
        goal = _shorten(goal, GOAL_TRIMMED_TO)
        title = build()
    if len(title) > limit:
        org = _shorten(org, max(len(org) - (len(title) - limit), 1))
        title = build()
    if len(title) > limit:
        who = _shorten(who, max(len(who) - (len(title) - limit), 1))
        title = build()
    return title


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


def _ein_digits(value) -> str:
    """Digits only, so 12-3456789 and 123456789 compare equal."""
    import re

    return re.sub(r"\D", "", str(value or ""))


def ein_plan(data: dict, duplicate: dict, csuite) -> dict:
    """{"action", "stored"} for an EXISTING organisation's EIN. One read.

    `stored` is what the profile holds now, for the reviewer. The guard is
    here, at plan time, and is evaluated again at the second "yes": a value
    on the profile — any value — is never overwritten.
    """
    from sync.profile_state import PROFILE_EXISTS, csuite_profile_state

    form = (data.get("ein") or "").strip()
    if duplicate.get("outcome") != EXISTING:
        return {"action": None, "stored": None}
    if not _ein_digits(form):
        return {"action": EIN_NO_FORM_VALUE, "stored": None}
    state, record = csuite_profile_state(csuite,
                                         _existing_id(duplicate))
    if state != PROFILE_EXISTS:
        return {"action": EIN_UNREADABLE, "stored": None}
    stored = str((record or {}).get("fedid") or "").strip()
    if not stored:
        return {"action": EIN_SET, "stored": None}
    if _ein_digits(stored) == _ein_digits(form):
        return {"action": EIN_SAME, "stored": stored}
    return {"action": EIN_MISMATCH, "stored": stored}


def ein_lines(data: dict, ein: dict, profile_id) -> list:
    """The EIN line(s) for the plan and the confirmation."""
    action = (ein or {}).get("action")
    form = (data.get("ein") or "").strip()
    if action == EIN_SET:
        return [f"• EIN: profile {mark_id(profile_id)} has none — the form's "
                f"EIN {form} will be set."]
    if action == EIN_SAME:
        return [f"• EIN: profile {mark_id(profile_id)} already holds {form}. "
                "No change."]
    if action == EIN_MISMATCH:
        return [f"• ⚠️ EIN MISMATCH: profile {mark_id(profile_id)} holds "
                f"{ein.get('stored')}, the form says {form}. Nothing is "
                "written to the profile — check which is right by hand."]
    if action == EIN_UNREADABLE:
        return [f"• ⚠️ EIN: profile {mark_id(profile_id)} could not be read, "
                "so its EIN is unknown. Nothing is written to the profile."]
    if action == EIN_NO_FORM_VALUE:
        return ["• EIN: the form has none. Nothing is written to the "
                "profile."]
    return []


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
    results["ein"] = ein_plan(data, duplicate, csuite)
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

    if duplicate["outcome"] == EXISTING:
        # V5.95: shown now so the plan is honest; checked again at the write.
        from sync.open_tasks import open_tasks_on_profile

        found, problem = open_tasks_on_profile(csuite, _existing_id(duplicate))
        results["open_task"] = found[0] if found else None
        results["open_task_problem"] = problem

    # Ids and the outcome only: this lives in the session cookie.
    state[PLAN_KEY] = {"submission_id": data.get("submission_id"),
                       "outcome": duplicate["outcome"],
                       "profile_id": _existing_id(duplicate)
                       if duplicate["outcome"] == EXISTING else None,
                       "ein_action": results["ein"]["action"]}
    state["step"] = "confirm_write"
    return render_plan(data, results, duplicate)


def render_plan(data: dict, results: dict, duplicate: dict) -> str:
    """What the second "yes" would write, and nothing more."""
    from intents.daf_workflow import form_task_type, task_due_date

    org = data.get("organization") or "the organisation"
    existing = duplicate["outcome"] == EXISTING
    profile_id = _existing_id(duplicate) if existing else None
    email = (data.get("email") or "").strip()
    title = task_title(data)
    ein_action = (results.get("ein") or {}).get("action")
    sets_ein = existing and ein_action == EIN_SET
    open_task = results.get("open_task") if existing else None
    # The profile step (create, or an EIN edit), the note, the task — no task
    # when the existing profile already has an open one (V5.95).
    writes = ((2 + (1 if sets_ein else 0)) if existing else 3) - \
        (1 if open_task else 0)
    note = note_text(data)

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
        if _ein_digits(data.get("ein")):
            sent.append(f"EIN {data['ein'].strip()} (as fedid)")
        lines += [f"{step}. `profile/create/org` — " + ", ".join(sent) + ".",
                  "   Not sent (the API cannot set them): the contact's "
                  "name and the nonprofit flag."]
        if not _ein_digits(data.get("ein")):
            lines.append("   The form has no EIN, so none is sent.")
        step += 1
    else:
        lines += ein_lines(data, results.get("ein"), profile_id)
        if sets_ein:
            lines.append(f"{step}. `profile/edit` on profile "
                         f"{mark_id(profile_id)} — sets ONLY fedid (the EIN). "
                         "Nothing else on the profile changes.")
            step += 1
    target = (f"profile {mark_id(profile_id)}" if existing
              else "the new organisation profile")
    lines += [f"{step}. `note/create` on {target} — the three essays, the "
              f"full starting goal, how they heard and the HubSpot link "
              f"({len(note)} characters). First line: \"Added by Jidhr from "
              f"HubSpot submission {data.get('submission_id')}\".",
              "   Sent once, never retried. Notes cannot be read back through "
              "the API, so CSuite's copy will not be checked."]
    step += 1
    if open_task:
        from sync import open_tasks as ot

        verdict, _days = ot.judge(open_task, data.get("submitted_at"))
        then = ("it will be recorded as handled" if verdict == ot.WITHIN
                else "it is more than 30 days older than this inquiry, so "
                     "the submission will be HELD for Carl")
        lines += [f"No `task/create`: profile {mark_id(profile_id)} already "
                  f"has an {ot.describe(open_task)}; {then}. No second "
                  "task.", ""]
    else:
        lines += [f"{step}. `task/create` — {target}:",
                  f"   *{title}*",
                  f"   ({len(title)} of {TASK_TITLE_MAX} characters — a "
                  f"CSuite task holds this one line and nothing else.) "
                  f"Assigned to employee {assignee()}, task type "
                  f"{form_task_type(_row())}, due "
                  f"{task_due_date(data.get('submitted_at'))}."]
        if existing and results.get("open_task_problem"):
            lines.append(f"   ⚠️ CSuite couldn't be checked for an open task "
                         f"on this profile ({results['open_task_problem']}). "
                         "It is checked again before writing; if it still "
                         "can't be, no task is created.")
        lines.append("")
    lines += [
              "The submission itself stays in HubSpot: "
              f"{submission_link(data.get('submission_id'))}",
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
    now_ein = (results.get("ein") or {}).get("action")
    if duplicate["outcome"] != plan.get("outcome") or \
            str(now_existing) != str(plan.get("profile_id")) or \
            now_ein != plan.get("ein_action"):
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
        if now_ein != plan.get("ein_action"):
            lines += [f"EIN step approved: {plan.get('ein_action')}; now: "
                      f"{now_ein}.", ""]
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
        if (results.get("ein") or {}).get("action") == EIN_SET:
            _set_ein(data, state, results, csuite)
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
                   if data.get("website") else {}),
                **({"fedid": data["ein"].strip()}
                   if _ein_digits(data.get("ein")) else {}))
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
        _create_note(data, state, results, csuite, meta)
        _create_task(data, state, results, csuite, employee, meta)
    else:
        results["note_skipped"] = True
        results["task_skipped"] = "no CSuite profile to attach it to"

    state["step"] = "done"
    return format_confirmation(data, state, results)


def _set_ein(data, state, results, csuite):
    """The one guarded profile write on an existing organisation: fedid only.

    Reached only when the plan said EIN_SET and the check at the second "yes"
    still says so — the profile had NO EIN a moment ago. A failure is
    reported and does not stop the task.
    """
    profile_id = state["profile_id"]
    try:
        response = csuite.edit_profile(profile_id,
                                       fedid=data["ein"].strip())
    except Exception as e:
        response = {"success": False, "error": str(e)}
    if response.get("success") and not response.get("fields_dropped") \
            and not response.get("nothing_stored"):
        results["ein_written"] = True
        if response.get("verify_warning"):
            results["warnings"].append(response["verify_warning"])
        logger.info("EIN set on existing org profile %s", profile_id)
    else:
        reason = (response.get("error")
                  or ("CSuite stored nothing" if response.get("nothing_stored")
                      else "CSuite did not store fedid"
                      if response.get("fields_dropped") else "unknown"))
        results["errors"].append(
            f"EIN NOT set on profile {profile_id} ({reason}). Set it by hand.")
        logger.error("EIN NOT set on existing org profile %s", profile_id)


def _create_note(data, state, results, csuite, meta):
    """ONE note/create. Never retried, whatever comes back.

    A note that may exist must not be sent twice: CSuite has no idempotency
    key, and it reports a created note as success: 0 (the client reads the
    note_id instead). A failure or an exception is reported, and the task
    still goes ahead.
    """
    profile_id = state["profile_id"]
    try:
        response = csuite.create_note(profile_id, note_text(data),
                                      audit_meta=meta)
    except Exception as e:
        results["note_failed"] = (
            f"the request did not complete ({e}), so the note may or may not "
            "exist — it was NOT retried. Check the profile's notes in CSuite "
            "before adding it by hand")
        logger.error("CSuite note on profile %s: outcome unknown, not "
                     "retried", profile_id)
        return
    note_id = ((response.get("data") or {}).get("note_id")
               if isinstance(response.get("data"), dict) else None)
    if response.get("success") and note_id:
        results["note_id"] = note_id
        results["note_warning"] = response.get("note_warning")
        logger.info("CSuite note %s added to profile %s", note_id, profile_id)
    else:
        error = response.get("error")
        # "Unknown error" is the client's word for a success: 0 that named
        # no error. For a note that means no note_id came back either.
        results["note_failed"] = (error if error and error != "Unknown error"
                                  else "CSuite returned no note id")
        logger.error("CSuite note NOT added to profile %s", profile_id)


def _create_task(data, state, results, csuite, employee, meta):
    from intents.daf_workflow import form_task_type, task_due_date

    row = _row()
    org = data.get("organization") or "unnamed organisation"
    # The task's only text, sent as both name and task_description, as DAF
    # does: CSuite keeps one of them and ignores task_description when name
    # is present (sandbox, 2026-10-15).
    title = task_title(data)
    due = task_due_date(data.get("submitted_at"))
    if not results.get("profile_created"):
        # V5.95: an existing organisation may already have an open task.
        from sync.open_tasks import open_tasks_on_profile

        found, problem = open_tasks_on_profile(csuite, state["profile_id"])
        if problem:
            results["task_check_failed"] = problem
            results["task_skipped"] = (
                f"CSuite couldn't be checked for an open task on profile "
                f"{state['profile_id']} ({problem}), so none was created")
            return
        if found:
            # V5.95.1: within 30 days, recorded as handled; older, held.
            from sync import open_tasks as ot

            task = found[0]
            verdict, days = ot.judge(task, data.get("submitted_at"))
            recorded = ot.record(verdict, meta["hubspot_form_id"],
                                 data.get("submission_id"),
                                 state["profile_id"], task)
            text = ot.outcome_text(verdict, task, data.get("submitted_at"),
                                   recorded=recorded)
            results["task_existing"] = dict(task, verdict=verdict, days=days,
                                            recorded=recorded, text=text)
            results["task_skipped"] = text
            logger.info("endowment task NOT created: profile %s has open "
                        "task %s (%s days, %s)", state["profile_id"],
                        task["task_id"], days, verdict)
            return
    try:
        response = csuite.create_task(
            name=title, employee_id=employee, due_date=due,
            description=title,
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


def _note_lines(data: dict, results: dict) -> list:
    if results.get("note_id"):
        out = [f"🗒️ Note {results['note_id']} added to the profile: the "
               "essays, the full starting goal, how they heard and the "
               "HubSpot link."]
        if results.get("note_warning"):
            out.append(f"   {results['note_warning']}")
        return out
    if results.get("note_failed"):
        return [f"⚠️ Note NOT added ({results['note_failed']}). The essays "
                f"are in HubSpot: {submission_link(data.get('submission_id'))}"]
    return []


NOT_ON_THE_PROFILE = ("Not on the CSuite profile, because the API cannot set "
                      "them: the contact's name and the nonprofit flag.")


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
        if _ein_digits(data.get("ein")):
            lines.append(f"🧾 EIN {data['ein'].strip()} set on the new "
                         "profile (fedid).")
        lines.append(f"ℹ️ {NOT_ON_THE_PROFILE}")
    if outcome == EXISTING:
        if results.get("ein_written"):
            lines.append(f"🧾 EIN {data['ein'].strip()} set on profile "
                         f"{mark_id(profile_id)} (it had none). Nothing else "
                         "on the profile changed.")
        elif (results.get("ein") or {}).get("action") != EIN_SET:
            lines += ein_lines(data, results.get("ein"), profile_id)
    lines += _note_lines(data, results)
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
