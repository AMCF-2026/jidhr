"""
"mark inquiry handled <form> <submission id> [note]" (V5.81)
=============================================================
Ola often answers an inquiry by email and never needs a CSuite task. Without
a record that says so, the submission is offered again forever: "handled"
means a write_audit row carries its id (clients.audit.processed_submissions).

This writes ONE such row, after a "yes" in chat:

    target_system  'jidhr'   (local bookkeeping; migrations/007)
    endpoint       'submission/link/marked_handled'
    payload        hubspot_form_id, hubspot_submission_id, actor_user_id

and nothing else: no CSuite request, no HubSpot request, no task. The note
is shown back and NOT stored — payload_meta keeps ids only, and a note is
free text that could carry anything. The form id is stored because the
processed history is read per form; without it the row would count for none.

Idempotent: a submission that is already handled is reported as such and
nothing is written.
"""

import logging

from config import Config

logger = logging.getLogger(__name__)

MARK_HANDLED_PHRASES = ["mark inquiry handled"]
MARKED_HANDLED = "marked_handled"
MIN_ID_PREFIX = 8


def _parse(query: str):
    """(key, submission id or prefix, note) from the command, case kept."""
    text = (query or "").strip()
    lowered = text.lower()
    for phrase in MARK_HANDLED_PHRASES:
        at = lowered.find(phrase)
        if at >= 0:
            text = text[at + len(phrase):]
            break
    parts = text.split(None, 2)
    key = parts[0].lower() if parts else ""
    sid = parts[1] if len(parts) > 1 else ""
    note = parts[2].strip() if len(parts) > 2 else ""
    return key, sid, note


def _switch_refusal(row):
    from intents.daf_workflow import endowment_refusal, form_switch

    if row["key"] == "endowment":
        return endowment_refusal()
    if not form_switch(row, "create_flag"):
        return (f"⏸️ {row['label']} inquiries are turned off "
                f"({row['create_flag']}). Nothing was written.")
    return None


def _find(submissions, wanted):
    """(match, reason). Exact id, or a unique prefix of MIN_ID_PREFIX+."""
    exact = [s for s in submissions if s["submission_id"] == wanted]
    if exact:
        return exact[0], None
    if len(wanted) < MIN_ID_PREFIX:
        return None, (f"give the whole submission id, or at least its first "
                      f"{MIN_ID_PREFIX} characters")
    hits = [s for s in submissions if s["submission_id"].startswith(wanted)]
    if len(hits) == 1:
        return hits[0], None
    if not hits:
        return None, "no submission on this form has that id"
    return None, f"{len(hits)} submissions start with that — give more of it"


def start(query: str, state: dict, hubspot, actor_user_id) -> str:
    """Find the submission and ask for confirmation. Writes nothing."""
    from clients.audit import ProcessedHistoryUnavailable  # noqa: F401
    from intents.daf_workflow import (_parse_submission, _processed_map,
                                      fetch_all_submissions,
                                      format_submitted_at, handled_endpoints,
                                      inquiry_form)
    from sync.contact_create import mask_email
    from sync.daf_reconcile import link_rows_allowed

    key, wanted, note = _parse(query)
    row = inquiry_form(key)
    if row is None:
        keys = ", ".join(f"`{r['key']}`" for r in Config.INQUIRY_FORMS)
        return ("❓ Say *\"mark inquiry handled <form> <submission id> "
                f"[note]\"*, where <form> is one of: {keys}. Nothing was "
                "written.")
    refusal = _switch_refusal(row)
    if refusal:
        return refusal
    if not wanted:
        return ("❓ Which submission? Add its id — the review shows it as "
                "🆔. Nothing was written.")
    if not actor_user_id:
        return "🛑 Who is asking could not be told. Nothing was written."
    try:
        if not link_rows_allowed():
            return ("🛑 write_audit does not admit target_system 'jidhr' — "
                    "run migrations/007_write_audit_jidhr_target.sql. "
                    "Nothing was written.")
    except Exception as e:
        return f"❌ The audit table could not be checked ({e}). Nothing was written."

    raw, error, _complete = fetch_all_submissions(hubspot, row["form_id"])
    if error:
        return (f"❌ The {row['label']} submissions could not be read "
                f"({error}). Nothing was written.")
    parsed = [_parse_submission(s, row["field_map"]) for s in raw]
    match, reason = _find(parsed, wanted)
    if match is None:
        return f"❓ Not marked: {reason}. Nothing was written."

    processed, warning = _processed_map(row["form_id"], handled_endpoints(row))
    if warning:
        return ("🛑 The processed history could not be read, so whether this "
                "is already handled cannot be told. Nothing was written.")
    sid = match["submission_id"]
    if sid in processed:
        return (f"✅ {row['label']} submission `{sid}` is already handled. "
                "Nothing was written.")

    from intents.daf_workflow import _reset_state

    _reset_state(state)
    state.update({"active": True, "workflow_type": "daf", "type": row["key"],
                  "step": "confirm_mark", "form_id": row["form_id"],
                  "submission_data": {"submission_id": sid},
                  "mark_actor": actor_user_id})
    who = match.get("organization") or ""
    lines = [f"📌 **Mark this {row['label']} submission handled, with no "
             "task?**", "",
             f"🆔 `{sid}`",
             f"📅 Submitted {format_submitted_at(match.get('submitted_at'))}",
             f"📧 {mask_email(match.get('email'))}" + (f" — {who}" if who
                                                       else "")]
    if note:
        lines.append(f"🗒️ Note (shown here only; it is NOT stored): {note}")
    lines += ["",
              "This writes ONE row to write_audit — the form id, the "
              "submission id and your user id — and nothing to CSuite or "
              "HubSpot. The submission is then never offered again.",
              "", '• Say *"Yes"* to record it', '• Say *"Cancel"* to stop']
    return "\n".join(lines)


def confirm(query: str, state: dict) -> str:
    """The "yes" step. One write_audit row, or none."""
    from clients.audit import (LINK_TARGET_SYSTEM, SUBMISSION_LINK_ENDPOINT,
                               AuditUnavailable, record_write)
    from intents.daf_workflow import (_processed_map, _reset_state,
                                      handled_endpoints, inquiry_form,
                                      says_yes)

    if not says_yes(query):
        return ('❓ Say *"Yes"* to record it as handled, or *"Cancel"*.')
    row = inquiry_form(state.get("type"))
    sid = str((state.get("submission_data") or {}).get("submission_id") or "")
    actor = state.get("mark_actor")
    _reset_state(state)
    if row is None or not sid or not actor:
        return "⚠️ The mark request was incomplete. Nothing was written."

    # Re-checked at the moment of writing: a second "yes", or someone else
    # handling it in between, must not make a second row.
    processed, warning = _processed_map(row["form_id"], handled_endpoints(row))
    if warning:
        return ("🛑 The processed history could not be read, so whether this "
                "is already handled cannot be told. Nothing was written.")
    if sid in processed:
        return (f"✅ {row['label']} submission `{sid}` is already handled. "
                "Nothing was written.")
    try:
        record_write(LINK_TARGET_SYSTEM, "LINK",
                     f"{SUBMISSION_LINK_ENDPOINT}/{MARKED_HANDLED}",
                     target_id=None,
                     payload={"hubspot_form_id": row["form_id"],
                              "hubspot_submission_id": sid,
                              "actor_user_id": actor},
                     status="success")
    except AuditUnavailable as e:
        logger.error("mark handled NOT recorded for submission %s: %s", sid,
                     e)
        return f"❌ Not recorded ({e}). Nothing was written."
    logger.info("submission %s on form %s marked handled by user %s", sid,
                row["form_id"], actor)
    return (f"✅ {row['label']} submission `{sid}` recorded as handled, with "
            "no task. It will not be offered again. One write_audit row; "
            "nothing sent to CSuite or HubSpot.")
