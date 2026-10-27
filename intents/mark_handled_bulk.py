"""
"mark handled <form> before <date>" (V5.96)
===========================================
Records the historic, unhandled submissions on a form as handled, so the
chat stops offering them — 72 DAF submissions before 2026-10-19 on
2026-10-27. Stateless: two separate commands, nothing in the session cookie.

    mark handled daf before 2026-10-19
        PREVIEW. Reads only. Lists every submission it would mark, and names
        the HOLD-BACK ids it will never mark.

    mark handled daf before 2026-10-19 exclude 36342dcd, ... apply
        Writes one jidhr row per listed submission
        (submission/link/bulk_marked_handled: form id, submission id, actor).
        Refuses unless the exclude list is EXACTLY the hold-back list, so
        nobody applies it without having read which ones are held back.

Never touches a submission on or after the date: "before" is strict, by the
submission's UTC moment. Already handled or held submissions are skipped.
The processed history is re-read immediately before writing. A failed row
stops the run where it is; nothing is retried.

HOLD_BACK is Carl's decision (2026-10-27): Munira's list — the seven $25k+
inquiries with no CSuite profile or task — plus 1a3fbc12 (a profile, but no
task and no gift since) and the three $50,001-$100,000 inquiries with no
profile. They wait for Munira, not a bulk mark.
"""

import logging
import re
from datetime import date, datetime, timezone

from config import Config

logger = logging.getLogger(__name__)

BULK_PHRASES = ["mark handled"]
BULK_MARKED = "bulk_marked_handled"
MAX_ROWS = 200

HOLD_BACK = {
    "daf": ("36342dcd", "07a1774d", "b0c6eb0a", "2fcd52ac",   # Munira's list
            "3a1ab06c", "40ccb6e0", "196de003",
            "1a3fbc12",                                       # profile, no task
            "ab4918a6", "3070e096", "14602884"),              # $50k-$100k
}

_COMMAND = re.compile(
    r"mark handled\s+(?P<key>\w+)\s+before\s+(?P<date>\d{4}-\d{2}-\d{2})"
    r"(?:\s+exclude\s+(?P<exclude>[0-9a-f]{8}(?:[\s,]+[0-9a-f]{8})*))?"
    r"(?P<apply>\s+apply)?\s*$")


def is_bulk(query: str) -> bool:
    text = (query or "").lower().strip()
    return text.startswith("mark handled ") and " before " in text


def parse(query: str):
    """(key, cutoff date, exclude tuple, apply) or raises ValueError."""
    m = _COMMAND.match((query or "").lower().strip())
    if not m:
        raise ValueError(
            'say *"mark handled <form> before YYYY-MM-DD"* to preview, then '
            '*"... exclude <ids> apply"* to record')
    cutoff = date.fromisoformat(m.group("date"))
    exclude = tuple(x for x in re.split(r"[\s,]+", m.group("exclude") or "")
                    if x)
    return m.group("key"), cutoff, exclude, bool(m.group("apply"))


def plan(row, cutoff, raw, processed, held) -> dict:
    """Which submissions a bulk mark would record. No calls."""
    from intents.daf_workflow import _parse_submission, _submitted_moment

    hold_back = set(HOLD_BACK.get(row["key"], ()))
    floor = datetime(cutoff.year, cutoff.month, cutoff.day,
                     tzinfo=timezone.utc)
    out = {"mark": [], "held_back": [], "already": 0, "on_or_after": 0,
           "undated": 0}
    for sub in raw:
        data = _parse_submission(sub, row["field_map"])
        sid = str(data.get("submission_id") or "")
        moment = _submitted_moment(data.get("submitted_at"))
        if moment is None:
            out["undated"] += 1           # never guessed into the range
            continue
        if moment >= floor:
            out["on_or_after"] += 1
            continue
        if sid in processed or sid in held:
            out["already"] += 1
            continue
        entry = {"submission_id": sid, "submitted": moment.date().isoformat()}
        if sid[:8] in hold_back:
            out["held_back"].append(entry)
        else:
            out["mark"].append(entry)
    for key in ("mark", "held_back"):
        out[key].sort(key=lambda e: (e["submitted"], e["submission_id"]))
    out["hold_back_ids"] = sorted(hold_back)
    return out


def handle(query: str, hubspot, actor_user_id) -> str:
    from clients.audit import (LINK_TARGET_SYSTEM, SUBMISSION_LINK_ENDPOINT,
                               AuditUnavailable, held_submissions,
                               record_write)
    from intents.daf_workflow import (_processed_map, fetch_all_submissions,
                                      handled_endpoints, inquiry_form)
    from intents.mark_handled import _switch_refusal
    from sync.daf_reconcile import link_rows_allowed

    try:
        key, cutoff, exclude, apply = parse(query)
    except ValueError as e:
        return f"❓ {str(e)[:1].upper()}{str(e)[1:]}. Nothing was written."
    row = inquiry_form(key)
    if row is None:
        keys = ", ".join(f"`{r['key']}`" for r in Config.INQUIRY_FORMS)
        return f"❓ <form> is one of: {keys}. Nothing was written."
    if cutoff > datetime.now(timezone.utc).date():
        return "🛑 The date is in the future. Nothing was written."
    refusal = _switch_refusal(row)
    if refusal:
        return refusal
    if apply and not actor_user_id:
        return "🛑 Who is asking could not be told. Nothing was written."

    raw, error, complete = fetch_all_submissions(hubspot, row["form_id"])
    if error or not complete:
        return (f"❌ The {row['label']} submissions could not all be read "
                f"({error or 'partial read'}). Nothing was written.")
    processed, warning = _processed_map(row["form_id"], handled_endpoints(row))
    if warning:
        return ("🛑 The processed history could not be read, so what is "
                "already handled cannot be told. Nothing was written.")
    try:
        held = held_submissions(row["form_id"])
    except Exception as e:
        return (f"🛑 The held list could not be read ({type(e).__name__}). "
                "Nothing was written.")
    out = plan(row, cutoff, raw, processed, held)

    if not apply:
        return _preview(row, cutoff, out)

    expected = set(out["hold_back_ids"])
    if set(exclude) != expected:
        missing = sorted(expected - set(exclude))
        extra = sorted(set(exclude) - expected)
        return ("🛑 **Not recorded.** The exclude list must be exactly the "
                f"{len(expected)} held-back ids the preview names."
                + (f" Missing: {', '.join(missing)}." if missing else "")
                + (f" Not on the hold-back list: {', '.join(extra)}."
                   if extra else "") + " Nothing was written.")
    if len(out["mark"]) > MAX_ROWS:
        return (f"🛑 {len(out['mark'])} rows is over the {MAX_ROWS} limit for "
                "one command. Nothing was written.")
    try:
        if not link_rows_allowed():
            return ("🛑 write_audit does not admit target_system 'jidhr'. "
                    "Nothing was written.")
    except Exception as e:
        return (f"❌ The audit table could not be checked "
                f"({type(e).__name__}). Nothing was written.")

    written, stopped = 0, None
    for entry in out["mark"]:
        try:
            record_write(LINK_TARGET_SYSTEM, "LINK",
                         f"{SUBMISSION_LINK_ENDPOINT}/{BULK_MARKED}",
                         target_id=None,
                         payload={"hubspot_form_id": row["form_id"],
                                  "hubspot_submission_id":
                                      entry["submission_id"],
                                  "actor_user_id": actor_user_id},
                         status="success")
            written += 1
        except AuditUnavailable as e:
            stopped = f"{entry['submission_id']}: {e}"
            break
    logger.info("bulk mark handled on form %s before %s: %d recorded%s",
                row["form_id"], cutoff, written,
                " (stopped)" if stopped else "")
    lines = [f"✅ **{written} {row['label']} submission"
             f"{'' if written == 1 else 's'} before {cutoff} recorded as "
             "handled.**",
             f"Held back, not touched: {len(out['held_back'])}. One "
             "write_audit row each; nothing sent to CSuite or HubSpot."]
    if stopped:
        lines += ["", f"🛑 Stopped at the first failed row ({stopped}). "
                      f"{len(out['mark']) - written} not recorded; nothing "
                      "was retried. Run the preview again to see what is "
                      "left."]
    return "\n".join(lines)


def _preview(row, cutoff, out) -> str:
    lines = [f"🧪 **Mark handled — PREVIEW** ({row['label']}, submitted "
             f"before {cutoff}). Nothing was written.", "",
             f"**Would record as handled: {len(out['mark'])}**"]
    lines += [f"• `{e['submission_id'][:8]}` — {e['submitted']}"
              for e in out["mark"]]
    lines += ["", f"**Held back, never marked: {len(out['held_back'])}**"]
    lines += [f"• `{e['submission_id'][:8]}` — {e['submitted']}"
              for e in out["held_back"]]
    absent = sorted(set(out["hold_back_ids"])
                    - {e["submission_id"][:8] for e in out["held_back"]})
    if absent:
        lines.append(f"(On the hold-back list but not unhandled before "
                     f"{cutoff}: {', '.join(absent)}.)")
    lines += ["",
              f"Already handled or held: {out['already']}. On or after "
              f"{cutoff}, never touched: {out['on_or_after']}."
              + (f" Undated, skipped: {out['undated']}." if out["undated"]
                 else ""),
              "", "To record them, say:",
              f"*mark handled {row['key']} before {cutoff} exclude "
              f"{', '.join(out['hold_back_ids'])} apply*"]
    return "\n".join(lines)
