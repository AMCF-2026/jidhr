"""
Daily digests for the "a person acts" (D) forms (V5.96)
======================================================
Run inside the nightly forms_sync job, from the submissions it has already
read. For each D form switched on (JIDHR_FORM_<KEY>_ON=true) with a
since-date (JIDHR_FORM_<KEY>_SINCE), one plain email listing the
submissions that arrived since the last successful forms_sync run (at most
7 days back; 24 hours if there is no earlier run), never before the
since-date. No email when there are none.

    preview (default)  to JIDHR_FORMS_REPORT_TO only, marked [Preview]
    live               to the form's owners: JIDHR_OWNER_EMAIL_<OWNER>
                       (e.g. JIDHR_OWNER_EMAIL_KODS). A live form whose owner
                       has no address set refuses with an [Jidhr ALERT].

WHAT A DIGEST MAY SHOW
----------------------
Only the fields in the form's `digest` list in sync/form_registry.py, each
SHOW, LAST4 or DATE, plus "given"/"not given" for `presence_only` fields.
Nothing else is read into the text — not the sensitive fields, not free
text. The submitter is shown by name with a masked email and a HubSpot link.
The ACH Request Form is never downloaded (counted=False), so it has no
digest. Nothing is written anywhere and nothing is logged but counts.
"""

import logging
import re
from datetime import datetime, timedelta, timezone

from config import Config
from sync import form_registry as registry

logger = logging.getLogger(__name__)

MAX_LOOKBACK = timedelta(days=7)
DEFAULT_LOOKBACK = timedelta(hours=24)

_LAST_SUCCESS_SQL = """
    SELECT max(finished_at) AS finished_at FROM jobs
     WHERE job_type = 'forms_sync' AND status = 'complete' AND id <> %s
"""


def last_success(job_id):
    """When the previous forms_sync run completed, or None. Read only."""
    try:
        from clients import database

        rows = database.execute_query(_LAST_SUCCESS_SQL, (job_id or 0,),
                                      fetch=True) or []
    except Exception as e:
        logger.warning("previous forms_sync run unknown (%s); digests cover "
                       "24 hours", type(e).__name__)
        return None
    value = (rows[0] or {}).get("finished_at") if rows else None
    return value if isinstance(value, datetime) else None


def window_start(now, previous) -> datetime:
    if previous is None:
        return now - DEFAULT_LOOKBACK
    return max(previous, now - MAX_LOOKBACK)


def owner_email_env(owner) -> str:
    return f"JIDHR_OWNER_EMAIL_{owner.upper()}"


def recipients(form, switches, environ) -> tuple:
    """(addresses, problem). Preview: the report recipients. Live: owners."""
    if switches["mode"] != "live":
        return tuple(getattr(Config, registry.DEFAULT_REPORT_TO_ENV, ())
                     or ()), None
    out, missing = [], []
    for owner in form["owners"]:
        address = str(environ.get(owner_email_env(owner), "") or "").strip()
        (out.append(address) if address else missing.append(
            owner_email_env(owner)))
    if missing or not out:
        return (), (f"{form['name']} is live but {', '.join(missing) or 'an owner address'}"
                    " is not set, so its digest has nobody to go to.")
    return tuple(out), None


def check_live_owners(switches, environ) -> list:
    """Every live digest form that has nobody to send to."""
    problems = []
    for form in registry.FORMS:
        s = switches.get(form["key"]) or {}
        if form["digest"] and s.get("on") and s.get("mode") == "live":
            _to, problem = recipients(form, s, environ)
            if problem:
                problems.append(problem)
    return problems


# --- values ------------------------------------------------------------------

def _values(sub) -> dict:
    return {v.get("name"): v.get("value") for v in sub.get("values") or []}


def last4(value) -> str:
    """A number masked to its last four; a name shown as it is."""
    text = str(value or "").strip()
    digits = re.sub(r"\D", "", text)
    if len(digits) >= 4 and len(digits) >= len(re.sub(r"\s", "", text)) / 2:
        return f"••••{digits[-4:]}"
    return text


def _date(value) -> str:
    text = str(value or "").strip()
    try:
        if re.fullmatch(r"\d{11,14}", text):
            d = datetime.fromtimestamp(int(text) / 1000, timezone.utc).date()
        else:
            d = datetime.fromisoformat(text[:10]).date()
        return f"{d:%a %b} {d.day}, {d.year}"
    except (ValueError, OSError):
        return "date unreadable"


def shown(treatment, value) -> str:
    if treatment == registry.LAST4:
        return last4(value)
    if treatment == registry.DATE:
        return _date(value)
    return re.sub(r"\s+", " ", str(value or "").strip())[:120]


def submission_lines(form, sub, index) -> list:
    from sync.contact_create import mask_email

    v = _values(sub)
    name = f"{v.get('firstname') or ''} {v.get('lastname') or ''}".strip()
    when = datetime.fromtimestamp(int(sub.get("submittedAt") or 0) / 1000,
                                  timezone.utc)
    lines = [f"{index}. {name or '(no name given)'} — "
             f"{mask_email(v.get('email'))} (submitted "
             f"{when:%a %b} {when.day})"]
    parts = [f"{label}: {shown(treatment, v.get(field))}"
             for field, label, treatment in form["digest"]
             if str(v.get(field) or "").strip()]
    parts += [f"{label}: {'given' if str(v.get(field) or '').strip() else 'not given'}"
              for field, label in form["presence_only"]]
    for i in range(0, len(parts), 3):
        lines.append("   " + " · ".join(parts[i:i + 3]))
    link = Config.HUBSPOT_FORM_SUBMISSIONS_URL.format(form_id=form["form_id"])
    lines.append(f"   HubSpot: {link} (submission "
                 f"{str(sub.get('conversionId') or '')[:8]})")
    return lines


# --- the digests ---------------------------------------------------------------

def _day(now):
    from zoneinfo import ZoneInfo
    local = now.astimezone(ZoneInfo("America/New_York"))
    return f"{local:%a %b} {local.day}"


def _since_text(start):
    from zoneinfo import ZoneInfo
    local = start.astimezone(ZoneInfo("America/New_York"))
    return f"{local:%a %b} {local.day}, {local:%-I:%M %p} ET"


def build(form, subs, switches, start, now) -> dict | None:
    """One form's digest, or None when there is nothing new."""
    floor = start
    if switches.get("since"):
        since = switches["since"]
        floor = max(floor, datetime(since.year, since.month, since.day,
                                    tzinfo=timezone.utc))
    cut = floor.timestamp() * 1000
    new = sorted((s for s in subs if int(s.get("submittedAt") or 0) >= cut),
                 key=lambda s: int(s.get("submittedAt") or 0))
    if not new:
        return None
    preview = switches["mode"] != "live"
    owners = "; ".join(registry.owner_names(form)) or "no owner"
    n = len(new)
    subject = (f"{'[Preview] ' if preview else ''}{form['name']} — {n} new "
               f"submission{'' if n == 1 else 's'} — {_day(now)}")
    lines = ["Good morning,", "",
             f"{form['name']}: {n} new since {_since_text(floor)}. A person "
             "acts on these; Jidhr wrote nothing anywhere."]
    if preview:
        lines += ["", f"This is a PREVIEW, sent to Carl only. When live it "
                      f"goes to: {owners}."]
    lines.append("")
    if form.get("group_by"):
        groups = {}
        for s in new:
            key = str(_values(s).get(form["group_by"]) or "not given").strip()
            groups.setdefault(key, []).append(s)
        for key in sorted(groups):
            lines.append(f"{key} ({len(groups[key])})")
            for i, s in enumerate(groups[key], 1):
                lines += submission_lines(form, s, i)
            lines.append("")
    else:
        for i, s in enumerate(new, 1):
            lines += submission_lines(form, s, i)
        lines.append("")
    hidden = [label for _f, label in form["presence_only"]]
    if hidden:
        lines.append(f"{', '.join(hidden)} are shown only as given or not "
                     "given; the values are in HubSpot.")
    lines += [f"Owner: {owners}.", "", "— Jidhr"]
    return {"key": form["key"], "subject": subject, "text": "\n".join(lines),
            "count": n, "mode": switches["mode"]}


def build_all(raw_by_key, switches, now, start) -> list:
    out = []
    for form in registry.FORMS:
        s = switches.get(form["key"]) or {}
        if not (form["digest"] and form.get("counted", True) and s.get("on")
                and s.get("since") and not form.get("observe_only")):
            continue
        digest = build(form, raw_by_key.get(form["key"]) or [], s, start, now)
        if digest:
            out.append(digest)
    return out


def send(digests, switches, environ) -> list:
    """Send each digest. [(key, sent, failed)]; counts only are logged."""
    from clients.notify import send_report

    results = []
    for d in digests:
        form = registry.BY_KEY[d["key"]]
        to, problem = recipients(form, switches[d["key"]], environ)
        if problem or not to:
            results.append((d["key"], [], ["no recipients"]))
            continue
        delivery = send_report(to, d["subject"], d["text"])
        results.append((d["key"], delivery.get("sent") or [],
                        delivery.get("failed") or []))
        logger.info("digest %s: %d submission(s), %d recipient(s)", d["key"],
                    d["count"], len(delivery.get("sent") or []))
    return results
