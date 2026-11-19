"""
Daily digests for the "a person acts" (D) forms (V5.96)
======================================================
Run inside the nightly forms_sync job, from the submissions it has already
read. For each D form switched on (JIDHR_FORM_<KEY>_ON=true) with a
since-date (JIDHR_FORM_<KEY>_SINCE), one plain email listing the
submissions that arrived since the last forms_sync run that delivered that
form's digest in full (per form; at most 7 days back; 24 hours if there is
no earlier run), never before the since-date. No email when there are none.
A failed digest is repeated the next night.

    preview (default)  to JIDHR_FORMS_REPORT_TO only, marked [Preview]
    live               to the form's owners: JIDHR_OWNER_EMAIL_<OWNER>
                       (e.g. JIDHR_OWNER_EMAIL_KODS). A live form whose owner
                       has no address set refuses with an [Jidhr ALERT].
                       JIDHR_FORMS_LIVE_CC (optional, comma-separated) is
                       CC'd on every live digest; unset, no CC. Preview
                       never reads it.

WHAT A DIGEST MAY SHOW
----------------------
Only the fields in the form's `digest` list in sync/form_registry.py, each
SHOW, LAST4 or DATE, plus "given"/"not given" for `presence_only` fields.
Nothing else is read into the text — not the sensitive fields, not free
text. The submitter is shown by name with a masked email and a HubSpot link.
The ACH Request Form is never downloaded (counted=False), so it has no
digest. Nothing is written anywhere and nothing is logged but counts.

A digest that fails for any recipient, or has none, sends one [Jidhr ALERT]
per form (safe_notify): form name, mode and counts, never submitter data.
"""

import logging
import re
from datetime import datetime, timedelta, timezone

from config import Config
from sync import form_registry as registry

logger = logging.getLogger(__name__)

MAX_LOOKBACK = timedelta(days=7)
DEFAULT_LOOKBACK = timedelta(hours=24)

# The window is PER FORM. A form's digest starts at the last complete
# forms_sync run that covered it: one whose stored result has a `digests`
# map with no failure for that form. A form with no entry in that map had
# nothing to send (or was off), so the run covered it. A run with no
# `digests` map at all (stood down on a bad setting, or disabled) covers no
# form, so it moves no window. The 7-day cap still applies (window_start).
_EVER_RUN_SQL = """
    SELECT max(finished_at) AS finished_at FROM jobs
     WHERE job_type = 'forms_sync' AND status = 'complete' AND id <> %s
"""
_RECENT_RUNS_SQL = """
    SELECT finished_at, result FROM jobs
     WHERE job_type = 'forms_sync' AND status = 'complete' AND id <> %s
       AND finished_at >= %s
     ORDER BY finished_at DESC
"""


def recent_runs(job_id, now):
    """The complete forms_sync runs inside the 7-day cap, newest first, and
    whether any complete run exists at all. One read for every form. None
    when the database cannot answer (every form then covers 24 hours)."""
    try:
        from clients import database

        ever = database.execute_query(_EVER_RUN_SQL, (job_id or 0,),
                                      fetch=True) or []
        rows = database.execute_query(
            _RECENT_RUNS_SQL, (job_id or 0, now - MAX_LOOKBACK),
            fetch=True) or []
    except Exception as e:
        logger.warning("previous forms_sync runs unknown (%s); digests cover "
                       "24 hours", type(e).__name__)
        return None
    last = (ever[0] or {}).get("finished_at") if ever else None
    runs = [(r.get("finished_at"), _result(r.get("result"))) for r in rows
            if isinstance(r.get("finished_at"), datetime)]
    runs.sort(key=lambda run: run[0], reverse=True)
    return {"ever": isinstance(last, datetime), "runs": runs}


def _result(value) -> dict:
    if isinstance(value, str):
        import json
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, dict) else {}


def covered(result, key) -> bool:
    """Did this run's stored result cover the form? See the note above."""
    digests = (result or {}).get("digests")
    if not isinstance(digests, dict):
        return False
    entry = digests.get(key)
    if entry is None:
        return True
    try:
        return int((entry or {}).get("failed") or 0) == 0
    except (TypeError, ValueError):
        return False


def last_success(key, runs, now):
    """When the form's window starts from, or None (no run ever: 24 hours).

    The newest recent run that covered the form. None of the recent runs
    did, but a run has completed before: the 7-day cap, so a failed or
    stood-down night is retried as far back as the cap allows."""
    if not runs:
        return None
    for finished, result in runs["runs"]:
        if covered(result, key):
            return finished
    if runs["ever"] or runs["runs"]:
        return now - MAX_LOOKBACK
    return None


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


LIVE_CC_ENV = "JIDHR_FORMS_LIVE_CC"


def live_cc(environ) -> tuple:
    """JIDHR_FORMS_LIVE_CC as addresses: comma-separated, blanks dropped,
    repeats dropped. Unset or empty is (), which is no CC."""
    raw = str((environ or {}).get(LIVE_CC_ENV, "") or "")
    out = []
    for part in raw.split(","):
        address = part.strip()
        if address and address.lower() not in {a.lower() for a in out}:
            out.append(address)
    return tuple(out)


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
    """`start` is {form key: window start} (per form), or one datetime for
    every form."""
    out = []
    for form in registry.FORMS:
        s = switches.get(form["key"]) or {}
        if not (form["digest"] and form.get("counted", True) and s.get("on")
                and s.get("since") and not form.get("observe_only")):
            continue
        form_start = (start.get(form["key"], now - DEFAULT_LOOKBACK)
                      if isinstance(start, dict) else start)
        digest = build(form, raw_by_key.get(form["key"]) or [], s,
                       form_start, now)
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
            _alert_failed(form, d, failed=0, attempted=0)
            continue
        # Live only. No CC is the call exactly as it was before the option.
        cc = live_cc(environ) if d["mode"] == "live" else ()
        if cc:
            delivery = send_report(to, d["subject"], d["text"], cc=cc)
        else:
            delivery = send_report(to, d["subject"], d["text"])
        results.append((d["key"], delivery.get("sent") or [],
                        delivery.get("failed") or []))
        logger.info("digest %s: %d submission(s), %d recipient(s)", d["key"],
                    d["count"], len(delivery.get("sent") or []))
        failed = len(delivery.get("failed") or [])
        if failed:
            _alert_failed(form, d, failed=failed,
                          attempted=failed + len(delivery.get("sent") or []))
    return results


def _alert_failed(form, digest, failed, attempted):
    """One [Jidhr ALERT] for a digest that did not fully go out, live or
    preview. Form name and counts only: no submitter, no addresses.

    The run records the failure in its result, so this form's window does
    not move: the next night's digest repeats these submissions (within the
    7-day cap). The alert says so.
    """
    from clients.notify import KIND_ALERT, safe_notify

    n = digest["count"]
    what = (f"{n} submission{'' if n == 1 else 's'}")
    if attempted:
        why = (f"failed for {failed} of {attempted} "
               f"recipient{'' if attempted == 1 else 's'}")
    else:
        why = "had no recipients, so it was not sent"
    safe_notify(
        f"{form['name']} digest not delivered",
        f"The {digest['mode']} digest for {form['name']} ({what}) {why}. "
        "The next digest repeats them (up to 7 days back); meanwhile they "
        "are in HubSpot: "
        + Config.HUBSPOT_FORM_SUBMISSIONS_URL.format(form_id=form["form_id"]),
        kind=KIND_ALERT, form=form["key"], mode=digest["mode"],
        submissions=n, failed_recipients=failed)
