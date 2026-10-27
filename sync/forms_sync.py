"""
Nightly forms report, OBSERVE mode (V5.94)
==========================================
Reached only from jobs/handlers/forms_sync.py, after daf_sync. Reads every
form in sync/form_registry.py and HubSpot's own form list, and emails
JIDHR_FORMS_REPORT_TO one plain report:

  * new submissions per form in the last 24 hours, the last 7 days and in
    all, and the date of the newest;
  * forms that have gone quiet (C and D forms with nothing in 30 days);
  * forms HubSpot has that the registry does not — the alert that a new
    form went up without anyone deciding what happens to it;
  * registry forms HubSpot no longer lists (archived, or deleted);
  * each form's Railway switches, as read.

OBSERVE MEANS READ-ONLY
-----------------------
No CSuite client is ever made, nothing is written anywhere — not even an
audit row — and the report carries form names and counts only: no
submitter's name, email or answer. HubSpot GETs only.

A per-form switch set to something it cannot mean (JIDHR_FORM_<KEY>_MODE
not preview/live, a _SINCE that is not a date) stands the whole step down
with an [Jidhr ALERT] and the job still exits 0 — the daf_sync rule.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from config import Config
from sync import form_registry as registry

logger = logging.getLogger(__name__)

QUIET_DAYS = 30
FORM_LIST_PAGE = 100
MAX_FORM_LIST_PAGES = 20


class Refused(RuntimeError):
    """The step cannot run at all. Nothing was read past the refusal."""


class BadSetting(Refused):
    """A Railway setting is wrong: alert, stand down, exit 0."""


def enabled() -> bool:
    """JIDHR_FORMS_SYNC_ENABLED, exact word "true", fail closed."""
    return bool(getattr(Config, "JIDHR_FORMS_SYNC_ENABLED", False))


def read_switches(environ=None) -> dict:
    """key -> switches for every registered form. Raises BadSetting naming
    every wrong value at once, so one alert fixes them all."""
    environ = os.environ if environ is None else environ
    found, problems = {}, []
    for form in registry.FORMS:
        try:
            found[form["key"]] = registry.switches(form, environ)
        except registry.BadSetting as bad:
            problems.append(str(bad))
    if problems:
        raise BadSetting(" ".join(problems))
    return found


def hubspot_forms(hubspot) -> tuple:
    """({form_id: name}, error). Every type, collected forms included."""
    from clients.hubspot import hubspot_error

    found, after = {}, None
    for _page in range(MAX_FORM_LIST_PAGES):
        params = {"limit": FORM_LIST_PAGE, "formTypes": "all"}
        if after:
            params["after"] = after
        response = hubspot._get("marketing/v3/forms", params)
        problem = hubspot_error(response)
        if problem:
            return {}, problem
        for row in response.get("results") or []:
            if row.get("id") and not row.get("archived"):
                found[row["id"]] = row.get("name") or ""
        after = ((response.get("paging") or {}).get("next") or {}).get("after")
        if not after:
            return found, None
    return found, "the form list did not end within the page limit"


def _stats(raw, now) -> dict:
    stamps = sorted(int(s.get("submittedAt") or 0) for s in raw
                    if s.get("submittedAt"))
    def since(delta):
        cut = (now - delta).timestamp() * 1000
        return sum(1 for t in stamps if t >= cut)
    newest = (datetime.fromtimestamp(stamps[-1] / 1000, timezone.utc)
              if stamps else None)
    return {"total": len(raw), "last_24h": since(timedelta(hours=24)),
            "last_7d": since(timedelta(days=7)),
            "last_30d": since(timedelta(days=QUIET_DAYS)),
            "newest": newest.date().isoformat() if newest else None}


def run(hubspot=None, now=None, environ=None) -> dict:
    """One observe pass. Raises BadSetting / Refused; never writes."""
    from intents.daf_workflow import fetch_all_submissions

    switches = read_switches(environ)
    now = now or datetime.now(timezone.utc)
    if hubspot is None:
        from clients.hubspot import HubSpotClient
        hubspot = HubSpotClient()

    listed, error = hubspot_forms(hubspot)
    if error:
        raise Refused(f"HubSpot's form list could not be read ({error}); "
                      "nothing was reported.")

    out = {"mode": "observe", "run_at": now.isoformat(), "forms": [],
           "unknown": sorted(({"form_id": fid, "name": name}
                              for fid, name in listed.items()
                              if fid not in registry.BY_ID),
                             key=lambda f: f["name"].lower()),
           "not_listed": [], "read_errors": []}
    for form in registry.FORMS:
        row = {"key": form["key"], "name": form["name"],
               "category": form["category"], "why": form["why"],
               "owners": registry.owner_names(form),
               "switches": switches[form["key"]],
               "listed": form["form_id"] in listed}
        if not row["listed"]:
            out["not_listed"].append(form["key"])
        raw, problem, complete = fetch_all_submissions(hubspot,
                                                       form["form_id"])
        if problem:
            row["error"] = str(problem)[:160]
            out["read_errors"].append(form["key"])
        else:
            row.update(_stats(raw, now))
            row["complete"] = complete
        out["forms"].append(row)
    return out


# ---------------------------------------------------------------------------
# The report
# ---------------------------------------------------------------------------

def _day(now) -> str:
    from zoneinfo import ZoneInfo
    local = now.astimezone(ZoneInfo("America/New_York"))
    return f"{local:%a %b} {local.day}"


def new_today(out) -> int:
    return sum(f.get("last_24h") or 0 for f in out["forms"])


def quiet(out) -> list:
    """C and D forms with nothing in QUIET_DAYS days."""
    return [f for f in out["forms"] if f["category"] in ("C", "D")
            and "error" not in f and not f.get("last_30d")]


def subject(out, now=None) -> str:
    now = now or datetime.fromisoformat(out["run_at"])
    parts = [f"{new_today(out)} new"]
    if out["unknown"]:
        n = len(out["unknown"])
        parts.append(f"{n} unknown form{'' if n == 1 else 's'}")
    if out["read_errors"]:
        n = len(out["read_errors"])
        parts.append(f"{n} unreadable")
    return f"[Observe] Website forms — {_day(now)} ({', '.join(parts)})"


def _switch_text(s) -> str:
    since = s["since"].isoformat() if s["since"] else "no since-date"
    return f"{'on' if s['on'] else 'off'}, {s['mode']}, {since}"


def email_text(out) -> str:
    lines = ["Good morning,", "",
             "Jidhr read every website form in HubSpot overnight. This is "
             "OBSERVE mode: it counts and writes nothing, anywhere.", ""]
    if out["unknown"]:
        lines += [f"NEW FORMS NOBODY HAS DECIDED ON ({len(out['unknown'])})"]
        lines += [f"• {f['name']} ({f['form_id']})" for f in out["unknown"]]
        lines += ["Add each one to sync/form_registry.py with a category.", ""]
    lines += ["SUBMISSIONS (last 24 hours · last 7 days · all time · newest)"]
    for category in registry.CATEGORIES:
        rows = [f for f in out["forms"] if f["category"] == category]
        if not rows:
            continue
        lines += ["", f"{category}: " + {
            "A": "CSuite record and task (own code)",
            "B": "CSuite note", "C": "HubSpot only",
            "D": "a person acts", "E": "ignored"}[category]]
        for f in rows:
            if "error" in f:
                lines.append(f"• {f['name']}: could not be read "
                             f"({f['error']})")
                continue
            partial = "" if f.get("complete", True) else " (partial read)"
            lines.append(f"• {f['name']}: {f['last_24h']} · {f['last_7d']} "
                         f"· {f['total']} · {f['newest'] or 'never'}"
                         f"{partial}")
    gone_quiet = quiet(out)
    if gone_quiet:
        lines += ["", f"QUIET FOR {QUIET_DAYS}+ DAYS ({len(gone_quiet)})"]
        lines += [f"• {f['name']} (last: {f['newest'] or 'never'})"
                  for f in gone_quiet]
    if out["not_listed"]:
        lines += ["", "IN THE REGISTRY BUT NOT LISTED BY HUBSPOT "
                      "(archived or deleted)"]
        lines += [f"• {registry.BY_KEY[k]['name']}" for k in out["not_listed"]]
    lines += ["", "SWITCHES (on/off, mode, since — nothing acts on them yet)"]
    for f in out["forms"]:
        if f["category"] in ("B", "D") or f["switches"]["on"]:
            owners = ", ".join(f["owners"]) or "no owner"
            lines.append(f"• {f['name']}: {_switch_text(f['switches'])} "
                         f"— owner: {owners}")
    lines += ["", "Nothing was written to CSuite or HubSpot.", "", "— Jidhr"]
    return "\n".join(lines)


def recipients() -> tuple:
    return tuple(getattr(Config, registry.DEFAULT_REPORT_TO_ENV, ()) or ())


def send_email(out) -> dict:
    """Every night while observing: the report is the heartbeat."""
    from clients.notify import send_report

    to = recipients()
    if not to:
        logger.warning("forms report not sent: %s is not set",
                       registry.DEFAULT_REPORT_TO_ENV)
        return {"sent": [], "failed": [], "skipped": "no recipients"}
    return send_report(to, subject(out), email_text(out))
