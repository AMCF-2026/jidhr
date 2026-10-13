"""
Job: registrations_sync
=======================
The scheduled sync chain — event sync, then contacts, then registrations —
run by the EXISTING jobs runner on the existing 06:15 UTC cron. No new
mechanism and no new Railway service.

SHIPS DISARMED. SCHEDULED_SYNC_ENABLED defaults to False, and when it is
False this handler logs "disabled" and exits: no CSuite call, no HubSpot
call, and NO EMAIL, because a deploy should be silent.

payload
    today       optional ISO date, for testing the window without waiting
                for a day to pass. Ignored in production.
    recurring / at
                read by the runner, not here.

result
    mode, window_floor, events_in_window, run_log_ids, and one entry per
    step with its counts. The same dict the summary email is built from, so
    the job row and the email can never disagree.

Exactly one email per enabled run: [Jidhr OK] on success, [Jidhr ALERT] on
any stop. A run with nothing to do still sends OK — a missing email is how
anybody knows the schedule died, and silence from a cron job is
indistinguishable from a cron job that never fired.
"""

import logging

from jobs.runner import JobFailed, register
from sync import scheduled

logger = logging.getLogger(__name__)

JOB_TYPE = "registrations_sync"


@register(JOB_TYPE)
def handle(job) -> dict:
    """Run the chain once, or exit quietly if the schedule is disarmed."""
    payload = (job or {}).get("payload") or {}

    if not scheduled.scheduled_enabled():
        logger.info("scheduled sync is DISABLED (SCHEDULED_SYNC_ENABLED is "
                    "not 'true') — nothing was read, nothing was written, "
                    "and no email was sent")
        return {"enabled": False, "mode": None,
                "note": "SCHEDULED_SYNC_ENABLED is off"}

    today = _today_from(payload)
    out = scheduled.run_chain(today=today)
    out["enabled"] = True

    # The email goes out whatever happened, and BEFORE the raise below, so
    # a stop is reported even though the job row is about to be failed.
    delivery = scheduled.send_summary(out)
    out["notified"] = delivery.get("sent") or []
    out["notify_failed"] = delivery.get("failed") or []

    if out.get("stopped"):
        # JobFailed keeps the result: a chain that created four events and
        # stopped on the fifth still records the four.
        raise JobFailed(
            f"the scheduled sync stopped at {out.get('stopped_step')}: "
            f"{out.get('stopped')}", result=out)
    return out


def _today_from(payload):
    """The window's anchor date, for tests. None means the real today."""
    from datetime import date

    raw = str((payload or {}).get("today") or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        logger.warning("ignoring unreadable payload today=%r", raw)
        return None
