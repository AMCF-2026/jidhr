"""
Job: daf_sync (V5.90)
=====================
The nightly DAF intake (sync/daf_sync.py), on the existing 06:15 UTC cron,
after registrations_sync (priority 45, between registrations' 40 and the
mirror's 50). A separate job, not a chain step, so a stopped event or
registrations step cannot hold DAF back, and the reverse.

Armed like the chain: nothing happens unless SCHEDULED_SYNC_ENABLED is
true. Then JIDHR_DAF_MODE decides — preview (default) writes nothing.

One email, to JIDHR_DAF_REPORT_TO, and only when at least one submission
was added or held. A step that cannot run at all (no JIDHR_DAF_SINCE, a
live run without the DAF switches, an unreadable history) sends an
[Jidhr ALERT] instead and fails the job; the next night runs again.
"""

import logging

from jobs.runner import JobFailed, register
from sync import daf_sync, scheduled

logger = logging.getLogger(__name__)

JOB_TYPE = "daf_sync"


@register(JOB_TYPE)
def handle(job) -> dict:
    if not scheduled.scheduled_enabled():
        logger.info("nightly DAF step is DISABLED (SCHEDULED_SYNC_ENABLED is "
                    "not 'true') — nothing read, nothing written, no email")
        return {"enabled": False}

    try:
        out = daf_sync.run()
    except daf_sync.Refused as refused:
        from clients.notify import KIND_ALERT, safe_notify

        safe_notify("nightly DAF step did not run", str(refused),
                    kind=KIND_ALERT, mode=daf_sync.mode())
        raise JobFailed(f"daf_sync refused: {refused}",
                        result={"enabled": True, "refused": str(refused)})

    out["enabled"] = True
    delivery = daf_sync.send_email([out])
    out["emailed"] = delivery.get("sent") or []
    out["email_failed"] = delivery.get("failed") or []
    out["email_skipped"] = delivery.get("skipped")
    logger.info("nightly DAF step (%s): %d candidate(s), %d added, %d held, "
                "%d already handled, %d already held", out["mode"],
                out["candidates"], len(out["added"]), len(out["held"]),
                out["skipped_handled"], out["skipped_held"])
    # The email carries names; the job row keeps ids and counts only.
    return {k: v for k, v in out.items() if k not in ("added", "held")} | {
        "added_ids": [e["submission_id"] for e in out["added"]],
        "held_ids": {e["submission_id"]: e["reason"] for e in out["held"]}}
