"""
Job: forms_sync (V5.94)
=======================
The nightly forms report (sync/forms_sync.py), OBSERVE mode: reads, counts,
writes nothing. Then (V5.96) one digest per switched-on D form
(sync/form_digests.py), from the submissions already read. Priority 46, right after daf_sync (45), before the mirror
(50).

Armed by two words: SCHEDULED_SYNC_ENABLED=true and
JIDHR_FORMS_SYNC_ENABLED=true. A wrong per-form switch stands the step down
with an [Jidhr ALERT] and the job still completes (exit 0); a HubSpot that
cannot list its forms sends an ALERT and fails the job.
"""

import logging

from jobs.runner import JobFailed, register
from sync import forms_sync, scheduled

logger = logging.getLogger(__name__)

JOB_TYPE = "forms_sync"


@register(JOB_TYPE)
def handle(job) -> dict:
    if not (scheduled.scheduled_enabled() and forms_sync.enabled()):
        logger.info("nightly forms report is DISABLED (needs "
                    "SCHEDULED_SYNC_ENABLED and JIDHR_FORMS_SYNC_ENABLED "
                    "set to 'true') — nothing read, no email")
        return {"enabled": False}

    from clients.notify import KIND_ALERT, safe_notify

    try:
        out = forms_sync.run()
    except forms_sync.BadSetting as bad:
        safe_notify("nightly forms report: a setting needs fixing", str(bad),
                    kind=KIND_ALERT)
        logger.error("nightly forms report stood down: a setting needs "
                     "fixing")
        return {"enabled": True, "refused": str(bad), "stood_down": True}
    except forms_sync.Refused as refused:
        safe_notify("nightly forms report did not run", str(refused),
                    kind=KIND_ALERT)
        raise JobFailed(f"forms_sync refused: {refused}",
                        result={"enabled": True, "refused": str(refused)})

    raw = out.pop("_raw", {})
    delivery = forms_sync.send_email(out)

    # V5.96: the D-form digests, from the submissions already read.
    import os
    from datetime import datetime, timezone

    from sync import form_digests

    now = datetime.now(timezone.utc)
    switches = {f["key"]: f["switches"] for f in out["forms"]}
    start = form_digests.window_start(
        now, form_digests.last_success((job or {}).get("id")))
    digests = form_digests.build_all(raw, switches, now, start)
    sent = form_digests.send(digests, switches, os.environ)
    del raw
    logger.info("nightly forms report: %d form(s), %d new in 24h, %d "
                "unknown, %d unreadable", len(out["forms"]),
                forms_sync.new_today(out), len(out["unknown"]),
                len(out["read_errors"]))
    # Counts only; the report itself carries no submitter data either.
    return {"enabled": True, "mode": out["mode"],
            "forms": len(out["forms"]), "new_24h": forms_sync.new_today(out),
            "unknown": [f["form_id"] for f in out["unknown"]],
            "not_listed": out["not_listed"],
            "read_errors": out["read_errors"],
            "digests": {key: {"sent": len(ok), "failed": len(bad)}
                        for key, ok, bad in sent},
            "emailed": delivery.get("sent") or [],
            "email_failed": delivery.get("failed") or [],
            "email_skipped": delivery.get("skipped")}
