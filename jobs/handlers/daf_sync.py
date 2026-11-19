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
was added, held or deferred. A step that cannot run at all (no JIDHR_DAF_SINCE, a
live run without the DAF switches, an unreadable history) sends an
[Jidhr ALERT] instead and fails the job; the next night runs again.

Then the nightly ENDOWMENT step (sync/endowment_sync.py), whatever DAF did,
with what DAF left of CSUITE_WRITE_BUDGET. Its block goes in the same email.
JIDHR_ENDOWMENT_SINCE unset: it does nothing. A refusal or a wrong setting
sends its own [Jidhr ALERT] and never fails the job or stops the DAF email.
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

    out = stood = refused = None
    try:
        out = daf_sync.run()
    except daf_sync.BadSetting as bad:
        # A wrong setting is not a crash: alert, stand down, finish the job
        # normally so the run exits 0. Nothing was written or held.
        from clients.notify import KIND_ALERT, safe_notify

        safe_notify("nightly DAF step: a setting needs fixing", str(bad),
                    kind=KIND_ALERT, mode=daf_sync.mode())
        logger.error("nightly DAF step stood down: a setting needs fixing")
        stood = {"enabled": True, "refused": str(bad), "stood_down": True}
    except daf_sync.Refused as error:
        from clients.notify import KIND_ALERT, safe_notify

        safe_notify("nightly DAF step did not run", str(error),
                    kind=KIND_ALERT, mode=daf_sync.mode())
        refused = error

    endowment = _endowment(out)
    extra = {"endowment": _endowment_ids(endowment)} if endowment else {}

    if out is None:
        if endowment:
            delivery = daf_sync.send_email([endowment])
            extra["endowment"]["emailed"] = delivery.get("sent") or []
        if refused is not None:
            raise JobFailed(f"daf_sync refused: {refused}",
                            result={"enabled": True,
                                    "refused": str(refused)} | extra)
        return stood | extra

    out["enabled"] = True
    delivery = daf_sync.send_email([out] + ([endowment] if endowment else []))
    out["emailed"] = delivery.get("sent") or []
    out["email_failed"] = delivery.get("failed") or []
    out["email_skipped"] = delivery.get("skipped")
    logger.info("nightly DAF step (%s): %d candidate(s), %d added, %d held, "
                "%d deferred, %d already handled, %d already held",
                out["mode"], out["candidates"], len(out["added"]),
                len(out["held"]), len(out.get("deferred") or []),
                out["skipped_handled"], out["skipped_held"])
    # The email carries names; the job row keeps ids and counts only.
    return {k: v for k, v in out.items()
            if k not in ("added", "held", "deferred", "existing_task")} | {
        "existing_task_ids": {e["submission_id"]: e["open_task"]["task_id"]
                              for e in out.get("existing_task") or []},
        "added_ids": [e["submission_id"] for e in out["added"]],
        "held_ids": {e["submission_id"]: e["reason"] for e in out["held"]},
        "deferred_ids": {e["submission_id"]: e["reason"]
                         for e in out.get("deferred") or []}} | extra


def _endowment(daf_out):
    """The endowment section, or None (off, refused, or crashed: alerted)."""
    from config import Config
    from sync import endowment_sync

    budget_left = None
    if daf_out:
        total = int(getattr(Config, "CSUITE_WRITE_BUDGET", 0) or 0)
        budget_left = total - int(daf_out.get("budget_allowed") or 0)
    try:
        section = endowment_sync.run(budget_left=budget_left)
    except daf_sync.BadSetting as bad:
        from clients.notify import KIND_ALERT, safe_notify

        safe_notify("nightly endowment step: a setting needs fixing",
                    str(bad), kind=KIND_ALERT, mode=endowment_sync.mode())
        logger.error("nightly endowment step stood down: a setting needs "
                     "fixing")
        return None
    except daf_sync.Refused as error:
        from clients.notify import KIND_ALERT, safe_notify

        safe_notify("nightly endowment step did not run", str(error),
                    kind=KIND_ALERT, mode=endowment_sync.mode())
        logger.error("nightly endowment step refused")
        return None
    except Exception as e:
        from clients.notify import KIND_ALERT, safe_notify

        safe_notify("nightly endowment step failed",
                    f"It stopped with {type(e).__name__}. The DAF report "
                    "still went out.", kind=KIND_ALERT)
        logger.error("nightly endowment step raised %s", type(e).__name__)
        return None
    if section is not None and section.get("stopped"):
        from clients.notify import KIND_ALERT, safe_notify

        stop = section["stopped"]
        rest = sum(1 for e in section["deferred"]
                   if e.get("reason") == "assignee_refused")
        safe_notify("nightly endowment step stopped: the assignee isn't a "
                    "valid CSuite employee",
                    f"CSuite refused a follow-up task for employee "
                    f"{stop.get('employee')} (CSUITE_ENDOWMENT_ASSIGNEE): "
                    f"{stop.get('error')}. Submission "
                    f"{stop.get('submission_id')} is in Needs Carl; "
                    f"{rest} later submission(s) got no writes and will be "
                    "tried again tomorrow night. Fix the variable.",
                    kind=KIND_ALERT, mode=endowment_sync.mode())
    if section is not None:
        logger.info("nightly endowment step (%s): %d candidate(s), %d "
                    "added, %d held, %d deferred, %d already handled, %d "
                    "already held", section["mode"], section["candidates"],
                    len(section["added"]), len(section["held"]),
                    len(section["deferred"]), section["skipped_handled"],
                    section["skipped_held"])
    return section


def _endowment_ids(section) -> dict:
    """Ids and counts only, for the job row."""
    return {"mode": section["mode"], "candidates": section["candidates"],
            "added_ids": [e["submission_id"] for e in section["added"]],
            "held_ids": {e["submission_id"]: e["reason"]
                         for e in section["held"]},
            "deferred_ids": {e["submission_id"]: e["reason"]
                             for e in section["deferred"]},
            "existing_task_ids": {
                e["submission_id"]: (e.get("open_task") or {}).get("task_id")
                for e in section["existing_task"]},
            "skipped_handled": section["skipped_handled"],
            "skipped_held": section["skipped_held"],
            "budget_allowed": section["budget_allowed"],
            "stopped": (section.get("stopped") or {}).get("submission_id")}
