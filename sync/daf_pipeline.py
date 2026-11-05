"""
DAF Recruitment pipeline record per DAF inquiry (nightly only)
==============================================================
Shazeen's decision (2026-11-05): every DAF inquiry the nightly step adds gets
a DAF Recruitment opportunity (otype_id 1003) on the donor's CSuite profile.
Called from sync/daf_sync.py after run_daf_writes; chat "new daf" never
calls it.

What CSuite allows (sandbox, 2026-10-28, opportunities 1033-1035)
-----------------------------------------------------------------
create works with otype_id + profile_id and lands at the first stage,
OUTREACH, with NO OWNER: stage and owner are ignored at create, and edit and
delete are "not available" to this API user. So the record is unowned —
Munira's follow-up task carries the assignee — and it is NOT linked to the
task: a task has one link (o + id), it stays on the profile, and the
open-task guard and the reconcile only see profile-linked tasks.

Never a second one
------------------
A profile that already has an OPEN DAF Recruitment opportunity gets no
second. opportunity/list is read whole, once per run, like task/list in
sync/open_tasks.py; fewer rows than its own count means "could not check",
and nothing is created. A rerun cannot get here at all for a handled
submission (daf_sync skips it), and one created in this run is added to the
run's index at once.

The inquiry never waits on this
-------------------------------
A failed or unchecked opportunity is reported for a person to add by hand;
the profile and task stand. Off (CSUITE_DAF_OPPORTUNITY_CREATE_ENABLED, the
default), nothing is created and the report says what WOULD be.
"""

import logging

from config import Config

logger = logging.getLogger(__name__)

LIST_LIMIT = 1000
FIRST_STAGE = "Outreach"

WOULD = "would"            # off, or preview: one would be created
CREATED = "created"
EXISTING = "existing"      # an open one is already on the profile
FAILED = "failed"          # create refused or raised
UNCHECKED = "unchecked"    # the list could not be read completely


def enabled() -> bool:
    return bool(getattr(Config, "CSUITE_DAF_OPPORTUNITY_CREATE_ENABLED",
                        False))


def otype_id() -> int:
    return int(getattr(Config, "DAF_RECRUITMENT_OTYPE_ID", 1003))


def load_open(csuite) -> tuple:
    """({profile_id: [op_id]} of OPEN DAF Recruitment opportunities, error).
    One CSuite read; a partial read is an error, never "none found"."""
    try:
        response = csuite.list_opportunities(LIST_LIMIT)
    except Exception as e:
        return {}, f"{type(e).__name__}"
    if not isinstance(response, dict) or not response.get("success"):
        return {}, str((response or {}).get("error")
                       or "opportunity/list failed")[:160]
    data = response.get("data") or {}
    rows = data.get("results") or []
    try:
        count = int(data.get("count"))
    except (TypeError, ValueError):
        return {}, ("opportunity/list gave no count, so a complete read "
                    "can't be shown")
    if len(rows) < count:
        return {}, (f"opportunity/list returned {len(rows)} of {count}, so "
                    "an open pipeline record could be missed")
    found = {}
    for row in rows:
        if str(row.get("otype_id")) != str(otype_id()):
            continue
        if str(row.get("opportunity_status") or "").lower() != "open":
            continue
        profile_id = str(row.get("profile_id") or "").strip()
        if profile_id and row.get("op_id"):
            found.setdefault(profile_id, []).append(row["op_id"])
    return found, None


def _index(csuite, cache) -> tuple:
    if "opportunities" not in cache:
        cache["opportunities"] = load_open(csuite)
    return cache["opportunities"]


def plan(csuite, profile_id, cache) -> dict:
    """What would happen, read-only. A new donor (no profile yet) has none."""
    if profile_id is None:
        return {"outcome": WOULD}
    found, error = _index(csuite, cache)
    if error:
        return {"outcome": UNCHECKED, "reason": error}
    existing = found.get(str(profile_id))
    if existing:
        return {"outcome": EXISTING, "op_id": existing[0]}
    return {"outcome": WOULD}


def ensure(csuite, profile_id, cache, audit_meta=None) -> dict:
    """Create the opportunity unless one is open on the profile. Live only,
    switch on. Never raises."""
    planned = plan(csuite, profile_id, cache)
    if planned["outcome"] != WOULD:
        return planned
    try:
        response = csuite.create_opportunity(profile_id, otype_id(),
                                             audit_meta=audit_meta)
    except Exception as e:
        logger.error("DAF opportunity create raised %s for profile %s",
                     type(e).__name__, profile_id)
        return {"outcome": FAILED, "reason": type(e).__name__}
    op_id = ((response or {}).get("data") or {}).get("op_id") \
        if isinstance(response, dict) and response.get("success") else None
    if not op_id:
        reason = str((response or {}).get("error") or "no op_id returned")
        logger.error("DAF opportunity create failed for profile %s",
                     profile_id)
        return {"outcome": FAILED, "reason": reason[:160]}
    found, _error = _index(csuite, cache)
    found.setdefault(str(profile_id), []).append(op_id)
    stage, owner = FIRST_STAGE, None
    try:
        shown = csuite.get_opportunity(op_id)
        record = (shown or {}).get("data") or {}
        if (shown or {}).get("success") and isinstance(record, dict):
            stage = record.get("stage_name") or stage
            owner = record.get("assigned_employee_id")
    except Exception:              # the read-back is a report, not a gate
        pass
    logger.info("DAF opportunity %s created on profile %s at %s", op_id,
                profile_id, stage)
    return {"outcome": CREATED, "op_id": op_id, "stage": stage,
            "owner": owner}


def line(result) -> str:
    """The morning report's line for one inquiry."""
    outcome = (result or {}).get("outcome")
    if outcome == WOULD:
        return f"Would create pipeline record at {FIRST_STAGE} (unowned)."
    if outcome == CREATED:
        owned = ("unowned" if not result.get("owner")
                 else f"owner {result['owner']}")
        return (f"Pipeline record {result['op_id']} created at "
                f"{result.get('stage') or FIRST_STAGE} ({owned}).")
    if outcome == EXISTING:
        return (f"Already has open pipeline record {result['op_id']}; no "
                "second one.")
    if outcome == UNCHECKED:
        return ("Pipeline record: CSuite's pipeline list couldn't be read "
                "completely, so none was made.")
    return "Pipeline record NOT created (CSuite refused or didn't answer)."


def problem(result) -> str:
    """The Needs-Carl wording when a live, switched-on create didn't happen."""
    if (result or {}).get("outcome") == UNCHECKED:
        return ("no pipeline record was made (CSuite's pipeline list couldn't "
                "be read completely) — add it by hand at Outreach")
    return ("the pipeline record wasn't created — add it by hand at "
            "Outreach")
