"""Repair registration_map rows for writes HubSpot actually accepted.

Why this exists
---------------
write_audit 80, production 2026-10-09. The POST to
attendance/csuite-1463/register/create returned **201**. The read-back ran
about 0.2 seconds later and found no participation, so the run was recorded
as a failure — while the participation record's own createdAt was
18:43:59.508, roughly a second after the read-back had already given up.
The registration was in HubSpot the whole time, and the UI showed it.

The read-back now retries (see registrations.confirm_registered), so that
particular false negative should not recur. This module is for the rows the
old behaviour already left behind, and for any future 2xx whose read-back
still cannot confirm it within the backoff: a write that may have landed
must not be resent, because the attendance endpoint has no idempotency key
and a resend is how one registration becomes two.

What it does, and does not
--------------------------
It asks HubSpot. A row is only proposed when HubSpot holds a REGISTERED
participation for that contact on that event — the same per-contact check
the sync uses, not a guess from the audit trail. The audit trail is only
used to decide WHICH events are worth looking at.

DRY RUN BY DEFAULT. A dry run makes no database writes and no HubSpot
writes; it reads, and prints what it would insert. Nothing here ever writes
to HubSpot in either mode — the whole point is that the HubSpot write
already happened.
"""

import logging

from clients import database
from sync import registrations as reg

logger = logging.getLogger(__name__)

# Successful attendance writes. The endpoint carries the externalEventId, so
# the audit trail alone says which events had a write accepted — which is
# all this is used for. 2xx only: a non-2xx wrote nothing to recover.
_AUDIT_SQL = """
    SELECT id, endpoint, created_at
      FROM write_audit
     WHERE target_system = 'hubspot'
       AND http_method = 'POST'
       AND endpoint LIKE '%%marketing-events/attendance/%%'
       AND http_status >= 200 AND http_status < 300
     ORDER BY id
"""

# A row in this state is NOT a completed registration, so it is a candidate
# for repair. 'synced' rows are left alone; there is nothing to fix.
REPAIRABLE_STATUSES = ("unverified", "unknown", "review", "error")


def accepted_writes() -> dict:
    """{event_date_id: [audit_id, ...]} for every 2xx attendance write."""
    try:
        rows = database.execute_query(_AUDIT_SQL, (), fetch=True)
    except Exception as e:
        logger.warning("could not read write_audit: %s", e)
        return {}
    out = {}
    for row in rows or []:
        event_id = _event_of(row["endpoint"])
        if event_id:
            out.setdefault(event_id, []).append(row["id"])
    return out


def _event_of(endpoint) -> str:
    """The CSuite event date id in an attendance endpoint, or ''.

    .../attendance/csuite-1463/register/create?externalAccountId=...
    """
    text = str(endpoint or "").split("?")[0]
    marker = "/attendance/"
    if marker not in text:
        return ""
    tail = text.split(marker, 1)[1].split("/")[0]
    return tail[len("csuite-"):] if tail.startswith("csuite-") else ""


def run(csuite=None, hubspot=None, dry_run: bool = True,
        event_ids=None) -> dict:
    """Propose, or write, registration_map rows HubSpot can confirm.

    `event_ids` narrows the scan; by default it is every event with a 2xx
    attendance write in write_audit.
    """
    from clients.csuite import CSuiteClient
    from clients.hubspot import HubSpotClient

    out = {"dry_run": dry_run, "error": None, "proposals": [],
           "events_scanned": 0, "rows_written": 0, "csuite_calls": 0,
           "hubspot_calls": 0, "confirmed": 0, "not_in_hubspot": 0,
           "already_synced": 0, "failed_writes": 0, "events_skipped": []}

    if not reg.migration_applied():
        out["error"] = ("hubsync.registration_map does not exist, so there "
                        "is nothing to reconcile into. Run "
                        "migrations/005_registration_map.sql first.")
        return out

    accepted = accepted_writes()
    if not accepted:
        return out

    wanted = ([str(e) for e in event_ids] if event_ids
              else sorted(accepted, key=reg._sort_key))
    csuite = csuite or CSuiteClient()
    hubspot = hubspot or HubSpotClient()
    known = reg.load_map()

    for event_id in wanted:
        audit_ids = accepted.get(str(event_id)) or []
        if not audit_ids:
            out["events_skipped"].append(
                (str(event_id), "no 2xx attendance write in write_audit"))
            continue

        rows, error = reg.read_registrants(csuite, event_id)
        out["csuite_calls"] += 1
        if error:
            out["events_skipped"].append(
                (str(event_id), f"the CSuite registrant read failed: {error}"))
            continue
        out["events_scanned"] += 1

        deduped, _dropped = reg.dedupe(rows or [])
        contacts, calls, contact_error = reg.resolve_contacts(
            hubspot, set(deduped))
        out["hubspot_calls"] += calls
        if contact_error:
            out["events_skipped"].append(
                (str(event_id),
                 f"HubSpot contacts could not be read: {contact_error}"))
            continue

        # held=False deliberately: a held event is not SENT, but a row that
        # HubSpot already holds is still a row this table should carry.
        plan = reg.plan_event(event_id, rows or [], contacts, {})
        external = f"csuite-{event_id}"
        latest_audit = audit_ids[-1]

        for record in _sendable(plan):
            email = record["contact_email"]
            prior = known.get((str(event_id), email)) or {}
            status = prior.get("status")
            if status == "synced":
                out["already_synced"] += 1
                continue

            found, why = reg.registered_in_portal(
                hubspot, external, record["hubspot_contact_id"])
            out["hubspot_calls"] += 1
            if found is None:
                out["events_skipped"].append(
                    (str(event_id), f"participation read failed: {why}"))
                continue
            if not found:
                out["not_in_hubspot"] += 1
                continue

            out["confirmed"] += 1
            proposal = {
                "event_date_id": str(event_id),
                "external_event_id": external,
                "hubspot_contact_id": record["hubspot_contact_id"],
                # The fingerprint, never the address — the same line
                # _outcome_rows draws.
                "email_sha1": record["email_sha1"],
                "current_status": status or "(no row)",
                "write_audit_id": prior.get("write_audit_id") or latest_audit,
                "new_status": "synced",
            }
            out["proposals"].append(proposal)

            if not dry_run:
                stored = reg.record_registration(
                    record, external, proposal["write_audit_id"], "synced")
                if stored:
                    out["rows_written"] += 1
                else:
                    out["failed_writes"] += 1
                    proposal["new_status"] = "COULD NOT WRITE"

    return out


def _sendable(plan) -> list:
    """Every planned record that has a HubSpot contact id.

    All the buckets, not just would_register: the rows worth reconciling are
    precisely the ones an earlier run moved OUT of would_register.
    """
    records = []
    for kind in ("would_register", "unverified", "already", "held"):
        for record in plan.get(kind) or []:
            if record.get("hubspot_contact_id"):
                records.append(record)
    return records
