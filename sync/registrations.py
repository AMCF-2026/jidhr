"""CSuite event registrants to HubSpot marketing-event attendance.

PHASE 1 IS PREVIEW ONLY. It plans, counts and reports; there is no write
path, and a live run refuses even with the flag on. The flag exists so phase
2 has somewhere to hang.

What was measured before any of this was written (2026-10-07, production)
------------------------------------------------------------------------
* `event/display/eventdate` is the only endpoint that returns registrants —
  one call per event date. The list endpoint does not carry them.
* A registrant row is {attended, event_profile_email, event_profile_name,
  guests, profile_id, rsvp}. Nothing references a ticket, order or payment,
  so no money field can be synced from here even by accident.
* **There is no cancelled state.** A cancellation is a row that stops being
  returned, which is indistinguishable from a read that came back short.
* `attended` is set on 44 of 10,704 rows portal-wide, and on NONE of the 113
  rows across the eleven mapped events. So ATTENDED has nothing to send.
* Of 100 unique registrant emails on those eleven events, 83 resolve to
  exactly one HubSpot contact, 17 to none, and none to more than one.
* `hs_marketable_status` is read-only to the API
  (modificationMetadata.readOnlyValue = true), so this sync cannot make
  anyone a marketing contact. 76 of the 83 already are; 7 are deliberately
  non-marketing and this never touches that field.

Phase 1 scope, deliberately narrow
----------------------------------
The eleven event dates that are mapped into HubSpot. REGISTERED only —
never ATTENDED, because there is nothing to send, and never CANCELLED,
because there is no source state and the guard below has to exist first.
Attendance by CONTACT ID, so a registrant with no contact is withheld and
listed rather than quietly created.
"""

import hashlib
import json
import logging

from clients import database
from sync.readback import normalise_email

logger = logging.getLogger(__name__)

# The event dates mapped into HubSpot as of 2026-10-07. Phase 1 does not
# discover its own scope: a sync that decides for itself which events to
# touch is a sync whose blast radius changes without anyone editing it.
PHASE_ONE_EVENT_IDS = ("1153", "1155", "1157", "1159", "1168", "1429",
                       "1430", "1462", "1463", "1464", "1466")

# The only state phase 1 would ever assert.
REGISTERED = "REGISTERED"

# Cancellation is inferred, so the inference is fenced.
#
# An event whose registrant list comes back EMPTY when the map holds rows for
# it is not an event everybody cancelled; it is very likely a read that
# failed. event/display/eventdate answers success=true with whatever it has.
# And a list that has lost more than this fraction of its rows is treated the
# same way. Measured baseline: 113 rows across the eleven events, 10,704
# portal-wide, on 2026-10-07.
MAX_SHRINK = 0.34

_MAP_TABLE_SQL = """
    SELECT table_name FROM information_schema.tables
     WHERE table_schema = 'hubsync' AND table_name = 'registration_map'
"""

_MAP_SQL = """
    SELECT csuite_eventdate_id, contact_email, hubspot_contact_id,
           last_state, status, last_seen_at
      FROM hubsync.registration_map
"""

_RUN_OPEN_SQL = """
    INSERT INTO hubsync.run_log (job, applied, status)
    VALUES ('registrations_sync', %s, 'running')
    RETURNING id
"""

_RUN_CLOSE_SQL = """
    UPDATE hubsync.run_log
       SET finished_at = NOW(), status = %s, error_summary = %s,
           csuite_calls = %s, hubspot_calls = %s, event_dates_read = %s,
           created_count = %s, updated_count = %s, unchanged_count = %s,
           skipped_count = %s, review_count = %s, failed_count = %s,
           outcomes = %s::jsonb
     WHERE id = %s
"""


class RegistrationsSyncDisabled(RuntimeError):
    """A live registrations run was asked for while the flag is off."""


class PhaseOnePreviewOnly(RuntimeError):
    """The flag is on, but no write path exists yet.

    Raised rather than returning a result with zero writes: "0 registered"
    is also what a successful run over an empty event looks like, and this
    repo has spent weeks removing answers that read like that.
    """


def registrations_sync_allowed() -> bool:
    """Reads config at call time, not at import."""
    import config
    return bool(getattr(config.Config, "REGISTRATIONS_SYNC_ENABLED", False))


def email_fingerprint(email) -> str:
    """A stable short hash, for a run log that must not hold addresses.

    clients/audit.payload_meta draws the same line: an id is the point of an
    audit trail, a value is not. A fingerprint lets two runs be diffed —
    "this person was in the last preview and is not in this one" — without
    the log becoming a second copy of the contact database.
    """
    normalised = normalise_email(email) or ""
    return hashlib.sha1(normalised.encode("utf-8")).hexdigest()[:12]


def migration_applied() -> bool:
    try:
        found = database.execute_query(_MAP_TABLE_SQL, (), fetch=True)
    except Exception as e:
        logger.warning("could not check for registration_map: %s", e)
        return False
    return bool(found)


def load_map() -> dict:
    """{(event_date_id, email): row}. Empty when the table is absent."""
    try:
        rows = database.execute_query(_MAP_SQL, (), fetch=True)
    except Exception as e:
        logger.warning("could not read registration_map: %s", e)
        return {}
    return {(str(r["csuite_eventdate_id"]), str(r["contact_email"])): r
            for r in rows or []}


def read_registrants(csuite, event_date_id) -> tuple:
    """(rows, error). One CSuite call; registrants for one event date."""
    try:
        response = csuite._request("event/display/eventdate",
                                   {"event_date_id": int(event_date_id)})
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"
    if not isinstance(response, dict) or not response.get("success"):
        return None, str((response or {}).get("error") or "read failed")[:200]
    data = response.get("data")
    if isinstance(data, list):
        data = data[0] if data else {}
    if not isinstance(data, dict):
        return None, f"unreadable payload: {type(data).__name__}"
    return (data.get("profiles") or []), None


def shrink_guard(event_date_id, rows, known_count) -> str:
    """Why this event's registrant list cannot be trusted, or None.

    Only ever refuses. It does not decide that anything was cancelled —
    phase 1 asserts no cancellations at all — but the guard is here from the
    start, because the moment cancellation IS inferred the inference will be
    made from exactly this list.
    """
    if known_count and not rows:
        return (f"the registrant list came back EMPTY while "
                f"registration_map holds {known_count} row(s) for this "
                f"event. Zero registrants and a failed read are the same "
                f"response from CSuite, so nothing is inferred")
    if known_count and len(rows) < known_count * (1 - MAX_SHRINK):
        lost = known_count - len(rows)
        return (f"the registrant list fell from {known_count} to "
                f"{len(rows)} ({lost} fewer, over the "
                f"{int(MAX_SHRINK * 100)}% limit) — treated as a short read, "
                f"not as {lost} cancellations")
    return None


def dedupe(rows) -> tuple:
    """({email: row}, duplicates_dropped). Guests are NOT included.

    One registrant per normalised email per event. A guest is a different
    person on someone else's row, with a different field name
    (contact_email), and phase 1 does not register them: a guest has not
    given this address to AMCF, and 8 of the 23 guest emails on these events
    are already a registrant somewhere anyway.
    """
    out, dropped = {}, 0
    for row in rows or []:
        email = normalise_email((row or {}).get("event_profile_email"))
        if not email:
            continue
        if email in out:
            dropped += 1
            continue
        out[email] = row
    return out, dropped


def resolve_contacts(hubspot, emails) -> tuple:
    """({email: {id, marketing}}, calls, error).

    batch/read by email, asking for hs_additional_emails as well as email,
    and indexing the contact under BOTH. That matters: batch/read resolves a
    secondary address to its contact but does not echo which input produced
    which record, so keying on the canonical email alone loses the aliases.
    Measured 2026-10-07 on the eleven events — 3 of the 83 matches are
    secondary-address matches, and keying on canonical only found 80 of them
    while calling the other 3 new.

    One call per 100 emails. 100 emails over the eleven events is one call;
    the 6,966 portal-wide would be 70, which is why phase 1 is scoped.
    """
    from clients.hubspot import hubspot_error

    found, calls = {}, 0
    batch = sorted(set(e for e in emails if e))
    for start in range(0, len(batch), 100):
        chunk = batch[start:start + 100]
        try:
            response = hubspot._post("crm/v3/objects/contacts/batch/read", {
                "idProperty": "email",
                "inputs": [{"id": email} for email in chunk],
                "properties": ["email", "hs_additional_emails",
                               "hs_marketable_status"]})
        except Exception as e:
            return found, calls, f"{type(e).__name__}: {e}"
        calls += 1
        if not isinstance(response, dict):
            return found, calls, "unreadable batch/read response"
        error = hubspot_error(response)
        # A 207 with per-row errors is how batch/read reports inputs that do
        # not exist. That is the normal case here, not a failed call.
        if error and not (response.get("results") or response.get("numErrors")):
            return found, calls, error

        for record in response.get("results") or []:
            props = record.get("properties") or {}
            marketing = (str(props.get("hs_marketable_status")).lower()
                         == "true")
            entry = {"id": record.get("id"), "marketing": marketing}
            keys = [normalise_email(props.get("email"))]
            for extra in str(props.get("hs_additional_emails") or "").split(";"):
                keys.append(normalise_email(extra))
            for key in keys:
                if key:
                    found.setdefault(key, entry)
    return found, calls, None


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------

def plan_event(event_date_id, rows, contacts, known) -> dict:
    """What this event would do. No calls; decides from what it was given."""
    deduped, dropped = dedupe(rows)
    known_count = sum(1 for (event, _email) in known
                      if event == str(event_date_id))

    out = {"event_date_id": str(event_date_id),
           "registrant_rows": len(rows or []),
           "unique_emails": len(deduped),
           "duplicates_dropped": dropped,
           "known_rows": known_count,
           "would_register": [], "withheld": [], "already": [],
           "review": None}

    refusal = shrink_guard(event_date_id, rows or [], known_count)
    if refusal:
        out["review"] = refusal
        return out

    for email, row in sorted(deduped.items()):
        contact = contacts.get(email)
        record = {"event_date_id": str(event_date_id),
                  "email_sha1": email_fingerprint(email),
                  "csuite_profile_id": str(row.get("profile_id") or "") or None,
                  "rsvp": str(row.get("rsvp")) if row.get("rsvp") else None}
        if contact is None:
            # Never created. attendance/create takes a contact id, and the
            # email variant would create a contact — which is a decision
            # about who AMCF may email, not a decision for a sync.
            record["why"] = ("no HubSpot contact for this address — withheld, "
                             "not created")
            out["withheld"].append(record)
            continue
        record["hubspot_contact_id"] = contact["id"]
        # Reported, never acted on. hs_marketable_status is read-only to the
        # API, so this cannot change it and must not look as though it might.
        record["marketing"] = contact["marketing"]
        if known.get((str(event_date_id), email)) and \
                known[(str(event_date_id), email)].get("last_state") == REGISTERED:
            out["already"].append(record)
        else:
            out["would_register"].append(record)
    return out


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

def open_run(applied: bool):
    """Claim a run_log row marked 'running'. Never raises."""
    try:
        found = database.execute_query(_RUN_OPEN_SQL, (applied,), fetch=True)
    except Exception as e:
        logger.error("could not open a run_log row: %s", e)
        return None
    row = (found or [{}])[0]
    return row.get("id") if isinstance(row, dict) else None


def close_run(run_id, status, counts, outcomes=None, error_summary=None):
    """Stamp the outcome, with the per-record inputs in `outcomes`.

    The inputs, not just the counts. run_log held counts only, which is why
    "why was 1528 a create on the 6th and withheld on the 7th" could not be
    answered from anything stored — the plan's inputs lived nowhere. A
    preview writes them here from the start.

    Addresses are NOT stored: each record carries an email_sha1, so two runs
    can be diffed without the log becoming a second contact database.
    """
    if run_id is None:
        logger.warning("no run_log row to close (status would be %s)", status)
        return
    try:
        database.execute_query(_RUN_CLOSE_SQL, (
            status, str(error_summary)[:1000] if error_summary else None,
            counts.get("csuite_calls", 0), counts.get("hubspot_calls", 0),
            counts.get("events_read", 0), counts.get("would_register", 0),
            0, counts.get("already", 0), counts.get("withheld", 0),
            counts.get("review", 0), counts.get("failed", 0),
            json.dumps(outcomes or [], default=str), run_id,
        ), fetch=False)
    except Exception as e:
        logger.error("could not close run_log row %s: %s", run_id, e)


def run(csuite=None, hubspot=None, dry_run: bool = True,
        event_ids=PHASE_ONE_EVENT_IDS) -> dict:
    """Preview the registrations sync. Writes nothing to HubSpot, ever.

    A live run is refused twice over: once because the flag defaults off,
    and once because phase 1 has no write path at all.
    """
    from clients.csuite import CSuiteClient
    from clients.hubspot import HubSpotClient

    if not dry_run:
        if not registrations_sync_allowed():
            raise RegistrationsSyncDisabled(
                "REGISTRATIONS_SYNC_ENABLED is off, so nothing was read and "
                "nothing was written. Say \"sync registrations dry run\" to "
                "preview it.")
        raise PhaseOnePreviewOnly(
            "the registrations sync is preview-only: phase 1 builds no write "
            "path, so there is nothing for the flag to enable yet. Nothing "
            "was read and nothing was written.")

    out = {"dry_run": True, "error": None, "events": [], "run_id": None,
           "run_logged": False, "migration_applied": False,
           "events_read": 0, "registrant_rows": 0, "unique_emails": 0,
           "duplicates_dropped": 0, "would_register": 0, "withheld": 0,
           "already": 0, "review": 0, "review_rows": [],
           "non_marketing": 0, "csuite_calls": 0, "hubspot_calls": 0}

    have_table = migration_applied()
    out["migration_applied"] = have_table
    known = load_map() if have_table else {}

    run_id = open_run(applied=False)
    out["run_id"] = run_id
    try:
        return _run_body(out, csuite or CSuiteClient(),
                         hubspot or HubSpotClient(), event_ids, known)
    finally:
        if run_id is not None:
            close_run(run_id, "failed" if out.get("error") else "complete",
                      out, outcomes=_outcome_rows(out),
                      error_summary=out.get("error"))
            out["run_logged"] = True


def _outcome_rows(out) -> list:
    """Every per-record input the preview saw, for run_log.outcomes."""
    rows = []
    for event in out.get("events") or []:
        for kind in ("would_register", "withheld", "already"):
            for record in event.get(kind) or []:
                rows.append(dict(record, outcome=kind))
        if event.get("review"):
            rows.append({"event_date_id": event["event_date_id"],
                         "outcome": "review", "why": event["review"]})
    return rows


def _run_body(out, csuite, hubspot, event_ids, known) -> dict:
    per_event, all_emails = [], set()

    for event_id in event_ids:
        rows, error = read_registrants(csuite, event_id)
        out["csuite_calls"] += 1
        if error:
            # One unreadable event does not invalidate the others, but it is
            # never silently treated as an event with no registrants.
            per_event.append({"event_date_id": str(event_id),
                              "registrant_rows": 0, "unique_emails": 0,
                              "duplicates_dropped": 0, "known_rows": 0,
                              "would_register": [], "withheld": [],
                              "already": [],
                              "review": f"the registrant read failed: {error}"})
            continue
        out["events_read"] += 1
        deduped, _dropped = dedupe(rows)
        all_emails |= set(deduped)
        per_event.append({"_rows": rows, "event_date_id": str(event_id)})

    contacts, calls, contact_error = resolve_contacts(hubspot, all_emails)
    out["hubspot_calls"] += calls
    if contact_error:
        # Without the contact index every registrant looks new, and every
        # new registrant looks like a contact to create. Refusing is the
        # only safe answer.
        out["error"] = (f"HubSpot contacts could not be read "
                        f"({contact_error}), so a registrant with a contact "
                        f"cannot be told from one without. Nothing was "
                        f"planned.")
        return out

    events = []
    for entry in per_event:
        if "_rows" not in entry:
            events.append(entry)
            continue
        events.append(plan_event(entry["event_date_id"], entry["_rows"],
                                 contacts, known))

    out["events"] = events
    for event in events:
        out["registrant_rows"] += event["registrant_rows"]
        out["unique_emails"] += event["unique_emails"]
        out["duplicates_dropped"] += event["duplicates_dropped"]
        out["would_register"] += len(event["would_register"])
        out["withheld"] += len(event["withheld"])
        out["already"] += len(event["already"])
        if event.get("review"):
            out["review"] += 1
            out["review_rows"].append((event["event_date_id"],
                                       event["review"]))
        out["non_marketing"] += sum(
            1 for r in event["would_register"] if r.get("marketing") is False)
    return out
