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

# Which rule set a record's interactionDateTime. Recorded per record in
# run_log.outcomes, because "min(event start, run time)" read back from a
# log is not the same as knowing which half of the min a given write used.
INTERACTION_EVENT_START = "event_start"
INTERACTION_RUN_TIME = "run_time"

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
           last_state, status, last_seen_at, write_audit_id
      FROM hubsync.registration_map
"""

# POST .../attendance/{externalEventId}/{subscriberState}/create
#
# Body shape confirmed from HubSpot's marketing-events guide:
#   "provide the ID of the contact using the `vid` field within the `inputs`
#    array of your request body"
#   "provide an `inputs` object that includes the following fields:
#    `interactionDateTime`: the date and time at which the contact
#    subscribed to the event."
#
# interactionDateTime is REQUIRED — the guide lists no default — and its
# examples are Unix milliseconds (1716382579000).
#
# CSuite records no registration timestamp, so the event's own start is sent
# and the assumption is stated in the report. The doc calls the field "the
# date and time at which the contact subscribed", which is NOT the event
# start; nothing in CSuite is closer, and inventing "now" would assert that
# everyone registered at the moment the sync ran.
_ATTENDANCE_PATH = ("marketing/v3/marketing-events/attendance/"
                    "{external_event_id}/{verb}/create")

# externalAccountId is a QUERY parameter on this endpoint, not a body field.
#
# write_audit 79, production 2026-10-08: POST .../attendance/csuite-1153/
# register/create returned HTTP 400 "externalAccountId is required". The
# audit row's endpoint column carried no query string at all, and the body
# (proven by reproducing payload_hash
# 580bd74d...b89178) was exactly
#
#   {"inputs": [{"vid": 269269281527, "interactionDateTime": 1798693200000}]}
#
# HubSpot's OpenAPI spec for this endpoint marks the parameter
#
#     externalAccountId   in: query   required: false   style: form
#
# "required: false" is wrong about this portal: the external-id form of the
# path resolves an event on the PAIR (externalAccountId, externalEventId),
# which is why the GET needs it too, and omitting it is a 400. The API is
# taken over the spec.
#
# It goes in the endpoint string rather than a `params=` argument because
# _send_with_status has no params argument, and because the audit records
# the endpoint — so the query string that was actually sent lands in
# write_audit, which is precisely what 79 could not tell us.
_ATTENDANCE_QUERY = "externalAccountId"

# The path segment is a VERB, not the subscriber state.
#
# write_audit 78, production 2026-10-08: POST .../attendance/csuite-1153/
# REGISTERED/create returned HTTP 400
#
#   "Unknown state for 'REGISTERED'. Correct are 'register', 'attend' or
#    'cancel'."
#
# The 405 probes could not catch this: the path does not validate the
# segment until POST, so every spelling returned 405 and looked equally
# real — including 'BOGUS', which is recorded in the sandbox-45 notes.
#
# REGISTERED stays the internal state name, and the read-back still matches
# on it, because the breakdown response may well report the state rather
# than the verb. Only the URL segment changes.
#
# An allowlist of ONE, not a mapping of three: phase 1 asserts registration
# and nothing else, and a dict with 'attend' and 'cancel' in it is an
# invitation to pass a variable.
_PATH_VERBS = {"REGISTERED": "register"}

_UPSERT_REGISTRATION_SQL = """
    INSERT INTO hubsync.registration_map (
        csuite_eventdate_id, csuite_profile_id, contact_email, email_sha1,
        external_event_id, hubspot_contact_id, last_state, last_state_at,
        csuite_rsvp, csuite_attended, status, last_error, write_audit_id,
        last_seen_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (csuite_eventdate_id, contact_email) DO UPDATE
       SET hubspot_contact_id = EXCLUDED.hubspot_contact_id,
           last_state = EXCLUDED.last_state,
           last_state_at = EXCLUDED.last_state_at,
           csuite_rsvp = EXCLUDED.csuite_rsvp,
           csuite_attended = EXCLUDED.csuite_attended,
           status = EXCLUDED.status,
           last_error = EXCLUDED.last_error,
           write_audit_id = EXCLUDED.write_audit_id,
           last_seen_at = NOW(),
           updated_at = NOW()
    RETURNING id
"""

_LATEST_AUDIT_SQL = """
    SELECT id FROM write_audit
     WHERE target_system = 'hubspot' AND endpoint = %s
     ORDER BY id DESC LIMIT 1
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


class LimitRequired(RuntimeError):
    """A live run was asked for without a record cap.

    There is no "all of them" for this sync. 92 registrations are planned
    across eleven events; a run that writes all 92 because nobody typed a
    number is a run whose blast radius was set by omission.
    """


class RegistrationWriteStopped(RuntimeError):
    """A write failed, came back ambiguous, or could not be verified.

    Carries the outcomes so far: a run that stops has still written things,
    and a report that omits them is worse than one that says where it got to.
    """

    def __init__(self, outcomes, reason):
        super().__init__(reason)
        self.outcomes = outcomes
        self.reason = reason


def registrations_sync_allowed() -> bool:
    """Reads config at call time, not at import."""
    import config
    return bool(getattr(config.Config, "REGISTRATIONS_SYNC_ENABLED", False))


def held_event_ids() -> tuple:
    """Event dates that are never sent, from config. Read at call time.

    A LIST, not a branch: releasing an event is removing it from
    Config.REGISTRATION_HELD_EVENT_IDS, and holding a new one is adding it.
    Neither needs new logic, and neither can be done by accident in here.
    """
    import config
    return tuple(str(e).strip() for e
                 in getattr(config.Config, "REGISTRATION_HELD_EVENT_IDS", ())
                 if str(e).strip())


HELD_REASON = "held for CSuite setup review"

# registration_map statuses that must NOT be sent again. A 2xx whose
# read-back did not confirm may still be in HubSpot, and the attendance
# endpoint has no idempotency key, so a resend is how one registration
# becomes two. 'review' is here because that is what the code wrote for an
# unconfirmed read-back before this hotfix — write_audit 80's row.
UNRESENDABLE_STATUSES = ("unverified", "unknown", "review")


class EventRefused(Exception):
    """An event was asked for by id and cannot be synced."""


def resolve_requested_event(event_id) -> str:
    """One event id to sync, or EventRefused naming it.

    Refused by NAME, not by falling back to all eleven: "sync registrations
    apply event 9999" silently running the whole phase-1 scope is how a
    narrow instruction becomes a broad write.
    """
    key = str(event_id or "").strip()
    if key not in PHASE_ONE_EVENT_IDS:
        raise EventRefused(
            f"event {key} is not one of the {len(PHASE_ONE_EVENT_IDS)} "
            f"mapped events. Phase 1 does not discover its own scope, so an "
            f"event that is not in the list cannot be synced by asking for "
            f"it by id. Mapped: "
            f"{', '.join(PHASE_ONE_EVENT_IDS)}.")
    if key in held_event_ids():
        raise EventRefused(
            f"event {key} is {HELD_REASON}, so nothing was read and nothing "
            f"was sent. Remove it from REGISTRATION_HELD_EVENT_IDS to "
            f"release it.")
    return key


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

def plan_event(event_date_id, rows, contacts, known, held=False) -> dict:
    """What this event would do. No calls; decides from what it was given.

    A held event still reads and still reports — the registrant rows are
    worth seeing — but every record that would have been sent goes to
    `held` instead of `would_register`, which is the only list _apply and
    first_sends ever look at.
    """
    deduped, dropped = dedupe(rows)
    known_count = sum(1 for (event, _email) in known
                      if event == str(event_date_id))

    out = {"event_date_id": str(event_date_id),
           "registrant_rows": len(rows or []),
           "unique_emails": len(deduped),
           "duplicates_dropped": dropped,
           "known_rows": known_count,
           "would_register": [], "withheld": [], "already": [],
           "held": [], "is_held": bool(held), "unverified": [],
           "review": None}

    refusal = shrink_guard(event_date_id, rows or [], known_count)
    if refusal:
        out["review"] = refusal
        return out

    for email, row in sorted(deduped.items()):
        contact = contacts.get(email)
        record = {"event_date_id": str(event_date_id),
                  # The address is needed to write registration_map and is
                  # stripped from the run log — see _outcome_rows, which
                  # whitelists rather than blacklists.
                  "contact_email": email,
                  "email_sha1": email_fingerprint(email),
                  "csuite_profile_id": str(row.get("profile_id") or "") or None,
                  "rsvp": str(row.get("rsvp")) if row.get("rsvp") else None,
                  "attended": (str(row.get("attended"))
                               if row.get("attended") else None)}
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
        # BOTH, not either. The row left by write_audit 78 carried
        # last_state 'REGISTERED' with status 'error', so a last_state check
        # alone treated a failed write as a completed one.
        prior = known.get((str(event_date_id), email)) or {}
        if prior.get("status") in UNRESENDABLE_STATUSES:
            # A 2xx whose read-back never confirmed. Something may be in
            # HubSpot, there is no idempotency key on the attendance
            # endpoint, and resending is how one registration becomes two.
            # Not "already" either — nobody has confirmed it.
            record["why"] = (f"an earlier run left this "
                             f"{prior.get('status')} (write_audit "
                             f"{prior.get('write_audit_id')}) — not resent; "
                             f"reconcile it")
            out["unverified"].append(record)
        elif prior.get("last_state") == REGISTERED and \
                prior.get("status") == "synced":
            out["already"].append(record)
        elif held:
            record["why"] = HELD_REASON
            out["held"].append(record)
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


# ---------------------------------------------------------------------------
# Writing, and reading it back
# ---------------------------------------------------------------------------

def event_object_ids(hubspot, event_ids) -> tuple:
    """({event_date_id: hubspot objectId}, calls, error).

    The read-back is by objectId: participations/{objectId}/breakdown works,
    and the externalEventId form answers "Unable to parse value for path
    parameter: marketingEventId" (measured 2026-10-08). One listing call.
    """
    from sync import event_hubspot as eh

    index, calls, error = eh.hubspot_index(hubspot)
    if error:
        return {}, calls, error
    out = {}
    for event_id in event_ids:
        record = index.get(eh.external_id(event_id))
        if record and record.get("objectId"):
            out[str(event_id)] = str(record["objectId"])
    return out, calls, None


def event_detail(csuite, event_date_id, cache) -> dict:
    """{"name", "start_ms"} for one event date, from ONE CSuite call.

    The cache holds the event's own start, never the clamped value — the
    clamp depends on when the run happened and the event does not.
    """
    from sync import event_hubspot as eh

    key = str(event_date_id)
    if key in cache:
        return cache[key]
    response = csuite._request("event/display/eventdate",
                              {"event_date_id": int(event_date_id)})
    data = (response or {}).get("data")
    if isinstance(data, list):
        data = data[0] if data else {}
    row = data or {}
    moment, _reason = eh.start_moment(row)
    detail = {"name": eh.event_title(row) or None,
              "start_ms": int(moment.timestamp() * 1000) if moment else None}
    cache[key] = detail
    return detail


def interaction_timestamp(csuite, event_date_id, cache, now_ms) -> tuple:
    """(unix milliseconds, rule) for one record's interactionDateTime.

    min(event start, run time). HubSpot documents the field as "the date and
    time at which the contact subscribed to the event", and CSuite records no
    registration timestamp, so the value is an assumption either way. But an
    assumption can still be checked against the calendar: six of the eleven
    phase-1 events START IN THE FUTURE (measured 2026-10-08 — 1153 is
    2026-12-31, and 1168, 1429, 1463, 1464 and 1466 follow), so the event's
    own start would have claimed a registration that has not happened yet.
    The run time is the latest moment anyone could have registered by.

    Past events are unchanged: their start is the nearest available fact and
    it is already in the past, so the min is the start.

    `now_ms` is a required argument, not a clock read in here. One moment for
    the whole run keeps every record in it consistent, and it lets a test
    pin the clock — this suite has broken twice on clock rollover.
    """
    stamp = event_detail(csuite, event_date_id, cache)["start_ms"]
    if stamp is None:
        return None, None
    if stamp <= now_ms:
        return stamp, INTERACTION_EVENT_START
    return now_ms, INTERACTION_RUN_TIME


def attendance_request(external_event_id, contact_id, when) -> tuple:
    """(endpoint_with_query_string, body) for one REGISTERED write.

    The COMPLETE request in one place — path, query string and body — so a
    test can check it against HubSpot's documented shape rather than against
    a template that only proves the code agrees with itself. Both halves of
    write_audit 79's defect lived in the gap between a path template and the
    request that actually went out.

    Documented shape, POST /marketing/v3/marketing-events/attendance/
    {externalEventId}/{subscriberState}/create:

        externalEventId     path    required
        subscriberState     path    required   'register' | 'attend' | 'cancel'
        externalAccountId   query   required in practice (see above)
        inputs              body    required   array of MarketingEventSubscriber
          vid                       required   int64, the HubSpot contact id
          interactionDateTime       required   int64, unix milliseconds
          properties                required   object of string -> string

    `properties` is in the spec's required list for MarketingEventSubscriber
    and was NOT sent by 79 — the 400 never got that far, because the query
    string is validated first. An empty map satisfies the schema and sets no
    contact property: this sync asserts attendance, not field values.
    """
    from urllib.parse import urlencode
    from sync.event_hubspot import EXTERNAL_ACCOUNT_ID

    verb = _PATH_VERBS.get(REGISTERED)
    if verb is None:                      # unreachable by construction
        raise RegistrationWriteStopped(
            [], f"no path verb is allowlisted for {REGISTERED}")
    path = _ATTENDANCE_PATH.format(external_event_id=external_event_id,
                                   verb=verb)
    query = urlencode({_ATTENDANCE_QUERY: EXTERNAL_ACCOUNT_ID})
    body = {"inputs": [{"vid": int(contact_id),
                        "interactionDateTime": when,
                        "properties": {}}]}
    return f"{path}?{query}", body


def write_registration(hubspot, external_event_id, contact_id, when) -> tuple:
    """POST one REGISTERED state. (audit_id, error, ambiguous).

    Three outcomes, as everywhere else in this repo: a definite failure, an
    ambiguous one (no status at all — it may have landed, and HubSpot offers
    no idempotency key here either), and success.
    """
    from clients.hubspot import hubspot_error

    url, body = attendance_request(external_event_id, contact_id, when)
    result, status = hubspot._send_with_status("POST", url, body)

    audit_id = _latest_audit_id(url)
    if status is None:
        return audit_id, None, (hubspot_error(result)
                                or "no HTTP status came back from HubSpot")
    if not 200 <= int(status) < 300:
        return audit_id, (f"HTTP {status}: "
                          f"{hubspot_error(result) or 'no detail'}"), None
    error = hubspot_error(result)
    if error:
        return audit_id, f"HTTP {status} but the body is an error: {error}", None
    return audit_id, None, None


def _latest_audit_id(endpoint):
    """The write_audit row this POST just created, or None. Never raises."""
    try:
        found = database.execute_query(_LATEST_AUDIT_SQL, (endpoint,),
                                       fetch=True)
    except Exception as e:
        logger.warning("could not read back the write_audit id: %s", e)
        return None
    row = (found or [{}])[0]
    return row.get("id") if isinstance(row, dict) else None


# Three attempts over roughly ten seconds. The participation index is
# eventually consistent, and the first read is the one most likely to be
# early — see the comment on confirm_registered.
VERIFY_BACKOFFS = (3.0, 7.0)

# The documented filters on the breakdown endpoint, measured against this
# portal 2026-10-09: contactIdentifier accepts a contact id OR an email and
# really does filter (a different contact returns total=0), and state really
# does filter (state=ATTENDED returns total=0 for a REGISTERED-only event).
VERIFY_STATE = "REGISTERED"


def participation_url(external_event_id) -> str:
    """The documented per-event participation breakdown path.

    Keyed on (externalAccountId, externalEventId), which is the form
    HubSpot documents. `participations/{objectId}/breakdown` also answers,
    but it is not in the docs and the objectId form of the sibling
    `counters` route parses its single segment as an externalAccountId —
    so the undocumented spelling is not the one to depend on.
    """
    from sync.event_hubspot import EXTERNAL_ACCOUNT_ID

    return (f"marketing/v3/marketing-events/participations/"
            f"{EXTERNAL_ACCOUNT_ID}/{external_event_id}/breakdown")


def registered_in_portal(hubspot, external_event_id, contact_id) -> tuple:
    """(True/False/None, detail). ONE read of the per-contact state.

    True  — HubSpot holds a REGISTERED participation for this contact.
    False — the read succeeded and holds no such participation.
    None  — the read itself failed, which is not evidence either way.

    Filtered server-side by contactIdentifier and state rather than by
    scanning the event's participations, because `limit` defaults to 10:
    the eleventh registration on an event would otherwise fall off page one
    and read back as missing. Matched on the documented FIELDS —
    properties.attendanceState and associations.contact.contactId — not by
    searching a JSON blob for the id, which would also match an id that
    happened to appear in an unrelated field.
    """
    from clients.hubspot import hubspot_error

    wanted = str(contact_id)
    response = hubspot._get(participation_url(external_event_id),
                            {"contactIdentifier": wanted,
                             "state": VERIFY_STATE})
    error = hubspot_error(response)
    if error:
        return None, f"the participation read-back failed: {error}"
    results = (response or {}).get("results")
    if not isinstance(results, list):
        return None, ("the participation read-back returned no results list "
                      f"(keys: {sorted((response or {}).keys())})")
    for entry in results:
        if not isinstance(entry, dict):
            continue
        props = entry.get("properties") or {}
        contact = (entry.get("associations") or {}).get("contact") or {}
        event = (entry.get("associations") or {}).get("marketingEvent") or {}
        if str(contact.get("contactId") or "") != wanted:
            continue
        if str(event.get("externalEventId") or "") != str(external_event_id):
            continue
        if str(props.get("attendanceState") or "").upper() == VERIFY_STATE:
            return True, None
        return False, (f"HubSpot shows contact {wanted} on "
                       f"{external_event_id} as "
                       f"{props.get('attendanceState')!r}, not "
                       f"{VERIFY_STATE}")
    return False, (f"HubSpot holds no {VERIFY_STATE} participation for "
                   f"contact {wanted} on {external_event_id}")


def confirm_registered(hubspot, external_event_id, contact_id,
                       sleep=None) -> tuple:
    """(reason or None, read calls). None means VERIFIED.

    A 2xx on the POST says HubSpot accepted the request, not that it
    recorded the state — the same distinction that made every CSuite write
    read itself back. So the participation is read and this contact looked
    for.

    RETRIED, because the participation index is eventually consistent and a
    single early read is a false negative. Measured on write_audit 80
    (2026-10-09): the POST was sent at 18:43:58.186 and returned 201 after
    220ms; the read-back ran about 0.2s later and found nothing; the
    participation record's own createdAt is 18:43:59.508, roughly a second
    after we had already given up. The write had landed, the run was
    recorded as a failure, and the UI showed the registration the whole
    time.

    So "not there yet" and "not there" are only distinguishable by waiting.
    A read error does NOT consume the benefit of the doubt differently from
    an absence — both are retried, and both end as a reason string.
    """
    import time

    sleep = sleep or time.sleep
    calls = 0
    reason = None
    for attempt in range(len(VERIFY_BACKOFFS) + 1):
        if attempt:
            sleep(VERIFY_BACKOFFS[attempt - 1])
        found, detail = registered_in_portal(hubspot, external_event_id,
                                             contact_id)
        calls += 1
        if found:
            return None, calls
        reason = detail
    waited = sum(VERIFY_BACKOFFS)
    return (f"{reason} — still not there after "
            f"{len(VERIFY_BACKOFFS) + 1} reads over ~{waited:.0f}s"), calls


def record_registration(record, external_event_id, audit_id, status,
                        error=None) -> bool:
    """Write the registration_map row. Only ever called after a 2xx.

    Returns whether it landed. A row that cannot be stored does not undo a
    write that happened, so the caller reports it and stops rather than
    pretending the write did not occur.
    """
    try:
        database.execute_query(_UPSERT_REGISTRATION_SQL, (
            record["event_date_id"], record.get("csuite_profile_id"),
            record["contact_email"], record["email_sha1"],
            external_event_id, str(record.get("hubspot_contact_id") or ""),
            REGISTERED, record.get("rsvp"), record.get("attended"),
            status, str(error)[:500] if error else None, audit_id,
        ), fetch=True)
        return True
    except Exception as e:
        logger.error("could not record registration_map row for %s/%s: %s",
                     record["event_date_id"], record["email_sha1"], e)
        return False


def run(csuite=None, hubspot=None, dry_run: bool = True, limit=1,
        event_ids=PHASE_ONE_EVENT_IDS, now_ms=None, run_id=None) -> dict:
    """Preview, or write, REGISTERED states for the mapped events.

    A live run needs three things, and refuses on any of them:
    REGISTRATIONS_SYNC_ENABLED on, a `limit` (there is no "all of them"),
    and hubsync.registration_map in place — without the map a successful
    write could not be recorded, and the next run would send it again.

    `now_ms` is the run time used to clamp interactionDateTime. Read once,
    here, so every record in one run shares a moment, and overridable so a
    test can pin the clock instead of racing it.
    """
    from datetime import datetime, timezone

    from clients.csuite import CSuiteClient
    from clients.hubspot import HubSpotClient

    if now_ms is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    if not dry_run:
        if not registrations_sync_allowed():
            raise RegistrationsSyncDisabled(
                "REGISTRATIONS_SYNC_ENABLED is off, so nothing was read and "
                "nothing was written. Say \"sync registrations dry run\" to "
                "preview it.")
        if limit is None:
            raise LimitRequired(
                "a live registrations run needs a limit: 92 registrations "
                "are planned across the eleven events, and there is no "
                'phrase for "all of them". Say "sync registrations apply '
                'limit 1" and raise it deliberately.')

    out = {"dry_run": dry_run, "limit": limit, "error": None, "events": [],
           "run_id": None, "run_logged": False, "migration_applied": False,
           "events_read": 0, "registrant_rows": 0, "unique_emails": 0,
           "duplicates_dropped": 0, "would_register": 0, "withheld": 0,
           "already": 0, "review": 0, "review_rows": [],
           "held": 0, "held_events": [],
           "unverified": 0, "unverified_prior": 0,
           "non_marketing": 0, "csuite_calls": 0, "hubspot_calls": 0,
           "registered": 0, "failed": 0, "deferred": 0, "stopped": None,
           "writes_attempted": 0, "write_audit_ids": [],
           "run_time_ms": now_ms, "first_sends": [],
           "interaction_rules": {INTERACTION_EVENT_START: 0,
                                 INTERACTION_RUN_TIME: 0}}

    have_table = migration_applied()
    out["migration_applied"] = have_table
    if not dry_run and not have_table:
        raise RegistrationWriteStopped(
            [], "hubsync.registration_map does not exist, so a successful "
                "write could not be recorded and the next run would send it "
                "again. Run migrations/005_registration_map.sql first. "
                "Nothing was written.")
    known = load_map() if have_table else {}

    # A background apply opens the row BEFORE starting its thread, so chat
    # can answer "started, run_log N" immediately. Passing it in keeps one
    # row per run rather than one per layer.
    if run_id is None:
        run_id = open_run(applied=not dry_run)
    out["run_id"] = run_id
    try:
        return _run_body(out, csuite or CSuiteClient(),
                         hubspot or HubSpotClient(), event_ids, known,
                         dry_run, limit, now_ms)
    finally:
        if run_id is not None:
            close_run(run_id,
                      "failed" if (out.get("error") or out.get("stopped"))
                      else "complete",
                      out,
                      outcomes={"summary": summary_of(out),
                                "records": _outcome_rows(out)},
                      error_summary=out.get("error") or out.get("stopped"))
            out["run_logged"] = True


def _apply(out, csuite, hubspot, events, limit, now_ms):
    """Write up to `limit` REGISTERED states, verifying each one.

    Stops on the first failure, ambiguity or unverified read-back. One
    failure is evidence about the next call, and an unverified write is a
    state nobody can say HubSpot holds.
    """
    object_ids, calls, error = event_object_ids(
        hubspot, [e["event_date_id"] for e in events])
    out["hubspot_calls"] += calls
    if error:
        raise RegistrationWriteStopped(
            [], f"HubSpot marketing events could not be listed ({error}), so "
                f"the read-back has no objectId to check. Nothing was "
                f"written.")

    written = 0
    stamps = {}
    for event in events:
        event["registered"], event["failed"] = [], []
        event.setdefault("unverified", [])
        event_id = event["event_date_id"]
        external = f"csuite-{event_id}"
        object_id = object_ids.get(event_id)

        for record in list(event.get("would_register") or []):
            if written >= limit:
                out["deferred"] += 1
                continue
            if not object_id:
                event["failed"].append(dict(
                    record, error="this event is not in the HubSpot listing, "
                                  "so a write could not be read back"))
                raise RegistrationWriteStopped(
                    _outcome_rows(out),
                    f"event {event_id} has no HubSpot objectId. Nothing "
                    f"further was written.")

            # Counted only when the call is actually made. The cache is
            # per run, so the second record on an event is free, and the
            # old unconditional increment reported CSuite calls that never
            # happened.
            cached = str(event_id) in stamps
            when, rule = interaction_timestamp(csuite, event_id, stamps,
                                               now_ms)
            if not cached:
                out["csuite_calls"] += 1
            if rule:
                record["interaction_rule"] = rule
                record["interaction_at"] = when
                out["interaction_rules"][rule] += 1
            if when is None:
                event["failed"].append(dict(
                    record, error="no usable event start, so "
                                  "interactionDateTime could not be set"))
                raise RegistrationWriteStopped(
                    _outcome_rows(out),
                    f"event {event_id} has no usable start time, and "
                    f"interactionDateTime is required. Nothing further was "
                    f"written.")

            out["writes_attempted"] += 1
            audit_id, write_error, ambiguous = write_registration(
                hubspot, external, record["hubspot_contact_id"], when)
            if audit_id:
                out["write_audit_ids"].append(audit_id)

            if ambiguous:
                # It may have landed. HubSpot offers no idempotency key
                # here, so a retry is how one registration becomes two.
                record_registration(record, external, audit_id, "unknown",
                                    ambiguous)
                event["failed"].append(dict(record, audit_id=audit_id,
                                            error=ambiguous))
                raise RegistrationWriteStopped(
                    _outcome_rows(out),
                    f"the write for event {event_id} came back ambiguous "
                    f"({ambiguous}). Recorded 'unknown' and NOT retried.")
            if write_error:
                # NO registration_map row. A definite failure means nothing
                # landed, so there is nothing to record and the next run
                # must send it again.
                #
                # write_audit 78 wrote one anyway, with status 'error' AND
                # last_state 'REGISTERED' — so the record read as already
                # registered and the next run would have skipped it. The
                # failure is in write_audit, which is where a failed request
                # belongs.
                event["failed"].append(dict(record, audit_id=audit_id,
                                            error=write_error))
                raise RegistrationWriteStopped(
                    _outcome_rows(out),
                    f"the write for event {event_id} failed "
                    f"({write_error}). Nothing further was written.")

            # A 2xx says HubSpot accepted the request, not that it recorded
            # the state. Retried, because the participation index is
            # eventually consistent — see confirm_registered.
            mismatch, reads = confirm_registered(
                hubspot, external, record["hubspot_contact_id"])
            out["hubspot_calls"] += reads
            if mismatch:
                # UNVERIFIED, which is neither of the other two outcomes.
                # The write got a 2xx, so something may well be in HubSpot;
                # a row with status 'unverified' is what stops the next run
                # resending it and creating a second registration. The old
                # code wrote 'review' here, which plan_event does not treat
                # as already-registered, so the record WOULD have been
                # resent — that is the row write_audit 80 left behind.
                stored = record_registration(record, external, audit_id,
                                             "unverified", mismatch)
                out["unverified"] += 1
                record["why"] = mismatch
                event["unverified"].append(dict(record, audit_id=audit_id))
                raise RegistrationWriteStopped(
                    _outcome_rows(out),
                    f"the write for event {event_id} returned 2xx but could "
                    f"not be verified: {mismatch}. Recorded 'unverified'"
                    + ("" if stored else " (and registration_map could NOT "
                                         "be updated)")
                    + ", NOT retried, and nothing further was written. "
                      "Reconcile it rather than sending it again.")

            stored = record_registration(record, external, audit_id, "synced")
            written += 1
            out["registered"] += 1
            event["registered"].append(dict(record, audit_id=audit_id))
            if not stored:
                raise RegistrationWriteStopped(
                    _outcome_rows(out),
                    f"the registration for event {event_id} was written to "
                    f"HubSpot and verified, but registration_map could not "
                    f"be updated. The next run would send it again, so this "
                    f"one stopped.")
    return out


# What a run_log row may contain. A WHITELIST, not a blacklist: a new field
# on a record has to be added here deliberately, so the next field nobody
# thought about cannot arrive in the log by default. contact_email is absent
# on purpose — clients/audit.payload_meta draws the same line.
_LOGGED_FIELDS = ("event_date_id", "email_sha1", "csuite_profile_id",
                  "hubspot_contact_id", "marketing", "rsvp", "attended",
                  "why", "audit_id", "error",
                  # Which half of min(event start, run time) this record
                  # used, and the value it got. Neither is PII, and without
                  # the rule the log cannot say whether a given write
                  # claimed the event's start or the moment of the sync.
                  "interaction_rule", "interaction_at")


def _sort_key(value):
    """(0, int) for a numeric id, (1, str) otherwise. Numbers before text,
    and 1153 before 1462 rather than after it."""
    text = str(value or "")
    return (0, int(text), "") if text.isdigit() else (1, 0, text)


def first_sends(out, csuite=None, cache=None, now_ms=0, count=3) -> list:
    """The first `count` records a live run would send, in send order.

    A preview that says "92 would be registered" does not say WHICH one a
    limit of 1 buys. Printing the head of the queue makes the next write
    inspectable before it happens.

    With `csuite`, each row also carries the event's name and start and the
    interactionDateTime the write would use — the value and the rule that
    chose it. That is one CSuite call per DISTINCT event in the head of the
    queue (at most `count`), and it is what makes the clamp checkable
    before a write rather than after one.
    """
    queue = []
    cache = cache if cache is not None else {}
    for event in out.get("events") or []:
        for record in event.get("would_register") or []:
            event_id = record["event_date_id"]
            row = {"event_date_id": event_id,
                   "hubspot_contact_id": record.get("hubspot_contact_id"),
                   "marketing": record.get("marketing"),
                   "event_name": None, "event_start_ms": None,
                   "interaction_at": None, "interaction_rule": None}
            if csuite is not None:
                cached = str(event_id) in cache
                when, rule = interaction_timestamp(csuite, event_id, cache,
                                                   now_ms)
                if not cached:
                    out["csuite_calls"] = out.get("csuite_calls", 0) + 1
                detail = cache[str(event_id)]
                row["event_name"] = detail["name"]
                row["event_start_ms"] = detail["start_ms"]
                row["interaction_at"] = when
                row["interaction_rule"] = rule
            queue.append(row)
            if len(queue) >= count:
                return queue
    return queue


def summary_of(out) -> dict:
    """The run's report, minus anything that holds an address.

    Stored in run_log.outcomes so a run whose HTTP response was lost can
    still be reported in full — see sync/registration_jobs.py. `events` is
    the one key dropped: its records carry contact_email, which is exactly
    what _outcome_rows whitelists out of the log. Everything the report
    formatter reads is a count, an id, a hash or a timestamp.
    """
    return {key: value for key, value in (out or {}).items()
            if key != "events"}


def _outcome_rows(out) -> list:
    """Every per-record input and outcome, for run_log.outcomes."""
    rows = []
    for event in out.get("events") or []:
        for kind in ("would_register", "withheld", "already", "registered",
                     "failed", "held", "unverified"):
            for record in event.get(kind) or []:
                row = {k: record[k] for k in _LOGGED_FIELDS if k in record}
                row["outcome"] = kind
                rows.append(row)
        if event.get("review"):
            rows.append({"event_date_id": event["event_date_id"],
                         "outcome": "review", "why": event["review"]})
    return rows


def _run_body(out, csuite, hubspot, event_ids, known, dry_run=True,
              limit=1, now_ms=0) -> dict:
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

    held = held_event_ids()
    events = []
    for entry in per_event:
        if "_rows" not in entry:
            events.append(entry)
            continue
        events.append(plan_event(entry["event_date_id"], entry["_rows"],
                                 contacts, known,
                                 held=entry["event_date_id"] in held))

    # Deterministic: by event_date_id, then by contact id. A limit of 1 has
    # to buy the SAME record every time, or "apply limit 1" is a different
    # experiment on each run and a stopped run cannot be resumed by eye.
    # Sorted numerically where the ids are numeric, so 1153 precedes 1462
    # rather than following it as strings would.
    events.sort(key=lambda e: _sort_key(e["event_date_id"]))
    for event in events:
        event["would_register"].sort(
            key=lambda r: _sort_key(r.get("hubspot_contact_id")))

    out["events"] = events
    for event in events:
        out["registrant_rows"] += event["registrant_rows"]
        out["unique_emails"] += event["unique_emails"]
        out["duplicates_dropped"] += event["duplicates_dropped"]
        out["would_register"] += len(event["would_register"])
        out["withheld"] += len(event["withheld"])
        out["already"] += len(event["already"])
        # Held records are counted on their own line and are NOT in
        # would_register, so "to register" never includes one.
        out["held"] += len(event.get("held") or [])
        out["unverified_prior"] += len(event.get("unverified") or [])
        if event.get("is_held"):
            out["held_events"].append(event["event_date_id"])
        if event.get("review"):
            out["review"] += 1
            out["review_rows"].append((event["event_date_id"],
                                       event["review"]))
        out["non_marketing"] += sum(
            1 for r in event["would_register"] if r.get("marketing") is False)

    if dry_run:
        # The preview reads the events in the head of the queue so the table
        # can show the interactionDateTime the write WOULD use. Reads only.
        out["first_sends"] = first_sends(out, csuite, {}, now_ms)
        for row in out["first_sends"]:
            if row.get("interaction_rule"):
                out["interaction_rules"][row["interaction_rule"]] += 1
        return out

    try:
        _apply(out, csuite, hubspot, events, limit, now_ms)
    except RegistrationWriteStopped as stop:
        out["stopped"] = stop.reason
    # Tallied AFTER the writes, and after a stop, because the per-event
    # counts above are the PLAN and these are what happened. Without this
    # `failed` stayed 0 over a run that had just reported a 400.
    out["failed"] = sum(len(e.get("failed") or []) for e in events)
    out["registered"] = sum(len(e.get("registered") or []) for e in events)
    # This run's unverified writes, NOT the prior rows plan_event held
    # back — those are counted separately, before any write happens.
    out["unverified"] = sum(
        len(e.get("unverified") or []) for e in events) - out[
            "unverified_prior"]
    return out
