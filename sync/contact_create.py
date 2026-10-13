"""Create HubSpot contacts for registrants who do not have one.

Why this exists
---------------
Phase 1 WITHHELD a registrant with no HubSpot contact rather than creating
one, because creating a contact is a decision about who AMCF may email and
not a decision for a sync. Carl has now made that decision: withheld
registrants get a contact, and the existing registrations sync registers
them afterwards.

17 registrants are withheld across the eleven mapped events; 5 of them are
on event 1430 (run_log 47).

What this does NOT do
---------------------
* It does not register anybody. Creating and registering are separate runs
  on purpose: the registrations sync already has the verified write path,
  the retrying read-back and the registration_map row, and duplicating any
  of that here would be a second thing to keep correct. The next
  registrations sync picks these contacts up.
* It never sets marketing status, subscription status or list membership.
  Newsletter consent is a separate decision, and the property is read-only
  to the API anyway — hs_marketable_status carries readOnlyValue=True in
  this portal (verified 2026-10-12), so this code could not set it if it
  tried. A test asserts no such key reaches the request body regardless.
* It does not update anybody. An address already in HubSpot is left exactly
  as it is: the id is recorded and nothing is written.

Duplicate protection
--------------------
There is no idempotency key on contact creation either, so:

  1. Immediately before each create, HubSpot is read by email — by
     batch/read, which also resolves SECONDARY addresses. A search on the
     `email` property alone would miss a registrant whose address is an
     alias on an existing contact, and create a duplicate of a person who
     is already there.
  2. A 409 from the create is treated as "already exists", not as a
     failure. HubSpot answers 409 when the email is already on a contact's
     email or additional-email field, and puts the existing id in the
     message.

Both paths record the id they found and move on.
"""

import logging
import os
import re

from clients.notify import safe_notify
from sync import registrations as reg

logger = logging.getLogger(__name__)


def _int_from_env(name, default) -> int:
    """A positive int from the environment, or the default.

    Anything unparseable or <= 0 falls back rather than disabling the
    guard: a guard switched off by a typo is worse than no guard, because
    it looks present.
    """
    raw = (os.environ.get(name) or "").strip()
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default

CONTACT_PATH = "crm/v3/objects/contacts"

# The ONLY properties this sync will ever send. An allowlist, so a property
# nobody thought about cannot arrive in the body by someone adding a field
# to the mapper.
ALLOWED_PROPERTIES = ("email", "firstname", "lastname", "company")

# Anything whose name contains one of these is a consent or audience
# decision, not a contact detail. Checked against the body before it is
# sent, because "we would never do that" is not a guarantee.
FORBIDDEN_PROPERTY_HINTS = ("marketable", "marketing", "subscri", "optin",
                            "opt_in", "consent", "list", "legal_basis",
                            "lifecycle", "lead_status")

# HubSpot's 409 on an existing email: "A contact with the email
# 'x@y.z' already exists. Existing ID: 64732954744". The id is the only part
# worth keeping — the message carries the ADDRESS, and write_audit stores
# error text, so it is redacted before it is logged or stored.
_EXISTING_ID_RE = re.compile(r"Existing ID:\s*(\d+)")
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")


def redact(text) -> str:
    """Any email address in a message replaced by a placeholder.

    write_audit stores the error string, and HubSpot's 409 names the
    address. An audit table that must not hold addresses cannot be handed
    one by an error path.
    """
    return _EMAIL_RE.sub("<address redacted>", str(text or ""))[:400]


def mask_email(email) -> str:
    """`r***@icccschool.org`. Enough to recognise, not enough to publish.

    The domain is kept because it is what makes a listing checkable at a
    glance; the local part is what identifies a person.
    """
    text = str(email or "").strip()
    if "@" not in text:
        return "***"
    local, _, domain = text.partition("@")
    first = local[:1] or "*"
    return f"{first}***@{domain}"


def existing_id_from(result) -> str:
    """The contact id inside a 409 body, or ''.

    Two documented shapes: the id in `message`, and the id in an
    `errors[].message`. Both are searched rather than assuming one.
    """
    if not isinstance(result, dict):
        return ""
    texts = [str(result.get("message") or "")]
    for entry in result.get("errors") or []:
        if isinstance(entry, dict):
            texts.append(str(entry.get("message") or ""))
    for text in texts:
        found = _EXISTING_ID_RE.search(text)
        if found:
            return found.group(1)
    return ""


def profile_details(csuite, profile_id, cache) -> dict:
    """{first, last, company} for one CSuite profile. One call, cached.

    The registrant row carries only `event_profile_name`, a single combined
    field. profile/display carries first_name and last_name separately —
    populated on all 5 of event 1430's withheld registrants (verified
    2026-10-12) — so the names are read rather than guessed at by splitting
    a string on whitespace.
    """
    key = str(profile_id)
    if key in cache:
        return cache[key]
    detail = {"first": "", "last": "", "company": ""}
    try:
        response = csuite._request("profile/display",
                                   {"profile_id": int(profile_id)})
    except Exception as e:
        logger.warning("profile/display failed for %s: %s", key, e)
        cache[key] = detail
        return detail
    data = (response or {}).get("data")
    if isinstance(data, list):
        data = data[0] if data else {}
    row = data if isinstance(data, dict) else {}
    detail["first"] = str(row.get("first_name") or "").strip()
    detail["last"] = str(row.get("last_name") or "").strip()
    # `organization` is empty on all five of 1430's withheld registrants,
    # so this is usually absent; work_name is the fallback CSuite uses for
    # an employer on an individual profile.
    detail["company"] = str(row.get("organization")
                            or row.get("work_name") or "").strip()
    cache[key] = detail
    return detail


def contact_properties(email, detail) -> dict:
    """The request body's `properties`. Allowlisted, and never empty keys."""
    props = {"email": str(email or "").strip()}
    if (detail or {}).get("first"):
        props["firstname"] = detail["first"]
    if (detail or {}).get("last"):
        props["lastname"] = detail["last"]
    if (detail or {}).get("company"):
        props["company"] = detail["company"]
    return props


def assert_safe_properties(props):
    """Raise if the body carries anything that is a consent decision.

    Belt and braces over the allowlist: the allowlist is what the mapper
    builds from, this is what the writer refuses to send.
    """
    for key in props or {}:
        if key not in ALLOWED_PROPERTIES:
            raise ContactCreateStopped(
                f"{key!r} is not one of the properties this sync may send "
                f"({', '.join(ALLOWED_PROPERTIES)}). Nothing was sent.")
        lowered = key.lower()
        for hint in FORBIDDEN_PROPERTY_HINTS:
            if hint in lowered:
                raise ContactCreateStopped(
                    f"{key!r} looks like a consent or audience property "
                    f"({hint!r}). Newsletter and marketing status are a "
                    f"separate decision. Nothing was sent.")


def contacts_only_categories() -> tuple:
    """CSuite `event_name` values that mean "sign-up list", lowercased."""
    import config

    return tuple(
        str(c).strip().lower() for c
        in getattr(config.Config, "CONTACTS_ONLY_EVENT_CATEGORIES", ())
        if str(c).strip())


def is_contacts_only_row(event_row) -> bool:
    """Whether this event date's CATEGORY makes it a sign-up list.

    Reads CSuite's `event_name` — the parent series, which takes three
    values across all 179 dates — not `event_description`, which is the
    individual event's title.
    """
    category = str((event_row or {}).get("event_name") or "").strip().lower()
    return bool(category) and category in contacts_only_categories()


def contacts_only_event_ids() -> tuple:
    """Event dates that are a sign-up LIST, not an event. Read at call time.

    These are held for REGISTRATIONS and allowed for contact creation. 1153
    is the case it exists for: HubSpot's "All Emails" segment is "Email is
    known", so a contact existing IS the newsletter sign-up, and there is
    nothing to register anybody to.
    """
    import config

    return tuple(str(e).strip() for e
                 in getattr(config.Config, "CONTACTS_ONLY_EVENT_IDS", ())
                 if str(e).strip())


def resolve_requested_event(event_id) -> str:
    """One event id this path may run on, or EventRefused naming it.

    Same as the registrations resolver, with ONE difference: an id in
    CONTACTS_ONLY_EVENT_IDS is allowed through even though it is held,
    because the hold is about registrations. An id that is held and not
    listed there is still refused.
    """
    key = str(event_id or "").strip()
    if key not in reg.PHASE_ONE_EVENT_IDS:
        raise reg.EventRefused(
            f"event {key} is not one of the "
            f"{len(reg.PHASE_ONE_EVENT_IDS)} mapped events. Mapped: "
            f"{', '.join(reg.PHASE_ONE_EVENT_IDS)}.")
    if key in reg.held_event_ids() and key not in contacts_only_event_ids():
        raise reg.EventRefused(
            f"event {key} is {reg.HELD_REASON}, and it is not in "
            f"CONTACTS_ONLY_EVENT_IDS, so contacts are not created for it "
            f"either. Nothing was read. Add it to CONTACTS_ONLY_EVENT_IDS "
            f"to create contacts for its registrants without registering "
            f"them.")
    return key


class ContactCreateStopped(RuntimeError):
    """The run stopped before, or part way through, creating contacts."""


class LimitRequired(RuntimeError):
    """A live contact-create run was asked for without a cap."""


def find_contact(hubspot, email) -> tuple:
    """(contact_id or None, calls, error). batch/read, so aliases count."""
    contacts, calls, error = reg.resolve_contacts(hubspot, {email})
    if error:
        return None, calls, error
    found = (contacts or {}).get(reg.normalise_email(email))
    return (found or {}).get("id"), calls, None


def create_contact(hubspot, props) -> tuple:
    """(contact_id, outcome, detail). Outcomes: created, existing, failed,
    unknown.

    `unknown` is never retried: there is no idempotency key here, so a
    retry is how one person becomes two contacts.
    """
    from clients.hubspot import hubspot_error

    assert_safe_properties(props)
    result, status = hubspot._send_with_status(
        "POST", CONTACT_PATH, {"properties": props})

    if status == 409:
        # Not a failure. The person is already in HubSpot, which is the
        # outcome this run wanted.
        existing = existing_id_from(result)
        if existing:
            return existing, "existing", None
        return None, "failed", redact(
            "HubSpot answered 409 (the address is already on a contact) but "
            "the body carried no existing id: "
            + str((result or {}).get("message") or ""))

    error = hubspot_error(result)
    if status is None:
        # No status at all. It may have landed.
        return None, "unknown", redact(
            error or "no HTTP status came back from the create")
    if not 200 <= status < 300:
        return None, "failed", redact(f"HTTP {status}: {error or ''}")
    if error:
        return None, "failed", redact(f"HTTP {status} with an error body: "
                                      f"{error}")
    contact_id = str((result or {}).get("id") or "")
    if not contact_id:
        return None, "unknown", (f"HTTP {status} but the response carried no "
                                 f"contact id")
    return contact_id, "created", None


# ---------------------------------------------------------------------------
# run_log, reusing the registrations table under its own job name
# ---------------------------------------------------------------------------

JOB = "contact_create"

_RUN_OPEN_SQL = """
    INSERT INTO hubsync.run_log (job, applied, status)
    VALUES (%s, %s, 'running')
    RETURNING id
"""

_RUN_CLOSE_SQL = """
    UPDATE hubsync.run_log
       SET finished_at = NOW(), status = %s, error_summary = %s,
           csuite_calls = %s, hubspot_calls = %s, event_dates_read = %s,
           created_count = %s, skipped_count = %s, failed_count = %s,
           outcomes = %s::jsonb
     WHERE id = %s
"""


def open_run(applied: bool):
    from clients import database

    try:
        found = database.execute_query(_RUN_OPEN_SQL, (JOB, applied),
                                      fetch=True)
    except Exception as e:
        logger.error("could not open a run_log row: %s", e)
        return None
    row = (found or [{}])[0]
    return row.get("id") if isinstance(row, dict) else None


def close_run(run_id, status, out, error_summary=None):
    import json

    from clients import database

    try:
        database.execute_query(_RUN_CLOSE_SQL, (
            status, str(error_summary)[:1000] if error_summary else None,
            out.get("csuite_calls", 0), out.get("hubspot_calls", 0),
            out.get("events_read", 0), out.get("created", 0),
            out.get("existing", 0), out.get("failed", 0),
            json.dumps({"summary": summary_of(out),
                        "records": outcome_rows(out)}, default=str),
            run_id), fetch=False)
    except Exception as e:
        logger.error("could not close run_log %s: %s", run_id, e)


# What a run_log row may carry. A WHITELIST, and contact_email is NOT on it
# — the masked form and the fingerprint are, which is the same line
# _outcome_rows and clients/audit.payload_meta draw.
LOGGED_FIELDS = ("event_date_id", "csuite_profile_id", "email_masked",
                 "email_sha1", "hubspot_contact_id", "outcome", "why")


def summary_of(out) -> dict:
    """The report, minus anything holding a full address."""
    return {k: v for k, v in (out or {}).items() if k != "events"}


def outcome_rows(out) -> list:
    rows = []
    for event in out.get("events") or []:
        for kind in ("to_create", "created", "existing", "failed", "skipped"):
            for record in event.get(kind) or []:
                row = {k: record[k] for k in LOGGED_FIELDS if k in record}
                row["outcome"] = kind
                rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------

# How many non-marketing contacts the report NAMES before summarising.
NON_MARKETING_SHOWN = 25

# The most contacts one run may create, whatever limit was typed.
#
# The typed limit is a human saying "this many"; this is the code saying
# "no more than this without someone changing a setting". They guard
# different mistakes: the limit guards a slip of the fingers, this guards a
# CSuite read that comes back with far more registrants than an event has
# ever had. 17 was the largest real backlog.
MAX_CREATE_BATCH = _int_from_env("MAX_CREATE_BATCH", 25)

# New contacts take whatever the portal's per-app setting says. This is a
# STATIC note: there is no API for that setting, so the code cannot read it
# and must not claim to have.
NEW_CONTACT_MARKETING_NOTE = (
    "New contacts take the portal's connected-app marketing setting "
    "(currently ON for this app). That is a static note in the code, not a "
    "reading from the API — there is no endpoint for it. Verify at Settings "
    "> Integrations > Marketing Contacts if it matters."
)


def marketing_state(entry) -> str:
    """"marketing", "non_marketing" or "unknown" for one contact.

    NEVER a guess. hs_marketable_status is returned as the string 'true' or
    'false'; a contact whose property was not returned at all is unknown,
    not non-marketing. The boolean `marketing` on the same entry collapses
    those two, which is why the raw value is carried alongside it.
    """
    raw = (entry or {}).get("marketable_raw")
    if raw is None:
        return "unknown"
    text = str(raw).strip().lower()
    if text == "true":
        return "marketing"
    if text == "false":
        return "non_marketing"
    return "unknown"


def _tally_marketing(out, already, contacts):
    """Count the marketing status of everyone who already has a contact."""
    for record in already:
        entry = (contacts or {}).get(
            reg.normalise_email(record.get("contact_email"))) or {}
        state = marketing_state(entry)
        if state == "marketing":
            out["marketing"] += 1
        elif state == "non_marketing":
            out["non_marketing"] += 1
            out["non_marketing_rows"].append({
                "event_date_id": record.get("event_date_id"),
                "hubspot_contact_id": entry.get("id"),
                "email_masked": mask_email(record.get("contact_email")),
            })
        else:
            out["marketing_unknown"] += 1


def check_reconciles(out):
    """Both invariants, checked in code rather than left to the reader.

    A report that prints numbers which do not add up is worse than one that
    says it cannot account for them: the first looks checked.
    """
    problems = []
    rows = out.get("registrant_rows", 0)
    parts = (out.get("excluded", 0) + out.get("duplicates_dropped", 0)
             + out.get("unique_emails", 0))
    if rows != parts:
        problems.append(
            f"{rows} registrant row(s) do not account for "
            f"{out.get('excluded', 0)} excluded + "
            f"{out.get('duplicates_dropped', 0)} duplicate(s) + "
            f"{out.get('unique_emails', 0)} unique = {parts}")

    unique = out.get("unique_emails", 0)
    split = out.get("already_contacts", 0) + out.get("to_create", 0)
    if unique != split:
        problems.append(
            f"{unique} unique email(s) do not split into "
            f"{out.get('already_contacts', 0)} already a contact + "
            f"{out.get('to_create', 0)} to create = {split}")

    marketing = (out.get("marketing", 0) + out.get("non_marketing", 0)
                 + out.get("marketing_unknown", 0))
    if marketing != out.get("already_contacts", 0):
        problems.append(
            f"{out.get('already_contacts', 0)} already a contact do not "
            f"split into {out.get('marketing', 0)} marketing + "
            f"{out.get('non_marketing', 0)} non-marketing + "
            f"{out.get('marketing_unknown', 0)} unknown = {marketing}")

    out["reconcile_problems"] = problems
    out["reconciles"] = not problems
    return out["reconciles"]


def plan(csuite, hubspot, event_ids, out) -> list:
    """Which withheld registrants would get a contact, per event.

    Reuses the registrations planner so "withheld" means exactly what it
    means there — one definition of who has no HubSpot contact, not a
    second one that can disagree with the preview Carl is reading.
    """
    # Held for REGISTRATIONS is not held for contacts. An id in
    # CONTACTS_ONLY_EVENT_IDS is read and planned here; one that is held
    # and not listed there is skipped entirely, as before.
    contacts_only = contacts_only_event_ids()
    held = tuple(e for e in reg.held_event_ids() if e not in contacts_only)
    events = []
    profiles = {}
    for event_id in event_ids:
        key = str(event_id)
        if key in held:
            out["held_events"].append(key)
            events.append({"event_date_id": key, "is_held": True,
                           "to_create": [], "created": [], "existing": [],
                           "failed": [], "skipped": [], "review": None})
            continue
        if key in contacts_only:
            out["contacts_only_events"].append(key)

        rows, error, event_row = reg.read_registrants(csuite, key)
        out["csuite_calls"] += 1
        if error:
            events.append({"event_date_id": key, "is_held": False,
                           "to_create": [], "created": [], "existing": [],
                           "failed": [], "skipped": [],
                           "review": f"the registrant read failed: {error}"})
            continue
        out["events_read"] += 1
        # By CATEGORY as well as by id, so a sign-up list created next
        # month is handled without anyone editing a list. The id list is
        # still consulted first, above, because it can name one this
        # would miss.
        if key not in contacts_only and is_contacts_only_row(event_row):
            out["contacts_only_events"].append(key)
            out["contacts_only_by_category"].append(key)

        deduped, dropped, excluded = reg.classify_rows(rows or [])
        out["registrant_rows"] += len(rows or [])
        out["unique_emails"] += len(deduped)
        out["duplicates_dropped"] += dropped
        out["excluded"] += len(excluded)
        for _row, reason in excluded:
            out["excluded_reasons"][reason] = (
                out["excluded_reasons"].get(reason, 0) + 1)
        contacts, calls, contact_error = reg.resolve_contacts(
            hubspot, set(deduped))
        out["hubspot_calls"] += calls
        if contact_error:
            events.append({"event_date_id": key, "is_held": False,
                           "to_create": [], "created": [], "existing": [],
                           "failed": [], "skipped": [],
                           "review": f"HubSpot contacts could not be read "
                                     f"({contact_error}), so a registrant "
                                     f"with a contact cannot be told from "
                                     f"one without"})
            continue

        planned = reg.plan_event(key, rows or [], contacts, {})
        # Everybody who already has a contact, however the registrations
        # planner bucketed them: a contacts-only event has no registrations,
        # so "already registered" is not the question here — "already a
        # contact" is.
        already = [record for kind in ("would_register", "already", "held",
                                       "unverified")
                   for record in (planned.get(kind) or [])]
        out["already_contacts"] += len(already)
        _tally_marketing(out, already, contacts)
        entry = {"event_date_id": key, "is_held": False, "to_create": [],
                 "created": [], "existing": [], "failed": [], "skipped": [],
                 "review": planned.get("review")}
        for record in planned.get("withheld") or []:
            email = record["contact_email"]
            profile_id = record.get("csuite_profile_id")
            # Counted BEFORE the call, because profile_details caches: asking
            # afterwards always finds the key and the counter never moved.
            fresh = bool(profile_id) and str(profile_id) not in profiles
            detail = (profile_details(csuite, profile_id, profiles)
                      if profile_id else {})
            if fresh:
                out["csuite_calls"] += 1
            entry["to_create"].append({
                "event_date_id": key,
                "contact_email": email,
                "email_masked": mask_email(email),
                "email_sha1": record["email_sha1"],
                "csuite_profile_id": record.get("csuite_profile_id"),
                "properties": contact_properties(email, detail),
            })
        events.append(entry)

    # Deterministic, the same way the registrations sync is: a limit of 1
    # has to buy the same record every time.
    events.sort(key=lambda e: reg._sort_key(e["event_date_id"]))
    for event in events:
        event["to_create"].sort(key=lambda r: str(r.get("email_sha1") or ""))
    return events


def _apply(out, hubspot, events, limit):
    """Create up to `limit` contacts, stopping on the first real failure."""
    done = 0
    for event in events:
        for record in list(event.get("to_create") or []):
            if done >= limit:
                out["deferred"] += 1
                continue

            # IMMEDIATELY before the create, not once at plan time: the
            # plan may be minutes old by now, and somebody else may have
            # added the person in between.
            existing, calls, error = find_contact(hubspot,
                                                  record["contact_email"])
            out["hubspot_calls"] += calls
            if error:
                record["why"] = f"the pre-create read failed: {error}"
                event["failed"].append(record)
                raise ContactCreateStopped(
                    f"HubSpot could not be read for one of event "
                    f"{event['event_date_id']}'s registrants ({error}), so a "
                    f"duplicate could not be ruled out. Nothing further was "
                    f"created.")
            if existing:
                record["hubspot_contact_id"] = existing
                record["why"] = "already in HubSpot — found before creating"
                event["existing"].append(record)
                out["existing"] += 1
                continue

            contact_id, outcome, detail = create_contact(
                hubspot, record["properties"])
            out["writes_attempted"] += 1
            audit_id = reg._latest_audit_id(CONTACT_PATH)
            if audit_id:
                out["write_audit_ids"].append(audit_id)
            record["hubspot_contact_id"] = contact_id

            if outcome == "created":
                done += 1
                out["created"] += 1
                event["created"].append(record)
                continue
            if outcome == "existing":
                # A 409. The person is there; the create did not happen.
                record["why"] = "HubSpot answered 409 — already a contact"
                out["existing"] += 1
                event["existing"].append(record)
                continue
            if outcome == "unknown":
                record["why"] = detail
                out["unknown"] += 1
                event["failed"].append(record)
                raise ContactCreateStopped(
                    f"the create for one of event {event['event_date_id']}'s "
                    f"registrants came back ambiguous ({detail}). NOT "
                    f"retried — there is no idempotency key here, so a retry "
                    f"is how one person becomes two contacts. Nothing "
                    f"further was created.")
            record["why"] = detail
            event["failed"].append(record)
            raise ContactCreateStopped(
                f"the create for one of event {event['event_date_id']}'s "
                f"registrants failed ({detail}). Nothing further was "
                f"created.")
    return out


def _notify_stop(out, reason):
    """Tell a human a contact-create run stopped. Counts only."""
    safe_notify(
        "Jidhr: contact creation stopped",
        reason,
        path="create contacts",
        run_log=out.get("run_id"),
        events=", ".join(e["event_date_id"] for e in (out.get("events") or [])
                         if e.get("to_create")) or "none",
        rows=out.get("registrant_rows"),
        unique=out.get("unique_emails"),
        already_a_contact=out.get("already_contacts"),
        to_create=out.get("to_create"),
        created=out.get("created"),
        existing=out.get("existing"),
        failed=out.get("failed"),
        limit=out.get("limit"),
        write_audit=", ".join(str(i) for i in
                              (out.get("write_audit_ids") or [])) or "none",
    )


def run(csuite=None, hubspot=None, dry_run: bool = True, limit=None,
        event_ids=None, run_id=None) -> dict:
    """Preview, or create, HubSpot contacts for withheld registrants.

    Gated by REGISTRATIONS_SYNC_ENABLED rather than a flag of its own:
    creating these contacts exists only to feed the registrations sync, and
    a second env var to set is a second thing to forget. A live run also
    needs a limit; there is no phrase for "all of them".
    """
    from clients.csuite import CSuiteClient
    from clients.hubspot import HubSpotClient

    if event_ids is None:
        event_ids = reg.PHASE_ONE_EVENT_IDS

    if not dry_run:
        if not reg.registrations_sync_allowed():
            raise reg.RegistrationsSyncDisabled(
                "REGISTRATIONS_SYNC_ENABLED is off, so nothing was read and "
                "no contact was created. Say \"create contacts dry run\" to "
                "preview it.")
        if limit is None:
            raise LimitRequired(
                "a live contact-create run needs a limit, and there is no "
                'phrase for "all of them". Say "create contacts apply limit '
                '1" and raise it deliberately.')

    out = {"dry_run": dry_run, "limit": limit, "error": None, "events": [],
           "run_id": None, "run_logged": False, "events_read": 0,
           "to_create": 0, "created": 0, "existing": 0, "failed": 0,
           "unknown": 0, "deferred": 0, "review": 0, "review_rows": [],
           "held_events": [], "csuite_calls": 0, "hubspot_calls": 0,
           "writes_attempted": 0, "write_audit_ids": [], "stopped": None,
           "with_name": 0, "with_company": 0, "already_contacts": 0,
           "registrant_rows": 0, "unique_emails": 0, "duplicates_dropped": 0,
           "contacts_only_events": [], "contacts_only_by_category": [],
           "excluded": 0, "excluded_reasons": {},
           "marketing": 0, "non_marketing": 0, "marketing_unknown": 0,
           "non_marketing_rows": [], "reconciles": True,
           "reconcile_problems": []}

    if run_id is None:
        run_id = open_run(applied=not dry_run)
    out["run_id"] = run_id
    try:
        return _run_body(out, csuite or CSuiteClient(),
                         hubspot or HubSpotClient(), event_ids, dry_run,
                         limit)
    finally:
        if run_id is not None:
            close_run(run_id,
                      "failed" if (out.get("error") or out.get("stopped"))
                      else "complete",
                      out,
                      error_summary=out.get("error") or out.get("stopped"))
            out["run_logged"] = True


def _run_body(out, csuite, hubspot, event_ids, dry_run, limit) -> dict:
    events = plan(csuite, hubspot, event_ids, out)
    out["events"] = events
    for event in events:
        out["to_create"] += len(event["to_create"])
        if event.get("review"):
            out["review"] += 1
            out["review_rows"].append((event["event_date_id"],
                                       event["review"]))
        for record in event["to_create"]:
            props = record["properties"]
            if props.get("firstname") or props.get("lastname"):
                out["with_name"] += 1
            if props.get("company"):
                out["with_company"] += 1

    check_reconciles(out)

    if dry_run:
        return out

    # CHANGE 2: the reconcile check GATES the write now. It used to be
    # computed here and then ignored — the report warned and _apply ran
    # anyway. Numbers that do not add up mean the plan is not understood,
    # and a plan that is not understood is not one to write from.
    if not out["reconciles"]:
        out["stopped"] = (
            "the planned counts do not reconcile, so NOTHING was created: "
            + "; ".join(out["reconcile_problems"])
            + ". This is a bug in the count, not a reason to apply anyway.")
        _notify_stop(out, "counts did not reconcile")
        return out

    # CHANGE 3: a cap the typed limit cannot raise.
    cap = MAX_CREATE_BATCH
    planned = min(out["to_create"], limit) if limit else out["to_create"]
    if planned > cap:
        out["stopped"] = (
            f"this run would create {planned} contact(s), more than the "
            f"maximum of {cap}. NOTHING was created. Raise MAX_CREATE_BATCH "
            f"deliberately, or run it in smaller batches with a lower "
            f"limit.")
        _notify_stop(out, f"batch too large: would write {planned}, "
                          f"max {cap}")
        return out

    try:
        _apply(out, hubspot, events, limit)
    except ContactCreateStopped as stop:
        out["stopped"] = str(stop)
        _notify_stop(out, str(stop))
    # Tallied after the writes: the counts above are the PLAN.
    out["failed"] = sum(len(e.get("failed") or []) for e in events)
    return out
