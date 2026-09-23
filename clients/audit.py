"""
Write Audit
===========
One row in `write_audit` for every write Jidhr makes to HubSpot or CSuite.

Why it lives at the client layer: handlers change often and each one would
have to remember to log. `_request`/`_send` is the single place every write
must pass through, so hooking there is the only version that cannot be
forgotten when a new handler is added.

What is recorded, and what is deliberately not:

    recorded      who, when, which system, which endpoint, which record id,
                  whether it worked, how long it took, and a hash of the
                  payload so two identical writes can be spotted
    NOT recorded  any field VALUE — no emails, no amounts, no note bodies

payload_meta carries the *shape* of a payload (its top-level key names, list
lengths, and any id values) so a row is diagnosable without the audit table
becoming a second copy of the donor database. A hash lets you prove two
writes carried the same payload without being able to read either.

Auditing never changes what a caller sees. Every failure in here is caught
and logged; the real write's result is returned untouched. An audit trail
that can take down a donation sync is worse than no audit trail.

The `write_audit` table is created out of band — this module only inserts.
"""

import hashlib
import json
import logging
import re

logger = logging.getLogger(__name__)

# Imported lazily, not at module scope. `intents.context` is inside the
# `intents` package, so importing it here would run intents/__init__.py,
# which imports every handler, one of which imports sync/, which imports
# this client — a cycle. The lookup is cached after the first call.
_context_vars = None


def _get_context_vars():
    """(current_actor, current_intent), imported on first use."""
    global _context_vars
    if _context_vars is None:
        from intents.context import current_actor, current_intent
        _context_vars = (current_actor, current_intent)
    return _context_vars


def _current_intent():
    try:
        return _get_context_vars()[1].get()
    except Exception:  # pragma: no cover - import failure is not fatal here
        return None

_INSERT_SQL = """
    INSERT INTO write_audit (
        actor_user_id, actor_label, intent, target_system, http_method,
        endpoint, target_id, payload_hash, payload_meta, status,
        http_status, error, duration_ms, sync_run_id
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s)
"""

# The pre-flight pair. RESERVE goes in before the HTTP call and COMPLETE
# stamps the outcome on the same row afterwards.
#
# ASSUMPTION, unverified: `write_audit` has an `id` primary key. The table
# is created out of band and this host cannot reach the database to check
# (DATABASE_URL names Railway's private host). If the column is absent,
# reserve_write raises AuditUnavailable and every write refuses — loudly,
# which is the correct direction to fail, but confirm the column before
# deploying this.
_RESERVE_SQL = _INSERT_SQL + " RETURNING id"

_COMPLETE_SQL = """
    UPDATE write_audit
       SET status = %s, http_status = %s, error = %s, duration_ms = %s
     WHERE id = %s
"""

ATTEMPTED = "attempted"


class AuditUnavailable(RuntimeError):
    """The audit store would not take a row, so the write must not happen.

    Raised by reserve_write only. It is deliberately a hard error: a write
    that cannot be recorded is a write nobody can account for later, and
    the 2026-09-23 email probe is what that costs — two HubSpot writes
    with no row between them, discovered only because someone looked.
    """

# Error text is truncated: a stack-trace-sized error would dominate the row
# and add nothing a log line does not already have.
MAX_ERROR_CHARS = 500

# A path segment that looks like a record id rather than a collection name.
_ID_SEGMENT_RE = re.compile(
    r"^(?:\d+|[0-9a-fA-F]{8}-[0-9a-fA-F-]{27,})$")

_ID_KEY_RE = re.compile(r"(^id$|_id$|_guid$|^guid$)", re.IGNORECASE)

# Logged once, not per write: a missing DATABASE_URL would otherwise produce
# one warning per API call for the life of the process.
_warned_no_database = False


# ---------------------------------------------------------------------------
# Payload summarising
# ---------------------------------------------------------------------------

def payload_hash(payload) -> str | None:
    """sha256 of the payload in canonical JSON form.

    Canonical = sorted keys and tight separators, so the same payload always
    hashes the same regardless of dict ordering.
    """
    if payload is None:
        return None
    try:
        canonical = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        canonical = repr(payload)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _collect_ids(payload, prefix="", depth=0):
    """Id-looking values from a payload, one level into nested dicts.

    Ids are identifiers, not content: recording that a write targeted
    contact 701 is the point of the audit. Everything else stays out.
    """
    found = {}
    if not isinstance(payload, dict) or depth > 1:
        return found

    for key, value in payload.items():
        path = f"{prefix}{key}"
        if isinstance(value, dict):
            found.update(_collect_ids(value, f"{path}.", depth + 1))
        elif isinstance(value, (str, int)) and _ID_KEY_RE.search(str(key)):
            found[path] = value
    return found


def payload_meta(payload) -> dict:
    """The shape of a payload — never its values.

    Returns top-level key names, element counts for lists, and any id values.
    """
    meta = {}

    if payload is None:
        return {"keys": [], "ids": {}}

    if isinstance(payload, dict):
        meta["keys"] = sorted(str(k) for k in payload)
        counts = {
            str(k): len(v) for k, v in payload.items()
            if isinstance(v, (list, tuple))
        }
        if counts:
            meta["count"] = counts
        # A HubSpot write wraps its fields in "properties"; the names of
        # those fields are shape, their values are not.
        nested = payload.get("properties")
        if isinstance(nested, dict):
            meta["property_keys"] = sorted(str(k) for k in nested)
        meta["ids"] = _collect_ids(payload)
    elif isinstance(payload, (list, tuple)):
        meta["keys"] = []
        meta["count"] = len(payload)
        meta["ids"] = {}
    else:
        meta["keys"] = []
        meta["ids"] = {}

    return meta


# ---------------------------------------------------------------------------
# Target id
# ---------------------------------------------------------------------------

def target_id_from_endpoint(endpoint: str) -> str | None:
    """The record id in a URL path, e.g. crm/v3/objects/contacts/701 -> 701."""
    if not endpoint:
        return None
    path = str(endpoint).split("?")[0].rstrip("/")
    last = path.rsplit("/", 1)[-1]
    return last if _ID_SEGMENT_RE.match(last) else None


def target_id_from_payload(payload) -> str | None:
    """The first id-looking value in a payload."""
    ids = _collect_ids(payload) if isinstance(payload, dict) else {}
    if not ids:
        return None
    # Prefer a bare "id"/"*_id" at the top level over a nested one.
    for key in sorted(ids, key=lambda k: (k.count("."), k)):
        return str(ids[key])
    return None


def resolve_target_id(endpoint: str, payload) -> str | None:
    """Path id first, payload id second."""
    return target_id_from_endpoint(endpoint) or target_id_from_payload(payload)


# ---------------------------------------------------------------------------
# Actor
# ---------------------------------------------------------------------------

def _actor_fields():
    """(actor_user_id, actor_label) for the current request.

    Writes made outside a request — a cron sync, a script — have no actor.
    They are labelled rather than left blank, so an unattributed write is
    visibly a system write and not a missing value.
    """
    current_actor, current_intent = _get_context_vars()
    actor = current_actor.get()
    intent = current_intent.get()

    if actor is None:
        return None, f"system:{intent or 'background'}"

    user_id = getattr(actor, "user_id", None)
    email = getattr(actor, "email", None)
    return user_id, email or f"user:{user_id}"


# ---------------------------------------------------------------------------
# The recorder
# ---------------------------------------------------------------------------

def _row_values(target_system, http_method, endpoint, target_id, payload,
                status, http_status, error, duration_ms, sync_run_id):
    actor_user_id, actor_label = _actor_fields()
    if target_id is None:
        target_id = resolve_target_id(endpoint, payload)
    return (
        actor_user_id,
        actor_label,
        _current_intent(),
        target_system,
        (http_method or "").upper(),
        endpoint,
        str(target_id) if target_id is not None else None,
        payload_hash(payload),
        json.dumps(payload_meta(payload), default=str),
        status,
        http_status,
        str(error)[:MAX_ERROR_CHARS] if error else None,
        int(duration_ms) if duration_ms is not None else None,
        sync_run_id,
    )


def reserve_write(target_system: str, http_method: str, endpoint: str,
                  target_id=None, payload=None, sync_run_id=None):
    """Claim an audit row BEFORE the write happens. Returns its id.

    Raises AuditUnavailable if the row cannot be stored, and the caller
    must then not perform the write. This is the whole point of the
    2026-09-23 redesign: auditing used to run after the fact and swallow
    its own failures, so when the store was unreachable every write to
    HubSpot and CSuite still went out, unrecorded, with nothing upstream
    able to tell.

    The row lands with status 'attempted'. complete_write stamps the
    outcome on it. A crash in between therefore leaves a visible
    'attempted' row rather than silence — which is the second reason for
    the pair, and the one that matters when a process is killed
    mid-flight the way two mirror_refresh runs were on 2026-09-15.
    """
    try:
        from clients.database import execute_query, is_configured
    except Exception as e:  # pragma: no cover - import failure is fatal here
        raise AuditUnavailable(f"audit store unavailable: {e}") from e

    if not is_configured():
        raise AuditUnavailable(
            "DATABASE_URL is not set, so this write cannot be audited and "
            "will not be attempted. Set DATABASE_URL (or DATABASE_PUBLIC_URL "
            "for a run outside Railway) and retry.")

    try:
        found = execute_query(
            _RESERVE_SQL,
            _row_values(target_system, http_method, endpoint, target_id,
                        payload, ATTEMPTED, None, None, None, sync_run_id),
            fetch=True,
        )
    except Exception as e:
        raise AuditUnavailable(
            f"could not reserve an audit row for {http_method} {endpoint}, "
            f"so the write was not attempted: {e}") from e

    row = found[0] if isinstance(found, (list, tuple)) and found else None
    if row is None:
        raise AuditUnavailable(
            f"the audit store accepted no row for {http_method} {endpoint}, "
            "so the write was not attempted")
    return row.get("id") if isinstance(row, dict) else row[0]


def complete_write(reservation, status: str = "success", http_status=None,
                   error=None, duration_ms=None) -> bool:
    """Stamp the outcome on a reserved row. Returns True if it landed.

    Never raises, and deliberately does NOT refuse anything: by the time
    this runs the write has already happened, so re-raising would hide a
    real result behind a bookkeeping failure. A lost completion leaves
    the 'attempted' row standing, which is visible and findable — the
    failure mode this design is built to produce instead of silence.
    """
    if reservation is None:
        return False
    try:
        from clients.database import execute_query
        execute_query(
            _COMPLETE_SQL,
            (status, http_status,
             str(error)[:MAX_ERROR_CHARS] if error else None,
             int(duration_ms) if duration_ms is not None else None,
             reservation),
            fetch=False,
        )
        return True
    except Exception as e:
        logger.error(
            "audit row %s stays 'attempted' — the write happened but its "
            "outcome could not be stored: %s", reservation, e, exc_info=True)
        return False


def record_write(target_system: str, http_method: str, endpoint: str,
                 target_id=None, payload=None, status: str = "success",
                 http_status=None, error=None, duration_ms=None,
                 sync_run_id=None) -> bool:
    """Record one finished write in a single row. Returns True on success.

    For writes that have ALREADY happened and cannot be un-happened —
    replaying var/audit_spool/api_writes_pending.jsonl, and the 'skipped'
    rows a client writes when it never had credentials to try with.
    Raises AuditUnavailable if the row cannot be stored, so a lost row is
    never silent.

    Live writes do NOT use this. They use reserve_write/complete_write,
    so the audit row exists before the request leaves the process.
    """
    try:
        from clients.database import execute_query, is_configured
    except Exception as e:  # pragma: no cover
        raise AuditUnavailable(f"audit store unavailable: {e}") from e

    if not is_configured():
        raise AuditUnavailable(
            "DATABASE_URL is not set, so this write cannot be audited.")

    try:
        execute_query(
            _INSERT_SQL,
            _row_values(target_system, http_method, endpoint, target_id,
                        payload, status, http_status, error, duration_ms,
                        sync_run_id),
            fetch=False,
        )
        return True
    except Exception as e:
        raise AuditUnavailable(
            f"could not record the audit row for {http_method} {endpoint}: "
            f"{e}") from e
