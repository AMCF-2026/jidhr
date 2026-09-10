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

def record_write(target_system: str, http_method: str, endpoint: str,
                 target_id=None, payload=None, status: str = "success",
                 http_status=None, error=None, duration_ms=None,
                 sync_run_id=None) -> bool:
    """Record one write. Returns True if a row was inserted.

    Never raises. Callers use this for its side effect and ignore the return;
    the boolean exists so tests can assert on it.
    """
    global _warned_no_database

    try:
        from clients.database import execute_query, is_configured

        if not is_configured():
            if not _warned_no_database:
                logger.warning(
                    "DATABASE_URL is not set — writes to %s and CSuite will "
                    "not be audited for the life of this process.",
                    "HubSpot")
                _warned_no_database = True
            return False

        actor_user_id, actor_label = _actor_fields()

        if target_id is None:
            target_id = resolve_target_id(endpoint, payload)

        execute_query(
            _INSERT_SQL,
            (
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
            ),
            fetch=False,
        )
        return True

    except Exception as e:
        # Deliberately swallowed. The write itself already happened (or
        # already failed); losing its audit row must not change what the
        # caller sees, and must not turn a working sync into a broken one.
        logger.error(
            "Failed to record write audit for %s %s %s: %s",
            target_system, http_method, endpoint, e, exc_info=True)
        return False
