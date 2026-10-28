"""
Redaction (V5.97)
=================
What may leave a value behind in `write_audit`, and the one place each is
made safe. clients/audit.py calls these; nothing here touches a database.

    payload_hash   a KEYED hash (HMAC-SHA256, JIDHR_AUDIT_HASH_KEY). A plain
                   sha256 of a small payload is a lookup, not a secret:
                   {"email": x} can be confirmed for any guessed x, and an
                   amount has a few thousand plausible values. With no key
                   (or a short one) the hash is NULL, never unkeyed.
    ids            payload_meta keeps `*_id` values because they name the
                   record a write touched. A value is kept only if it looks
                   like a record id; a key that names a PERSON's identifier
                   (tax_id, an account number) is never kept, whatever it
                   holds.
    error text     HubSpot and CSuite echo what they rejected ("x@y.org is
                   not a valid email", "5555 was not a valid long"). The
                   error column keeps the reason and loses the value.

Each one fails toward losing information, not toward keeping it: a NULL hash
or an "<id redacted>" costs a diagnosis, a kept value cannot be taken back.
"""

import hashlib
import hmac
import json
import logging
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Hash
# ---------------------------------------------------------------------------

HASH_KEY_ENV = "JIDHR_AUDIT_HASH_KEY"

# Shorter than this is treated as unset. A flag value copied into the wrong
# variable ("true") would otherwise be a key anyone could guess — the same
# typo-fails-closed rule as CSUITE_WRITE_BUDGET.
MIN_HASH_KEY_CHARS = 32

_warned_no_key = False


def hash_key() -> bytes | None:
    """The audit hash key, or None when it is unset or too short.

    Read through the MODULE (config.Config) at call time, for the reason
    clients.audit.current_csuite_env gives: tests reload config.
    """
    global _warned_no_key
    try:
        import config
        raw = str(getattr(config.Config, HASH_KEY_ENV, "") or "").strip()
    except Exception:               # pragma: no cover
        raw = ""
    if len(raw) >= MIN_HASH_KEY_CHARS:
        return raw.encode("utf-8")
    if not _warned_no_key:
        _warned_no_key = True
        logger.warning(
            "%s is %s, so write_audit rows carry no payload_hash. Set it to "
            "%d+ random characters on every service that writes, the SAME "
            "value on each.", HASH_KEY_ENV,
            "too short" if raw else "not set", MIN_HASH_KEY_CHARS)
    return None


def keyed_hash(payload) -> str | None:
    """HMAC-SHA256 of the payload in canonical JSON form, or None.

    Canonical = sorted keys and tight separators, so the same payload always
    hashes the same regardless of dict ordering. None when there is no
    payload or no usable key.
    """
    if payload is None:
        return None
    key = hash_key()
    if key is None:
        return None
    try:
        canonical = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        canonical = repr(payload)
    return hmac.new(key, canonical.encode("utf-8"),
                    hashlib.sha256).hexdigest()


# ---------------------------------------------------------------------------
# Ids
# ---------------------------------------------------------------------------

ID_REDACTED = "<id redacted>"

# Key names that identify a PERSON or a bank account, not a record in HubSpot
# or CSuite — the same tokens scripts/probe_apis.py never treats as ids.
# Matched as whole words of the key (split on _), so `ein` catches
# `org_ein_id` and not `reinvest_id`. Not `account` (cash_account_id is a
# CSuite ledger record, and the id the 2026-10-01 audit got wrong), not
# `email` (email_id is a HubSpot marketing email): an address VALUE is
# stopped by _RECORD_ID_RE whatever its key. `vender` is the ACH form's
# spelling of its vendor number.
_PERSONAL_ID_WORDS = frozenset((
    "tax", "taxid", "ssn", "ein", "tin", "itin", "fedid", "national",
    "passport", "licence", "license", "routing", "card", "iban", "swift",
    "vender", "phone", "fax", "mobile",
))

# What a record id looks like here: a CSuite number, a HubSpot number, a
# UUID, a HubSpot conversionId, or the submittedAt a submission falls back
# to (intents/daf_workflow._parse_submission). Deliberately a CHARACTER SET,
# not a list of formats: processed tracking reads hubspot_submission_id back
# out of payload_meta, and an id dropped for an unexpected shape would offer
# a handled donor again (2026-10-05). No @, no spaces, no + or ( — so an
# email, a name or a phone number never passes.
_RECORD_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")


def _personal_key(key) -> bool:
    words = re.split(r"[^a-z0-9]+", str(key).lower())
    return any(w in _PERSONAL_ID_WORDS for w in words)


def safe_id(key, value):
    """The value if it may be kept under `key` in payload_meta.ids, else
    ID_REDACTED. An int is a record id; a bool is not."""
    if _personal_key(key):
        return ID_REDACTED
    if isinstance(value, bool):
        return ID_REDACTED
    if isinstance(value, int):
        return value
    if isinstance(value, str) and _RECORD_ID_RE.match(value):
        return value
    return ID_REDACTED


# ---------------------------------------------------------------------------
# Error text
# ---------------------------------------------------------------------------

# The same pattern and marker as clients.notify.redact, so an address reads
# the same wherever it was taken out.
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_BEARER_RE = re.compile(r"(?i)\b(bearer|token|api[_-]?key|hapikey|secret)"
                        r"(\s*[:=]\s*|\s+)[^\s,;\"']+")
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_EIN_RE = re.compile(r"\b\d{2}-\d{7}\b")
_PHONE_RE = re.compile(
    r"(?<![\w-])(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}(?![\w-])")
# 13+ digits is a card or an account number. HubSpot ids run to 12, and an
# id in an error ("Existing ID: 399921857254") is what makes it actionable.
_LONG_NUMBER_RE = re.compile(r"\b\d{13,}\b")
# A JSON field that carries the rejected value itself.
_VALUE_FIELD_RE = re.compile(
    r"(\"(?:value|propertyValue|invalidValue|input)\"\s*:\s*)"
    r"(\"(?:[^\"\\]|\\.)*\"|[^,}\]\s]+)")
# HubSpot's "<value> was not a valid long." / "... was not one of the
# allowed options". The reason stays; the value before it goes.
_WAS_NOT_RE = re.compile(
    r"(\"[^\"]*\"|'[^']*'|\S+)(\s+(?:was|is) not (?:a valid|one of))")


def scrub_text(text) -> str:
    """Error text with the values taken out and the reasons left in."""
    out = str(text or "")
    out = _EMAIL_RE.sub("<address redacted>", out)
    out = _BEARER_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}<redacted>", out)
    out = _VALUE_FIELD_RE.sub(r'\1"<value redacted>"', out)
    out = _WAS_NOT_RE.sub(r"<value redacted>\2", out)
    out = _SSN_RE.sub("<number redacted>", out)
    out = _EIN_RE.sub("<number redacted>", out)
    out = _PHONE_RE.sub("<phone redacted>", out)
    out = _LONG_NUMBER_RE.sub("<number redacted>", out)
    return out


def scrub_error(error, limit: int) -> str | None:
    """scrub_text, THEN cut to `limit`: cut first and an address split at
    the boundary would no longer look like one. None stays None."""
    if not error:
        return None
    return scrub_text(error)[:limit]
