"""
Read-back verification
======================
After a CSuite write, read the record and check that what was sent is
what was stored.

Why
---
2026-09-30: `profile/create/individual` was called with
`primary_email="hubsync-sentinel@example.invalid"`. CSuite returned
HTTP 200 with a `profile_id`. `profile/display` on that id showed
`primary_email: None` — the field exists, and nothing was stored in it.

**CSuite validates the fields it knows and discards the rest without
comment.** A 200 means the request was accepted, not that the data was
stored. The same behaviour was seen on the read side, where an
unrecognised `profile/list` filter returned all 18,797 rows instead of
erroring.

Nothing in the response distinguishes stored from ignored. The only
check that works is to read the record back and compare, which is what
this does.

Email normalisation
-------------------
`primary_email` matching is **exact and case-sensitive** (verified
2026-09-30: the same address uppercased returns 0). So an address must be
normalised the same way before it is sent and before it is searched for,
or a duplicate check will miss a record that differs only in case and
create a second one.
"""

import logging

logger = logging.getLogger(__name__)

# Fields CSuite derives rather than stores as given. Comparing them would
# report a difference on every write: `name` and `label` are assembled
# from the name parts, `ptype` from which create endpoint was used.
DERIVED_FIELDS = frozenset({
    "name", "label", "ptype", "profile_guid", "created_ts", "modified_ts",
    "individual", "profile_id",
})

# What a sent field is called when it is read back. CSuite's create input
# names are not always its display output names — that is the whole
# reason this module exists — so the mapping is explicit and empty until
# a pair has actually been observed.
SENT_TO_STORED = {
    "email": "primary_email",
}


class FieldDropped(RuntimeError):
    """CSuite accepted the write and did not store some of it."""

    def __init__(self, dropped: dict, endpoint: str = "", record_id=None):
        self.dropped = dropped
        self.endpoint = endpoint
        self.record_id = record_id
        names = ", ".join(sorted(dropped))
        super().__init__(
            f"CSuite accepted {endpoint or 'the write'} for record "
            f"{record_id} and did NOT store: {names}. A 200 means the "
            "request was accepted, not that the data was kept.")


def normalise_email(value):
    """Trimmed and lowercased, or None.

    Applied before sending AND before searching. `primary_email` matching
    is exact and case-sensitive, so the two have to agree or a duplicate
    check misses a record it should have found.
    """
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def normalise_payload(payload: dict) -> dict:
    """A copy of `payload` with every email field normalised."""
    out = dict(payload or {})
    for key in list(out):
        if "email" in key.lower() and isinstance(out[key], str):
            out[key] = normalise_email(out[key])
    return out


def stored_name(sent_field: str) -> str:
    """What `sent_field` is called when it is read back."""
    return SENT_TO_STORED.get(sent_field, sent_field)


def _same(sent, stored) -> bool:
    """Is `stored` what `sent` asked for?

    Compared as text, because CSuite returns 1 for a boolean and "1005"
    for an integer id often enough that strict equality would report
    differences that are not differences. Emails are compared normalised,
    for the same reason they are sent normalised.
    """
    if sent is None:
        return True
    if stored is None:
        return False
    if isinstance(sent, str) and "@" in sent:
        return normalise_email(sent) == normalise_email(stored)
    return _as_text(sent) == _as_text(stored)


def _as_text(value) -> str:
    """One text form for comparison.

    Booleans become "1"/"0" because that is what CSuite stores and
    returns for them — `str(True)` is "True", and comparing that against
    a stored 1 would report a drop on a field that was written fine.
    """
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value).strip()


def compare(sent: dict, stored: dict, ignore=DERIVED_FIELDS) -> dict:
    """{sent field: (sent value, stored value)} for everything not kept."""
    dropped = {}
    for field, value in (sent or {}).items():
        if field in ignore or field in ("env", "epoch"):
            continue
        key = stored_name(field)
        if key in ignore:
            continue
        if not _same(value, (stored or {}).get(key)):
            dropped[field] = (value, (stored or {}).get(key))
    return dropped


def verify(read, endpoint: str, sent: dict, record_id, id_field="profile_id",
           display_endpoint="profile/display"):
    """Read the record back and raise FieldDropped if anything was lost.

    `read(endpoint, body) -> response` is injected so this stays testable
    without a network, and so the caller keeps control of pacing.

    Returns the stored record on success.
    """
    response = read(display_endpoint, {id_field: record_id})
    data = response.get("data") if isinstance(response, dict) else None
    if isinstance(data, list) and data:
        data = data[0]
    if not isinstance(data, dict):
        raise FieldDropped(
            {"<read-back failed>": (None, None)}, endpoint, record_id)

    dropped = compare(sent, data)
    if dropped:
        logger.error("CSuite %s on %s dropped: %s", endpoint, record_id,
                     sorted(dropped))
        raise FieldDropped(dropped, endpoint, record_id)

    logger.info("read-back OK: every field sent to %s was stored on %s",
                endpoint, record_id)
    return data
