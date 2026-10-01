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
    # 2026-09-30, profile/edit on 21626: sent "7035550100", stored
    # "703-555-0100". CSuite punctuates it.
    "phone_number": "primary_phone_number",
    # 2026-10-01, profile/edit on 21626: four dotted keys together, all four
    # stored. CSuite then derived primary_citystatezip,
    # primary_address_string and primary_country ("US") on its own.
    "address.address": "primary_address",
    "address.city": "primary_city",
    "address.state": "primary_state",
    "address.zipcode": "primary_zipcode",
}

# A sent field whose value is an OBJECT, and the display field each of its
# subkeys lands in.
#
# 2026-10-01, profile/create/individual: `address` was sent as
# {"address", "city", "state", "zipcode"} and every part was stored. The
# read-back still reported `fields_dropped: {address: ...}` and
# `verified: false`, because it looked for a display field called `address`
# and there is none — the value fanned out across four of them.
#
# That is a false alarm of the same family as the boolean one and the
# reformatting one, and the most expensive yet: with verify_writes on, every
# create carrying an address would have told a user the address was
# discarded, at the exact moment addresses finally started working.
NESTED_TO_STORED = {
    "address": {
        "address": "primary_address",
        "city": "primary_city",
        "state": "primary_state",
        "zipcode": "primary_zipcode",
    },
}

# Fields CSuite assembles from an address it was given. Never sent, so never
# compared — they would otherwise look like unexplained changes.
DERIVED_FROM_ADDRESS = frozenset({
    "primary_citystatezip", "primary_address_string", "primary_country",
})


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


class NothingStored(FieldDropped):
    """CSuite answered success and did not touch the record at all.

    Measured 2026-09-30: `profile/edit` on 21626 was sent four address
    fields it does not recognise. It answered HTTP 200 with
    `success: true`, changed **0 of 81 fields**, and left `modified_ts`
    byte-identical.

    So `modified_ts` is the one part of an edit's result that carries
    information the success flag does not. It is checked on its own, ahead
    of the per-field comparison, because it is the stronger statement: a
    record CSuite did not write to cannot have stored anything, whatever a
    field-by-field guess concludes.

    A subclass of FieldDropped so callers that stop on a failed check keep
    stopping.
    """

    def __init__(self, endpoint: str = "", record_id=None,
                 modified_ts=None):
        self.modified_ts = modified_ts
        super().__init__({"<nothing stored>": (None, modified_ts)},
                         endpoint, record_id)
        self.args = (
            f"CSuite accepted {endpoint or 'the edit'} for record "
            f"{record_id} with success=true and did NOT touch the record: "
            f"modified_ts is unchanged at {modified_ts!r}. Nothing was "
            "stored, whichever fields were sent.",)


class ReadBackUnavailable(FieldDropped):
    """The record could not be read, so nothing is known either way.

    A subclass of FieldDropped so a caller that stops on a failed check
    keeps stopping — the sandbox path does, and should. It is separate so a
    caller that reports the result can say "not checked" instead of "field
    lost", which are different claims and only one of them is true.
    """


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
    """Is `stored` exactly what `sent` asked for?

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


def _digits(value) -> str:
    return "".join(c for c in str(value) if c.isdigit())


def _alnum(value) -> str:
    return "".join(c for c in str(value).lower() if c.isalnum())


def _reformatted(sent, stored) -> bool:
    """Did CSuite store this value in a shape of its own?

    Measured 2026-09-30: `profile/edit` was sent
    phone_number="7035550100" and stored primary_phone_number
    "703-555-0100". The value was kept in full and punctuated by the
    server.

    Without this, every phone write would be reported as dropped — a
    false alarm on a field that was stored correctly, which is exactly as
    damaging to trust in the check as a missed drop. So the test is
    deliberately narrow: the two must carry the **same digits** or the
    same letters-and-digits, and the sent value must not be empty of both.
    A different number, a truncated number, or a blank still fails.
    """
    if sent is None or stored is None:
        return False
    sent_digits, stored_digits = _digits(sent), _digits(stored)
    if sent_digits and sent_digits == stored_digits:
        return True
    sent_alnum, stored_alnum = _alnum(sent), _alnum(stored)
    return bool(sent_alnum) and sent_alnum == stored_alnum


def _as_text(value) -> str:
    """One text form for comparison.

    Booleans become "1"/"0" because that is what CSuite stores and
    returns for them — `str(True)` is "True", and comparing that against
    a stored 1 would report a drop on a field that was written fine.
    """
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value).strip()


def compare_detail(sent: dict, stored: dict, ignore=DERIVED_FIELDS):
    """(dropped, reformatted), each {sent field: (sent value, stored value)}.

    `dropped` is what CSuite did not keep. `reformatted` is what it kept in
    a shape of its own — the same value, punctuated differently. The two
    are separated because only one of them is a fault.
    """
    dropped, reformatted = {}, {}
    for field, value in (sent or {}).items():
        if field in ignore or field in ("env", "epoch"):
            continue

        # An object-valued field lands in several display fields at once.
        # Compared subkey by subkey, and reported as "address.city" so the
        # message names the part that was lost rather than the whole object.
        subkeys = NESTED_TO_STORED.get(field)
        if subkeys and isinstance(value, dict):
            for subkey, subvalue in value.items():
                target = subkeys.get(subkey)
                if target is None:
                    dropped[f"{field}.{subkey}"] = (subvalue, None)
                    continue
                held = (stored or {}).get(target)
                if _same(subvalue, held):
                    continue
                bucket = (reformatted if _reformatted(subvalue, held)
                          else dropped)
                bucket[f"{field}.{subkey}"] = (subvalue, held)
            continue

        key = stored_name(field)
        if key in ignore:
            continue
        held = (stored or {}).get(key)
        if _same(value, held):
            continue
        if _reformatted(value, held):
            reformatted[field] = (value, held)
            continue
        dropped[field] = (value, held)
    return dropped, reformatted


def compare(sent: dict, stored: dict, ignore=DERIVED_FIELDS) -> dict:
    """{sent field: (sent value, stored value)} for everything not kept."""
    return compare_detail(sent, stored, ignore)[0]


# funit/create input -> funit/display output. Measured 2026-10-01 against the
# fund sandbox-11 created without authorisation (1564): `name` is read back as
# `fund_name`, and fgroup_id / cash_account_id keep their names — except that
# the cash account comes back as `account_id`.
FUND_SENT_TO_STORED = {
    "name": "fund_name",
    "fgroup_id": "fgroup_id",
    "cash_account_id": "account_id",
}


def compare_fund(sent: dict, stored: dict) -> dict:
    """{sent field: (sent, stored)} for anything funit/create did not keep.

    A separate mapping from SENT_TO_STORED because a fund is a different
    object with its own vocabulary, and because getting this wrong on
    2026-10-01 recorded a fund's cash account as the fund's own id.
    """
    dropped = {}
    for field, target in FUND_SENT_TO_STORED.items():
        if field not in (sent or {}):
            continue
        held = (stored or {}).get(target)
        if not _same(sent[field], held) and not _reformatted(sent[field], held):
            dropped[field] = (sent[field], held)
    return dropped


def verify_fund(read, sent: dict, funit_id, display_endpoint="funit/display"):
    """Read a created fund back and raise FieldDropped if it is wrong.

    `funit/create` has never had a read-back. Sandbox-11 created fund 1564
    and nothing checked what was in it — the only reason its contents are
    known is that I chose to look afterwards.
    """
    response = read(display_endpoint, {"funit_id": funit_id})
    data = response.get("data") if isinstance(response, dict) else None
    if isinstance(data, list) and data:
        data = data[0]
    if not isinstance(data, dict) or not data.get("funit_id"):
        raise ReadBackUnavailable(
            {"<fund not found>": (funit_id, None)}, "funit/create", funit_id)

    dropped = compare_fund(sent, data)
    if dropped:
        logger.error("CSuite funit/create on %s did NOT store: %s",
                     funit_id, sorted(dropped))
        raise FieldDropped(dropped, "funit/create", funit_id)

    logger.info("read-back OK: fund %s holds what was sent", funit_id)
    return data


def verify(read, endpoint: str, sent: dict, record_id, id_field="profile_id",
           display_endpoint="profile/display", modified_before=None):
    """Read the record back and raise FieldDropped if anything was lost.

    `read(endpoint, body) -> response` is injected so this stays testable
    without a network, and so the caller keeps control of pacing.

    `modified_before` is the record's `modified_ts` as it stood before the
    write. When it is given and has not moved, the write stored nothing and
    `NothingStored` is raised — ahead of the field comparison, because it
    is the stronger evidence. It is optional because a create has no
    before-state to compare against.

    Returns the stored record on success.
    """
    response = read(display_endpoint, {id_field: record_id})
    data = response.get("data") if isinstance(response, dict) else None
    if isinstance(data, list) and data:
        data = data[0]
    if not isinstance(data, dict):
        raise ReadBackUnavailable(
            {"<read-back failed>": (None, None)}, endpoint, record_id)

    if modified_before is not None and \
            _as_text(data.get("modified_ts")) == _as_text(modified_before):
        logger.warning("CSuite %s on %s returned success and did not touch "
                       "the record: modified_ts unchanged at %s",
                       endpoint, record_id, modified_before)
        raise NothingStored(endpoint, record_id, data.get("modified_ts"))

    dropped, reformatted = compare_detail(sent, data)
    if reformatted:
        # Stored, not lost. Logged so a value the server rewrote is on the
        # record somewhere, and not raised, because nothing went wrong.
        logger.info("CSuite %s on %s stored %s in its own format",
                    endpoint, record_id, sorted(reformatted))
    if dropped:
        logger.error("CSuite %s on %s dropped: %s", endpoint, record_id,
                     sorted(dropped))
        raise FieldDropped(dropped, endpoint, record_id)

    logger.info("read-back OK: every field sent to %s was stored on %s",
                endpoint, record_id)
    return data
