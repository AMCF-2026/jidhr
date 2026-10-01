"""
CSuite Client
=============
Client for CSuite Fund Accounting API with HMAC-SHA256 authentication.

Jidhr v1.3 - Complete client covering:
- Profile CRUD (Kods' DAF workflow)
- Fund CRUD + fee types (Muhi's fee calculations)
- Grant queries with date filtering (quarterly reporting)
- Donation queries with date filtering (Ramadan comparisons)
- Check tracking (Muhi's uncashed check reports)
- Voucher lookups (grant disbursement tracking)
- Event management (Lisa's event workflows)
- Task management (CSuite-side tasks)
- Account + investment strategy lookups
"""

import hashlib
import hmac
import base64
import json
import time
import logging
import requests
from config import Config
from clients.audit import (AuditUnavailable, complete_write,
                           record_write, reserve_write,
                           target_id_from_response)

logger = logging.getLogger(__name__)

# CSuite signs the JSON request body, so EVERY call is an HTTP POST — the
# verb carries no information about whether a call changes anything. The
# endpoint name is the only signal, so the rule lives here where it can be
# read, rather than being inferred at each call site.
#
# Verified against every endpoint string in this file: these five words
# catch every write and match none of the reads.
CSUITE_WRITE_PATTERNS = ("create", "edit", "delete", "complete", "update")

# Version prefixes that carry no meaning for classification. Stripped so
# `/api/v1/note/create` is judged as `note/create`, and so a future
# `/api/v3/` cannot smuggle a write past a rule that only knew about v2.
_API_PREFIX_SEGMENTS = ("api", "v1", "v2", "v3")


def _path_segments(endpoint: str) -> list:
    """The meaningful, lowercased segments of an endpoint path."""
    text = str(endpoint or "").lower().split("?")[0].split("#")[0]
    return [seg for seg in text.replace("\\", "/").split("/")
            if seg and seg not in _API_PREFIX_SEGMENTS]


def is_csuite_write(endpoint: str) -> bool:
    """True if this CSuite endpoint changes something.

    CSuite signs the request body, so every call is an HTTP POST and the
    verb says nothing about what a call does. The endpoint name is the
    only signal, which is why the rule lives here where it can be read
    rather than being re-derived at each call site.

    Matched on path SEGMENTS, not on the whole string, so every sub-path
    is caught: `profile/create/individual`, `task/edit/complete`,
    `custom_field/delete`, and `/api/v1/note/create` after its version
    prefix is stripped.

    The whole-string check is kept alongside the segment check, as a
    union. It is redundant for every endpoint known today, and it means
    this function can never become LESS strict than the substring rule it
    replaced — an endpoint like `profile/createhousehold`, where the word
    is inside a segment rather than equal to it, still reads as a write.
    """
    whole = str(endpoint or "").lower()
    if any(pattern in whole for pattern in CSUITE_WRITE_PATTERNS):
        return True
    return any(pattern in segment
               for segment in _path_segments(endpoint)
               for pattern in CSUITE_WRITE_PATTERNS)


# ---------------------------------------------------------------------------
# Which CSuite are we talking to
# ---------------------------------------------------------------------------
# Three things have to agree: the host, the `env` value inside the signed
# body, and which key/secret pair signs it. They are derived HERE, once,
# and nothing else is allowed to pick any of them independently — a host
# chosen in one place and an `env` chosen in another is how a sandbox run
# writes to the live fund ledger.

ENV_LIVE = "live"
ENV_SANDBOX = "sandbox"
VALID_ENVS = (ENV_LIVE, ENV_SANDBOX)

# A host is sandbox if its hostname says so. Matched on the hostname, not
# the whole URL, so a query string or a path cannot spoof it.
_SANDBOX_HOST_MARKER = "sandbox"


# What happened to a call, beyond "it didn't work".
#
# On 2026-09-30 a deliberate auth failure came back as
# {"success": false, "error": "Unknown error"} — the 401 had been
# swallowed, and telling an expired key from a malformed body meant
# capturing the raw response by hand. An error that does not name its own
# cause sends someone looking in the wrong place.
OUTCOME_OK = "ok"
OUTCOME_AUTH_REJECTED = "auth_rejected"      # 401 (and 403)
OUTCOME_INVALID_REQUEST = "invalid_request"  # 400, 422
OUTCOME_RATE_LIMITED = "rate_limited"        # 429
OUTCOME_SERVER_ERROR = "server_error"        # 5xx
OUTCOME_NETWORK = "network_error"            # timeout, connection refused
OUTCOME_BAD_RESPONSE = "bad_response"        # 2xx that is not JSON
OUTCOME_REJECTED = "rejected"                # HTTP 2xx, success != 1


# Response bodies are echoed back to the caller so a failure can be read
# without re-running it. CSuite never puts a credential in a response —
# the signature travels in a request header — but the cap is here anyway,
# because a body is the one place an unexpected value could appear.
MAX_BODY_CHARS = 600


def _safe_body(response) -> str:
    """The response body, length-capped, for a diagnosable error."""
    try:
        text = getattr(response, "text", "") or ""
    except Exception:  # pragma: no cover
        return ""
    flat = " ".join(str(text).split())
    return flat if len(flat) <= MAX_BODY_CHARS else flat[:MAX_BODY_CHARS] + "…"


def classify_status(status_code, exception=None) -> str:
    """The outcome name for an HTTP status, or for a transport failure."""
    if exception is not None:
        return OUTCOME_NETWORK
    if status_code is None:
        return OUTCOME_NETWORK
    code = int(status_code)
    if code in (401, 403):
        return OUTCOME_AUTH_REJECTED
    if code == 429:
        return OUTCOME_RATE_LIMITED
    if 400 <= code < 500:
        return OUTCOME_INVALID_REQUEST
    if code >= 500:
        return OUTCOME_SERVER_ERROR
    return OUTCOME_OK


# =============================================================================
# CONFIRMED INPUT FIELD NAMES
# =============================================================================
# CSuite validates the fields it recognises and discards the rest without
# comment. A create with an unrecognised field returns HTTP 200 and a
# profile_id, and the value is simply gone — measured 2026-09-30, when
# `primary_email` was sent to profile/create/individual and read back as
# None. The same happens on reads, where an unrecognised profile/list
# filter returned all 18,797 rows.
#
# So an input name is not confirmed because it appears in profile/display,
# and not because it reads like the field it sets. `primary_email` is a
# valid DISPLAY name and an invalid INPUT name: CSuite's input and output
# vocabularies are two different lists. A name goes below only after a
# value sent under it has been read back off the record.
#
# Everything reaching a write method through **kwargs is checked against
# this set BEFORE the request is built, because after the request there is
# nothing left to check: the response to a dropped field is identical to
# the response to a stored one.
CONFIRMED_INPUT_FIELDS = frozenset({
    "first_name",   # profile/create/individual -> first_name
    "last_name",    # profile/create/individual -> last_name
    "email",        # profile/create/individual, profile/edit -> primary_email
    "website",      # profile/edit -> website
    "phone_number", # profile/edit -> primary_phone_number (CSuite punctuates)
    "env",          # every endpoint; supplied by _build_payload
    "profile_id",   # profile/edit, profile/display
    # 2026-10-01, profile/edit on 21626: all four sent together, all four
    # stored. CSuite then derived primary_citystatezip,
    # primary_address_string and primary_country ("US") by itself.
    "address.address",   # -> primary_address
    "address.city",      # -> primary_city
    "address.state",     # -> primary_state
    "address.zipcode",   # -> primary_zipcode
})

# Names confirmed on SOME endpoints and disproved on others.
#
# `address` as a nested object is a confirmed input to
# profile/create/individual (2026-10-01, sentinel 21660: every part stored)
# and was silently discarded by profile/edit (2026-10-01, sentinel 21626:
# 200, nothing stored, modified_ts unchanged). One name, two answers — so a
# single flat allowlist cannot express what is now known, and a flat
# KNOWN_INVALID entry would refuse a confirmed create input.
#
# Checked against the endpoint the caller names. Anything not listed here is
# decided by CONFIRMED_INPUT_FIELDS as before.
ENDPOINT_CONFIRMED_FIELDS = {
    "address": ("profile/create/individual", "profile/create/org",
                "profile/create/household"),
    # VERIFIED 2026-10-01 by task/create -> task/display on sandbox task 1034.
    # Every one of these was sent and read back. They are endpoint-scoped
    # because `id` in particular must never become a global input name: a task
    # already has a `task_id`, and `id` here means the id of the LINKED
    # object, not of the task.
    "name": ("task/create",),            # required; text read back as task_description
    "task_description": ("task/create",),
    "employee_id": ("task/create",),
    "due_ts": ("task/create",),          # "2026-10-05" -> due_date "2026-10-05"
    "task_type_id": ("task/create",),    # 1065 -> task_type "DIY Form-Contact"
    "o": ("task/create",),               # "profile" -> o "profile"
    "id": ("task/create",),              # 21661 -> id 21661
}

# Names refused on specific endpoints EVEN THOUGH they are globally confirmed.
#
# CONFIRMED_INPUT_FIELDS is endpoint-agnostic, so `profile_id` — confirmed for
# profile/edit — was accepted on task/create, where CSuite would silently
# discard it. A task's link is `o` + `id`, not `profile_id`, VERIFIED on task
# 1034. Checked before the allowlist, so a block wins.
ENDPOINT_BLOCKED_FIELDS = {
    "profile_id": ("task/create",),
}

# Names allowed on specific endpoints that are **NOT confirmed inputs**.
#
# This set exists for one reason: `task/create` had no gate at all, so any
# name a caller invented went to CSuite to be silently discarded. A gate is
# better than no gate even when nothing has been read back — but these names
# must never be mistaken for measured ones, so they live apart from
# CONFIRMED_INPUT_FIELDS and a test asserts the two never overlap.
#
# Where they come from, 2026-10-01: `task/list` and `task/display` on the
# sandbox's seven tasks return `task_description`, `due_date`, `employee_id`,
# `task_type_id`, `task_id` and `task_guid`. They are CSuite's own names for
# task fields, which makes them the only candidates worth allowing — and
# `primary_email` was a valid display name and an invalid input, so being in
# the output vocabulary proves nothing about the input.
#
# **`name` is deliberately absent.** `create_task` sends it as a required
# argument and `task/display` has no `name` field at all. That is the
# `primary_email` shape exactly, and it is unresolved — see
# INCONCLUSIVE_PROBES.
# OUTPUT names only. Observed on a record, never sent, never proven as inputs.
#
# This set exists because of a mistake. On 2026-10-01 I read seven sandbox
# tasks, found `id`, `o` and `task_object` null on every one, and concluded
# that "a CSuite task cannot be attached to a profile". Carl then made a task
# in the CSuite UI against profile 21661 and the link was right there:
#
#     o            "profile"
#     id           21661
#     task_object  "Profile :: SENTINEL 6 - SANDBOX ONLY, HUBSYNC"
#
# The seven were simply unlinked. A feature nobody had used looked like a
# feature that did not exist, and I generalised from the only sample I had —
# the same error as reading one address key alone and calling it invalid.
#
# These names are recorded so the knowledge is not lost, and kept OUT of
# CONFIRMED_INPUT_FIELDS and ENDPOINT_ALLOWED_UNVERIFIED because a read name
# is not a write name. `primary_email` is a valid display field and an invalid
# input; `o` and `id` may well be `object_type` and `object_id` on the way in.
# Only a sandbox write and a read-back can settle it.
OBSERVED_OUTPUT = {
    "task/display": {
        "o": 'the linked object TYPE, e.g. "profile" (UI task 1033, '
             '2026-10-01)',
        "id": "the linked object's id, e.g. 21661 — NOT the task's own id, "
              "which is task_id (UI task 1033, 2026-10-01)",
        "task_object": 'a derived label, e.g. "Profile :: SENTINEL 6 - '
                       'SANDBOX ONLY, HUBSYNC" (UI task 1033, 2026-10-01)',
        "task_type_id": "1065 = DIY Form-Contact (UI task 1033, 2026-10-01)",
        "task_type": "nested {task_type_id, task_type_name}",
        "task_description": "holds the task's title text; there is no `name` "
                            "field on any read endpoint",
    },
}


ENDPOINT_ALLOWED_UNVERIFIED = {
    # Everything else on task/create graduated to ENDPOINT_CONFIRMED_FIELDS on
    # 2026-10-01, read back off task 1034. These two have not: neither was
    # sent, and `task/edit/complete` has never been called at all.
    "task_id": ("task/create", "task/edit/complete"),
    "task_guid": ("task/create", "task/edit/complete"),
}

# Recognised by CSuite but NOT yet confirmed to store a value, so
# deliberately absent from the set above.
#
# 2026-09-30: profile/create/individual was sent phone_number="555-0100"
# and CSuite answered HTTP 400, `phone_number: phone [5550100] is not
# valid` — it parsed the field, validated it, and rejected the whole
# create. That is the opposite of the `primary_email` case, which returned
# 200 and dropped the value. So CSuite VALIDATES the names it recognises
# and SILENTLY DROPS the ones it does not, which makes a deliberately
# invalid value the cheapest way to test a candidate name: a 400 naming
# the field proves the name is recognised, and no record is created.
#
# phone_number graduated on 2026-09-30: profile/edit on sentinel 21626 was
# sent phone_number="7035550100" and profile/display returned
# primary_phone_number "703-555-0100". Stored, and punctuated by CSuite —
# which is why sync/readback.py now tells a reformatted value apart from a
# dropped one. Nothing is left in this set.
# 2026-10-01, task/create returned HTTP 400 naming two fields as REQUIRED:
#     due_ts: due_ts is required
#     name:   name is required
# CSuite therefore parses and demands both, which makes them real input names.
# Neither has been STORED — the create was rejected, so nothing was read back,
# and `due_ts`'s accepted FORMAT is unknown (the output is a plain
# "2026-10-05", but "ts" suggests a timestamp). Same holding pen `phone_number`
# sat in before a read-back promoted it.
# Both entries graduated on 2026-10-01: task 1034 was created with `name` and
# `due_ts="2026-10-05"`, and read back with due_date "2026-10-05". The format
# is no longer a guess, and nothing is left in this set.
RECOGNISED_UNCONFIRMED_FIELDS = frozenset()

# Names PROVEN not to work as inputs, each by a sandbox write and a
# read-back. They are all valid `profile/display` output names, which is the
# trap: the display list reads like a field list and is not one.
#
# 2026-09-30, profile/create/individual on 21626: `primary_email` -> 200,
# value gone. 2026-09-30, profile/edit on 21626: the four address names
# below -> 200, `success: true`, and **0 of 81 fields changed, modified_ts
# included**. CSuite did not touch the record and said nothing.
#
# Kept so the refusal can cite the evidence instead of only saying "not
# confirmed", and so nobody spends another sandbox write proving it twice.
KNOWN_INVALID_INPUT_FIELDS = {
    "primary_email": "use `email` (confirmed 2026-09-30)",
    "primary_phone_number": "use `phone_number` (confirmed 2026-09-30)",
    "primary_address": "dropped by profile/edit, 2026-09-30",
    "primary_city": "dropped by profile/edit, 2026-09-30",
    "primary_state": "dropped by profile/edit, 2026-09-30",
    "primary_zipcode": "dropped by profile/edit, 2026-09-30",
    "primary_address_string": "a display name; no input name confirmed yet",
    # 2026-10-01, profile/edit on 21626, this key ALONE: HTTP 200,
    # success: true, 0 of 81 fields changed, modified_ts unchanged,
    # primary_city still null. Dropped exactly like an unrecognised flat
    # name — so the HTTP 500 of 2026-09-30 came from sending nine
    # conflicting dotted keys at once, not from the dot itself.
    # 2026-10-01, profile/edit on 21626, two isolated single-key edits:
    # "address" as a plain string ("41 Test Way, Fairfax, VA 22031") and
    # "address" as a nested object ({"city": "Vienna"}). Both returned
    # HTTP 200, success: true, 0 of 81 fields changed, modified_ts
    # unchanged. Neither is the documented EDIT shape.
    # Endpoint-specific: see ENDPOINT_CONFIRMED_FIELDS. Nested `address` IS
    # a confirmed input to profile/create/individual as of 2026-10-01.
    # 2026-10-01, task/create: `due_date` was SENT and CSuite still answered
    # HTTP 400 `due_ts: due_ts is required`. So due_date did not satisfy the
    # due-date requirement — it is the OUTPUT name (task/display returns
    # due_date "2026-10-05") and not the input name.
    "due_date": "sent to task/create and CSuite still required `due_ts`, "
                "2026-10-01; it is the output name — use `due_ts`",
    "address": "dropped by profile/EDIT as a plain string AND as a nested "
               "object, 2026-10-01. On profile/CREATE the nested object is "
               "CONFIRMED — use the dotted address.* keys to edit",
}

# Probes whose result proved nothing, kept so the measurement is not lost and
# not mistaken for a verdict.
#
# This list is a record, NOT a gate. check_input_fields never consults it: a
# name in here may be sent freely, because "we learned nothing" is not
# "this is wrong". Confusing the two is how `address.city` ended up in
# KNOWN_INVALID_INPUT_FIELDS on 2026-10-01 — it is a DOCUMENTED edit key, and
# the probe that looked like a refutation had a confound nobody had ruled
# out.
INCONCLUSIVE_PROBES = {
    # 2026-10-01, read-only. `create_task` sends `name` as a required
    # argument. `task/list` and `task/display` on all seven sandbox tasks
    # return NO `name` field — only `task_description`. Either CSuite stores
    # it somewhere it does not display, or it discards it exactly as it
    # discarded `primary_email`. One sandbox create would tell us; no write
    # was spent, because the same read showed a task cannot be attached to a
    # profile at all, which is what the task was for.
    "name (task/create)":
        "sandbox-17, 2026-10-01: sent by create_task as required, absent from "
        "every task read endpoint — the title text is read back as "
        "`task_description`. UNTESTED as an input: no task has been created "
        "through the API.",
    # The profile link exists after all — see OBSERVED_OUTPUT. What is unknown
    # is what to CALL it on the way in.
    "the task -> profile link (task/create)":
        "sandbox-17, 2026-10-01: concluded ABSENT from seven sandbox tasks "
        "that all had it null, then found populated on a UI-made task as "
        "o=\"profile\" / id=21661. The capability is VERIFIED; the input "
        "names are UNKNOWN. create_task has no parameter for it.",
    # The key is CONFIRMED — it stores, with the other three. What is NOT
    # known is the smallest set that works.
    #
    # Corrected 2026-10-01 (sandbox-13). Sandbox-12 inferred that one key
    # alone failed because the address row did not exist yet. That was wrong:
    # sent alone against 21626, which by then HELD a full address, it stored
    # nothing again — 200, 0 of 81 fields changed, modified_ts unchanged. So
    # the precondition is not a missing row.
    #
    # Nothing was blanked either time, so a partial address edit does not
    # destroy an address that is already there.
    "address.city": "sandbox-9 and sandbox-13, 2026-10-01: sent ALONE, both "
                    "against a profile with NO address and against one with "
                    "a full address — 200, nothing stored, modified_ts "
                    "unchanged, nothing blanked, both times. So "
                    "profile/edit does not act on a single address key. All "
                    "four together DO store (sandbox-12). The smallest "
                    "working set is untested: two and three keys have never "
                    "been tried.",
    "address.street / address.address1 / address.line1 / address.zip / "
    "address.zipcode / address.postal_code (and address.address, "
    "address.city, address.state)": (
        "sandbox-8, 2026-09-30: nine candidate keys in ONE payload, four "
        "street names and three postcode names contradicting each other — "
        "HTTP 500 with an empty errors array. The request failed as a whole, "
        "so no individual key was tested."),
}


class UnconfirmedField(ValueError):
    """A write was asked to send a field name no read-back has confirmed.

    Raised before the request is built. Sending it instead would be the
    worse outcome: CSuite would accept the call, drop the field, and
    return the same 200 it returns for a field that was stored.
    """

    def __init__(self, unknown, endpoint: str = ""):
        self.unknown = sorted(unknown)
        self.endpoint = endpoint
        names = ", ".join(repr(n) for n in self.unknown)
        # A name already proven wrong gets the evidence and the replacement,
        # rather than the generic "not confirmed yet" — they are different
        # situations and only one of them needs a sandbox write to resolve.
        proven = [f"{n!r}: {KNOWN_INVALID_INPUT_FIELDS[n]}"
                  for n in self.unknown if n in KNOWN_INVALID_INPUT_FIELDS]
        detail = ("  Proven not to work: " + "; ".join(proven) + "."
                  if proven else "")
        super().__init__(
            f"refusing {endpoint or 'this CSuite write'}: {names} "
            f"{'is' if len(self.unknown) == 1 else 'are'} not a confirmed "
            "CSuite input name. CSuite would accept the call, silently drop "
            "the field and return 200. Confirm the name with a sandbox write "
            f"and a read-back, then add it to CONFIRMED_INPUT_FIELDS.{detail} "
            "Nothing was sent.")


# CSuite VALIDATES phone_number and rejects the whole create when it does
# not like the value — measured 2026-09-30:
# `phone_number: phone [5550100] is not valid`, HTTP 400, no profile made.
# Its predecessor `primary_phone_number` was not validated because it was
# not recognised at all, so a bad number used to be dropped in silence.
#
# That turns one bad digit in a HubSpot form field into a blocked profile,
# so the value is checked here instead and a number CSuite would refuse is
# left out of the create. The profile is made either way; the number is
# reported to a human to enter by hand. Losing the phone is recoverable
# with one profile/edit; not having the profile is not.
PHONE_DIGITS = 10


def normalize_phone(raw):
    """(value, warning) — a 10-digit number CSuite will accept, or None.

    `value` is ten digits with no punctuation; CSuite punctuates what it
    stores (verified 2026-09-30: "7035550100" was stored as
    "703-555-0100"), so sending the bare digits is sending the value, not a
    format preference.

    `warning` is None only when `value` is a number. Anything this cannot
    reduce to ten digits comes back as (None, warning) with the raw value
    quoted — an extension, an international number, a seven-digit local
    number, or free text. Never (None, None): a dropped phone number
    without a warning is the silent loss this whole module exists to stop.
    """
    text = "" if raw is None else str(raw)
    digits = "".join(c for c in text if c.isdigit())

    # US country code. "+1 703 555 0100" and "1-703-555-0100" are the same
    # number as "703-555-0100", and CSuite wants the ten.
    if len(digits) == PHONE_DIGITS + 1 and digits.startswith("1"):
        digits = digits[1:]

    if len(digits) == PHONE_DIGITS:
        return digits, None

    return None, (f"Phone not stored: {text!r} isn't a 10-digit US number. "
                  "Enter it manually.")


# The four parts of the nested address CSuite accepts on a create, and the
# display field each one fills. VERIFIED 2026-10-01, sentinel 21660.
ADDRESS_PARTS = ("address", "city", "state", "zipcode")


def build_address(address_line, city, state, zipcode):
    """(nested address | None, warning | None) for profile/create/individual.

    All four parts or none. Two reasons, both measured:

    * CSuite derives `primary_address_string`, `primary_citystatezip` and
      `primary_country` from the parts it is given, so a partial object puts
      a malformed address on the record — and an address nobody can trust is
      worse than one a person is asked to enter.
    * On `profile/edit`, a single address key stores **nothing at all**
      (2026-10-01, sentinel 21626, tried both against an empty address and
      against a full one). All four together store. The smallest working set
      has never been established, so "all four" is the only size known to
      work.

    `address2` is deliberately absent: it is not in the confirmed set.
    """
    parts = {
        "address": (address_line or "").strip(),
        "city": (city or "").strip(),
        "state": (state or "").strip(),
        "zipcode": (zipcode or "").strip(),
    }
    missing = [name for name in ADDRESS_PARTS if not parts[name]]

    if not missing:
        return parts, None

    if len(missing) == len(ADDRESS_PARTS):
        # Nothing was submitted. Not an incomplete address — no address.
        return None, None

    given = ", ".join(parts[name] for name in ADDRESS_PARTS if parts[name])
    return None, (f"🏠 Address incomplete, not stored: {given!r}. "
                  "Enter it manually.")


def check_input_fields(names, endpoint: str = "") -> None:
    """Raise UnconfirmedField unless every name is confirmed for `endpoint`.

    A name confirmed on one endpoint is not confirmed on all of them —
    `address` is a valid nested input to profile/create/individual and is
    silently discarded by profile/edit, both measured 2026-10-01.
    """
    clean = str(endpoint or "").strip("/")
    unknown = [
        n for n in (names or ())
        # A block wins over the global allowlist: a name confirmed elsewhere
        # can still be wrong here.
        if clean in ENDPOINT_BLOCKED_FIELDS.get(n, ())
        or (n not in CONFIRMED_INPUT_FIELDS
            and clean not in ENDPOINT_CONFIRMED_FIELDS.get(n, ())
            and clean not in ENDPOINT_ALLOWED_UNVERIFIED.get(n, ()))
    ]
    if unknown:
        logger.error("CSuite %s: unconfirmed field name(s) %s — nothing sent",
                     endpoint or "(write)", sorted(unknown))
        raise UnconfirmedField(unknown, endpoint)


class CSuiteEnvMismatch(RuntimeError):
    """The host, the body `env`, and the credentials do not agree.

    Raised at client construction, before anything can be sent. A
    mismatch is not a thing to detect in a log afterwards.
    """


def host_of(url: str) -> str:
    """The hostname of a base URL, lowercased. No scheme, no path."""
    from urllib.parse import urlparse

    text = str(url or "").strip()
    parsed = urlparse(text if "//" in text else f"//{text}")
    return (parsed.hostname or "").lower()


def ui_url(template: str, api_base_url=None, **ids) -> str:
    """A CSuite UI link on the host that actually holds the record.

    `Config.CSUITE_UI_BASE_URL` is a hardcoded production host, fixed at class
    definition, so every UI link this repo has ever printed pointed at
    production — including the links in sandbox confirmations.
    
    That is not cosmetic. Measured 2026-10-01: production profile **21662**
    exists and is an unrelated real ORG, 21626 is an unrelated real individual,
    and task 1034 is a real task from 2025-08-25. A sandbox link opened by
    staff lands on a different donor's record, and an edit made there believing
    it was the sentinel would be real damage done by a confirmation line.

    So the host comes from the base URL of the client that performed the write.
    The PATH is still INFERRED from the pattern of the others — no CSuite UI
    path has been opened and confirmed — and that is unchanged by this.

    With no `api_base_url` the template is returned as-is, so a caller that
    cannot say which environment it was in gets the old behaviour rather than a
    silently wrong guess.
    """
    from urllib.parse import urlsplit, urlunsplit

    url = template.format(**ids)
    host = host_of(api_base_url) if api_base_url else ""
    if not host:
        return url
    parts = urlsplit(url)
    return urlunsplit((parts.scheme or "https", host, parts.path, parts.query,
                       parts.fragment))


def host_looks_like_sandbox(url: str) -> bool:
    return _SANDBOX_HOST_MARKER in host_of(url)


def resolve_csuite_env(env=None, base_url=None, key=None, secret=None,
                       sandbox_key=None, sandbox_secret=None,
                       sandbox_base_url=None) -> dict:
    """The single place that decides host, body `env`, and credentials.

    Returns {"env", "base_url", "key_var", "secret_var", "api_key",
    "api_secret"}. The *_var entries name the environment variable each
    credential came from, so a report can say where a key came from
    without printing it.

    Raises CSuiteEnvMismatch when the host and the env disagree, or when
    the selected credential pair is missing. Both are refusals rather
    than warnings: the failure they prevent is a write to the wrong
    database, and there is no safe way to continue past either.
    """
    from config import Config

    env = (env if env is not None else Config.CSUITE_ENV)
    env = str(env or ENV_LIVE).strip().lower()
    if env not in VALID_ENVS:
        raise CSuiteEnvMismatch(
            f"CSUITE_ENV is {env!r}; allowed values are "
            f"{' | '.join(VALID_ENVS)}")

    if env == ENV_SANDBOX:
        base_url = base_url if base_url is not None else (
            sandbox_base_url if sandbox_base_url is not None
            else Config.CSUITE_SANDBOX_BASE_URL)
        api_key = sandbox_key if sandbox_key is not None \
            else Config.CSUITE_SANDBOX_KEY
        api_secret = sandbox_secret if sandbox_secret is not None \
            else Config.CSUITE_SANDBOX_SECRET
        key_var, secret_var = "CSUITE_SANDBOX_KEY", "CSUITE_SANDBOX_SECRET"
    else:
        base_url = base_url if base_url is not None else Config.CSUITE_BASE_URL
        api_key = key if key is not None else Config.CSUITE_API_KEY
        api_secret = secret if secret is not None else Config.CSUITE_API_SECRET
        key_var, secret_var = "CSUITE_API_KEY", "CSUITE_API_SECRET"

    sandbox_host = host_looks_like_sandbox(base_url)
    if env == ENV_SANDBOX and not sandbox_host:
        raise CSuiteEnvMismatch(
            f"CSUITE_ENV=sandbox but the host is {host_of(base_url)!r}, "
            "which is not a sandbox host. Refusing to start: a sandbox "
            "`env` against a production host is a request the live "
            "system may well accept.")
    if env == ENV_LIVE and sandbox_host:
        raise CSuiteEnvMismatch(
            f"CSUITE_ENV=live but the host is {host_of(base_url)!r}, "
            "which is a sandbox host. Refusing to start rather than "
            "guessing which one was meant.")

    missing = [name for name, value in ((key_var, api_key),
                                       (secret_var, api_secret)) if not value]
    if missing:
        raise CSuiteEnvMismatch(
            f"CSUITE_ENV={env} needs {' and '.join(missing)}, which "
            f"{'is' if len(missing) == 1 else 'are'} not set. Names only — "
            "no value is read or logged here.")

    return {"env": env, "base_url": base_url, "key_var": key_var,
            "secret_var": secret_var, "api_key": api_key,
            "api_secret": api_secret}


class CSuiteClient:
    """Client for CSuite API with proper HMAC authentication"""
    
    def __init__(self, env=None, base_url=None, api_key=None,
                 api_secret=None):
        # One resolver decides host, body `env` and credentials together,
        # and refuses to start if they disagree. The arguments exist for
        # tests and for the deliberate cross-checks in
        # reports/csuite_sandbox_reads.md; nothing in the app passes them.
        resolved = resolve_csuite_env(
            env=env, base_url=base_url, key=api_key, secret=api_secret,
            sandbox_key=api_key if env == ENV_SANDBOX else None,
            sandbox_secret=api_secret if env == ENV_SANDBOX else None)
        self.env = resolved["env"]
        self.base_url = resolved["base_url"]
        self.api_key = resolved["api_key"]
        self.api_secret = resolved["api_secret"]
        # Variable NAMES, kept so a diagnostic can say where a credential
        # came from without reading its value.
        self.key_var = resolved["key_var"]
        self.secret_var = resolved["secret_var"]
        self.session = requests.Session()
        logger.info("CSuite client: env=%s host=%s key from $%s",
                    self.env, host_of(self.base_url), self.key_var)

        # A cap from config, so a client built anywhere starts with one.
        # Default 0: nothing can be written unless someone raised it on
        # purpose. Callers that manage their own cap (the sandbox scripts)
        # reassign client.write_budget after construction.
        self.write_budget = self._budget_from_config()

    # =========================================================================
    # AUTHENTICATION & HTTP
    # =========================================================================
    
    def _generate_signature(self, body: str) -> str:
        """Generate HMAC-SHA256 Base64 signature"""
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            body.encode('utf-8'),
            hashlib.sha256
        )
        return base64.b64encode(signature.digest()).decode('utf-8')
    
    def _build_payload(self, data: dict = None) -> dict:
        """Build request payload with required fields"""
        payload = {
            "env": self.env,
            "epoch": int(time.time())
        }
        if data:
            payload.update(data)
        return payload
    
    # Read-back verification on the PRODUCTION path, ON by default.
    #
    # It costs one extra profile/display per profile write. That is the
    # price of knowing whether the write did anything: CSuite returns the
    # same 200 and the same profile_id whether it stored a field or
    # discarded it, so without the read-back a lost field is invisible
    # forever. Only profile writes are covered — see READBACK_TARGETS.
    #
    # A drop does NOT raise here. It annotates the response and logs at
    # ERROR. Raising after a create that succeeded would tell the caller
    # the write failed when a record now exists, and CSuite has no
    # idempotency key, so the natural response to that — try again — makes
    # a second profile. A wrong record is recoverable; a duplicate pair is
    # worse. The sandbox path (sync/sandbox_writes.py) still raises,
    # because there the whole point is to stop.
    verify_writes = True

    # Which write endpoints can be read back, and how. An endpoint absent
    # from this map is not verified — not because it is safe, but because
    # nothing here knows how to look it up.
    READBACK_TARGETS = {
        "profile/create/individual": ("profile/display", "profile_id"),
        "profile/create/org": ("profile/display", "profile_id"),
        "profile/create/household": ("profile/display", "profile_id"),
        "profile/edit": ("profile/display", "profile_id"),
    }

    # An optional WriteBudget (sync/sandbox_writes.py). When set, every
    # write through this client is counted and the one after the limit
    # raises, before the request is built.
    #
    # It lives on the CLIENT rather than on a wrapper because a wrapper does
    # not hold. 2026-10-01: a run wrapped the client in a proxy that
    # overrode _request and delegated everything else through __getattr__.
    # `proxy.create_individual_profile` returned the INNER client's bound
    # method, whose `self` is the inner client, so the proxy's _request was
    # never reached. Two writes went out under a cap of one and the run
    # printed "budget 0 of 1" — a guard that silently does not guard, which
    # is worse than no guard, because the output looked clean.
    write_budget = None

    def _budget_from_config(self):
        """The cap this client starts with, from Config.CSUITE_WRITE_BUDGET.

        Default 0, so a client built with no deliberate budget cannot write.
        See the note on CSUITE_WRITE_BUDGET in config.py for how to raise it
        for a live run.
        """
        from sync.sandbox_writes import WriteBudget
        return WriteBudget(int(getattr(Config, "CSUITE_WRITE_BUDGET", 0) or 0))

    # Endpoints where a before-state is worth reading. An edit that stores
    # nothing leaves modified_ts alone, and that is the only part of the
    # result that says so — CSuite answers success either way. A create has
    # no before-state, so it is not listed.
    SNAPSHOT_BEFORE = ("profile/edit",)

    def _modified_before(self, endpoint: str, data: dict):
        """The record's `modified_ts` before this write, or None.

        None means "not compared", never "unchanged" — the two would lead to
        opposite conclusions and only one of them is knowable from a failed
        read.
        """
        clean = str(endpoint or "").strip("/")
        if clean not in self.SNAPSHOT_BEFORE:
            return None
        target = self.READBACK_TARGETS.get(clean)
        if not target:
            return None
        display_endpoint, id_field = target
        record_id = (data or {}).get(id_field)
        if record_id is None:
            return None
        response = self._request(display_endpoint, {id_field: record_id})
        record = response.get("data") if isinstance(response, dict) else None
        if isinstance(record, list) and record:
            record = record[0]
        if not isinstance(record, dict):
            return None
        return record.get("modified_ts")

    def _verify_write(self, endpoint: str, sent: dict, response: dict,
                      modified_before=None) -> dict:
        """Read the record back; annotate `response` if anything was lost.

        Returns `response`, with `verified` set, and `fields_dropped` added
        when CSuite kept less than it was sent, or `nothing_stored` when it
        did not touch the record at all.
        """
        target = self.READBACK_TARGETS.get(str(endpoint or "").strip("/"))
        if not target:
            return response
        display_endpoint, id_field = target

        record_id = None
        payload = response.get("data")
        if isinstance(payload, dict):
            record_id = payload.get(id_field)
        if record_id is None:
            record_id = (sent or {}).get(id_field)
        if record_id is None:
            logger.warning("no %s to read back after %s; write not verified",
                           id_field, endpoint)
            response["verified"] = None
            return response

        # Imported here, not at module scope: sync.readback is a pure
        # module today and this keeps clients.csuite importable on its own
        # if that ever stops being true.
        from sync.readback import (FieldDropped, NothingStored,
                                    ReadBackUnavailable, verify)

        try:
            verify(self._request, endpoint, sent or {}, record_id,
                   id_field=id_field, display_endpoint=display_endpoint,
                   modified_before=modified_before)
        except NothingStored as e:
            # The strongest of the three: CSuite did not write to the record.
            # Caught first because it is also a FieldDropped.
            logger.warning("CSuite %s on %s %s STORED NOTHING: %s", endpoint,
                           id_field, record_id, e)
            response["verified"] = False
            response["nothing_stored"] = True
            response["fields_dropped"] = {
                field: (value, None)
                for field, value in (sent or {}).items()
                if field not in ("profile_id", "env", "epoch")}
            return response
        except ReadBackUnavailable as e:
            # "Not checked" is not "field lost". Caught before FieldDropped
            # because it is a subclass of it.
            logger.error("could not read %s %s back after CSuite %s: %s",
                         id_field, record_id, endpoint, e)
            response["verified"] = None
            return response
        except FieldDropped as dropped:
            logger.error("CSuite %s on %s %s DID NOT STORE: %s", endpoint,
                         id_field, record_id, sorted(dropped.dropped))
            response["verified"] = False
            response["fields_dropped"] = dropped.dropped
            return response
        except Exception as e:  # the read-back itself failed
            logger.error("read-back after CSuite %s on %s failed: %s",
                         endpoint, record_id, e)
            response["verified"] = None
            return response

        response["verified"] = True
        return response

    def _request(self, endpoint: str, data: dict = None) -> dict:
        """Make authenticated POST request to CSuite API
        
        All CSuite API calls are POST with HMAC-SHA256 signature.
        
        Returns:
            dict with keys: success (bool), data (dict/None), error (str/None),
                           errors (list), messages (list)
        """
        if not self.api_key or not self.api_secret:
            logger.error("CSuite API credentials not configured")
            if is_csuite_write(endpoint):
                # 'skipped', not 'failed': nothing was attempted, so a
                # missing audit row here costs nothing.
                try:
                    record_write(
                        "csuite", "POST", endpoint, payload=data,
                        status="skipped",
                        error="CSuite API credentials not configured",
                        duration_ms=0)
                except AuditUnavailable as e:
                    logger.warning("skipped write not audited: %s", e)
            return {"error": "CSuite API credentials not configured",
                    "success": False, "http_status": None,
                    "outcome": OUTCOME_AUTH_REJECTED}
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        payload = self._build_payload(data)
        body = json.dumps(payload)
        
        headers = {
            "Content-Type": "application/json",
            "SIGNER": self.api_key,
            "SIGNATURE": self._generate_signature(body)
        }
        
        audited = is_csuite_write(endpoint)

        # Claimed before the audit row and before the request, so a refusal
        # costs nothing and leaves nothing behind. Raises; never returns
        # False, because a cap that can be read past is not a cap.
        if audited and self.write_budget is not None:
            self.write_budget.spend(endpoint)
            logger.info("CSuite write %d/%d: %s", self.write_budget.used,
                        self.write_budget.limit, endpoint)

        reservation = None
        if audited:
            # Pre-flight: no audit row, no request. Every CSuite call is a
            # POST, so `audited` is decided by endpoint name, not verb —
            # see is_csuite_write.
            try:
                reservation = reserve_write(
                    "csuite", "POST", endpoint, payload=data)
            except AuditUnavailable as e:
                # Raised, not returned — see the matching note in
                # clients/hubspot._send_with_status.
                logger.error("CSuite POST %s REFUSED: %s", endpoint, e)
                raise

        logger.info(f"CSuite POST: {endpoint} | data keys: {list((data or {}).keys())}")

        # Read before writing, so an edit that changes nothing can be told
        # from an edit that changed something. Only for the endpoints in
        # SNAPSHOT_BEFORE, and only when verification is on.
        modified_before = None
        if audited and self.verify_writes:
            try:
                modified_before = self._modified_before(endpoint, data)
            except Exception as e:  # pragma: no cover - never block the write
                logger.warning("could not snapshot %s before the write: %s",
                               endpoint, e)

        started = time.perf_counter()

        def audit(status, http_status=None, error=None, body=None):
            if audited:
                # The created id exists only in the response. reserve_write
                # runs before the request and deliberately records NULL for a
                # create rather than guessing from the payload — see
                # clients/audit.target_id_from_payload.
                complete_write(
                    reservation, status=status, http_status=http_status,
                    error=error,
                    duration_ms=(time.perf_counter() - started) * 1000,
                    target_id=target_id_from_response(body))

        try:
            response = self.session.post(
                url,
                data=body,
                headers=headers,
                timeout=30
            )
            logger.info(f"CSuite Response: {response.status_code}")
            status_code = getattr(response, "status_code", None)

            try:
                json_response = response.json()

                if json_response.get("success") == 1:
                    audit("success", status_code, body=json_response)
                    result = {
                        "success": True,
                        "data": json_response.get("data"),
                        "messages": json_response.get("messages", []),
                        "http_status": status_code,
                        "outcome": OUTCOME_OK,
                    }
                    # A 200 means the request was accepted, not that the
                    # data was stored. Read it back before calling it done.
                    if audited and self.verify_writes:
                        result = self._verify_write(endpoint, data, result,
                                                    modified_before)
                    return result
                else:
                    errors = json_response.get("errors", [])
                    logger.warning(f"CSuite API error: {errors}")
                    error_text = errors[0] if errors else "Unknown error"
                    # HTTP 200 with success != 1 is still a failed write.
                    audit("failed", status_code, error_text)
                    # A 2xx whose body says success != 1 is a
                    # rejection by the application, not by HTTP. Named
                    # separately so it is not read as a transport fault.
                    outcome = classify_status(status_code)
                    if outcome == OUTCOME_OK:
                        outcome = OUTCOME_REJECTED
                    return {
                        "success": False,
                        "error": error_text,
                        "errors": errors,
                        "http_status": status_code,
                        "outcome": outcome,
                        "body": _safe_body(response),
                    }

            except json.JSONDecodeError as e:
                logger.error(f"CSuite JSON decode error: {str(e)}")
                audit("failed", status_code, f"Invalid JSON response: {e}")
                return {"error": f"Invalid JSON response: {str(e)}",
                        "success": False, "http_status": status_code,
                        "outcome": (classify_status(status_code)
                                    if classify_status(status_code)
                                    != OUTCOME_OK else OUTCOME_BAD_RESPONSE),
                        "body": _safe_body(response)}

        except requests.exceptions.RequestException as e:
            logger.error(f"CSuite Request error: {str(e)}")
            audit("failed", None, str(e))
            return {"error": str(e), "success": False, "http_status": None,
                    "outcome": OUTCOME_NETWORK}
    
    # =========================================================================
    # PAGINATION HELPER
    # =========================================================================
    
    def _get_all_pages(self, endpoint: str, data: dict = None,
                       max_iterations: int = 200, batch_size: int = 100) -> list:
        """Fetch all pages of a paginated endpoint.
        
        CSuite uses view_offset (not cur_page) for pagination.
        
        Args:
            endpoint: API endpoint
            data: Additional request data (filters, etc.)
            max_iterations: Safety limit to prevent infinite loops
            batch_size: Records per page
            
        Returns:
            list of all result objects across all pages
        """
        all_results = []
        offset = 0
        base_data = data or {}
        
        for _ in range(max_iterations):
            request_data = {
                **base_data,
                "view_limit": batch_size,
                "view_offset": offset
            }
            
            result = self._request(endpoint, request_data)
            
            if not result.get("success"):
                logger.error(f"Pagination failed at offset {offset}: {result.get('error')}")
                break
            
            results = result.get("data", {}).get("results", [])
            if not results:
                break
            
            all_results.extend(results)
            
            if len(results) < batch_size:
                break
            
            offset += batch_size
            
            # Log progress every 500 records
            if len(all_results) % 500 == 0:
                logger.info(f"Fetched {len(all_results)} records from {endpoint}...")
        
        logger.info(f"Retrieved {len(all_results)} total records from {endpoint}")
        return all_results
    
    # =========================================================================
    # PROFILES
    # =========================================================================
    
    def get_profiles(self, limit: int = 100, offset: int = 0) -> dict:
        """Get profiles (donors, vendors, etc.)"""
        return self._request("profile/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_profile(self, profile_id: int) -> dict:
        """Get specific profile details"""
        return self._request("profile/display", {"profile_id": profile_id})
    
    def search_profiles(self, query: str) -> dict:
        """Search profiles by name
        
        Note: Returns mixed results - profiles AND funds matching the query.
        Filter by result['object'] == 'profile' for profiles only.
        """
        return self._request("profile/list/search", {"q": query})
    
    def get_all_profiles(self, max_iterations: int = 200) -> list:
        """Get all profiles across all pages"""
        return self._get_all_pages("profile/list", max_iterations=max_iterations)
    
    def create_individual_profile(self, first_name: str, last_name: str,
                                   email: str = None, phone: str = None,
                                   address_line: str = None, city: str = None,
                                   state: str = None, zipcode: str = None,
                                   **kwargs) -> dict:
        """Create an individual profile in CSuite.

        Used by: DAF/Endowment inquiry workflow (Kods)

        Field names, and how each one was established
        ---------------------------------------------
        From 2026-03-17 to 2026-10-01 this method sent `primary_email`,
        `primary_phone_number` and `primary_address_string`. All three are
        valid `profile/display` OUTPUT names and none of them is an input
        name: CSuite accepted the create, returned HTTP 200 and a
        profile_id, and discarded every one of them.

        What it sends now, every name confirmed by a sandbox write and a
        read-back:

        - `email` -> `primary_email`. VERIFIED 2026-09-30, create and edit.
        - `phone_number` -> `primary_phone_number`. VERIFIED 2026-09-30.
          CSuite punctuates a ten-digit value ("7035550100" came back
          "703-555-0100") and stores anything else verbatim, so the number is
          normalised first — see normalize_phone.
        - `address` as a NESTED object of `address`, `city`, `state`,
          `zipcode`. VERIFIED 2026-10-01 on sentinel 21660: all four stored,
          and CSuite derived `primary_citystatezip`,
          `primary_address_string` and `primary_country` ("US") by itself.

        Args:
            first_name: First name (required)
            last_name: Last name (required)
            email: email address -> primary_email
            phone: any format; normalised to ten digits or omitted with a
                warning on the response as `phone_warning`
            address_line: street -> address.address -> primary_address
            city: -> address.city -> primary_city
            state: -> address.state -> primary_state
            zipcode: -> address.zipcode -> primary_zipcode
            **kwargs: checked against CONFIRMED_INPUT_FIELDS first

        **All four address parts or none.** A partial nested object is never
        sent: CSuite derives `primary_address_string` and
        `primary_citystatezip` from the parts, so a half-filled address
        produces a malformed one on the record, and an address nobody can
        trust is worse than an address a person is asked to enter. When any
        part is missing the create goes ahead without it and the response
        carries `address_warning`.

        `address2` is never sent — it is not in the confirmed set.

        Returns:
            dict with 'data': {'profile_id': int} on success, plus
            `phone_warning` / `address_warning` when something was left out.
        """
        data = {
            "first_name": first_name,
            "last_name": last_name,
        }
        if email:
            data["email"] = email

        # A number CSuite would store as unsearchable text is left out rather
        # than sent. The warning travels with the response so the caller can
        # put it in front of a person — see normalize_phone.
        phone_warning = None
        if phone:
            number, phone_warning = normalize_phone(phone)
            if number:
                data["phone_number"] = number
            else:
                logger.warning("CSuite profile create: %s", phone_warning)

        address, address_warning = build_address(address_line, city, state,
                                                zipcode)
        if address:
            data["address"] = address
        elif address_warning:
            logger.warning("CSuite profile create: %s", address_warning)

        # Checked before the payload is built, so an unconfirmed name is a
        # refusal rather than a silent drop.
        check_input_fields(kwargs, "profile/create/individual")
        data.update(kwargs)

        logger.info(f"Creating individual profile: {first_name} {last_name}")
        response = self._request("profile/create/individual", data)
        if isinstance(response, dict):
            if phone_warning:
                response["phone_warning"] = phone_warning
            if address_warning:
                response["address_warning"] = address_warning
        return response

    def create_org_profile(self, organization: str, email: str = None,
                           phone: str = None, **kwargs) -> dict:
        """Create an organization profile in CSuite. **UNVERIFIED.**

        Used by: nothing. This method has no caller in any commit, and
        `profile/create/org` has never been called from this repository —
        not in production, not in the sandbox.

        **UNVERIFIED:** `email` and `phone_number` are carried over from
        `profile/create/individual`, where both were confirmed by a
        read-back. Nothing has shown that the org endpoint takes the same
        input names, and `organization` itself has never been confirmed
        either. CSuite's input and output vocabularies differ per field, so
        they may well differ per endpoint.

        Before this is called for real: one sandbox create and one
        `profile/display`, exactly as `profile/create/individual` was
        confirmed. Until then treat a 200 from here as meaning nothing
        about what was stored.

        Args:
            organization: Organization name (required) — UNVERIFIED name
            email: email address -> primary_email (UNVERIFIED on this endpoint)
            phone: phone number -> phone_number (UNVERIFIED on this endpoint)
            **kwargs: checked against CONFIRMED_INPUT_FIELDS first

        Returns:
            dict with 'data': {'profile_id': int} on success
        """
        data = {"organization": organization}
        if email:
            data["email"] = email
        if phone:
            data["phone_number"] = phone
        check_input_fields(kwargs, "profile/create/org")
        data.update(kwargs)

        logger.info(f"Creating org profile: {organization}")
        return self._request("profile/create/org", data)
    
    def create_household_profile(self, household: str, **kwargs) -> dict:
        """Create a household profile in CSuite.
        
        Args:
            household: Household name (required)
            **kwargs: Additional profile fields
            
        Returns:
            dict with 'data': {'profile_id': int} on success
        """
        data = {"household": household}
        check_input_fields(kwargs, "profile/create/household")
        data.update(kwargs)
        
        logger.info(f"Creating household profile: {household}")
        return self._request("profile/create/household", data)
    
    def edit_profile(self, profile_id: int, **kwargs) -> dict:
        """Edit an existing profile.
        
        Args:
            profile_id: CSuite profile ID
            **kwargs: Fields to update (e.g., primary_email, primary_phone_number)
            
        Returns:
            dict with success status
        """
        check_input_fields(kwargs, "profile/edit")
        data = {"profile_id": profile_id, **kwargs}
        logger.info(f"Editing profile {profile_id}: {list(kwargs.keys())}")
        return self._request("profile/edit", data)
    
    # =========================================================================
    # FUNDS
    # =========================================================================
    
    def get_funds(self, limit: int = 100, offset: int = 0) -> dict:
        """Get list of funds"""
        return self._request("funit/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_fund(self, fund_id: int) -> dict:
        """Get specific fund details including balance"""
        return self._request("funit/display", {"funit_id": fund_id})
    
    def search_funds(self, query: str) -> dict:
        """Search funds by name"""
        return self._request("funit/list/search", {"q": query})
    
    def get_all_funds(self, max_iterations: int = 10) -> list:
        """Get all funds across all pages"""
        return self._get_all_pages("funit/list", max_iterations=max_iterations)
    
    def create_fund(self, name: str, fgroup_id: int,
                    cash_account_id: int = None, **kwargs) -> dict:
        """Create a new fund in CSuite.
        
        Used by: DAF/Endowment inquiry workflow (Kods)
        
        Args:
            name: Fund name (required) - e.g., "Smith Family Fund-(DAF0XXX)"
            fgroup_id: Fund group ID (required) - 1002 for DAF, use Config.FUND_GROUP_*
            cash_account_id: Cash account (defaults to Config.DEFAULT_CASH_ACCOUNT_ID)
            **kwargs: Additional fund fields (e.g., fund_type_id, invest_id)
            
        Returns:
            dict with 'data': {'funit_id': int} on success
        """
        data = {
            "name": name,
            "fgroup_id": fgroup_id,
            "cash_account_id": cash_account_id or Config.DEFAULT_CASH_ACCOUNT_ID,
        }
        check_input_fields(kwargs, "funit/create")
        data.update(kwargs)

        logger.info(f"Creating fund: {name} (group: {fgroup_id})")
        response = self._request("funit/create", data)

        # funit/create had no read-back until 2026-10-01. Sandbox-11 created
        # fund 1564 and nothing checked what was in it; its contents are known
        # only because I chose to look afterwards. A fund pointed at the wrong
        # cash account is a finance problem, not a data-entry one.
        if not (self.verify_writes and isinstance(response, dict)
                and response.get("success")):
            return response
        payload = response.get("data")
        funit_id = payload.get("funit_id") if isinstance(payload, dict) else None
        if funit_id is None:
            logger.warning("no funit_id came back from funit/create; the fund "
                           "was not verified")
            response["verified"] = None
            return response

        from sync.readback import (FieldDropped, ReadBackUnavailable,
                                   verify_fund)
        try:
            verify_fund(self._request, data, funit_id)
        except ReadBackUnavailable as e:
            logger.error("could not read fund %s back: %s", funit_id, e)
            response["verified"] = None
            response["fund_warning"] = (
                f"⚠️ Fund {funit_id} was created but could not be read back, "
                "so its group and cash account are unconfirmed. Check it in "
                "CSuite.")
        except FieldDropped as dropped:
            logger.error("fund %s does not hold what was sent: %s", funit_id,
                         sorted(dropped.dropped))
            response["verified"] = False
            response["fields_dropped"] = dropped.dropped
            response["fund_warning"] = (
                f"⚠️ Fund {funit_id} was created but CSuite did not store: "
                f"{', '.join(sorted(dropped.dropped))}. Check its fund group "
                "and cash account in CSuite before using it.")
        else:
            response["verified"] = True
        return response
    
    def get_fund_groups(self) -> dict:
        """Get fund groups (DAF, Endowment, Fiscal Sponsorship, etc.)"""
        return self._request("funit/list/fgroup")
    
    def get_fund_types(self) -> dict:
        """Get fund types (Permanently Restricted, Temporarily Restricted, etc.)"""
        return self._request("funit/list/fundtype")
    
    def get_fund_fee_types(self) -> dict:
        """Get fund admin fee types and schedules.
        
        Used by: Fee calculation on fund balances (Muhi)
        
        Returns fee structure including:
        - admin_fee_type_name: e.g., "Fund Admin Fees"
        - admin_fee_apply_fee: "quarterly", "annually", etc.
        - admin_fee_min_fee: Minimum fee amount
        - admin_fee_percent: Fee percentage (if flat rate)
        - admin_fee_type_type: "percent_range", "flat", etc.
        - admin_fee_ladder: Whether fees are tiered
        - admin_fee_use_adb: Whether to use average daily balance
        """
        return self._request("funit/feetype")
    
    def get_fund_subgroups(self) -> dict:
        """Get fund subgroups"""
        return self._request("funit/list/fsubgroup")
    
    # =========================================================================
    # DONATIONS
    # =========================================================================
    
    def get_donations(self, limit: int = 100, offset: int = 0) -> dict:
        """Get donations list"""
        return self._request("donation/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_donation(self, donation_id: int) -> dict:
        """Get specific donation details"""
        return self._request("donation/display", {"donation_id": donation_id})
    
    def get_donations_by_profile(self, profile_id: int) -> dict:
        """Get donations for a specific profile"""
        return self._request("donation/list", {"profile_id": profile_id})
    
    def get_donations_by_fund(self, funit_id: int, limit: int = 100, offset: int = 0) -> dict:
        """Get donations for a specific fund"""
        return self._request("donation/list", {
            "funit_id": funit_id,
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_all_donations(self, max_iterations: int = 300) -> list:
        """Get all donations across all pages (24,910+ records)
        
        Warning: This fetches a LOT of data. Use sparingly.
        For targeted queries, use get_donations_by_profile() or get_donations_by_fund().
        """
        return self._get_all_pages("donation/list", max_iterations=max_iterations)
    
    def get_donations_with_limit(self, limit: int = None) -> list:
        """Get donations with optional cap on total records.
        
        Used by: Donation sync, Ramadan comparisons, reporting
        
        Args:
            limit: Max total donations to fetch (None = all)
        """
        all_donations = []
        offset = 0
        batch_size = 100
        
        while True:
            result = self.get_donations(limit=batch_size, offset=offset)
            
            if not result.get("success"):
                logger.error(f"Failed to get donations at offset {offset}")
                break
            
            data = result.get("data", {})
            donations = data.get("results", [])
            
            if not donations:
                break
            
            all_donations.extend(donations)
            
            if limit and len(all_donations) >= limit:
                all_donations = all_donations[:limit]
                break
            
            if len(donations) < batch_size:
                break
            
            offset += batch_size
            
            if offset % 500 == 0:
                logger.info(f"Fetched {offset} donations so far...")
        
        logger.info(f"Retrieved {len(all_donations)} donations")
        return all_donations
    
    # =========================================================================
    # GRANTS
    # =========================================================================
    
    def get_grants(self, limit: int = 100, offset: int = 0) -> dict:
        """Get grants list"""
        return self._request("grant/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_grant(self, grant_id: int) -> dict:
        """Get specific grant details"""
        return self._request("grant/display", {"grant_id": grant_id})
    
    def get_grants_by_fund(self, funit_id: int = None, fund_name_link_id: int = None,
                           limit: int = 100, offset: int = 0) -> dict:
        """Get grants for a specific fund.
        
        Used by: Fund activity summaries, grant reporting
        
        Args:
            funit_id: Fund unit ID
            fund_name_link_id: Fund name link ID (sometimes used instead of funit_id)
            limit: Records per page
            offset: Pagination offset
        """
        data = {
            "view_limit": limit,
            "view_offset": offset
        }
        if funit_id:
            data["funit_id"] = funit_id
        if fund_name_link_id:
            data["fund_name_link_id"] = fund_name_link_id
        return self._request("grant/list", data)
    
    def get_grants_by_profile(self, profile_id: int, limit: int = 100, offset: int = 0) -> dict:
        """Get grants associated with a specific profile"""
        return self._request("grant/list", {
            "profile_id": profile_id,
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_all_grants(self, max_iterations: int = 100) -> list:
        """Get all grants across all pages (5,338+ records)
        
        Used by: Quarterly grant reports, inactive fund analysis
        """
        return self._get_all_pages("grant/list", max_iterations=max_iterations)
    
    # =========================================================================
    # CHECKS
    # =========================================================================
    
    def get_checks(self, limit: int = 100, offset: int = 0) -> dict:
        """Get checks list.
        
        Used by: Uncashed check reports (Muhi)
        
        Check fields include:
        - check_id, check_num, check_date, amount
        - cleared (0/1): Whether the check has been cashed
        - voided (0/1), void_date, void_reason
        - account_name, account_id
        - is_electronic (0/1), memo
        """
        return self._request("check/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_check(self, check_id: int) -> dict:
        """Get specific check details"""
        return self._request("check/display", {"check_id": check_id})
    
    def get_all_checks(self, max_iterations: int = 60) -> list:
        """Get all checks across all pages (5,324+ records)"""
        return self._get_all_pages("check/list", max_iterations=max_iterations)
    
    def get_uncashed_checks(self, max_pages: int = 5) -> list:
        """Get checks that haven't been cleared (not cashed yet).

        Used by: Muhi's "which charities have cashed their checks" query

        Capped at max_pages (default 5 = 500 checks) to avoid tying up
        gunicorn workers. Full check list is 5750+ records.

        Returns:
            list of check dicts where cleared == 0 and voided == 0
        """
        all_checks = []
        offset = 0
        batch_size = 100

        for _ in range(max_pages):
            result = self._request("check/list", {
                "view_limit": batch_size,
                "view_offset": offset
            })
            if not result.get("success") or not result.get("data"):
                break
            page = result["data"].get("results", [])
            if not page:
                break
            all_checks.extend(page)
            if len(page) < batch_size:
                break
            offset += batch_size

        uncashed = [
            c for c in all_checks
            if c.get("cleared") == 0 and c.get("voided") == 0
            and not c.get("unused", 0)
        ]

        logger.info(f"Found {len(uncashed)} uncashed checks out of {len(all_checks)} fetched (capped at {max_pages} pages)")
        return uncashed
    
    # =========================================================================
    # VOUCHERS
    # =========================================================================
    
    def get_vouchers(self, limit: int = 100, offset: int = 0) -> dict:
        """Get vouchers list"""
        return self._request("voucher/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_voucher(self, voucher_id: int) -> dict:
        """Get specific voucher details"""
        return self._request("voucher/display", {"voucher_id": voucher_id})
    
    # =========================================================================
    # EVENTS
    # =========================================================================
    
    def get_event_dates(self, limit: int = 100) -> dict:
        """Get event dates list (campaigns)"""
        return self._request("event/list/dates", {"view_limit": limit})
    
    def get_event_date(self, event_date_id: int) -> dict:
        """Get specific event date details including attendees"""
        return self._request("event/display/eventdate", {"event_date_id": event_date_id})
    
    def get_event(self, event_id: int) -> dict:
        """Get specific event details"""
        return self._request("event/display", {"event_id": event_id})
    
    def create_event_date(self, event_id: int, **kwargs) -> dict:
        """Create a new event date.
        
        Args:
            event_id: Parent event ID (required)
            **kwargs: event_date, start_time, location, event_description, etc.
        """
        check_input_fields(kwargs, "event/create/eventdate")
        data = {"event_id": event_id, **kwargs}
        logger.info(f"Creating event date for event {event_id}")
        return self._request("event/create/eventdate", data)
    
    def edit_event_date(self, event_date_id: int, **kwargs) -> dict:
        """Edit an existing event date.
        
        Args:
            event_date_id: Event date ID (required)
            **kwargs: Fields to update
        """
        check_input_fields(kwargs, "event/edit/eventdate")
        data = {"event_date_id": event_date_id, **kwargs}
        return self._request("event/edit/eventdate", data)
    
    # =========================================================================
    # TASKS
    # =========================================================================
    
    def get_tasks(self, limit: int = 100) -> dict:
        """Get CSuite tasks list"""
        return self._request("task/list", {"view_limit": limit})
    
    def get_task(self, task_id: int) -> dict:
        """Get specific task details"""
        return self._request("task/display", {"task_id": task_id})
    
    def create_task(self, name: str, employee_id: int, due_date: str = None,
                    description: str = None, linked_profile_id: int = None,
                    task_type_id: int = None, **kwargs) -> dict:
        """Create a task in CSuite, optionally linked to a profile.

        Every field name here was VERIFIED on 2026-10-01 by creating sandbox
        task 1034 and reading it back with `task/display`:

        | sent | read back as |
        |---|---|
        | `name` | the text appears in `task_description` |
        | `task_description` | `task_description` |
        | `employee_id` | `employee_id`, and `assigned_employee` |
        | `due_ts` `"2026-10-05"` | `due_date` `"2026-10-05"` |
        | `task_type_id` `1065` | `task_type_id`, and `task_type.task_type_name` |
        | `o` `"profile"` | `o` `"profile"` |
        | `id` `21661` | `id` `21661` |

        CSuite then derives `task_object`, e.g.
        `"Profile :: SENTINEL 6 - SANDBOX ONLY, HUBSYNC"`.

        **`name` and `due_ts` are both REQUIRED** — measured 2026-10-01, when a
        create without them was refused with
        `["due_ts: due_ts is required", "name: name is required"]`.

        **`due_date` is NOT the input name.** It was sent, and CSuite still
        demanded `due_ts`. It is the output name only.

        **The link is `o` + `id`, not `profile_id`.** `o` names the object type
        and `id` its id, so this is polymorphic — a task could presumably hang
        off a fund or a grant the same way, untested. `profile_id` is BLOCKED
        on this endpoint: it is confirmed for `profile/edit` and would be
        silently discarded here.

        Args:
            name: task title (required by CSuite)
            employee_id: the assignee's employee_id — 1006 is Carl and 1007 is
                Kods in the SANDBOX. 1007 is confirmed in production; 1006 is
                NOT (no production task carries it). Do not assume.
            due_date: YYYY-MM-DD, sent as `due_ts`. Required by CSuite, so a
                task with no due date cannot be created through this method.
            description: sent as `task_description`
            linked_profile_id: the profile this task is about. Sent as
                `o="profile"` + `id=<profile_id>`.
            task_type_id: 1065 is "DIY Form-Contact" in the SANDBOX. **NOT
                confirmed in production** — no production task carries any
                type at all.
            **kwargs: checked against the task/create allowlist first
        """
        data = {"name": name, "employee_id": employee_id}
        if due_date:
            # `due_ts`, not `due_date`. VERIFIED 2026-10-01: due_ts accepts
            # YYYY-MM-DD and reads back as due_date.
            data["due_ts"] = due_date
        if description:
            data["task_description"] = description
        if task_type_id is not None:
            data["task_type_id"] = task_type_id
        if linked_profile_id is not None:
            # The polymorphic link, VERIFIED on task 1034. `id` is the LINKED
            # object's id — the task's own id is `task_id`, which CSuite mints.
            data["o"] = "profile"
            data["id"] = linked_profile_id

        check_input_fields(kwargs, "task/create")
        data.update(kwargs)

        logger.info("Creating CSuite task %r for employee %s%s", name,
                    employee_id,
                    f" on profile {linked_profile_id}" if linked_profile_id
                    else " (unlinked)")
        response = self._request("task/create", data)

        # Read back, like the profile and fund creates. A task whose link went
        # missing looks exactly like a task that worked: `o` and `id` are two
        # names CSuite would discard in silence.
        if not (self.verify_writes and isinstance(response, dict)
                and response.get("success")):
            return response
        payload = response.get("data")
        task_id = payload.get("task_id") if isinstance(payload, dict) else None
        if task_id is None:
            logger.warning("no task_id came back from task/create; the task "
                           "was not verified")
            response["verified"] = None
            return response

        from sync.readback import (FieldDropped, ReadBackUnavailable,
                                   verify_task)
        record = None
        try:
            record = verify_task(self._request, data, task_id)
        except ReadBackUnavailable as e:
            logger.error("could not read task %s back: %s", task_id, e)
            response["verified"] = None
            response["task_warning"] = (
                f"⚠️ Task {task_id} was created but could not be read back, "
                "so its link and due date are unconfirmed. Check it in CSuite.")
        except FieldDropped as dropped:
            logger.error("task %s does not hold what was sent: %s", task_id,
                         sorted(dropped.dropped))
            response["verified"] = False
            response["task_warning"] = (
                f"⚠️ Task {task_id} was created but CSuite did not store: "
                f"{', '.join(sorted(dropped.dropped))}. Check it in CSuite.")
        else:
            response["verified"] = True
            # The assignee's NAME, so a confirmation can say "Zouita, Kods"
            # rather than "1007". It is only known after the read-back:
            # task/create returns task_id and task_guid and nothing else.
            stored = record if isinstance(record, dict) else {}
            assigned = stored.get("assigned_employee") or {}
            if assigned.get("employee_name"):
                response["assignee_name"] = assigned["employee_name"]
        return response

    def complete_task(self, task_id: int = None, task_guid: str = None) -> dict:
        """Mark a CSuite task as complete.
        
        Args:
            task_id: Task ID (use one or the other)
            task_guid: Task GUID (use one or the other)
        """
        data = {}
        if task_id:
            data["task_id"] = task_id
        if task_guid:
            data["task_guid"] = task_guid
        return self._request("task/edit/complete", data)
    
    # =========================================================================
    # ACCOUNTS
    # =========================================================================
    
    def get_accounts(self, limit: int = 100) -> dict:
        """Get accounts list"""
        return self._request("account/list", {"view_limit": limit})
    
    def get_investment_strategies(self) -> dict:
        """Get investment strategies (e.g., Saturna)"""
        return self._request("account/list/strategy")
    
    # =========================================================================
    # ACCOUNTS PAYABLE
    # =========================================================================
    
    def get_ap_summary(self) -> dict:
        """Get accounts payable summary by vendor.
        
        Returns AP and SP (scholarship payable) totals with aging buckets
        (30/60/90/91+ days).
        """
        return self._request("ap/list")
    
    def get_ap_open_vouchers(self) -> dict:
        """Get open vouchers that can be paid"""
        return self._request("ap/list/openvouchers")
    
    # =========================================================================
    # VENDORS & GRANTEES
    # =========================================================================
    
    def make_vendor(self, profile_id: int) -> dict:
        """Make a profile a vendor (required before creating vouchers for them)"""
        return self._request("vendor/create", {"profile_id": profile_id})
    
    def make_grantee(self, profile_id: int) -> dict:
        """Make a profile a grantee (required before creating grants for them)"""
        return self._request("grantee/create", {"profile_id": profile_id})
    
    # =========================================================================
    # GRANT TYPES & DISTRIBUTION TYPES
    # =========================================================================
    
    def get_grant_types(self) -> dict:
        """Get grant types (NTEE categories: Education, Human Services, etc.)"""
        return self._request("grant_type/list")
    
    def get_distribution_types(self) -> dict:
        """Get distribution types"""
        return self._request("distribution/list/type")
