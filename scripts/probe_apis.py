#!/usr/bin/env python3
"""
API Probe — read-only field inventory for CSuite and HubSpot
============================================================

Purpose
-------
Produce evidence of what the two APIs actually return, so CSuite fields can be
mapped to HubSpot properties from data instead of memory.

Read-only guarantee
-------------------
This script only calls list / search / display / properties endpoints. It never
creates, edits, patches or deletes anything.

A note on HTTP verbs: the CSuite v2 API uses POST as its *transport* for every
call, including `profile/list` and `funit/display`, because the HMAC signature
is computed over the JSON body. POST here does not mean "write". Every CSuite
endpoint probed below is a list/display read. The same applies to HubSpot's
`crm/v3/lists/search`, which is a POST-only read used as a fallback when the
plain GET is unavailable. Each such call is flagged in the output with
`post_transport: true` so the receipt is honest about it.

Masking
-------
Every value is passed through `mask_value()` before it is stored or printed.
See that function for the rules.

Usage
-----
    python scripts/probe_apis.py --system all --limit 5 --out scripts/probe_output
"""

import argparse
import json
import os
import re
import sys
import time
from collections import Counter, OrderedDict
from datetime import datetime

# Repo root on sys.path so `config` / `clients` import when run from anywhere.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


# =============================================================================
# MASKING
# =============================================================================

REDACTED = "<redacted>"

# Field-name patterns, checked in this order. First match wins.
_ID_EXACT = {"id", "guid", "uuid", "hs_object_id", "objectid"}
_ID_SUFFIXES = ("_id", "_ids", "_guid", "_uuid", "_num", "_number")
# camelCase identifiers: channelId, eventDateGuid, ...
_ID_CAMEL_RE = re.compile(r"[a-z0-9](Id|Ids|Guid|Uuid|ID|GUID|UUID)$")

# Names that must never be treated as identifiers, however they end.
_NEVER_ID_TOKENS = ("phone", "fax", "mobile", "ssn", "fedid", "tax_id",
                    "taxid", "account_number", "routing", "card")

# The brief names amount|balance|total|fee|value. That list alone would let
# `lifetime_giving` through untouched, which is exactly the number we least
# want in a committed file, so the list is widened to every money-shaped word
# these two APIs actually use.
_MONEY_TOKENS = (
    "amount", "balance", "total", "fee", "value",
    "giving", "gift", "contribution", "donation_amt", "revenue",
    "payment", "paid", "price", "cost", "principal", "deposit",
    "disbursement", "payout", "salary", "income",
)

# Date detection is split in two because plain substring matching is wrong
# here: "lifetime_value" contains "time" but is money, not a date. Ambiguous
# words are matched only as whole name parts; unambiguous ones as substrings.
_DATE_PARTS = {
    "date", "dates", "time", "times", "timestamp", "datetime",
    "created", "createdate", "modified", "updated", "at", "on",
    "epoch", "year", "years", "month", "quarter", "day",
    "expires", "expiration", "start", "end", "since", "until",
}
_DATE_SUBSTRINGS = ("date", "timestamp", "datetime")

_PART_SPLIT_RE = re.compile(r"[^A-Za-z0-9]+|(?<=[a-z0-9])(?=[A-Z])")

# Suffixes that mark a field as a label/enum rather than a figure. Checked
# before the money rule: `first_gift_fund` and `payment_method_name` hold a
# fund name and a payment method, not an amount, and reporting them as
# "<non-numeric>" hid what they actually are.
_LABEL_SUFFIXES = ("_name", "_type", "_status", "_method", "_code", "_label",
                   "_desc", "_fund", "_state", "_stage", "_kind", "_class")

_EMAIL_TOKENS = ("email", "e_mail", "mail_address")

_PII_TOKENS = (
    "name", "fund", "address", "addr", "street", "city", "state", "zip", "postal",
    "province", "country", "phone", "mobile", "fax", "salutation", "prefix",
    "suffix", "title", "household", "organization", "organisation", "company",
    "employer", "spouse", "contact", "recipient", "payee", "signer", "owner",
    "attention", "attn", "website", "url", "domain",
    # Government and financial identifiers: an EIN, SSN or bank account is
    # more sensitive than a name. (`account_id` stays an id — only the
    # account *number* is redacted.)
    "fedid", "ssn", "taxid", "tax_id", "ein", "tin",
    "account_number", "routing", "card_number",
)

# Free text that could carry donor names verbatim. Length is recorded so the
# type is still visible; the text itself never lands in the receipt.
_FREETEXT_TOKENS = (
    "memo", "note", "notes", "comment", "comments", "description", "desc",
    "body", "subject", "message", "content", "reason", "purpose", "narrative",
)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _norm(field_name):
    """Lowercase the leaf segment of a dotted field path."""
    if field_name is None:
        return ""
    leaf = str(field_name).split(".")[-1]
    return leaf.strip().lower()


def _is_id_field(name, raw_leaf=""):
    # A "number" is not automatically an identifier: `primary_phone_number`
    # ends in _number and was being kept verbatim by the id rule, which put a
    # real phone number in the receipt. These names are never ids.
    if _has_token(name, _NEVER_ID_TOKENS):
        return False
    if name in _ID_EXACT:
        return True
    if name.endswith(_ID_SUFFIXES):
        return True
    return bool(_ID_CAMEL_RE.search(raw_leaf or ""))


def _has_token(name, tokens):
    return any(tok in name for tok in tokens)


def _parts(raw_leaf):
    """Split a field name into lowercase words: 'lastModifiedDate' -> [...]"""
    return [p.lower() for p in _PART_SPLIT_RE.split(raw_leaf or "") if p]


def _is_date_field(name, raw_leaf):
    if any(part in _DATE_PARTS for part in _parts(raw_leaf)):
        return True
    return _has_token(name, _DATE_SUBSTRINGS)


def mask_email(value):
    """first char + '*' + '@domain'  ->  'j*@example.org'"""
    text = str(value)
    if "@" not in text:
        return REDACTED
    local, _, domain = text.partition("@")
    if not local:
        return "*@" + domain
    return "{}*@{}".format(local[0], domain)


def mask_amount(value):
    """Order of magnitude only. Never the real figure."""
    try:
        number = float(str(value).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return "<non-numeric>"

    sign = "-" if number < 0 else ""
    magnitude = abs(number)

    if magnitude == 0:
        return "$0"
    buckets = [
        (10, "$0-10"),
        (100, "$10-100"),
        (1_000, "$100-1k"),
        (10_000, "$1k-10k"),
        (100_000, "$10k-100k"),
        (1_000_000, "$100k-1M"),
        (10_000_000, "$1M-10M"),
    ]
    for ceiling, label in buckets:
        if magnitude < ceiling:
            return sign + label
    return sign + "$10M+"


def mask_value(field_name, value):
    """Mask one value according to its field name.

    Rules, in precedence order:
      1. None            -> None (nothing to leak)
      2. bool            -> kept (0/1 flags are the point of the inventory)
      3. dict / list     -> recursed / summarised
      4. id-ish name     -> kept as-is
      5. date-ish name   -> kept as-is (formats are what we are after)
      6. money-ish name, unless it ends in a label suffix (_name, _type,
         _fund, _status, ...) -> order of magnitude only ("$1k-10k")
      7. email field, or any value shaped like an email -> "j*@domain.org"
      8. name/address/phone and friends -> "<redacted>"
      9. free text       -> "<text len=N>"
     10. anything else   -> kept as-is (numbers, codes, enum strings, 0/1 flags)
    """
    if value is None:
        return None

    name = _norm(field_name)
    raw_leaf = str(field_name).split(".")[-1] if field_name is not None else ""

    if isinstance(value, bool):
        return value

    if isinstance(value, dict):
        return {k: mask_value(k, v) for k, v in value.items()}

    if isinstance(value, (list, tuple)):
        return [mask_value(field_name, v) for v in value]

    if _is_id_field(name, raw_leaf):
        return value

    if _is_date_field(name, raw_leaf):
        return value

    if _has_token(name, _MONEY_TOKENS) and not name.endswith(_LABEL_SUFFIXES):
        return mask_amount(value)

    if _has_token(name, _EMAIL_TOKENS):
        return mask_email(value)

    # A value shaped like an email is masked whatever the field is called.
    # This runs before the name-based PII rule so an address hiding in a
    # field named "contact_point" is still recognisably an email in the
    # receipt rather than a flat <redacted>.
    if isinstance(value, str) and _EMAIL_RE.match(value.strip()):
        return mask_email(value.strip())

    if _has_token(name, _PII_TOKENS):
        return REDACTED

    if _has_token(name, _FREETEXT_TOKENS):
        return "<text len={}>".format(len(str(value)))

    if isinstance(value, str) and len(value) > 80:
        return "<text len={}>".format(len(value))

    return value


# =============================================================================
# FIELD INSPECTION
# =============================================================================

_TOTAL_KEY_RE = re.compile(
    r"^(total|count|num_|number_|record_count|result_count|totalcount|"
    r"total_results|num_results|total_count|totalrecords)",
    re.IGNORECASE,
)

# Envelope keys that hold the record collection, in priority order.
_COLLECTION_KEYS = (
    "results", "records", "items", "objects", "rows", "inputs",
    "lists", "subscriptionDefinitions", "definitions", "participations",
    "contacts", "events", "forms", "channels", "breakdowns",
)

_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")
_NUMERIC_STR_RE = re.compile(r"^-?\d+(\.\d+)?$")


def infer_type(value):
    """Human-readable type, with a hint for strings that encode something."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, dict):
        return "dict"
    if isinstance(value, (list, tuple)):
        if not value:
            return "list[empty]"
        return "list[{}]".format(infer_type(value[0]))
    if isinstance(value, str):
        stripped = value.strip()
        if _DATETIME_RE.match(stripped):
            return "str(datetime)"
        if _DATE_ONLY_RE.match(stripped):
            return "str(date)"
        if _EMAIL_RE.match(stripped):
            return "str(email)"
        if _NUMERIC_STR_RE.match(stripped):
            return "str(numeric)"
        return "str"
    return type(value).__name__


def flatten(record, prefix="", depth=0, max_depth=3):
    """Flatten nested dicts into dotted paths so real fields are visible.

    HubSpot buries everything under `properties`; CSuite nests address blocks.
    Lists are left intact and typed as list[...] rather than exploded.
    """
    flat = OrderedDict()
    if not isinstance(record, dict):
        return {prefix or "<value>": record}
    for key, value in record.items():
        path = "{}.{}".format(prefix, key) if prefix else str(key)
        if isinstance(value, dict) and depth < max_depth and value:
            flat.update(flatten(value, path, depth + 1, max_depth))
        else:
            flat[path] = value
    return flat


def field_stats(records):
    """Per-field: type, % non-null across the sample, one masked example."""
    if not records:
        return []

    flattened = [flatten(r) for r in records]
    sample_size = len(flattened)

    order = []
    seen = set()
    for row in flattened:
        for key in row:
            if key not in seen:
                seen.add(key)
                order.append(key)

    stats = []
    for field in order:
        values = [row.get(field) for row in flattened]
        non_null = [
            v for v in values
            if v is not None and v != "" and v != [] and v != {}
        ]
        types = Counter(infer_type(v) for v in non_null)
        example = mask_value(field, non_null[0]) if non_null else None
        stats.append({
            "field": field,
            "type": types.most_common(1)[0][0] if types else "null",
            "all_types": sorted(types),
            "pct_populated": round(100.0 * len(non_null) / sample_size, 1),
            "non_null": len(non_null),
            "sample_size": sample_size,
            "example": example,
        })
    return stats


def find_records(payload):
    """Pull the record list out of a response of unknown shape.

    Returns (records, container_path).
    """
    if payload is None:
        return [], None
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)], "<root list>"
    if not isinstance(payload, dict):
        return [], None

    # Known envelope keys first. HubSpot is inconsistent: crm/v3/objects/*
    # uses "results", crm/v3/lists uses "lists", and
    # communication-preferences/v3/definitions uses "subscriptionDefinitions".
    for key in _COLLECTION_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            return [r for r in value if isinstance(r, dict)], key

    # Generic fallback: the first top-level key holding a non-empty list of
    # objects. Catches envelope names we have not met yet instead of silently
    # reporting zero records.
    for key, value in payload.items():
        if isinstance(value, list) and value and all(
                isinstance(item, dict) for item in value):
            return list(value), "{} (detected)".format(key)

    data = payload.get("data")
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)], "data"
    if isinstance(data, dict):
        nested, path = find_records(data)
        if nested:
            return nested, "data.{}".format(path)
        # An empty collection means zero records — not one record that happens
        # to be the envelope. Without this, a search returning no matches was
        # reported as "1 record with field `results`".
        for key in _COLLECTION_KEYS:
            if isinstance(data.get(key), list):
                return [], "data.{} (empty)".format(key)
        # A display endpoint: the payload itself is the single record.
        return [data], "data (single record)"

    # Single-object response with no envelope.
    scalar_ish = [k for k, v in payload.items() if not isinstance(v, (dict, list))]
    if scalar_ish:
        return [payload], "<root object>"
    return [], None


def find_reported_totals(payload):
    """Any total/count field the API volunteers, at root or under `data`."""
    found = {}

    def scan(obj, prefix=""):
        if not isinstance(obj, dict):
            return
        for key, value in obj.items():
            path = "{}.{}".format(prefix, key) if prefix else key
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                if _TOTAL_KEY_RE.match(str(key)):
                    found[path] = value
            elif isinstance(value, str) and _NUMERIC_STR_RE.match(value.strip()):
                if _TOTAL_KEY_RE.match(str(key)):
                    found[path] = value

    scan(payload)
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        scan(payload["data"], "data")
    if isinstance(payload, dict) and isinstance(payload.get("paging"), dict):
        scan(payload["paging"], "paging")
    return found


def top_level_keys(payload):
    if isinstance(payload, dict):
        return sorted(payload.keys())
    if isinstance(payload, list):
        return ["<root list len={}>".format(len(payload))]
    return []


# =============================================================================
# HTTP RECORDING
#
# Neither client surfaces the HTTP status or timing, and we are not allowed to
# add methods to them. So we wrap the transport they already use and record
# what goes past. Nothing about the clients' auth or behaviour changes.
# =============================================================================

class HttpRecorder:
    """Captures status / elapsed / raw body of the most recent HTTP call."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.status = None
        self.elapsed_ms = None
        self.raw = None
        self.transport_error = None

    def capture(self, response, elapsed_ms):
        self.status = getattr(response, "status_code", None)
        self.elapsed_ms = elapsed_ms
        try:
            self.raw = response.json()
        except Exception:
            body = getattr(response, "text", "") or ""
            self.raw = {"<non-json body>": body[:200]}


class _RequestsProxy:
    """Stands in for the `requests` module inside clients.hubspot.

    Forwards everything to the real module except `get` and `post`, which are
    timed and recorded. The only POST this script ever routes through HubSpot
    is `crm/v3/lists/search`, a read-only search that HubSpot exposes as
    POST-only. No mutating endpoint is called.
    """

    def __init__(self, real, recorder):
        self._real = real
        self._recorder = recorder

    def __getattr__(self, item):
        return getattr(self._real, item)

    def get(self, *args, **kwargs):
        return self._timed(self._real.get, *args, **kwargs)

    def post(self, *args, **kwargs):
        return self._timed(self._real.post, *args, **kwargs)

    def _timed(self, fn, *args, **kwargs):
        start = time.perf_counter()
        response = fn(*args, **kwargs)
        elapsed = (time.perf_counter() - start) * 1000.0
        self._recorder.capture(response, round(elapsed, 1))
        return response


def wrap_csuite_session(client, recorder):
    """Wrap the CSuite client's session.post to record status and timing."""
    original = client.session.post

    def recording_post(*args, **kwargs):
        start = time.perf_counter()
        response = original(*args, **kwargs)
        elapsed = (time.perf_counter() - start) * 1000.0
        recorder.capture(response, round(elapsed, 1))
        return response

    client.session.post = recording_post
    return original


# =============================================================================
# PROBE RUNNER
# =============================================================================

class Probe:
    """Runs one endpoint, records everything, never raises."""

    def __init__(self, recorder):
        self.recorder = recorder
        self.results = []
        # Unmasked bodies, kept in memory only so the discovery pass can
        # cross-reference endpoints. Nothing from here is ever written to
        # disk except through mask_value().
        self.raw_payloads = {}

    def run(self, system, endpoint, call, request_summary=None,
            post_transport=False, note=None, records_from=None,
            store_fields=True):
        """Call one endpoint and record the inventory.

        Args:
            system: "csuite" or "hubspot"
            endpoint: endpoint path, used as the record key
            call: zero-arg callable performing the request
            request_summary: masked description of what was sent
            post_transport: True when POST is the transport for a read
            note: free-form note for the receipt
            records_from: optional callable(payload) -> list of records
            store_fields: False to record counts only
        """
        self.recorder.reset()
        record = {
            "system": system,
            "endpoint": endpoint,
            "request": request_summary or {},
            "post_transport": post_transport,
            "note": note,
            "http_status": None,
            "elapsed_ms": None,
            "ok": False,
            "error": None,
            "top_level_keys": [],
            "raw_top_level_keys": [],
            "raw_body_type": None,
            "reported_totals": {},
            "record_count": 0,
            "records_container": None,
            "field_count": 0,
            "fields": [],
        }

        start = time.perf_counter()
        try:
            returned = call()
        except Exception as exc:  # errors never abort the run
            record["error"] = "{}: {}".format(type(exc).__name__, exc)
            record["elapsed_ms"] = round((time.perf_counter() - start) * 1000.0, 1)
            self.results.append(record)
            return record

        record["http_status"] = self.recorder.status
        record["elapsed_ms"] = (
            self.recorder.elapsed_ms
            if self.recorder.elapsed_ms is not None
            else round((time.perf_counter() - start) * 1000.0, 1)
        )

        payload = self.recorder.raw if self.recorder.raw is not None else returned

        # Client-level error (bad credentials, transport failure, API error).
        if isinstance(returned, dict) and returned.get("error"):
            record["error"] = str(returned["error"])[:300]
        if isinstance(payload, dict) and payload.get("errors"):
            record["error"] = record["error"] or str(payload["errors"])[:300]

        # A non-2xx body is an error envelope, not data, even when the API
        # does not use an "error"/"errors" key to say so.
        status = record["http_status"]
        if status is not None and not 200 <= status < 300 and not record["error"]:
            detail = ""
            if isinstance(payload, dict):
                detail = str(
                    payload.get("message")
                    or payload.get("status")
                    or payload
                )[:200]
            record["error"] = "HTTP {}: {}".format(status, detail).strip()

        record["top_level_keys"] = top_level_keys(payload)
        # Recorded whatever happened, so a wrong collection key is diagnosable
        # from the receipt rather than needing another run.
        record["raw_top_level_keys"] = top_level_keys(self.recorder.raw)
        record["raw_body_type"] = type(self.recorder.raw).__name__
        record["reported_totals"] = find_reported_totals(payload)

        if record["error"]:
            # An error envelope is not a record. Counting it as one would put
            # the error text into the field inventory as if it were data.
            records, container = [], None
        elif records_from is not None:
            records = records_from(payload) or []
            container = "<custom>"
        else:
            records, container = find_records(payload)

        record["record_count"] = len(records)
        record["records_container"] = container

        if records and store_fields:
            fields = field_stats(records)
            record["fields"] = fields
            record["field_count"] = len(fields)
        elif records:
            record["field_count"] = len(field_stats(records))

        record["ok"] = bool(
            record["error"] is None
            and (status is None or 200 <= status < 300)
        )
        self.raw_payloads[endpoint] = self.recorder.raw
        self.results.append(record)
        return record


# =============================================================================
# CSUITE
# =============================================================================

# Fund search uses a deliberately generic literal so no person's name is sent
# to the API or written into the receipt.
FUND_SEARCH_QUERY = "fund"

# H1: the properties the RE migration and the sync code both care about.
H1_PROPERTIES = [
    "first_gift_date", "first_gift_amount", "first_gift_fund",
    "latest_gift_date", "latest_gift_amount", "latest_gift_fund",
    "greatest_gift_amount", "greatest_gift_date",
    "lifetime_giving", "donation_count",
    "last_donation_amount", "last_donation_date",
    "csuite_profile_id", "csuite_fund_id",
    "hs_email_optout", "hs_marketable_status", "lastmodifieddate",
]

# H2: words that would signal an existing property we must not duplicate.
H2_KEYWORDS = ("fund", "daf", "endow", "deceased", "dead", "grant",
               "advisor", "holder", "relationship", "constituent")

# H5: the identity properties a CSuite -> HubSpot profile write would touch.
H5_IDENTITY_PROPERTIES = [
    "firstname", "lastname", "company", "address", "city", "state", "zip",
    "country", "phone", "mobilephone", "email",
]

MAPPED_HUBSPOT_PROPERTIES = [
    "lifetime_giving",
    "donation_count",
    "last_donation_amount",
    "last_donation_date",
    "csuite_profile_id",
    "csuite_fund_id",
]


def _first_id(payload, *candidates):
    """First value of any candidate key across the records in a payload."""
    records, _ = find_records(payload)
    for record in records:
        for key in candidates:
            value = record.get(key)
            if value not in (None, "", 0):
                return value
    return None


def probe_csuite(limit, recorder, probe):
    """Probe CSuite list/display endpoints. Returns an extras dict."""
    from clients.csuite import CSuiteClient

    client = CSuiteClient()
    wrap_csuite_session(client, recorder)

    post_note = "CSuite v2 signs the JSON body, so every read is an HTTP POST."

    probe.run(
        "csuite", "profile/list",
        lambda: client.get_profiles(limit=limit),
        {"view_limit": limit, "view_offset": 0},
        post_transport=True, note=post_note,
    )

    # profile/list/search is skipped on purpose: it needs a person's name as
    # the query and config.py holds no known name to use.
    probe.results.append({
        "system": "csuite", "endpoint": "profile/list/search",
        "request": {}, "post_transport": True,
        "note": "SKIPPED — requires a person name as the query and config.py "
                "defines no known profile name. Probing it would have meant "
                "inventing a name or reusing live donor data.",
        "http_status": None, "elapsed_ms": None, "ok": False,
        "error": "skipped (no known name in config)",
        "top_level_keys": [], "reported_totals": {}, "record_count": 0,
        "records_container": None, "field_count": 0, "fields": [],
        "skipped": True,
    })

    profiles_first_payload = recorder.raw
    profile_id = _first_id(profiles_first_payload, "profile_id", "id")

    if profile_id is not None:
        probe.run(
            "csuite", "profile/display",
            lambda: client.get_profile(profile_id),
            {"profile_id": profile_id},
            post_transport=True,
            note=post_note + " Profile id from the first profile/list record. "
                 "Read-only display, used for the reverse fund link (C1).",
        )
    else:
        probe.results.append(_skipped_record(
            "csuite", "profile/display",
            "no profile id available — profile/list returned no records"))

    probe.run(
        "csuite", "donation/list",
        lambda: client.get_donations(limit=limit),
        {"view_limit": limit, "view_offset": 0},
        post_transport=True, note=post_note,
    )
    probe.run(
        "csuite", "grant/list",
        lambda: client.get_grants(limit=limit),
        {"view_limit": limit, "view_offset": 0},
        post_transport=True, note=post_note,
    )

    probe.run(
        "csuite", "funit/list",
        lambda: client.get_funds(limit=limit),
        {"view_limit": limit, "view_offset": 0},
        post_transport=True, note=post_note,
    )
    funds_payload = recorder.raw
    fund_ids = _pick_fund_ids(
        funds_payload,
        activity_payloads=[probe.raw_payloads.get("donation/list"),
                           probe.raw_payloads.get("grant/list")],
        limit=3)
    fund_id = fund_ids[0] if fund_ids else None

    probe.run(
        "csuite", "funit/list/search",
        lambda: client.search_funds(FUND_SEARCH_QUERY),
        {"q": FUND_SEARCH_QUERY},
        post_transport=True,
        note=post_note + " Query is a fixed generic word, not a real name.",
    )

    # funit/list returns only 6 fields and no fgroup_id, so a representative
    # fund cannot be chosen from the list alone. Sample a few and let C1 merge:
    # one fund with a null profile_id proves nothing about the schema.
    for index, fid in enumerate(fund_ids):
        endpoint = "funit/display" if index == 0 else \
            "funit/display #{}".format(index + 1)
        probe.run(
            "csuite", endpoint,
            lambda f=fid: client.get_fund(f),
            {"funit_id": fid},
            post_transport=True,
            note=post_note + " Fund {} of {} sampled for the C1 profile link."
                 .format(index + 1, len(fund_ids)),
        )
    if not fund_ids:
        probe.results.append(_skipped_record(
            "csuite", "funit/display",
            "no fund id available — funit/list returned no records"))

    # C1 fallback: does searching funds by a profile id surface the link?
    # search_funds hits funit/list/search, a read.
    if profile_id is not None:
        probe.run(
            "csuite", "funit/list/search (by profile id)",
            lambda: client.search_funds(str(profile_id)),
            {"q": "<profile id from profile/list>"},
            post_transport=True,
            note=post_note + " C1 fallback: probes whether fund search "
                 "resolves a profile id to that profile's funds.",
        )
    else:
        probe.results.append(_skipped_record(
            "csuite", "funit/list/search (by profile id)",
            "no profile id available"))

    probe.run(
        "csuite", "funit/feetype",
        lambda: client.get_fund_fee_types(),
        {},
        post_transport=True,
        note=post_note + " Client sends no fund id for this endpoint.",
    )

    probe.run(
        "csuite", "event/list/dates",
        lambda: client.get_event_dates(limit=limit),
        {"view_limit": limit},
        post_transport=True, note=post_note,
    )
    events_payload = recorder.raw
    event_date_id = _first_id(events_payload, "event_date_id", "eventdate_id", "id")

    # C3 wants registrant shape across up to three events that actually have
    # a date, so the sample is not one empty placeholder event.
    # event/display/eventdate answers in two shapes: ticket/fund detail for
    # some event dates, registrant rows for others. Sampling a fixed three
    # made C3's answer depend on which shape happened to come back, so keep
    # asking (bounded) until three registrant-shaped responses are seen.
    candidate_ids = _dated_event_ids(events_payload, limit=C3_MAX_EVENT_CALLS)
    if not candidate_ids and event_date_id is not None:
        candidate_ids = [event_date_id]

    dated_event_ids = []
    registrant_events = 0
    for ev_id in candidate_ids:
        index = len(dated_event_ids)
        endpoint = ("event/display/eventdate" if index == 0
                    else "event/display/eventdate #{}".format(index + 1))
        probe.run(
            "csuite", endpoint,
            lambda eid=ev_id: client.get_event_date(eid),
            {"event_date_id": ev_id},
            post_transport=True,
            note=post_note + " Event date sampled for C3 registrant shape.",
        )
        dated_event_ids.append(ev_id)
        if _looks_like_registrants(recorder.raw):
            registrant_events += 1
        if registrant_events >= C3_WANTED_REGISTRANT_EVENTS:
            break
        if len(dated_event_ids) >= C3_MAX_EVENT_CALLS:
            break

    if not dated_event_ids:
        probe.results.append(_skipped_record(
            "csuite", "event/display/eventdate",
            "no event date id available — event/list/dates returned no records"))

    probe.run(
        "csuite", "check/list",
        lambda: client.get_checks(limit=limit),
        {"view_limit": limit, "view_offset": 0},
        post_transport=True, note=post_note,
    )

    # ---- One full page of profiles: totals + the newsletter field ----------
    full = probe.run(
        "csuite", "profile/list (full page)",
        lambda: client.get_profiles(limit=100),
        {"view_limit": 100, "view_offset": 0},
        post_transport=True,
        note="Single 100-record page. Field statistics only — no records are "
             "stored. Used to read the reported total and the newsletter field.",
    )
    profiles_payload = recorder.raw
    newsletter = _analyse_newsletter(
        profiles_payload, source_ok=full["ok"], source_error=full.get("error"))

    return {
        "profile_total": full.get("reported_totals", {}),
        "profile_full_page_count": full.get("record_count", 0),
        "newsletter": newsletter,
        "first_fund_id": fund_id,
        "sampled_fund_ids": fund_ids,
        "first_profile_id": profile_id,
        "first_event_date_id": event_date_id,
        "dated_event_ids": dated_event_ids,
        "registrant_shaped_events": registrant_events,
    }


_SYSTEM_FUND_IDS = {1000}


def _pick_fund_ids(payload, activity_payloads=(), limit=3):
    """Fund ids to sample for C1, most representative first.

    `funit/list` returns the lowest ids first, which are CSuite's internal
    system funds — they carry no advisor or holder data, so sampling them
    answers C1 with a false negative. Funds referenced by real donations and
    grants are the ones that would actually have an advisor, so those lead.
    """
    def fund_id_of(record):
        return record.get("funit_id") or record.get("fund_id") or record.get("id")

    active = []
    for activity in activity_payloads:
        for record in find_records(activity)[0]:
            fid = record.get("funit_id")
            if fid is not None and fid not in _SYSTEM_FUND_IDS and fid not in active:
                active.append(fid)

    listed, system = [], []
    for record in find_records(payload)[0]:
        fid = fund_id_of(record)
        if fid is None or fid in active:
            continue
        target = system if fid in _SYSTEM_FUND_IDS else listed
        if fid not in target:
            target.append(fid)

    return (active + listed + system)[:limit]


C3_WANTED_REGISTRANT_EVENTS = 3
C3_MAX_EVENT_CALLS = 8


def _is_registrant_row(record):
    """A registrant row rather than ticket/fund detail."""
    keys = {str(k).lower() for k in record}
    return "profile_id" in keys and bool(keys & {
        "rsvp", "attended", "event_profile_email", "event_profile_name",
        "guests"})


def _looks_like_registrants(payload):
    records, _ = find_records(payload)
    return any(_is_registrant_row(r) for r in records)


def _dated_event_ids(payload, limit=3):
    """Event date ids whose event_date is actually populated."""
    records, _ = find_records(payload)
    ids = []
    for record in records:
        if not record.get("event_date"):
            continue
        ev_id = record.get("event_date_id") or record.get("id")
        if ev_id is not None and ev_id not in ids:
            ids.append(ev_id)
        if len(ids) >= limit:
            break
    return ids


def _skipped_record(system, endpoint, reason):
    return {
        "system": system, "endpoint": endpoint, "request": {},
        "post_transport": True, "note": "SKIPPED — " + reason,
        "http_status": None, "elapsed_ms": None, "ok": False,
        "error": "skipped ({})".format(reason),
        "top_level_keys": [], "reported_totals": {}, "record_count": 0,
        "records_container": None, "field_count": 0, "fields": [],
        "skipped": True,
    }


def _analyse_newsletter(payload, source_ok=True, source_error=None):
    """Does profile/list carry a `newsletter` field, and what does it hold?

    `exists` is tri-state. False means profile/list answered and the field was
    genuinely absent; None means the call never succeeded, so the question is
    still open.
    """
    if not source_ok:
        return {"exists": None,
                "reason": "profile/list did not return successfully"
                          + (": {}".format(source_error) if source_error else "")}
    records, _ = find_records(payload)
    if not records:
        return {"exists": None, "reason": "no profile records returned"}

    flattened = [flatten(r) for r in records]
    matches = sorted({
        key for row in flattened
        for key in row
        if "newsletter" in key.lower()
    })
    if not matches:
        return {
            "exists": False,
            "sample_size": len(records),
            "fields_seen": sorted({k for row in flattened for k in row}),
        }

    detail = {}
    for field in matches:
        values = [row.get(field) for row in flattened]
        non_null = [v for v in values if v is not None and v != ""]
        distinct = Counter(
            json.dumps(v, default=str) if isinstance(v, (dict, list)) else v
            for v in non_null
        )
        detail[field] = {
            "type": Counter(infer_type(v) for v in non_null).most_common(1)[0][0]
                    if non_null else "null",
            "pct_populated": round(100.0 * len(non_null) / len(records), 1),
            # 0/1 opt-in flags are not personal data; recorded verbatim.
            "distinct_values": [
                {"value": v, "count": c} for v, c in distinct.most_common(10)
            ],
        }
    return {"exists": True, "sample_size": len(records), "fields": detail}


# =============================================================================
# HUBSPOT
# =============================================================================

def _property_catalog(payload):
    """Normalise a crm/v3/properties/* response into a flat catalog.

    Property metadata (internal name, label, type, group) is schema, not donor
    data, so it is recorded verbatim — that is the whole point of the receipt.
    """
    records, _ = find_records(payload)
    catalog = []
    for prop in records:
        hubspot_defined = prop.get("hubspotDefined", False)
        catalog.append({
            "name": prop.get("name"),
            "label": prop.get("label"),
            "type": prop.get("type"),
            "fieldType": prop.get("fieldType"),
            "groupName": prop.get("groupName"),
            "hubspotDefined": bool(hubspot_defined),
            "origin": "hubspot" if hubspot_defined else "custom",
            "calculated": bool(prop.get("calculated", False)),
            "options_count": len(prop.get("options") or []),
        })
    catalog.sort(key=lambda p: (p["origin"] != "custom", (p["name"] or "")))
    return catalog


def probe_hubspot(limit, recorder, probe):
    """Probe HubSpot read endpoints. Returns an extras dict."""
    import clients.hubspot as hubspot_module
    from clients.hubspot import HubSpotClient

    real_requests = hubspot_module.requests
    hubspot_module.requests = _RequestsProxy(real_requests, recorder)
    try:
        return _probe_hubspot_inner(limit, recorder, probe, HubSpotClient())
    finally:
        hubspot_module.requests = real_requests


def _probe_hubspot_inner(limit, recorder, probe, client):
    extras = {}

    # ---- Contact properties ------------------------------------------------
    contact_props_record = probe.run(
        "hubspot", "crm/v3/properties/contacts",
        lambda: client._get("crm/v3/properties/contacts"),
        {},
        note="Full contact property schema.",
    )
    contact_props = _property_catalog(recorder.raw)
    extras["contact_properties"] = contact_props
    extras["contact_properties_ok"] = contact_props_record["ok"]

    contact_names = {p["name"] for p in contact_props}
    # exists stays None unless the schema call actually answered — otherwise
    # every property would be reported "MISSING" on a bad token.
    extras["mapped_property_check"] = {
        name: {
            "exists": (name in contact_names) if contact_props_record["ok"] else None,
            "detail": next(
                (p for p in contact_props if p["name"] == name), None
            ),
        }
        for name in MAPPED_HUBSPOT_PROPERTIES
    }

    # ---- Company properties ------------------------------------------------
    probe.run(
        "hubspot", "crm/v3/properties/companies",
        lambda: client._get("crm/v3/properties/companies"),
        {},
        note="Full company property schema.",
    )
    extras["company_properties"] = _property_catalog(recorder.raw)

    # ---- Contact objects ---------------------------------------------------
    probe.run(
        "hubspot", "crm/v3/objects/contacts",
        lambda: client._get("crm/v3/objects/contacts", {"limit": limit}),
        {"limit": limit},
        note="HubSpot returns only default properties unless `properties=` is "
             "passed, so absent fields here do not mean absent in the schema.",
    )

    # ---- H1: contact sample scoped to the properties we care about --------
    # Without an explicit `properties=` HubSpot returns only its defaults, so
    # this second read is what makes "% populated" mean anything.
    h1_record = probe.run(
        "hubspot", "crm/v3/objects/contacts (H1 properties)",
        lambda: client._get("crm/v3/objects/contacts", {
            "limit": 100,
            "properties": ",".join(H1_PROPERTIES),
        }),
        {"limit": 100, "properties": H1_PROPERTIES},
        note="100-contact sample requesting the H1 properties explicitly, "
             "for the %-populated column.",
    )
    extras["h1_sample_ok"] = h1_record["ok"]
    extras["h1_sample_size"] = h1_record["record_count"]

    # ---- Lists -------------------------------------------------------------
    lists_record = probe.run(
        "hubspot", "crm/v3/lists",
        lambda: client._get("crm/v3/lists", {"count": 100}),
        {"count": 100},
        note="Plain GET listing.",
    )
    lists_payload = recorder.raw

    if not lists_record["ok"] or lists_record["record_count"] == 0:
        # HubSpot v3 exposes list enumeration only as POST /crm/v3/lists/search.
        # It is a read: it returns lists, it does not create or modify them.
        probe.run(
            "hubspot", "crm/v3/lists/search",
            lambda: client._post("crm/v3/lists/search", {"count": 100}),
            {"count": 100},
            post_transport=True,
            note="Fallback. HubSpot offers no GET that enumerates lists; "
                 "lists/search is POST-only and read-only.",
        )
        lists_payload = recorder.raw

    extras["lists"] = _summarise_lists(lists_payload)

    # ---- Marketing events --------------------------------------------------
    probe.run(
        "hubspot", "marketing/v3/marketing-events",
        lambda: client.get_marketing_events(limit=limit),
        {"limit": limit},
        note="Client method get_marketing_events (GET).",
    )

    # ---- H3: marketing event participation (GET only) ---------------------
    events_payload = recorder.raw
    external_id = _first_external_event_id(events_payload)
    extras["h3_external_event_id"] = external_id
    extras["h3_endpoints"] = {}

    if external_id:
        h3_calls = [
            ("marketing/v3/marketing-events/external/{}".format(external_id),
             "event read by external id (client method)"),
            ("marketing/v3/marketing-events/participations/{}/breakdown".format(
                external_id), "participation breakdown by external event id"),
            ("marketing/v3/marketing-events/participations/contacts/"
             "{}/breakdown".format("<contact id>"),
             "participation breakdown by contact id"),
        ]
        for endpoint, description in h3_calls:
            if "<contact id>" in endpoint:
                contact_id = _first_contact_id(
                    probe.raw_payloads.get("crm/v3/objects/contacts"))
                if not contact_id:
                    probe.results.append(_skipped_record(
                        "hubspot", endpoint,
                        "no contact id available from crm/v3/objects/contacts"))
                    extras["h3_endpoints"][endpoint] = {
                        "http_status": None, "supported": None,
                        "note": "skipped — no contact id"}
                    continue
                endpoint = endpoint.replace("<contact id>", str(contact_id))
            record = probe.run(
                "hubspot", endpoint,
                lambda ep=endpoint: client._get(ep),
                {},
                note="H3 — {} (GET only).".format(description),
            )
            extras["h3_endpoints"][endpoint] = {
                "http_status": record["http_status"],
                "supported": None if record["http_status"] is None
                             else bool(200 <= record["http_status"] < 300),
                "error": record["error"],
                "description": description,
                "subscriber_states": _subscriber_states(recorder.raw),
            }
    else:
        for endpoint in ("marketing/v3/marketing-events/external/<id>",
                         "marketing/v3/marketing-events/participations/"
                         "<externalEventId>/breakdown",
                         "marketing/v3/marketing-events/participations/"
                         "contacts/<contactId>/breakdown"):
            probe.results.append(_skipped_record(
                "hubspot", endpoint,
                "no synced event with a csuite- externalEventId available"))
            extras["h3_endpoints"][endpoint] = {
                "http_status": None, "supported": None,
                "note": "skipped — no csuite- externalEventId found"}

    # ---- Subscription definitions -----------------------------------------
    probe.run(
        "hubspot", "communication-preferences/v3/definitions",
        lambda: client._get("communication-preferences/v3/definitions"),
        {},
        note="Subscription types available for newsletter sync.",
    )
    extras["subscription_definitions"] = _summarise_subscriptions(recorder.raw)

    # ---- Forms -------------------------------------------------------------
    probe.run(
        "hubspot", "marketing/v3/forms",
        lambda: client.get_forms(limit=100),
        {"limit": 100},
        note="Names and ids only.",
    )
    extras["forms"] = _summarise_forms(recorder.raw)

    # ---- Marketing emails --------------------------------------------------
    probe.run(
        "hubspot", "marketing/v3/emails",
        lambda: client._get("marketing/v3/emails", {"limit": limit}),
        {"limit": limit},
        note="Ids, names and state only.",
    )
    extras["marketing_emails"] = _summarise_emails(recorder.raw)

    # ---- Clone source resolution ------------------------------------------
    clone_ids = sorted(set(client.EMAIL_CLONE_SOURCES.values()))
    clone_results = {}
    for email_id in clone_ids:
        record = probe.run(
            "hubspot", "marketing/v3/emails/{}".format(email_id),
            lambda eid=email_id: client._get("marketing/v3/emails/{}".format(eid)),
            {"email_id": email_id},
            note="EMAIL_CLONE_SOURCES resolution check.",
        )
        raw = recorder.raw if isinstance(recorder.raw, dict) else {}
        aliases = sorted(
            k for k, v in client.EMAIL_CLONE_SOURCES.items() if v == email_id
        )
        clone_results[email_id] = {
            "aliases": aliases,
            "http_status": record["http_status"],
            # None when no HTTP happened at all: unresolved is not the same
            # claim as "we never asked".
            "resolved": None if record["http_status"] is None
                        else bool(200 <= record["http_status"] < 300),
            "name": raw.get("name"),
            "state": raw.get("state"),
            "error": record["error"],
        }
    extras["email_clone_sources"] = clone_results

    # ---- Social channels ---------------------------------------------------
    probe.run(
        "hubspot", "broadcast/v1/channels/setting/publish/current",
        lambda: client.get_social_channels(),
        {},
        note="Channel names and guids.",
    )
    extras["social_channels"] = _summarise_channels(recorder.raw)

    return extras


def _first_external_event_id(payload):
    """externalEventId of the first synced (csuite-prefixed) marketing event."""
    records, _ = find_records(payload)
    fallback = None
    for record in records:
        value = record.get("externalEventId")
        if not value:
            continue
        if str(value).startswith("csuite-"):
            return value
        fallback = fallback or value
    return fallback


def _first_contact_id(payload):
    records, _ = find_records(payload)
    for record in records:
        value = record.get("id") or record.get("hs_object_id")
        if value:
            return value
    return None


def _subscriber_states(payload):
    """Distinct subscriberState values a participation response reports."""
    if not isinstance(payload, (dict, list)):
        return []
    found = set()

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if "subscriberstate" in str(key).lower() and isinstance(value, str):
                    found.add(value)
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return sorted(found)


def _records_of(payload):
    records, _ = find_records(payload)
    return records


def _summarise_lists(payload):
    out = []
    for item in _records_of(payload):
        out.append({
            "listId": item.get("listId") or item.get("id"),
            "name": item.get("name"),
            "processingType": item.get("processingType") or item.get("listType"),
            "objectTypeId": item.get("objectTypeId"),
            "size": item.get("size") or item.get("additionalProperties", {}).get("hs_list_size"),
        })
    return out


def _summarise_forms(payload):
    return [
        {"id": f.get("id") or f.get("guid"),
         "name": f.get("name"),
         "formType": f.get("formType")}
        for f in _records_of(payload)
    ]


def _summarise_emails(payload):
    return [
        {"id": e.get("id"), "name": e.get("name"), "state": e.get("state"),
         "publishDate": e.get("publishDate")}
        for e in _records_of(payload)
    ]


def _summarise_subscriptions(payload):
    return [
        {"id": s.get("id"), "name": s.get("name"),
         "communicationMethod": s.get("communicationMethod"),
         "isActive": s.get("isActive"),
         "isDefault": s.get("isDefault"),
         "subscriptionType": s.get("subscriptionType")}
        for s in _records_of(payload)
    ]


def _summarise_channels(payload):
    out = []
    for c in _records_of(payload):
        out.append({
            "channelGuid": c.get("channelGuid") or c.get("channelId"),
            "channelKey": c.get("channelKey"),
            "name": c.get("name") or c.get("accountDisplayName"),
            "type": c.get("type") or c.get("channelType"),
            "active": c.get("active"),
        })
    return out


# =============================================================================
# MAPPINGS ALREADY IN CODE
#
# Read out of sync/donations.py, sync/events.py, sync/newsletter.py and
# intents/daf_workflow.py. Status "in code" means the repo writes it today —
# not that the field was confirmed to exist by this probe.
# =============================================================================

IN_CODE_MAPPINGS = [
    # (csuite endpoint, csuite field, hubspot property, source, note)
    ("donation/list", "donation_amount (sum by profile_id)", "lifetime_giving",
     "sync/donations.py", "float -> str, 2dp"),
    ("donation/list", "donation_id (count by profile_id)", "donation_count",
     "sync/donations.py", "int -> str"),
    ("donation/list", "donation_amount (most recent by donation_date)",
     "last_donation_amount", "sync/donations.py", "float -> str, 2dp"),
    ("donation/list", "donation_date (max)", "last_donation_date",
     "sync/donations.py", "YYYY-MM-DD -> YYYY-MM-DDT00:00:00.000Z"),
    ("donation/list", "profile_id", "(join key only)", "sync/donations.py",
     "links donations to profiles"),
    ("profile/list", "profile_id", "csuite_profile_id", "sync/donations.py",
     "int -> str"),
    ("profile/list", "primary_email", "email (match key)", "sync/donations.py",
     "lowercased + stripped; contact lookup key for the whole sync"),

    ("event/list/dates", "event_description (falls back to event_name)",
     "marketing event .eventName", "sync/events.py",
     "CSuite event_name is generic e.g. 'Event - Other'"),
    ("event/list/dates", "event_description + location",
     "marketing event .eventDescription", "sync/events.py", "joined with ' | '"),
    ("event/list/dates", "event_date + start_time",
     "marketing event .startDateTime", "sync/events.py",
     "messy time strings parsed; falls back to midnight"),
    ("event/list/dates", "event_date + start_time + 2h",
     "marketing event .endDateTime", "sync/events.py", "default 2h duration"),
    ("event/list/dates", "event_date_id (falls back to event_id)",
     "marketing event .externalEventId", "sync/events.py",
     "prefixed 'csuite-'; dedupe key"),
    ("event/list/dates", "event_type_code", "marketing event .eventType",
     "sync/events.py", "mapped through map_event_type, default 'Other'"),
    ("event/list/dates", "location", "(folded into eventDescription)",
     "sync/events.py", "no dedicated HubSpot property"),
    ("event/list/dates", "archived", "(filter only — never written)",
     "sync/events.py", "archived events are skipped"),
    ("(config)", "DEFAULT_EVENT_OWNER_ID", "marketing event .eventOrganizer",
     "sync/events.py", "constant, not a CSuite field"),

    ("profile/list", "newsletter (== 1)",
     "communication-preferences subscription 1265988358", "sync/newsletter.py",
     "opt-in only; never unsubscribes"),
    ("profile/list", "primary_email", "subscribe_contact(email)",
     "sync/newsletter.py", "lowercased + stripped"),
    ("profile/list", "name", "(logged only — never written)",
     "sync/newsletter.py", ""),

    ("profile/create/individual (response)", "profile_id", "csuite_profile_id",
     "intents/daf_workflow.py", "int -> str; write-direction workflow"),
    ("funit/create (response)", "funit_id", "csuite_fund_id",
     "intents/daf_workflow.py", "int -> str; write-direction workflow"),
    ("(HubSpot form submission)", "firstname / lastname / email / phone",
     "-> CSuite profile/create/individual", "intents/daf_workflow.py",
     "reverse direction: HubSpot -> CSuite"),
]

# HubSpot contact properties the repo writes to today.
WRITTEN_HUBSPOT_PROPERTIES = {
    "lifetime_giving", "donation_count", "last_donation_amount",
    "last_donation_date", "csuite_profile_id", "csuite_fund_id",
    "firstname", "lastname", "email", "phone", "constituent_codes",
}

# CSuite fields already accounted for by the in-code mappings above.
MAPPED_CSUITE_FIELDS = {
    "profile/list": {"profile_id", "primary_email", "newsletter", "name"},
    "donation/list": {"donation_amount", "donation_date", "donation_id",
                      "profile_id"},
    "event/list/dates": {"event_description", "event_name", "event_date",
                         "start_time", "event_date_id", "event_id",
                         "event_type_code", "location", "archived"},
}


# =============================================================================
# REPORT WRITERS
# =============================================================================

def _cell(value):
    """Render one value for a markdown table cell."""
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "`{}`".format(value)
    text = json.dumps(value, default=str) if isinstance(value, (dict, list)) else str(value)
    text = text.replace("|", "\\|").replace("\n", " ")
    if len(text) > 70:
        text = text[:67] + "..."
    return "`{}`".format(text)


def _tri(value, yes="**yes**", no="**NO**", unknown="undetermined"):
    """Render a True/False/None tri-state.

    None means the call that would have answered this did not succeed. Saying
    "MISSING" there would be a claim the probe never earned.
    """
    if value is None:
        return unknown
    return yes if value else no


def _ms(record):
    """Elapsed milliseconds. 0.0 is a real measurement, not a missing one."""
    value = record.get("elapsed_ms")
    return "—" if value is None else value


def _status_cell(record):
    if record.get("skipped"):
        return "skipped"
    status = record.get("http_status")
    if status is None:
        return "no HTTP" if record.get("error") else "—"
    return str(status)


def write_csuite_fields(results, extras, path):
    lines = [
        "# CSuite field inventory",
        "",
        "Generated by `scripts/probe_apis.py`. Overwritten on every run.",
        "",
        "Generated: {}".format(datetime.now().isoformat(timespec="seconds")),
        "",
        "All values are masked. Emails are `x*@domain`; names, addresses and "
        "phones are `<redacted>`; anything whose field name contains "
        "amount/balance/total/fee/value is reduced to an order of magnitude; "
        "ids and dates are kept verbatim because the formats are the point.",
        "",
        "> CSuite v2 signs the JSON request body, so every read below is an "
        "HTTP POST. These are all list/display endpoints — nothing is written.",
        "",
        "## Summary",
        "",
        "| Endpoint | Status | Elapsed ms | Records | Fields | Reported total |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for r in results:
        totals = ", ".join(
            "{}={}".format(k, v) for k, v in (r.get("reported_totals") or {}).items()
        ) or "—"
        lines.append("| `{}` | {} | {} | {} | {} | {} |".format(
            r["endpoint"], _status_cell(r), _ms(r),
            r["record_count"], r["field_count"], totals))

    lines += ["", "## Newsletter field on profiles", ""]
    newsletter = extras.get("newsletter") or {}
    if newsletter.get("exists") is True:
        lines.append("`newsletter` **exists** on `profile/list` records "
                     "(sample of {} from one 100-record page).".format(
                         newsletter.get("sample_size")))
        lines += ["", "| Field | Type | % populated | Distinct values (count) |",
                  "| --- | --- | --- | --- |"]
        for field, detail in (newsletter.get("fields") or {}).items():
            values = ", ".join(
                "{} ({})".format(_cell(d["value"]), d["count"])
                for d in detail["distinct_values"]
            )
            lines.append("| `{}` | `{}` | {}% | {} |".format(
                field, detail["type"], detail["pct_populated"], values or "—"))
    elif newsletter.get("exists") is False:
        lines.append("`newsletter` **does not appear** on any `profile/list` "
                     "record in a 100-record sample. `sync/newsletter.py` "
                     "reads `profile.get('newsletter', 0)` and would find "
                     "nothing to sync.")
    else:
        lines.append("**undetermined** — {}.".format(
            newsletter.get("reason", "no data")))

    lines += ["", "## Endpoints", ""]
    for r in results:
        lines += ["### `{}`".format(r["endpoint"]), ""]
        lines.append("- HTTP status: **{}**".format(_status_cell(r)))
        lines.append("- Elapsed: {} ms".format(_ms(r)))
        lines.append("- Request: {}".format(_cell(r.get("request") or {})))
        lines.append("- Top-level response keys: {}".format(
            ", ".join("`{}`".format(k) for k in r["top_level_keys"]) or "—"))
        lines.append("- Records container: {}".format(
            _cell(r.get("records_container"))))
        lines.append("- Records returned: {}".format(r["record_count"]))
        totals = r.get("reported_totals") or {}
        lines.append("- Total/count reported by the API: {}".format(
            ", ".join("`{}` = {}".format(k, v) for k, v in totals.items()) or "none"))
        if r.get("note"):
            lines.append("- Note: {}".format(r["note"]))
        if r.get("error"):
            lines.append("- **Error:** `{}`".format(str(r["error"]).replace("|", "\\|")))
        lines.append("")

        if r["fields"]:
            lines += ["| Field | Type | % populated | Example (masked) |",
                      "| --- | --- | --- | --- |"]
            for f in r["fields"]:
                lines.append("| `{}` | `{}` | {}% | {} |".format(
                    f["field"], f["type"], f["pct_populated"], _cell(f["example"])))
            lines.append("")

    _write(path, "\n".join(lines))


def write_hubspot_properties(results, extras, path):
    by_endpoint = {r["endpoint"]: r for r in results}
    lines = [
        "# HubSpot property inventory",
        "",
        "Generated by `scripts/probe_apis.py`. Overwritten on every run.",
        "",
        "Generated: {}".format(datetime.now().isoformat(timespec="seconds")),
        "",
        "Property metadata (internal name, label, type, group) is schema, not "
        "donor data, and is recorded verbatim. Record-level values are masked.",
        "",
        "## Summary",
        "",
        "| Endpoint | Status | Elapsed ms | Records | Fields |",
        "| --- | --- | --- | --- | --- |",
    ]
    for r in results:
        lines.append("| `{}` | {} | {} | {} | {} |".format(
            r["endpoint"], _status_cell(r), _ms(r),
            r["record_count"], r["field_count"]))

    # ---- The six mapped properties ----------------------------------------
    lines += ["", "## Properties the sync code depends on", "",
              "| Property | Exists | Type | Field type | Group | Origin |",
              "| --- | --- | --- | --- | --- | --- |"]
    for name in MAPPED_HUBSPOT_PROPERTIES:
        check = (extras.get("mapped_property_check") or {}).get(name, {})
        detail = check.get("detail") or {}
        lines.append("| `{}` | {} | {} | {} | {} | {} |".format(
            name, _tri(check.get("exists")),
            _cell(detail.get("type")), _cell(detail.get("fieldType")),
            _cell(detail.get("groupName")), _cell(detail.get("origin"))))

    # ---- Full property tables ---------------------------------------------
    for label, key in (("Contact", "contact_properties"),
                       ("Company", "company_properties")):
        props = extras.get(key) or []
        custom = [p for p in props if p["origin"] == "custom"]
        builtin = [p for p in props if p["origin"] != "custom"]
        lines += [
            "", "## {} properties".format(label), "",
            "{} total — {} custom, {} HubSpot-defined. Custom first.".format(
                len(props), len(custom), len(builtin)),
            "",
            "### Custom {} properties".format(label.lower()),
            "",
        ]
        lines += _property_table(custom)
        lines += ["", "### HubSpot-defined {} properties".format(label.lower()), ""]
        lines += _property_table(builtin)

    # ---- Contact object field stats ---------------------------------------
    contacts = by_endpoint.get("crm/v3/objects/contacts")
    lines += ["", "## `crm/v3/objects/contacts` sample field stats", ""]
    if contacts and contacts["fields"]:
        lines.append(contacts.get("note") or "")
        lines += ["", "| Field | Type | % populated | Example (masked) |",
                  "| --- | --- | --- | --- |"]
        for f in contacts["fields"]:
            lines.append("| `{}` | `{}` | {}% | {} |".format(
                f["field"], f["type"], f["pct_populated"], _cell(f["example"])))
    else:
        lines.append("No records returned. {}".format(
            (contacts or {}).get("error") or ""))

    # ---- Lists / forms / subscriptions / emails / channels ----------------
    lines += ["", "## Lists", ""]
    lists = extras.get("lists") or []
    if lists:
        lines += ["| List id | Name | Processing type | Object type | Size |",
                  "| --- | --- | --- | --- | --- |"]
        for item in lists:
            lines.append("| {} | {} | {} | {} | {} |".format(
                _cell(item["listId"]), _cell(item["name"]),
                _cell(item["processingType"]), _cell(item["objectTypeId"]),
                _cell(item["size"])))
    else:
        lines.append("None returned.")

    lines += ["", "## Forms", ""]
    forms = extras.get("forms") or []
    if forms:
        lines += ["| Form id | Name | Type |", "| --- | --- | --- |"]
        for f in forms:
            lines.append("| {} | {} | {} |".format(
                _cell(f["id"]), _cell(f["name"]), _cell(f["formType"])))
    else:
        lines.append("None returned.")

    lines += ["", "## Subscription definitions", ""]
    subs = extras.get("subscription_definitions") or []
    if subs:
        lines += ["| Id | Name | Method | Active | Default | Type |",
                  "| --- | --- | --- | --- | --- | --- |"]
        for s in subs:
            lines.append("| {} | {} | {} | {} | {} | {} |".format(
                _cell(s["id"]), _cell(s["name"]), _cell(s["communicationMethod"]),
                _cell(s["isActive"]), _cell(s["isDefault"]),
                _cell(s["subscriptionType"])))
    else:
        lines.append("None returned.")

    lines += ["", "## Marketing emails (sample)", ""]
    emails = extras.get("marketing_emails") or []
    if emails:
        lines += ["| Id | Name | State | Publish date |",
                  "| --- | --- | --- | --- |"]
        for e in emails:
            lines.append("| {} | {} | {} | {} |".format(
                _cell(e["id"]), _cell(e["name"]), _cell(e["state"]),
                _cell(e["publishDate"])))
    else:
        lines.append("None returned.")

    lines += ["", "### EMAIL_CLONE_SOURCES resolution", "",
              "| Email id | Aliases | HTTP | Resolved | Name | State |",
              "| --- | --- | --- | --- | --- | --- |"]
    for email_id, info in (extras.get("email_clone_sources") or {}).items():
        lines.append("| `{}` | {} | {} | {} | {} | {} |".format(
            email_id, ", ".join("`{}`".format(a) for a in info["aliases"]),
            _cell(info["http_status"]), _tri(info["resolved"]),
            _cell(info["name"]), _cell(info["state"])))

    lines += ["", "## Social channels", ""]
    channels = extras.get("social_channels") or []
    if channels:
        lines += ["| Channel guid | Key | Name | Type | Active |",
                  "| --- | --- | --- | --- | --- |"]
        for c in channels:
            lines.append("| {} | {} | {} | {} | {} |".format(
                _cell(c["channelGuid"]), _cell(c["channelKey"]), _cell(c["name"]),
                _cell(c["type"]), _cell(c["active"])))
    else:
        lines.append("None returned.")

    lines += ["", "## Errors", ""]
    failed = [r for r in results if r.get("error")]
    if failed:
        for r in failed:
            lines.append("- `{}` — {} — `{}`".format(
                r["endpoint"], _status_cell(r),
                str(r["error"]).replace("|", "\\|")))
    else:
        lines.append("None.")

    _write(path, "\n".join(lines))


def _property_table(props):
    if not props:
        return ["_None._"]
    rows = ["| Name | Label | Type | Field type | Group | Options |",
            "| --- | --- | --- | --- | --- | --- |"]
    for p in props:
        rows.append("| `{}` | {} | `{}` | `{}` | `{}` | {} |".format(
            p["name"], _cell(p["label"]), p["type"], p["fieldType"],
            p["groupName"], p["options_count"] or "—"))
    return rows


def write_mapping_draft(csuite_results, hubspot_extras, discovery, path):
    lines = [
        "# CSuite -> HubSpot mapping draft",
        "",
        "Generated by `scripts/probe_apis.py`. Overwritten on every run.",
        "",
        "Generated: {}".format(datetime.now().isoformat(timespec="seconds")),
        "",
        "Section 1 is what the repo does **today**, read out of the sync code. "
        "Section 2 is everything the probe saw that nothing connects yet. "
        "Nothing here has been decided — it is a worksheet.",
        "",
        "## 1. In code today",
        "",
        "| CSuite endpoint | CSuite field | -> HubSpot property | Status | Source | Note |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for endpoint, field, prop, source, note in IN_CODE_MAPPINGS:
        lines.append("| `{}` | `{}` | `{}` | in code | `{}` | {} |".format(
            endpoint, field, prop, source, note or ""))

    lines += ["", "## 2. Candidates", "",
              "### 2.0 What discovery established", "",
              "Carried over from `mapping_discovery.md`. An `undetermined:` "
              "answer means the question is still open, not answered no.", ""]
    lines += _table(["Item", "Finding"],
                    [("**{}**".format(d["id"]), d["answer"].replace("|", "\\|"))
                     for d in discovery])
    lines += ["",
              "### 2a. CSuite fields with no HubSpot counterpart", ""]

    any_candidates = False
    for record in csuite_results:
        if not record["fields"] or record.get("skipped"):
            continue
        already = MAPPED_CSUITE_FIELDS.get(record["endpoint"], set())
        unmapped = [
            f for f in record["fields"]
            if f["field"].split(".")[-1] not in already and f["pct_populated"] > 0
        ]
        if not unmapped:
            continue
        any_candidates = True
        lines += ["#### `{}`".format(record["endpoint"]), "",
                  "| CSuite field | Type | % populated | Example (masked) | "
                  "-> HubSpot property | Status |",
                  "| --- | --- | --- | --- | --- | --- |"]
        for f in unmapped:
            lines.append("| `{}` | `{}` | {}% | {} | _(none)_ | candidate |".format(
                f["field"], f["type"], f["pct_populated"], _cell(f["example"])))
        lines.append("")
    if not any_candidates:
        lines += ["_No CSuite records were returned, so no candidates could be "
                  "derived. Re-run with working credentials._", ""]

    lines += ["### 2b. HubSpot custom properties nothing writes to", ""]
    contact_props = hubspot_extras.get("contact_properties") or []
    custom_unwritten = [
        p for p in contact_props
        if p["origin"] == "custom"
        and p["name"] not in WRITTEN_HUBSPOT_PROPERTIES
        and not p["calculated"]
    ]
    if custom_unwritten:
        lines += ["| HubSpot property | Label | Type | Group | "
                  "<- CSuite field | Status |",
                  "| --- | --- | --- | --- | --- | --- |"]
        for p in custom_unwritten:
            lines.append("| `{}` | {} | `{}` | `{}` | _(none)_ | candidate |".format(
                p["name"], _cell(p["label"]), p["type"], p["groupName"]))
    else:
        lines.append("_No custom contact properties returned. Re-run with "
                     "working credentials._")

    company_custom = [
        p for p in (hubspot_extras.get("company_properties") or [])
        if p["origin"] == "custom" and not p["calculated"]
    ]
    lines += ["", "### 2c. Custom company properties (nothing in the repo "
              "writes to companies at all)", ""]
    if company_custom:
        lines += ["| HubSpot property | Label | Type | Group |",
                  "| --- | --- | --- | --- |"]
        for p in company_custom:
            lines.append("| `{}` | {} | `{}` | `{}` |".format(
                p["name"], _cell(p["label"]), p["type"], p["groupName"]))
    else:
        lines.append("_None returned._")

    _write(path, "\n".join(lines))


def _write(path, text):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text.rstrip() + "\n")


# =============================================================================
# DISCOVERY (probe extension #2)
#
# Every section answers one C*/H* question and ends in a single ANSWER line.
# When the call that would settle a question never succeeded, the answer is
# "undetermined: <reason>" — never a claim of absence.
# =============================================================================

C1_PROFILE_REF_TOKENS = ("profile", "advisor", "holder", "donor", "contact",
                         "steward")
C2_GRANTEE_TOKENS = ("grantee", "charity", "nonprofit", "organization",
                     "organisation", "recipient", "payee", "vendor", "org")
C2_STATUS_TOKENS = ("status", "paid", "cleared", "approved", "state", "stage",
                    "voided", "posted")
C4_IDENTITY_TOKENS = ("name", "org", "address", "addr", "street", "city",
                      "state", "zip", "postal", "country", "phone", "mobile",
                      "email", "dead", "deceased", "cf_profile", "created",
                      "modified", "updated")
C6_EXCLUSION_TOKENS = ("type", "soft", "credit", "anon", "refund", "reversed",
                       "void", "status", "in_memory", "tribute")


# Fields that identify the receiving profile on a grant record.
_GRANTEE_REF_FIELDS = {"name", "profile_id", "name_link_id",
                       "grantee_profile_id", "grantee_id"}


def _is_flag_field(field_stat):
    """A 0/1 integer flag rather than an identity or amount."""
    leaf = field_stat["field"].split(".")[-1].lower()
    return (field_stat["type"] in ("int", "bool")
            and any(leaf.endswith(suffix) or leaf.startswith(suffix)
                    for suffix in ("complete", "is_", "has_", "no_", "_flag",
                                   "check_", "void", "canceled", "cancelled")))


def _by_endpoint(probe):
    return {r["endpoint"]: r for r in probe.results}


def _fields_of(record):
    return (record or {}).get("fields") or []


def _field_names(record):
    return [f["field"] for f in _fields_of(record)]


def _pick(record, tokens):
    """Field stats whose leaf name contains any of the tokens."""
    out = []
    for field in _fields_of(record):
        leaf = field["field"].split(".")[-1].lower()
        if any(tok in leaf for tok in tokens):
            out.append(field)
    return out


def _distinct_values(payload, field_name, limit=12):
    """Masked distinct values of one field across a payload's records."""
    records, _ = find_records(payload)
    counter = Counter()
    for record in records:
        flat = flatten(record)
        if field_name not in flat:
            continue
        value = flat[field_name]
        masked = mask_value(field_name, value)
        if isinstance(masked, (dict, list)):
            masked = json.dumps(masked, default=str)[:60]
        counter[masked] += 1
    return [{"value": v, "count": c} for v, c in counter.most_common(limit)]


def _nested_stats(payloads, key):
    """Merge field stats for a nested array (profiles[] / guests[]) across
    several display payloads."""
    rows = []
    containers = 0
    for payload in payloads:
        records, _ = find_records(payload)
        for record in records:
            value = record.get(key)
            if isinstance(value, list):
                containers += 1
                rows.extend([item for item in value if isinstance(item, dict)])
    return {
        "key": key,
        "present_on": containers,
        "row_count": len(rows),
        "fields": field_stats(rows) if rows else [],
        "rows": rows,
    }


def _unavailable(record, what):
    """Reason string when an endpoint could not answer."""
    if record is None:
        return "{} was never called".format(what)
    if record.get("skipped"):
        return "{} skipped — {}".format(what, record.get("error"))
    if not record.get("ok"):
        return "{} did not succeed ({}: {})".format(
            what, _status_cell(record), record.get("error"))
    if record.get("record_count", 0) == 0:
        return "{} returned no records".format(what)
    return None


def _table(headers, rows):
    out = ["| " + " | ".join(headers) + " |",
           "| " + " | ".join("---" for _ in headers) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(c) for c in row) + " |")
    return out


def _field_rows(fields):
    return [(("`%s`" % f["field"]), ("`%s`" % f["type"]),
             "{}%".format(f["pct_populated"]), _cell(f["example"]))
            for f in fields]


FIELD_HEADERS = ["Field", "Type", "% populated", "Example (masked)"]


# ---------------------------------------------------------------------------
# CSuite discovery
# ---------------------------------------------------------------------------

def discover_c1(probe):
    """Profile <-> fund link direction."""
    idx = _by_endpoint(probe)
    lines = []
    fund_display = idx.get("funit/display")
    profile_display = idx.get("profile/display")
    profile_list = idx.get("profile/list")
    fund_search = idx.get("funit/list/search (by profile id)")

    # Forward: funit/display -> profile reference. Sampled across every
    # funit/display* call, because one fund with a null profile_id says
    # nothing about the schema.
    fund_endpoints = [e for e in idx if e.startswith("funit/display")]
    fund_display = idx.get("funit/display")
    forward_refs, forward_populated = [], []
    blocked = _unavailable(fund_display, "funit/display")
    if blocked:
        lines.append("**Forward (fund -> profile):** {}.".format(blocked))
    else:
        sampled = [idx[e] for e in fund_endpoints if idx[e].get("ok")]
        lines.append("**Forward (fund -> profile):** sampled {} fund(s) — {}."
                     .format(len(sampled),
                             ", ".join("`funit_id={}`".format(
                                 idx[e]["request"].get("funit_id"))
                                 for e in fund_endpoints if idx[e].get("ok"))))

        # Merge the per-fund field stats so % populated spans the sample.
        merged = {}
        for record in sampled:
            for f in _fields_of(record):
                cur = merged.setdefault(f["field"], dict(f))
                if f["pct_populated"] > cur["pct_populated"]:
                    merged[f["field"]] = dict(f)
        candidates = [f for name, f in merged.items()
                      if any(tok in name.split(".")[-1].lower()
                             for tok in C1_PROFILE_REF_TOKENS)]

        # A boolean config toggle named "..._advisors" is not a reference.
        # An employee/user id points at AMCF staff, not at a donor profile.
        # Reporting fund_steward_employee_id as the profile link would be wrong.
        staff = [f for f in candidates
                 if any(tok in f["field"].split(".")[-1].lower()
                        for tok in ("employee", "user", "staff"))]
        refs = [f for f in candidates
                if f not in staff and not _is_flag_field(f)
                and f["type"] != "bool"]
        flags = [f for f in candidates if f not in refs and f not in staff]

        profile_ids = set()
        for record in find_records(probe.raw_payloads.get("profile/list"))[0]:
            pid = record.get("profile_id") or record.get("id")
            if isinstance(pid, int):
                profile_ids.add(pid)
        by_value = []
        for endpoint in fund_endpoints:
            for record in find_records(probe.raw_payloads.get(endpoint))[0]:
                for key, value in flatten(record).items():
                    if isinstance(value, int) and value in profile_ids:
                        by_value.append(key)

        lines += ["", "Reference-shaped fields:", ""]
        lines += _table(FIELD_HEADERS, _field_rows(refs)) if refs else \
            ["_None._"]
        if staff:
            lines += ["", "Staff references (AMCF employees, **not** donor "
                      "profiles): {}".format(
                          ", ".join("`{}` ({}% populated)".format(
                              f["field"], f["pct_populated"]) for f in staff))]
        if flags:
            lines += ["", "Boolean config toggles that merely match the "
                      "keywords (not references): {}".format(
                          ", ".join("`%s`" % f["field"] for f in flags))]
        if by_value:
            lines += ["", "Fields whose int value matches a `profile_id` from "
                      "`profile/list`: {}".format(
                          ", ".join("`%s`" % f for f in sorted(set(by_value))))]

        forward_refs = sorted({f["field"] for f in refs} | set(by_value))
        forward_populated = sorted(
            {f["field"] for f in refs if f["pct_populated"] > 0}
            | set(by_value))
        if refs and not forward_populated:
            lines += ["", "Every reference-shaped field is **null on every "
                      "sampled fund** — the field exists in the schema but "
                      "carries no value here."]

    # Reverse: profile/display -> fund reference
    reverse_hits = []
    blocked_rev = _unavailable(profile_display, "profile/display")
    lines.append("")
    if blocked_rev:
        lines.append("**Reverse (profile -> fund):** {}.".format(blocked_rev))
    else:
        fund_fields = _pick(profile_display, ("fund", "funit"))
        list_fields = [f for f in _fields_of(profile_display)
                       if f["type"].startswith("list")]
        reverse_hits = sorted({f["field"] for f in fund_fields}
                              | {f["field"] for f in list_fields})
        lines.append("**Reverse (profile -> fund):** `profile/display` returned "
                     "{} fields.".format(profile_display["field_count"]))
        if fund_fields or list_fields:
            lines.append("")
            lines += _table(FIELD_HEADERS,
                            _field_rows(fund_fields + [
                                f for f in list_fields if f not in fund_fields]))
        else:
            lines.append("")
            lines.append("No fund id and no list-valued field on the profile.")

    # Fallback: fund search by profile id
    lines.append("")
    blocked_search = _unavailable(fund_search, "funit/list/search (by profile id)")
    if blocked_search:
        lines.append("**Fallback (fund search by profile id):** {}.".format(
            blocked_search))
    else:
        lines.append("**Fallback (fund search by profile id):** returned {} "
                     "record(s) with fields {}.".format(
                         fund_search["record_count"],
                         ", ".join("`%s`" % f for f in _field_names(fund_search))
                         or "none"))

    if forward_populated and reverse_hits:
        answer = ("both directions carry it — fund side {}, profile side {}"
                  .format(", ".join("`%s`" % f for f in forward_populated),
                          ", ".join("`%s`" % f for f in reverse_hits)))
    elif forward_populated:
        answer = ("fund -> profile carries it, via {} (populated)"
                  .format(", ".join("`%s`" % f for f in forward_populated)))
    elif reverse_hits:
        answer = ("profile -> fund carries it, via {}"
                  .format(", ".join("`%s`" % f for f in reverse_hits)))
    elif blocked and blocked_rev:
        answer = "undetermined: {}; {}".format(blocked, blocked_rev)
    elif forward_refs:
        answer = ("undetermined: `funit/display` defines {} but every one is "
                  "null across the sampled funds, and `profile/display` "
                  "exposes no fund field — needs a fund with an advisor set"
                  .format(", ".join("`%s`" % f for f in forward_refs)))
    else:
        answer = ("neither funit/display nor profile/display exposes the link; "
                  "fund search by profile id did not resolve it either")
    return {"id": "C1", "title": "Profile <-> fund link", "lines": lines,
            "answer": answer}


def discover_c2(probe):
    """Grantee identity, grant date, status and check join on grant/list."""
    idx = _by_endpoint(probe)
    record = idx.get("grant/list")
    lines = []
    blocked = _unavailable(record, "grant/list")
    if blocked:
        return {"id": "C2", "title": "Grantee on grants", "lines": [blocked],
                "answer": "undetermined: {}".format(blocked)}

    lines.append("`grant/list` returned {} fields over {} record(s).".format(
        record["field_count"], record["record_count"]))

    # CSuite models a grantee as a profile, so the receiving charity arrives as
    # a name + profile reference, not as a "grantee_*" field. Boolean
    # compliance flags that merely contain "charity" are reported separately.
    grantee = [f for f in _pick(record, C2_GRANTEE_TOKENS)
               if not _is_flag_field(f)]
    grantee += [f for f in _fields_of(record)
                if f["field"].split(".")[-1].lower() in _GRANTEE_REF_FIELDS
                and f not in grantee]
    compliance = [f for f in _pick(record, C2_GRANTEE_TOKENS)
                  if _is_flag_field(f)]

    lines += ["", "**Receiving-charity fields**", ""]
    lines += _table(FIELD_HEADERS, _field_rows(grantee)) if grantee else \
        ["_No field name matches {}._".format(
            ", ".join("`%s`" % t for t in C2_GRANTEE_TOKENS))]
    if compliance:
        lines += ["", "Compliance flags (not the grantee identity): {}".format(
            ", ".join("`%s`" % f["field"] for f in compliance))]

    dates = [f for f in _fields_of(record)
             if _is_date_field(f["field"].split(".")[-1].lower(),
                               f["field"].split(".")[-1])]
    lines += ["", "**Date fields (name + observed format)**", ""]
    lines += _table(FIELD_HEADERS, _field_rows(dates)) if dates else \
        ["_None._"]

    status = _pick(record, C2_STATUS_TOKENS)
    lines += ["", "**Status / paid / cleared fields**", ""]
    if status:
        rows = []
        payload = probe.raw_payloads.get("grant/list")
        for f in status:
            distinct = _distinct_values(payload, f["field"])
            rows.append((("`%s`" % f["field"]), ("`%s`" % f["type"]),
                         "{}%".format(f["pct_populated"]),
                         ", ".join("{} ({})".format(_cell(d["value"]), d["count"])
                                   for d in distinct) or "—"))
        lines += _table(["Field", "Type", "% populated", "Distinct values"], rows)
    else:
        lines.append("_None._")

    # A join key, not merely a field whose name contains "check":
    # `charity_check_complete` is a 0/1 compliance flag, not a check reference.
    check_fields = [f for f in _fields_of(record)
                    if f["field"].split(".")[-1].lower().startswith("check")]
    lines += ["", "**Check reference (grant -> check join)**", ""]
    lines += _table(FIELD_HEADERS, _field_rows(check_fields)) if check_fields \
        else ["_No `check_id` / `check_num` on a grant record — grant -> check "
              "cannot be joined from `grant/list` alone._"]

    grantee_names = [f["field"] for f in grantee]
    answer_bits = []
    answer_bits.append("grantee fields: {}".format(
        ", ".join("`%s`" % n for n in grantee_names) or "none found"))
    answer_bits.append("date: {}".format(
        ", ".join("`%s`" % f["field"] for f in dates) or "none"))
    answer_bits.append("status: {}".format(
        ", ".join("`%s`" % f["field"] for f in status) or "none"))
    answer_bits.append("check join: {}".format(
        ", ".join("`%s`" % f["field"] for f in check_fields)
        or "NOT possible from grant/list (no check_id/check_num)"))
    return {"id": "C2", "title": "Grantee on grants", "lines": lines,
            "answer": "; ".join(answer_bits)}


def discover_c3(probe, csuite_extras):
    """Registrant shape: profiles[] and guests[] on event dates."""
    idx = _by_endpoint(probe)
    endpoints = [e for e in idx if e.startswith("event/display/eventdate")]
    usable = [e for e in endpoints if idx[e].get("ok")]
    lines = []
    if not usable:
        reason = (_unavailable(idx.get(endpoints[0]) if endpoints else None,
                               "event/display/eventdate")
                  or "no event display call succeeded")
        return {"id": "C3", "title": "Registrant shape", "lines": [reason],
                "answer": "undetermined: {}".format(reason)}

    payloads = [probe.raw_payloads.get(e) for e in usable]
    lines.append("Merged over {} event(s) with a non-null `event_date`: {}."
                 .format(len(usable), ", ".join("`%s`" % e for e in usable)))

    # event/display/eventdate returns two different shapes. Ticket-style event
    # dates answer with ticket/fund detail; registrant-style ones answer with
    # the registrants themselves as the top-level rows. Treat the latter as
    # the profiles[] equivalent rather than reporting "no registrants".
    registrant_rows = []
    for payload in payloads:
        registrant_rows.extend(
            r for r in find_records(payload)[0] if _is_registrant_row(r))

    found_any = False
    rsvp_summary = []

    lines += ["", "**Top-level registrant rows** (the `profiles[]` equivalent)",
              ""]
    if registrant_rows:
        found_any = True
        top_stats = field_stats(registrant_rows)
        lines.append("{} registrant row(s). `event/display/eventdate` returns "
                     "these as its `results` array — there is no nested "
                     "`profiles[]` key.".format(len(registrant_rows)))
        lines.append("")
        lines += _table(FIELD_HEADERS, _field_rows(top_stats))
        for token in ("rsvp", "attended"):
            for f in [x for x in top_stats
                      if token in x["field"].split(".")[-1].lower()]:
                counter = Counter()
                for row in registrant_rows:
                    flat = flatten(row)
                    if f["field"] in flat:
                        counter[mask_value(f["field"], flat[f["field"]])] += 1
                values = ", ".join("{} ({})".format(v, c)
                                   for v, c in counter.most_common(10))
                lines.append("")
                lines.append("- `{}` — type `{}`, {}% populated, values: {}"
                             .format(f["field"], f["type"],
                                     f["pct_populated"], values or "—"))
                rsvp_summary.append("{} ({}, values {})".format(
                    f["field"], f["type"], values or "none"))
    else:
        lines.append("_No sampled event date returned registrant rows._")

    for key in ("profiles", "guests"):
        stats = _nested_stats(payloads, key)
        lines += ["", "**`{}[]`**".format(key), ""]
        if not stats["fields"]:
            lines.append("_Not present on any sampled event, or empty._")
            continue
        found_any = True
        lines.append("{} row(s) across {} event(s) that carried the array."
                     .format(stats["row_count"], stats["present_on"]))
        lines.append("")
        lines += _table(FIELD_HEADERS, _field_rows(stats["fields"]))

        for token in ("rsvp", "attended"):
            hits = [f for f in stats["fields"]
                    if token in f["field"].split(".")[-1].lower()]
            for f in hits:
                counter = Counter()
                for row in stats["rows"]:
                    flat = flatten(row)
                    if f["field"] in flat:
                        counter[mask_value(f["field"], flat[f["field"]])] += 1
                values = ", ".join("{} ({})".format(v, c)
                                   for v, c in counter.most_common(10))
                lines.append("")
                lines.append("- `{}.{}` — type `{}`, {}% populated, values: {}"
                             .format(key, f["field"], f["type"],
                                     f["pct_populated"], values or "—"))
                rsvp_summary.append("{}.{} ({}, values {})".format(
                    key, f["field"], f["type"], values or "none"))

    if not found_any:
        answer = ("no registrant rows and no `profiles[]`/`guests[]` array on "
                  "any sampled event date")
    else:
        answer = "{} registrant row(s), {} guest row(s); rsvp/attended: {}".format(
            len(registrant_rows),
            _nested_stats(payloads, "guests")["row_count"],
            "; ".join(rsvp_summary) or "none found")
    return {"id": "C3", "title": "Registrant shape", "lines": lines,
            "answer": answer}


def discover_c4(probe):  # noqa: C901 - one section, read top to bottom
    """Profile identity fields and write-back conflict detection."""
    idx = _by_endpoint(probe)
    record = idx.get("profile/list")
    blocked = _unavailable(record, "profile/list")
    if blocked:
        return {"id": "C4", "title": "Profile identity fields",
                "lines": [blocked], "answer": "undetermined: {}".format(blocked)}

    payload = probe.raw_payloads.get("profile/list")
    identity = _pick(record, C4_IDENTITY_TOKENS)
    lines = ["`profile/list` returned {} fields; {} are identity-shaped."
             .format(record["field_count"], len(identity)), ""]
    lines += _table(FIELD_HEADERS, _field_rows(identity))

    names = [f["field"].split(".")[-1].lower() for f in _fields_of(record)]
    has_split = any(n in ("first_name", "firstname") for n in names) and \
        any(n in ("last_name", "lastname") for n in names)
    has_single = "name" in names
    name_shape = ("split into first/last" if has_split and not has_single else
                  "both a combined `name` and first/last parts" if has_split
                  else "a single combined `name` field" if has_single
                  else "no obvious name field")

    # "address" can sit anywhere in the leaf: profile/list returns
    # `primary_address_string`, profile/display returns `primary_city` etc.
    addr_parts = [n for n in names
                  if "address" in n or "addr" in n
                  or any(n.endswith(part) or n == part for part in
                         ("city", "state", "zip", "zipcode", "postal_code",
                          "country", "citystatezip"))]
    single_string = [n for n in addr_parts if n.endswith("_string")]
    structured = [n for n in addr_parts if not n.endswith("_string")]
    if single_string and not structured:
        addr_shape = "one flattened string ({})".format(
            ", ".join("`%s`" % a for a in sorted(single_string)))
    elif structured and single_string:
        addr_shape = ("both — a flattened {} and {} structured part(s) ({})"
                      .format(", ".join("`%s`" % a for a in sorted(single_string)),
                              len(structured),
                              ", ".join("`%s`" % a for a in sorted(structured))))
    elif structured:
        addr_shape = "structured into {} parts ({})".format(
            len(structured), ", ".join("`%s`" % a for a in sorted(structured)))
    else:
        addr_shape = "not returned"

    lines += ["", "- Name shape: {}".format(name_shape),
              "- Address shape: {}".format(addr_shape)]

    dead_fields = [f for f in _fields_of(record)
                   if f["field"].split(".")[-1].lower() in ("dead", "deceased")]
    if dead_fields:
        for f in dead_fields:
            distinct = _distinct_values(payload, f["field"])
            lines.append("- `{}` distinct values: {}".format(
                f["field"],
                ", ".join("{} ({})".format(_cell(d["value"]), d["count"])
                          for d in distinct) or "—"))
    else:
        lines.append("- No `dead` / `deceased` field on profile/list.")

    modified = [f for f in _fields_of(record)
                if any(tok in f["field"].split(".")[-1].lower()
                       for tok in ("modified", "updated", "changed", "edited"))]
    if modified:
        lines.append("- Last-modified candidates: {}".format(
            ", ".join("`{}` ({})".format(f["field"], f["type"])
                      for f in modified)))
    else:
        lines.append("- No last-modified timestamp on `profile/list`.")

    # profile/list is the sync's workhorse, but profile/display may still carry
    # a modified timestamp — that changes the write-back answer entirely.
    display = _by_endpoint(probe).get("profile/display")
    display_modified = []
    if display and display.get("ok"):
        display_modified = [
            f for f in _fields_of(display)
            if any(tok in f["field"].split(".")[-1].lower()
                   for tok in ("modified", "updated", "changed", "edited"))]
        lines.append("- On `profile/display`: {}".format(
            ", ".join("`{}` ({}, {}% populated)".format(
                f["field"], f["type"], f["pct_populated"])
                for f in display_modified)
            or "no last-modified field either"))

    if modified:
        modified_answer = ", ".join("`%s`" % f["field"] for f in modified)             + " on profile/list"
    elif display_modified:
        modified_answer = ("absent from profile/list, but "
                           + ", ".join("`%s`" % f["field"] for f in display_modified)
                           + " exists on profile/display")
    else:
        modified_answer = "ABSENT from both profile/list and profile/display"

    answer = ("name is {}; address is {}; last-modified: {}".format(
        name_shape, addr_shape, modified_answer))
    return {"id": "C4", "title": "Profile identity fields", "lines": lines,
            "answer": answer}


_WRITE_SEGMENTS = ("create", "edit", "delete", "update", "complete", "remove",
                   "add", "post", "void")


def discover_c5():
    """Static inventory of every endpoint string in clients/csuite.py.

    Deliberately reads the source; it makes no call of any kind.
    """
    path = os.path.join(REPO_ROOT, "clients", "csuite.py")
    lines = []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            source = handle.read()
    except OSError as exc:
        reason = "could not read clients/csuite.py: {}".format(exc)
        return {"id": "C5", "title": "Write-back endpoint inventory",
                "lines": [reason], "answer": "undetermined: {}".format(reason)}

    found = {}
    for match in re.finditer(r'_request\(\s*["\']([^"\']+)["\']', source):
        endpoint = match.group(1)
        line_no = source[:match.start()].count("\n") + 1
        found.setdefault(endpoint, line_no)

    reads, writes = [], []
    for endpoint, line_no in sorted(found.items()):
        segments = endpoint.lower().split("/")
        (writes if any(seg in _WRITE_SEGMENTS for seg in segments)
         else reads).append((endpoint, line_no))

    lines.append("Static grep of `clients/csuite.py` — {} distinct endpoint "
                 "strings. No call was made.".format(len(found)))
    lines += ["", "**Read endpoints ({})**".format(len(reads)), ""]
    lines += _table(["Endpoint", "clients/csuite.py"],
                    [("`%s`" % e, "L%d" % n) for e, n in reads])
    lines += ["", "**Write endpoints ({}) — none of these were called**".format(
        len(writes)), ""]
    lines += _table(["Endpoint", "clients/csuite.py"],
                    [("`%s`" % e, "L%d" % n) for e, n in writes])

    # Is CSuite API documentation vendored anywhere in the repo?
    doc_hits = []
    for root, dirs, files in os.walk(REPO_ROOT):
        dirs[:] = [d for d in dirs
                   if d not in (".git", ".venv", "venv", "__pycache__",
                                "node_modules", ".pytest_cache", "probe_output")]
        for filename in files:
            if not filename.lower().endswith((".md", ".txt", ".json", ".yaml",
                                              ".yml", ".pdf", ".html")):
                continue
            rel = os.path.relpath(os.path.join(root, filename), REPO_ROOT)
            lowered = filename.lower()
            if any(tok in lowered for tok in ("csuite", "fcsuite", "api-doc",
                                              "apidoc", "openapi", "swagger")):
                doc_hits.append(rel)

    profile_edit = "profile/edit" in found
    lines += ["", "**Documentation for the profile update endpoint**", ""]
    if doc_hits:
        lines.append("Possible vendored docs: {}".format(
            ", ".join("`%s`" % h for h in doc_hits)))
    else:
        lines.append("No CSuite API documentation is vendored or linked in the "
                     "repo — no `docs/`, no OpenAPI/Swagger file, and `README.md` "
                     "only lists the credential env vars.")
        lines.append("")
        lines.append("The client does define the write endpoint: "
                     "`profile/edit` at `clients/csuite.py:290`, called by "
                     "`edit_profile(profile_id, **kwargs)`. Its accepted field "
                     "list is not documented anywhere in the repo — the "
                     "docstring only gives `primary_email` and "
                     "`primary_phone_number` as examples.")
        lines.append("")
        lines.append("**not in repo — ask Shazeen for the profile update "
                     "endpoint doc.** Stopping here rather than discovering the "
                     "accepted fields by probing, which would mean issuing "
                     "writes.")

    if doc_hits:
        answer = ("profile update endpoint is `profile/edit` "
                  "(clients/csuite.py:290); candidate docs in repo: {}".format(
                      ", ".join(doc_hits)))
    elif profile_edit:
        answer = ("write endpoint is `profile/edit` (clients/csuite.py:290) via "
                  "edit_profile(**kwargs); accepted fields not in repo — ask "
                  "Shazeen for the profile update endpoint doc")
    else:
        answer = ("no profile update endpoint in the client; not in repo — ask "
                  "Shazeen for the profile update endpoint doc")
    return {"id": "C5", "title": "Write-back endpoint inventory", "lines": lines,
            "answer": answer}


def discover_c6(probe):
    """Donation roll-up fields and giving-total exclusion flags."""
    idx = _by_endpoint(probe)
    record = idx.get("donation/list")
    blocked = _unavailable(record, "donation/list")
    if blocked:
        return {"id": "C6", "title": "Donation roll-up fields",
                "lines": [blocked], "answer": "undetermined: {}".format(blocked)}

    payload = probe.raw_payloads.get("donation/list")
    lines = ["`donation/list` returned {} fields over {} record(s)."
             .format(record["field_count"], record["record_count"]), ""]
    lines += _table(FIELD_HEADERS, _field_rows(_fields_of(record)))

    def first_match(tokens):
        """Exact leaf name wins over a substring hit.

        `fund_name_link_id` contains "fund_name" but is a link id, not the
        fund's name — matching it as the name role was wrong.
        """
        for token in tokens:
            for f in _fields_of(record):
                if f["field"].split(".")[-1].lower() == token:
                    return f["field"]
        hits = _pick(record, tokens)
        return hits[0]["field"] if hits else None

    roles = {
        "amount": first_match(("amount",)),
        "date": first_match(("date",)),
        "fund id": first_match(("funit_id", "fund_id")),
        "fund name": first_match(("fund_name",)),
        "profile id": first_match(("profile_id",)),
    }
    lines += ["", "**Roll-up roles**", ""]
    rows = []
    for role, field in roles.items():
        example = next((f["example"] for f in _fields_of(record)
                        if f["field"] == field), None)
        rows.append((role, "`%s`" % field if field else "**not found**",
                     _cell(example)))
    lines += _table(["Role", "Field", "Example (masked)"], rows)

    exclusion = _pick(record, C6_EXCLUSION_TOKENS)
    lines += ["", "**Flags that should exclude a row from giving totals**", ""]
    if exclusion:
        rows = []
        for f in exclusion:
            distinct = _distinct_values(payload, f["field"])
            rows.append((("`%s`" % f["field"]), ("`%s`" % f["type"]),
                         "{}%".format(f["pct_populated"]),
                         ", ".join("{} ({})".format(_cell(d["value"]), d["count"])
                                   for d in distinct) or "—"))
        lines += _table(["Field", "Type", "% populated", "Distinct values"], rows)
    else:
        lines.append("_No type / soft-credit / anonymous / refund flag on "
                     "donation/list._")

    answer = ("amount={}, date={}, fund={}, profile={}; exclusion flags: {}"
              .format(roles["amount"] or "NOT FOUND",
                      roles["date"] or "NOT FOUND",
                      roles["fund id"] or roles["fund name"] or "NOT FOUND",
                      roles["profile id"] or "NOT FOUND",
                      ", ".join("`%s`" % f["field"] for f in exclusion)
                      or "none found"))
    return {"id": "C6", "title": "Donation roll-up fields", "lines": lines,
            "answer": answer}


# ---------------------------------------------------------------------------
# HubSpot discovery
# ---------------------------------------------------------------------------

def discover_h1(probe, extras):
    idx = _by_endpoint(probe)
    schema_ok = extras.get("contact_properties_ok")
    sample = idx.get("crm/v3/objects/contacts (H1 properties)")
    props = {p["name"]: p for p in (extras.get("contact_properties") or [])}
    sample_fields = {f["field"].split(".")[-1]: f for f in _fields_of(sample)}

    lines = []
    if not schema_ok:
        lines.append("Property schema call did not succeed — existence is "
                     "undetermined for every row below.")
    if sample and not sample.get("ok"):
        lines.append("The 100-contact sample did not succeed — "
                     "% populated is undetermined.")
    lines.append("")

    rows = []
    for name in H1_PROPERTIES:
        detail = props.get(name)
        exists = None if not schema_ok else (name in props)
        stat = sample_fields.get(name)
        rows.append((
            "`%s`" % name,
            _tri(exists),
            _cell(detail.get("type") if detail else None),
            _cell(detail.get("fieldType") if detail else None),
            _cell(detail.get("groupName") if detail else None),
            "{}%".format(stat["pct_populated"]) if stat else "undetermined",
            _cell(stat["example"]) if stat else "—",
        ))
    lines += _table(["Property", "Exists", "Type", "Field type", "Group",
                     "% populated", "Example (masked)"], rows)

    if not schema_ok:
        answer = ("undetermined: crm/v3/properties/contacts did not return "
                  "(all 17 properties unresolved)")
    else:
        present = [n for n in H1_PROPERTIES if n in props]
        missing = [n for n in H1_PROPERTIES if n not in props]
        answer = "{}/{} exist ({}); absent: {}".format(
            len(present), len(H1_PROPERTIES),
            ", ".join(present) or "none", ", ".join(missing) or "none")
    return {"id": "H1", "title": "Gift-summary and sync properties",
            "lines": lines, "answer": answer}


def discover_h2(extras):
    props = extras.get("contact_properties") or []
    ok = extras.get("contact_properties_ok")
    if not ok:
        reason = "crm/v3/properties/contacts did not return"
        return {"id": "H2", "title": "Candidate-property keyword search",
                "lines": [reason], "answer": "undetermined: {}".format(reason)}

    hits = []
    for prop in props:
        haystack = "{} {}".format(prop.get("name") or "",
                                  prop.get("label") or "").lower()
        matched = [kw for kw in H2_KEYWORDS if kw in haystack]
        if matched:
            hits.append((prop, matched))

    lines = ["Searched {} contact properties (name + label) for {}.".format(
        len(props), ", ".join("`%s`" % k for k in H2_KEYWORDS)), ""]
    if hits:
        lines += _table(
            ["Property", "Label", "Type", "Group", "Origin", "Matched"],
            [("`%s`" % p["name"], _cell(p["label"]), "`%s`" % p["type"],
              "`%s`" % p["groupName"], p["origin"], ", ".join(m))
             for p, m in hits])
    else:
        lines.append("_No property name or label matches any keyword._")

    answer = "{} matching properties: {}".format(
        len(hits), ", ".join(p["name"] for p, _ in hits) or "none")
    return {"id": "H2", "title": "Candidate-property keyword search",
            "lines": lines, "answer": answer}


def discover_h3(extras):
    endpoints = extras.get("h3_endpoints") or {}
    external_id = extras.get("h3_external_event_id")
    lines = ["First synced event externalEventId: {}".format(
        "`%s`" % external_id if external_id
        else "**none found** (no marketing event with a `csuite-` prefix)"), ""]
    if endpoints:
        lines += _table(["Endpoint", "HTTP", "Supported", "subscriberState values"],
                        [("`%s`" % ep,
                          _cell(info.get("http_status")),
                          _tri(info.get("supported")),
                          ", ".join("`%s`" % v
                                    for v in (info.get("subscriber_states") or []))
                          or "—")
                         for ep, info in endpoints.items()])
    else:
        lines.append("_No participation endpoint was reached._")

    supported = [ep for ep, i in endpoints.items() if i.get("supported") is True]
    unsupported = [ep for ep, i in endpoints.items() if i.get("supported") is False]
    unknown = [ep for ep, i in endpoints.items() if i.get("supported") is None]
    if supported or unsupported:
        answer = "supported: {}; not supported: {}".format(
            ", ".join(supported) or "none", ", ".join(unsupported) or "none")
        if unknown:
            answer += "; undetermined: {}".format(", ".join(unknown))
    else:
        answer = ("undetermined: no participation endpoint returned "
                  "(all {} unresolved)".format(len(endpoints) or 3))
    return {"id": "H3", "title": "Marketing event participants",
            "lines": lines, "answer": answer}


def discover_h4(probe, extras):
    idx = _by_endpoint(probe)
    lists = extras.get("lists") or []
    get_record = idx.get("crm/v3/lists")
    search_record = idx.get("crm/v3/lists/search")
    lines = []
    for label, record in (("GET crm/v3/lists", get_record),
                          ("POST crm/v3/lists/search", search_record)):
        if record is None:
            continue
        lines.append("- {} — {} — raw top-level keys: {} — container: {}".format(
            label, _status_cell(record),
            ", ".join("`%s`" % k for k in record.get("raw_top_level_keys") or [])
            or "—",
            _cell(record.get("records_container"))))
    lines.append("")

    if not lists:
        reason = (_unavailable(search_record or get_record, "list enumeration")
                  or "no lists returned")
        lines.append("_No lists returned._")
        return {"id": "H4", "title": "Lists", "lines": lines,
                "answer": "undetermined: {}".format(reason)}

    event_lists = [l for l in lists if str(l.get("name") or "").startswith("Event:")]
    lines += _table(["List id", "Name", "processingType", "Size", "Event list?"],
                    [(_cell(l["listId"]), _cell(l["name"]),
                      _cell(l["processingType"]), _cell(l["size"]),
                      "**yes**" if l in event_lists else "")
                     for l in lists])
    answer = "{} lists; {} named 'Event:' (created by intents/events.py)".format(
        len(lists), len(event_lists))
    return {"id": "H4", "title": "Lists", "lines": lines, "answer": answer}


def discover_h5(extras):
    props = {p["name"]: p for p in (extras.get("contact_properties") or [])}
    ok = extras.get("contact_properties_ok")
    if not ok:
        reason = "crm/v3/properties/contacts did not return"
        return {"id": "H5", "title": "Contact identity properties",
                "lines": [reason], "answer": "undetermined: {}".format(reason)}

    rows = []
    for name in H5_IDENTITY_PROPERTIES:
        prop = props.get(name)
        rows.append(("`%s`" % name, _tri(name in props),
                     _cell(prop.get("type") if prop else None),
                     _cell(prop.get("fieldType") if prop else None),
                     _cell(prop.get("groupName") if prop else None)))
    lines = _table(["Property", "Exists", "Type", "Field type", "Group"], rows)

    address_like = sorted(
        p["name"] for p in props.values()
        if "address" in "{} {}".format(p["name"], p.get("label") or "").lower())
    lines += ["", "**Address-shaped properties in this portal**", "",
              ", ".join("`%s`" % a for a in address_like) or "_none_"]
    has_address2 = "address2" in props
    custom_address = [p["name"] for p in props.values()
                      if p["origin"] == "custom" and "address" in p["name"].lower()]
    lines += ["", "- `address2`: {}".format(_tri(has_address2)),
              "- Custom address properties: {}".format(
                  ", ".join("`%s`" % c for c in custom_address) or "none")]

    missing = [n for n in H5_IDENTITY_PROPERTIES if n not in props]
    answer = ("{}/{} identity properties exist; address2: {}; custom address "
              "properties: {}".format(
                  len(H5_IDENTITY_PROPERTIES) - len(missing),
                  len(H5_IDENTITY_PROPERTIES),
                  "yes" if has_address2 else "no",
                  ", ".join(custom_address) or "none"))
    return {"id": "H5", "title": "Contact identity properties",
            "lines": lines, "answer": answer}


def build_discovery(probe, csuite_extras, hubspot_extras):
    return [
        discover_c1(probe),
        discover_c2(probe),
        discover_c3(probe, csuite_extras),
        discover_c4(probe),
        discover_c5(),
        discover_c6(probe),
        discover_h1(probe, hubspot_extras),
        discover_h2(hubspot_extras),
        discover_h3(hubspot_extras),
        discover_h4(probe, hubspot_extras),
        discover_h5(hubspot_extras),
    ]


def write_mapping_discovery(sections, path):
    lines = [
        "# Mapping discovery",
        "",
        "Generated by `scripts/probe_apis.py`. Overwritten on every run.",
        "",
        "Generated: {}".format(datetime.now().isoformat(timespec="seconds")),
        "",
        "One section per discovery item, each ending in a single ANSWER line. "
        "An answer beginning `undetermined:` means the call that would have "
        "settled it did not succeed — it is not a finding of absence.",
        "",
        "All values masked per probe #1 rules.",
        "",
        "## Answers at a glance",
        "",
    ]
    lines += _table(["Item", "Answer"],
                    [("**{}**".format(s["id"]),
                      s["answer"].replace("|", "\\|")) for s in sections])
    lines.append("")
    for section in sections:
        lines += ["---", "",
                  "## {} — {}".format(section["id"], section["title"]), ""]
        lines += section["lines"]
        lines += ["",
                  "**ANSWER ({}):** {}".format(section["id"], section["answer"]),
                  ""]
    _write(path, "\n".join(lines))


# =============================================================================
# SUMMARY SCREEN
# =============================================================================

def print_summary(results, csuite_extras, hubspot_extras, out_dir,
                  discovery=None):
    width = 78
    print("")
    print("=" * width)
    print("API PROBE SUMMARY  ({})".format(
        datetime.now().isoformat(timespec="seconds")))
    print("=" * width)
    print("{:<8} {:<44} {:>6} {:>6} {:>6}".format(
        "SYSTEM", "ENDPOINT", "STATUS", "COUNT", "FIELDS"))
    print("-" * width)
    for r in results:
        endpoint = r["endpoint"]
        if len(endpoint) > 43:
            endpoint = endpoint[:40] + "..."
        print("{:<8} {:<44} {:>6} {:>6} {:>6}".format(
            r["system"], endpoint, _status_cell(r),
            r["record_count"], r["field_count"]))
    print("-" * width)

    ok = sum(1 for r in results if r["ok"])
    failed = [r for r in results if not r["ok"] and not r.get("skipped")]
    skipped = [r for r in results if r.get("skipped")]
    print("{} endpoints — {} ok, {} failed, {} skipped".format(
        len(results), ok, len(failed), len(skipped)))

    totals = csuite_extras.get("profile_total") or {}
    print("profile/list reported total: {}".format(
        ", ".join("{}={}".format(k, v) for k, v in totals.items())
        or "not reported by the API"))

    newsletter = csuite_extras.get("newsletter") or {}
    if newsletter.get("exists") is True:
        for field, detail in (newsletter.get("fields") or {}).items():
            values = ", ".join(
                str(d["value"]) for d in detail["distinct_values"][:6])
            print("newsletter: '{}' type={} populated={}% values=[{}]".format(
                field, detail["type"], detail["pct_populated"], values))
    elif newsletter.get("exists") is False:
        print("newsletter: NOT PRESENT on profile/list records")
    else:
        print("newsletter: undetermined ({})".format(
            newsletter.get("reason", "no data")))

    checks = hubspot_extras.get("mapped_property_check") or {}
    if checks:
        present = [n for n, c in checks.items() if c["exists"] is True]
        missing = [n for n, c in checks.items() if c["exists"] is False]
        unknown = [n for n, c in checks.items() if c["exists"] is None]
        print("hubspot props present: {}".format(", ".join(present) or "none"))
        print("hubspot props MISSING: {}".format(", ".join(missing) or "none"))
        if unknown:
            print("hubspot props undetermined: {}".format(", ".join(unknown)))

    clones = hubspot_extras.get("email_clone_sources") or {}
    for email_id, info in clones.items():
        print("clone source {}: HTTP {} -> {}".format(
            email_id, info["http_status"] if info["http_status"] is not None
            else "no call",
            _tri(info["resolved"], "resolved", "NOT resolved", "undetermined")))

    if failed:
        print("-" * width)
        print("FAILURES")
        for r in failed:
            print("  {} {} -> {}".format(
                r["system"], r["endpoint"], str(r["error"])[:100]))
    if skipped:
        print("SKIPPED")
        for r in skipped:
            print("  {} {} -> {}".format(
                r["system"], r["endpoint"], str(r["error"])[:100]))

    if discovery:
        print("-" * width)
        print("DISCOVERY ANSWERS")
        for section in discovery:
            answer = section["answer"]
            print("  {}: {}".format(section["id"], answer[:200]))

    print("=" * width)
    print("Output: {}".format(out_dir))
    print("=" * width)


# =============================================================================
# MAIN
# =============================================================================

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Read-only field inventory for CSuite and HubSpot.")
    parser.add_argument("--system", choices=["csuite", "hubspot", "all"],
                        default="all")
    parser.add_argument("--limit", type=int, default=5,
                        help="Records per sample request (default 5).")
    parser.add_argument("--out", default=os.path.join("scripts", "probe_output"),
                        help="Output directory (default scripts/probe_output).")
    args = parser.parse_args(argv)

    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)

    recorder = HttpRecorder()
    probe = Probe(recorder)
    csuite_extras = {}
    hubspot_extras = {}

    if args.system in ("csuite", "all"):
        try:
            csuite_extras = probe_csuite(args.limit, recorder, probe) or {}
        except Exception as exc:  # a broken client must not lose the rest
            csuite_extras = {"fatal": "{}: {}".format(type(exc).__name__, exc)}
            probe.results.append(_skipped_record(
                "csuite", "<all>", "probe aborted: {}".format(exc)))

    if args.system in ("hubspot", "all"):
        try:
            hubspot_extras = probe_hubspot(args.limit, recorder, probe) or {}
        except Exception as exc:
            hubspot_extras = {"fatal": "{}: {}".format(type(exc).__name__, exc)}
            probe.results.append(_skipped_record(
                "hubspot", "<all>", "probe aborted: {}".format(exc)))

    csuite_results = [r for r in probe.results if r["system"] == "csuite"]
    hubspot_results = [r for r in probe.results if r["system"] == "hubspot"]

    discovery = build_discovery(probe, csuite_extras, hubspot_extras)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "args": {"system": args.system, "limit": args.limit, "out": out_dir},
        "masking": {
            "emails": "first char + '*@' + domain",
            "names_addresses_phones": REDACTED,
            "money_fields": "order of magnitude only",
            "money_field_tokens": list(_MONEY_TOKENS),
            "ids": "kept verbatim",
            "dates": "kept verbatim",
            "free_text": "replaced with '<text len=N>'",
        },
        "endpoints": probe.results,
        "csuite_extras": csuite_extras,
        "hubspot_extras": hubspot_extras,
        "discovery": [
            {"id": d["id"], "title": d["title"], "answer": d["answer"]}
            for d in discovery
        ],
    }

    json_path = os.path.join(out_dir, "_probe.json")
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str, sort_keys=False)
        handle.write("\n")

    write_csuite_fields(csuite_results, csuite_extras,
                        os.path.join(out_dir, "csuite_fields.md"))
    write_hubspot_properties(hubspot_results, hubspot_extras,
                             os.path.join(out_dir, "hubspot_properties.md"))
    write_mapping_draft(csuite_results, hubspot_extras, discovery,
                        os.path.join(out_dir, "mapping_draft.md"))
    write_mapping_discovery(discovery,
                            os.path.join(out_dir, "mapping_discovery.md"))

    print_summary(probe.results, csuite_extras, hubspot_extras, out_dir,
                  discovery)
    return 0


if __name__ == "__main__":
    sys.exit(main())
