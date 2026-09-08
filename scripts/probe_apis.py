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

_EMAIL_TOKENS = ("email", "e_mail", "mail_address")

_PII_TOKENS = (
    "name", "address", "addr", "street", "city", "state", "zip", "postal",
    "province", "country", "phone", "mobile", "fax", "salutation", "prefix",
    "suffix", "title", "household", "organization", "organisation", "company",
    "employer", "spouse", "contact", "recipient", "payee", "signer", "owner",
    "attention", "attn", "website", "url", "domain",
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
      6. money-ish name  -> order of magnitude only ("$1k-10k")
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

    if _has_token(name, _MONEY_TOKENS):
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

    for key in ("results", "records", "items", "objects", "rows", "inputs"):
        value = payload.get(key)
        if isinstance(value, list):
            return [r for r in value if isinstance(r, dict)], key

    data = payload.get("data")
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)], "data"
    if isinstance(data, dict):
        nested, path = find_records(data)
        if nested:
            return nested, "data.{}".format(path)
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
        self.results.append(record)
        return record


# =============================================================================
# CSUITE
# =============================================================================

# Fund search uses a deliberately generic literal so no person's name is sent
# to the API or written into the receipt.
FUND_SEARCH_QUERY = "fund"

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
    fund_id = _first_id(funds_payload, "funit_id", "fund_id", "id")

    probe.run(
        "csuite", "funit/list/search",
        lambda: client.search_funds(FUND_SEARCH_QUERY),
        {"q": FUND_SEARCH_QUERY},
        post_transport=True,
        note=post_note + " Query is a fixed generic word, not a real name.",
    )

    if fund_id is not None:
        probe.run(
            "csuite", "funit/display",
            lambda: client.get_fund(fund_id),
            {"funit_id": fund_id},
            post_transport=True,
            note=post_note + " Fund id taken from the first funit/list record.",
        )
    else:
        probe.results.append(_skipped_record(
            "csuite", "funit/display",
            "no fund id available — funit/list returned no records"))

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

    if event_date_id is not None:
        probe.run(
            "csuite", "event/display/eventdate",
            lambda: client.get_event_date(event_date_id),
            {"event_date_id": event_date_id},
            post_transport=True,
            note=post_note + " Event id from the first event/list/dates record.",
        )
    else:
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
    newsletter = _analyse_newsletter(profiles_payload)

    return {
        "profile_total": full.get("reported_totals", {}),
        "profile_full_page_count": full.get("record_count", 0),
        "newsletter": newsletter,
        "first_fund_id": fund_id,
        "first_event_date_id": event_date_id,
    }


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


def _analyse_newsletter(payload):
    """Does profile/list carry a `newsletter` field, and what does it hold?"""
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
    probe.run(
        "hubspot", "crm/v3/properties/contacts",
        lambda: client._get("crm/v3/properties/contacts"),
        {},
        note="Full contact property schema.",
    )
    contact_props = _property_catalog(recorder.raw)
    extras["contact_properties"] = contact_props

    contact_names = {p["name"] for p in contact_props}
    extras["mapped_property_check"] = {
        name: {
            "exists": name in contact_names,
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
            "resolved": bool(record["http_status"] == 200),
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
        lines.append("Could not determine — {}.".format(
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
            name,
            "**yes**" if check.get("exists") else "**NO**",
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
            _cell(info["http_status"]),
            "**yes**" if info["resolved"] else "**NO**",
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


def write_mapping_draft(csuite_results, hubspot_extras, path):
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
# SUMMARY SCREEN
# =============================================================================

def print_summary(results, csuite_extras, hubspot_extras, out_dir):
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
        present = [n for n, c in checks.items() if c["exists"]]
        missing = [n for n, c in checks.items() if not c["exists"]]
        print("hubspot props present: {}".format(", ".join(present) or "none"))
        print("hubspot props MISSING: {}".format(", ".join(missing) or "none"))

    clones = hubspot_extras.get("email_clone_sources") or {}
    for email_id, info in clones.items():
        print("clone source {}: HTTP {} -> {}".format(
            email_id, info["http_status"],
            "resolved" if info["resolved"] else "NOT resolved"))

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
    }

    json_path = os.path.join(out_dir, "_probe.json")
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str, sort_keys=False)
        handle.write("\n")

    write_csuite_fields(csuite_results, csuite_extras,
                        os.path.join(out_dir, "csuite_fields.md"))
    write_hubspot_properties(hubspot_results, hubspot_extras,
                             os.path.join(out_dir, "hubspot_properties.md"))
    write_mapping_draft(csuite_results, hubspot_extras,
                        os.path.join(out_dir, "mapping_draft.md"))

    print_summary(probe.results, csuite_extras, hubspot_extras, out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
