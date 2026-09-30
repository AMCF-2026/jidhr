"""
Filter trust
============
Proving that a CSuite list filter actually filters, before any decision
is made on what it returned.

Why this exists
---------------
On 2026-09-30, `profile/list` was called with `q="HUBSYNC SENTINEL"`. It
answered **18,797** — the exact total number of profiles — and the rows
it returned were unrelated people. **CSuite does not reject an
unrecognised filter parameter. It ignores it and returns the whole
table.**

That is a sharp edge under search-before-create, which is the only
duplicate protection CSuite offers: it has no idempotency key. A mistyped
or unsupported filter returns everything, so a duplicate check either
always blocks or never blocks depending on how the caller reads it, and
neither failure announces itself.

So a filter is not trusted because it is documented, or because it
worked last week. It is trusted when, right now, on this endpoint:

  * the unfiltered total is greater than zero — otherwise an empty table
    makes any filter look like it works; and
  * the same filter with a value known to match nothing returns exactly
    zero — the one answer an ignored filter cannot give.

Both conditions, every time, before the filter is used to decide whether
a record already exists.
"""

import logging
import uuid

logger = logging.getLogger(__name__)

TRUSTED = "trusted"
FILTER_IGNORED = "filter_ignored"
EMPTY_TABLE = "empty_table"
ERROR = "error"


def absent_value(field: str) -> str:
    """A value for `field` that cannot match an existing record.

    Random, not a fixed string: a hard-coded probe value is one
    coincidence away from matching something real, and the whole check
    rests on the probe matching nothing.
    """
    token = uuid.uuid4().hex
    if "email" in field.lower():
        return f"filter-probe-{token}@example.invalid"
    return f"FILTER-PROBE-{token}"


class FilterTrust:
    """The verdict on one filter, with the numbers that produced it."""

    __slots__ = ("field", "verdict", "unfiltered", "probe_count", "detail")

    def __init__(self, field, verdict, unfiltered=None, probe_count=None,
                 detail=None):
        self.field = field
        self.verdict = verdict
        self.unfiltered = unfiltered
        self.probe_count = probe_count
        self.detail = detail

    @property
    def trusted(self) -> bool:
        return self.verdict == TRUSTED

    def __repr__(self):
        return (f"FilterTrust({self.field!r}, {self.verdict!r}, "
                f"unfiltered={self.unfiltered}, probe={self.probe_count})")

    def why(self) -> str:
        if self.verdict == TRUSTED:
            return (f"{self.field!r} filters: {self.unfiltered:,} unfiltered, "
                    f"0 for a value that matches nothing")
        if self.verdict == FILTER_IGNORED:
            if self.detail:
                return (f"{self.field!r} is NOT trusted: {self.detail}. It "
                        "must not be used for a duplicate decision.")
            return (f"{self.field!r} is IGNORED by CSuite: a value that "
                    f"matches nothing still returned {self.probe_count:,}, "
                    f"the same as the unfiltered total. It must not be used "
                    f"for a duplicate decision.")
        if self.verdict == EMPTY_TABLE:
            return (f"the unfiltered total is {self.unfiltered}, so nothing "
                    f"can be concluded about {self.field!r} — an empty table "
                    f"makes every filter look like it works")
        return f"{self.field!r} could not be checked: {self.detail}"


def _count(response):
    """The count from a CSuite list response, or None."""
    if not isinstance(response, dict) or not response.get("success"):
        return None
    data = response.get("data")
    if not isinstance(data, dict):
        return None
    try:
        return int(data.get("count"))
    except (TypeError, ValueError):
        return None


def check_filter(read, endpoint: str, field: str) -> FilterTrust:
    """Two reads: unfiltered, then filtered by a value that matches nothing.

    `read(endpoint, body) -> response dict` is injected so this is
    testable without a network and so the caller keeps control of pacing
    and of the read-only guarantee.
    """
    unfiltered_response = read(endpoint, {"view_limit": 1})
    unfiltered = _count(unfiltered_response)
    if unfiltered is None:
        return FilterTrust(field, ERROR, detail=_error_of(unfiltered_response))
    if unfiltered <= 0:
        return FilterTrust(field, EMPTY_TABLE, unfiltered=unfiltered)

    probe = absent_value(field)
    probe_response = read(endpoint, {field: probe, "view_limit": 1})
    probe_count = _count(probe_response)
    if probe_count is None:
        return FilterTrust(field, ERROR, unfiltered=unfiltered,
                           detail=_error_of(probe_response))

    if probe_count == 0:
        return FilterTrust(field, TRUSTED, unfiltered, probe_count)

    if probe_count == unfiltered:
        logger.error("CSuite ignored the filter %r on %s — a value matching "
                     "nothing returned all %d rows", field, endpoint,
                     probe_count)
        return FilterTrust(field, FILTER_IGNORED, unfiltered, probe_count)

    # Neither zero nor everything. Something matched a value built from a
    # fresh uuid, which should be impossible — so the filter is doing
    # something we do not understand, and that is not a basis for
    # deciding whether a record exists.
    return FilterTrust(field, FILTER_IGNORED, unfiltered, probe_count,
                       detail=f"a value that should match nothing returned "
                              f"{probe_count}, which is neither 0 nor the "
                              f"{unfiltered} unfiltered total")


def _error_of(response) -> str:
    if not isinstance(response, dict):
        return f"unreadable response: {type(response).__name__}"
    return (f"http={response.get('http_status')} "
            f"outcome={response.get('outcome')} "
            f"error={str(response.get('error'))[:120]}")


class FilterNotTrusted(RuntimeError):
    """A duplicate decision was about to rest on a filter that does not
    filter."""


def search_before_create(read, endpoint: str, field: str, value):
    """(count, FilterTrust) for `field == value`. Raises if untrusted.

    The trust check runs FIRST, every time. Caching it across runs would
    mean trusting that CSuite's behaviour has not changed since — which
    is the assumption that produced the 18,797 result in the first place.
    """
    trust = check_filter(read, endpoint, field)
    if not trust.trusted:
        raise FilterNotTrusted(trust.why())

    response = read(endpoint, {field: value, "view_limit": 5})
    count = _count(response)
    if count is None:
        raise FilterNotTrusted(
            f"the search for {field}={value!r} failed: "
            f"{_error_of(response)}")
    return count, response
