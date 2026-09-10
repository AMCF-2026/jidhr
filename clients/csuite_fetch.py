"""
CSuite Fetch
============
Paged, paced, READ-ONLY reads from CSuite.

This module never writes. Every endpoint it knows about is a `list` or a
`display`, and `clients.csuite.is_csuite_write()` returns False for all of
them — so nothing here reaches the write-audit path either.

Why it exists separately from CSuiteClient._get_all_pages
---------------------------------------------------------
That helper stops when a page comes back shorter than the batch size it
asked for. CSuite does not honour `view_limit` on most endpoints (probe #3,
C11: profile/donation/grant are pinned at 100, check at 200 regardless of
what you ask for), so "short page" is not a reliable end-of-data signal —
the very first page is already "short" against a requested 500. The only
signal CSuite gives is an empty page, which is what this module waits for.

The pagination contract, measured (probe #3, C11)
-------------------------------------------------
    endpoint            offset?  limit honoured?  page size   count?
    profile/list        yes      no               100         data.count
    donation/list       yes      no               100         data.count
    grant/list          yes      no               100         data.count
    check/list          yes      no               200         data.count
    funit/list          yes      YES              as asked    data.count
    event/list/dates    NO       no               all 176     data.count
    funit/feetype       NO       no               all 3       data.count

Endpoints without an offset parameter must NOT be paged: asking for a
second page returns the same rows again, so a naive "loop until empty"
would duplicate forever. `ENDPOINT_CONTRACTS` records which is which.

Pacing and rate limits
----------------------
CSuite is a live accounting API with a rate limit that probe #3 tripped at
roughly 400 calls. Every call is separated by CSUITE_PACE_MS (default 150).
An HTTP 429 buys one 5-second retry; a second 429 stops the fetch with
complete=False. Nothing in here raises — a caller always gets a
FetchResult and can see for itself whether the data is whole.
"""

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

DEFAULT_PACE_MS = 150
RATE_LIMIT_BACKOFF_S = 5.0

# A safety stop, not a real limit. donation/list is the largest sweep at
# ~266 pages; 2000 is far beyond any legitimate run and exists only so a
# server that never returns an empty page cannot spin forever.
MAX_PAGES = 2000

RATE_LIMITED_ERROR = "rate limited"


@dataclass(frozen=True)
class PageContract:
    """How one endpoint pages, as measured — not as documented."""

    paginate: bool
    view_limit: int | None = None


# funit/list is the one endpoint that honours view_limit, so all 397 funds
# arrive in a single call instead of four.
ENDPOINT_CONTRACTS = {
    "profile/list": PageContract(paginate=True, view_limit=100),
    "donation/list": PageContract(paginate=True, view_limit=100),
    "grant/list": PageContract(paginate=True, view_limit=100),
    "check/list": PageContract(paginate=True, view_limit=200),
    "funit/list": PageContract(paginate=True, view_limit=500),
    "event/list/dates": PageContract(paginate=False, view_limit=1000),
    "funit/feetype": PageContract(paginate=False),
    "funit/display": PageContract(paginate=False),
    "profile/display": PageContract(paginate=False),
}

# An endpoint nobody has measured is assumed to page like the majority.
DEFAULT_CONTRACT = PageContract(paginate=True, view_limit=100)


@dataclass(frozen=True)
class FetchResult:
    """What came back, and whether it is all of it.

    `complete` is the field that matters. False means the caller is holding
    a partial answer and must not treat it as the truth — sync/mirror.py
    refuses to write anything at all when it sees False.
    """

    records: list = field(default_factory=list)
    complete: bool = False
    pages: int = 0
    expected: int | None = None
    error: str | None = None
    calls: int = 0

    @property
    def count(self) -> int:
        return len(self.records)

    @property
    def count_matches_expected(self) -> bool | None:
        """True/False if CSuite reported a total; None if it did not.

        A complete fetch that disagrees with data.count is worth noticing:
        it means rows were added or removed while the sweep was running.
        """
        if self.expected is None:
            return None
        return len(self.records) == self.expected


# ---------------------------------------------------------------------------
# Pacing
# ---------------------------------------------------------------------------

def configured_pace_ms() -> int:
    """CSUITE_PACE_MS from the environment, or the default.

    A bad value falls back to the default rather than failing the run: the
    cost of a wrong pace is a slower sweep, not a wrong answer.
    """
    raw = os.environ.get("CSUITE_PACE_MS")
    if raw is None or str(raw).strip() == "":
        return DEFAULT_PACE_MS
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        logger.warning(
            "CSUITE_PACE_MS=%r is not an integer — using %d ms",
            raw, DEFAULT_PACE_MS)
        return DEFAULT_PACE_MS
    if value < 0:
        logger.warning(
            "CSUITE_PACE_MS=%d is negative — using %d ms", value,
            DEFAULT_PACE_MS)
        return DEFAULT_PACE_MS
    return value


def pace_seconds(pace_ms=None) -> float:
    """Seconds to wait between calls."""
    if pace_ms is None:
        pace_ms = configured_pace_ms()
    try:
        return max(0.0, float(pace_ms) / 1000.0)
    except (TypeError, ValueError):
        return DEFAULT_PACE_MS / 1000.0


def pace_sleep(seconds: float) -> None:
    """Wait between calls.

    A named indirection rather than a bare time.sleep so tests can
    neutralise pacing without patching the time module globally.
    """
    if seconds > 0:
        time.sleep(seconds)


# ---------------------------------------------------------------------------
# Rate-limit detection
# ---------------------------------------------------------------------------

# clients/csuite.py's _request() returns a dict with no HTTP status in it, so
# a 429 arrives as either an "Invalid JSON response" error (CSuite serves a
# non-JSON body) or an errors[] entry. _StatusTap below is the reliable
# signal; this pattern is the fallback for when the tap cannot be installed.
_RATE_LIMIT_RE = re.compile(
    r"(?:\b429\b|too\s+many\s+requests|rate[\s_-]?limit)", re.IGNORECASE)


def looks_rate_limited(status_code=None, error=None) -> bool:
    """True if this response is CSuite refusing us for going too fast."""
    if status_code == 429:
        return True
    if error and _RATE_LIMIT_RE.search(str(error)):
        return True
    return False


class _StatusTap:
    """Records the HTTP status of the client's most recent POST.

    `CSuiteClient._request` deliberately returns a normalised dict and drops
    the status code, and Step 3a may not modify that file. Wrapping the
    session's `post` is how scripts/probe_apis.py solves the same problem
    (see `wrap_csuite_session` there); this is the same trick, scoped to a
    single fetch and always unwound, including on exception.

    A client with no `session` attribute — a test stub, say — taps nothing
    and leaves `status` at None. Detection then falls back to the error text.
    """

    def __init__(self, client):
        self._client = client
        self._session = None
        self._original = None
        self._had_own_post = False
        self.status = None

    def __enter__(self):
        session = getattr(self._client, "session", None)
        original = getattr(session, "post", None)
        if session is None or not callable(original):
            return self

        # Whether `post` was already an instance attribute decides how to
        # put things back: restoring a bound method that came from the
        # class would leave a permanent instance attribute behind.
        try:
            self._had_own_post = "post" in vars(session)
        except TypeError:  # pragma: no cover - session has no __dict__
            self._had_own_post = True

        def recording_post(*args, **kwargs):
            response = original(*args, **kwargs)
            self.status = getattr(response, "status_code", None)
            return response

        try:
            session.post = recording_post
        except Exception:  # pragma: no cover - session refuses attribute set
            return self

        self._session = session
        self._original = original
        return self

    def __exit__(self, *exc_info):
        if self._session is not None:
            try:
                if self._had_own_post:
                    self._session.post = self._original
                else:
                    del self._session.post
            except Exception:  # pragma: no cover
                pass
        self._session = None
        self._original = None
        return False

    def reset(self):
        self.status = None


# ---------------------------------------------------------------------------
# Response shape
# ---------------------------------------------------------------------------

def _envelope(result):
    """The `data` object out of a CSuite response, or None."""
    if not isinstance(result, dict):
        return None
    data = result.get("data")
    return data


def read_records(data) -> list:
    """The row list out of a `data` envelope.

    Every CSuite list endpoint measured puts its rows at data.results
    (probe #3). A bare list is accepted too, for robustness.
    """
    if isinstance(data, dict):
        results = data.get("results")
        if isinstance(results, list):
            return results
        return []
    if isinstance(data, list):
        return data
    return []


def read_expected(data) -> int | None:
    """data.count, when CSuite reports one."""
    if not isinstance(data, dict):
        return None
    count = data.get("count")
    if isinstance(count, bool):
        return None
    if isinstance(count, int):
        return count
    if isinstance(count, str) and count.strip().isdigit():
        return int(count.strip())
    return None


def _error_text(result) -> str | None:
    """The error out of a CSuite response, or None if it succeeded."""
    if not isinstance(result, dict):
        return f"unexpected response type {type(result).__name__}"
    if result.get("error"):
        return str(result["error"])
    if result.get("success") is True:
        return None
    errors = result.get("errors")
    if isinstance(errors, list) and errors:
        return str(errors[0])
    return "CSuite reported failure with no error text"


# ---------------------------------------------------------------------------
# One call, with the 429 policy
# ---------------------------------------------------------------------------

@dataclass
class _CallOutcome:
    data: object = None
    error: str | None = None
    calls: int = 0


def _call_once(client, endpoint: str, request: dict, tap: _StatusTap,
               label: str) -> _CallOutcome:
    """One CSuite call. On 429: wait 5s, retry once, then give up.

    Never raises: a transport exception from deep inside the client becomes
    an error string like any other failure.
    """
    outcome = _CallOutcome()

    for attempt in (1, 2):
        tap.reset()
        try:
            result = client._request(endpoint, dict(request))
        except Exception as e:  # pragma: no cover - client swallows its own
            outcome.calls += 1
            outcome.error = f"{type(e).__name__}: {e}"
            return outcome

        outcome.calls += 1
        error = _error_text(result)

        if error is None:
            outcome.data = _envelope(result)
            return outcome

        if looks_rate_limited(tap.status, error):
            if attempt == 1:
                logger.warning(
                    "CSuite rate limited on %s (%s) — waiting %.0fs for one "
                    "retry", endpoint, label, RATE_LIMIT_BACKOFF_S)
                pace_sleep(RATE_LIMIT_BACKOFF_S)
                continue
            logger.error(
                "CSuite rate limited twice on %s (%s) — stopping",
                endpoint, label)
            outcome.error = RATE_LIMITED_ERROR
            return outcome

        outcome.error = error
        return outcome

    return outcome  # pragma: no cover - loop always returns


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_all(client, endpoint: str, params: dict = None, *,
              pace_ms=None, max_pages: int = MAX_PAGES) -> FetchResult:
    """Every record from a CSuite list endpoint, or an honest partial.

    Pages by `view_offset` until a page comes back empty. Endpoints with no
    offset parameter (event/list/dates, funit/feetype) are fetched once —
    see ENDPOINT_CONTRACTS.

    Returns a FetchResult. Never raises.
    """
    contract = ENDPOINT_CONTRACTS.get(endpoint, DEFAULT_CONTRACT)
    pace = pace_seconds(pace_ms)

    base = dict(params or {})
    if contract.view_limit is not None:
        base.setdefault("view_limit", contract.view_limit)

    records: list = []
    pages = 0
    calls = 0
    expected = None
    offset = 0
    started = time.perf_counter()

    with _StatusTap(client) as tap:
        while True:
            if pages >= max_pages:
                error = (f"stopped after {max_pages} pages — {endpoint} never "
                         "returned an empty page")
                logger.error(error)
                return FetchResult(records, False, pages, expected, error,
                                   calls)

            request = dict(base)
            if contract.paginate:
                request["view_offset"] = offset

            if calls:
                pace_sleep(pace)

            outcome = _call_once(client, endpoint, request, tap,
                                 f"offset {offset}")
            calls += outcome.calls

            if outcome.error is not None:
                logger.error(
                    "CSuite fetch of %s failed at offset %d after %d rows: %s",
                    endpoint, offset, len(records), outcome.error)
                return FetchResult(records, False, pages, expected,
                                   outcome.error, calls)

            pages += 1

            if expected is None:
                expected = read_expected(outcome.data)

            page = read_records(outcome.data)
            if not page:
                break

            records.extend(page)

            if not contract.paginate:
                # No offset parameter: a second call would re-serve page one.
                break

            offset += len(page)

    elapsed = time.perf_counter() - started
    logger.info(
        "CSuite fetch %s: %d records in %d pages / %d calls (%.1fs), "
        "expected %s",
        endpoint, len(records), pages, calls, elapsed,
        expected if expected is not None else "unreported")

    if expected is not None and len(records) != expected:
        # Not an error. CSuite is live; rows move while a 266-page sweep
        # runs. Worth a line in the log so a large drift is visible.
        logger.info(
            "CSuite fetch %s: got %d but data.count said %d (difference %+d)",
            endpoint, len(records), expected, len(records) - expected)

    return FetchResult(records, True, pages, expected, None, calls)


def fetch_one(client, endpoint: str, params: dict = None, *,
              pace_ms=None) -> FetchResult:
    """One display-style record, with the same pacing and 429 policy.

    Used for the funit/display sweep, which is one call per fund and needs
    exactly the rate-limit behaviour fetch_all has. Returns the object at
    `data` as a single-element `records` list.
    """
    with _StatusTap(client) as tap:
        outcome = _call_once(client, endpoint, dict(params or {}), tap,
                             "single")

    if outcome.error is not None:
        return FetchResult([], False, 0, None, outcome.error, outcome.calls)

    data = outcome.data
    records = [data] if isinstance(data, dict) else read_records(data)
    return FetchResult(records, True, 1, None, None, outcome.calls)


def canonical_json(payload) -> str:
    """Payload as canonical JSON — sorted keys, tight separators.

    Shared by the hash and the jsonb column so a row's stored `data` and its
    `data_hash` can never disagree about what was hashed.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      default=str)
