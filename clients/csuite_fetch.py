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
CSuite is a live accounting API with a rate limit, and we have now
measured where it is. On 2026-09-10 a run made 401 calls, paused three
minutes, made 265 more, and was refused at a cumulative ~666. It then
refused EVERY call for the next 15 seconds and more.

Two things follow, and both were wrong in the first version:

  * The window is cumulative over minutes, not per-second. Pacing alone
    does not avoid it — a 960-call sweep will hit it whatever the gap
    between calls. That is what the caller's budget is for.
  * A 5-second retry is useless. Once tripped, the limiter stays shut for
    longer than that, so a short retry just spends another call on
    another 429. Backoff is 30s, 60s, 120s, and a Retry-After header
    from CSuite overrides all three.

Every call is separated by CSUITE_PACE_MS (default 400). Nothing in here
raises — a caller always gets a FetchResult and can see for itself
whether the data is whole, how many times it was refused, and when the
refusals started.
"""

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

# Raised from 150 after the 2026-09-10 rate limit. Pacing is not what
# saves a long sweep, but a slower one spends its budget over a longer
# window, which is the axis the limiter actually measures.
DEFAULT_PACE_MS = 400

# Waits after a 429, in order. Three waits, so four attempts in all. The
# first is 30s because the limiter was observed still refusing 15s after
# it tripped; 5s was measurably too short.
RATE_LIMIT_BACKOFFS = (30.0, 60.0, 120.0)

# A Retry-After longer than this is not honoured — CSuite asking us to
# wait half an hour is a reason to stop the run and come back, not to
# hold a database connection open through it.
MAX_RETRY_AFTER_S = 300.0

# A safety stop, not a real limit. donation/list is the largest sweep at
# ~266 pages; 2000 is far beyond any legitimate run and exists only so a
# server that never returns an empty page cannot spin forever.
MAX_PAGES = 2000

RATE_LIMITED_ERROR = "rate limited"
BUDGET_ERROR = "budget reached"


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
    # When CSuite first refused us, and how many times in total. Recorded
    # even on a fetch that eventually succeeded: the point is to learn the
    # shape of the limiter from the run ledger rather than from a log
    # nobody kept.
    first_429_at: str | None = None
    total_429s: int = 0

    @property
    def count(self) -> int:
        return len(self.records)

    @property
    def budget_exhausted(self) -> bool:
        return self.error == BUDGET_ERROR

    @property
    def rate_limited(self) -> bool:
        return self.error == RATE_LIMITED_ERROR

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
# Call budget
# ---------------------------------------------------------------------------

class CallBudget:
    """A shared ceiling on how many CSuite calls a run may make.

    One budget is threaded through every fetch in a run, so a limit of 500
    means 500 calls total and not 500 per record type. Stopping at a
    number we chose is strictly better than stopping at the number CSuite
    chose: a budget stop leaves staged work behind and a clean
    complete=False, while a 429 wastes the call it was refused on and
    every call for the next 15 seconds.
    """

    __slots__ = ("limit", "used")

    def __init__(self, limit=None):
        self.limit = limit if limit is None or limit > 0 else None
        self.used = 0

    @property
    def remaining(self):
        if self.limit is None:
            return None
        return max(0, self.limit - self.used)

    @property
    def exhausted(self) -> bool:
        return self.limit is not None and self.used >= self.limit

    def spend(self, calls: int = 1) -> None:
        self.used += calls

    def __repr__(self):  # pragma: no cover - diagnostics only
        return f"CallBudget(used={self.used}, limit={self.limit})"


def _budget(budget):
    """A budget for a call site that was given none: unlimited."""
    return budget if budget is not None else CallBudget(None)


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


def parse_retry_after(value, now=None) -> float | None:
    """Seconds to wait, from a Retry-After header. None if unusable.

    RFC 9110 allows either a delay in seconds or an HTTP date. Both are
    accepted; anything else, anything negative, and anything longer than
    MAX_RETRY_AFTER_S is ignored in favour of our own backoff.
    """
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    seconds = None
    try:
        seconds = float(text)
    except ValueError:
        try:
            when = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None
        if when is None:
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        reference = now or datetime.now(timezone.utc)
        seconds = (when - reference).total_seconds()

    if seconds is None or seconds < 0:
        return None
    if seconds > MAX_RETRY_AFTER_S:
        logger.warning(
            "CSuite asked us to wait %.0fs, which is past the %.0fs cap — "
            "stopping instead of holding the run open",
            seconds, MAX_RETRY_AFTER_S)
        return None
    return seconds


def _clock() -> str:
    """UTC wall-clock time, for a log line someone will read tomorrow."""
    return datetime.now(timezone.utc).strftime("%H:%M:%S UTC")


def _stamp() -> str:
    """An ISO timestamp for the run ledger."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class _StatusTap:
    """Records the HTTP status and Retry-After of the most recent POST.

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
        self.retry_after = None

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
            headers = getattr(response, "headers", None) or {}
            try:
                self.retry_after = headers.get("Retry-After")
            except Exception:  # pragma: no cover - exotic header mapping
                self.retry_after = None
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
        self.retry_after = None


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
    first_429_at: str | None = None
    total_429s: int = 0


def _call_once(client, endpoint: str, request: dict, tap: _StatusTap,
               label: str, budget: CallBudget) -> _CallOutcome:
    """One CSuite call, with the rate-limit policy around it.

    A 429 is waited out — Retry-After if CSuite sent one, otherwise 30s,
    60s, 120s — and retried, up to three waits. A fourth refusal gives up
    with error="rate limited".

    Never raises: a transport exception from deep inside the client
    becomes an error string like any other failure.
    """
    outcome = _CallOutcome()
    attempts = len(RATE_LIMIT_BACKOFFS) + 1

    for attempt in range(attempts):
        if budget.exhausted:
            outcome.error = BUDGET_ERROR
            return outcome

        tap.reset()
        budget.spend()
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

        if not looks_rate_limited(tap.status, error):
            outcome.error = error
            return outcome

        outcome.total_429s += 1
        if outcome.first_429_at is None:
            outcome.first_429_at = _stamp()

        if attempt >= len(RATE_LIMIT_BACKOFFS):
            logger.error(
                "CSuite rate limited %d times on %s (%s) — giving up at %s",
                outcome.total_429s, endpoint, label, _clock())
            outcome.error = RATE_LIMITED_ERROR
            return outcome

        retry_after = parse_retry_after(tap.retry_after)
        wait = retry_after if retry_after is not None \
            else RATE_LIMIT_BACKOFFS[attempt]
        source = "Retry-After" if retry_after is not None else "backoff"

        logger.warning(
            "CSuite rate limited on %s (%s) at %s — waiting %.0fs (%s), "
            "try %d of %d",
            endpoint, label, _clock(), wait, source,
            attempt + 2, attempts)
        pace_sleep(wait)

    return outcome  # pragma: no cover - loop always returns


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_all(client, endpoint: str, params: dict = None, *,
              pace_ms=None, max_pages: int = MAX_PAGES,
              budget: "CallBudget" = None) -> FetchResult:
    """Every record from a CSuite list endpoint, or an honest partial.

    Pages by `view_offset` until a page comes back empty. Endpoints with no
    offset parameter (event/list/dates, funit/feetype) are fetched once —
    see ENDPOINT_CONTRACTS.

    `budget`, if given, is shared across every fetch in a run: the sweep
    stops cleanly with error="budget reached" rather than running until
    CSuite refuses it.

    Returns a FetchResult. Never raises.
    """
    contract = ENDPOINT_CONTRACTS.get(endpoint, DEFAULT_CONTRACT)
    pace = pace_seconds(pace_ms)
    budget = _budget(budget)

    base = dict(params or {})
    if contract.view_limit is not None:
        base.setdefault("view_limit", contract.view_limit)

    records: list = []
    pages = 0
    calls = 0
    expected = None
    offset = 0
    first_429_at = None
    total_429s = 0
    started = time.perf_counter()

    def finish(complete, error):
        return FetchResult(records, complete, pages, expected, error, calls,
                           first_429_at, total_429s)

    with _StatusTap(client) as tap:
        while True:
            if pages >= max_pages:
                error = (f"stopped after {max_pages} pages — {endpoint} never "
                         "returned an empty page")
                logger.error(error)
                return finish(False, error)

            if budget.exhausted:
                logger.warning(
                    "call budget reached on %s after %d rows — stopping "
                    "cleanly with %d pages fetched",
                    endpoint, len(records), pages)
                return finish(False, BUDGET_ERROR)

            request = dict(base)
            if contract.paginate:
                request["view_offset"] = offset

            if calls:
                pace_sleep(pace)

            outcome = _call_once(client, endpoint, request, tap,
                                 f"offset {offset}", budget)
            calls += outcome.calls
            total_429s += outcome.total_429s
            if first_429_at is None:
                first_429_at = outcome.first_429_at

            if outcome.error is not None:
                logger.error(
                    "CSuite fetch of %s stopped at offset %d after %d rows: "
                    "%s", endpoint, offset, len(records), outcome.error)
                return finish(False, outcome.error)

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
        "expected %s%s",
        endpoint, len(records), pages, calls, elapsed,
        expected if expected is not None else "unreported",
        f", {total_429s} rate limits survived" if total_429s else "")

    if expected is not None and len(records) != expected:
        # Not an error. CSuite is live; rows move while a 266-page sweep
        # runs. Worth a line in the log so a large drift is visible.
        logger.info(
            "CSuite fetch %s: got %d but data.count said %d (difference %+d)",
            endpoint, len(records), expected, len(records) - expected)

    return finish(True, None)


def fetch_one(client, endpoint: str, params: dict = None, *,
              pace_ms=None, budget: "CallBudget" = None) -> FetchResult:
    """One display-style record, with the same rate-limit policy.

    Used for the funit/display sweep, which is one call per fund and needs
    exactly the backoff behaviour fetch_all has. Returns the object at
    `data` as a single-element `records` list.
    """
    budget = _budget(budget)

    if budget.exhausted:
        return FetchResult([], False, 0, None, BUDGET_ERROR, 0)

    with _StatusTap(client) as tap:
        outcome = _call_once(client, endpoint, dict(params or {}), tap,
                             "single", budget)

    if outcome.error is not None:
        return FetchResult([], False, 0, None, outcome.error, outcome.calls,
                           outcome.first_429_at, outcome.total_429s)

    data = outcome.data
    records = [data] if isinstance(data, dict) else read_records(data)
    return FetchResult(records, True, 1, None, None, outcome.calls,
                       outcome.first_429_at, outcome.total_429s)


def canonical_json(payload) -> str:
    """Payload as canonical JSON — sorted keys, tight separators.

    Shared by the hash and the jsonb column so a row's stored `data` and its
    `data_hash` can never disagree about what was hashed.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      default=str)
