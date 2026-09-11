"""
Mirror Read
===========
The read side of `csuite_mirror`. Reports query this instead of CSuite.

Why reports stopped calling CSuite
----------------------------------
Every report in intents/reports.py used to paginate a live endpoint with a
page cap on it — 10 pages of grants, 200 funds, 500 checks, 50 fund detail
calls. Each cap produced a number that was quietly a lower bound: "6 funds
have had no grants in 12 months" meant "6 of the 200 funds I happened to
read". Raising the caps was never the answer, because a report that makes
2,000 CSuite calls is a report that gets rate limited (see
clients/csuite_fetch.py for what that costs).

So the reports read a local table filled by scripts/mirror_refresh.py, and
a mirror-backed report makes NO live call at all. The trade is freshness
for completeness, and the trade is made visible: every one of those reports
ends with `as_of_line()`, which names the moment the data was taken.

The rule, and why it has no fallback
------------------------------------
If the mirror has no rows for a type a report needs, the report says
`not_loaded()` and stops. It does NOT quietly fall back to a capped live
fetch, because that is the failure this whole step exists to remove: a
silent fallback puts the lower-bound number back, with nothing on screen
to say so.

Staleness is deliberately NOT an error. A row past its expires_at is still
returned, because "here is the answer, taken four days ago" is useful and
"no answer" is not. The as_of_line is what makes the age visible; a reader
who sees a date from last week knows to run mirror_refresh.
"""

import contextvars
import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from clients import database
from config import Config

logger = logging.getLogger(__name__)

# Every timestamp a person reads in this app is shown in this zone. AMCF is
# an East Coast organisation; a mirror stamp in UTC reads as "four hours
# ago" to the people who use it.
DISPLAY_TZ = ZoneInfo("America/New_York")
DISPLAY_TZ_LABEL = "ET"

# Record types whose rows belong to a fund, and the field that says which.
_FUND_LINKED_TYPES = {
    "grant": "funit_id",
    "donation_fund_quarter": "funit_id",
}


_ROWS_SQL = """
    SELECT csuite_id, fund_group_id, data, synced_at
      FROM csuite_mirror
     WHERE record_type = %s
"""

_GET_SQL = _ROWS_SQL + " AND csuite_id = %s"

_FRESHNESS_SQL = """
    SELECT MAX(synced_at) AS synced_at
      FROM csuite_mirror
     WHERE record_type = %s
"""

_COUNT_SQL = """
    SELECT COUNT(*) AS n
      FROM csuite_mirror
     WHERE record_type = %s
"""


def _first_row(found):
    """The first row of a result set, or None.

    Guards against execute_query returning a rowcount rather than rows —
    which is what a mis-specified `fetch` argument looks like, and which
    would otherwise crash a report with TypeError instead of degrading.
    """
    if not isinstance(found, (list, tuple)) or not found:
        return None
    return found[0]


def _cell(row, key, index):
    """One column, whether the driver handed back a dict or a tuple."""
    if isinstance(row, dict):
        return row.get(key)
    try:
        return row[index]
    except (IndexError, TypeError):  # pragma: no cover
        return None


def _merge(row) -> dict:
    """One mirror row as a flat dict.

    The jsonb payload first, then the columns on top of it. Order matters:
    csuite_id, fund_group_id and synced_at are what the mirror knows about
    a record, and must win over anything similarly named inside the stored
    payload.
    """
    data = _cell(row, "data", 2)
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except ValueError:  # pragma: no cover - jsonb never round-trips badly
            data = {}
    merged = dict(data) if isinstance(data, dict) else {}
    merged["csuite_id"] = _cell(row, "csuite_id", 0)
    merged["fund_group_id"] = _cell(row, "fund_group_id", 1)
    merged["synced_at"] = _cell(row, "synced_at", 3)
    return merged


def rows(record_type: str, where_sql: str = "", params=(),
         exclude_test: bool = True, exclude_system: bool = True) -> list:
    """Every mirrored record of one type, as flat dicts.

    `where_sql` is appended to the WHERE clause and must be written by the
    caller, not built from user input — e.g.::

        rows("grant", "AND data->>'grant_status' = %s", ("paid",))

    Filtering in SQL rather than in Python matters for grants and
    donations, where the alternative is pulling 6,700 or 26,500 jsonb
    documents across the wire to throw most of them away.

    Two kinds of fund are dropped by default, along with the grant and
    quarter rows that belong to them:

    `exclude_test`    CSuite's test funds — names matching
                      Config.TEST_FUND_PATTERNS.
    `exclude_system`  the System fund group: Z_Agency Contra, Z_Cash
                      Balancing, Z_Revenue Share Holding. Accounting
                      plumbing, not funds anyone advises — a dormant-fund
                      list that names "Z_Cash Balancing" is noise.

    Pass False only when those funds are the point, e.g. a diagnostic.
    """
    found = _raw_rows(record_type, where_sql, params)
    if not exclude_test and not exclude_system:
        return found
    return _without_excluded_funds(record_type, found, exclude_test,
                                   exclude_system)


def _raw_rows(record_type: str, where_sql: str = "", params=()) -> list:
    sql = _ROWS_SQL + (f" {where_sql}" if where_sql else "")
    found = database.execute_query(sql, (record_type,) + tuple(params),
                                   fetch=True)
    if not isinstance(found, (list, tuple)):
        logger.error("mirror query for %s returned %s, not rows",
                     record_type, type(found).__name__)
        return []
    return [_merge(row) for row in found]


# ---------------------------------------------------------------------------
# Funds that no report should count
# ---------------------------------------------------------------------------

def is_test_fund_name(name) -> bool:
    """True if a fund name matches any Config.TEST_FUND_PATTERNS entry."""
    if not name:
        return False
    lowered = str(name).lower()
    return any(pattern and pattern.lower() in lowered
               for pattern in Config.TEST_FUND_PATTERNS)


# The three funds that make up CSuite's System group. Used to READ the
# group's id from the mirror rather than trusting a constant: whichever
# fgroup_id these three carry is the System group, whatever the config
# says. Matched case-insensitively on the name's leading text so the code
# suffix ("-(SYS0001)" or similar) does not matter.
SYSTEM_FUND_NAMES = (
    "z_agency contra",
    "z_cash balancing",
    "z_revenue share holding",
)


def is_system_fund_name(name) -> bool:
    if not name:
        return False
    lowered = str(name).lower().strip()
    return any(lowered.startswith(known) for known in SYSTEM_FUND_NAMES)


def _fund_group_of(fund: dict):
    for key in ("fund_group_id", "fgroup_id"):
        value = fund.get(key)
        if value not in (None, ""):
            try:
                return int(value)
            except (TypeError, ValueError):
                return value
    return None


def system_fund_group_id(funds=None):
    """The System group's fgroup_id, read from the mirror.

    Whatever group the three named system funds sit in is the answer. If
    they are not in the mirror (or disagree with each other), the value
    falls back to Config.FUND_GROUP_SYSTEM, and the disagreement is logged
    rather than silently resolved: a config constant that no longer
    matches the accounting system is exactly the kind of drift a report
    should not paper over.
    """
    if funds is None:
        funds = _raw_rows("fund")

    observed = {
        _fund_group_of(fund)
        for fund in funds
        if is_system_fund_name(fund.get("fund_name"))
    }
    observed.discard(None)

    configured = Config.FUND_GROUP_SYSTEM

    if not observed:
        return configured
    if len(observed) > 1:
        logger.warning(
            "the system funds sit in more than one group %s — using the "
            "configured %s", sorted(observed), configured)
        return configured

    found = observed.pop()
    if found != configured:
        logger.warning(
            "System fund group read from the mirror is %s but "
            "Config.FUND_GROUP_SYSTEM says %s — using the mirror's %s",
            found, configured, found)
    return found


def excluded_fund_ids(exclude_test: bool = True,
                      exclude_system: bool = True) -> dict:
    """{csuite_id: reason} for every mirrored fund a report should skip."""
    funds = _raw_rows("fund")
    excluded = {}

    system_group = system_fund_group_id(funds) if exclude_system else None

    for fund in funds:
        fund_id = str(fund.get("csuite_id"))
        if exclude_test and is_test_fund_name(fund.get("fund_name")):
            excluded[fund_id] = "test"
        elif exclude_system and _fund_group_of(fund) == system_group:
            excluded[fund_id] = "system"
    return excluded


def test_fund_ids() -> set:
    """csuite_ids (as text) of every mirrored fund that is a test fund."""
    return {fund_id for fund_id, reason
            in excluded_fund_ids(True, False).items() if reason == "test"}


# Excluded ids are logged once per report rather than once per query — a
# dormant-fund report reads funds AND grants, and the same test ids do not
# need announcing twice. reports.handle() opens the scope; outside one,
# every rows() call logs on its own.
_exclusion_scope: contextvars.ContextVar = contextvars.ContextVar(
    "jidhr_mirror_exclusion_scope", default=None)


@contextmanager
def exclusion_log_scope():
    """Within this block, each excluded fund id is logged at most once."""
    token = _exclusion_scope.set(set())
    try:
        yield
    finally:
        _exclusion_scope.reset(token)


def _log_exclusions(record_type: str, excluded: dict) -> None:
    """excluded: {fund_id: reason}."""
    if not excluded:
        return
    scope = _exclusion_scope.get()
    if scope is not None:
        excluded = {i: r for i, r in excluded.items() if i not in scope}
        if not excluded:
            return
        scope.update(excluded)

    by_reason = {}
    for fund_id, reason in excluded.items():
        by_reason.setdefault(reason, []).append(str(fund_id))
    described = "; ".join(
        f"{len(ids)} {reason} fund(s): {', '.join(sorted(ids))}"
        for reason, ids in sorted(by_reason.items()))
    logger.info("excluded from %s rows — %s", record_type, described)


def _without_excluded_funds(record_type: str, found: list,
                            exclude_test: bool, exclude_system: bool) -> list:
    if record_type == "fund":
        system_group = system_fund_group_id(found) if exclude_system else None
        kept, excluded = [], {}
        for fund in found:
            fund_id = str(fund.get("csuite_id"))
            if exclude_test and is_test_fund_name(fund.get("fund_name")):
                excluded[fund_id] = "test"
            elif exclude_system and _fund_group_of(fund) == system_group:
                excluded[fund_id] = "system"
            else:
                kept.append(fund)
        _log_exclusions(record_type, excluded)
        return kept

    link_field = _FUND_LINKED_TYPES.get(record_type)
    if link_field is None:
        return found

    skip = excluded_fund_ids(exclude_test, exclude_system)
    if not skip:
        return found

    kept, excluded = [], {}
    for row in found:
        value = row.get(link_field)
        fund_id = str(value) if value not in (None, "") else None
        if fund_id in skip:
            excluded[fund_id] = skip[fund_id]
        else:
            kept.append(row)
    _log_exclusions(record_type, excluded)
    return kept


def get(record_type: str, csuite_id) -> dict | None:
    """One mirrored record by its CSuite id, or None."""
    if csuite_id in (None, ""):
        return None
    row = _first_row(database.execute_query(
        _GET_SQL, (record_type, str(csuite_id)), fetch=True))
    return _merge(row) if row is not None else None


def freshness(record_type: str):
    """When this type was last written, or None if it never was."""
    row = _first_row(
        database.execute_query(_FRESHNESS_SQL, (record_type,), fetch=True))
    return _cell(row, "synced_at", 0) if row is not None else None


def count(record_type: str) -> int:
    """How many rows of this type the mirror holds."""
    row = _first_row(
        database.execute_query(_COUNT_SQL, (record_type,), fetch=True))
    if row is None:
        return 0
    try:
        return int(_cell(row, "n", 0))
    except (TypeError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# What a report says when it cannot answer
# ---------------------------------------------------------------------------

def not_loaded(record_type: str) -> str:
    """The one line a report prints instead of guessing."""
    return (f"⚠️ CSuite mirror not loaded for {record_type} — "
            "run mirror_refresh.")


def require(*record_types) -> str | None:
    """None if every named type has rows; otherwise the line to print.

    Checked before any work is done, so a report with an empty mirror
    costs one COUNT and returns — rather than rendering a confident,
    empty answer.
    """
    for record_type in record_types:
        try:
            if count(record_type) == 0:
                return not_loaded(record_type)
        except Exception as e:
            logger.error("mirror availability check failed for %s: %s",
                         record_type, e)
            return not_loaded(record_type)
    return None


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

def _to_datetime(value):
    """A datetime (tz-aware, UTC-anchored if it was naive) from any of the
    timestamp shapes this app meets, or None.

    Accepted: datetime; ISO 8601 text (with Z, an offset, or naive); epoch
    milliseconds as int, float or numeric text (what HubSpot puts in
    hs_last_activity_date); epoch seconds if the number is too small to be
    milliseconds.
    """
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, bool):
        return None
    elif isinstance(value, (int, float)) or (
            isinstance(value, str) and value.strip().lstrip("-").isdigit()):
        number = float(value)
        # Anything under 10^11 is seconds, not milliseconds: 10^11 ms is
        # 1973, 10^11 s is the year 5138.
        seconds = number / 1000.0 if abs(number) >= 1e11 else number
        try:
            dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z") or text.endswith("z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None

    if dt.tzinfo is None:
        # Postgres NOW() on Railway is UTC; a naive stamp from the mirror
        # is a UTC stamp that lost its label, not a local one.
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def fmt_ts(value) -> str:
    """A timestamp as a person reads it: "Sep 10, 2026, 1:04 PM ET".

    One function for every timestamp the app shows — mirror provenance,
    "last contacted", anything else — so they all read the same way and
    all sit in the same zone. Takes an ISO string, epoch milliseconds, or
    a datetime; returns "unknown" for anything it cannot read rather than
    raising inside a report.
    """
    dt = _to_datetime(value)
    if dt is None:
        return "unknown"
    local = dt.astimezone(DISPLAY_TZ)
    hour = local.hour % 12 or 12
    return (f"{local:%b} {local.day}, {local.year}, "
            f"{hour}:{local:%M} {local:%p} {DISPLAY_TZ_LABEL}")


def as_of_line(*record_types) -> str:
    """The provenance footer every mirror-backed report ends with.

    Uses the OLDEST freshness among the types the report read, because a
    report is only as current as its stalest input: a dormant-fund answer
    that joins day-old funds to week-old grants is a week-old answer.
    """
    stamps = []
    for record_type in record_types:
        try:
            stamp = freshness(record_type)
        except Exception as e:  # pragma: no cover - reported, never fatal
            logger.error("freshness lookup failed for %s: %s", record_type, e)
            stamp = None
        if stamp is not None:
            stamps.append(stamp)

    if not stamps:
        return "📅 Data as of unknown (CSuite mirror)"

    # Normalised before comparison so a tz-aware stamp and a naive one
    # (the driver can hand back either) do not raise on min().
    normalised = [dt for dt in (_to_datetime(s) for s in stamps) if dt]
    oldest = min(normalised) if normalised else None
    return f"📅 Data as of {fmt_ts(oldest)} (CSuite mirror)"
