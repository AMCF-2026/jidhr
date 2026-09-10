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

import logging

from clients import database

logger = logging.getLogger(__name__)


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
        import json
        try:
            data = json.loads(data)
        except ValueError:  # pragma: no cover - jsonb never round-trips badly
            data = {}
    merged = dict(data) if isinstance(data, dict) else {}
    merged["csuite_id"] = _cell(row, "csuite_id", 0)
    merged["fund_group_id"] = _cell(row, "fund_group_id", 1)
    merged["synced_at"] = _cell(row, "synced_at", 3)
    return merged


def rows(record_type: str, where_sql: str = "", params=()) -> list:
    """Every mirrored record of one type, as flat dicts.

    `where_sql` is appended to the WHERE clause and must be written by the
    caller, not built from user input — e.g.::

        rows("grant", "AND data->>'grant_status' = %s", ("paid",))

    Filtering in SQL rather than in Python matters for grants and
    donations, where the alternative is pulling 6,700 or 26,500 jsonb
    documents across the wire to throw most of them away.
    """
    sql = _ROWS_SQL + (f" {where_sql}" if where_sql else "")
    found = database.execute_query(sql, (record_type,) + tuple(params),
                                   fetch=True)
    if not isinstance(found, (list, tuple)):
        logger.error("mirror query for %s returned %s, not rows",
                     record_type, type(found).__name__)
        return []
    return [_merge(row) for row in found]


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

def _format_stamp(value) -> str:
    """A synced_at as something a person can read."""
    if value is None:
        return "unknown"
    formatter = getattr(value, "strftime", None)
    if formatter is not None:
        return formatter("%Y-%m-%d %H:%M UTC")
    return str(value)


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

    try:
        oldest = min(stamps)
    except TypeError:  # pragma: no cover - mixed tz-aware and naive stamps
        oldest = stamps[0]
    return f"📅 Data as of {_format_stamp(oldest)} (CSuite mirror)"
