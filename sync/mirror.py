"""
CSuite Mirror
=============
Fills `csuite_mirror` from CSuite so the assistant can answer from a local
table instead of paying a live API sweep per question.

READ-ONLY against CSuite. Every endpoint touched here is a list or a
display; nothing in this module can change a record in the accounting
system. The only writes are to two Postgres tables, both of which already
exist and are never created or altered from here.

The one rule everything else follows from
-----------------------------------------
A partial fetch is never written. If any page of any endpoint fails — a
rate limit, a transport error, a fund whose display call did not come
back — the run is marked 'failed' and NOT ONE ROW is written for that
record type. Half a mirror is worse than a stale one, because a stale
mirror is visibly stale and a half-written one is not: rows that failed
to refresh would keep their old `synced_at` while their neighbours moved
on, and the delete-what-we-did-not-see pass would drop live records
purely because the sweep stopped early.

Freshness
---------
Reference data (fund, fee_type, event, grant, check) has no expiry — it
changes rarely and a stale row is still a true row. Anything derived from
donors (profile, donation_agg) expires 96 hours after it is written, so a
mirror that stops refreshing stops being trusted rather than quietly
ageing.

Run ledger
----------
Each record type is its own `sync_runs` row with sync_type='mirror'. The
record type itself lives in `notes.record_type`, since sync_type is a
constrained vocabulary. Statuses walk:

    running -> fetched -> verified -> writing -> complete
                                   \\-> (dry run stops at 'verified')
    any stage -> failed
    a database failure mid-write -> aborted

Who started it
--------------
`sync_runs.triggered_by` is BIGINT REFERENCES users(id). It holds a user
id or NULL — never a label. A CLI run has no user, so it writes NULL,
sets trigger_source='cli', and puts the human-readable label in
`notes.trigger`. A future chat-triggered run passes the signed-in user's
id as `triggered_by_user_id` and gets a real foreign key.

Nothing about "who ran this" is worth a broken foreign key, so the two
are kept in separate parameters and a value in the wrong one is rejected
rather than coerced.

Counts on the row mean:
    expected_count   what CSuite's data.count claimed
    fetched_count    how many records we actually hold
    written_count    inserted or updated (hash differed)
    unchanged_count  already present with an identical hash
    skipped_count    deleted — present in the mirror, absent from CSuite
    failed_count     records CSuite would not give us
"""

import hashlib
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from clients import database
from clients.csuite import CSuiteClient
from clients.csuite_fetch import (
    canonical_json,
    fetch_all,
    fetch_one,
    pace_seconds,
    pace_sleep,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# What gets mirrored
# ---------------------------------------------------------------------------

# Order matters. Reference data first, donor-derived data last: a run that
# dies partway through still leaves the cheap, stable types refreshed.
RECORD_TYPES = (
    "fund",
    "fee_type",
    "event",
    "grant",
    "check",
    "profile",
    "donation_agg",
)

# Hours until a row stops being trusted. Absent = never expires.
TTL_HOURS = {
    "profile": 96,
    "donation_agg": 96,
}

# Every field a mirrored profile row may carry. Everything else CSuite
# returns — phone numbers, work details, website, custom fields — is
# dropped before the row is built, not filtered on the way out. The mirror
# is a query cache, not a second copy of the donor database.
#
# Two names differ from the brief because they differ in the live API
# (verified against probe #3's profile/list field inventory):
#   created_date  CSuite calls this `created_ts`; both are accepted and
#                 stored under `created_date`.
#   first/last    are `first_name` / `last_name`.
PROFILE_FIELDS = (
    "profile_id",
    "ptype",
    "name",
    "first_name",
    "last_name",
    "organization",
    "primary_email",
    "primary_address_string",
    "dead",
)
PROFILE_CREATED_SOURCES = ("created_date", "created_ts")

# Rows are upserted in batches rather than one statement per record —
# 18,600 profiles is 38 statements at this size instead of 18,600.
UPSERT_BATCH = 500
DELETE_BATCH = 1000

# notes.deleted_ids is a sample, not the full list: a type that lost 5,000
# records should not put 5,000 ids in a jsonb column.
MAX_NOTED_IDS = 50


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _hash(payload) -> str:
    """sha256 of the canonical JSON form of a record."""
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _key(value) -> str | None:
    """A record's mirror key, as text.

    Compared as strings throughout so the code does not depend on whether
    csuite_mirror.csuite_id is a text or a bigint column.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_key(row: dict, names) -> str | None:
    for name in names:
        key = _key(row.get(name))
        if key is not None:
            return key
    return None


def _money(value) -> Decimal:
    """A CSuite amount as a Decimal. Unparsable amounts are zero.

    CSuite returns donation amounts as numeric strings. Decimal, not float:
    26,000 donations summed in binary floating point drifts, and a lifetime
    total that is off by cents is a support ticket.
    """
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return Decimal("0")
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text:
        return Decimal("0")
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1]
    try:
        amount = Decimal(text)
    except (InvalidOperation, ValueError):
        return Decimal("0")
    return -amount if negative else amount


def _money_str(amount: Decimal) -> str:
    """Two decimal places, as text — jsonb has no Decimal."""
    try:
        return str(amount.quantize(Decimal("0.01")))
    except (InvalidOperation, ValueError):  # pragma: no cover
        return str(amount)


def _date(value) -> str | None:
    """A date string, or None. CSuite dates are already YYYY-MM-DD."""
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _order_id(value) -> int:
    """A tiebreaker for two donations on the same date."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# Gathering — what each record type fetches and what it stores
# ---------------------------------------------------------------------------

@dataclass
class MirrorRow:
    csuite_id: str
    data: dict
    fund_group_id: object = None


@dataclass
class Gathered:
    """The result of fetching one record type, before anything is written."""

    rows: list = field(default_factory=list)
    complete: bool = False
    expected: int | None = None
    calls: int = 0
    pages: int = 0
    failed: int = 0
    error: str | None = None
    notes: dict = field(default_factory=dict)


def _gather_list(client, endpoint: str, key_names, pace_ms) -> Gathered:
    """A plain list endpoint: every row, keyed by its id."""
    result = fetch_all(client, endpoint, pace_ms=pace_ms)

    gathered = Gathered(
        complete=result.complete,
        expected=result.expected,
        calls=result.calls,
        pages=result.pages,
        error=result.error,
        notes={"endpoint": endpoint},
    )
    if not result.complete:
        return gathered

    unkeyed = 0
    for row in result.records:
        key = _first_key(row, key_names)
        if key is None:
            unkeyed += 1
            continue
        gathered.rows.append(MirrorRow(csuite_id=key, data=row))

    if unkeyed:
        # Not fatal, but never silent: a row CSuite returned that we cannot
        # key is a row the mirror will not have.
        gathered.notes["unkeyed_rows"] = unkeyed
        logger.warning(
            "%s: %d rows had none of %s and were not mirrored",
            endpoint, unkeyed, list(key_names))

    return gathered


def _gather_fund(client, pace_ms) -> Gathered:
    """Every fund, as its funit/display payload.

    funit/list carries six fields and neither fgroup_id nor the balance, so
    the useful record is the display. That is one call per fund — the
    largest paced sweep in this module at ~397 calls.
    """
    listing = fetch_all(client, "funit/list", pace_ms=pace_ms)

    gathered = Gathered(
        complete=False,
        expected=listing.expected,
        calls=listing.calls,
        pages=listing.pages,
        error=listing.error,
        notes={"endpoint": "funit/list + funit/display"},
    )
    if not listing.complete:
        return gathered

    fund_ids = []
    for row in listing.records:
        key = _first_key(row, ("funit_id", "id"))
        if key is not None:
            fund_ids.append(key)

    gathered.notes["funds_listed"] = len(listing.records)
    gathered.notes["display_calls_planned"] = len(fund_ids)

    pause = pace_seconds(pace_ms)
    for index, fund_id in enumerate(fund_ids):
        if index:
            # fetch_all paces between its own pages; this sweep is a series
            # of separate single calls, so it paces itself.
            pace_sleep(pause)

        display = fetch_one(client, "funit/display",
                            {"funit_id": _display_id(fund_id)},
                            pace_ms=pace_ms)
        gathered.calls += display.calls

        if not display.complete or not display.records:
            gathered.failed += 1
            gathered.error = (
                f"funit/display failed for fund {fund_id} after "
                f"{len(gathered.rows)} of {len(fund_ids)} funds: "
                f"{display.error or 'empty response'}")
            return gathered

        payload = display.records[0]
        gathered.rows.append(MirrorRow(
            csuite_id=_first_key(payload, ("funit_id",)) or fund_id,
            data=payload,
            # The mirror column is fund_group_id; CSuite's field is
            # fgroup_id (probe #3, funit/display field inventory). Both
            # names are accepted so a rename upstream does not go unnoticed
            # as a column full of NULLs.
            fund_group_id=payload.get("fgroup_id",
                                      payload.get("fund_group_id")),
        ))

    gathered.complete = True
    return gathered


def _display_id(key: str):
    """Pass an id back to CSuite as the type it gave us."""
    try:
        return int(key)
    except (TypeError, ValueError):
        return key


def _gather_profile(client, pace_ms) -> Gathered:
    """Every profile, reduced to the whitelisted fields and nothing else."""
    result = fetch_all(client, "profile/list", pace_ms=pace_ms)

    gathered = Gathered(
        complete=result.complete,
        expected=result.expected,
        calls=result.calls,
        pages=result.pages,
        error=result.error,
        notes={"endpoint": "profile/list"},
    )
    if not result.complete:
        return gathered

    unkeyed = 0
    for row in result.records:
        key = _key(row.get("profile_id"))
        if key is None:
            unkeyed += 1
            continue
        gathered.rows.append(MirrorRow(csuite_id=key,
                                       data=profile_record(row)))

    if unkeyed:
        gathered.notes["unkeyed_rows"] = unkeyed
    gathered.notes["stored_fields"] = list(PROFILE_FIELDS) + ["created_date"]
    return gathered


def profile_record(row: dict) -> dict:
    """A profile reduced to the fields the mirror is allowed to hold.

    Built by naming what is kept, never by removing what is not: a new
    field appearing in CSuite must not silently start being stored.
    """
    record = {name: row.get(name) for name in PROFILE_FIELDS}
    created = None
    for name in PROFILE_CREATED_SOURCES:
        if row.get(name) not in (None, ""):
            created = row.get(name)
            break
    record["created_date"] = created
    return record


def _gather_donation_agg(client, pace_ms) -> Gathered:
    """Donations, aggregated per profile in memory and never stored raw.

    26,500 donation rows go in; roughly one row per giving profile comes
    out. The individual donations are deliberately not mirrored — that is
    the accounting system's job, and a local copy of every gift is a
    liability with no query this assistant needs.
    """
    result = fetch_all(client, "donation/list", pace_ms=pace_ms)

    gathered = Gathered(
        complete=result.complete,
        expected=result.expected,
        calls=result.calls,
        pages=result.pages,
        error=result.error,
        notes={"endpoint": "donation/list"},
    )
    if not result.complete:
        return gathered

    aggregates, dropped = aggregate_donations(result.records)

    for profile_id, record in aggregates.items():
        gathered.rows.append(MirrorRow(csuite_id=profile_id, data=record))

    gathered.notes["donations_read"] = len(result.records)
    gathered.notes["profiles_aggregated"] = len(aggregates)
    if dropped:
        gathered.notes["donations_without_profile"] = dropped
        logger.warning(
            "donation/list: %d donations had no profile_id and are in no "
            "aggregate", dropped)

    return gathered


def aggregate_donations(rows) -> tuple[dict, int]:
    """Per-profile giving totals from raw donation rows.

    Returns (aggregates_by_profile_id, donations_dropped).

    first/latest are decided by donation_date, with donation_id breaking
    ties so the same input always produces the same output. A donation
    with no date still counts toward the total and the count, but can
    never be the first or latest — a missing date is unknown, not oldest.
    """
    working: dict = {}
    dropped = 0

    for row in rows:
        if not isinstance(row, dict):
            dropped += 1
            continue

        profile_id = _key(row.get("profile_id"))
        if profile_id is None:
            dropped += 1
            continue

        amount = _money(row.get("donation_amount"))
        date = _date(row.get("donation_date"))
        fund = row.get("fund_name")
        order = (date, _order_id(row.get("donation_id")))

        agg = working.get(profile_id)
        if agg is None:
            agg = working[profile_id] = {
                "profile_id": profile_id,
                "count": 0,
                "total": Decimal("0"),
                "first": None,
                "latest": None,
                "greatest": None,
            }

        agg["count"] += 1
        agg["total"] += amount

        entry = {"date": date, "amount": amount, "fund": fund, "order": order}

        if date is not None:
            if agg["first"] is None or order < agg["first"]["order"]:
                agg["first"] = entry
            if agg["latest"] is None or order > agg["latest"]["order"]:
                agg["latest"] = entry

        greatest = agg["greatest"]
        if greatest is None or amount > greatest["amount"]:
            agg["greatest"] = entry
        elif amount == greatest["amount"]:
            # Same size twice: the earlier gift is the one to name.
            if date is not None and (greatest["date"] is None
                                     or date < greatest["date"]):
                agg["greatest"] = entry

    aggregates = {}
    for profile_id, agg in working.items():
        first = agg["first"]
        latest = agg["latest"]
        greatest = agg["greatest"]
        aggregates[profile_id] = {
            "profile_id": profile_id,
            "lifetime_total": _money_str(agg["total"]),
            "count": agg["count"],
            "first_date": first["date"] if first else None,
            "first_amount": _money_str(first["amount"]) if first else None,
            "first_fund": first["fund"] if first else None,
            "latest_date": latest["date"] if latest else None,
            "latest_amount": _money_str(latest["amount"]) if latest else None,
            "latest_fund": latest["fund"] if latest else None,
            "greatest_amount": (_money_str(greatest["amount"])
                                if greatest else None),
            "greatest_date": greatest["date"] if greatest else None,
        }

    return aggregates, dropped


# How each record type is gathered. Keyed lookups use the first id field
# present, so an endpoint that returns `id` instead of `<thing>_id` still
# mirrors — funit/list/search already does exactly that.
GATHERERS = {
    "fund": _gather_fund,
    "fee_type": lambda c, p: _gather_list(
        c, "funit/feetype", ("fund_fee_type_id", "id"), p),
    "event": lambda c, p: _gather_list(
        c, "event/list/dates", ("event_date_id", "id"), p),
    "grant": lambda c, p: _gather_list(
        c, "grant/list", ("grant_id", "id"), p),
    "check": lambda c, p: _gather_list(
        c, "check/list", ("check_id", "id"), p),
    "profile": _gather_profile,
    "donation_agg": _gather_donation_agg,
}


# ---------------------------------------------------------------------------
# The run ledger (sync_runs)
# ---------------------------------------------------------------------------

_START_RUN_SQL = """
    INSERT INTO sync_runs (
        sync_type, triggered_by, trigger_source, dry_run, status,
        started_at, notes
    )
    VALUES ('mirror', %s, %s, %s, 'running', NOW(), %s::jsonb)
    RETURNING id
"""


DEFAULT_TRIGGER_LABEL = "cli:mirror_refresh"


def _user_id(value):
    """A users.id for the triggered_by column: an int, or None.

    Rejects anything else loudly. triggered_by is a foreign key, so a
    label like "cli:mirror_refresh" landing here is not a value the
    database can store — and a run that dies on its own ledger INSERT
    fails after the work, not before it.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError(
            f"triggered_by_user_id must be a users.id or None, got {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    raise TypeError(
        f"triggered_by_user_id must be a users.id or None, got {value!r}. "
        "A label describing what started the run belongs in notes.trigger — "
        "pass it as triggered_by.")


def _trigger_label(value):
    """The human-readable label for notes.trigger.

    An int here is almost certainly a user id put in the wrong parameter,
    which would otherwise be silently filed as a label and leave the
    foreign key NULL. Say so instead.
    """
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        raise TypeError(
            f"triggered_by is a label, not a user id (got {value!r}). "
            "Pass a users.id as triggered_by_user_id.")
    return str(value)


def _start_run(triggered_by_user_id, trigger_source, dry_run,
               notes) -> int | None:
    """Open a sync_runs row and return its id.

    Deliberately allowed to raise. Unlike the write audit — which must
    never break the write it is recording — the ledger is the only record
    that a mirror fill happened at all. A fill with no run row is exactly
    the invisible-work problem this table exists to solve, so if the
    ledger cannot be written, the fill does not happen.
    """
    rows = database.execute_query(
        _START_RUN_SQL,
        (_user_id(triggered_by_user_id), trigger_source, bool(dry_run),
         canonical_json(notes or {})),
        fetch=True,
    )
    if not rows:
        raise RuntimeError("sync_runs INSERT returned no id")
    row = rows[0]
    return row["id"] if isinstance(row, dict) else row[0]


# Only the columns a caller actually sets are written, so a mid-run update
# cannot blank a value an earlier stage recorded.
_RUN_COLUMNS = (
    "status", "fetch_finished_at", "finished_at", "expected_count",
    "fetched_count", "fetch_complete", "written_count", "skipped_count",
    "failed_count", "unchanged_count", "error_summary", "notes",
)

_RUN_COLUMN_SQL = {
    "notes": "notes = %s::jsonb",
}


def _update_run(run_id, **values) -> None:
    """Patch a sync_runs row. NOW() for the two timestamp flags."""
    if run_id is None:
        return

    assignments = []
    params = []
    for column in _RUN_COLUMNS:
        if column not in values:
            continue
        value = values[column]
        if column in ("fetch_finished_at", "finished_at") and value is True:
            assignments.append(f"{column} = NOW()")
            continue
        assignments.append(_RUN_COLUMN_SQL.get(column, f"{column} = %s"))
        params.append(canonical_json(value) if column == "notes" else value)

    if not assignments:
        return

    params.append(run_id)
    database.execute_query(
        f"UPDATE sync_runs SET {', '.join(assignments)} WHERE id = %s",
        tuple(params),
        fetch=False,
    )


# ---------------------------------------------------------------------------
# The mirror table
# ---------------------------------------------------------------------------

_EXISTING_SQL = """
    SELECT csuite_id, data_hash
      FROM csuite_mirror
     WHERE record_type = %s
"""

_UPSERT_HEAD = """
    INSERT INTO csuite_mirror (
        record_type, csuite_id, fund_group_id, data, data_hash,
        synced_at, run_id, expires_at
    )
    VALUES
"""

# Two row shapes, because expires_at is computed by Postgres rather than
# bound as a value: NOW() + interval for the types that expire, a literal
# NULL for the ones that do not. Binding a Python datetime instead would
# put this process's clock (and timezone) into the column rather than the
# database's, which is the clock every other timestamp here uses.
_UPSERT_ROW_TTL = "(%s, %s, %s, %s::jsonb, %s, NOW(), %s, NOW() + %s::interval)"
_UPSERT_ROW_FOREVER = "(%s, %s, %s, %s::jsonb, %s, NOW(), %s, NULL)"

_UPSERT_TAIL = """
    ON CONFLICT (record_type, csuite_id) DO UPDATE SET
        fund_group_id = EXCLUDED.fund_group_id,
        data          = EXCLUDED.data,
        data_hash     = EXCLUDED.data_hash,
        synced_at     = EXCLUDED.synced_at,
        run_id        = EXCLUDED.run_id,
        expires_at    = EXCLUDED.expires_at
"""

_DELETE_SQL = """
    DELETE FROM csuite_mirror
     WHERE record_type = %s
       AND csuite_id IN %s
"""


def _deduplicate(rows) -> tuple[list, int]:
    """One row per csuite_id, keeping the last seen. Returns (rows, dropped).

    Not defensive padding: CSuite is live, and a 266-page sweep of
    donation/list takes minutes. A record inserted while the sweep is
    running shifts every later row down by one, which can serve the same
    id on two consecutive pages. Postgres refuses a multi-row INSERT that
    touches the same conflict target twice ("cannot affect row a second
    time"), so an unguarded duplicate does not corrupt the mirror — it
    aborts the whole batch. Collapsing here keeps that from turning a
    routine race into a failed run.
    """
    by_key = {}
    duplicates = 0
    for row in rows:
        if row.csuite_id in by_key:
            duplicates += 1
        by_key[row.csuite_id] = row
    if not duplicates:
        return list(rows), 0
    return list(by_key.values()), duplicates


def _existing_hashes(record_type: str) -> dict:
    """{csuite_id: data_hash} already in the mirror for this type."""
    rows = database.execute_query(_EXISTING_SQL, (record_type,), fetch=True)
    existing = {}
    for row in rows or []:
        if isinstance(row, dict):
            key, value = row.get("csuite_id"), row.get("data_hash")
        else:
            key, value = row[0], row[1]
        key = _key(key)
        if key is not None:
            existing[key] = value
    return existing


def _expires_clause(record_type: str) -> str | None:
    """The interval literal for this type's TTL, or None for no expiry."""
    hours = TTL_HOURS.get(record_type)
    return None if hours is None else f"{hours} hours"


def _upsert(record_type: str, rows, run_id, expires) -> int:
    """Write rows in batches. Returns how many were sent."""
    template = _UPSERT_ROW_TTL if expires else _UPSERT_ROW_FOREVER

    written = 0
    for start in range(0, len(rows), UPSERT_BATCH):
        batch = rows[start:start + UPSERT_BATCH]
        params = []
        for row in batch:
            params.extend((
                record_type,
                row.csuite_id,
                row.fund_group_id,
                canonical_json(row.data),
                _hash(row.data),
                run_id,
            ))
            if expires:
                params.append(expires)
        sql = (_UPSERT_HEAD
               + ", ".join([template] * len(batch))
               + _UPSERT_TAIL)
        database.execute_query(sql, tuple(params), fetch=False)
        written += len(batch)
    return written


def _delete(record_type: str, keys) -> int:
    """Remove mirror rows CSuite no longer has. Returns how many."""
    keys = list(keys)
    deleted = 0
    for start in range(0, len(keys), DELETE_BATCH):
        batch = tuple(keys[start:start + DELETE_BATCH])
        if not batch:
            continue
        database.execute_query(_DELETE_SQL, (record_type, batch), fetch=False)
        deleted += len(batch)
    return deleted


# ---------------------------------------------------------------------------
# One record type, start to finish
# ---------------------------------------------------------------------------

@dataclass
class TypeResult:
    """What the CLI prints, and what the sync_runs row says."""

    record_type: str
    run_id: object = None
    status: str = "failed"
    expected: int | None = None
    fetched: int = 0
    complete: bool = False
    written: int = 0
    unchanged: int = 0
    deleted: int = 0
    failed: int = 0
    calls: int = 0
    pages: int = 0
    seconds: float = 0.0
    error: str | None = None
    notes: dict = field(default_factory=dict)


def refresh_type(record_type: str, client=None, pace_ms=None,
                 dry_run: bool = False, triggered_by=None,
                 trigger_source: str = "cli",
                 triggered_by_user_id=None) -> TypeResult:
    """Fetch one record type and mirror it. Never raises for API failures.

    Database failures DO propagate: a mirror that cannot reach its own
    table has nothing useful to report and should stop the run.

    Args:
        triggered_by_user_id: a users.id, or None. Goes to the
            sync_runs.triggered_by foreign key. CLI runs have no user and
            pass None.
        triggered_by: a label describing what started the run, e.g.
            "cli:mirror_refresh". Goes to notes.trigger and NEVER to the
            triggered_by column, which is a BIGINT foreign key.
    """
    if record_type not in GATHERERS:
        raise ValueError(
            f"unknown record type {record_type!r} — "
            f"expected one of {', '.join(RECORD_TYPES)}")

    started = time.perf_counter()
    client = client or CSuiteClient()

    label = _trigger_label(triggered_by)
    if label is None and trigger_source == "cli":
        label = DEFAULT_TRIGGER_LABEL
    run_notes = {"record_type": record_type}
    if label is not None:
        run_notes["trigger"] = label

    run_id = _start_run(triggered_by_user_id, trigger_source, dry_run,
                        run_notes)
    result = TypeResult(record_type=record_type, run_id=run_id)

    gathered = GATHERERS[record_type](client, pace_ms)

    result.expected = gathered.expected
    result.fetched = len(gathered.rows)
    result.complete = gathered.complete
    result.calls = gathered.calls
    result.pages = gathered.pages
    result.failed = gathered.failed
    result.notes = dict(gathered.notes)
    result.notes.update(run_notes)
    result.notes["calls"] = gathered.calls
    result.notes["pages"] = gathered.pages

    logger.info(
        "mirror %s: %d records from %d CSuite calls (complete=%s)",
        record_type, result.fetched, result.calls, result.complete)

    fetch_seconds = time.perf_counter() - started
    result.notes["fetch_seconds"] = round(fetch_seconds, 2)

    if not gathered.complete:
        result.status = "failed"
        result.error = gathered.error or "fetch did not complete"
        result.seconds = round(time.perf_counter() - started, 2)
        _update_run(
            run_id,
            status="failed",
            fetch_finished_at=True,
            finished_at=True,
            expected_count=result.expected,
            fetched_count=result.fetched,
            fetch_complete=False,
            written_count=0,
            skipped_count=0,
            unchanged_count=0,
            failed_count=max(result.failed, 1),
            error_summary=result.error,
            notes=result.notes,
        )
        logger.error("mirror %s FAILED, nothing written: %s",
                     record_type, result.error)
        return result

    _update_run(
        run_id,
        status="fetched",
        fetch_finished_at=True,
        expected_count=result.expected,
        fetched_count=result.fetched,
        fetch_complete=True,
    )

    rows, duplicates = _deduplicate(gathered.rows)
    if duplicates:
        result.notes["duplicate_ids"] = duplicates
        result.fetched = len(rows)
        logger.warning(
            "mirror %s: %d duplicate ids across pages — keeping the last "
            "copy of each", record_type, duplicates)

    # Compare against what is already stored before writing anything, so an
    # unchanged record costs a hash rather than a row update.
    existing = _existing_hashes(record_type)
    seen = set()
    to_write = []
    for row in rows:
        seen.add(row.csuite_id)
        if existing.get(row.csuite_id) == _hash(row.data):
            result.unchanged += 1
        else:
            to_write.append(row)

    stale = [key for key in existing if key not in seen]
    result.notes["deleted"] = len(stale)
    if stale:
        result.notes["deleted_ids"] = sorted(stale)[:MAX_NOTED_IDS]

    if dry_run:
        result.status = "verified"
        result.deleted = len(stale)
        result.seconds = round(time.perf_counter() - started, 2)
        result.notes["would_write"] = len(to_write)
        result.notes["would_delete"] = len(stale)
        result.notes["dry_run"] = True
        _update_run(
            run_id,
            status="verified",
            finished_at=True,
            written_count=0,
            skipped_count=0,
            unchanged_count=result.unchanged,
            failed_count=0,
            notes=result.notes,
        )
        logger.info(
            "mirror %s dry run: would write %d, %d unchanged, %d to delete",
            record_type, len(to_write), result.unchanged, len(stale))
        return result

    _update_run(run_id, status="verified", notes=result.notes)
    _update_run(run_id, status="writing")

    expires = _expires_clause(record_type)
    try:
        result.written = _upsert(record_type, to_write, run_id, expires)
        result.deleted = _delete(record_type, stale)
    except Exception as e:
        # The database went away mid-write. Re-raised, because a mirror
        # that cannot write has nothing to fall back on — but the run row
        # is closed out first: a row left at 'writing' with no finished_at
        # is indistinguishable from a job still in flight, and would still
        # look that way tomorrow.
        result.status = "aborted"
        result.error = f"{type(e).__name__}: {e}"
        result.seconds = round(time.perf_counter() - started, 2)
        try:
            _update_run(
                run_id,
                status="aborted",
                finished_at=True,
                written_count=result.written,
                error_summary=result.error,
                notes=result.notes,
            )
        except Exception:  # pragma: no cover - the database is already gone
            logger.error("mirror %s aborted and the run row could not be "
                         "closed: %s", record_type, result.error)
        logger.error("mirror %s aborted mid-write: %s",
                     record_type, result.error)
        raise

    result.status = "complete"
    result.seconds = round(time.perf_counter() - started, 2)
    result.notes["write_seconds"] = round(
        result.seconds - fetch_seconds, 2)

    _update_run(
        run_id,
        status="complete",
        finished_at=True,
        written_count=result.written,
        skipped_count=result.deleted,
        unchanged_count=result.unchanged,
        failed_count=0,
        notes=result.notes,
    )

    logger.info(
        "mirror %s complete: %d written, %d unchanged, %d deleted in %.1fs",
        record_type, result.written, result.unchanged, result.deleted,
        result.seconds)
    return result


def refresh(record_types=None, pace_ms=None, dry_run: bool = False,
            client=None, triggered_by=None,
            trigger_source: str = "cli", triggered_by_user_id=None) -> list:
    """Refresh each record type in turn. Returns one TypeResult per type.

    A type that fails does not stop the ones after it — each is its own
    run row and its own transaction, and a rate limit on profiles says
    nothing about whether grants can be fetched. The caller sees every
    result and can decide what a partial success means.
    """
    types = list(record_types) if record_types else list(RECORD_TYPES)

    unknown = [t for t in types if t not in GATHERERS]
    if unknown:
        raise ValueError(
            f"unknown record type(s): {', '.join(unknown)} — "
            f"expected from {', '.join(RECORD_TYPES)}")

    client = client or CSuiteClient()
    results = []
    for record_type in types:
        results.append(refresh_type(
            record_type, client=client, pace_ms=pace_ms, dry_run=dry_run,
            triggered_by=triggered_by, trigger_source=trigger_source,
            triggered_by_user_id=triggered_by_user_id))
    return results
