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

Surviving the rate limit
------------------------
On 2026-09-10 a fund refresh made 666 cumulative calls over a few minutes
and CSuite started refusing everything. 261 funit/display results — 261
calls already paid for — were thrown away, because they were held in
memory until the whole sweep finished.

They are now staged as they arrive. Each display is written to
`sync_staging` the moment it comes back, and the next fund run reuses any
staged row less than 24 hours old instead of calling for it again. A run
that stops halfway therefore costs nothing: the next one picks up where
it left off. The staging rows are deleted once a complete fund fetch has
been written to the mirror, and the 96h expiry sweep catches any orphans.

The other half of the answer is not making the calls at all: `refresh`
takes a CallBudget shared across every record type, so a run can stop at
a number we chose rather than the number CSuite chose.

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
import json
import logging
import time
from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation

from clients import database
from clients.csuite import CSuiteClient
from config import Config
from clients.csuite_fetch import (
    BUDGET_ERROR,
    RATE_LIMITED_ERROR,
    CallBudget,
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
    "donation_fund_quarter",
)

# donation_agg and donation_fund_quarter are two shapes of the same 267-page
# donation/list sweep, so asking for one always produces the other. The
# fetch itself is cached per refresh() call, which is what makes the second
# one free.
COMPANION_TYPES = {"donation_agg": "donation_fund_quarter"}

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

# sync_staging.record_type for one funit/display payload.
STAGED_FUND_DISPLAY = "fund_display"

# How old a staged display may be and still be reused instead of re-fetched.
# A fund's display payload changes when someone edits the fund or a
# transaction posts to it; a day is short enough that a reused row is not
# meaningfully staler than the mirror row it becomes.
STAGED_MAX_AGE = "24 hours"


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


def ramadan_year(donation_date) -> int | None:
    """The Ramadan year a gift falls in, or None if it falls outside one.

    Config.get_ramadan_range keys its ranges by Gregorian year, so a date
    is checked against its own year and the two either side: Ramadan moves
    ~11 days earlier annually and will straddle a New Year within a decade,
    at which point checking only the date's own year would start silently
    dropping gifts.
    """
    date = _date(donation_date)
    if not date or len(date) < 4:
        return None
    try:
        year = int(date[:4])
    except ValueError:
        return None

    for candidate in (year - 1, year, year + 1):
        try:
            start, end = Config.get_ramadan_range(candidate)
        except Exception:  # pragma: no cover - config always returns a pair
            continue
        if start <= date <= end:
            return candidate
    return None


def quarter_of(date_str) -> tuple:
    """(year, quarter) for a YYYY-MM-DD date, or (None, None)."""
    date = _date(date_str)
    if not date or len(date) < 7:
        return None, None
    try:
        year = int(date[:4])
        month = int(date[5:7])
    except ValueError:
        return None, None
    if not 1 <= month <= 12:
        return None, None
    return year, (month - 1) // 3 + 1


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
    first_429_at: str | None = None
    total_429s: int = 0
    reused_staged: int = 0
    # The bare sentinel behind `error`, when there is one. `error` is
    # prose meant for a human reading the ledger and gets wrapped with
    # context ("...after 136 of 397 funds"); this stays comparable.
    stop_reason: str | None = None

    def absorb(self, result) -> None:
        """Fold one FetchResult's call and rate-limit counters in."""
        self.calls += result.calls
        self.total_429s += result.total_429s
        if self.first_429_at is None:
            self.first_429_at = result.first_429_at
        if result.error in (BUDGET_ERROR, RATE_LIMITED_ERROR):
            self.stop_reason = result.error


@dataclass
class GatherContext:
    """Everything a gatherer needs that is not the record type itself.

    Passed as one object rather than four positional arguments so that
    adding the next one — a budget, a run id — does not mean editing every
    gatherer signature again.
    """

    client: object
    pace_ms: object = None
    budget: object = None
    run_id: object = None
    # Endpoint results already fetched during this refresh() call, so two
    # record types built from the same sweep pay for it once. Keyed by
    # endpoint; lives only as long as the refresh call that made it.
    cache: dict = field(default_factory=dict)

    def fetch_shared(self, endpoint: str):
        """fetch_all for an endpoint, reusing this run's result if there is one.

        A cache hit is returned with its call counters zeroed. They belong
        to the run row of the type that actually made the calls; leaving
        them on would bill 267 donation pages twice and make the CLI
        report 534 calls for a 267-call sweep.
        """
        cached = self.cache.get(endpoint)
        if cached is not None:
            logger.info("reusing the %s fetch from earlier in this run "
                        "(%d records, 0 further calls)",
                        endpoint, len(cached.records))
            return replace(cached, calls=0, pages=0, total_429s=0,
                           first_429_at=None)
        result = fetch_all(self.client, endpoint, pace_ms=self.pace_ms,
                           budget=self.budget)
        if result.complete:
            # Only a whole fetch is worth reusing. Caching a partial one
            # would hand the second record type a truncated sweep with no
            # way to tell it apart from a good one.
            self.cache[endpoint] = result
        return result


def _gather_list(ctx: GatherContext, endpoint: str, key_names) -> Gathered:
    """A plain list endpoint: every row, keyed by its id."""
    result = fetch_all(ctx.client, endpoint, pace_ms=ctx.pace_ms,
                       budget=ctx.budget)

    gathered = Gathered(
        complete=result.complete,
        expected=result.expected,
        pages=result.pages,
        error=result.error,
        notes={"endpoint": endpoint},
    )
    gathered.absorb(result)
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


def _gather_fund(ctx: GatherContext) -> Gathered:
    """Every fund, as its funit/display payload.

    funit/list carries six fields and neither fgroup_id nor the balance, so
    the useful record is the display. That is one call per fund — the
    largest paced sweep in this module at ~397 calls, and the one that got
    us rate limited.

    Each display is staged the moment it arrives. A run that stops
    partway — budget, rate limit, anything — leaves those results behind
    for the next run to pick up instead of paying for them twice.
    """
    listing = fetch_all(ctx.client, "funit/list", pace_ms=ctx.pace_ms,
                        budget=ctx.budget)

    gathered = Gathered(
        complete=False,
        expected=listing.expected,
        pages=listing.pages,
        error=listing.error,
        notes={"endpoint": "funit/list + funit/display"},
    )
    gathered.absorb(listing)
    if not listing.complete:
        return gathered

    fund_ids = []
    for row in listing.records:
        key = _first_key(row, ("funit_id", "id"))
        if key is not None:
            fund_ids.append(key)

    staged = _load_staged_displays()
    reusable = [fund_id for fund_id in fund_ids if fund_id in staged]
    to_fetch = [fund_id for fund_id in fund_ids if fund_id not in staged]

    gathered.reused_staged = len(reusable)
    gathered.notes["funds_listed"] = len(listing.records)
    gathered.notes["display_calls_planned"] = len(to_fetch)
    gathered.notes["reused_staged"] = len(reusable)

    logger.info("reused %d staged displays, fetching %d",
                len(reusable), len(to_fetch))

    pause = pace_seconds(ctx.pace_ms)
    called = 0

    for fund_id in fund_ids:
        payload = staged.get(fund_id)

        if payload is None:
            if called:
                # fetch_all paces between its own pages; this sweep is a
                # series of separate single calls, so it paces itself. A
                # reused display costs no call and so earns no pause.
                pace_sleep(pause)

            display = fetch_one(ctx.client, "funit/display",
                                {"funit_id": _display_id(fund_id)},
                                pace_ms=ctx.pace_ms, budget=ctx.budget)
            called += 1
            gathered.absorb(display)

            if not display.complete or not display.records:
                gathered.failed += 1
                reason = display.error or "empty response"
                if reason == BUDGET_ERROR:
                    gathered.error = (
                        f"budget reached after {len(gathered.rows)} of "
                        f"{len(fund_ids)} funds — "
                        f"{called - 1} displays staged for the next run")
                else:
                    gathered.error = (
                        f"funit/display failed for fund {fund_id} after "
                        f"{len(gathered.rows)} of {len(fund_ids)} funds: "
                        f"{reason}")
                logger.warning(
                    "fund sweep stopped: %s. Staged results are kept.",
                    gathered.error)
                return gathered

            payload = display.records[0]
            _stage_display(ctx.run_id, fund_id, payload)

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


def _gather_profile(ctx: GatherContext) -> Gathered:
    """Every profile, reduced to the whitelisted fields and nothing else."""
    result = fetch_all(ctx.client, "profile/list", pace_ms=ctx.pace_ms,
                       budget=ctx.budget)

    gathered = Gathered(
        complete=result.complete,
        expected=result.expected,
        pages=result.pages,
        error=result.error,
        notes={"endpoint": "profile/list"},
    )
    gathered.absorb(result)
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


def _gather_donation_agg(ctx: GatherContext) -> Gathered:
    """Donations, aggregated per profile in memory and never stored raw.

    26,500 donation rows go in; roughly one row per giving profile comes
    out. The individual donations are deliberately not mirrored — that is
    the accounting system's job, and a local copy of every gift is a
    liability with no query this assistant needs.
    """
    result = ctx.fetch_shared("donation/list")

    gathered = Gathered(
        complete=result.complete,
        expected=result.expected,
        pages=result.pages,
        error=result.error,
        notes={"endpoint": "donation/list"},
    )
    gathered.absorb(result)
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

    ramadan_years is the sorted set of Ramadan years the profile gave in,
    per Config.get_ramadan_range.
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
                "ramadan_years": set(),
            }

        agg["count"] += 1
        agg["total"] += amount

        ramadan = ramadan_year(date)
        if ramadan is not None:
            agg["ramadan_years"].add(ramadan)

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
            # Which Ramadans this profile gave in — the whole input to the
            # lapsed-donor report, computed once here rather than by
            # re-reading 26,500 donations every time someone asks.
            "ramadan_years": sorted(agg["ramadan_years"]),
        }

    return aggregates, dropped


def _gather_donation_fund_quarter(ctx: GatherContext) -> Gathered:
    """Donations rolled up to one row per fund per calendar quarter.

    Reads the same donation/list sweep as donation_agg — via
    ctx.fetch_shared, so running both costs one fetch — and rolls it the
    other way: by fund and quarter instead of by profile.

    Carries no profile ids and no donor names. The quarterly report needs
    "what came into this fund in Q3" and nothing more, so this aggregate
    is not donor data at all and has no expiry.
    """
    result = ctx.fetch_shared("donation/list")

    gathered = Gathered(
        complete=result.complete,
        expected=result.expected,
        pages=result.pages,
        error=result.error,
        notes={"endpoint": "donation/list"},
    )
    gathered.absorb(result)
    if not result.complete:
        return gathered

    aggregates, dropped = aggregate_donations_by_fund_quarter(result.records)

    for key, record in aggregates.items():
        gathered.rows.append(MirrorRow(csuite_id=key, data=record))

    gathered.notes["donations_read"] = len(result.records)
    gathered.notes["fund_quarters"] = len(aggregates)
    if dropped:
        gathered.notes["donations_unbucketed"] = dropped
        logger.warning(
            "donation/list: %d donations had no fund or no usable date and "
            "are in no quarter total", dropped)

    return gathered


def aggregate_donations_by_fund_quarter(rows) -> tuple[dict, int]:
    """{"<funit_id>:<YYYY>Q<n>": {...}} from raw donation rows.

    Returns (aggregates, donations_dropped). A donation with no fund id or
    no parseable date is dropped and counted rather than filed under
    "Unknown", which would make a bucket that looks like a fund.
    """
    working: dict = {}
    dropped = 0

    for row in rows:
        if not isinstance(row, dict):
            dropped += 1
            continue

        funit_id = _first_key(row, ("funit_id", "fund_name_link_id"))
        year, quarter = quarter_of(row.get("donation_date"))
        if funit_id is None or year is None:
            dropped += 1
            continue

        key = f"{funit_id}:{year}Q{quarter}"
        bucket = working.get(key)
        if bucket is None:
            bucket = working[key] = {
                "funit_id": funit_id,
                "fund_name": row.get("fund_name"),
                "year": year,
                "quarter": quarter,
                "total": Decimal("0"),
                "count": 0,
            }
        bucket["total"] += _money(row.get("donation_amount"))
        bucket["count"] += 1
        if not bucket["fund_name"] and row.get("fund_name"):
            bucket["fund_name"] = row.get("fund_name")

    return (
        {key: dict(bucket, total=_money_str(bucket["total"]))
         for key, bucket in working.items()},
        dropped,
    )


# How each record type is gathered. Keyed lookups use the first id field
# present, so an endpoint that returns `id` instead of `<thing>_id` still
# mirrors — funit/list/search already does exactly that.
GATHERERS = {
    "fund": _gather_fund,
    "fee_type": lambda ctx: _gather_list(
        ctx, "funit/feetype", ("fund_fee_type_id", "id")),
    "event": lambda ctx: _gather_list(
        ctx, "event/list/dates", ("event_date_id", "id")),
    "grant": lambda ctx: _gather_list(
        ctx, "grant/list", ("grant_id", "id")),
    "check": lambda ctx: _gather_list(
        ctx, "check/list", ("check_id", "id")),
    "profile": _gather_profile,
    "donation_agg": _gather_donation_agg,
    "donation_fund_quarter": _gather_donation_fund_quarter,
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


# ---------------------------------------------------------------------------
# Staging (sync_staging) — results kept across a stopped run
# ---------------------------------------------------------------------------

# Written as one delete + one insert rather than an upsert. An ON CONFLICT
# needs a unique constraint, and reuse is explicitly "any run", so the
# natural key is (record_type, source_id) — which is not necessarily what
# the table is actually constrained on. Delete-then-insert reaches the
# same end state without betting on a constraint name.
_STAGE_DELETE_SQL = """
    DELETE FROM sync_staging
     WHERE record_type = %s
       AND source_id = %s
"""

_STAGE_INSERT_SQL = """
    INSERT INTO sync_staging (
        run_id, record_type, source_id, proposed_values, status
    )
    VALUES (%s, %s, %s, %s::jsonb, 'staged')
"""

# Age comes from the run that staged the row, not from a timestamp on the
# staging row itself: sync_runs.started_at is a column these briefs have
# confirmed, and a run lasts minutes, so it is an accurate proxy.
_STAGED_LOAD_SQL = """
    SELECT s.source_id, s.proposed_values
      FROM sync_staging s
      JOIN sync_runs r ON r.id = s.run_id
     WHERE s.record_type = %s
       AND s.status = 'staged'
       AND r.started_at > NOW() - %s::interval
"""

_STAGE_CLEAR_SQL = """
    DELETE FROM sync_staging
     WHERE record_type = %s
"""


def _stage_display(run_id, fund_id, payload) -> None:
    """Keep one funit/display result so a stopped run does not waste it.

    Failure here is logged and swallowed. Staging is an optimisation: a
    run that cannot stage is slower next time, but a run that dies because
    it could not write a cache row has turned a saving into a liability.
    """
    try:
        database.execute_query(
            _STAGE_DELETE_SQL, (STAGED_FUND_DISPLAY, fund_id), fetch=False)
        database.execute_query(
            _STAGE_INSERT_SQL,
            (run_id, STAGED_FUND_DISPLAY, fund_id, canonical_json(payload)),
            fetch=False,
        )
    except Exception as e:
        logger.warning("could not stage display for fund %s: %s", fund_id, e)


def _load_staged_displays() -> dict:
    """{funit_id: payload} for staged displays younger than STAGED_MAX_AGE.

    Swallows its own failure for the same reason as _stage_display: an
    unreadable cache means a slower run, not a failed one.
    """
    try:
        rows = database.execute_query(
            _STAGED_LOAD_SQL, (STAGED_FUND_DISPLAY, STAGED_MAX_AGE),
            fetch=True)
    except Exception as e:
        logger.warning("could not read staged displays: %s", e)
        return {}

    if not isinstance(rows, (list, tuple)):
        # execute_query returns a rowcount rather than rows when fetch is
        # False; anything but a sequence here means the query did not do
        # what this function assumes, and guessing would be worse.
        logger.warning(
            "staged display query returned %s, not rows — ignoring the cache",
            type(rows).__name__)
        return {}

    staged = {}
    for row in rows:
        if isinstance(row, dict):
            source_id, payload = row.get("source_id"), row.get(
                "proposed_values")
        else:
            source_id, payload = row[0], row[1]

        key = _key(source_id)
        if key is None:
            continue
        # psycopg2 hands back jsonb as a dict; a text column would arrive
        # as a string. Accept both rather than assuming the column type.
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except ValueError:
                continue
        if isinstance(payload, dict):
            staged[key] = payload

    return staged


def _clear_staging(record_type: str = STAGED_FUND_DISPLAY) -> int:
    """Drop staged rows once their contents are safely in the mirror."""
    try:
        return database.execute_query(
            _STAGE_CLEAR_SQL, (record_type,), fetch=False)
    except Exception as e:
        logger.warning("could not clear %s staging rows: %s", record_type, e)
        return 0


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
    first_429_at: str | None = None
    total_429s: int = 0
    reused_staged: int = 0
    stop_reason: str | None = None


def refresh_type(record_type: str, client=None, pace_ms=None,
                 dry_run: bool = False, triggered_by=None,
                 trigger_source: str = "cli",
                 triggered_by_user_id=None, budget=None,
                 cache=None) -> TypeResult:
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
        budget: a CallBudget shared with the rest of the run. When it runs
            out the fetch stops cleanly, marked incomplete with
            "budget reached", and nothing is written.
        cache: endpoint results shared with the rest of the run, so two
            record types built from one sweep fetch it once.
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

    gathered = GATHERERS[record_type](GatherContext(
        client=client, pace_ms=pace_ms, budget=budget, run_id=run_id,
        cache=cache if cache is not None else {}))

    result.expected = gathered.expected
    result.fetched = len(gathered.rows)
    result.complete = gathered.complete
    result.calls = gathered.calls
    result.pages = gathered.pages
    result.failed = gathered.failed
    result.first_429_at = gathered.first_429_at
    result.total_429s = gathered.total_429s
    result.reused_staged = gathered.reused_staged
    result.stop_reason = gathered.stop_reason

    result.notes = dict(gathered.notes)
    result.notes.update(run_notes)
    result.notes["calls"] = gathered.calls
    result.notes["pages"] = gathered.pages
    # On every row, not only the ones that were refused: a run with
    # total_429s of 0 at 600 calls is as much of a data point as one that
    # was refused at 666, and the window is only learnable from both.
    result.notes["first_429_at"] = gathered.first_429_at
    result.notes["total_429s"] = gathered.total_429s
    result.notes["reused_staged"] = gathered.reused_staged
    if gathered.stop_reason:
        result.notes["stop_reason"] = gathered.stop_reason

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

    if record_type == "fund":
        # The displays are in the mirror now, so the staged copies have
        # nothing left to protect. Only after the write, never before.
        cleared = _clear_staging(STAGED_FUND_DISPLAY)
        result.notes["staging_cleared"] = cleared

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


def expand_types(record_types=None) -> list:
    """The types to run, with companions added and original order kept.

    Asking for donation_agg gets donation_fund_quarter too: they are two
    roll-ups of one sweep, and refreshing one without the other leaves the
    quarterly report reading numbers from a different day than the lapsed
    donor report.
    """
    types = list(record_types) if record_types else list(RECORD_TYPES)

    expanded = []
    for record_type in types:
        if record_type not in expanded:
            expanded.append(record_type)
        companion = COMPANION_TYPES.get(record_type)
        if companion and companion not in expanded:
            expanded.append(companion)
    return expanded


def refresh(record_types=None, pace_ms=None, dry_run: bool = False,
            client=None, triggered_by=None,
            trigger_source: str = "cli", triggered_by_user_id=None,
            budget=None) -> list:
    """Refresh each record type in turn. Returns one TypeResult per type.

    A type that fails does not stop the ones after it — each is its own
    run row and its own transaction, and a bad response on profiles says
    nothing about whether grants can be fetched.

    A rate limit is the exception. CSuite's limiter is cumulative over
    minutes and stays shut for at least fifteen seconds, so once it has
    refused us four times through the full backoff there is no reason to
    believe the next record type will fare better — carrying on would
    spend six more sweeps discovering the same thing. The run stops, and
    the types not attempted are returned as 'skipped' so the caller can
    see what did not run rather than inferring it from a short list.

    Args:
        budget: total CSuite calls this run may make, as a CallBudget or a
            plain int. Shared across every record type.
    """
    if budget is not None and not isinstance(budget, CallBudget):
        budget = CallBudget(int(budget))
    types = expand_types(record_types)

    unknown = [t for t in types if t not in GATHERERS]
    if unknown:
        raise ValueError(
            f"unknown record type(s): {', '.join(unknown)} — "
            f"expected from {', '.join(RECORD_TYPES)}")

    client = client or CSuiteClient()
    results = []
    # One cache for the whole run, discarded when it returns. This is what
    # makes donation_fund_quarter cost nothing once donation_agg has run.
    cache: dict = {}

    for index, record_type in enumerate(types):
        result = refresh_type(
            record_type, client=client, pace_ms=pace_ms, dry_run=dry_run,
            triggered_by=triggered_by, trigger_source=trigger_source,
            triggered_by_user_id=triggered_by_user_id, budget=budget,
            cache=cache)
        results.append(result)

        if result.stop_reason in (RATE_LIMITED_ERROR, BUDGET_ERROR):
            remaining = types[index + 1:]
            if remaining:
                logger.warning(
                    "stopping after %s (%s) — not attempting %s",
                    record_type, result.stop_reason, ", ".join(remaining))
                results.extend(
                    TypeResult(record_type=name, status="skipped",
                               error=f"not attempted: {result.stop_reason} on "
                                     f"{record_type}",
                               stop_reason=result.stop_reason)
                    for name in remaining)
            break

    return results
