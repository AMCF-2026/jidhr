"""Step 3a: CSuite mirror fill.

No network and no database. The CSuite client is a stub whose `_request`
returns canned pages; clients.database.execute_query is a fake that keeps
the SQL and params it was handed so tests can assert on what would have
been written.

Every fixture here is invented. No live donor data is used as a test
fixture, ever — see tests/test_probe_masking.py for why that rule exists.
"""

import json
import re

import pytest

from clients import csuite_fetch
from clients.csuite_fetch import (
    FetchResult,
    fetch_all,
    fetch_one,
    looks_rate_limited,
)
from sync import mirror


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------

class StubClient:
    """Stands in for CSuiteClient at the _request seam.

    `pages` maps an endpoint to a list of responses, served in order. A
    callable is invoked with the request dict so a test can vary by offset.
    """

    def __init__(self, pages=None):
        self.pages = pages or {}
        self.calls = []

    def _request(self, endpoint, data=None):
        self.calls.append((endpoint, dict(data or {})))
        responses = self.pages.get(endpoint)
        if responses is None:
            return {"error": f"no stub for {endpoint}"}
        if callable(responses):
            return responses(dict(data or {}))
        index = sum(1 for e, _ in self.calls if e == endpoint) - 1
        if index >= len(responses):
            return ok([])
        return responses[index]


def ok(results, count=None):
    """A successful CSuite envelope."""
    data = {"results": list(results)}
    if count is not None:
        data["count"] = count
    return {"success": True, "data": data, "messages": []}


def ok_object(payload):
    """A successful display-shaped envelope: the object sits at data."""
    return {"success": True, "data": dict(payload), "messages": []}


def fail(error):
    return {"success": False, "error": error, "errors": [error]}


class FakeDB:
    """Stands in for clients.database.execute_query.

    Records every statement. SELECTs are answered from `stored`, a
    {(record_type, csuite_id): data_hash} map representing the mirror as it
    already exists.
    """

    def __init__(self, stored=None, next_run_id=1000, staged=None):
        self.statements = []
        self.stored = dict(stored or {})
        self.next_run_id = next_run_id
        # {(record_type, source_id): payload dict} — sync_staging rows that
        # are already there and young enough to reuse.
        self.staged = dict(staged or {})

    def __call__(self, sql, params=None, fetch=True):
        collapsed = " ".join(str(sql).split())
        self.statements.append((collapsed, tuple(params or ())))

        if collapsed.startswith("INSERT INTO sync_runs"):
            run_id = self.next_run_id
            self.next_run_id += 1
            return [{"id": run_id}]

        if collapsed.startswith("SELECT csuite_id, data_hash"):
            record_type = params[0]
            return [
                {"csuite_id": csuite_id, "data_hash": data_hash}
                for (rtype, csuite_id), data_hash in self.stored.items()
                if rtype == record_type
            ]

        if collapsed.startswith("SELECT s.source_id, s.proposed_values"):
            record_type = params[0]
            return [
                {"source_id": source_id, "proposed_values": payload}
                for (rtype, source_id), payload in self.staged.items()
                if rtype == record_type
            ]

        if collapsed.startswith("INSERT INTO sync_staging"):
            _, record_type, source_id, payload = params
            self.staged[(record_type, source_id)] = json.loads(payload)
            return 1

        if collapsed.startswith("DELETE FROM sync_staging"):
            record_type = params[0]
            source_id = params[1] if len(params) > 1 else None
            for key in list(self.staged):
                if key[0] != record_type:
                    continue
                if source_id is None or key[1] == source_id:
                    del self.staged[key]
            return 1

        return 1

    # -- convenience views over what was recorded --------------------------

    def matching(self, prefix):
        return [(sql, params) for sql, params in self.statements
                if sql.startswith(prefix)]

    @property
    def upserts(self):
        return self.matching("INSERT INTO csuite_mirror")

    @property
    def deletes(self):
        return self.matching("DELETE FROM csuite_mirror")

    @property
    def run_inserts(self):
        return self.matching("INSERT INTO sync_runs")

    @property
    def staging_inserts(self):
        return self.matching("INSERT INTO sync_staging")

    @property
    def staging_deletes(self):
        return self.matching("DELETE FROM sync_staging")

    @property
    def staging_clears(self):
        """Deletes that target a whole record_type, not a single row."""
        return [(sql, params) for sql, params in self.staging_deletes
                if len(params) == 1]

    @property
    def run_updates(self):
        return self.matching("UPDATE sync_runs")

    def run_field(self, name):
        """The last value written to a sync_runs column, or None."""
        for sql, params in reversed(self.run_updates):
            columns = re.findall(r"(\w+) = (?:%s|NOW\(\))", sql)
            placeholders = [c for c in columns
                            if f"{c} = NOW()" not in sql]
            if name not in placeholders:
                continue
            return params[placeholders.index(name)]
        return None

    def upserted_rows(self):
        """Every (csuite_id, data, fund_group_id) actually sent."""
        rows = []
        for sql, params in self.upserts:
            expiring = "interval" in sql
            width = 7 if expiring else 6
            for start in range(0, len(params), width):
                chunk = params[start:start + width]
                rows.append({
                    "record_type": chunk[0],
                    "csuite_id": chunk[1],
                    "fund_group_id": chunk[2],
                    "data": json.loads(chunk[3]),
                    "data_hash": chunk[4],
                    "run_id": chunk[5],
                    "expires": chunk[6] if expiring else None,
                })
        return rows


@pytest.fixture(autouse=True)
def no_pacing(monkeypatch):
    """Tests must not actually sleep — including the 5s rate-limit wait."""
    monkeypatch.setattr(csuite_fetch, "pace_sleep", lambda seconds: None)
    monkeypatch.setattr(mirror, "pace_sleep", lambda seconds: None)


@pytest.fixture
def db(monkeypatch):
    fake = FakeDB()
    monkeypatch.setattr("clients.database.execute_query", fake)
    return fake


# ---------------------------------------------------------------------------
# fetch_all: pagination stops on an empty page
# ---------------------------------------------------------------------------

def test_pagination_stops_on_empty_page():
    client = StubClient({
        "grant/list": [
            ok([{"grant_id": i} for i in range(100)], count=250),
            ok([{"grant_id": i} for i in range(100, 200)]),
            ok([{"grant_id": i} for i in range(200, 250)]),
            ok([]),
        ]
    })

    result = fetch_all(client, "grant/list", pace_ms=0)

    assert result.complete is True
    assert len(result.records) == 250
    assert result.pages == 4
    assert result.expected == 250


def test_pagination_advances_by_rows_returned_not_by_requested_limit():
    """CSuite ignores view_limit on most endpoints, so the offset must
    follow what actually came back."""
    client = StubClient({
        "profile/list": [
            ok([{"profile_id": i} for i in range(100)], count=150),
            ok([{"profile_id": i} for i in range(100, 150)]),
            ok([]),
        ]
    })

    result = fetch_all(client, "profile/list", pace_ms=0)

    offsets = [data["view_offset"] for endpoint, data in client.calls]
    assert offsets == [0, 100, 150]
    assert result.complete is True
    assert len(result.records) == 150


def test_short_first_page_is_not_treated_as_the_end():
    """funit/list honours view_limit=500 and returns 397 — one short page
    that is nonetheless followed by a real empty-page check."""
    client = StubClient({
        "funit/list": [
            ok([{"funit_id": i} for i in range(397)], count=397),
            ok([]),
        ]
    })

    result = fetch_all(client, "funit/list", pace_ms=0)

    assert result.complete is True
    assert len(result.records) == 397
    assert client.calls[0][1]["view_limit"] == 500


def test_endpoint_without_offset_is_fetched_once():
    """event/list/dates re-serves page one for any offset. Paging it would
    duplicate every row forever."""
    client = StubClient({
        "event/list/dates": lambda data: ok(
            [{"event_date_id": i} for i in range(176)], count=176)
    })

    result = fetch_all(client, "event/list/dates", pace_ms=0)

    assert result.complete is True
    assert len(result.records) == 176
    assert len(client.calls) == 1
    assert "view_offset" not in client.calls[0][1]


def test_max_pages_stops_and_reports_incomplete():
    client = StubClient({
        "grant/list": lambda data: ok([{"grant_id": 1}])
    })

    result = fetch_all(client, "grant/list", pace_ms=0, max_pages=5)

    assert result.complete is False
    assert "never returned an empty page" in result.error
    assert result.pages == 5


def test_mid_sweep_error_returns_incomplete_with_the_error():
    client = StubClient({
        "check/list": [
            ok([{"check_id": 1}], count=3),
            fail("connection reset"),
        ]
    })

    result = fetch_all(client, "check/list", pace_ms=0)

    assert result.complete is False
    assert result.error == "connection reset"
    assert len(result.records) == 1


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status, error", [
    (429, None),
    (None, "Invalid JSON response: HTTP 429 Too Many Requests"),
    (None, "rate limit exceeded"),
    (None, "Rate-Limited"),
    (None, "too many requests"),
])
def test_rate_limit_is_recognised(status, error):
    assert looks_rate_limited(status, error) is True


@pytest.mark.parametrize("status, error", [
    (200, None),
    (500, "internal error"),
    (None, "profile 4291 not found"),
    (None, None),
])
def test_non_rate_limit_errors_are_not_mistaken_for_one(status, error):
    assert looks_rate_limited(status, error) is False


def test_one_429_is_retried_once_and_succeeds():
    responses = [fail("HTTP 429 Too Many Requests"),
                 ok([{"grant_id": 1}], count=1),
                 ok([])]
    client = StubClient({"grant/list": responses})

    result = fetch_all(client, "grant/list", pace_ms=0)

    assert result.complete is True
    assert len(result.records) == 1
    assert result.calls == 3


def test_a_wall_of_429s_stops_the_fetch_after_three_backoffs():
    client = StubClient({
        "grant/list": lambda data: fail("HTTP 429 Too Many Requests")
    })

    result = fetch_all(client, "grant/list", pace_ms=0)

    assert result.complete is False
    assert result.error == "rate limited"
    # One original call plus one per backoff in RATE_LIMIT_BACKOFFS.
    assert result.calls == len(csuite_fetch.RATE_LIMIT_BACKOFFS) + 1 == 4
    assert result.total_429s == 4
    assert result.first_429_at is not None


def test_sustained_429s_write_nothing_and_fail_the_run(db):
    client = StubClient({
        "grant/list": lambda data: fail("HTTP 429 Too Many Requests")
    })

    result = mirror.refresh_type("grant", client=client, pace_ms=0)

    assert result.status == "failed"
    assert result.complete is False
    assert result.error == "rate limited"
    assert db.upserts == [], "a rate-limited fetch must write no mirror rows"
    assert db.deletes == [], "a rate-limited fetch must delete nothing"
    assert db.run_inserts, "the run must still be recorded"
    assert db.run_field("status") == "failed"
    assert db.run_field("error_summary") == "rate limited"
    assert db.run_field("fetch_complete") is False


def test_status_tap_reads_429_from_the_http_response():
    """The client's dict has no status code in it, so the tap on
    session.post is what makes a bare 429 visible."""

    class Response:
        status_code = 429

        def json(self):
            return {"success": 0, "errors": ["slow down"]}

    class Session:
        def post(self, *args, **kwargs):
            return Response()

    class TappedClient(StubClient):
        def __init__(self):
            super().__init__({})
            self.session = Session()

        def _request(self, endpoint, data=None):
            self.calls.append((endpoint, dict(data or {})))
            self.session.post("https://example.invalid")
            return {"success": False, "error": "slow down", "errors": []}

    client = TappedClient()

    result = fetch_all(client, "grant/list", pace_ms=0)

    assert result.complete is False
    assert result.error == "rate limited"
    assert "post" not in vars(client.session), "the tap must be removed again"
    assert client.session.post.__func__ is Session.post


# ---------------------------------------------------------------------------
# Hashing: unchanged rows are not rewritten
# ---------------------------------------------------------------------------

def test_unchanged_rows_count_as_unchanged_and_are_not_written(db):
    rows = [{"grant_id": 1, "grant_amount": "100.00"},
            {"grant_id": 2, "grant_amount": "250.00"}]
    client = StubClient({"grant/list": [ok(rows, count=2), ok([])]})

    # Pre-store grant 1 with exactly the hash it will produce this run.
    db.stored[("grant", "1")] = mirror._hash(rows[0])

    result = mirror.refresh_type("grant", client=client, pace_ms=0)

    assert result.status == "complete"
    assert result.unchanged == 1
    assert result.written == 1
    written_ids = [row["csuite_id"] for row in db.upserted_rows()]
    assert written_ids == ["2"]
    assert db.run_field("unchanged_count") == 1
    assert db.run_field("written_count") == 1


def test_changed_row_is_rewritten(db):
    rows = [{"grant_id": 1, "grant_amount": "100.00"}]
    client = StubClient({"grant/list": [ok(rows, count=1), ok([])]})
    db.stored[("grant", "1")] = "a-hash-from-an-older-payload"

    result = mirror.refresh_type("grant", client=client, pace_ms=0)

    assert result.written == 1
    assert result.unchanged == 0


def test_hash_is_stable_across_key_order():
    a = {"grant_id": 1, "amount": "5.00", "fund": "X"}
    b = {"fund": "X", "amount": "5.00", "grant_id": 1}
    assert mirror._hash(a) == mirror._hash(b)


# ---------------------------------------------------------------------------
# Deletion of records CSuite no longer has
# ---------------------------------------------------------------------------

def test_missing_row_is_deleted_and_counted_as_skipped(db):
    rows = [{"check_id": 1}, {"check_id": 2}]
    client = StubClient({"check/list": [ok(rows, count=2), ok([])]})

    db.stored[("check", "1")] = mirror._hash(rows[0])
    db.stored[("check", "2")] = mirror._hash(rows[1])
    db.stored[("check", "999")] = "hash-of-a-check-csuite-no-longer-has"

    result = mirror.refresh_type("check", client=client, pace_ms=0)

    assert result.deleted == 1
    assert db.run_field("skipped_count") == 1

    assert len(db.deletes) == 1
    sql, params = db.deletes[0]
    assert params[0] == "check"
    assert params[1] == ("999",)

    assert result.notes["deleted"] == 1
    assert result.notes["deleted_ids"] == ["999"]


def test_nothing_is_deleted_when_a_fetch_is_incomplete(db):
    client = StubClient({
        "check/list": [ok([{"check_id": 1}], count=2), fail("timeout")]
    })
    db.stored[("check", "999")] = "still-here"

    result = mirror.refresh_type("check", client=client, pace_ms=0)

    assert result.status == "failed"
    assert db.deletes == []


def test_a_type_with_no_stale_rows_issues_no_delete(db):
    rows = [{"check_id": 1}]
    client = StubClient({"check/list": [ok(rows, count=1), ok([])]})
    db.stored[("check", "1")] = mirror._hash(rows[0])

    mirror.refresh_type("check", client=client, pace_ms=0)

    assert db.deletes == []


# ---------------------------------------------------------------------------
# Donation aggregation
# ---------------------------------------------------------------------------

# Six invented donations across two profiles.
DONATIONS = [
    {"donation_id": 1, "profile_id": 7001, "donation_amount": "100.00",
     "donation_date": "2023-01-15", "fund_name": "Alpha Fund-(DAF0001)"},
    {"donation_id": 2, "profile_id": 7001, "donation_amount": "2500.00",
     "donation_date": "2024-06-01", "fund_name": "Beta Fund-(DAF0002)"},
    {"donation_id": 3, "profile_id": 7001, "donation_amount": "75.50",
     "donation_date": "2025-03-09", "fund_name": "Alpha Fund-(DAF0001)"},
    {"donation_id": 4, "profile_id": 7002, "donation_amount": "40.00",
     "donation_date": "2022-11-30", "fund_name": "Gamma Fund-(END0003)"},
    {"donation_id": 5, "profile_id": 7002, "donation_amount": "40.00",
     "donation_date": "2024-02-02", "fund_name": "Gamma Fund-(END0003)"},
    {"donation_id": 6, "profile_id": 7002, "donation_amount": "1000.25",
     "donation_date": "2023-07-04", "fund_name": "Delta Fund-(DAF0004)"},
]


def test_donation_aggregation_math():
    aggregates, dropped = mirror.aggregate_donations(DONATIONS)

    assert dropped == 0
    assert set(aggregates) == {"7001", "7002"}

    first_donor = aggregates["7001"]
    assert first_donor["count"] == 3
    assert first_donor["lifetime_total"] == "2675.50"
    assert first_donor["first_date"] == "2023-01-15"
    assert first_donor["first_amount"] == "100.00"
    assert first_donor["first_fund"] == "Alpha Fund-(DAF0001)"
    assert first_donor["latest_date"] == "2025-03-09"
    assert first_donor["latest_amount"] == "75.50"
    assert first_donor["latest_fund"] == "Alpha Fund-(DAF0001)"
    assert first_donor["greatest_amount"] == "2500.00"
    assert first_donor["greatest_date"] == "2024-06-01"

    second_donor = aggregates["7002"]
    assert second_donor["count"] == 3
    assert second_donor["lifetime_total"] == "1080.25"
    assert second_donor["first_date"] == "2022-11-30"
    assert second_donor["latest_date"] == "2024-02-02"
    assert second_donor["greatest_amount"] == "1000.25"
    assert second_donor["greatest_date"] == "2023-07-04"


def test_donation_aggregation_is_order_independent():
    forwards, _ = mirror.aggregate_donations(DONATIONS)
    backwards, _ = mirror.aggregate_donations(list(reversed(DONATIONS)))
    assert forwards == backwards


def test_equal_greatest_amounts_name_the_earlier_gift():
    rows = [
        {"donation_id": 1, "profile_id": 8001, "donation_amount": "500.00",
         "donation_date": "2024-05-05", "fund_name": "A"},
        {"donation_id": 2, "profile_id": 8001, "donation_amount": "500.00",
         "donation_date": "2021-01-01", "fund_name": "B"},
    ]
    aggregates, _ = mirror.aggregate_donations(rows)
    assert aggregates["8001"]["greatest_date"] == "2021-01-01"


def test_a_donation_with_no_date_counts_but_is_never_first_or_latest():
    rows = [
        {"donation_id": 1, "profile_id": 8002, "donation_amount": "10.00",
         "donation_date": None, "fund_name": "A"},
        {"donation_id": 2, "profile_id": 8002, "donation_amount": "20.00",
         "donation_date": "2024-01-01", "fund_name": "B"},
    ]
    aggregates, _ = mirror.aggregate_donations(rows)
    agg = aggregates["8002"]

    assert agg["count"] == 2
    assert agg["lifetime_total"] == "30.00"
    assert agg["first_date"] == "2024-01-01"
    assert agg["latest_date"] == "2024-01-01"


def test_donations_without_a_profile_are_dropped_and_counted():
    rows = list(DONATIONS) + [
        {"donation_id": 9, "profile_id": None, "donation_amount": "5.00",
         "donation_date": "2024-01-01"},
    ]
    aggregates, dropped = mirror.aggregate_donations(rows)
    assert dropped == 1
    assert set(aggregates) == {"7001", "7002"}


def test_money_parsing_survives_formatting_and_junk():
    assert mirror._money("1,234.56") == mirror._money("1234.56")
    assert mirror._money("$99.00") == mirror._money("99.00")
    assert mirror._money("(50.00)") == mirror._money("-50.00")
    assert mirror._money(None) == mirror._money("0")
    assert mirror._money("not a number") == mirror._money("0")


def test_donation_agg_stores_one_row_per_profile_and_no_raw_donations(db):
    client = StubClient({
        "donation/list": [ok(DONATIONS, count=6), ok([])]
    })

    result = mirror.refresh_type("donation_agg", client=client, pace_ms=0)

    assert result.status == "complete"
    assert result.fetched == 2
    assert result.written == 2

    rows = db.upserted_rows()
    assert {row["csuite_id"] for row in rows} == {"7001", "7002"}
    for row in rows:
        assert "donation_id" not in row["data"], (
            "an individual donation must never reach the mirror")
        assert set(row["data"]) == {
            "profile_id", "lifetime_total", "count",
            "first_date", "first_amount", "first_fund",
            "latest_date", "latest_amount", "latest_fund",
            "greatest_amount", "greatest_date",
        }


# ---------------------------------------------------------------------------
# Profile field whitelist
# ---------------------------------------------------------------------------

PROFILE_ROW = {
    "profile_id": 4242,
    "ptype": "individual",
    "name": "Testcase, Aisha",
    "first_name": "Aisha",
    "last_name": "Testcase",
    "organization": None,
    "primary_email": "aisha@example.invalid",
    "primary_address_string": "1 Example Way, Springfield IL 62701",
    "dead": 0,
    "created_ts": "2021-04-02 09:15:00.000000",
    # None of the below may reach the mirror.
    "primary_phone_number": "415-555-0142",
    "website": "https://example.invalid",
    "work_name": "Example Corp",
    "work_title": "Director",
    "middle_name": "Q",
    "name_link_id": 4242,
    "cf_profile_1001": "a custom field",
}

FORBIDDEN_PROFILE_FIELDS = (
    "primary_phone_number", "website", "work_name", "work_title",
    "middle_name", "name_link_id", "cf_profile_1001", "created_ts",
)


def test_profile_record_keeps_only_whitelisted_fields():
    record = mirror.profile_record(PROFILE_ROW)

    assert set(record) == set(mirror.PROFILE_FIELDS) | {"created_date"}
    for field_name in FORBIDDEN_PROFILE_FIELDS:
        assert field_name not in record, f"{field_name} leaked into the mirror"


def test_profile_created_date_falls_back_to_created_ts():
    """The brief names created_date; the live API field is created_ts."""
    assert mirror.profile_record(PROFILE_ROW)["created_date"] == \
        "2021-04-02 09:15:00.000000"

    explicit = dict(PROFILE_ROW, created_date="2020-01-01")
    assert mirror.profile_record(explicit)["created_date"] == "2020-01-01"


def test_an_unlisted_field_never_reaches_the_row(db):
    client = StubClient({"profile/list": [ok([PROFILE_ROW], count=1), ok([])]})

    mirror.refresh_type("profile", client=client, pace_ms=0)

    rows = db.upserted_rows()
    assert len(rows) == 1
    stored = rows[0]["data"]
    for field_name in FORBIDDEN_PROFILE_FIELDS:
        assert field_name not in stored

    # And not merely absent from the parsed dict — absent from the JSON.
    raw = db.upserts[0][1][3]
    assert "415-555-0142" not in raw
    assert "primary_phone_number" not in raw


def test_a_new_csuite_field_is_not_mirrored_by_default():
    """Whitelist, not blacklist: a field CSuite adds tomorrow is dropped
    until someone chooses to store it."""
    record = mirror.profile_record(
        dict(PROFILE_ROW, brand_new_pii_field="something sensitive"))
    assert "brand_new_pii_field" not in record


# ---------------------------------------------------------------------------
# Funds: list then display, and fund_group_id
# ---------------------------------------------------------------------------

FUND_LIST = [{"funit_id": 1000, "fund_name": "Alpha Fund-(DAF0001)"},
             {"funit_id": 1001, "fund_name": "Beta Fund-(END0002)"}]

FUND_DISPLAYS = {
    1000: {"funit_id": 1000, "fgroup_id": 1002,
           "fund_name": "Alpha Fund-(DAF0001)",
           "current_fundbalance": "12345.67"},
    1001: {"funit_id": 1001, "fgroup_id": 1008,
           "fund_name": "Beta Fund-(END0002)",
           "current_fundbalance": "500.00"},
}


def fund_client():
    return StubClient({
        "funit/list": [ok(FUND_LIST, count=2), ok([])],
        "funit/display": lambda data: ok_object(
            FUND_DISPLAYS[data["funit_id"]]),
    })


def test_fund_mirrors_the_display_payload_with_fund_group_id(db):
    client = fund_client()

    result = mirror.refresh_type("fund", client=client, pace_ms=0)

    assert result.status == "complete"
    assert result.fetched == 2

    rows = {row["csuite_id"]: row for row in db.upserted_rows()}
    assert set(rows) == {"1000", "1001"}
    assert rows["1000"]["fund_group_id"] == 1002
    assert rows["1001"]["fund_group_id"] == 1008
    # The display payload, not the list row: the balance is display-only.
    assert rows["1000"]["data"]["current_fundbalance"] == "12345.67"


def test_fund_calls_display_once_per_fund(db):
    client = fund_client()
    mirror.refresh_type("fund", client=client, pace_ms=0)

    displays = [data for endpoint, data in client.calls
                if endpoint == "funit/display"]
    assert [d["funit_id"] for d in displays] == [1000, 1001]


def test_one_failed_display_fails_the_whole_fund_run(db):
    def display(data):
        if data["funit_id"] == 1001:
            return fail("fund not found")
        return ok_object(FUND_DISPLAYS[1000])

    client = StubClient({
        "funit/list": [ok(FUND_LIST, count=2), ok([])],
        "funit/display": display,
    })

    result = mirror.refresh_type("fund", client=client, pace_ms=0)

    assert result.status == "failed"
    assert "funit/display failed for fund 1001" in result.error
    assert db.upserts == [], "a half-swept fund set must not be written"


# ---------------------------------------------------------------------------
# Keys, TTLs and the run ledger
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("record_type, endpoint, row, expected_key", [
    ("fee_type", "funit/feetype", {"fund_fee_type_id": 1000}, "1000"),
    ("event", "event/list/dates", {"event_date_id": 1396}, "1396"),
    ("grant", "grant/list", {"grant_id": 7974}, "7974"),
    ("check", "check/list", {"check_id": 7898}, "7898"),
])
def test_each_type_keys_on_its_own_id(db, record_type, endpoint, row,
                                      expected_key):
    pages = [ok([row], count=1)]
    if csuite_fetch.ENDPOINT_CONTRACTS.get(
            endpoint, csuite_fetch.DEFAULT_CONTRACT).paginate:
        pages.append(ok([]))
    client = StubClient({endpoint: pages})

    mirror.refresh_type(record_type, client=client, pace_ms=0)

    assert [r["csuite_id"] for r in db.upserted_rows()] == [expected_key]


def test_a_row_with_id_instead_of_typed_id_still_keys(db):
    client = StubClient({"grant/list": [ok([{"id": 55}], count=1), ok([])]})
    mirror.refresh_type("grant", client=client, pace_ms=0)
    assert [r["csuite_id"] for r in db.upserted_rows()] == ["55"]


@pytest.mark.parametrize("record_type", ["profile", "donation_agg"])
def test_donor_derived_types_expire(record_type):
    assert mirror._expires_clause(record_type) == "96 hours"


@pytest.mark.parametrize("record_type",
                         ["fund", "fee_type", "event", "grant", "check"])
def test_reference_types_never_expire(record_type):
    assert mirror._expires_clause(record_type) is None


def test_expiring_types_bind_an_interval_and_others_bind_null(db):
    client = StubClient({"profile/list": [ok([PROFILE_ROW], count=1), ok([])]})
    mirror.refresh_type("profile", client=client, pace_ms=0)
    sql, params = db.upserts[0]
    assert "NOW() + %s::interval" in sql
    assert params[-1] == "96 hours"

    other = FakeDB()
    client = StubClient({"grant/list": [ok([{"grant_id": 1}], count=1),
                                        ok([])]})
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("clients.database.execute_query", other)
        mirror.refresh_type("grant", client=client, pace_ms=0)
    sql, _ = other.upserts[0]
    assert "expires_at" in sql
    assert "NULL)" in sql


def test_the_run_row_records_counts_and_the_record_type(db):
    rows = [{"grant_id": 1}, {"grant_id": 2}]
    client = StubClient({"grant/list": [ok(rows, count=2), ok([])]})

    result = mirror.refresh_type("grant", client=client, pace_ms=0)

    insert_sql, insert_params = db.run_inserts[0]
    assert "sync_type, triggered_by, trigger_source, dry_run" in insert_sql
    assert "'mirror'" in insert_sql
    assert json.loads(insert_params[3])["record_type"] == "grant"
    assert insert_params[0] is None, "a CLI run has no user"
    assert insert_params[1] == "cli"

    assert db.run_field("status") == "complete"
    assert db.run_field("expected_count") == 2
    assert db.run_field("fetched_count") == 2
    assert db.run_field("written_count") == 2
    assert db.run_field("failed_count") == 0
    assert result.run_id is not None


def test_run_rows_are_one_per_record_type(db):
    client = StubClient({
        "grant/list": [ok([{"grant_id": 1}], count=1), ok([])],
        "check/list": [ok([{"check_id": 2}], count=1), ok([])],
    })

    results = mirror.refresh(record_types=["grant", "check"], client=client,
                             pace_ms=0)

    assert len(results) == 2
    assert len(db.run_inserts) == 2


def test_an_ordinary_failure_does_not_stop_the_next_type(db):
    """A malformed response on grants says nothing about checks."""
    client = StubClient({
        "grant/list": lambda data: fail("unexpected server error"),
        "check/list": [ok([{"check_id": 2}], count=1), ok([])],
    })

    results = mirror.refresh(record_types=["grant", "check"], client=client,
                             pace_ms=0)

    assert [r.status for r in results] == ["failed", "complete"]


def test_a_rate_limit_stops_the_whole_run(db):
    """CSuite's limiter is cumulative over minutes. Once it has refused us
    through the full backoff, the next six sweeps will find it shut too."""
    client = StubClient({
        "grant/list": lambda data: fail("HTTP 429 Too Many Requests"),
        "check/list": [ok([{"check_id": 2}], count=1), ok([])],
    })

    results = mirror.refresh(record_types=["grant", "check", "profile"],
                             client=client, pace_ms=0)

    assert [r.record_type for r in results] == ["grant", "check", "profile"]
    assert [r.status for r in results] == ["failed", "skipped", "skipped"]
    assert "rate limited" in results[1].error
    # Nothing was attempted for the skipped types.
    assert not [e for e, _ in client.calls if e == "check/list"]
    assert len(db.run_inserts) == 1, "a skipped type gets no run row"


def test_unknown_record_type_is_rejected():
    with pytest.raises(ValueError, match="unknown record type"):
        mirror.refresh(record_types=["funds"], client=StubClient())

    with pytest.raises(ValueError, match="unknown record type"):
        mirror.refresh_type("nope", client=StubClient())


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def test_dry_run_writes_no_mirror_rows_but_records_the_run(db):
    rows = [{"grant_id": 1}, {"grant_id": 2}]
    client = StubClient({"grant/list": [ok(rows, count=2), ok([])]})
    db.stored[("grant", "1")] = mirror._hash(rows[0])
    db.stored[("grant", "999")] = "stale"

    result = mirror.refresh_type("grant", client=client, pace_ms=0,
                                 dry_run=True)

    assert result.status == "verified"
    assert result.written == 0
    assert db.upserts == []
    assert db.deletes == []

    insert_sql, insert_params = db.run_inserts[0]
    assert insert_params[2] is True, "dry_run must be recorded on the row"
    assert result.notes["would_write"] == 1
    assert result.notes["would_delete"] == 1
    assert result.unchanged == 1


def test_dry_run_still_reads_the_existing_hashes(db):
    client = StubClient({"grant/list": [ok([{"grant_id": 1}], count=1),
                                        ok([])]})
    mirror.refresh_type("grant", client=client, pace_ms=0, dry_run=True)
    assert db.matching("SELECT csuite_id, data_hash")


# ---------------------------------------------------------------------------
# fetch_one and misc contract
# ---------------------------------------------------------------------------

def test_fetch_one_returns_the_display_object():
    client = StubClient({
        "funit/display": lambda data: ok_object({"funit_id": 7, "x": 1})
    })
    result = fetch_one(client, "funit/display", {"funit_id": 7}, pace_ms=0)

    assert result.complete is True
    assert result.records == [{"funit_id": 7, "x": 1}]


def test_fetch_one_reports_failure_rather_than_an_empty_record():
    client = StubClient({"funit/display": lambda data: fail("no such fund")})
    result = fetch_one(client, "funit/display", {"funit_id": 7}, pace_ms=0)

    assert result.complete is False
    assert result.records == []
    assert result.error == "no such fund"


def test_fetch_result_flags_a_count_mismatch():
    assert FetchResult([1, 2], True, 1, 2).count_matches_expected is True
    assert FetchResult([1], True, 1, 2).count_matches_expected is False
    assert FetchResult([1], True, 1, None).count_matches_expected is None


def test_every_record_type_has_a_gatherer():
    assert set(mirror.GATHERERS) == set(mirror.RECORD_TYPES)


def test_no_mirrored_endpoint_is_a_write():
    """A typo that turned a list into an edit would be the worst possible
    bug in a module documented as read-only."""
    from clients.csuite import is_csuite_write

    for endpoint in csuite_fetch.ENDPOINT_CONTRACTS:
        assert not is_csuite_write(endpoint), endpoint


def test_pace_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("CSUITE_PACE_MS", "400")
    assert csuite_fetch.configured_pace_ms() == 400
    assert csuite_fetch.pace_seconds() == pytest.approx(0.4)

    monkeypatch.setenv("CSUITE_PACE_MS", "not-a-number")
    assert csuite_fetch.configured_pace_ms() == csuite_fetch.DEFAULT_PACE_MS

    monkeypatch.delenv("CSUITE_PACE_MS", raising=False)
    assert csuite_fetch.configured_pace_ms() == 400


def test_duplicate_ids_across_pages_collapse_to_one_row(db):
    """A record inserted mid-sweep shifts later rows down a page, which can
    serve the same id twice. Postgres aborts a multi-row upsert that hits
    the same conflict target twice, so the duplicate has to go."""
    client = StubClient({
        "grant/list": [
            ok([{"grant_id": 1, "v": "old"}, {"grant_id": 2}], count=3),
            ok([{"grant_id": 2}, {"grant_id": 3}]),
            ok([]),
        ]
    })

    result = mirror.refresh_type("grant", client=client, pace_ms=0)

    assert result.status == "complete"
    written = [row["csuite_id"] for row in db.upserted_rows()]
    assert sorted(written) == ["1", "2", "3"]
    assert len(written) == len(set(written))
    assert result.notes["duplicate_ids"] == 1
    assert result.fetched == 3


def test_no_duplicates_leaves_the_note_off(db):
    client = StubClient({"grant/list": [ok([{"grant_id": 1}], count=1),
                                        ok([])]})
    result = mirror.refresh_type("grant", client=client, pace_ms=0)
    assert "duplicate_ids" not in result.notes


def test_a_database_failure_mid_write_closes_the_run_as_aborted(monkeypatch):
    """A run row left at 'writing' with no finished_at looks like a job
    still in flight — including tomorrow."""

    class BreakingDB(FakeDB):
        def __call__(self, sql, params=None, fetch=True):
            collapsed = " ".join(str(sql).split())
            if collapsed.startswith("INSERT INTO csuite_mirror"):
                self.statements.append((collapsed, tuple(params or ())))
                raise RuntimeError("connection closed")
            return super().__call__(sql, params, fetch)

    broken = BreakingDB()
    monkeypatch.setattr("clients.database.execute_query", broken)
    client = StubClient({"grant/list": [ok([{"grant_id": 1}], count=1),
                                        ok([])]})

    with pytest.raises(RuntimeError, match="connection closed"):
        mirror.refresh_type("grant", client=client, pace_ms=0)

    assert broken.run_field("status") == "aborted"
    assert "connection closed" in broken.run_field("error_summary")


def test_a_failed_fetch_never_reaches_the_writing_status(db):
    client = StubClient({"grant/list": [fail("boom")]})
    mirror.refresh_type("grant", client=client, pace_ms=0)

    statuses = [params[0] for sql, params in db.run_updates
                if sql.startswith("UPDATE sync_runs SET status")]
    assert "writing" not in statuses
    assert statuses[-1] == "failed"


# ---------------------------------------------------------------------------
# Who started the run: sync_runs.triggered_by is BIGINT REFERENCES users(id)
# ---------------------------------------------------------------------------

def run_grant(db, **kwargs):
    client = StubClient({"grant/list": [ok([{"grant_id": 1}], count=1),
                                        ok([])]})
    return mirror.refresh_type("grant", client=client, pace_ms=0, **kwargs)


def insert_columns(db):
    """(triggered_by, trigger_source, dry_run, notes) from the run INSERT."""
    _, params = db.run_inserts[0]
    return params[0], params[1], params[2], json.loads(params[3])


def test_a_cli_run_writes_null_to_the_foreign_key(db):
    run_grant(db)
    triggered_by, source, _, notes = insert_columns(db)

    assert triggered_by is None
    assert source == "cli"
    assert notes["trigger"] == "cli:mirror_refresh"


def test_the_label_the_cli_passes_lands_in_notes_not_the_column(db):
    """scripts/mirror_refresh.py passes triggered_by="cli:mirror_refresh".
    That string must never reach a BIGINT foreign key."""
    run_grant(db, triggered_by="cli:mirror_refresh")
    triggered_by, _, _, notes = insert_columns(db)

    assert triggered_by is None
    assert notes["trigger"] == "cli:mirror_refresh"


def test_no_run_insert_ever_binds_a_string_to_triggered_by(db):
    mirror.refresh(
        record_types=["grant", "check"],
        client=StubClient({
            "grant/list": [ok([{"grant_id": 1}], count=1), ok([])],
            "check/list": [ok([{"check_id": 2}], count=1), ok([])],
        }),
        pace_ms=0,
        triggered_by="cli:mirror_refresh",
        trigger_source="cli",
    )

    assert len(db.run_inserts) == 2
    for _, params in db.run_inserts:
        assert params[0] is None or isinstance(params[0], int)
        assert not isinstance(params[0], str)


def test_a_chat_triggered_run_records_the_user_id(db):
    run_grant(db, triggered_by_user_id=42, trigger_source="chat",
              triggered_by="chat:carl@amuslimcf.org")
    triggered_by, source, _, notes = insert_columns(db)

    assert triggered_by == 42
    assert source == "chat"
    assert notes["trigger"] == "chat:carl@amuslimcf.org"


def test_a_non_cli_run_without_a_label_gets_no_trigger_note(db):
    run_grant(db, trigger_source="chat", triggered_by_user_id=7)
    triggered_by, source, _, notes = insert_columns(db)

    assert triggered_by == 7
    assert "trigger" not in notes


def test_a_numeric_string_user_id_is_accepted_as_an_int(db):
    run_grant(db, triggered_by_user_id="42")
    triggered_by, _, _, _ = insert_columns(db)
    assert triggered_by == 42


@pytest.mark.parametrize("bad", [
    "cli:mirror_refresh",
    "carl@amuslimcf.org",
    True,
    3.5,
    object(),
])
def test_a_non_user_id_in_the_foreign_key_parameter_is_rejected(db, bad):
    with pytest.raises(TypeError, match="triggered_by_user_id"):
        run_grant(db, triggered_by_user_id=bad)


def test_a_user_id_in_the_label_parameter_is_rejected(db):
    """Silently filing a user id as a label would leave the foreign key
    NULL and look like it worked."""
    with pytest.raises(TypeError, match="label, not a user id"):
        run_grant(db, triggered_by=42)


def test_the_trigger_note_survives_every_later_notes_write(db):
    """_update_run replaces the notes column rather than merging into it,
    so the trigger has to be carried through to the final write."""
    result = run_grant(db, triggered_by="cli:mirror_refresh")

    assert result.notes["trigger"] == "cli:mirror_refresh"
    assert result.notes["record_type"] == "grant"

    notes_writes = [json.loads(params[-2]) for sql, params in db.run_updates
                    if "notes = %s::jsonb" in sql]
    assert notes_writes, "the run should write notes at least once"
    for notes in notes_writes:
        assert notes["trigger"] == "cli:mirror_refresh"
        assert notes["record_type"] == "grant"


def test_the_trigger_note_survives_a_failed_run(db):
    client = StubClient({"grant/list": [fail("boom")]})
    result = mirror.refresh_type("grant", client=client, pace_ms=0,
                                 triggered_by="cli:mirror_refresh")

    assert result.status == "failed"
    assert result.notes["trigger"] == "cli:mirror_refresh"


def test_user_id_coercion_in_isolation():
    assert mirror._user_id(None) is None
    assert mirror._user_id(7) == 7
    assert mirror._user_id("7") == 7
    assert mirror._user_id(" -7 ") == -7
    with pytest.raises(TypeError):
        mirror._user_id("cli:mirror_refresh")
    with pytest.raises(TypeError):
        mirror._user_id(True)


# ---------------------------------------------------------------------------
# 3a-rate: the 429 policy that replaced the useless 5-second retry
# ---------------------------------------------------------------------------

class Waits:
    """Captures every pace_sleep call so backoffs can be asserted on."""

    def __init__(self):
        self.seconds = []

    def __call__(self, seconds):
        self.seconds.append(seconds)

    @property
    def long(self):
        """Only the rate-limit backoffs, not the inter-call pacing."""
        return [s for s in self.seconds if s >= 1]


@pytest.fixture
def waits(monkeypatch):
    recorder = Waits()
    monkeypatch.setattr(csuite_fetch, "pace_sleep", recorder)
    monkeypatch.setattr(mirror, "pace_sleep", recorder)
    return recorder


class RateLimitedSession:
    """A session whose POSTs come back 429, optionally with Retry-After."""

    def __init__(self, retry_after=None, refusals=None):
        self.retry_after = retry_after
        self.refusals = refusals
        self.posts = 0

    def post(self, *args, **kwargs):
        self.posts += 1
        limited = self.refusals is None or self.posts <= self.refusals
        headers = {}
        if limited and self.retry_after is not None:
            headers["Retry-After"] = self.retry_after
        return type("Response", (), {
            "status_code": 429 if limited else 200,
            "headers": headers,
        })()


class LimitedClient(StubClient):
    """A stub whose _request drives a real 429-shaped HTTP response.

    The 429 arrives with no marker in the response body — only the HTTP
    status and the Retry-After header — so this exercises the _StatusTap
    path rather than the error-text fallback.

    After `refusals` calls the limiter opens and one page plus a
    terminating empty page are served.
    """

    def __init__(self, retry_after=None, refusals=1):
        super().__init__({})
        self.session = RateLimitedSession(retry_after, refusals)
        self.served = 0

    def _request(self, endpoint, data=None):
        self.calls.append((endpoint, dict(data or {})))
        response = self.session.post("https://example.invalid")
        if response.status_code == 429:
            # A body that says nothing about rate limits: the status code
            # is the only signal.
            return {"success": False, "error": "request failed",
                    "errors": ["request failed"]}
        self.served += 1
        if self.served == 1:
            return ok([{"grant_id": 1}], count=1)
        return ok([])


def test_retry_after_header_is_honoured_over_the_backoff(waits):
    """CSuite knows when its window reopens; we do not."""
    client = LimitedClient(retry_after="12", refusals=1)

    result = fetch_all(client, "grant/list", pace_ms=0)

    assert waits.long == [12.0], "the 30s backoff must yield to Retry-After"
    assert result.total_429s == 1
    assert result.first_429_at is not None


def test_an_http_date_retry_after_is_honoured(waits, monkeypatch):
    from datetime import datetime, timedelta, timezone

    when = datetime.now(timezone.utc) + timedelta(seconds=45)
    header = when.strftime("%a, %d %b %Y %H:%M:%S GMT")
    client = LimitedClient(retry_after=header, refusals=1)

    fetch_all(client, "grant/list", pace_ms=0)

    assert len(waits.long) == 1
    assert 40 <= waits.long[0] <= 46


def test_an_absurd_retry_after_falls_back_to_our_own_backoff(waits):
    """Half an hour is a reason to stop the run, not to hold it open."""
    client = LimitedClient(retry_after="1800", refusals=1)

    fetch_all(client, "grant/list", pace_ms=0)

    assert waits.long == [30.0]


@pytest.mark.parametrize("header", ["", "soon", "-5", None])
def test_an_unusable_retry_after_falls_back_to_the_backoff(waits, header):
    client = LimitedClient(retry_after=header, refusals=1)
    fetch_all(client, "grant/list", pace_ms=0)
    assert waits.long == [30.0]


def test_three_backoffs_then_fail(waits):
    """30s, 60s, 120s — then stop. The 5s retry this replaced was useless:
    the limiter stayed shut for 15s+ once tripped."""
    client = StubClient({
        "grant/list": lambda data: fail("HTTP 429 Too Many Requests")
    })

    result = fetch_all(client, "grant/list", pace_ms=0)

    assert waits.long == [30.0, 60.0, 120.0]
    assert result.complete is False
    assert result.error == "rate limited"
    assert result.calls == 4
    assert result.total_429s == 4


def test_a_429_that_clears_lets_the_fetch_finish(waits):
    responses = [fail("HTTP 429 Too Many Requests"),
                 fail("HTTP 429 Too Many Requests"),
                 ok([{"grant_id": 1}], count=1),
                 ok([])]
    client = StubClient({"grant/list": responses})

    result = fetch_all(client, "grant/list", pace_ms=0)

    assert waits.long == [30.0, 60.0]
    assert result.complete is True
    assert result.total_429s == 2, "recorded even though the fetch succeeded"
    assert result.first_429_at is not None


def test_retry_after_parsing_in_isolation():
    from clients.csuite_fetch import parse_retry_after
    assert parse_retry_after("30") == 30.0
    assert parse_retry_after("30.5") == 30.5
    assert parse_retry_after("0") == 0.0
    assert parse_retry_after(None) is None
    assert parse_retry_after("") is None
    assert parse_retry_after("nonsense") is None
    assert parse_retry_after("-1") is None
    assert parse_retry_after("99999") is None


# ---------------------------------------------------------------------------
# 3a-rate: staged fund displays
# ---------------------------------------------------------------------------

def test_each_display_is_staged_as_it_returns(db):
    client = fund_client()
    mirror.refresh_type("fund", client=client, pace_ms=0)

    staged = [params for _, params in db.staging_inserts]
    assert len(staged) == 2
    for run_id, record_type, source_id, payload in staged:
        assert record_type == "fund_display"
        assert run_id is not None
        assert json.loads(payload)["funit_id"] == int(source_id)


def test_a_staged_display_is_reused_and_the_call_is_not_made(db):
    """The whole point: a display already paid for is not paid for twice."""
    db.staged[("fund_display", "1000")] = FUND_DISPLAYS[1000]
    client = fund_client()

    result = mirror.refresh_type("fund", client=client, pace_ms=0)

    assert result.status == "complete"
    assert result.reused_staged == 1

    called = [d["funit_id"] for e, d in client.calls if e == "funit/display"]
    assert called == [1001], "fund 1000 must not have been fetched again"

    rows = {row["csuite_id"]: row for row in db.upserted_rows()}
    assert set(rows) == {"1000", "1001"}
    assert rows["1000"]["fund_group_id"] == 1002


def test_staged_displays_are_read_with_an_age_limit(db):
    client = fund_client()
    mirror.refresh_type("fund", client=client, pace_ms=0)

    loads = db.matching("SELECT s.source_id, s.proposed_values")
    assert len(loads) == 1
    sql, params = loads[0]
    assert params == ("fund_display", "24 hours")
    assert "started_at > NOW() - %s::interval" in sql


def test_staging_rows_are_deleted_after_a_complete_fund_write(db):
    client = fund_client()
    result = mirror.refresh_type("fund", client=client, pace_ms=0)

    assert result.status == "complete"
    assert db.staging_clears, "a complete fund write must clear staging"
    assert db.staging_clears[-1][1] == ("fund_display",)
    assert not db.staged, "no staged rows should survive"


def test_staging_survives_a_stopped_fund_run(db):
    """A run that dies partway must leave its paid-for displays behind."""
    def display(data):
        if data["funit_id"] == 1001:
            return fail("HTTP 429 Too Many Requests")
        return ok_object(FUND_DISPLAYS[1000])

    client = StubClient({
        "funit/list": [ok(FUND_LIST, count=2), ok([])],
        "funit/display": display,
    })

    result = mirror.refresh_type("fund", client=client, pace_ms=0)

    assert result.status == "failed"
    assert db.upserts == [], "an incomplete sweep writes no mirror rows"
    assert db.staged.get(("fund_display", "1000")) == FUND_DISPLAYS[1000]
    assert not db.staging_clears, "staging must NOT be cleared on failure"


def test_a_second_run_after_a_stop_reuses_everything_staged(db):
    """Run one dies on fund 1001; run two makes exactly one display call."""
    window_shut = {"yes": True}

    def display(data):
        # 1001 is refused for as long as the limiter is shut, including
        # through every backoff — which is what a real 429 window does.
        if data["funit_id"] == 1001 and window_shut["yes"]:
            return fail("HTTP 429 Too Many Requests")
        return ok_object(FUND_DISPLAYS[data["funit_id"]])

    first = StubClient({"funit/list": [ok(FUND_LIST, count=2), ok([])],
                        "funit/display": display})
    assert mirror.refresh_type("fund", client=first,
                               pace_ms=0).status == "failed"

    window_shut["yes"] = False
    second = StubClient({"funit/list": [ok(FUND_LIST, count=2), ok([])],
                         "funit/display": display})
    result = mirror.refresh_type("fund", client=second, pace_ms=0)

    assert result.status == "complete"
    assert result.reused_staged == 1
    called = [d["funit_id"] for e, d in second.calls if e == "funit/display"]
    assert called == [1001], "only the fund that was never fetched"


def test_a_reused_display_costs_no_pacing_pause(db, waits):
    db.staged[("fund_display", "1000")] = FUND_DISPLAYS[1000]
    db.staged[("fund_display", "1001")] = FUND_DISPLAYS[1001]
    client = fund_client()

    mirror.refresh_type("fund", client=client, pace_ms=250)

    assert not [e for e, _ in client.calls if e == "funit/display"]
    # The only pause is funit/list paging between its two pages. Two reused
    # displays add none, because neither made a call.
    assert waits.seconds == [0.25]


def test_an_unreadable_staging_table_only_costs_speed(db, monkeypatch):
    """Staging is an optimisation. It must never be able to fail a run."""
    original = db.__call__

    def breaking(sql, params=None, fetch=True):
        if "sync_staging" in sql:
            raise RuntimeError("relation does not exist")
        return original(sql, params, fetch)

    monkeypatch.setattr("clients.database.execute_query", breaking)
    client = fund_client()

    result = mirror.refresh_type("fund", client=client, pace_ms=0)

    assert result.status == "complete"
    assert result.reused_staged == 0
    assert len(db.upserted_rows()) == 2


# ---------------------------------------------------------------------------
# 3a-rate: the call budget
# ---------------------------------------------------------------------------

def test_budget_stops_a_paged_fetch_at_n():
    client = StubClient({
        "grant/list": lambda data: ok([{"grant_id": data["view_offset"]}])
    })
    budget = csuite_fetch.CallBudget(3)

    result = fetch_all(client, "grant/list", pace_ms=0, budget=budget)

    assert result.complete is False
    assert result.error == "budget reached"
    assert result.calls == 3
    assert budget.used == 3
    assert len(client.calls) == 3


def test_budget_is_shared_across_record_types(db):
    client = StubClient({
        "grant/list": lambda data: ok([{"grant_id": data["view_offset"]}]),
        "check/list": lambda data: ok([{"check_id": data["view_offset"]}]),
    })
    budget = csuite_fetch.CallBudget(4)

    results = mirror.refresh(record_types=["grant", "check"], client=client,
                             pace_ms=0, budget=budget)

    assert budget.used == 4
    assert results[0].status == "failed"
    assert results[0].stop_reason == "budget reached"
    assert results[1].status == "skipped"
    assert not [e for e, _ in client.calls if e == "check/list"]


def test_budget_stops_the_fund_sweep_and_keeps_what_it_staged(db):
    funds = [{"funit_id": i} for i in range(1000, 1010)]
    client = StubClient({
        "funit/list": [ok(funds, count=10), ok([])],
        "funit/display": lambda d: ok_object({"funit_id": d["funit_id"],
                                              "fgroup_id": 1002}),
    })
    # 2 calls for the listing, then 4 displays.
    budget = csuite_fetch.CallBudget(6)

    result = mirror.refresh_type("fund", client=client, pace_ms=0,
                                 budget=budget)

    assert result.status == "failed"
    assert result.stop_reason == "budget reached"
    assert "budget reached" in result.error
    assert db.upserts == [], "an incomplete sweep writes nothing"

    staged_ids = sorted(k[1] for k in db.staged)
    assert staged_ids == ["1000", "1001", "1002", "1003"]
    assert not db.staging_clears


def test_an_integer_budget_is_accepted(db):
    client = StubClient({
        "grant/list": lambda data: ok([{"grant_id": data["view_offset"]}])
    })
    results = mirror.refresh(record_types=["grant"], client=client,
                             pace_ms=0, budget=2)
    assert results[0].stop_reason == "budget reached"


def test_no_budget_means_no_ceiling(db):
    client = StubClient({"grant/list": [ok([{"grant_id": 1}], count=1),
                                        ok([])]})
    result = mirror.refresh_type("grant", client=client, pace_ms=0)
    assert result.status == "complete"


def test_call_budget_arithmetic():
    budget = csuite_fetch.CallBudget(3)
    assert budget.remaining == 3 and not budget.exhausted
    budget.spend(3)
    assert budget.remaining == 0 and budget.exhausted

    unlimited = csuite_fetch.CallBudget(None)
    unlimited.spend(10_000)
    assert unlimited.remaining is None and not unlimited.exhausted


# ---------------------------------------------------------------------------
# 3a-rate: the ledger learns the shape of the limiter
# ---------------------------------------------------------------------------

def notes_written(db):
    """The last notes payload written to sync_runs."""
    writes = [json.loads(params[-2]) for sql, params in db.run_updates
              if "notes = %s::jsonb" in sql]
    assert writes, "the run should write notes at least once"
    return writes[-1]


def test_every_run_row_carries_the_rate_limit_fields(db):
    client = StubClient({"grant/list": [ok([{"grant_id": 1}], count=1),
                                        ok([])]})
    result = mirror.refresh_type("grant", client=client, pace_ms=0)

    notes = notes_written(db)
    assert notes["first_429_at"] is None
    assert notes["total_429s"] == 0
    assert notes["reused_staged"] == 0
    assert result.notes["total_429s"] == 0


def test_a_rate_limited_run_records_when_and_how_often(db, waits):
    client = StubClient({
        "grant/list": lambda data: fail("HTTP 429 Too Many Requests")
    })
    result = mirror.refresh_type("grant", client=client, pace_ms=0)

    notes = notes_written(db)
    assert notes["total_429s"] == 4
    assert notes["first_429_at"] is not None
    assert notes["stop_reason"] == "rate limited"
    assert result.total_429s == 4


def test_a_fund_run_records_how_many_displays_it_reused(db):
    db.staged[("fund_display", "1000")] = FUND_DISPLAYS[1000]
    client = fund_client()
    mirror.refresh_type("fund", client=client, pace_ms=0)

    assert notes_written(db)["reused_staged"] == 1


def test_the_default_pace_is_slower_than_it_was():
    """150ms was measured into a rate limit at 666 cumulative calls."""
    assert csuite_fetch.DEFAULT_PACE_MS == 400
