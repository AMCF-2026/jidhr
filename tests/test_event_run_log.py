"""A run_log row exists even when the run dies.

It used to be a single INSERT at the very end with status 'complete'
hardcoded. So the apply that crashed on 2026-10-06 left NO row at all — the
only reason anyone knows it happened is that a person saw the error in chat.
A table whose rows only appear when nothing went wrong cannot answer "what
happened last night".

And the counts were taken from the raw plan rather than from what was
written. Both rows in production read created_count 77 and updated_count 11
for dry runs that would have written 3 and 5.

No network, no database.
"""

from datetime import date

import pytest

from sync import event_apply as ea
from sync import event_hubspot as eh

PINNED_TODAY = date(2026, 10, 6)


def row(event_date_id, event_date="2026-12-01", archived=0):
    return {"event_date_id": event_date_id, "event_id": 900,
            "event_name": "E", "event_description": "An event",
            "event_date": event_date, "start_time": "3 pm ET",
            "location": "Virtual", "archived": archived,
            "goal_amount": None, "available_seats": 1}


class Ledger:
    """The run_log, as the two SQL statements see it."""

    def __init__(self):
        self.opened = []
        self.closed = []
        self.next_id = 7

    def execute_query(self, sql, params=None, fetch=True):
        text = " ".join(str(sql).split())
        if text.startswith("INSERT INTO hubsync.run_log"):
            self.opened.append(params)
            return [{"id": self.next_id}]
        if text.startswith("UPDATE hubsync.run_log"):
            self.closed.append(params)
            return 1
        return 1 if not fetch else []


@pytest.fixture
def ledger(monkeypatch):
    book = Ledger()
    monkeypatch.setattr(ea.database, "execute_query", book.execute_query)
    monkeypatch.setattr(ea, "_save_map", lambda *a, **k: None)
    monkeypatch.setattr(ea, "migration_applied", lambda: True)
    monkeypatch.setattr(ea, "load_map", lambda: {})
    monkeypatch.setattr("clients.csuite.CSuiteClient", lambda: object())
    return book


def arrange(monkeypatch, rows, index=None, hs_error=None, complete=True):
    monkeypatch.setattr(ea.eh, "fetch_event_dates",
                        lambda client, pace_ms=None: eh.Fetched(
                            rows=rows, calls=2, complete=complete,
                            error=None, total_429s=0))
    monkeypatch.setattr(ea.eh, "hubspot_index",
                        lambda hubspot: (dict(index or {}), 1, hs_error))


class Seam:
    def __init__(self, answers=None):
        self.answers = list(answers or [])
        self.sent = []

    def _send_with_status(self, method, endpoint, data=None):
        self.sent.append(endpoint)
        if self.answers:
            return self.answers.pop(0)
        return {"objectId": "hs-new"}, 200


# ---------------------------------------------------------------------------
# Opened at the start
# ---------------------------------------------------------------------------

def test_a_row_is_opened_as_running_before_anything_happens(ledger,
                                                            monkeypatch):
    arrange(monkeypatch, [row(1528, event_date="2026-12-15")])

    ea.run(hubspot=Seam(), dry_run=True, today=PINNED_TODAY)

    assert len(ledger.opened) == 1
    applied, = ledger.opened[0]
    assert applied is False, "a dry run is logged as not applied"


def test_a_live_run_opens_its_row_as_applied(ledger, monkeypatch):
    arrange(monkeypatch, [row(1528, event_date="2026-12-15")])

    ea.run(hubspot=Seam(), dry_run=False, today=PINNED_TODAY)

    assert ledger.opened[0] == (True,)


def test_the_opened_row_says_running(ledger, monkeypatch):
    """So an abandoned run is visibly abandoned rather than absent."""
    assert "'running'" in ea._RUN_OPEN_SQL


# ---------------------------------------------------------------------------
# Closed in a finally — including when the run dies
# ---------------------------------------------------------------------------

def test_a_clean_run_is_closed_complete(ledger, monkeypatch):
    arrange(monkeypatch, [row(1528, event_date="2026-12-15")])

    ea.run(hubspot=Seam(), dry_run=False, today=PINNED_TODAY)

    assert len(ledger.closed) == 1
    assert ledger.closed[0][0] == "complete"
    assert ledger.closed[0][-1] == 7, "the id that was opened"


def test_a_refused_run_is_closed_failed(ledger, monkeypatch):
    """A CSuite read that did not complete refuses to plan. The row still
    closes, and it closes as failed."""
    arrange(monkeypatch, [row(1528)], complete=False)

    result = ea.run(hubspot=Seam(), dry_run=True, today=PINNED_TODAY)

    assert result["error"]
    assert ledger.closed[0][0] == "failed"
    assert "did not complete" in ledger.closed[0][1]


def test_a_stopped_run_is_closed_failed(ledger, monkeypatch):
    arrange(monkeypatch, [row(1464)],
            index={"csuite-1464": {"objectId": "hs-1",
                                   "externalEventId": "csuite-1464"}})
    seam = Seam([({"status": "error", "message": "no"}, 404)])

    result = ea.run(hubspot=seam, dry_run=False, today=PINNED_TODAY)

    assert result["stopped"]
    assert ledger.closed[0][0] == "failed"


def test_an_unexpected_exception_still_closes_the_row(ledger, monkeypatch):
    """The 2026-10-06 case: a NameError left no row at all."""
    arrange(monkeypatch, [row(1464)])

    def explode(*args, **kwargs):
        raise RuntimeError("something nobody predicted")

    monkeypatch.setattr(ea, "plan", explode)

    with pytest.raises(RuntimeError):
        ea.run(hubspot=Seam(), dry_run=True, today=PINNED_TODAY)

    assert len(ledger.closed) == 1, "the row was left open"
    assert ledger.closed[0][0] == "complete" or \
        ledger.closed[0][0] == "failed"


def test_exactly_one_row_per_run(ledger, monkeypatch):
    arrange(monkeypatch, [row(1528, event_date="2026-12-15")])

    ea.run(hubspot=Seam(), dry_run=True, today=PINNED_TODAY)

    assert len(ledger.opened) == 1
    assert len(ledger.closed) == 1


# ---------------------------------------------------------------------------
# Counts are what was written, not the raw plan
# ---------------------------------------------------------------------------

def closed_counts(ledger):
    """The UPDATE's parameters, by name."""
    (status, error, csuite_calls, hubspot_calls, dates, created, updated,
     unchanged, skipped, review, failed, outcomes, run_id) = ledger.closed[0]
    return {"status": status, "created": created, "updated": updated,
            "unchanged": unchanged, "skipped": skipped, "review": review,
            "failed": failed, "dates": dates}


def test_a_dry_run_logs_what_would_be_written_not_the_plan(ledger,
                                                           monkeypatch):
    """Production logged created_count 77 for a run that would write 3."""
    rows = [row(1043, event_date="2021-07-29", archived=1),   # withheld twice
            row(1528, event_date="2026-12-15"),               # writable
            row(1495, event_date="2026-09-30")]               # past
    arrange(monkeypatch, rows)

    result = ea.run(hubspot=Seam(), dry_run=True, today=PINNED_TODAY)

    counts = closed_counts(ledger)
    assert result["created"] == 1
    assert counts["created"] == 1, "not 3"
    assert counts["review"] == 1


def test_a_live_run_logs_what_was_actually_written(ledger, monkeypatch):
    arrange(monkeypatch, [row(1528, event_date="2026-12-15")])

    ea.run(hubspot=Seam(), dry_run=False, today=PINNED_TODAY)

    assert closed_counts(ledger)["created"] == 1


def test_a_stopped_run_logs_only_what_landed(ledger, monkeypatch):
    arrange(monkeypatch, [row(1466), row(1464)],
            index={f"csuite-{i}": {"objectId": f"hs-{i}",
                                   "externalEventId": f"csuite-{i}"}
                   for i in (1466, 1464)})
    seam = Seam([({"objectId": "hs-1466"}, 200),
                 ({"status": "error", "message": "no"}, 404)])

    ea.run(hubspot=seam, dry_run=False, today=PINNED_TODAY)

    counts = closed_counts(ledger)
    assert counts["updated"] == 1, "the one that landed"
    assert counts["failed"] == 1
    assert counts["status"] == "failed"


def test_the_call_counts_are_recorded(ledger, monkeypatch):
    arrange(monkeypatch, [row(1528, event_date="2026-12-15")])

    ea.run(hubspot=Seam(), dry_run=True, today=PINNED_TODAY)

    counts = closed_counts(ledger)
    assert counts["dates"] == 1


def test_closing_survives_a_database_failure(monkeypatch):
    """Bookkeeping must not be able to fail a run that already happened."""
    def explode(sql, params=None, fetch=True):
        raise RuntimeError("database gone")

    monkeypatch.setattr(ea.database, "execute_query", explode)

    ea.close_run(7, "complete", {})          # must not raise


def test_opening_survives_a_database_failure(monkeypatch):
    def explode(sql, params=None, fetch=True):
        raise RuntimeError("database gone")

    monkeypatch.setattr(ea.database, "execute_query", explode)

    assert ea.open_run(applied=True) is None


def test_closing_a_row_that_was_never_opened_is_harmless(monkeypatch):
    calls = []
    monkeypatch.setattr(ea.database, "execute_query",
                        lambda *a, **k: calls.append(a) or 1)

    ea.close_run(None, "failed", {})

    assert calls == [], "nothing to update"
