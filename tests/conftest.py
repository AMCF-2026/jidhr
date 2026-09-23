"""Shared pytest configuration.

Two jobs:

1. Put the repo root on sys.path so tests can import the application
   packages (clients/, content/, intents/) regardless of where pytest
   is invoked from.
2. Guarantee DATABASE_URL is UNSET for every test. Step 1a's whole
   point is that import and collection must not require a database, so
   the suite must never silently pass because a developer happened to
   have DATABASE_URL exported in their shell.
"""

import os
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


@pytest.fixture(autouse=True)
def _unset_database_url(monkeypatch):
    """Remove DATABASE_URL from the environment for the duration of each test."""
    monkeypatch.delenv("DATABASE_URL", raising=False)


class AuditStore:
    """An in-memory stand-in for the `write_audit` table.

    Since 2026-09-23 auditing is pre-flight: clients.audit.reserve_write
    claims a row BEFORE the request goes out and refuses the write if it
    cannot. That makes an audit store a precondition for any test that
    exercises a write — without one, every write is correctly refused and
    the test ends up asserting on the refusal instead of on its subject.

    Tests that want to see the refusal install their own failing store;
    this one always succeeds.
    """

    def __init__(self):
        self.rows = []

    def __call__(self, sql, params=None, fetch=True):
        text = " ".join(str(sql).split())
        if text.startswith("UPDATE write_audit"):
            status, http_status, error, duration_ms, row_id = params
            row = self.rows[int(row_id) - 1]
            row.update(status=status, http_status=http_status, error=error,
                       duration_ms=duration_ms)
            return 1
        self.rows.append({"params": params})
        if "RETURNING id" in text:
            return [{"id": len(self.rows)}]
        return 1


@pytest.fixture
def audit_store(monkeypatch):
    """A working audit store, so writes are not refused pre-flight."""
    store = AuditStore()
    monkeypatch.setattr("clients.database.execute_query", store)
    monkeypatch.setattr("clients.database.is_configured", lambda: True)
    return store
