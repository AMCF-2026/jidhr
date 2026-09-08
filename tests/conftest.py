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
