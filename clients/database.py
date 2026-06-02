"""
Database Client
===============
PostgreSQL connection management for Jidhr.

This module only handles connection management — pool lifecycle,
context-managed checkout, and a one-shot query helper. Business
queries live in intents/; schema/migrations live elsewhere.

Each gunicorn worker imports this module on fork and gets its own
ThreadedConnectionPool. With 8 workers × maxconn=2, peak usage is
16 connections — well under the Railway Postgres hobby tier cap.
"""

import logging
import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pool initialization (module load — one pool per gunicorn worker)
# ---------------------------------------------------------------------------

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable is not set. "
        "Railway should inject this automatically when a Postgres "
        "plugin is attached; check the service's Variables tab."
    )

try:
    _pool = ThreadedConnectionPool(minconn=1, maxconn=2, dsn=DATABASE_URL)
    logger.info("Database pool initialized (minconn=1, maxconn=2)")
except psycopg2.Error as e:
    logger.error(f"Failed to initialize database pool: {e}")
    raise


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@contextmanager
def get_connection():
    """Yield a pooled connection; commit on success, rollback on exception.

    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("...")
    """
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def execute_query(sql: str, params=None, fetch: bool = True):
    """Run a one-shot query.

    Args:
        sql: SQL statement (use %s placeholders for parameters).
        params: Sequence/mapping bound to placeholders. NEVER interpolated.
        fetch: True → return list of dict rows (RealDictCursor).
               False → return rowcount for INSERT/UPDATE/DELETE.
    """
    param_count = len(params) if params else 0
    logger.debug(f"execute_query: sql={sql!r} params={param_count}")

    try:
        with get_connection() as conn:
            cursor_factory = RealDictCursor if fetch else None
            with conn.cursor(cursor_factory=cursor_factory) as cur:
                cur.execute(sql, params)
                if fetch:
                    return [dict(row) for row in cur.fetchall()]
                return cur.rowcount
    except psycopg2.Error as e:
        logger.error(f"Query failed: {e}", exc_info=True)
        raise


def health_check() -> bool:
    """Return True if the pool can serve a connection and run SELECT 1."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {e}", exc_info=True)
        return False
