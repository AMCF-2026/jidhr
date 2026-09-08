"""
Database Client
===============
PostgreSQL connection management for Jidhr.

This module only handles connection management — pool lifecycle,
context-managed checkout, and a one-shot query helper. Business
queries live in intents/; schema/migrations live elsewhere.

The pool is built lazily on first use rather than at import time, so
this module (and everything that imports it) can be imported without
DATABASE_URL set — tests, tooling, and `python -c "import intents"`.
A missing DATABASE_URL now fails at first query, not at import.

Each gunicorn worker builds its own ThreadedConnectionPool. With
8 workers × maxconn=2, peak usage is 16 connections — well under the
Railway Postgres hobby tier cap.
Note: gunicorn also runs --threads 4, so 4 threads within a worker
contend for those 2 connections; getconn() blocks when both are out.
"""

import logging
import os
import threading
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)


_MISSING_DATABASE_URL_MESSAGE = (
    "DATABASE_URL environment variable is not set. "
    "Railway should inject this automatically when a Postgres "
    "plugin is attached; check the service's Variables tab."
)


# ---------------------------------------------------------------------------
# Lazy pool initialization (first use — one pool per gunicorn worker)
# ---------------------------------------------------------------------------

_pool = None
_pool_lock = threading.Lock()


def is_configured() -> bool:
    """Return True if DATABASE_URL is set.

    Cheap and side-effect free: reads the environment only. Does not
    build the pool and does not open a connection.
    """
    return bool(os.environ.get('DATABASE_URL'))


def get_pool():
    """Return the process-wide connection pool, building it on first call.

    Thread-safe via double-checked locking: once the pool exists the
    common path is a single read with no lock acquired.

    Raises:
        RuntimeError: DATABASE_URL is unset at the time of the call.
        psycopg2.Error: the pool could not be created.
    """
    global _pool

    if _pool is not None:
        return _pool

    with _pool_lock:
        # Re-check under the lock: another thread may have built it
        # while this one waited.
        if _pool is not None:
            return _pool

        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            raise RuntimeError(_MISSING_DATABASE_URL_MESSAGE)

        try:
            _pool = ThreadedConnectionPool(
                minconn=1, maxconn=2, dsn=database_url
            )
            logger.info("Database pool initialized (minconn=1, maxconn=2)")
        except psycopg2.Error as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise

    return _pool


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
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


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
