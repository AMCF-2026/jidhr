"""
Users Client
============
Read/write helpers for the `users` table.

The table is the source of truth for who may sign in and what they may do.
Google OAuth proves *which mailbox* someone controls; this table decides
whether that person still has access (`is_active`) and at what level
(`role`). Those are different questions, and only the second one is ours.

Schema (created out of band — this module never creates or alters it):

    users(
        id                BIGSERIAL PRIMARY KEY,
        email             TEXT UNIQUE NOT NULL,
        display_name      TEXT,
        role              TEXT NOT NULL DEFAULT 'staff'
                          CHECK (role IN ('admin','staff','donor')),
        csuite_profile_id BIGINT,
        hubspot_contact_id TEXT,
        is_active         BOOLEAN NOT NULL DEFAULT TRUE,
        last_login_at     TIMESTAMPTZ,
        created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )

Email is normalised to lowercase on the way into every query. Google returns
addresses in whatever case the user typed, and `email` is UNIQUE — without
normalising, `Carl@` and `carl@` would become two rows and the ON CONFLICT
below would never fire.
"""

import logging

from clients.database import execute_query

logger = logging.getLogger(__name__)


# One statement so login is a single round trip and two concurrent logins
# cannot race into a duplicate-key error. ON CONFLICT makes the insert
# idempotent; the update branch is what records the login.
_UPSERT_SQL = """
    INSERT INTO users (email, display_name, last_login_at)
    VALUES (%s, %s, NOW())
    ON CONFLICT (email) DO UPDATE
        SET last_login_at = NOW(),
            -- COALESCE, not a bare assignment: Google occasionally returns a
            -- profile with no name, and EXCLUDED.display_name would then
            -- overwrite a good stored name with NULL.
            display_name  = COALESCE(EXCLUDED.display_name, users.display_name),
            updated_at    = NOW()
    RETURNING *
"""

_BY_ID_SQL = "SELECT * FROM users WHERE id = %s"

_BY_EMAIL_SQL = "SELECT * FROM users WHERE email = %s"


def normalize_email(email) -> str:
    """Lowercase and strip an address for storage and lookup."""
    return (email or "").strip().lower()


def get_or_create_user(email: str, display_name: str = None) -> dict | None:
    """Record a login, creating the user row if this is their first.

    Returns the full row, including `is_active` and `role` — the caller is
    responsible for checking those before granting access. Creating a row
    here is not the same as authorising the person.
    """
    email = normalize_email(email)
    if not email:
        raise ValueError("get_or_create_user requires an email address")

    rows = execute_query(_UPSERT_SQL, (email, display_name))
    if not rows:
        # RETURNING * on a successful upsert always yields a row; no row
        # means something is wrong with the statement or the table.
        logger.error("User upsert returned no row for %s", email)
        return None

    row = rows[0]
    logger.info("User login recorded: id=%s role=%s active=%s",
                row.get("id"), row.get("role"), row.get("is_active"))
    return row


def get_user_by_id(user_id) -> dict | None:
    """Fetch one user by primary key, or None."""
    rows = execute_query(_BY_ID_SQL, (int(user_id),))
    return rows[0] if rows else None


def get_user_by_email(email: str) -> dict | None:
    """Fetch one user by address, or None."""
    email = normalize_email(email)
    if not email:
        return None
    rows = execute_query(_BY_EMAIL_SQL, (email,))
    return rows[0] if rows else None
