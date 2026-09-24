"""
Pending Drafts
==============
Where a half-finished email or social draft lives between two chat
messages.

Why it is not the session cookie any more
-----------------------------------------
It used to be. `assistant._save_state_to_session` wrote the whole draft
into Flask's signed session cookie, which is client-side and capped at
about 4 KB by every browser. A short draft fitted; a newsletter did not.

Measured on 2026-09-24 with the 2026-09-22 Giving Circle draft: **3,851
signed bytes against a 4,096-byte limit — 242 bytes of headroom.** A
slightly longer newsletter, or one with more URLs and names (which
compress poorly), goes over, the browser silently drops the cookie, and
the next message finds no draft. That is what produced "I don't have a
recent draft to act on" after a brief that had just been drafted
successfully.

Nothing warns when this happens. The response still carries a
Set-Cookie header; the browser simply declines it. A store that fails by
size, silently, on exactly the largest and most valuable drafts is the
wrong store.

The shape
---------
One row per (user, channel), replaced outright when a new draft is
generated and deleted when one is saved or cancelled. Two hours to live,
enforced in SQL so an expired draft is never returned even if the
sweeper has not run.

There is deliberately NO in-memory fallback. A fallback that works on
one gunicorn worker and not the other eight is a bug that reproduces
once a day and never in testing; production runs `--workers 8
--threads 4`.
"""

import json
import logging

from clients import database

logger = logging.getLogger(__name__)

# How long a half-finished draft is worth keeping. Long enough to take a
# phone call, short enough that yesterday's abandoned draft never
# swallows today's message.
TTL_HOURS = 2

DEFAULT_CHANNEL = "web"

_UPSERT_SQL = """
    INSERT INTO pending_drafts (user_id, channel, draft, expires_at)
    VALUES (%s, %s, %s::jsonb, NOW() + INTERVAL '%s hours')
    ON CONFLICT (user_id, channel) DO UPDATE
       SET draft = EXCLUDED.draft,
           updated_at = NOW(),
           expires_at = EXCLUDED.expires_at
    RETURNING id
"""

# expires_at is checked here rather than trusted to a sweeper: a draft
# that has aged out must not come back just because nothing has tidied
# up yet.
_SELECT_SQL = """
    SELECT draft
      FROM pending_drafts
     WHERE user_id = %s AND channel = %s AND expires_at > NOW()
"""

_DELETE_SQL = """
    DELETE FROM pending_drafts
     WHERE user_id = %s AND channel = %s
"""

_SWEEP_SQL = """
    DELETE FROM pending_drafts WHERE expires_at <= NOW()
"""


def _key(user_id, channel):
    """(user_id, channel) or None if there is no usable requester."""
    if user_id in (None, ""):
        return None
    try:
        user_id = int(user_id)
    except (TypeError, ValueError):
        return None
    return user_id, (channel or DEFAULT_CHANNEL)


def save(user_id, draft: dict, channel: str = DEFAULT_CHANNEL) -> bool:
    """Store this draft as THE pending draft for the requester.

    Replaces whatever was there: a person who asks for a second draft
    has moved on from the first, and keeping both would only raise the
    question of which one "save this" means.
    """
    key = _key(user_id, channel)
    if key is None:
        logger.warning("no user id — the draft cannot be stored")
        return False
    try:
        database.execute_query(
            _UPSERT_SQL,
            (key[0], key[1], json.dumps(draft or {}, default=str), TTL_HOURS),
            fetch=True)
        return True
    except Exception as e:
        logger.error("could not store the pending draft for user %s: %s",
                     key[0], e, exc_info=True)
        return False


def load(user_id, channel: str = DEFAULT_CHANNEL) -> dict | None:
    """The pending draft, or None if there is none or it has expired."""
    key = _key(user_id, channel)
    if key is None:
        return None
    try:
        found = database.execute_query(_SELECT_SQL, key, fetch=True)
    except Exception as e:
        logger.error("could not read the pending draft for user %s: %s",
                     key[0], e, exc_info=True)
        return None

    if not isinstance(found, (list, tuple)) or not found:
        return None
    row = found[0]
    draft = row.get("draft") if isinstance(row, dict) else row[0]
    if isinstance(draft, str):
        try:
            draft = json.loads(draft)
        except ValueError:  # pragma: no cover - jsonb round-trips cleanly
            return None
    return draft if isinstance(draft, dict) else None


def clear(user_id, channel: str = DEFAULT_CHANNEL) -> bool:
    """Drop the pending draft. Called on save and on cancel."""
    key = _key(user_id, channel)
    if key is None:
        return False
    try:
        database.execute_query(_DELETE_SQL, key, fetch=False)
        return True
    except Exception as e:
        logger.error("could not clear the pending draft for user %s: %s",
                     key[0], e, exc_info=True)
        return False


def sweep() -> int:
    """Delete every expired draft. Returns how many went."""
    try:
        return int(database.execute_query(_SWEEP_SQL, (), fetch=False) or 0)
    except Exception as e:
        logger.error("pending draft sweep failed: %s", e, exc_info=True)
        return 0
