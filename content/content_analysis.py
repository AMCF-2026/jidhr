"""
Content Analysis
================
Read-only query layer over content_history for topic-repetition and
cadence analysis. No LLM calls; no writes. Time-window filtering uses
COALESCE(sent_at, created_at) so rows with NULL sent_at still appear,
keyed by their insertion time.

Used by the upcoming pre-draft "have we covered this already?" check
and cadence/slot-collision reports — this module is the SQL layer only.

Failure policy: every public function logs ERROR with exc_info on any
DB exception and returns an empty list. Callers do not need try/except.
"""

import logging
import re

from clients.database import execute_query

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal normalization helpers
# ---------------------------------------------------------------------------

_PUNCT_RE = re.compile(r"[^a-z0-9\s]")
_WS_RE = re.compile(r"\s+")


def _normalize_for_match(s) -> str:
    """Lowercase, strip punctuation, collapse whitespace. Returns '' on
    non-string input."""
    if not isinstance(s, str):
        return ""
    cleaned = _PUNCT_RE.sub(" ", s.lower())
    return _WS_RE.sub(" ", cleaned).strip()


def _strip_trailing_slash(url) -> str:
    """Strip ALL trailing '/' characters. Returns '' on non-string input."""
    if not isinstance(url, str):
        return ""
    return url.rstrip("/")


# ---------------------------------------------------------------------------
# 1. Topic frequency
# ---------------------------------------------------------------------------

def get_topic_frequency(days: int = 42, content_type=None) -> list[dict]:
    """Aggregate topic counts over a recent window.

    For each normalized topic (lowercase + trimmed) appearing in any
    row's `topics` JSONB array within the window, returns:

        {
          "topic":        str,
          "total":        int,            # row count this topic appears in
          "per_channel":  {channel: int}, # NULL channels bucketed as 'unknown'
          "last_sent_at": datetime,
          "source_urls":  [str, ...]      # distinct non-null URLs, sorted
        }

    Duplicate topics within the same row count once.
    Ordered: total desc, then topic asc as tie-break.
    Empty list on any DB error.
    """
    days_int = int(days)

    sql = """
        SELECT id, channel,
               COALESCE(sent_at, created_at) AS effective_sent_at,
               source_url, topics
        FROM content_history
        WHERE topics IS NOT NULL
          AND jsonb_typeof(topics) = 'array'
          AND COALESCE(sent_at, created_at) >= NOW() - (%s * INTERVAL '1 day')
    """
    params = [days_int]
    if content_type is not None:
        sql += "          AND content_type = %s\n"
        params.append(content_type)

    try:
        rows = execute_query(sql, params=tuple(params), fetch=True)
    except Exception as e:
        logger.error(f"get_topic_frequency: query failed: {e}", exc_info=True)
        return []

    counts: dict[str, dict] = {}
    for row in rows:
        raw_topics = row.get("topics")
        if not isinstance(raw_topics, list):
            continue
        channel_key = row.get("channel") or "unknown"
        sent = row.get("effective_sent_at")
        url = row.get("source_url")

        seen_in_row: set[str] = set()
        for raw in raw_topics:
            if not isinstance(raw, str):
                continue
            normalized = raw.strip().lower()
            if not normalized or normalized in seen_in_row:
                continue
            seen_in_row.add(normalized)

            entry = counts.get(normalized)
            if entry is None:
                entry = {
                    "topic": normalized,
                    "total": 0,
                    "per_channel": {},
                    "last_sent_at": None,
                    "source_urls": set(),
                }
                counts[normalized] = entry

            entry["total"] += 1
            entry["per_channel"][channel_key] = (
                entry["per_channel"].get(channel_key, 0) + 1
            )
            if sent is not None and (
                entry["last_sent_at"] is None or sent > entry["last_sent_at"]
            ):
                entry["last_sent_at"] = sent
            if url:
                entry["source_urls"].add(url)

    result = []
    for entry in counts.values():
        entry["source_urls"] = sorted(entry["source_urls"])
        result.append(entry)
    result.sort(key=lambda e: (-e["total"], e["topic"]))
    return result


# ---------------------------------------------------------------------------
# 2. Topic-match search (pre-draft repetition check)
# ---------------------------------------------------------------------------

def find_topic_matches(topic, days: int = 42, content_type=None) -> list[dict]:
    """Return content_history rows whose topics resemble the input topic.

    Matching is in Python (not SQL): two normalized strings (lowercase,
    punct→space, collapsed whitespace) match if EITHER
      - one is a substring of the other, OR
      - their word sets overlap by at least 50% of the smaller set's size.

    Returns one dict per matched row with:
        channel, sent_at, matched_topic (original string from storage),
        title, source_url.

    Channel is preserved unchanged so callers can distinguish same-channel
    repetition from deliberate cross-network rotation.

    Sorted: sent_at desc. Empty list on any DB error or empty input.
    """
    input_norm = _normalize_for_match(topic)
    if not input_norm:
        return []
    input_words = set(input_norm.split())

    days_int = int(days)

    sql = """
        SELECT id, channel,
               COALESCE(sent_at, created_at) AS effective_sent_at,
               title, topics, source_url
        FROM content_history
        WHERE topics IS NOT NULL
          AND jsonb_typeof(topics) = 'array'
          AND COALESCE(sent_at, created_at) >= NOW() - (%s * INTERVAL '1 day')
    """
    params = [days_int]
    if content_type is not None:
        sql += "          AND content_type = %s\n"
        params.append(content_type)

    try:
        rows = execute_query(sql, params=tuple(params), fetch=True)
    except Exception as e:
        logger.error(f"find_topic_matches: query failed: {e}", exc_info=True)
        return []

    matches = []
    for row in rows:
        raw_topics = row.get("topics")
        if not isinstance(raw_topics, list):
            continue

        matched_original = None
        for raw in raw_topics:
            if not isinstance(raw, str):
                continue
            stored_norm = _normalize_for_match(raw)
            if not stored_norm:
                continue

            substring_match = (
                input_norm in stored_norm or stored_norm in input_norm
            )
            overlap_match = False
            if input_words:
                stored_words = set(stored_norm.split())
                if stored_words:
                    inter = input_words & stored_words
                    smaller = min(len(input_words), len(stored_words))
                    overlap_match = (len(inter) / smaller) >= 0.5

            if substring_match or overlap_match:
                matched_original = raw
                break

        if matched_original is None:
            continue

        matches.append({
            "channel": row.get("channel"),
            "sent_at": row.get("effective_sent_at"),
            "matched_topic": matched_original,
            "title": row.get("title"),
            "source_url": row.get("source_url"),
        })

    matches.sort(key=lambda m: m["sent_at"], reverse=True)
    return matches


# ---------------------------------------------------------------------------
# 3. Exact-URL match (highest-precision repetition signal)
# ---------------------------------------------------------------------------

def find_url_matches(url, days: int = 90) -> list[dict]:
    """Return rows whose source_url matches the input after trailing-slash
    normalization on both sides.

    Same return shape as find_topic_matches; matched_topic is None because
    the URL is the match basis, not a topic. Sorted: sent_at desc.
    Empty list on any DB error or empty/non-string input.
    """
    stripped = _strip_trailing_slash(url)
    if not stripped:
        return []

    days_int = int(days)

    sql = """
        SELECT channel,
               COALESCE(sent_at, created_at) AS effective_sent_at,
               title, source_url
        FROM content_history
        WHERE source_url IS NOT NULL
          AND COALESCE(sent_at, created_at) >= NOW() - (%s * INTERVAL '1 day')
          AND rtrim(source_url, '/') = %s
        ORDER BY COALESCE(sent_at, created_at) DESC;
    """

    try:
        rows = execute_query(sql, params=(days_int, stripped), fetch=True)
    except Exception as e:
        logger.error(f"find_url_matches: query failed: {e}", exc_info=True)
        return []

    return [
        {
            "channel": r.get("channel"),
            "sent_at": r.get("effective_sent_at"),
            "matched_topic": None,
            "title": r.get("title"),
            "source_url": r.get("source_url"),
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# 4. Posting activity (cadence + slot collisions)
# ---------------------------------------------------------------------------

def get_posting_activity(days: int = 30, content_type: str = "social_post") -> list[dict]:
    """Return cadence stats and slot collisions for a given content_type.

    Two row types in the returned list, distinguished by "kind":
      - {"kind": "daily",     "channel": str, "date": date,      "count": int}
      - {"kind": "collision", "channel": str, "sent_at": dt,     "count": int}

    A "daily" row is one (channel, calendar_date) bucket. A "collision"
    row is when two or more posts share the EXACT same sent_at on the
    same channel — i.e. they're scheduled to publish in the same slot.

    Returns empty list on any DB error in either query (atomic semantics).
    """
    days_int = int(days)

    daily_sql = """
        SELECT channel,
               date(COALESCE(sent_at, created_at)) AS day,
               COUNT(*) AS count
        FROM content_history
        WHERE content_type = %s
          AND COALESCE(sent_at, created_at) >= NOW() - (%s * INTERVAL '1 day')
        GROUP BY channel, date(COALESCE(sent_at, created_at))
        ORDER BY channel, day;
    """

    collision_sql = """
        SELECT channel,
               COALESCE(sent_at, created_at) AS sent_at_exact,
               COUNT(*) AS count
        FROM content_history
        WHERE content_type = %s
          AND COALESCE(sent_at, created_at) >= NOW() - (%s * INTERVAL '1 day')
        GROUP BY channel, COALESCE(sent_at, created_at)
        HAVING COUNT(*) > 1
        ORDER BY channel, sent_at_exact;
    """

    try:
        daily_rows = execute_query(
            daily_sql, params=(content_type, days_int), fetch=True
        )
        coll_rows = execute_query(
            collision_sql, params=(content_type, days_int), fetch=True
        )
    except Exception as e:
        logger.error(f"get_posting_activity: query failed: {e}", exc_info=True)
        return []

    result = []
    for r in daily_rows:
        result.append({
            "kind": "daily",
            "channel": r.get("channel"),
            "date": r.get("day"),
            "count": r.get("count"),
        })
    for r in coll_rows:
        result.append({
            "kind": "collision",
            "channel": r.get("channel"),
            "sent_at": r.get("sent_at_exact"),
            "count": r.get("count"),
        })
    return result


# ---------------------------------------------------------------------------
# 5. CTA normalization helper
# ---------------------------------------------------------------------------

def normalize_cta(cta):
    """Return None for missing/empty/'none' CTA, else the trimmed string.

    extract_topics stores the literal string 'none' (lowercase) when a
    post has no call-to-action; callers should treat that the same as
    a NULL row. Comparison is case-insensitive and whitespace-trimmed.
    """
    if not isinstance(cta, str):
        return None
    trimmed = cta.strip()
    if not trimmed:
        return None
    if trimmed.lower() == "none":
        return None
    return trimmed
