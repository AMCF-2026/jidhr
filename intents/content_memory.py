"""
Content Memory
==============
Shared module for content history: extracts structured metadata via
Claude and reads/writes the content_history table.

Used by:
  - Email and social save flows (log content after it goes out).
  - The backfill script (import past content).
  - Draft-time context lookup ("what have we covered recently?").

Failure policy: extract_topics() never raises; log_content() returns
None on any DB error; get_recent_content() returns [] on any DB error.
These are shared helpers — callers must not fail because logging or
extraction hiccupped.
"""

import json
import logging

from clients.database import execute_query
from clients.openrouter import OpenRouterClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Topic extraction
# ---------------------------------------------------------------------------

_EXTRACT_SYSTEM_PROMPT = (
    "You extract structured metadata from AMCF marketing content. "
    "Always return valid JSON, no preamble, no code fences."
)

_EXTRACT_USER_TEMPLATE = """\
Extract from this {content_type}:

1. TOPICS: 3-6 short tags (1-3 words each). Use specific names — "Nonprofit Summit 2026" not "event", "EverWaqf launch" not "fundraising".
2. SUMMARY: One sentence describing what was covered.
3. CTA: The primary call-to-action. If none, return "none".

CONTENT:
{full_body}

Return ONLY JSON:
{{
  "topics": ["tag1", "tag2", "tag3"],
  "summary": "One sentence.",
  "cta": "Primary call-to-action."
}}
"""

_EXTRACT_DEFAULT = {
    "topics": [],
    "summary": "(extraction failed)",
    "cta": "none",
}


def _strip_code_fences(text: str) -> str:
    """Strip leading ```json/``` opener and trailing ``` closer if present."""
    s = (text or "").strip()
    if s.startswith("```"):
        first_nl = s.find("\n")
        if first_nl != -1:
            s = s[first_nl + 1:]
        else:
            s = s[3:]
    if s.endswith("```"):
        s = s[:-3]
    return s.strip()


def extract_topics(content_type: str, full_body: str) -> dict:
    """Extract topics/summary/cta from a piece of content via Claude.

    Never raises. On any failure (network, parse, missing key) returns
    the safe default so callers cannot break because of extraction.
    """
    user_prompt = _EXTRACT_USER_TEMPLATE.format(
        content_type=content_type,
        full_body=full_body,
    )

    try:
        client = OpenRouterClient()
        raw = client.chat(
            messages=[{"role": "user", "content": user_prompt}],
            system_prompt=_EXTRACT_SYSTEM_PROMPT,
        )
    except Exception as e:
        logger.warning(f"extract_topics: Claude call raised: {e}")
        return dict(_EXTRACT_DEFAULT)

    cleaned = _strip_code_fences(raw)

    try:
        parsed = json.loads(cleaned)
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(
            f"extract_topics: JSON parse failed ({e}); raw response: {raw!r}"
        )
        return dict(_EXTRACT_DEFAULT)

    if not isinstance(parsed, dict):
        logger.warning(f"extract_topics: parsed JSON not a dict: {parsed!r}")
        return dict(_EXTRACT_DEFAULT)

    return {
        "topics": parsed.get("topics") or [],
        "summary": parsed.get("summary") or "(no summary)",
        "cta": parsed.get("cta") or "none",
    }


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------

_INSERT_SQL = """
    INSERT INTO content_history
        (content_type, channel, external_id, title, topics, summary,
         cta, full_body, sent_at, logged_by)
    VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s)
    RETURNING id;
"""


def log_content(content_type, channel, external_id, title, full_body,
                sent_at, logged_by):
    """Extract topics and insert a row into content_history.

    Returns the new row's id on success, or None on any failure.
    Callers (email/social save) must succeed even if logging fails.
    """
    meta = extract_topics(content_type, full_body)
    topics_json = json.dumps(meta["topics"])

    try:
        rows = execute_query(
            _INSERT_SQL,
            params=(
                content_type,
                channel,
                external_id,
                title,
                topics_json,
                meta["summary"],
                meta["cta"],
                full_body,
                sent_at,
                logged_by,
            ),
            fetch=True,
        )
    except Exception as e:
        logger.error(f"log_content: DB insert failed: {e}", exc_info=True)
        return None

    if not rows:
        logger.error("log_content: INSERT...RETURNING produced no rows")
        return None

    new_id = rows[0].get("id")
    logger.info(
        f"log_content: inserted id={new_id} type={content_type} channel={channel}"
    )
    return new_id


# ---------------------------------------------------------------------------
# DB read
# ---------------------------------------------------------------------------

def get_recent_content(content_type, channel=None, days=90):
    """Return recent content_history rows (no full_body, to keep payload small).

    Always returns a list (empty on no matches or on DB error).
    """
    days_int = int(days)

    sql = """
        SELECT id, content_type, channel, external_id, title, topics,
               summary, cta, sent_at, logged_by
        FROM content_history
        WHERE content_type = %s
    """
    params = [content_type]

    if channel is not None:
        sql += " AND channel = %s"
        params.append(channel)

    sql += " AND sent_at >= NOW() - (%s * INTERVAL '1 day')"
    params.append(days_int)

    sql += " ORDER BY sent_at DESC;"

    try:
        rows = execute_query(sql, params=tuple(params), fetch=True)
        return rows or []
    except Exception as e:
        logger.error(f"get_recent_content: query failed: {e}", exc_info=True)
        return []
