"""
Social Content Capture
======================
Backfill published HubSpot social broadcasts into content_history.

Pulls all SUCCESS/SUCCESS_ARCHIVE broadcasts via the HubSpot client,
extracts topics per post via the existing extract_topics() helper, and
inserts each row into content_history with ON CONFLICT DO NOTHING for
idempotency (deduped by content_type + external_id).

Resilient:
  - One post's topic-extraction failure logs and continues; the run
    proceeds with topics=None for that row.
  - One post's INSERT failure logs and continues with the next row.

Run standalone (e.g. from the Railway shell) via:
    /opt/venv/bin/python content/social_capture.py
"""

import json
import logging
import os
import sys

# Make the repo root importable when run as `python content/social_capture.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clients.database import execute_query
from intents.content_memory import extract_topics

logger = logging.getLogger(__name__)


# JSONB cast on topics matches the pattern in intents/content_memory.py's
# log_content INSERT. ON CONFLICT relies on uq_content_external
# (content_type, external_id) — the unique index in production.
_INSERT_SQL = """
    INSERT INTO content_history
        (content_type, channel, external_id, full_body, sent_at,
         source_url, topics, summary, cta, logged_by)
    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
    ON CONFLICT (content_type, external_id) DO NOTHING;
"""


def backfill_social_content() -> dict:
    """Pull published social broadcasts, tag topics, insert into content_history.

    Returns:
        {
          "fetched": <total returned by HubSpot>,
          "inserted": <new rows written>,
          "skipped_duplicate": <ON CONFLICT no-ops>,
          "topic_extraction_failures": <extract_topics raised>,
        }
    """
    # Lazy import — mirrors the pattern in intents.content_memory.run_email_backfill.
    from clients.hubspot import HubSpotClient

    hubspot = HubSpotClient()
    records = hubspot.get_published_social_broadcasts_with_content()

    summary = {
        "fetched": len(records),
        "inserted": 0,
        "skipped_duplicate": 0,
        "topic_extraction_failures": 0,
    }
    logger.info(f"backfill_social_content: fetched={summary['fetched']}")

    for rec in records:
        external_id = rec["external_id"]
        full_body = rec["full_body"]

        # Topic extraction — guarded even though extract_topics is documented
        # never-raises. Template .format() can still raise KeyError if
        # full_body contains literal {…} placeholders.
        topics = None
        summary_text = None
        cta_text = None
        if full_body:
            try:
                meta = extract_topics("social_post", full_body)
                topics = meta.get("topics")
                summary_text = meta.get("summary")
                cta_text = meta.get("cta")
            except Exception as e:
                logger.warning(
                    f"backfill_social_content: extract_topics failed for "
                    f"external_id={external_id}: {e}"
                )
                summary["topic_extraction_failures"] += 1
                topics = None
                summary_text = None
                cta_text = None

        topics_json = json.dumps(topics) if topics is not None else None

        try:
            rowcount = execute_query(
                _INSERT_SQL,
                params=(
                    "social_post",
                    rec["channel"],
                    external_id,
                    full_body,
                    rec["sent_at"],
                    rec["source_url"],
                    topics_json,
                    summary_text,
                    cta_text,
                    "system:social_backfill",
                ),
                fetch=False,
            )
        except Exception as e:
            logger.warning(
                f"backfill_social_content: INSERT failed for "
                f"external_id={external_id}: {e}"
            )
            continue

        if rowcount == 1:
            summary["inserted"] += 1
        else:
            summary["skipped_duplicate"] += 1

    logger.info(f"backfill_social_content: summary={summary}")
    return summary


if __name__ == "__main__":
    print(json.dumps(backfill_social_content(), indent=2))
