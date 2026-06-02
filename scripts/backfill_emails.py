"""
Email Backfill Script
=====================
Backfill sent HubSpot marketing emails (last 90 days) into the
content_history table. Idempotent — re-running skips emails already
logged (matched by external_id).

# Run from Railway shell (web service):
#   /opt/venv/bin/python scripts/backfill_emails.py                       # 90 days (default)
#   /opt/venv/bin/python scripts/backfill_emails.py --days-back 365       # 12 months
#   /opt/venv/bin/python scripts/backfill_emails.py --limit 3             # quick test
#   /opt/venv/bin/python scripts/backfill_emails.py --limit 3 --days-back 365
# If that Python path isn't right, find it with: which gunicorn
# Test with --limit 3 first. Inspect rows in TablePlus.
# Then re-run without --limit for the full window.
"""

import argparse
import os
import sys

# Make the repo root importable when run as `python scripts/backfill_emails.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clients.database import execute_query
from clients.hubspot import HubSpotClient
from intents.content_memory import log_content


def _classify_channel(email: dict) -> str:
    from_name = (email.get("from_name") or "").lower()
    reply_to = (email.get("reply_to") or "").lower()
    if "giving circle" in from_name or "givingcircles" in reply_to:
        return "giving_circle"
    return "amcf_newsletter"


def _topics_for(row_id):
    """Read back the topics array for a freshly inserted row (for display)."""
    try:
        rows = execute_query(
            "SELECT topics FROM content_history WHERE id = %s",
            params=(row_id,),
            fetch=True,
        )
    except Exception:
        return []
    if not rows:
        return []
    topics = rows[0].get("topics") or []
    if isinstance(topics, list):
        return topics[:3]
    return []


def main():
    parser = argparse.ArgumentParser(
        description="Backfill sent HubSpot emails into content_history."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap the number of emails processed (after dedup ordering). Default: no limit.",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=90,
        help="How many days of history to fetch from HubSpot. Default: 90.",
    )
    args = parser.parse_args()

    limit_label = args.limit if args.limit is not None else "no limit"
    print(f"Starting email backfill — last {args.days_back} days (limit: {limit_label})...")

    # 1. Existing external_ids
    try:
        rows = execute_query(
            "SELECT external_id FROM content_history "
            "WHERE content_type = %s AND external_id IS NOT NULL",
            params=("email",),
            fetch=True,
        )
    except Exception as e:
        print(f"Failed to query existing content_history rows: {e}")
        return 1

    existing = {r["external_id"] for r in rows}
    print(f"Found {len(existing)} email(s) already logged.")

    # 2. Fetch from HubSpot
    hubspot = HubSpotClient()
    emails = hubspot.get_sent_emails_with_content(days_back=args.days_back)

    if isinstance(emails, dict) and "error" in emails:
        print(f"HubSpot error: {emails['error']}")
        return 1

    print(f"Fetched {len(emails)} email(s) from HubSpot.")

    # 3. Apply --limit
    if args.limit is not None:
        emails = emails[: args.limit]
        print(f"Limited to {len(emails)} after --limit.")

    # 4. Iterate
    processed = 0
    skipped = 0
    failed = 0

    for email in emails:
        ext_id = email.get("id")
        subject = email.get("subject") or "(no subject)"

        if ext_id in existing:
            print(f"↷ Skipped (dup): {subject}")
            skipped += 1
            continue

        channel = _classify_channel(email)

        try:
            new_id = log_content(
                content_type="email",
                channel=channel,
                external_id=ext_id,
                title=subject,
                full_body=email.get("plain_body", ""),
                sent_at=email.get("sent_at"),
                logged_by="system_backfill",
            )
        except Exception as e:
            failed += 1
            print(f"✗ Failed: {subject} — {e}")
            continue

        if new_id is None:
            failed += 1
            print(f"✗ Failed: {subject} — log_content returned None (see server logs)")
            continue

        topics = _topics_for(new_id)
        processed += 1
        print(f"✓ Logged: {subject} ({channel}) — topics: {topics}")

    print(f"\nProcessed: {processed} | Skipped: {skipped} | Failed: {failed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
