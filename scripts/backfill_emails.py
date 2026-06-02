"""
Email Backfill Script
=====================
Thin CLI wrapper around intents.content_memory.run_email_backfill.

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

from intents.content_memory import run_email_backfill


def main():
    parser = argparse.ArgumentParser(
        description="Backfill sent HubSpot emails into content_history."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap the number of emails processed. Default: no limit.",
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

    result = run_email_backfill(days_back=args.days_back, limit=args.limit)

    print(
        f"\nProcessed: {result['processed']} | "
        f"Skipped: {result['skipped']} | "
        f"Failed: {result['failed']}"
    )

    if result["errors"]:
        print("\nErrors:")
        for err in result["errors"]:
            print(
                f"  - id={err.get('id')} "
                f"subject={err.get('subject')!r} "
                f"error={err.get('error')}"
            )

    return 1 if result["failed"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
