"""One-off backfill of past CSuite events into HubSpot (V5.92). Create-only.

    python scripts/event_backfill.py --since 2026-01-01
    python scripts/event_backfill.py --since 2026-01-01 --include-archived \
        --assume-eastern --show-payload 1430
    python scripts/event_backfill.py --since 2026-01-01 --include-archived \
        --assume-eastern --apply
    python scripts/event_backfill.py --since 2026-01-01 --include-archived \
        --assume-eastern --exclude csuite-1161,csuite-1264

Preview unless --apply. Never updates or adopts an event HubSpot already has.
See sync/event_backfill.py for the rules.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main(argv=None) -> int:
    from sync import event_backfill as backfill

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--since", required=True, help="YYYY-MM-DD")
    parser.add_argument("--include-archived", action="store_true")
    parser.add_argument("--assume-eastern", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=backfill.DEFAULT_LIMIT)
    parser.add_argument("--exclude", default="", metavar="csuite-ID,...",
                        help="event dates to leave out, listed separately "
                             "in the preview")
    parser.add_argument("--show-payload", metavar="EVENT_DATE_ID",
                        help="print the HubSpot payload for this event "
                             "(the first one if it is not released)")
    args = parser.parse_args(argv)
    try:
        out = backfill.run(since=args.since,
                           include_archived=args.include_archived,
                           assume_eastern=args.assume_eastern,
                           apply=args.apply, limit=args.limit,
                           exclude=args.exclude)
    except backfill.Refused as e:
        print(f"Event backfill did not run: {e}", file=sys.stderr)
        return 2
    print(backfill.report(out, show_payload_for=args.show_payload))
    return 1 if (out.get("stopped") or out.get("failed")) else 0


if __name__ == "__main__":
    sys.exit(main())
