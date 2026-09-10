"""
CSuite Mirror Refresh
=====================
Thin CLI over sync.mirror.refresh.

READ-ONLY against CSuite. Writes only to csuite_mirror and sync_runs.

    # Everything, at the default 400ms pace:
    python scripts/mirror_refresh.py

    # See what would change without writing a mirror row:
    python scripts/mirror_refresh.py --dry-run

    # One type, slowly, because CSuite is busy:
    python scripts/mirror_refresh.py --types profile --pace-ms 800

    # Half now, half later — the safe way to fill an empty mirror:
    python scripts/mirror_refresh.py --budget 500

A full run is roughly 960 CSuite calls — 399 of them the funit/display
sweep and 267 the pages of donation/list. At the default 150ms pace and
CSuite's measured latency that is about 7-11 minutes. Run it from
the Railway shell, where DATABASE_URL and the CSuite credentials are
already in the environment.

Probe #3 completed 440 calls at this pace without a rate limit, so a full
run is roughly twice as long a stretch as anything measured. If it comes
back "rate limited", raise --pace-ms or refresh in two passes with
--types rather than retrying immediately.

Exit codes: 0 every type completed, 1 at least one type failed.
"""

import argparse
import logging
import os
import sys

# Make the repo root importable when run as `python scripts/mirror_refresh.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clients.csuite_fetch import CallBudget  # noqa: E402
from sync.mirror import RECORD_TYPES, refresh  # noqa: E402


# Column widths for the summary table. Fixed rather than computed: the
# point is that two runs line up when you scroll back through a log.
COLUMNS = (
    ("type", 13, "<"),
    ("expected", 9, ">"),
    ("fetched", 8, ">"),
    ("complete", 9, ">"),
    ("written", 8, ">"),
    ("unchanged", 10, ">"),
    ("deleted", 8, ">"),
    ("calls", 6, ">"),
    ("reused", 7, ">"),
    ("429s", 5, ">"),
    ("seconds", 8, ">"),
)


def _cell(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "NO"
    if isinstance(value, float):
        return f"{value:.1f}"
    return str(value)


def _row(values) -> str:
    return "  ".join(
        f"{_cell(value):{align}{width}}"
        for value, (_, width, align) in zip(values, COLUMNS)
    )


def _header() -> str:
    heading = _row([name for name, _, _ in COLUMNS])
    return f"{heading}\n{'-' * len(heading)}"


def parse_types(raw: str) -> list:
    """--types fund,event -> ['fund', 'event'], validated.

    Rejects an unknown name instead of quietly refreshing a subset: a typo
    that silently skips a record type is exactly the kind of half-done job
    a mirror should not have.
    """
    if not raw:
        return list(RECORD_TYPES)

    names = [part.strip() for part in raw.split(",") if part.strip()]
    unknown = [name for name in names if name not in RECORD_TYPES]
    if unknown:
        raise argparse.ArgumentTypeError(
            f"unknown record type(s): {', '.join(unknown)}. "
            f"Choose from: {', '.join(RECORD_TYPES)}")
    return names or list(RECORD_TYPES)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fill csuite_mirror from CSuite. Read-only on CSuite.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Record types: {', '.join(RECORD_TYPES)}",
    )
    parser.add_argument(
        "--types",
        type=parse_types,
        default=list(RECORD_TYPES),
        help="Comma-separated record types to refresh. Default: all.",
    )
    parser.add_argument(
        "--pace-ms",
        type=int,
        default=None,
        help="Milliseconds between CSuite calls. "
             "Default: $CSUITE_PACE_MS, or 400.",
    )
    parser.add_argument(
        "--budget",
        type=int,
        default=None,
        help="Stop cleanly before making more than N CSuite calls this "
             "run, across all types. Default: no budget.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch, hash and compare, but write no mirror rows. "
             "Still records a sync_runs row with dry_run=true.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only print the summary table, not the progress log.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stderr,
    )

    mode = "DRY RUN — nothing will be written" if args.dry_run else "writing"
    print(f"CSuite mirror refresh ({mode})")
    print(f"Types: {', '.join(args.types)}")
    if args.budget:
        print(f"Budget: {args.budget} CSuite calls")
    print()

    budget = CallBudget(args.budget) if args.budget else None

    results = refresh(
        record_types=args.types,
        pace_ms=args.pace_ms,
        dry_run=args.dry_run,
        trigger_source="cli",
        triggered_by="cli:mirror_refresh",
        budget=budget,
    )

    print(_header())
    for result in results:
        print(_row([
            result.record_type,
            result.expected,
            result.fetched,
            result.complete,
            result.written,
            result.unchanged,
            result.deleted,
            result.calls,
            result.reused_staged,
            result.total_429s,
            result.seconds,
        ]))

    total_calls = sum(r.calls for r in results)
    total_seconds = sum(r.seconds for r in results)
    print(f"\n{total_calls} CSuite calls, {total_seconds:.1f}s total.")

    first_429 = next((r.first_429_at for r in results if r.first_429_at), None)
    total_429s = sum(r.total_429s for r in results)
    if total_429s:
        print(f"Rate limited {total_429s}x, first at {first_429}. "
              "The ledger has the rest: "
              "SELECT notes FROM sync_runs WHERE sync_type = 'mirror'.")

    if any(r.record_type == "donation_agg" for r in results):
        # The two columns count different things for this one type, which
        # would otherwise read as 26,000 records mysteriously becoming 4,000.
        print("donation_agg: 'expected' counts donations read from CSuite; "
              "'fetched' counts the per-profile aggregates they became.")

    if args.dry_run:
        print("Dry run: 'written' is 0 and 'deleted' is what WOULD be "
              "deleted. No mirror rows changed.")

    unfinished = [r for r in results if r.status != "complete"
                  and not (args.dry_run and r.status == "verified")]
    if not unfinished:
        return 0

    # A budget stop is a clean stop we asked for, not a failure. Shouting
    # "FAILED" at someone who set --budget is how a working safeguard
    # gets switched off.
    stopped = [r for r in results if r.stop_reason]
    print("\nStopped early — nothing was written for these types:"
          if stopped else
          "\nFAILED — nothing was written for these types:")
    for result in unfinished:
        print(f"  {result.record_type}: {result.error or result.status}")

    if stopped:
        print("\nNothing is lost. Fund displays fetched this run are staged "
              "and will be reused for 24h — run the same command again in a "
              "few minutes to continue.")

    return 1


if __name__ == "__main__":
    sys.exit(main())
