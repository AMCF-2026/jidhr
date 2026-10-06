"""Preview the donation sync. Reads only; writes nothing, anywhere.

    python scripts/donation_preview.py                      # 500-row sample
    python scripts/donation_preview.py --full               # every profile
    python scripts/donation_preview.py --full --out report.md

Why this is a script and not a chat command
-------------------------------------------
A full preview reads ~18,800 CSuite profiles (189 paged calls) and ~26,600
donations (266 calls), then searches HubSpot once per donor profile that has
an email — 7,604 of them, measured against the mirror on 2026-10-06.

HubSpot's Search API is capped at 5 requests per second across all object
types, separately from the per-app burst limit and regardless of tier:

  https://developers.hubspot.com/docs/developer-tooling/platform/usage-guidelines
  https://developers.hubspot.com/docs/api/usage-details

This paces at 4/s to stay clear of that cap, so the HubSpot half alone takes
about 32 minutes (7,604 / 4 = 1,901 seconds); even pacing right at the cap it
would be about 25. gunicorn kills a request at 180 seconds, so run from chat
it would die roughly a tenth of the way in, take its worker's other in-flight
requests with it, and discard everything it had read. "sync donations dry run
full" in chat prints this command instead of attempting it.

The same planner either way: this calls the same DonationSync.sync and the
same report formatter the chat command uses, so a preview here and a preview
there cannot disagree.

Exit codes: 0 ok · 1 a read failed · 2 the read was partial.
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from intents.sync_commands import (  # noqa: E402
    LINK_ROWS_SHOWN, _format_donation_sync_results)
from sync.donations import DonationSync  # noqa: E402

EXIT_OK = 0
EXIT_READ_FAILED = 1
EXIT_PARTIAL = 2


def progress_printer(started):
    def report(line: str):
        elapsed = time.monotonic() - started
        print(f"  [{elapsed:6.1f}s] {line}", file=sys.stderr, flush=True)
    return report


def header(results: dict, full: bool) -> str:
    from datetime import datetime, timezone

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return "\n".join([
        f"# Donation sync preview — {'FULL' if full else 'SAMPLE'}",
        "",
        f"Generated {stamp} by `scripts/donation_preview.py`. "
        "**Nothing was written.**",
        "",
        "| | |",
        "|---|---|",
        f"| CSuite profiles read | {results.get('profiles_read', 0):,} |",
        f"| CSuite donations read | {results.get('donations_read', 0):,} |",
        f"| CSuite calls | {results.get('csuite_calls', 0):,} |",
        f"| HubSpot contact searches | "
        f"{results.get('hubspot_searches', 0):,} |",
        f"| CSuite rate-limit waits | "
        f"{results.get('csuite_rate_limit_waits', 0)} |",
        f"| HubSpot rate-limit waits | "
        f"{results.get('hubspot_rate_limit_waits', 0)} |",
        f"| Read complete | "
        f"{'NO — see below' if results.get('partial') else 'yes'} |",
        "",
    ])


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full", action="store_true",
                        help="Page every profile and donation, not a sample "
                             "of 500. Takes tens of minutes.")
    parser.add_argument("--out", default=None,
                        help="Write the report here instead of stdout.")
    parser.add_argument("--pace-ms", type=int, default=None,
                        help="Extra pause between CSuite calls.")
    args = parser.parse_args(argv)

    started = time.monotonic()
    print(f"Donation preview — {'FULL' if args.full else 'sample'}. "
          "Nothing will be written.", file=sys.stderr)
    if args.full:
        print("  Expect tens of minutes: the HubSpot search cap is 5/s and "
              "this searches once per donor.", file=sys.stderr)

    sync = DonationSync(pace_ms=args.pace_ms,
                        progress=progress_printer(started))
    results = sync.sync(dry_run=True, quick=not args.full)
    sync.resolve_shown_links(results, limit=LINK_ROWS_SHOWN)

    report = header(results, args.full) + _format_donation_sync_results(
        results, dry_run=True)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(report + "\n")
        print(f"\nReport written to {args.out}", file=sys.stderr)
    else:
        print(report)

    elapsed = time.monotonic() - started
    print(f"Done in {elapsed:.1f}s. Nothing was written.", file=sys.stderr)

    if results.get("partial"):
        print(f"\nPARTIAL READ: {results.get('partial_reason')}\n"
              "The counts above are NOT totals. Re-run when CSuite is "
              "answering.", file=sys.stderr)
        return EXIT_PARTIAL
    if results.get("errors"):
        return EXIT_READ_FAILED
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
