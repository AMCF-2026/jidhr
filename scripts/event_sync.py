"""
Event Sync — CSuite event dates to HubSpot marketing events
===========================================================

    python scripts/event_sync.py                 # dry run (the default)
    python scripts/event_sync.py --apply         # writes to HubSpot
    python scripts/event_sync.py --out FILE.md   # dry-run report to a file

One way. CSuite is read-only, enforced by sync/event_hubspot.read_only_endpoint
rather than left to care. HubSpot is never deleted from: an event date that
vanishes or is archived in CSuite is recorded for review.

Requires hubsync.event_map and hubsync.run_log — migrations/001_hubsync_event_map.sql.
This script does NOT create them; it reports their absence and stops,
because a job that silently creates its own tables is a job nobody
reviewed the schema of.

Exit codes: 0 · 1 a read failed · 2 the migration has not been run.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clients import database  # noqa: E402,F401
from sync import event_hubspot as eh  # noqa: E402
# One implementation, shared with the chat command. Everything that touches
# hubsync or writes to HubSpot lives there; this file is the CLI and the
# report around it.
from sync.event_apply import (DEFAULT_ORGANIZER, FirstFailureStop,  # noqa: E402
                              apply_plan, load_map, migration_applied, plan,
                              record_run, summarise_outcomes)
from sync.event_apply import CREATE_ENDPOINT  # noqa: E402,F401

EXIT_OK = 0
EXIT_READ_FAILED = 1
EXIT_NO_MIGRATION = 2
EXIT_STOPPED = 3        # --apply halted on the first bad create

def render(result, fetched, hs_calls, applied, organizer) -> str:
    c, u, n = result["creates"], result["updates"], result["unchanged"]
    s, r = result["skipped"], result["review"]
    lines = [
        f"# Event sync — {'APPLIED' if applied else 'DRY RUN'} "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "CSuite read-only, enforced by `read_only_endpoint()`. "
        f"{'HubSpot was written to.' if applied else '**No HubSpot write was made.**'}",
        "",
        "| | |", "|---|---|",
        f"| CSuite event dates read | {len(fetched.rows)} |",
        f"| CSuite calls | {fetched.calls} (budget {eh.CALL_BUDGET}) |",
        f"| CSuite 429s | {fetched.total_429s} |",
        f"| HubSpot read calls | {hs_calls} |",
        f"| would create | **{len(c)}** |",
        f"| would update | **{len(u)}** |",
        f"| unchanged | {len(n)} |",
        f"| not syncable (skipped) | {len(s)} |",
        f"| flagged for review | {len(r)} |",
        f"| event organizer sent | {organizer} |",
        "",
    ]

    lines += ["## Sample of 5 mapped records", ""]
    sample = [(m, "CREATE", why) for m, why in c][:5]
    sample += [(m, "UPDATE", why) for m, _h, why in u][:max(0, 5 - len(sample))]
    if not sample:
        lines.append("Nothing to create or update.")
    for mapped, action, why in sample[:5]:
        lines += [
            f"### {action} `{mapped.external_event_id}` — {why}", "",
            "```json",
            json.dumps(mapped.payload, indent=2, ensure_ascii=False),
            "```",
            f"content_hash `{mapped.content_hash[:16]}…`"
            + (f"  \n⚠️ review: {mapped.review_reason}"
               if mapped.review_reason else ""),
            "",
        ]

    if r:
        lines += ["## Flagged for review — nothing was done to these", ""]
        for mapped, reason in r[:40]:
            lines.append(f"- `{mapped.external_event_id}` — {reason}")
        if len(r) > 40:
            lines.append(f"- … {len(r) - 40} more")
        lines.append("")

    if s:
        from collections import Counter
        lines += ["## Not syncable", ""]
        for reason, count in Counter(reason for _m, reason in s).most_common():
            lines.append(f"- {count} × {reason}")
        lines += ["", "Every one of them, by CSuite id and name — so the "
                  "list can be worked through rather than described:", "",
                  "| csuite_eventdate_id | name | reason |", "|---|---|---|"]
        for mapped, reason in sorted(
                s, key=lambda pair: (len(pair[0].csuite_eventdate_id),
                                     pair[0].csuite_eventdate_id)):
            name = (mapped.source_name or "(no name)").replace("|", "/")
            lines.append(f"| `{mapped.csuite_eventdate_id}` | {name[:70]} "
                         f"| {reason} |")
        lines.append("")

    lines += ["## Per-record outcome counts", "",
              f"- create {len(c)} · update {len(u)} · unchanged {len(n)} "
              f"· skipped {len(s)} · review {len(r)}", ""]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# The write half. Only --apply reaches it.
# ---------------------------------------------------------------------------



def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Sync CSuite event dates to HubSpot marketing events. "
                    "Dry run unless --apply. CSuite is read-only.")
    parser.add_argument("--apply", action="store_true",
                        help="Write to HubSpot. Without this nothing is "
                             "written anywhere.")
    parser.add_argument("--out", default=None, help="Write the report here.")
    parser.add_argument("--organizer", default=DEFAULT_ORGANIZER)
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="With --apply, write at most N records this "
                             "run. Updates count toward N as well as "
                             "creates: the point is to bound how much one "
                             "run can change.")
    parser.add_argument("--pace-ms", type=int, default=None)
    args = parser.parse_args(argv)

    # The migration gates --apply, not the dry run. A dry run writes
    # nothing, so it can and should be usable before the SQL has been
    # run — that is how the SQL gets reviewed with real numbers beside
    # it. --apply without the tables would have nowhere to record what it
    # did, which is the case worth refusing.
    have_tables = migration_applied()
    if args.apply and not have_tables:
        print("hubsync.event_map / hubsync.run_log are missing. Run "
              "migrations/001_hubsync_event_map.sql first — this script "
              "does not create its own tables, and --apply with nowhere to "
              "record what it did is how a create becomes a duplicate.",
              file=sys.stderr)
        return EXIT_NO_MIGRATION
    if not have_tables:
        print("note: hubsync tables are absent, so every event date reads "
              "as unmapped. Run migrations/001_hubsync_event_map.sql before "
              "--apply.", file=sys.stderr)

    from clients.csuite import CSuiteClient
    from clients.hubspot import HubSpotClient

    fetched = eh.fetch_event_dates(CSuiteClient(), pace_ms=args.pace_ms)
    if not fetched.complete:
        print(f"CSuite read incomplete: {fetched.error}", file=sys.stderr)
        return EXIT_READ_FAILED

    hubspot = HubSpotClient()
    index, hs_calls, hs_error = eh.hubspot_index(hubspot)
    if hs_error:
        print(f"HubSpot marketing-event read failed: {hs_error}",
              file=sys.stderr)
        return EXIT_READ_FAILED

    # Skipped entirely when the tables are absent, rather than caught:
    # a failing query logs a traceback, and a traceback in a dry run reads
    # as something broken.
    result = plan(fetched.rows, load_map() if have_tables else {},
                  index, args.organizer)
    report = render(result, fetched, hs_calls, args.apply, args.organizer)

    if args.out:
        directory = os.path.dirname(args.out)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(report)

    print(report)

    if not args.apply:
        # Say what was actually written. This used to claim "nothing was
        # written to HubSpot or to hubsync" on the line after record_run had
        # inserted a run_log row — record_run's own docstring says a dry run
        # is logged, so the message contradicted the code above it.
        if have_tables:
            record_run(result, fetched, hs_calls, applied=False)
            print("\nDRY RUN — nothing was written to HubSpot. One "
                  "hubsync.run_log row was written, recording that this "
                  "preview happened.")
        else:
            print("\nDRY RUN — nothing was written anywhere; hubsync is not "
                  "available, so not even the run log.")
        return EXIT_OK

    try:
        outcomes = apply_plan(hubspot, result, limit=args.limit)
        stopped = None
    except FirstFailureStop as stop:
        outcomes, stopped = stop.outcomes, stop.reason

    record_run(result, fetched, hs_calls, applied=True, outcomes=outcomes)
    for line in summarise_outcomes(outcomes):
        print(line)
    if stopped:
        print(f"\nSTOPPED: {stopped}\nNothing further was created. Re-run "
              "after checking HubSpot — the next run resolves the "
              "ambiguous record by lookup rather than retrying it.",
              file=sys.stderr)
        return EXIT_STOPPED
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
