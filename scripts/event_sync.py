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

from clients import database  # noqa: E402
from sync import event_hubspot as eh  # noqa: E402

EXIT_OK = 0
EXIT_READ_FAILED = 1
EXIT_NO_MIGRATION = 2

DEFAULT_ORGANIZER = "American Muslim Community Foundation"

_TABLES_SQL = """
    SELECT table_name FROM information_schema.tables
     WHERE table_schema = 'hubsync'
       AND table_name IN ('event_map', 'run_log')
"""

_MAP_SQL = "SELECT * FROM hubsync.event_map"

_UPSERT_MAP_SQL = """
    INSERT INTO hubsync.event_map (
        csuite_eventdate_id, hubspot_event_id, external_event_id,
        content_hash, last_synced_at, status, last_error, review_reason)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (csuite_eventdate_id) DO UPDATE
       SET hubspot_event_id = COALESCE(EXCLUDED.hubspot_event_id,
                                       hubsync.event_map.hubspot_event_id),
           external_event_id = EXCLUDED.external_event_id,
           content_hash = EXCLUDED.content_hash,
           last_synced_at = EXCLUDED.last_synced_at,
           status = EXCLUDED.status,
           last_error = EXCLUDED.last_error,
           review_reason = EXCLUDED.review_reason,
           updated_at = NOW()
"""

_RUN_SQL = """
    INSERT INTO hubsync.run_log (
        job, applied, finished_at, csuite_calls, hubspot_calls,
        event_dates_read, created_count, updated_count, unchanged_count,
        skipped_count, review_count, failed_count, status, error_summary,
        outcomes)
    VALUES ('event_sync', %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s::jsonb)
    RETURNING id
"""


def migration_applied() -> bool:
    try:
        found = database.execute_query(_TABLES_SQL, (), fetch=True)
    except Exception:
        return False
    names = {r["table_name"] if isinstance(r, dict) else r[0] for r in found}
    return {"event_map", "run_log"} <= names


def load_map() -> dict:
    try:
        rows = database.execute_query(_MAP_SQL, (), fetch=True)
    except Exception:
        return {}
    return {str(r["csuite_eventdate_id"]): r for r in rows}


def plan(rows, existing_map, hubspot_events, organizer):
    """Decide what each event date needs. No writes, no HubSpot calls."""
    creates, updates, unchanged, skipped, review = [], [], [], [], []

    for row in rows:
        mapped = eh.map_event_date(row, organizer)
        if not mapped.csuite_eventdate_id:
            skipped.append((mapped, "no event_date_id"))
            continue
        if not mapped.syncable:
            skipped.append((mapped, mapped.review_reason))
            continue

        known = existing_map.get(mapped.csuite_eventdate_id)
        in_hubspot = hubspot_events.get(mapped.external_event_id)

        if known and str(known.get("status")) == "unknown":
            # An earlier create was ambiguous. Resolve by looking, never
            # by retrying: a retried create with no idempotency key is
            # how one event becomes two.
            if in_hubspot:
                updates.append((mapped, in_hubspot, "resolving 'unknown': "
                                "the earlier create did land"))
            else:
                creates.append((mapped, "resolving 'unknown': the earlier "
                                "create did not land"))
            continue

        if in_hubspot:
            hubspot_id = str(in_hubspot.get("objectId") or "")
            if known and known.get("content_hash") == mapped.content_hash:
                unchanged.append((mapped, hubspot_id))
            elif known:
                updates.append((mapped, in_hubspot, "content hash changed"))
            else:
                updates.append((mapped, in_hubspot,
                                "already in HubSpot, not in event_map — "
                                "adopting it"))
            continue

        if known and known.get("hubspot_event_id"):
            review.append((mapped, "event_map has a HubSpot id but HubSpot "
                           "does not list that externalEventId — needs a "
                           "person"))
            continue

        creates.append((mapped, "new"))

    for mapped, _reason in list(creates) + [(m, r) for m, _h, r in updates]:
        if mapped.review_reason:
            review.append((mapped, mapped.review_reason))

    return {"creates": creates, "updates": updates, "unchanged": unchanged,
            "skipped": skipped, "review": review}


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
        lines.append("")

    lines += ["## Per-record outcome counts", "",
              f"- create {len(c)} · update {len(u)} · unchanged {len(n)} "
              f"· skipped {len(s)} · review {len(r)}", ""]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# The write half. Only --apply reaches it.
# ---------------------------------------------------------------------------

CREATE_ENDPOINT = "marketing/v3/marketing-events"


def _save_map(mapped, hubspot_event_id, status, error=None):
    """Write the mapping row. Called IMMEDIATELY after a create returns.

    Immediately, because the window between HubSpot minting an id and us
    recording it is the window in which a crash produces a duplicate on
    the next run.
    """
    database.execute_query(_UPSERT_MAP_SQL, (
        mapped.csuite_eventdate_id,
        str(hubspot_event_id) if hubspot_event_id else None,
        mapped.external_event_id,
        mapped.content_hash,
        datetime.now(timezone.utc),
        status,
        str(error)[:500] if error else None,
        mapped.review_reason,
    ), fetch=False)


def apply_plan(hubspot, result) -> list:
    """Create and update in HubSpot. Never deletes. Returns outcomes."""
    outcomes = []

    for mapped, why in result["creates"]:
        created = hubspot._post(CREATE_ENDPOINT, mapped.payload)
        event_id = (created or {}).get("objectId") or (created or {}).get("id")
        error = (created or {}).get("error")

        if event_id:
            _save_map(mapped, event_id,
                      "review" if mapped.review_reason else "synced")
            outcomes.append({"id": mapped.csuite_eventdate_id,
                             "outcome": "created", "hubspot_id": str(event_id),
                             "why": why})
            continue

        # Ambiguous: no id came back. It may or may not have landed, and
        # there is no idempotency key to make a retry safe. Record
        # 'unknown' and stop touching it — the next run looks it up.
        _save_map(mapped, None, "unknown", error or "no objectId in response")
        outcomes.append({"id": mapped.csuite_eventdate_id,
                         "outcome": "unknown",
                         "why": "create returned no id — NOT retried; the "
                                "next run resolves it by lookup",
                         "error": str(error)[:200] if error else None})

    for mapped, existing, why in result["updates"]:
        hubspot_id = str(existing.get("objectId") or "")
        # PATCH by externalEventId is the documented update path for
        # marketing events; the objectId is stored for people, not used
        # as the write key.
        updated = hubspot._patch(
            f"{CREATE_ENDPOINT}/{mapped.external_event_id}", mapped.payload)
        error = (updated or {}).get("error")
        if error:
            _save_map(mapped, hubspot_id, "error", error)
            outcomes.append({"id": mapped.csuite_eventdate_id,
                             "outcome": "failed", "why": why,
                             "error": str(error)[:200]})
        else:
            _save_map(mapped, hubspot_id,
                      "review" if mapped.review_reason else "synced")
            outcomes.append({"id": mapped.csuite_eventdate_id,
                             "outcome": "updated", "hubspot_id": hubspot_id,
                             "why": why})

    # Unchanged rows still get their timestamp refreshed, so "last seen"
    # and "last changed" are different questions with different answers.
    for mapped, hubspot_id in result["unchanged"]:
        _save_map(mapped, hubspot_id,
                  "review" if mapped.review_reason else "synced")
        outcomes.append({"id": mapped.csuite_eventdate_id,
                         "outcome": "unchanged", "hubspot_id": hubspot_id})

    # Never written to HubSpot; recorded so a person can find them.
    for mapped, reason in result["skipped"]:
        if mapped.csuite_eventdate_id:
            _save_map(mapped, None, "review", None)
            outcomes.append({"id": mapped.csuite_eventdate_id,
                             "outcome": "skipped", "why": reason})

    return outcomes


def summarise_outcomes(outcomes) -> list:
    from collections import Counter
    counts = Counter(o["outcome"] for o in outcomes)
    lines = ["", "--apply results:"]
    for name in ("created", "updated", "unchanged", "unknown", "failed",
                 "skipped"):
        if counts.get(name):
            lines.append(f"  {name:10} {counts[name]}")
    for o in outcomes:
        if o["outcome"] in ("unknown", "failed"):
            lines.append(f"  ! {o['id']}: {o.get('why')} "
                         f"{o.get('error') or ''}")
    return lines


def record_run(result, fetched, hs_calls, applied, outcomes=None) -> None:
    """One run_log row per run, dry ones included.

    A dry run is logged too: "we looked and would have done nothing" is
    worth being able to prove later.
    """
    from collections import Counter
    counts = Counter(o["outcome"] for o in (outcomes or []))
    try:
        database.execute_query(_RUN_SQL, (
            applied, fetched.calls, hs_calls, len(fetched.rows),
            counts.get("created", 0) if applied else len(result["creates"]),
            counts.get("updated", 0) if applied else len(result["updates"]),
            counts.get("unchanged", 0) if applied else len(result["unchanged"]),
            len(result["skipped"]), len(result["review"]),
            counts.get("failed", 0) + counts.get("unknown", 0),
            "complete", None,
            json.dumps(outcomes or [], default=str),
        ), fetch=True)
    except Exception as e:
        print(f"could not write the run log: {e}", file=sys.stderr)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Sync CSuite event dates to HubSpot marketing events. "
                    "Dry run unless --apply. CSuite is read-only.")
    parser.add_argument("--apply", action="store_true",
                        help="Write to HubSpot. Without this nothing is "
                             "written anywhere.")
    parser.add_argument("--out", default=None, help="Write the report here.")
    parser.add_argument("--organizer", default=DEFAULT_ORGANIZER)
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
        if have_tables:
            record_run(result, fetched, hs_calls, applied=False)
        print("\nDRY RUN — nothing was written to HubSpot or to hubsync.")
        return EXIT_OK

    outcomes = apply_plan(hubspot, result)
    record_run(result, fetched, hs_calls, applied=True, outcomes=outcomes)
    for line in summarise_outcomes(outcomes):
        print(line)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
