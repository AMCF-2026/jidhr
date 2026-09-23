"""
Audit Spool Replay
==================
Inserts spooled write-audit rows into `write_audit`, once each.

    python scripts/audit_replay.py            # dry run: says what it would do
    python scripts/audit_replay.py --apply    # inserts

Why a spool exists at all
-------------------------
Until 2026-09-23 auditing ran after the fact and swallowed its own
failures, so a write whose audit row could not be stored went out
anyway, unrecorded. `DATABASE_URL` names Railway's private host, which
resolves only inside Railway, so every write from a developer machine
was in that state — including the two HubSpot email probes of
2026-09-23, which left no row at all.

Auditing is now pre-flight (clients/audit.reserve_write): no row, no
request. So nothing new is ever spooled. This file replays what was
stranded before that change, and exists so the replay is reproducible
rather than something someone once did by hand.

Idempotent: a line that already carries `replayed_at` is skipped, and
every replayed line is written back with the `write_audit` id it landed
on. Running it twice does not double-count.

Exit codes: 0 · 1 the spool could not be read · 2 no database.
"""

import argparse
import io
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clients import database  # noqa: E402

EXIT_OK = 0
EXIT_BAD_SPOOL = 1
EXIT_NO_DATABASE = 2

DEFAULT_SPOOL = os.path.join("var", "audit_spool", "api_writes_pending.jsonl")

# Same columns as clients/audit._INSERT_SQL, in the same order.
_INSERT_SQL = """
    INSERT INTO write_audit (
        actor_user_id, actor_label, intent, target_system, http_method,
        endpoint, target_id, payload_hash, payload_meta, status,
        http_status, error, duration_ms, sync_run_id
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s)
    RETURNING id
"""


def read_spool(path: str) -> list:
    if not os.path.exists(path):
        return []
    rows = []
    with io.open(path, encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except ValueError as e:
                raise ValueError(f"{path} line {number}: {e}") from e
    return rows


def write_spool(path: str, rows: list) -> None:
    with io.open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=str) + "\n")


def replay_row(row: dict):
    """Insert one spooled row. Returns its new write_audit id.

    The reason it was spooled goes into `error`, so nobody later reads
    a replayed row as one that landed normally at the time.
    """
    note = f"replayed from spool: {row.get('reason', '')}"[:500]
    found = database.execute_query(_INSERT_SQL, (
        row.get("actor_user_id"),
        row.get("actor_label"),
        row.get("intent"),
        row["target_system"],
        row["http_method"],
        row["endpoint"],
        row.get("target_id"),
        row.get("payload_hash"),
        json.dumps(row.get("payload_meta") or {}, default=str),
        row["status"],
        row.get("http_status"),
        note,
        row.get("duration_ms"),
        row.get("sync_run_id"),
    ), fetch=True)
    return found[0]["id"] if isinstance(found[0], dict) else found[0][0]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Replay spooled write-audit rows into write_audit.")
    parser.add_argument("--spool", default=DEFAULT_SPOOL,
                        help=f"Spool file (default {DEFAULT_SPOOL}).")
    parser.add_argument("--apply", action="store_true",
                        help="Actually insert. Without it, nothing is "
                             "written and the plan is printed.")
    args = parser.parse_args(argv)

    try:
        rows = read_spool(args.spool)
    except ValueError as e:
        print(e, file=sys.stderr)
        return EXIT_BAD_SPOOL

    if not rows:
        print(f"{args.spool}: nothing spooled.")
        return EXIT_OK

    pending = [row for row in rows if not row.get("replayed_at")]
    print(f"{args.spool}: {len(rows)} row(s), {len(pending)} not yet replayed.")
    for row in pending:
        print(f"  {row['http_method']:6} {row['endpoint']} "
              f"-> {row.get('http_status')}")

    if not pending:
        return EXIT_OK

    if not args.apply:
        print("\nDRY RUN — nothing written. Re-run with --apply.")
        return EXIT_OK

    if not database.is_configured():
        print("No database configured. Set DATABASE_PUBLIC_URL to replay "
              "from outside Railway.", file=sys.stderr)
        return EXIT_NO_DATABASE

    replayed = 0
    for row in pending:
        row_id = replay_row(row)
        row["replayed_at"] = datetime.now(timezone.utc).isoformat()
        row["write_audit_id"] = row_id
        replayed += 1
        print(f"  replayed -> write_audit id {row_id}: "
              f"{row['http_method']} {row['endpoint']}")

    write_spool(args.spool, rows)
    print(f"\n{replayed} row(s) replayed; spool marked.")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
