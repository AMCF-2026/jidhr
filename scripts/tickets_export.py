"""
Open Tickets Export
===================
Every open HubSpot ticket to a CSV, one row each, with the same age / idle
/ never-touched facts the chat report shows.

HubSpot READ only. Same fetch as the chat report (intents/tickets.py), so
the two never disagree about what "open" means.

    python scripts/tickets_export.py                  # -> tickets_open_<date>.csv
    python scripts/tickets_export.py --out /tmp/t.csv

The file carries the ticket subject — which is whatever the requester
typed — and nothing else about a person beyond an owner's name. Treat it
like the HubSpot ticket list itself.

Exit codes: 0 complete, 1 HubSpot returned a partial list (the file is
still written, and its header row says it is partial).
"""

import argparse
import csv
import os
import sys
from datetime import date

# Make the repo root importable when run as `python scripts/tickets_export.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clients.hubspot import HubSpotClient  # noqa: E402
from config import Config  # noqa: E402
from intents.tickets import describe  # noqa: E402

COLUMNS = (
    "id", "pipeline", "stage", "subject", "created", "age_days",
    "last_activity", "idle_days", "never_touched", "owner", "source",
    "daf_name", "url",
)


def ticket_url(ticket_id) -> str:
    """The HubSpot UI link. Portal id comes from Config, not a literal."""
    return Config.HUBSPOT_TICKET_URL.format(ticket_id=ticket_id)


def export_rows(tickets: list, owners: dict, now=None) -> list:
    """Ticket dicts -> CSV rows in COLUMNS order."""
    rows = []
    for ticket in tickets:
        facts = describe(ticket, owners, now=now)
        if facts is None:
            continue
        rows.append({
            "id": facts["id"],
            "pipeline": facts["pipeline_label"],
            "stage": facts["stage_label"],
            "subject": facts["subject"],
            "created": facts["created"] or "",
            "age_days": facts["age_days"] if facts["age_days"] is not None
            else "",
            "last_activity": facts["last_activity"] or "",
            "idle_days": facts["idle_days"] if facts["idle_days"] is not None
            else "",
            "never_touched": "yes" if not facts["touched"] else "no",
            "owner": facts["owner"] or "",
            "source": facts["source"] or "",
            "daf_name": facts["daf_name"] or "",
            "url": ticket_url(facts["id"]),
        })
    # Oldest first, same as the report.
    rows.sort(key=lambda r: (-(r["age_days"] if r["age_days"] != "" else -1),
                             r["id"]))
    return rows


def write_csv(path: str, rows: list, complete: bool) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        if not complete:
            # A comment line a spreadsheet will show as its first row —
            # the incompleteness travels with the file, not just the
            # terminal that produced it.
            handle.write("# PARTIAL: HubSpot returned an incomplete list; "
                         "counts derived from this file are incomplete\n")
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def header_line(rows: list, complete: bool) -> str:
    total = len(rows)
    pipelines = {r["pipeline"] for r in rows}
    never = sum(1 for r in rows if r["never_touched"] == "yes")
    pct = round(100 * never / total) if total else 0
    line = (f"{total} open tickets across {len(pipelines)} pipelines — "
            f"{never} never touched ({pct}%)")
    if not complete:
        line = "⚠️ PARTIAL — " + line
    return line


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Export every open HubSpot ticket to CSV. Read-only.")
    parser.add_argument(
        "--out", default=None,
        help="Output path. Default: tickets_open_<YYYY-MM-DD>.csv in the "
             "current directory.")
    args = parser.parse_args(argv)

    path = args.out or f"tickets_open_{date.today().isoformat()}.csv"

    client = HubSpotClient()
    tickets, complete = client.fetch_open_tickets()
    owners = client.get_owners() or {}

    rows = export_rows(tickets, owners)
    write_csv(path, rows, complete)

    print(header_line(rows, complete))
    print(f"Wrote {len(rows)} rows to {path}")
    return 0 if complete else 1


if __name__ == "__main__":
    sys.exit(main())
