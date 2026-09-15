"""
Jidhr Open Tickets Report
=========================
Every open HubSpot ticket, grouped by pipeline, oldest first, with how
long each has sat and whether anyone has ever touched it.

HubSpot READ only. One search (paged) plus one owners lookup, cached per
process. Nothing here writes, and nothing here asks Claude: the report is
built deterministically from the ticket rows, so the same tickets always
produce the same text and no number in it is a model's guess.

What the numbers mean (probe #4, H12-H14)
----------------------------------------
"Open" is decided by HubSpot's own stage metadata, not by stage name:
Config.TICKET_OPEN_STAGES holds every stage id whose ticketState is OPEN.
Every pipeline has exactly one CLOSED stage.

age_days   days since createdate.
touched    hs_lastactivitydate is set — HubSpot logged an email, call,
           note or meeting against the ticket at some point. On the probe
           sample 76% of tickets had NO activity date at all.
idle_days  days since hs_lastactivitydate, only for touched tickets. For a
           ticket nobody has touched there is no reply to measure from,
           so idle is None and the line says "never touched" instead of
           inventing a zero.

ACH Setup is special: its only CLOSED stage is "Entered Into CSuite", so a
ticket leaving the open count there means the data entry got done, not
that the requester was answered. That pipeline carries a note saying so.
"""

import logging
import re
from datetime import datetime, timezone

from clients import mirror_read
from config import Config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Triggers
# ---------------------------------------------------------------------------

TRIGGER_PHRASES = [
    "open tickets", "stale tickets", "ticket report", "tickets older than",
    "tickets for ",
]

# "tickets older than 30 days" / "tickets older than 2 weeks"
_OLDER_THAN_RE = re.compile(
    r"tickets\s+older\s+than\s+(\d+)\s*(day|days|week|weeks|month|months)?",
    re.IGNORECASE)

# "tickets for Nora" — an owner filter, matched against the owner label.
_FOR_OWNER_RE = re.compile(r"tickets\s+for\s+(.+?)\s*[?.!]*$", re.IGNORECASE)

_DAYS_PER = {"day": 1, "days": 1, "week": 7, "weeks": 7,
             "month": 30, "months": 30}

OLDEST_SHOWN = 10


# ---------------------------------------------------------------------------
# Registry interface
# ---------------------------------------------------------------------------

# Staff-and-above: a ticket list names the people who wrote in.
ALLOWED_ROLES = frozenset({"admin", "staff"})


def can_handle(query: str, **kwargs) -> bool:
    q = query.lower().strip()
    return any(phrase in q for phrase in TRIGGER_PHRASES)


def handle(query: str, ctx) -> str:
    hubspot = ctx.services.hubspot
    q = query.strip()

    min_age = parse_min_age_days(q)
    owner_filter = parse_owner_filter(q)

    tickets, complete = hubspot.fetch_open_tickets()
    owners = hubspot.get_owners() or {}

    rows = [describe(t, owners) for t in tickets]
    rows = [r for r in rows if r is not None]

    if min_age is not None:
        rows = [r for r in rows if r["age_days"] is not None
                and r["age_days"] >= min_age]
    if owner_filter:
        rows = [r for r in rows if owner_matches(r, owner_filter)]

    return render(rows, complete=complete, min_age=min_age,
                  owner_filter=owner_filter)


# ---------------------------------------------------------------------------
# Query parsing
# ---------------------------------------------------------------------------

def parse_min_age_days(query: str):
    """The N in "tickets older than N days", in days, or None."""
    match = _OLDER_THAN_RE.search(query or "")
    if not match:
        return None
    count = int(match.group(1))
    unit = (match.group(2) or "days").lower()
    return count * _DAYS_PER.get(unit, 1)


def parse_owner_filter(query: str):
    """The name in "tickets for <name>", or None.

    "tickets for the DAF pipeline" would match too; the filter is applied
    to owner labels and, if nothing matches, the report says so rather
    than silently showing everything.
    """
    if _OLDER_THAN_RE.search(query or ""):
        return None
    match = _FOR_OWNER_RE.search(query or "")
    if not match:
        return None
    name = match.group(1).strip()
    return name or None


def owner_matches(row: dict, needle: str) -> bool:
    label = (row.get("owner") or "").lower()
    return needle.lower() in label


# ---------------------------------------------------------------------------
# Per-ticket facts
# ---------------------------------------------------------------------------

def _now():
    return datetime.now(timezone.utc)


def _days_since(stamp, now=None) -> int | None:
    """Whole days between a HubSpot timestamp and now, or None."""
    then = mirror_read._to_datetime(stamp)
    if then is None:
        return None
    now = now or _now()
    return max(0, int((now - then).total_seconds() // 86400))


def describe(ticket: dict, owners: dict, now=None) -> dict | None:
    """One ticket as the facts the report prints."""
    if not isinstance(ticket, dict):
        return None
    ticket_id = ticket.get("id")
    if ticket_id in (None, ""):
        return None
    props = ticket.get("properties") or {}

    pipeline = str(props.get("hs_pipeline") or "")
    stage = str(props.get("hs_pipeline_stage") or "")
    created = props.get("createdate")
    last_activity = props.get("hs_lastactivitydate")
    touched = last_activity not in (None, "")

    owner_id = props.get("hubspot_owner_id")
    owner = None
    if owner_id not in (None, ""):
        owner = owners.get(str(owner_id)) or f"owner {owner_id}"

    return {
        "id": str(ticket_id),
        "pipeline": pipeline,
        "pipeline_label": Config.TICKET_PIPELINE_LABELS.get(
            pipeline, f"pipeline {pipeline}" if pipeline else "no pipeline"),
        "stage": stage,
        "stage_label": Config.TICKET_STAGE_LABELS.get(
            stage, f"stage {stage}" if stage else "no stage"),
        "subject": (props.get("subject") or "").strip() or "(no subject)",
        "created": created,
        "age_days": _days_since(created, now),
        "last_activity": last_activity if touched else None,
        "touched": touched,
        "idle_days": _days_since(last_activity, now) if touched else None,
        "owner": owner,
        "owner_id": str(owner_id) if owner_id not in (None, "") else None,
        "source": props.get("source_type"),
        "daf_name": props.get("daf_name"),
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _pct(part: int, whole: int) -> int:
    return round(100 * part / whole) if whole else 0


def _age(days) -> str:
    return f"{days}d" if days is not None else "?d"


def ticket_line(row: dict) -> str:
    """"#id · subject · 41d old · 3d idle · Nora Moorefield"."""
    idle = (f"{row['idle_days']}d idle" if row["touched"]
            else "never touched")
    owner = row["owner"] or "unassigned"
    return (f"#{row['id']} · {row['subject']} · {_age(row['age_days'])} old · "
            f"{idle} · {owner}")


def render(rows: list, complete: bool = True, min_age=None,
           owner_filter=None) -> str:
    lines = []

    if not complete:
        lines += ["⚠️ HubSpot returned a partial list — counts below are "
                  "incomplete.", ""]

    scope = []
    if min_age is not None:
        scope.append(f"older than {min_age} days")
    if owner_filter:
        scope.append(f"owned by '{owner_filter}'")
    scope_text = f" ({', '.join(scope)})" if scope else ""

    total = len(rows)
    if not total:
        lines.append(f"✅ No open tickets{scope_text}.")
        if owner_filter:
            lines.append(f"(No owner label contains '{owner_filter}' — try a "
                         "first name as it appears in HubSpot.)")
        return "\n".join(lines)

    by_pipeline = {}
    for row in rows:
        by_pipeline.setdefault(row["pipeline"], []).append(row)

    never = sum(1 for r in rows if not r["touched"])
    lines.append(
        f"🎫 **{total} open tickets across {len(by_pipeline)} "
        f"pipeline{'s' if len(by_pipeline) != 1 else ''}{scope_text} — "
        f"{never} never touched ({_pct(never, total)}%)**")
    lines.append("")

    # Most open first; ties by label so the order is stable.
    ordered = sorted(by_pipeline.items(),
                     key=lambda kv: (-len(kv[1]), kv[1][0]["pipeline_label"]))

    for pipeline_id, group in ordered:
        label = group[0]["pipeline_label"]
        group_never = sum(1 for r in group if not r["touched"])
        ages = [r["age_days"] for r in group if r["age_days"] is not None]
        oldest = max(ages) if ages else None

        lines.append(
            f"**{label}** — {len(group)} open, {group_never} never touched"
            f"{f', oldest {oldest}d' if oldest is not None else ''}")

        note = Config.TICKET_PIPELINE_NOTES.get(pipeline_id)
        if note:
            lines.append(f"ℹ️ {note}")

        stages = {}
        for row in group:
            stages.setdefault(row["stage_label"], 0)
            stages[row["stage_label"]] += 1
        lines.append("Stages: " + ", ".join(
            f"{name} {count}" for name, count in
            sorted(stages.items(), key=lambda kv: (-kv[1], kv[0]))))

        oldest_first = sorted(
            group, key=lambda r: (-(r["age_days"] if r["age_days"] is not None
                                    else -1), r["id"]))
        for row in oldest_first[:OLDEST_SHOWN]:
            lines.append(f"• {ticket_line(row)}")
        if len(oldest_first) > OLDEST_SHOWN:
            lines.append(f"• ... and {len(oldest_first) - OLDEST_SHOWN} more")
        lines.append("")

    lines.append("_Age is days since the ticket was created; idle is days "
                 "since HubSpot last logged activity on it. \"Never touched\" "
                 "means no activity has ever been logged._")
    return "\n".join(lines)
