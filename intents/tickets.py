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

# The personal view: the signed-in person's own tickets, resolved from
# their login email — never from anything they type.
PERSONAL_PHRASES = [
    "my tickets", "my open tickets", "how many tickets do i have",
    "tickets assigned to me", "what am i waiting on",
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


def is_personal(query: str) -> bool:
    q = (query or "").lower().strip()
    return any(phrase in q for phrase in PERSONAL_PHRASES)


def can_handle(query: str, **kwargs) -> bool:
    q = query.lower().strip()
    return is_personal(q) or any(phrase in q for phrase in TRIGGER_PHRASES)


def handle(query: str, ctx) -> str:
    hubspot = ctx.services.hubspot
    q = query.strip()

    personal = is_personal(q)
    min_age = parse_min_age_days(q)
    owner_filter = None if personal else parse_owner_filter(q)

    owners = hubspot.get_owners() or {}

    own_id = None
    if personal:
        actor_email = getattr(getattr(ctx, "actor", None), "email", None)
        own_id = owner_id_for_email(actor_email, owners)
        if own_id is None:
            # The person asking is not a HubSpot owner. Say so and stop:
            # guessing which owner they "probably" are, or asking them,
            # would turn a login identity into a typed one.
            logger.info("personal ticket view: %r matches no HubSpot owner",
                        actor_email)
            return (f"ℹ️ {actor_email or 'Your login'} isn't a HubSpot ticket "
                    "owner — nothing assigned to you.")

    tickets, complete = hubspot.fetch_open_tickets()

    rows = [describe(t, owners) for t in tickets]
    rows = [r for r in rows if r is not None]

    if own_id is not None:
        rows = [r for r in rows if r["owner_id"] == own_id]
    if min_age is not None:
        rows = [r for r in rows if r["age_days"] is not None
                and r["age_days"] >= min_age]
    if owner_filter:
        rows = [r for r in rows if owner_matches(r, owner_filter)]

    return render(rows, complete=complete, min_age=min_age,
                  owner_filter=owner_filter, personal=personal)


# ---------------------------------------------------------------------------
# Who is asking
# ---------------------------------------------------------------------------

# get_owners() labels are "First Last (email)", or the bare email when
# there is no name. The address is the only part that can be matched to a
# login, and it is matched whole, case-insensitively.
_LABEL_EMAIL_RE = re.compile(r"\(([^()\s]+@[^()\s]+)\)\s*$")


def owner_email_from_label(label) -> str | None:
    text = (label or "").strip()
    match = _LABEL_EMAIL_RE.search(text)
    if match:
        return match.group(1).lower()
    if "@" in text and " " not in text:
        return text.lower()
    return None


def owner_id_for_email(email, owners: dict) -> str | None:
    """The HubSpot owner id whose address equals `email`, or None."""
    wanted = (email or "").strip().lower()
    if not wanted or "@" not in wanted:
        return None
    for owner_id, label in (owners or {}).items():
        if owner_email_from_label(label) == wanted:
            return str(owner_id)
    return None


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

    pipeline_label = Config.TICKET_PIPELINE_LABELS.get(
        pipeline, f"pipeline {pipeline}" if pipeline else "no pipeline")

    age_days = _days_since(created, now)
    idle_days = _days_since(last_activity, now) if touched else None

    # hs_lastactivitydate EARLIER than createdate: an engagement logged
    # against the contact before the ticket existed and associated later.
    # "Idle 400 days" on a 40-day-old ticket is not a fact about the
    # ticket, so idle is capped at the ticket's age and the line says why.
    predates = (touched and age_days is not None and idle_days is not None
                and idle_days > age_days)
    if predates:
        idle_days = age_days

    subject, nameless = clean_subject(props.get("subject"), pipeline_label)

    return {
        "id": str(ticket_id),
        "pipeline": pipeline,
        "pipeline_label": pipeline_label,
        "stage": stage,
        "stage_label": Config.TICKET_STAGE_LABELS.get(
            stage, f"stage {stage}" if stage else "no stage"),
        "subject": subject,
        "nameless": nameless,
        "created": created,
        "age_days": age_days,
        "last_activity": last_activity if touched else None,
        "touched": touched,
        "idle_days": idle_days,
        "activity_predates_ticket": predates,
        "owner": owner,
        "owner_id": str(owner_id) if owner_id not in (None, "") else None,
        "source": props.get("source_type"),
        "daf_name": props.get("daf_name"),
    }


# Subjects HubSpot writes when a form creates a ticket and nobody has
# renamed it. Neither says who wrote in, so a list of them is a list of
# "DAF Form Submission - " twenty times over.
_FORM_SUBJECT_PREFIX_RE = re.compile(
    r"^(?:daf|endowment|ach|investment)?\s*form\s+submission\s*-?\s*$",
    re.IGNORECASE)
_GENERIC_SUBJECTS = {
    "new ticket created from form submission",
}


def clean_subject(raw, pipeline_label: str) -> tuple:
    """(subject to show, is_nameless).

    Blank, "DAF Form Submission - " with nothing after the dash, and the
    generic "New ticket created from form submission" all become
    "<pipeline> (no name on ticket)" so the reader knows the ticket has
    to be opened to learn who it is about.
    """
    text = (raw or "").strip()
    if (not text
            or _FORM_SUBJECT_PREFIX_RE.match(text)
            or text.lower() in _GENERIC_SUBJECTS):
        return f"{pipeline_label} (no name on ticket)", True
    return text, False


# ---------------------------------------------------------------------------
# Links
# ---------------------------------------------------------------------------

def real_id(value) -> bool:
    """True for an id worth putting in a URL.

    None, "", "None" and whitespace are what a missing id looks like after
    it has been through a dict.get and an f-string; a link built from one
    reads exactly like a real link and 404s.
    """
    if value is None or isinstance(value, bool):
        return False
    text = str(value).strip()
    return bool(text) and text.lower() != "none"


def ticket_url(ticket_id) -> str | None:
    """The HubSpot record link for a ticket, or None without a real id."""
    if not real_id(ticket_id):
        return None
    return Config.HUBSPOT_TICKET_URL.format(ticket_id=str(ticket_id).strip())


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _pct(part: int, whole: int) -> int:
    return round(100 * part / whole) if whole else 0


def _age(days) -> str:
    return f"{days}d" if days is not None else "?d"


def ticket_line(row: dict) -> str:
    """"#id · subject · 41d old · 3d idle · Nora Moorefield"."""
    if not row["touched"]:
        idle = "never touched"
    elif row.get("activity_predates_ticket"):
        idle = f"≥ {row['idle_days']}d idle (activity predates ticket)"
    else:
        idle = f"{row['idle_days']}d idle"
    owner = row["owner"] or "unassigned"
    line = (f"#{row['id']} · {row['subject']} · {_age(row['age_days'])} old · "
            f"{idle} · {owner}")
    link = ticket_url(row["id"])
    if link:
        line += f" · [open]({link})"
    return line


def render(rows: list, complete: bool = True, min_age=None,
           owner_filter=None, personal: bool = False) -> str:
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
        if personal:
            lines.append(f"✅ You have no open tickets{scope_text}.")
        else:
            lines.append(f"✅ No open tickets{scope_text}.")
        if owner_filter:
            lines.append(f"(No owner label contains '{owner_filter}' — try a "
                         "first name as it appears in HubSpot.)")
        return "\n".join(lines)

    by_pipeline = {}
    for row in rows:
        by_pipeline.setdefault(row["pipeline"], []).append(row)

    never = sum(1 for r in rows if not r["touched"])
    if personal:
        lines.append(
            f"🎫 **You have {total} open ticket{'s' if total != 1 else ''}"
            f"{scope_text} — {never} never touched**")
    else:
        lines.append(
            f"🎫 **{total} open tickets across {len(by_pipeline)} "
            f"pipeline{'s' if len(by_pipeline) != 1 else ''}{scope_text} — "
            f"{never} never touched ({_pct(never, total)}%)**")
    nameless = sum(1 for r in rows if r.get("nameless"))
    if nameless:
        lines.append(f"{nameless} ticket{'s' if nameless != 1 else ''} "
                     f"{'have' if nameless != 1 else 'has'} no name in the "
                     "subject")
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
