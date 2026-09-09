"""
Events Intent Handler
=====================
Bridges CSuite events with HubSpot for email outreach.

Commands:
    - "upcoming events" / "list events"          → list future events from CSuite
    - "who's registered for [Name]"              → show attendee list from CSuite
    - "set up event [Name]" / "sync event [Name]"→ multi-step: sync attendees to HubSpot list
    - "post-event follow-up for [Name]"          → draft follow-up email for attendees

Data flow:
    CSuite event/display/eventdate → attendee profiles
    → HubSpot contacts (create/update each)
    → HubSpot static list (target for emails)
"""

import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Trigger phrases
# ---------------------------------------------------------------------------

_LIST_TRIGGERS = [
    "upcoming events", "list events", "what events", "show events",
    "event list", "events coming up",
]

_ATTENDEE_TRIGGERS = [
    "who's registered", "who is registered", "who registered",
    "who is attending", "who's attending", "who attended",
    "attendees for", "registrations for", "event attendees",
    "who signed up", "rsvp list for", "registered for",
    "attendance for", "attendance list",
]

_SYNC_TRIGGERS = [
    "set up event", "sync event", "create event list",
    "event workflow", "sync attendees",
]

_FOLLOWUP_TRIGGERS = [
    "post-event follow-up", "post event follow-up",
    "event follow-up", "send follow-up for",
    "post-event email",
]

_COMPARE_TRIGGERS = [
    "attended last year but not",
    "attended but not registered",
    "last year but haven't registered",
    "event comparison",
    "compare event",
    "who came to",
    "attended but didn't",
]

ALL_TRIGGERS = (_LIST_TRIGGERS + _ATTENDEE_TRIGGERS + _SYNC_TRIGGERS +
                _FOLLOWUP_TRIGGERS + _COMPARE_TRIGGERS)


# ---------------------------------------------------------------------------
# Public API: can_handle / handle
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Access control
# ---------------------------------------------------------------------------

# Nothing is donor-facing yet; every handler is staff-and-above.
ALLOWED_ROLES = frozenset({"admin", "staff"})

def can_handle(query: str, workflow_state: dict = None, **kwargs) -> bool:
    """Match if trigger phrase detected OR events workflow is active."""
    if workflow_state and workflow_state.get("active"):
        return workflow_state.get("workflow_type") == "events"

    # A bare "2" answering a numbered event list. Deliberately narrow: while a
    # pick is pending this claims digits only, so an unrelated question still
    # routes wherever it normally would.
    if workflow_state and workflow_state.get("pending_event_pick"):
        if _PICK_RE.match(query or ""):
            return True

    q = query.lower().strip()
    return any(p in q for p in ALL_TRIGGERS)


def handle(query: str, ctx) -> str:
    """Route to the appropriate sub-handler."""
    state = ctx.workflow_state
    hubspot = ctx.services.hubspot
    csuite = ctx.services.csuite
    q = query.lower().strip()

    # A pending numbered list takes priority: "2" means the second event.
    # take_pending_event_pick clears the list either way, so a non-numeric
    # message drops it and routing continues normally below.
    if state.get("pending_event_pick"):
        action, picked, extra = take_pending_event_pick(query, state)
        if picked is not None:
            return _dispatch_event_action(
                action, picked, query, q, state, hubspot, csuite, extra)

    # Active workflow — handle conversation
    if state.get("active") and state.get("workflow_type") == "events":
        return _handle_active_workflow(query, q, state, hubspot, csuite)

    # New command routing
    if any(p in q for p in _COMPARE_TRIGGERS):
        return _compare_events(query, q, csuite, state)

    if any(p in q for p in _SYNC_TRIGGERS):
        return _start_sync_workflow(query, q, state, csuite)

    if any(p in q for p in _ATTENDEE_TRIGGERS):
        return _show_attendees(query, q, csuite, state)

    if any(p in q for p in _FOLLOWUP_TRIGGERS):
        return _start_followup(query, q, csuite, hubspot, state)

    if any(p in q for p in _LIST_TRIGGERS):
        return _list_upcoming(csuite)

    return "I matched an events command but couldn't determine which one. Try 'upcoming events' or 'sync event [Name]'."


# ---------------------------------------------------------------------------
# Command: List upcoming events
# ---------------------------------------------------------------------------

def _list_upcoming(csuite) -> str:
    """List future events from CSuite."""
    try:
        result = csuite.get_event_dates(limit=200)
    except Exception as e:
        return f"Failed to fetch events: {e}"

    if not result.get("success") or not result.get("data"):
        return "Could not retrieve events from CSuite."

    events = result["data"].get("results", [])
    today = datetime.now().strftime("%Y-%m-%d")
    upcoming = [
        e for e in events
        if e.get("event_date") and e["event_date"] >= today and not e.get("archived")
    ]

    if not upcoming:
        return "No upcoming events found in CSuite."

    upcoming.sort(key=lambda e: e.get("event_date") or "9999")

    lines = [f"**Upcoming Events** ({len(upcoming)} found)\n"]
    for e in upcoming[:20]:
        date = e.get("event_date", "No date")
        desc = e.get("event_description", e.get("event_name", "Unnamed"))
        time = e.get("start_time", "")
        location = e.get("location", "")
        event_date_id = e.get("event_date_id", "?")

        detail = f"- **{desc}** — {date}"
        if time:
            detail += f" at {time}"
        if location:
            detail += f" ({location})"
        detail += f"  [ID: {event_date_id}]"
        lines.append(detail)

    if len(upcoming) > 20:
        lines.append(f"\n...and {len(upcoming) - 20} more")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Command: Show attendees
# ---------------------------------------------------------------------------

def _show_attendees(query: str, query_lower: str, csuite,
                    state: dict | None = None, event: dict | None = None) -> str:
    """Show attendees for a specific event."""
    try:
        event_data = event if event is not None else _find_event(
            query, query_lower, csuite, state, action="attendees")
        if isinstance(event_data, str):
            return event_data  # Error message

        event_date_id = event_data.get("event_date_id")
        if not event_date_id:
            return "Could not determine the event ID. Please try specifying the event name more precisely."

        event_detail = _fetch_event_detail(event_date_id, csuite)
        if event_detail is None:
            return _detail_unavailable(event_data)

        profiles = event_detail.get("profiles") or []
        desc = event_detail.get("event_description", event_detail.get("event_name", "Event"))
        date = event_detail.get("event_date", "")

        rsvp_count = sum(1 for p in profiles if p.get("rsvp"))
        guest_count = sum(len(p.get("guests", [])) for p in profiles)

        lines = [
            f"**{desc}** — {date}",
            f"Registered: {len(profiles)} | RSVP'd: {rsvp_count} | Guests: {guest_count}\n",
        ]

        for p in profiles[:50]:
            name = p.get("event_profile_name", "Unknown")
            email = p.get("event_profile_email", "no email")
            rsvp = "RSVP" if p.get("rsvp") else ""
            attended = "Attended" if p.get("attended") else ""
            status = " | ".join(filter(None, [rsvp, attended]))
            status_str = f" [{status}]" if status else ""
            lines.append(f"- {name} ({email}){status_str}")
            for g in p.get("guests", []):
                g_name = g.get("contact_name", "Guest")
                g_email = g.get("contact_email", "")
                lines.append(f"  - Guest: {g_name} ({g_email})")

        if len(profiles) > 50:
            lines.append(f"\n...and {len(profiles) - 50} more attendees")

        return "\n".join(lines)

    except Exception as e:
        logger.exception(f"Attendee lookup crashed: {e}")
        return f"Something went wrong fetching attendees. Error: {e}"


# ---------------------------------------------------------------------------
# Command: Sync event workflow (multi-step)
# ---------------------------------------------------------------------------

def _start_sync_workflow(query: str, query_lower: str, state: dict, csuite,
                         event: dict | None = None) -> str:
    """Step 1: Search for the event and ask for confirmation."""
    try:
        event_data = event if event is not None else _find_event(
            query, query_lower, csuite, state, action="sync")
    except Exception as e:
        logger.exception(f"Event search crashed: {e}")
        return f"Something went wrong searching for the event. Error: {e}"

    if isinstance(event_data, str):
        return event_data  # Error message

    event_date_id = event_data.get("event_date_id")
    if not event_date_id:
        return "Could not determine the event ID. Please try specifying the event name more precisely."

    # Fetch full details to show attendee count
    try:
        event_detail = _fetch_event_detail(event_date_id, csuite)
    except Exception as e:
        logger.exception(f"Event detail fetch crashed: {e}")
        return f"Something went wrong fetching event details. Error: {e}"

    if event_detail is None:
        return _detail_unavailable(event_data)

    profiles = event_detail.get("profiles", [])
    desc = event_detail.get("event_description", event_detail.get("event_name", "Event"))
    date = event_detail.get("event_date", "")

    # Activate workflow
    state.update({
        "active": True,
        "workflow_type": "events",
        "step": "confirm_sync",
        "event_date_id": event_date_id,
        "event_description": desc,
        "event_date": date,
        "attendee_count": len(profiles),
    })

    return (
        f"**Ready to sync event to HubSpot**\n\n"
        f"Event: **{desc}**\n"
        f"Date: {date}\n"
        f"Attendees: {len(profiles)}\n\n"
        f"This will:\n"
        f"1. Create/update {len(profiles)} HubSpot contacts\n"
        f"2. Create a static list: \"Event: {desc}\"\n"
        f"3. Add all attendees to that list for email targeting\n\n"
        f"Proceed? (yes/no)"
    )


def _handle_active_workflow(query: str, query_lower: str, state: dict,
                            hubspot, csuite) -> str:
    """Handle conversation within an active events workflow."""
    step = state.get("step")

    # Cancel
    if any(w in query_lower for w in ["cancel", "stop", "nevermind", "never mind"]):
        _reset_state(state)
        return "Event sync cancelled."

    if step == "confirm_sync":
        if any(w in query_lower for w in ["yes", "y", "proceed", "go", "do it"]):
            return _execute_sync(state, hubspot, csuite)
        elif any(w in query_lower for w in ["no", "n"]):
            _reset_state(state)
            return "Event sync cancelled."
        else:
            return "Proceed with syncing attendees to HubSpot? (yes/no)"

    # Shouldn't reach here
    _reset_state(state)
    return "Workflow state was unclear — reset. Try again with 'sync event [Name]'."


def _execute_sync(state: dict, hubspot, csuite) -> str:
    """Execute the sync: create contacts, create list, add members."""
    event_date_id = state.get("event_date_id")
    desc = state.get("event_description", "Event")
    date = state.get("event_date", "")

    # Fetch attendees
    event_detail = _fetch_event_detail(event_date_id, csuite)
    if event_detail is None:
        _reset_state(state)
        return (
            f"⚠️ Couldn't load details for {desc} — nothing was synced. "
            "Try again in a moment."
        )

    profiles = event_detail.get("profiles", [])
    if not profiles:
        _reset_state(state)
        return "No attendees found for this event."

    # --- 1. Create/update HubSpot contacts ---
    created = 0
    updated = 0
    failed = 0
    contact_ids = []

    for p in profiles:
        email = p.get("event_profile_email")
        if not email:
            continue

        name_parts = (p.get("event_profile_name") or "").split(", ", 1)
        last_name = name_parts[0] if name_parts else ""
        first_name = name_parts[1] if len(name_parts) > 1 else ""

        props = {
            "firstname": first_name.strip(),
            "lastname": last_name.strip(),
            "email": email,
        }

        try:
            # Try update first
            result = hubspot.update_contact_by_email(email, props)
            if result and "error" not in result:
                contact_id = result.get("id")
                if contact_id:
                    contact_ids.append(int(contact_id))
                updated += 1
            elif result and "Contact not found" in result.get("error", ""):
                # Create new contact
                create_result = hubspot.create_contact(props)
                if create_result and create_result.get("id"):
                    contact_ids.append(int(create_result["id"]))
                    created += 1
                else:
                    failed += 1
            else:
                failed += 1
        except Exception as e:
            logger.error(f"Error syncing contact {email}: {e}")
            failed += 1

    # --- 2. Create static HubSpot list ---
    list_name = f"Event: {desc} - {date}"
    list_id = None
    try:
        list_result = hubspot.create_contact_list(list_name)
        if list_result:
            list_obj = list_result if isinstance(list_result, dict) else {}
            # Handle nested response: {list: {listId: ...}}
            if "list" in list_obj:
                list_id = list_obj["list"].get("listId")
            else:
                list_id = list_obj.get("listId") or list_obj.get("id")
    except Exception as e:
        logger.error(f"Error creating list: {e}")

    # --- 3. Add contacts to list ---
    members_added = 0
    if list_id and contact_ids:
        try:
            add_result = hubspot.add_contacts_to_list(str(list_id), contact_ids)
            if add_result:
                members_added = len(contact_ids)
        except Exception as e:
            logger.error(f"Error adding members to list: {e}")

    _reset_state(state)

    # --- Build confirmation ---
    lines = [
        f"**Event sync complete: {desc}**\n",
        f"**Contacts:** {created} created, {updated} updated"
    ]
    if failed:
        lines.append(f", {failed} failed")
    if list_id:
        lines.append(f"\n**List:** \"{list_name}\" (ID: {list_id}) — {members_added} members added")
        lines.append(f"\nThis list is ready to target with a marketing email in HubSpot.")
    else:
        lines.append(f"\nFailed to create the contact list. Contacts were still synced.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Command: Post-event follow-up
# ---------------------------------------------------------------------------

def _start_followup(query: str, query_lower: str, csuite, hubspot,
                    state: dict | None = None, event: dict | None = None) -> str:
    """Draft a follow-up email for event attendees."""
    event_data = event if event is not None else _find_event(
        query, query_lower, csuite, state, action="followup")
    if isinstance(event_data, str):
        return event_data

    event_detail = _fetch_event_detail(event_data["event_date_id"], csuite)
    if event_detail is None:
        return _detail_unavailable(event_data)

    profiles = event_detail.get("profiles", [])
    desc = event_detail.get("event_description", event_detail.get("event_name", "Event"))
    date = event_detail.get("event_date", "")
    attended = [p for p in profiles if p.get("attended")]
    total = len(profiles)

    return (
        f"**Post-event follow-up for: {desc}** ({date})\n\n"
        f"Total registered: {total}\n"
        f"Marked as attended: {len(attended)}\n\n"
        f"To draft a follow-up email, say:\n"
        f"  *\"Draft a thank-you email for {desc}\"*\n\n"
        f"To sync attendees to a HubSpot list first, say:\n"
        f"  *\"Sync event {desc}\"*\n\n"
        f"If you have a recording link or photos to include, mention them in your draft request."
    )


# ---------------------------------------------------------------------------
# Command: Compare events (year-over-year)
# ---------------------------------------------------------------------------

# "X vs Y" / "X versus Y" — the only separators unambiguous enough to split
# on. " and " is not: "Fundraiser and Gala" is one event name.
_COMPARE_SPLIT_RE = re.compile(r"\s+(?:vs\.?|versus)\s+", re.IGNORECASE)


def _split_comparison(query: str):
    """Split "A vs B" into two sides, or return (query, None) if one-sided."""
    parts = _COMPARE_SPLIT_RE.split(query, maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return parts[0].strip(), parts[1].strip()
    return query, None


def _resolve_compare_side(text: str, csuite, state, side: str,
                          other: dict | None) -> dict | str:
    """Resolve one side of a comparison through the shared matcher.

    Returns the event, or the message _find_event produced. When that message
    is a numbered list, the side already resolved rides along in the pending
    record so picking a number resumes the comparison.
    """
    return _find_event(
        text, text.lower(), csuite, state, action="compare",
        extra={"side": side, "resolved_other": other},
    )


def _compare_events(query: str, query_lower: str, csuite,
                    state: dict | None = None, picked: dict | None = None,
                    picked_side: str | None = None,
                    other: dict | None = None) -> str:
    """Compare attendees between two events — who came before but hasn't
    registered this time.

    Both sides go through _find_event, so they get the same four matching
    tiers, the same trailing-date stripping and the same numbered-pick
    behaviour as every other event command.

    Two shapes are accepted:
      "compare X vs Y"  — each side resolved independently
      "who attended the symposium last year but not this year" — one name,
        resolved to every event sharing it, most recent two compared.

    Examples:
        "Who attended last year's symposium but hasn't registered this year?"
        "Compare event Annual Symposium 2025 vs 2026"
    """
    # --- Resuming after a numbered pick -----------------------------------
    if picked is not None:
        if other is None:
            return (
                "I lost track of the other event in that comparison. "
                "Please ask again with both events named."
            )
        if picked_side == "prior":
            current_event, prior_event = other, picked
        else:
            current_event, prior_event = picked, other
        return _render_comparison(current_event, prior_event, csuite)

    left_text, right_text = _split_comparison(query)

    # --- Two named sides --------------------------------------------------
    if right_text is not None:
        left = _resolve_compare_side(left_text, csuite, state, "current", None)
        if isinstance(left, str):
            return left  # error, or a numbered list for this side

        right = _resolve_compare_side(right_text, csuite, state, "prior", left)
        if isinstance(right, str):
            return right

        current_event, prior_event = _order_by_date(left, right)
        return _render_comparison(current_event, prior_event, csuite)

    # --- One name, compared across its own occurrences --------------------
    name_text, date_filter = _split_event_date(query)
    name = _extract_event_name(name_text, name_text.lower())

    try:
        result = csuite.get_event_dates(limit=200)
    except Exception as e:
        return f"Failed to fetch events: {e}"

    if not result.get("success") or not result.get("data"):
        return "Could not retrieve events from CSuite."

    events = result["data"].get("results", [])
    if not events:
        return "No events found."

    if not name:
        non_archived = [e for e in events if not e.get("archived")]
        non_archived.sort(key=lambda e: e.get("event_date") or "0000", reverse=True)
        lines = ["I need to know which event to compare. Here are recent events:\n"]
        for i, e in enumerate(non_archived[:10], 1):
            lines.append(f"{i}. **{_event_label(e)}** — {e.get('event_date', '')}")
        lines.append(
            "\nSay something like: *\"Who attended the Annual Symposium last year "
            "but hasn't registered this year?\"*"
        )
        return "\n".join(lines)

    # Same tiers as every other lookup — no bespoke matching here.
    matches = _match_events(events, name, date_filter)

    if len(matches) == 1:
        return (
            f"Only found one event matching '{name}': "
            f"**{_event_label(matches[0])}**\n\n"
            "I need at least two events (e.g., same event in different years) "
            "to compare."
        )
    if not matches:
        return f"No events found matching '{name}'."

    dated = [m for m in matches if m.get("event_date") is not None]
    if len(dated) < 2:
        return f"Not enough dated events matching '{name}' to compare."

    dated.sort(key=lambda e: e.get("event_date") or "0000", reverse=True)
    return _render_comparison(dated[0], dated[1], csuite)


def _order_by_date(a: dict, b: dict):
    """Return (current, prior) — the later event first."""
    if (b.get("event_date") or "0000") > (a.get("event_date") or "0000"):
        return b, a
    return a, b


def _render_comparison(current_event: dict, prior_event: dict, csuite) -> str:
    """Fetch both attendee lists and report who lapsed."""
    current_eid = current_event.get("event_date_id")
    prior_eid = prior_event.get("event_date_id")
    if not current_eid or not prior_eid:
        return "Could not determine event IDs for comparison."

    if current_eid == prior_eid:
        return (
            "Both sides of that comparison resolved to the same event. "
            "Name two different events, or add a date to each."
        )

    try:
        current_detail = _fetch_event_detail(current_eid, csuite)
        if current_detail is None:
            return _detail_unavailable(current_event)

        prior_detail = _fetch_event_detail(prior_eid, csuite)
        if prior_detail is None:
            return _detail_unavailable(prior_event)
    except Exception as e:
        logger.exception(f"Event comparison fetch crashed: {e}")
        return f"Something went wrong fetching event details for comparison. Error: {e}"

    current_emails = {
        p.get("event_profile_email", "").lower()
        for p in current_detail.get("profiles", [])
        if p.get("event_profile_email")
    }
    prior_profiles = prior_detail.get("profiles", [])
    prior_emails = {
        p.get("event_profile_email", "").lower()
        for p in prior_profiles
        if p.get("event_profile_email")
    }

    # Who was at the prior event but NOT the current one
    lapsed = []
    for p in prior_profiles:
        email = (p.get("event_profile_email") or "").lower()
        if email and email not in current_emails:
            name_str = p.get("event_profile_name", "Unknown")
            lapsed.append(f"- {name_str} ({email})")

    current_desc = current_detail.get("event_description", "Current")
    current_date = current_detail.get("event_date", "")
    prior_desc = prior_detail.get("event_description", "Prior")
    prior_date = prior_detail.get("event_date", "")

    lines = [
        f"**Event Comparison**\n",
        f"Prior: **{prior_desc}** ({prior_date}) — {len(prior_emails)} attendees",
        f"Current: **{current_desc}** ({current_date}) — {len(current_emails)} attendees\n",
        f"**Attended prior but NOT registered for current: {len(lapsed)}**\n",
    ]

    for entry in lapsed[:50]:
        lines.append(entry)
    if len(lapsed) > 50:
        lines.append(f"\n...and {len(lapsed) - 50} more")

    if not lapsed:
        lines.append("Everyone from the prior event is registered for the current one!")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _dispatch_event_action(action, event, query, query_lower, state,
                           hubspot, csuite, extra: dict | None = None) -> str:
    """Resume the command that produced a numbered list, now that one was picked."""
    extra = extra or {}

    if action == "compare":
        # `resolved_other` is the side that matched exactly first time round.
        return _compare_events(
            query, query_lower, csuite, state,
            picked=event, picked_side=extra.get("side"),
            other=extra.get("resolved_other"))

    if action == "sync":
        return _start_sync_workflow(query, query_lower, state, csuite, event=event)
    if action == "followup":
        return _start_followup(query, query_lower, csuite, hubspot,
                               state, event=event)
    # "attendees" is the default: it is the command that most often lands on
    # an ambiguous name.
    return _show_attendees(query, query_lower, csuite, state, event=event)


# Picks are single digits, so never offer more than nine choices.
_MAX_PICK_CHOICES = 9

# "Spring Gala — 2026-04-11" / "Spring Gala - 2026-04-11". The list this
# handler prints uses an em dash, so a user pasting a line back is the normal
# case, not an edge case.
_EVENT_DATE_SUFFIX_RE = re.compile(r"\s*[—–-]\s*(\d{4}-\d{2}-\d{2})\s*$")

_PICK_RE = re.compile(r"^\s*([1-9])\s*$")


def _split_event_date(text: str):
    """Split a trailing ' — YYYY-MM-DD' off a query.

    Returns (text_without_date, date_filter_or_None).
    """
    if not text:
        return text, None
    match = _EVENT_DATE_SUFFIX_RE.search(text)
    if not match:
        return text, None
    return text[:match.start()].strip(), match.group(1)


def _event_titles(event: dict) -> list:
    """The strings an event can be matched against."""
    return [
        str(event[key]) for key in ("event_description", "event_name")
        if event.get(key)
    ]


def _event_label(event: dict) -> str:
    return event.get("event_description") or event.get("event_name") or "Unnamed"


def _match_events(events: list, name: str, date_filter: str | None) -> list:
    """Match by decreasing precision, returning the first tier that hits.

    Tiers: exact title, prefix, whole-phrase substring, then the original
    word-level behaviour. Without the tiers, "Gala" and "Gala Dinner 2026"
    were equally good matches for the query "Gala", and the caller got an
    ambiguity prompt for a query that names one event exactly.
    """
    pool = events
    if date_filter:
        pool = [e for e in pool if (e.get("event_date") or "") == date_filter]

    if not name:
        return []

    needle = name.strip().lower()

    def matching(predicate):
        return [
            e for e in pool
            if any(predicate(title.lower()) for title in _event_titles(e))
        ]

    for predicate in (
        lambda title: title.strip() == needle,
        lambda title: title.startswith(needle),
        lambda title: needle in title,
    ):
        found = matching(predicate)
        if found:
            return found

    words = [w for w in needle.split() if len(w) >= 3]
    if words:
        return [
            e for e in pool
            if any(w in title.lower() for title in _event_titles(e) for w in words)
        ]
    return []


def _format_event_choices(matches: list, name: str, state: dict | None,
                          action: str | None, extra: dict | None = None) -> str:
    """Number the candidates and remember them, so "2" can answer.

    `extra` is merged into the stored record. The comparison path uses it to
    keep the side it already resolved, so picking the ambiguous side resumes
    the comparison instead of starting over.
    """
    lines = [f"Found {len(matches)} events matching '{name}':\n"]
    for i, event in enumerate(matches, 1):
        lines.append(
            f"{i}. **{_event_label(event)}** — {event.get('event_date', '')}")
    lines.append("\nReply with the number, or add the date to narrow it down.")

    if state is not None:
        record = {"action": action, "events": matches}
        record.update(extra or {})
        state["pending_event_pick"] = record
    return "\n".join(lines)


def take_pending_event_pick(query: str, state: dict):
    """Resolve a bare 1-9 against a stored candidate list.

    Returns (action, event, extra) on a successful pick, else
    (None, None, {}). `extra` carries whatever the caller stashed alongside
    the candidates — for a comparison, the side already resolved.

    The pending list is cleared either way: a pick consumes it, and any
    other message means the user moved on.
    """
    pending = (state or {}).get("pending_event_pick")
    if not pending:
        return None, None, {}

    match = _PICK_RE.match(query or "")
    if not match:
        state.pop("pending_event_pick", None)
        return None, None, {}

    index = int(match.group(1)) - 1
    events = pending.get("events") or []
    extra = {k: v for k, v in pending.items() if k not in ("action", "events")}
    state.pop("pending_event_pick", None)

    if 0 <= index < len(events):
        return pending.get("action"), events[index], extra
    return None, None, {}


def _find_event(query: str, query_lower: str, csuite,
                state: dict | None = None, action: str | None = None,
                extra: dict | None = None) -> dict | str:
    """Search CSuite events by name. Returns an event dict or a message string.

    When several events match, the candidates are numbered and remembered in
    `state["pending_event_pick"]` so the next message can just be "2".
    """
    query, date_filter = _split_event_date(query)
    query_lower, _ = _split_event_date(query_lower)

    name = _extract_event_name(query, query_lower)

    try:
        result = csuite.get_event_dates(limit=200)
    except Exception as e:
        return f"Failed to fetch events: {e}"

    if not result.get("success") or not result.get("data"):
        return "Could not retrieve events from CSuite."

    events = result["data"].get("results", [])
    if not events:
        return "No events found in CSuite."

    if not name:
        # No name extracted — show recent events for user to pick
        non_archived = [
            e for e in events
            if not e.get("archived") and e.get("event_date") is not None
        ]
        non_archived.sort(key=lambda e: e.get("event_date") or "0000", reverse=True)
        recent = non_archived[:5]
        lines = ["I couldn't determine which event. Here are the most recent:\n"]
        for i, e in enumerate(recent, 1):
            lines.append(f"{i}. **{_event_label(e)}** — {e.get('event_date', '')}")
        lines.append("\nPlease specify the event name.")
        return "\n".join(lines)

    matches = _match_events(events, name, date_filter)

    if not matches:
        suffix = f" on {date_filter}" if date_filter else ""
        return (
            f"No events found matching '{name}'{suffix}. "
            "Try 'list events' to see what's available."
        )

    if len(matches) == 1:
        return matches[0]

    if len(matches) > _MAX_PICK_CHOICES:
        # Printing 55 lines helps nobody. Say how many and ask for more words.
        suffix = f" on {date_filter}" if date_filter else ""
        return (
            f"Found {len(matches)} events matching '{name}'{suffix} — too many "
            "to list. Add more of the event's name, or include the date as "
            "'— YYYY-MM-DD'."
        )

    return _format_event_choices(matches, name, state, action, extra)


def _extract_event_name(query: str, query_lower: str) -> str:
    """Extract the event name from a query string."""
    # Remove trigger phrases to isolate the event name
    all_triggers = _LIST_TRIGGERS + _ATTENDEE_TRIGGERS + _SYNC_TRIGGERS + _FOLLOWUP_TRIGGERS + _COMPARE_TRIGGERS
    remaining = query_lower
    for phrase in sorted(all_triggers, key=len, reverse=True):
        remaining = remaining.replace(phrase, "")

    # Clean up common filler words and punctuation
    for word in ["the", "for", "about", "our", "my", "a", "an", "event", "events"]:
        remaining = re.sub(rf"\b{word}\b", "", remaining)
    remaining = re.sub(r"[?!.,;:]", "", remaining)

    name = remaining.strip().strip('"\'')
    return name if len(name) > 2 else ""


def _fetch_event_detail(event_date_id: int, csuite) -> dict | None:
    """Fetch full event details including attendees, or None on failure.

    Returns None rather than an error string. A string return meant every
    caller had to remember an isinstance check, and a caller that forgot got
    a str where it expected a dict — .get() on it raises, or worse, the error
    text flowed on as if it were data.
    """
    try:
        result = csuite.get_event_date(event_date_id)
    except Exception as e:
        logger.warning(
            f"Event detail fetch failed for {event_date_id}: {e}", exc_info=True)
        return None

    if not result.get("success") or not result.get("data"):
        logger.warning(
            "Event detail unavailable for %s: %s",
            event_date_id, result.get("error", "no data returned"))
        return None

    return result["data"]


def _detail_unavailable(event) -> str:
    """The line a user sees when an event's details could not be loaded."""
    label = _event_label(event) if isinstance(event, dict) else str(event)
    return f"⚠️ Couldn't load details for {label}."


def _reset_state(state: dict):
    """Reset workflow state."""
    from intents.daf_workflow import default_workflow_state
    state.update(default_workflow_state())


def _format_event_summary(event_data: dict) -> str:
    """Format a single event's details."""
    desc = event_data.get("event_description", event_data.get("event_name", "Unnamed"))
    date = event_data.get("event_date", "No date")
    time = event_data.get("start_time", "")
    location = event_data.get("location", "")
    profiles = event_data.get("profiles", [])
    tickets = event_data.get("tickets", [])

    lines = [f"**{desc}**", f"Date: {date}"]
    if time:
        lines.append(f"Time: {time}")
    if location:
        lines.append(f"Location: {location}")
    lines.append(f"Attendees: {len(profiles)}")

    for t in tickets:
        t_name = t.get("ticket_name", "Ticket")
        t_sold = t.get("sold_tickets", 0)
        t_price = t.get("ticket_price", "0.00")
        lines.append(f"Ticket: {t_name} — ${t_price} ({t_sold} sold)")

    return "\n".join(lines)
