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
    "attendees for", "registrations for", "event attendees",
    "who signed up",
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

def can_handle(query: str, workflow_state: dict = None, **kwargs) -> bool:
    """Match if trigger phrase detected OR events workflow is active."""
    if workflow_state and workflow_state.get("active"):
        return workflow_state.get("workflow_type") == "events"
    q = query.lower().strip()
    return any(p in q for p in ALL_TRIGGERS)


def handle(query: str, assistant) -> str:
    """Route to the appropriate sub-handler."""
    state = assistant.workflow_state
    hubspot = assistant.hubspot
    csuite = assistant.csuite
    q = query.lower().strip()

    # Active workflow — handle conversation
    if state.get("active") and state.get("workflow_type") == "events":
        return _handle_active_workflow(query, q, state, hubspot, csuite)

    # New command routing
    if any(p in q for p in _COMPARE_TRIGGERS):
        return _compare_events(query, q, csuite)

    if any(p in q for p in _SYNC_TRIGGERS):
        return _start_sync_workflow(query, q, state, csuite)

    if any(p in q for p in _ATTENDEE_TRIGGERS):
        return _show_attendees(query, q, csuite)

    if any(p in q for p in _FOLLOWUP_TRIGGERS):
        return _start_followup(query, q, csuite, hubspot)

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
    upcoming = [e for e in events if e.get("event_date", "") >= today and not e.get("archived")]

    if not upcoming:
        return "No upcoming events found in CSuite."

    upcoming.sort(key=lambda e: e.get("event_date", ""))

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

def _show_attendees(query: str, query_lower: str, csuite) -> str:
    """Show attendees for a specific event."""
    event_data = _find_event(query, query_lower, csuite)
    if isinstance(event_data, str):
        return event_data  # Error message

    event_detail = _fetch_event_detail(event_data["event_date_id"], csuite)
    if isinstance(event_detail, str):
        return event_detail  # Error message

    profiles = event_detail.get("profiles", [])
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


# ---------------------------------------------------------------------------
# Command: Sync event workflow (multi-step)
# ---------------------------------------------------------------------------

def _start_sync_workflow(query: str, query_lower: str, state: dict, csuite) -> str:
    """Step 1: Search for the event and ask for confirmation."""
    event_data = _find_event(query, query_lower, csuite)
    if isinstance(event_data, str):
        return event_data  # Error message

    # Fetch full details to show attendee count
    event_detail = _fetch_event_detail(event_data["event_date_id"], csuite)
    if isinstance(event_detail, str):
        return event_detail

    profiles = event_detail.get("profiles", [])
    desc = event_detail.get("event_description", event_detail.get("event_name", "Event"))
    date = event_detail.get("event_date", "")

    # Activate workflow
    state.update({
        "active": True,
        "workflow_type": "events",
        "step": "confirm_sync",
        "event_date_id": event_data["event_date_id"],
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
    if isinstance(event_detail, str):
        _reset_state(state)
        return event_detail

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

def _start_followup(query: str, query_lower: str, csuite, hubspot) -> str:
    """Draft a follow-up email for event attendees."""
    event_data = _find_event(query, query_lower, csuite)
    if isinstance(event_data, str):
        return event_data

    event_detail = _fetch_event_detail(event_data["event_date_id"], csuite)
    if isinstance(event_detail, str):
        return event_detail

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

def _compare_events(query: str, query_lower: str, csuite) -> str:
    """Compare attendees between two events — who came before but hasn't registered this time.

    Examples:
        "Who attended last year's symposium but hasn't registered this year?"
        "Compare event Annual Symposium 2025 vs 2026"
    """
    # Fetch all events to find matches
    try:
        result = csuite.get_event_dates(limit=200)
    except Exception as e:
        return f"Failed to fetch events: {e}"

    if not result.get("success") or not result.get("data"):
        return "Could not retrieve events from CSuite."

    events = result["data"].get("results", [])
    if not events:
        return "No events found."

    # Try to extract the event name from the query
    name = _extract_event_name(query, query_lower)

    if not name:
        # Fall back to showing recent events for the user to pick
        non_archived = [e for e in events if not e.get("archived")]
        non_archived.sort(key=lambda e: e.get("event_date", ""), reverse=True)
        lines = [
            "I need to know which event to compare. Here are recent events:\n"
        ]
        for i, e in enumerate(non_archived[:10], 1):
            desc = e.get("event_description", e.get("event_name", "Unnamed"))
            date = e.get("event_date", "")
            lines.append(f"{i}. **{desc}** — {date}")
        lines.append(
            "\nSay something like: *\"Who attended the Annual Symposium last year "
            "but hasn't registered this year?\"*"
        )
        return "\n".join(lines)

    # Find all events matching this name (should get multiple years)
    matches = [
        e for e in events
        if name.lower() in (e.get("event_description") or "").lower()
        or name.lower() in (e.get("event_name") or "").lower()
    ]

    if len(matches) < 2:
        if len(matches) == 1:
            desc = matches[0].get("event_description", "")
            return (
                f"Only found one event matching '{name}': **{desc}**\n\n"
                f"I need at least two events (e.g., same event in different years) to compare."
            )
        return f"No events found matching '{name}'."

    # Sort by date — most recent first
    matches.sort(key=lambda e: e.get("event_date", ""), reverse=True)
    current_event = matches[0]
    prior_event = matches[1]

    # Fetch attendees for both
    current_detail = _fetch_event_detail(current_event["event_date_id"], csuite)
    if isinstance(current_detail, str):
        return current_detail

    prior_detail = _fetch_event_detail(prior_event["event_date_id"], csuite)
    if isinstance(prior_detail, str):
        return prior_detail

    # Build email sets
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

def _find_event(query: str, query_lower: str, csuite) -> dict | str:
    """Search CSuite events by name extracted from query. Returns event dict or error string."""
    # Extract event name from query
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

    # Search by name/description match
    if name:
        matches = [
            e for e in events
            if name.lower() in (e.get("event_description") or "").lower()
            or name.lower() in (e.get("event_name") or "").lower()
        ]
    else:
        # No name extracted — show recent events for user to pick
        non_archived = [e for e in events if not e.get("archived")]
        non_archived.sort(key=lambda e: e.get("event_date", ""), reverse=True)
        recent = non_archived[:5]
        lines = ["I couldn't determine which event. Here are the most recent:\n"]
        for i, e in enumerate(recent, 1):
            desc = e.get("event_description", e.get("event_name", "Unnamed"))
            date = e.get("event_date", "")
            lines.append(f"{i}. **{desc}** — {date}")
        lines.append("\nPlease specify the event name.")
        return "\n".join(lines)

    if not matches:
        return f"No events found matching '{name}'. Try 'list events' to see what's available."

    if len(matches) == 1:
        return matches[0]

    # Multiple matches — show options
    lines = [f"Found {len(matches)} events matching '{name}':\n"]
    for i, e in enumerate(matches, 1):
        desc = e.get("event_description", e.get("event_name", "Unnamed"))
        date = e.get("event_date", "")
        lines.append(f"{i}. **{desc}** — {date}")
    lines.append("\nPlease be more specific or include the date.")
    return "\n".join(lines)


def _extract_event_name(query: str, query_lower: str) -> str:
    """Extract the event name from a query string."""
    # Remove trigger phrases to isolate the event name
    all_triggers = _LIST_TRIGGERS + _ATTENDEE_TRIGGERS + _SYNC_TRIGGERS + _FOLLOWUP_TRIGGERS
    remaining = query_lower
    for phrase in sorted(all_triggers, key=len, reverse=True):
        remaining = remaining.replace(phrase, "")

    # Clean up common filler words
    for word in ["the", "for", "about", "our", "my", "a", "an"]:
        remaining = re.sub(rf"\b{word}\b", "", remaining)

    name = remaining.strip().strip('"\'')
    return name if len(name) > 2 else ""


def _fetch_event_detail(event_date_id: int, csuite) -> dict | str:
    """Fetch full event details including attendees."""
    try:
        result = csuite.get_event_date(event_date_id)
    except Exception as e:
        return f"Failed to fetch event details: {e}"

    if not result.get("success") or not result.get("data"):
        return "Could not retrieve event details from CSuite."

    return result["data"]


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
