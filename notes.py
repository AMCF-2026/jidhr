"""
Jidhr Note Logging
==================
Log call notes, meeting notes, and generic notes to HubSpot contacts.

NEW in v1.3 — Survey: Ola rated 5, Shazeen rated 5, Muhi rated 3

Flow:
  1. Extract contact name + note body from query
  2. Search HubSpot for the contact
  3. Determine note type (call / meeting / generic)
  4. Create the engagement in HubSpot
  5. Return confirmation with link
"""

import logging
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Trigger keywords
# ---------------------------------------------------------------------------

TRIGGER_PHRASES = [
    'log a call', 'log call', 'log my call', 'call notes',
    'log a meeting', 'log meeting', 'meeting notes',
    'add a note', 'log a note', 'note about',
    'just spoke with', 'just met with', 'had a call with',
    'had a meeting with', 'spoke with', 'met with',
    # Giving Circle status upgrades (Lisa)
    'upgrade to voting member', 'make voting member',
    'set gc status', 'giving circle status',
    'upgrade gc', 'upgrade giving circle',
]

# Used to determine engagement type
_CALL_WORDS = ['call', 'spoke', 'phone', 'rang', 'dialed']
_MEETING_WORDS = ['meeting', 'met', 'visited', 'visit', 'sat down']


# ---------------------------------------------------------------------------
# Registry interface
# ---------------------------------------------------------------------------

def can_handle(query: str, **kwargs) -> bool:
    q = query.lower().strip()
    return any(p in q for p in TRIGGER_PHRASES)


def handle(query: str, assistant) -> str:
    """
    Parse the query, find the contact, and log the note.
    Also handles Giving Circle status upgrades.
    """
    q = query.lower().strip()

    # Route GC upgrades separately
    if any(w in q for w in ['upgrade to voting', 'make voting member',
                             'set gc status', 'giving circle status',
                             'upgrade gc', 'upgrade giving circle']):
        return _handle_gc_upgrade(query, q, assistant.hubspot)

    parsed = _parse_note_query(query)

    if not parsed["contact_name"]:
        return (
            "❓ I need a contact name to log a note. "
            'Try: *"Log my call with Ahmed - discussed DAF contribution timeline"*'
        )

    if not parsed["body"]:
        return (
            f"❓ What should I note for **{parsed['contact_name']}**? "
            'Try: *"Log call with Ahmed - discussed DAF contribution timeline"*'
        )

    name = parsed["contact_name"]
    note_type = parsed["type"]
    body = parsed["body"]

    logger.info(f"Logging {note_type} note for: {name}")

    # --- Find the contact ---
    contact_id = None
    hubspot_link = None
    try:
        search = assistant.hubspot.search_contacts(name)
        results = search.get('results', [])
        if results:
            contact_id = results[0].get('id')
            hubspot_link = f"https://app-na2.hubspot.com/contacts/243832852/contact/{contact_id}"
        else:
            return (
                f"❓ I couldn't find **{name}** in HubSpot. "
                "Double-check the spelling, or try a last name only."
            )
    except Exception as e:
        logger.error(f"Error searching for contact '{name}': {e}")
        return f"❌ Failed to search for contact: {e}"

    # --- Create the engagement ---
    try:
        if note_type == "call":
            result = assistant.hubspot.create_call_note(body=body, contact_id=contact_id)
        elif note_type == "meeting":
            title = f"Meeting with {name}"
            result = assistant.hubspot.create_meeting_note(
                title=title, body=body, contact_id=contact_id,
            )
        else:
            result = assistant.hubspot.create_note(body=body, contact_id=contact_id)

        if result and "error" in result:
            return f"❌ Failed to log note: {result['error']}"

    except Exception as e:
        logger.error(f"Error creating {note_type} note: {e}")
        return f"❌ Failed to log note: {e}"

    # --- Confirmation ---
    type_label = {"call": "📞 Call", "meeting": "🤝 Meeting", "note": "📝 Note"}[note_type]
    link_line = f"\n🔗 [View in HubSpot]({hubspot_link})" if hubspot_link else ""

    return f"""✅ **{type_label} note logged for {name}**

📝 {body}{link_line}"""


# ---------------------------------------------------------------------------
# Query parser
# ---------------------------------------------------------------------------

# Regex to split "log call with Ahmed - discussed contribution"
#   group 1: trigger/type phrase
#   group 2: contact name
#   group 3: note body (after separator)
_SEPARATORS = r'[\-–—:,]'
_TRIGGER_RE = re.compile(
    r'^(?:log (?:a |my )?(?:call|meeting|note)|'
    r'(?:call|meeting) notes?|'
    r'add (?:a )?note|'
    r'note about|'
    r'just (?:spoke|met) with|'
    r'had (?:a )?(?:call|meeting) with|'
    r'spoke with|met with)'
    r'\s+',
    re.IGNORECASE,
)


def _parse_note_query(query: str) -> dict:
    """
    Extract contact name, note body, and note type from the query.

    Examples:
      "Log my call with Ahmed - discussed DAF timeline"
        → contact: Ahmed, body: discussed DAF timeline, type: call
      "Just met with Sara and talked about endowment options"
        → contact: Sara, body: talked about endowment options, type: meeting
      "Add a note about Lisa: sent follow-up email"
        → contact: Lisa, body: sent follow-up email, type: note
    """
    result = {"contact_name": None, "body": None, "type": "note"}

    # Determine type from the raw query
    q_lower = query.lower()
    if any(w in q_lower for w in _CALL_WORDS):
        result["type"] = "call"
    elif any(w in q_lower for w in _MEETING_WORDS):
        result["type"] = "meeting"

    # Strip trigger phrase
    remainder = _TRIGGER_RE.sub('', query).strip()

    if not remainder:
        return result

    # Try splitting on separator (dash, colon, comma)
    sep_match = re.split(_SEPARATORS, remainder, maxsplit=1)
    if len(sep_match) == 2:
        result["contact_name"] = sep_match[0].strip().rstrip(' ')
        result["body"] = sep_match[1].strip()
    else:
        # Try splitting on conjunctions: "and", "about", "that"
        conj_match = re.split(r'\b(?:and then|and|about|that)\b', remainder, maxsplit=1)
        if len(conj_match) == 2 and len(conj_match[1].strip()) > 5:
            result["contact_name"] = conj_match[0].strip()
            result["body"] = conj_match[1].strip()
        else:
            # Last resort: entire remainder is the contact name, no body
            result["contact_name"] = remainder.strip()

    # Clean up contact name (remove "with" prefix if leftover)
    if result["contact_name"]:
        result["contact_name"] = re.sub(r'^with\s+', '', result["contact_name"], flags=re.IGNORECASE).strip()

    return result


# ---------------------------------------------------------------------------
# Giving Circle status upgrade (Lisa — #34)
# ---------------------------------------------------------------------------

def _handle_gc_upgrade(query: str, query_lower: str, hubspot) -> str:
    """Upgrade a contact's Giving Circle constituent code.

    Examples:
        "Upgrade Sara to voting member"
        "Set GC status for Ahmed to member"
        "Make Lisa a voting member"
    """
    # Determine target status (stored in constituent_codes)
    if 'voting' in query_lower:
        new_status = 'GC Voting Member'
        status_label = 'GC Voting Member'
    else:
        new_status = "American Muslim Women's Giving Circle"
        status_label = 'GC Member'

    # Extract contact name
    name = query_lower
    for phrase in ['upgrade to voting member', 'make voting member',
                   'upgrade gc', 'upgrade giving circle',
                   'set gc status', 'giving circle status',
                   'to voting member', 'to member',
                   'for', 'make', 'set', 'a']:
        name = name.replace(phrase, '')
    name = name.strip().strip('-:,')

    if not name or len(name) < 2:
        return (
            "I need a contact name. Try:\n"
            '*"Upgrade Sara to voting member"*\n'
            '*"Set GC status for Ahmed to member"*'
        )

    # Search for the contact
    try:
        search_result = hubspot.search_contacts(name, limit=3)
    except Exception as e:
        return f"Failed to search contacts: {e}"

    results = search_result.get('results', []) if isinstance(search_result, dict) else []
    if not results:
        return f"No contact found matching '{name}'. Check the name and try again."

    contact = results[0]
    contact_id = contact.get('id')
    props = contact.get('properties', {})
    full_name = f"{props.get('firstname', '')} {props.get('lastname', '')}".strip()
    current_status = props.get('constituent_codes', 'none')

    if current_status == new_status:
        return f"**{full_name}** is already set to **{status_label}**."

    # Update the status
    try:
        result = hubspot.update_giving_circle_status(contact_id, new_status)
        if result and 'error' not in result:
            return (
                f"**{full_name}** upgraded to **{status_label}**\n\n"
                f"Previous status: {current_status or 'none'}\n"
                f"[View in HubSpot]({hubspot.get_contact_url(contact_id)})"
            )
        else:
            error = result.get('error', 'Unknown error') if result else 'No response'
            return f"Failed to update status: {error}"
    except Exception as e:
        return f"Failed to update Giving Circle status: {e}"