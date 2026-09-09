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

# Giving Circle status commands, matched with the contact name in the middle.
# Substring matching could not see "upgrade Sara to voting member": the
# literal 'upgrade to voting member' never appears, so can_handle returned
# False and the query — the example in _handle_gc_upgrade's own docstring —
# fell through to the Claude fallback instead of reaching this module.
#
# The verb forms are anchored to the start of the message on purpose. Without
# that, "log a call with Sara — we discussed her upgrade to voting member"
# would route to the status writer instead of the note logger.
_GC_TRIGGER_RE = re.compile(
    r"""
      ^\s*upgrade\s+[\w .'\-]{0,40}?\bto\s+(?:voting\s+)?member\b
    | ^\s*make\s+[\w .'\-]{0,40}?\b(?:a\s+)?voting\s+member\b
    | ^\s*set\s+gc\s+status\b
    | ^\s*upgrade\s+gc\b
    | ^\s*upgrade\s+giving\s+circle\b
    | \bgiving\s+circle\s+status\b
    """,
    re.IGNORECASE | re.VERBOSE,
)


def is_gc_status_command(query: str) -> bool:
    """True if this message asks to change someone's Giving Circle status."""
    return bool(_GC_TRIGGER_RE.search(query or ""))


# Trailing punctuation an extracted name should never keep: "Khan." is the
# same contact as "Khan", but HubSpot's search treats them as different.
_NAME_EDGE_CHARS = " \t.,!?;:'\"-–—"

# Connectives left over once the trigger phrase is consumed. "log a note
# about Sara" leaves "about Sara", and HubSpot finds nobody by that name.
_NAME_LEAD_RE = re.compile(
    r"^(?:about|for|on|with|re)\b[:\s]+", re.IGNORECASE)


def _strip_name_lead(name: str) -> str:
    """Drop leading connectives from an extracted name.

    Looped because a trigger can leave more than one ("note about with Sara"),
    and bounded so a pathological input cannot spin.
    """
    text = name or ""
    for _ in range(3):
        stripped = _NAME_LEAD_RE.sub("", text, count=1)
        if stripped == text:
            break
        text = stripped
    return text


def _clean_name(name) -> str:
    """Normalise an extracted contact name.

    Collapses whitespace, drops a leading connective, and trims edge
    punctuation. Internal punctuation is preserved — real names contain
    apostrophes, hyphens and full stops.
    """
    if not name:
        return ""
    text = " ".join(str(name).split())
    text = _strip_name_lead(text)
    return text.strip(_NAME_EDGE_CHARS).strip()


# Used to determine engagement type
_CALL_WORDS = ['call', 'spoke', 'phone', 'rang', 'dialed']
_MEETING_WORDS = ['meeting', 'met', 'visited', 'visit', 'sat down']

# Picks are single digits, matching the fund and event picks.
MAX_CONTACT_CHOICES = 9

_CONTACT_PICK_RE = re.compile(r"^\s*([1-9])\s*$")


def _contact_label(contact: dict) -> str:
    """"Firstname Lastname — email" for one HubSpot contact."""
    props = contact.get("properties", {}) or {}
    name = " ".join(
        part for part in (props.get("firstname"), props.get("lastname")) if part
    ).strip()
    email = props.get("email") or "no email"
    return f"{name or 'Unnamed contact'} — {email}"


def _format_contact_choices(name: str, contacts: list, workflow_state,
                            action: str, payload: dict) -> str:
    """Number the matches and remember them, instead of writing to the first.

    Taking results[0] meant a note about one Ahmed landed on a different
    Ahmed's timeline, with a "✅ logged" to say it had gone well.
    """
    lines = [
        f"❓ **{len(contacts)} contacts match '{name}'** — I haven't written "
        "anything yet. Which one?",
        "",
    ]
    for index, contact in enumerate(contacts, 1):
        lines.append(f"{index}. {_contact_label(contact)}")
    lines.append("")
    lines.append("Reply with the number, or give me a fuller name.")

    if workflow_state is not None:
        workflow_state["pending_contact_pick"] = {
            "action": action,
            "name": name,
            "contacts": contacts,
            "payload": payload,
        }
    return "\n".join(lines)


def take_pending_contact_pick(query: str, workflow_state):
    """Resolve a bare 1-9 against a stored contact list.

    Returns (contact, pending) on a pick, else (None, None). The list is
    cleared either way.
    """
    pending = (workflow_state or {}).get("pending_contact_pick")
    if not pending:
        return None, None

    match = _CONTACT_PICK_RE.match(query or "")
    if not match:
        workflow_state.pop("pending_contact_pick", None)
        return None, None

    index = int(match.group(1)) - 1
    contacts = pending.get("contacts") or []
    workflow_state.pop("pending_contact_pick", None)

    if 0 <= index < len(contacts):
        return contacts[index], pending
    return None, None


def _resolve_contact(hubspot, name: str, workflow_state, action: str,
                     payload: dict):
    """Find exactly one contact, or return the message the user should see.

    Returns (contact, message). Exactly one is not None.
    """
    try:
        search = hubspot.search_contacts(name, limit=MAX_CONTACT_CHOICES + 1)
    except Exception as e:
        logger.error(f"Error searching for contact '{name}': {e}", exc_info=True)
        return None, f"❌ Failed to search for contact: {e}"

    results = (search or {}).get("results", []) if isinstance(search, dict) else []

    if not results:
        return None, (
            f"❓ I couldn't find **{name}** in HubSpot. "
            "Double-check the spelling, or try a last name only."
        )

    if len(results) == 1:
        return results[0], None

    if len(results) > MAX_CONTACT_CHOICES:
        return None, (
            f"❓ **{len(results)} contacts match '{name}'** — too many to list, "
            "and I haven't written anything. Give me a fuller name."
        )

    return None, _format_contact_choices(
        name, results, workflow_state, action, payload)


# ---------------------------------------------------------------------------
# Registry interface
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Access control
# ---------------------------------------------------------------------------

# Nothing is donor-facing yet; every handler is staff-and-above.
ALLOWED_ROLES = frozenset({"admin", "staff"})

def can_handle(query: str, workflow_state: dict = None, **kwargs) -> bool:
    q = query.lower().strip()

    # A bare "2" answering a numbered contact list. Narrow on purpose: while
    # a pick is pending this claims digits only.
    if workflow_state and workflow_state.get("pending_contact_pick"):
        if _CONTACT_PICK_RE.match(query or ""):
            return True

    if is_gc_status_command(q):
        return True

    return any(p in q for p in TRIGGER_PHRASES)


def handle(query: str, ctx) -> str:
    """
    Parse the query, find the contact, and log the note.
    Also handles Giving Circle status upgrades.
    """
    hubspot = ctx.services.hubspot
    state = ctx.workflow_state
    q = query.lower().strip()

    # A pending numbered contact list takes priority: "2" means that contact.
    if state.get("pending_contact_pick"):
        contact, pending = take_pending_contact_pick(query, state)
        if contact is not None:
            if pending.get("action") == "gc_status":
                return _apply_gc_status(hubspot, pending["payload"], contact)
            return _log_note(hubspot, pending["payload"], contact)

    # Route GC upgrades separately
    if is_gc_status_command(q):
        return _handle_gc_upgrade(query, q, hubspot, state)

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

    note = {"name": name, "body": body, "type": note_type}

    contact, message = _resolve_contact(hubspot, name, state, "note", note)
    if contact is None:
        # Ambiguous, missing, or the search failed — nothing was written.
        return message

    return _log_note(hubspot, note, contact)


def _log_note(hubspot, note: dict, contact: dict) -> str:
    """Create the engagement against one resolved contact."""
    contact_id = contact.get("id")
    note_type = note.get("type", "note")
    body = note.get("body")
    label = _contact_label(contact)

    try:
        if note_type == "call":
            result = hubspot.create_call_note(body=body, contact_id=contact_id)
        elif note_type == "meeting":
            result = hubspot.create_meeting_note(
                title=f"Meeting with {note.get('name')}",
                body=body, contact_id=contact_id,
            )
        else:
            result = hubspot.create_note(body=body, contact_id=contact_id)
    except Exception as e:
        logger.error(f"Error creating {note_type} note: {e}", exc_info=True)
        return f"❌ Failed to log note: {e}"

    if not result or (isinstance(result, dict) and result.get("error")):
        reason = (result or {}).get("error", "HubSpot returned no confirmation")
        return f"❌ Failed to log note: {reason}"

    type_label = {"call": "📞 Call", "meeting": "🤝 Meeting",
                  "note": "📝 Note"}.get(note_type, "📝 Note")
    link = f"\n🔗 [View in HubSpot]({hubspot.get_contact_url(contact_id)})" \
        if contact_id else ""

    return f"""✅ **{type_label} note logged for {label}**

📝 {body}{link}"""


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

    # Strip trigger phrase, then any connective it left behind. This has to
    # happen BEFORE the separator split: "log a note re: Sara" leaves
    # "re: Sara", and the colon would otherwise be read as the name/body
    # separator, making the contact "re".
    remainder = _strip_name_lead(_TRIGGER_RE.sub('', query).strip()).strip()

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

    # Clean up the contact name: drop a leading connective ("with", "about",
    # ...) and trailing punctuation, so "about Sara!" and "Sara" search alike.
    if result["contact_name"]:
        result["contact_name"] = _clean_name(result["contact_name"])

    return result


# ---------------------------------------------------------------------------
# Giving Circle status upgrade (Lisa — #34)
# ---------------------------------------------------------------------------

# Phrases stripped when isolating the name from a GC-status command. Applied
# with word boundaries: the old code chained str.replace, so the bare token
# 'a' removed every letter "a" in the query and "Sara" arrived as "Sr".
_GC_STRIP_PHRASES = [
    'upgrade to voting member', 'make voting member',
    'upgrade giving circle', 'giving circle status',
    'set gc status', 'upgrade gc',
    'to voting member', 'to member', 'voting member',
    'giving circle', 'member', 'status', 'gc',
    'upgrade', 'make', 'set', 'for', 'the', 'a', 'an',
]

_GC_STRIP_RE = re.compile(
    r"\b(?:%s)\b" % "|".join(
        re.escape(p) for p in sorted(_GC_STRIP_PHRASES, key=len, reverse=True)
    ),
    re.IGNORECASE,
)


def _extract_gc_name(query_lower: str) -> str:
    """Pull the contact name out of a Giving Circle status command."""
    name = _GC_STRIP_RE.sub(" ", query_lower or "")
    name = re.sub(r"\s+", " ", name)
    return _clean_name(name)


def _handle_gc_upgrade(query: str, query_lower: str, hubspot,
                       workflow_state=None) -> str:
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

    name = _extract_gc_name(query_lower)

    if not name or len(name) < 2:
        return (
            "I need a contact name. Try:\n"
            '*"Upgrade Sara to voting member"*\n'
            '*"Set GC status for Ahmed to member"*'
        )

    payload = {"new_status": new_status, "status_label": status_label,
               "name": name}

    contact, message = _resolve_contact(
        hubspot, name, workflow_state, "gc_status", payload)
    if contact is None:
        # Ambiguous, missing, or the search failed — nothing was written.
        return message

    return _apply_gc_status(hubspot, payload, contact)


def _apply_gc_status(hubspot, payload: dict, contact: dict) -> str:
    """Write the constituent code against one resolved contact."""
    new_status = payload["new_status"]
    status_label = payload["status_label"]

    contact_id = contact.get('id')
    props = contact.get('properties', {}) or {}
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