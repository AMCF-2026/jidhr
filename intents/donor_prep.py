"""
Jidhr Donor Call Prep
=====================
Prepares talking points for calls/meetings with donors by pulling
data from both HubSpot and CSuite, then using Claude to generate
contextual talking points.

NEW in v1.3 — Survey priority: Muhi, Shazeen, Ola, Nora
"""

import logging
import re

from clients import mirror_read
from clients.users import get_user_by_email, normalize_email
from config import Config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Trigger keywords
# ---------------------------------------------------------------------------

TRIGGER_PHRASES = [
    'talking points', 'call prep', 'prepare for call', 'meeting with',
    'prepare for meeting', 'donor brief', 'call with', 'about to call',
    'visiting with', 'catching up with', 'prep for', 'brief me on',
    'brief on', 'background on',
]


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

    # A bare "2" answering a numbered candidate list. Narrow on purpose:
    # while a pick is pending this claims digits only, and only its own
    # pending key — notes.py has a separate one.
    if workflow_state and workflow_state.get(PENDING_PICK_KEY):
        if _PICK_RE.match(query or ""):
            return True

    return any(p in q for p in TRIGGER_PHRASES)


def handle(query: str, ctx) -> str:
    """
    Build a donor call-prep brief.

    Flow:
      1. Extract donor name from query
      2. Search HubSpot + CSuite, and keep only USABLE matches — rows with
         a real id whose name actually contains the name asked for
      3. Nobody usable in either system: say so and stop
      4. More than one usable match in a system: numbered list, stop
      5. Staff guard; then gather, Claude, brief
    """
    state = ctx.workflow_state

    # A pending numbered list takes priority: "2" means that candidate.
    if state and state.get(PENDING_PICK_KEY):
        picked = take_pending_pick(query, state)
        if picked is not None:
            name, contact, profile, message = picked
            if message:
                return message
            return _prep(name, contact, profile, ctx)

    name = _extract_donor_name(query)
    if not name:
        return (
            "❓ I need a donor name to prepare talking points. "
            'Try: *"Prepare talking points for a call with Ahmed"*'
        )

    logger.info(f"Preparing call prep for: {name}")

    hs_matches = _usable_hubspot_matches(name, ctx.services.hubspot)
    cs_matches = _usable_csuite_matches(name, ctx.services.csuite)

    if not hs_matches and not cs_matches:
        # Nothing to prep FROM. On 2026-09-11 this path produced talking
        # points for "Jones, Taisha Mumtazi": HubSpot found nothing, and
        # CSuite's search — which returns funds as well as profiles —
        # handed back a row with no profile_id and no email. It counted as
        # "found", and Claude wrote a brief with no facts in it.
        logger.info("Call prep: no usable match for %r in either system",
                    name)
        return (f"ℹ️ No contact named {name} found in HubSpot or CSuite — "
                "nothing to prep.")

    contact, profile, message = _choose(name, hs_matches, cs_matches, state)
    if message:
        return message

    return _prep(name, contact, profile, ctx)


def _prep(name: str, contact, profile, ctx) -> str:
    """Everything after the person is settled: guard, gather, write."""
    hubspot = ctx.services.hubspot
    csuite = ctx.services.csuite

    # ----- Stop if they work here -----
    #
    # BEFORE any data is gathered. A staff member's name matches a HubSpot
    # contact like anyone else's (everyone at AMCF is in the CRM), so
    # without this the brief would pull a colleague's notes, emails,
    # tickets and giving history and hand them to Claude to write talking
    # points about.
    staff_email = _staff_email_of(contact, profile)
    if staff_email:
        logger.info(
            "Call prep refused: %s resolves to staff address %s",
            name, staff_email)
        return (f"ℹ️ {name} is an AMCF staff member, not a donor — "
                "no call prep generated.")

    # ----- Gather data from both systems -----
    hs_data = _gather_hubspot_data(name, hubspot, contact=contact)
    cs_data = _gather_csuite_data(name, csuite, profile=profile)

    if not hs_data["found"] and not cs_data["found"]:
        return (f"ℹ️ No contact named {name} found in HubSpot or CSuite — "
                "nothing to prep.")

    # A match in only one system is a fact the brief should state, not
    # something the reader infers from a missing section.
    hs_data["missing_note"] = None if hs_data["found"] else \
        "Not found in HubSpot"
    cs_data["missing_note"] = None if cs_data["found"] else \
        "Not found in CSuite"

    # ----- Build context for Claude -----
    context = _build_context_block(name, hs_data, cs_data)

    # ----- Generate talking points via Claude -----
    talking_points = _generate_talking_points(name, context, ctx.services.claude)

    # ----- Format final output -----
    return _format_brief(name, hs_data, cs_data, talking_points)


# ---------------------------------------------------------------------------
# Name extraction
# ---------------------------------------------------------------------------

_STRIP_PREFIXES = re.compile(
    r'^(?:talking points for(?: a call)?|call prep|prepare for (?:call|meeting)|'
    r'donor brief|(?:call|meeting|visiting|catching up) with|'
    r'prep for|brief (?:me )?on|background on)\s+',
    re.IGNORECASE,
)


def _extract_donor_name(query: str) -> str | None:
    """Pull the donor name from the query after stripping trigger phrases."""
    cleaned = _STRIP_PREFIXES.sub('', query).strip()
    # Remove trailing punctuation
    cleaned = cleaned.rstrip('?!.')
    return cleaned if cleaned else None


# ---------------------------------------------------------------------------
# Deep links
# ---------------------------------------------------------------------------

def _real_id(value) -> bool:
    """True for an id worth putting in a URL.

    None, "", "None" and whitespace are what a missing id looks like after
    it has been through a dict.get and an f-string. A link built from one
    of those — .../contact/None — reads exactly like a real link and 404s.
    """
    if value is None or isinstance(value, bool):
        return False
    text = str(value).strip()
    return bool(text) and text.lower() != "none"


def _hubspot_contact_link(contact_id) -> str | None:
    """The HubSpot UI link for a contact, or None if there is no id."""
    if not _real_id(contact_id):
        return None
    return Config.HUBSPOT_CONTACT_URL.format(contact_id=str(contact_id).strip())


def _csuite_profile_link(profile_id) -> str | None:
    """The CSuite UI link for a profile, or None if there is no id."""
    if not _real_id(profile_id):
        return None
    return Config.CSUITE_PROFILE_URL.format(profile_id=str(profile_id).strip())


# ---------------------------------------------------------------------------
# Resolution and the staff guard
# ---------------------------------------------------------------------------

def _resolve_hubspot_contact(name: str, hubspot) -> dict | None:
    """The single usable HubSpot match, or None. Older callers only."""
    matches = _usable_hubspot_matches(name, hubspot)
    return matches[0] if len(matches) == 1 else None


def _resolve_csuite_profile(name: str, csuite) -> dict | None:
    """The single usable CSuite match, or None. Older callers only."""
    matches = _usable_csuite_matches(name, csuite)
    return matches[0] if len(matches) == 1 else None


# ---------------------------------------------------------------------------
# What counts as a match
# ---------------------------------------------------------------------------

_NAME_PUNCTUATION = re.compile(r"[^\w\s]", re.UNICODE)


def _name_tokens(text) -> list:
    """Lowercased word tokens of a name, punctuation stripped."""
    if not text:
        return []
    cleaned = _NAME_PUNCTUATION.sub(" ", str(text)).lower()
    return cleaned.split()


def query_last_name(name: str) -> str:
    """The surname the user typed, as tokens joined by spaces.

    "Jones, Taisha Mumtazi" -> "jones"  (before the comma)
    "Taisha Jones"          -> "jones"  (final word)
    "van der Berg, Anna"    -> "van der berg"
    "Aisha"                 -> "aisha"  (one word: it is what we have)
    """
    if not name:
        return ""
    if "," in name:
        head = name.split(",", 1)[0]
        return " ".join(_name_tokens(head))
    tokens = _name_tokens(name)
    return tokens[-1] if tokens else ""


def name_matches(query_name: str, candidate_name) -> bool:
    """True if the candidate's name contains the query's surname.

    Whole-word and case-insensitive, in either order — "Jones, Taisha"
    and "Taisha Jones" both match a query for Jones; "Jonesboro Trust"
    does not. A multi-word surname must appear as a phrase.
    """
    wanted = query_last_name(query_name)
    if not wanted:
        return False
    have = _name_tokens(candidate_name)
    if not have:
        return False
    needle = wanted.split()
    width = len(needle)
    return any(have[i:i + width] == needle
               for i in range(len(have) - width + 1))


def _hubspot_display_name(contact: dict) -> str:
    props = contact.get("properties") or {}
    parts = [props.get("firstname"), props.get("lastname")]
    name = " ".join(p for p in parts if p).strip()
    return name or props.get("email") or ""


def _csuite_display_name(profile: dict) -> str:
    name = profile.get("name")
    if name:
        return str(name)
    parts = [profile.get("first_name"), profile.get("last_name")]
    return " ".join(p for p in parts if p).strip()


def _usable_hubspot_matches(name: str, hubspot) -> list:
    """HubSpot contacts that have an id AND are actually this person."""
    try:
        search = hubspot.search_contacts(name)
        raw = search.get("results", []) if isinstance(search, dict) else []
    except Exception as e:
        logger.error(f"Error searching HubSpot for '{name}': {e}")
        return []

    usable = [
        c for c in raw
        if isinstance(c, dict)
        and _real_id(c.get("id"))
        and name_matches(name, _hubspot_display_name(c))
    ]
    logger.info("call prep search %r: HubSpot %d raw -> %d usable",
                name, len(raw), len(usable))
    return usable


def _usable_csuite_matches(name: str, csuite) -> list:
    """CSuite profiles that have a profile_id AND are actually this person.

    profile/list/search returns FUNDS as well as profiles (clients/csuite.py
    says so, and 2026-09-11 proved it): a fund row has no profile_id and
    no email, and used to count as "found".
    """
    try:
        search = csuite.search_profiles(name)
        if not isinstance(search, dict) or not search.get("success"):
            raw = []
        else:
            raw = (search.get("data") or {}).get("results", []) or []
    except Exception as e:
        logger.error(f"Error searching CSuite for '{name}': {e}")
        return []

    usable = [
        p for p in raw
        if isinstance(p, dict)
        and _real_id(p.get("profile_id"))
        and name_matches(name, _csuite_display_name(p))
    ]
    logger.info("call prep search %r: CSuite %d raw -> %d usable",
                name, len(raw), len(usable))
    return usable


# ---------------------------------------------------------------------------
# Choosing between several usable matches
# ---------------------------------------------------------------------------

PENDING_PICK_KEY = "pending_donor_pick"

# Picks are single digits, matching the contact / fund / event picks.
MAX_CHOICES = 9

_PICK_RE = re.compile(r"^\s*([1-9])\s*$")


def _candidate_label(system: str, record: dict) -> str:
    if system == "hubspot":
        props = record.get("properties") or {}
        who = _hubspot_display_name(record) or "Unnamed contact"
        detail = props.get("email") or "no email"
        return f"[HubSpot] {who} — {detail}"
    who = _csuite_display_name(record) or "Unnamed profile"
    detail = record.get("primary_email") or f"profile {record.get('profile_id')}"
    return f"[CSuite] {who} — {detail}"


def _choose(name: str, hs_matches: list, cs_matches: list, state,
            pinned: dict = None) -> tuple:
    """(contact, profile, message). A message means: stop and show it.

    A system with exactly one usable match is settled. A system with more
    than one is put to the user as a numbered list, and nothing is
    gathered or written until they answer. If both systems are ambiguous
    the user picks for one, then is asked about the other.
    """
    pinned = dict(pinned or {})

    ambiguous = []
    if "hubspot" not in pinned and len(hs_matches) > 1:
        ambiguous.append(("hubspot", hs_matches))
    if "csuite" not in pinned and len(cs_matches) > 1:
        ambiguous.append(("csuite", cs_matches))

    if ambiguous:
        system, candidates = ambiguous[0]
        if len(candidates) > MAX_CHOICES:
            return None, None, (
                f"❓ **{len(candidates)} {_system_label(system)} records "
                f"match '{name}'** — too many to list, and I haven't "
                "prepared anything. Give me a fuller name.")
        return None, None, _format_pick_list(
            name, system, candidates, state,
            hs_matches, cs_matches, pinned)

    contact = pinned.get("hubspot") or (hs_matches[0] if hs_matches else None)
    profile = pinned.get("csuite") or (cs_matches[0] if cs_matches else None)
    return contact, profile, None


def _system_label(system: str) -> str:
    return "HubSpot" if system == "hubspot" else "CSuite"


def _format_pick_list(name, system, candidates, state,
                      hs_matches, cs_matches, pinned) -> str:
    lines = [
        f"❓ **{len(candidates)} {_system_label(system)} records match "
        f"'{name}'** — I haven't prepared anything yet. Which one?",
        "",
    ]
    for index, record in enumerate(candidates, 1):
        lines.append(f"{index}. {_candidate_label(system, record)}")
    lines.append("")
    lines.append("Reply with the number, or give me a fuller name.")

    if state is not None:
        state[PENDING_PICK_KEY] = {
            "name": name,
            "system": system,
            "candidates": candidates,
            "hubspot": hs_matches,
            "csuite": cs_matches,
            "pinned": pinned,
        }
    return "\n".join(lines)


def take_pending_pick(query: str, state):
    """Resolve a bare 1-9 against the stored candidate list.

    Returns (name, contact, profile, message) on a pick — `message` set
    means a second list (the other system was ambiguous too) or an error.
    Returns None if the reply was not a pick. The pending list is cleared
    either way, so a non-digit reply falls through to normal routing.
    """
    pending = (state or {}).get(PENDING_PICK_KEY)
    if not pending:
        return None

    match = _PICK_RE.match(query or "")
    state.pop(PENDING_PICK_KEY, None)
    if not match:
        return None

    index = int(match.group(1)) - 1
    candidates = pending.get("candidates") or []
    if not 0 <= index < len(candidates):
        return (pending.get("name"), None, None,
                f"❓ There is no option {index + 1}. Ask again with a "
                "fuller name.")

    pinned = dict(pending.get("pinned") or {})
    pinned[pending["system"]] = candidates[index]

    name = pending.get("name")
    contact, profile, message = _choose(
        name, pending.get("hubspot") or [], pending.get("csuite") or [],
        state, pinned)
    return name, contact, profile, message


def is_staff_email(email) -> bool:
    """True if this address belongs to someone who works here.

    Two tests, either is enough: the domain is one staff log in from
    (Config.ALLOWED_LOGIN_DOMAINS), or the exact address has a row in
    `users` — which catches a colleague who signs in with a personal
    address the domain rule would miss.

    A users lookup that fails is treated as "not staff" and logged: the
    domain check has already run, and refusing every brief because the
    users table hiccupped would be the wrong trade.
    """
    email = normalize_email(email)
    if not email or "@" not in email:
        return False

    domain = email.rsplit("@", 1)[1]
    if domain in Config.ALLOWED_LOGIN_DOMAINS:
        return True

    try:
        return get_user_by_email(email) is not None
    except Exception as e:
        logger.warning(f"users lookup failed for staff check ({email}): {e}")
        return False


def _staff_email_of(contact, profile) -> str | None:
    """The first staff address among the resolved records, or None.

    Both records are checked. A colleague is usually in both systems; a
    guard that only read HubSpot would wave through anyone whose HubSpot
    record has no email but whose CSuite profile does.
    """
    candidates = []
    if isinstance(contact, dict):
        candidates.append((contact.get('properties') or {}).get('email'))
    if isinstance(profile, dict):
        candidates.append(profile.get('primary_email'))
        # The search endpoint's row shape is unmeasured (probe #3 skipped
        # profile/list/search); the mirrored profile carries the address
        # for certain, so it is consulted when the search row does not.
        if not profile.get('primary_email') and profile.get('profile_id'):
            try:
                mirrored = mirror_read.get("profile", profile.get('profile_id'))
            except Exception as e:
                logger.warning(f"mirror profile lookup failed: {e}")
                mirrored = None
            if mirrored:
                candidates.append(mirrored.get('primary_email'))

    for email in candidates:
        if is_staff_email(email):
            return normalize_email(email)
    return None


# ---------------------------------------------------------------------------
# HubSpot data gathering
# ---------------------------------------------------------------------------

def _gather_hubspot_data(name: str, hubspot, contact=None) -> dict:
    """Pull contact details + engagement history.

    `contact` is the record handle() already resolved; passing it avoids a
    second search. Left None, this searches itself (older callers).
    """
    data = {
        "found": False,
        "contact_id": None,
        "email": None,
        "phone": None,
        "company": None,
        "last_activity": None,
        "notes": [],
        "emails": [],
        "engagements": [],
        "tickets": [],
        "hubspot_link": None,
    }

    try:
        if contact is None:
            contact = _resolve_hubspot_contact(name, hubspot)
        if not contact:
            return data

        props = contact.get('properties', {})
        contact_id = contact.get('id')

        data.update({
            "found": True,
            "contact_id": contact_id,
            "email": props.get('email'),
            "phone": props.get('phone'),
            "company": props.get('company'),
            "last_activity": props.get('hs_last_activity_date') or props.get('lastmodifieddate'),
            "hubspot_link": _hubspot_contact_link(contact_id),
        })

        # Recent notes
        try:
            notes_resp = hubspot.get_contact_notes(contact_id, limit=5)
            note_results = notes_resp.get('results', []) if isinstance(notes_resp, dict) else []
            data["notes"] = [
                {
                    "body": n.get('properties', {}).get('hs_note_body', ''),
                    "timestamp": n.get('properties', {}).get('hs_timestamp', ''),
                }
                for n in note_results[:5]
            ]
        except Exception as e:
            logger.error(f"Error fetching notes for {contact_id}: {e}")

        # Recent emails
        try:
            emails_resp = hubspot.get_contact_emails(contact_id, limit=5)
            email_results = emails_resp.get('results', []) if isinstance(emails_resp, dict) else []
            data["emails"] = [
                {
                    "subject": e.get('properties', {}).get('hs_email_subject', ''),
                    "timestamp": e.get('properties', {}).get('hs_timestamp', ''),
                }
                for e in email_results[:5]
            ]
        except Exception as e:
            logger.error(f"Error fetching emails for {contact_id}: {e}")

        # Engagement history
        try:
            eng_resp = hubspot.get_contact_engagements(contact_id, limit=5)
            eng_results = eng_resp.get('results', []) if isinstance(eng_resp, dict) else []
            data["engagements"] = [
                {
                    "type": eg.get('type', ''),
                    "timestamp": eg.get('properties', {}).get('hs_timestamp', ''),
                }
                for eg in eng_results[:5]
            ]
        except Exception as e:
            logger.error(f"Error fetching engagements for {contact_id}: {e}")

        # Open tickets associated with THIS contact.
        #
        # What used to be here was hubspot.get_open_tickets() — every open
        # ticket in the portal — sliced to the first five and printed under
        # "Open Items" on this donor's brief. The comment said "best
        # effort"; there was no filter at all. Someone preparing for a call
        # was shown five strangers' tickets as this donor's open issues,
        # and Claude was handed them as context to build talking points
        # from. get_contact_tickets goes through the v4 associations
        # endpoint, so what appears here belongs to this contact.
        try:
            data["tickets"] = _contact_tickets(contact_id, hubspot)
        except Exception as e:
            logger.error(f"Error fetching tickets for {contact_id}: {e}")

    except Exception as e:
        logger.error(f"Error searching HubSpot for '{name}': {e}")

    return data


# HubSpot's default ticket pipeline uses "4" for Closed (see
# clients/hubspot.py close_ticket). Anything else is treated as open.
CLOSED_PIPELINE_STAGE = "4"

_TICKETS_SHOWN = 5


def _contact_tickets(contact_id, hubspot) -> list:
    """Open tickets associated with one contact, or an empty list.

    Never falls back to unassociated tickets: an empty "Open Items"
    section is correct, and a populated one about someone else is not.
    """
    tickets = hubspot.get_contact_tickets(contact_id)
    if not tickets:
        return []

    open_tickets = []
    for ticket in tickets:
        props = ticket.get("properties") or {}
        stage = str(props.get("hs_pipeline_stage") or "").strip()
        if stage == CLOSED_PIPELINE_STAGE:
            continue
        open_tickets.append({
            "subject": props.get("subject") or "No subject",
            "status": props.get("hs_pipeline_stage") or "Unknown",
            "created": (props.get("createdate") or "")[:10],
        })

    # Newest first: a brief has room for a few, and the recent ones are
    # what a call is likely to touch on.
    open_tickets.sort(key=lambda t: t["created"], reverse=True)
    return open_tickets[:_TICKETS_SHOWN]


# ---------------------------------------------------------------------------
# CSuite data gathering
# ---------------------------------------------------------------------------

def _gather_csuite_data(name: str, csuite, profile=None) -> dict:
    """Pull profile, giving and grants.

    `profile` is the record handle() already resolved; passing it avoids a
    second search. Left None, this searches itself (older callers).
    """
    data = {
        "found": False,
        "profile_id": None,
        "address": None,
        "status": None,
        "funds": [],
        "donations": [],
        "grants": [],
        "lifetime_giving": 0,
        "donation_count": None,
        "last_donation": None,
        "first_donation": None,
        "greatest_donation": None,
        "greatest_donation_date": None,
        "mirror_as_of": None,
        "giving_note": None,
        "csuite_link": None,
    }

    try:
        if profile is None:
            profile = _resolve_csuite_profile(name, csuite)
        if not profile:
            return data

        profile_id = profile.get('profile_id')

        data.update({
            "found": True,
            "profile_id": profile_id,
            "address": profile.get('address'),
            "status": profile.get('status'),
            "csuite_link": _csuite_profile_link(profile_id),
        })

        # Giving history, from the mirror's donation_agg row.
        #
        # This used to call get_donations_by_profile(profile_id, limit=20).
        # That method takes no `limit` (clients/csuite.py), so every call
        # raised TypeError, the except below swallowed it, and lifetime
        # giving stayed at its initial 0 — every brief printed
        # "Lifetime giving: $0.00" for every donor, including ones who had
        # given for years. The aggregate also covers all 26,500 donations
        # rather than one page of twenty.
        data.update(_giving_from_mirror(profile_id))

        # Grants by profile
        try:
            grants = csuite.get_grants_by_profile(profile_id, limit=10)
            if grants.get('success') and grants.get('data'):
                results = grants['data'].get('results', [])
                # `name` is the grantee (probe #2, C2). The grant list
                # endpoint carries no vendor field, so the old read
                # printed "to Unknown" for every grant a donor had
                # ever recommended.
                data["grants"] = [
                    {
                        "amount": g.get('grant_amount', '0'),
                        "vendor": g.get('name') or 'Unknown grantee',
                        "date": g.get('grant_date') or 'N/A',
                    }
                    for g in results[:10]
                ]
        except Exception as e:
            logger.error(f"Error fetching grants for profile {profile_id}: {e}")

    except Exception as e:
        logger.error(f"Error searching CSuite for '{name}': {e}")

    return data


def _giving_from_mirror(profile_id) -> dict:
    """Lifetime giving for one profile, from donation_agg.

    Returns only the keys it can fill, so the caller's defaults survive
    when the mirror has nothing. `giving_note` carries the reason when
    there are no figures, so the brief can say so rather than printing a
    confident $0.00.
    """
    empty = {"giving_note": "No recorded donations in CSuite mirror."}

    try:
        agg = mirror_read.get("donation_agg", profile_id)
    except Exception as e:
        logger.error(f"Mirror lookup failed for profile {profile_id}: {e}")
        return {"giving_note": "CSuite mirror unavailable — giving history "
                               "not shown."}

    if not agg:
        return empty

    def money(value) -> float:
        try:
            return float(str(value).replace(",", "").replace("$", "").strip())
        except (TypeError, ValueError):
            return 0.0

    gathered = {
        "lifetime_giving": money(agg.get("lifetime_total")),
        "donation_count": agg.get("count"),
        "last_donation": agg.get("latest_date"),
        "first_donation": agg.get("first_date"),
        "greatest_donation": agg.get("greatest_amount"),
        "greatest_donation_date": agg.get("greatest_date"),
        "mirror_as_of": agg.get("synced_at"),
        "giving_note": None,
    }

    # The aggregate holds no individual gifts by design, but it does name
    # the first and latest, which is what a brief actually quotes.
    highlights = []
    if agg.get("latest_date"):
        highlights.append({
            "amount": agg.get("latest_amount") or "0",
            "fund": agg.get("latest_fund") or "Unknown",
            "date": agg.get("latest_date"),
            "label": "Most recent",
        })
    if agg.get("first_date") and agg.get("first_date") != agg.get("latest_date"):
        highlights.append({
            "amount": agg.get("first_amount") or "0",
            "fund": agg.get("first_fund") or "Unknown",
            "date": agg.get("first_date"),
            "label": "First",
        })
    gathered["donations"] = highlights
    return gathered


# ---------------------------------------------------------------------------
# Context builder
# ---------------------------------------------------------------------------

def _build_context_block(name: str, hs: dict, cs: dict) -> str:
    """Assemble all gathered data into a context string for Claude."""
    sections = [f"Donor: {name}"]

    # HubSpot basics
    if hs["found"]:
        sections.append(
            f"HubSpot Contact: {hs['email'] or 'no email'}, "
            f"Phone: {hs['phone'] or 'none'}, "
            f"Company: {hs['company'] or 'none'}, "
            f"Last activity: {mirror_read.fmt_ts(hs['last_activity'])}"
        )
        if hs["notes"]:
            note_lines = [f"  - {n['timestamp']}: {n['body'][:120]}" for n in hs["notes"]]
            sections.append("Recent Notes:\n" + "\n".join(note_lines))
        if hs["emails"]:
            email_lines = [f"  - {e['timestamp']}: {e['subject']}" for e in hs["emails"]]
            sections.append("Recent Emails:\n" + "\n".join(email_lines))
        if hs["engagements"]:
            eng_lines = [f"  - {eg['timestamp']}: {eg['type']}" for eg in hs["engagements"]]
            sections.append("Recent Engagements:\n" + "\n".join(eng_lines))
        if hs["tickets"]:
            ticket_lines = [
                f"  - {t['subject']} (stage {t['status']}"
                + (f", opened {t['created']}" if t.get("created") else "") + ")"
                for t in hs["tickets"]
            ]
            sections.append(
                "Open Tickets associated with this contact:\n"
                + "\n".join(ticket_lines))

    # CSuite basics
    if cs["found"]:
        if cs.get("giving_note"):
            # Never a bare "$0.00": the model will build a talking point
            # around a donor having never given, which may simply be a
            # mirror that has not been loaded.
            sections.append(
                f"CSuite Profile ID: {cs['profile_id']}, "
                f"Status: {cs['status'] or 'unknown'}, "
                f"Giving history: {cs['giving_note']}"
            )
        else:
            count = cs.get("donation_count")
            sections.append(
                f"CSuite Profile ID: {cs['profile_id']}, "
                f"Status: {cs['status'] or 'unknown'}, "
                f"Lifetime giving: ${cs['lifetime_giving']:,.2f}"
                + (f" across {count} donations" if count else "") + ", "
                f"First donation: {cs.get('first_donation') or 'unknown'}, "
                f"Last donation: {cs['last_donation'] or 'unknown'}, "
                f"Largest donation: "
                f"${_money(cs.get('greatest_donation')):,.2f} on "
                f"{cs.get('greatest_donation_date') or 'unknown date'}"
            )
        if cs["donations"]:
            don_lines = [
                f"  - {d.get('label', 'Donation')}: ${d['amount']} to "
                f"{d['fund']} ({d['date']})"
                for d in cs["donations"][:5]
            ]
            sections.append("Donation Highlights:\n" + "\n".join(don_lines))
        if cs["grants"]:
            grant_lines = [f"  - ${g['amount']} to {g['vendor']} ({g['date']})" for g in cs["grants"][:5]]
            sections.append("Recent Grants:\n" + "\n".join(grant_lines))

    return "\n\n".join(sections)


# ---------------------------------------------------------------------------
# Claude talking-point generation
# ---------------------------------------------------------------------------

def _generate_talking_points(name: str, context: str, claude) -> str:
    """Send gathered context to Claude and get back talking points."""
    prompt = f"""Based on the following donor data, generate 4-6 concise talking points
for an upcoming call with {name}. Include:
- A warm opening reference (recent engagement or donation to acknowledge)
- Any follow-up items (open tickets, pending grants)
- Opportunities (fund growth, upcoming events, giving circle participation)
- A suggested ask or next step

Donor data:
{context}

Return only the talking points as a bulleted list. Be specific — use names,
dates, and dollar amounts from the data."""

    try:
        return claude.chat(
            messages=[{"role": "user", "content": prompt}],
            system_prompt=(
                "You are a donor relations advisor for AMCF (American Muslim Community Foundation). "
                "Generate warm, actionable talking points grounded in the provided data."
            ),
        )
    except Exception as e:
        logger.error(f"Error generating talking points: {e}")
        return "• (Could not generate talking points — see data above)"


# ---------------------------------------------------------------------------
# Output formatter
# ---------------------------------------------------------------------------

def _format_brief(name: str, hs: dict, cs: dict, talking_points: str) -> str:
    """Format the final call-prep brief."""
    lines = [f"📞 **Call Prep: {name}**", ""]

    # Deep links
    links = []
    if cs.get("csuite_link"):
        links.append(f"CSuite: {cs['csuite_link']}")
    if hs.get("hubspot_link"):
        links.append(f"HubSpot: {hs['hubspot_link']}")
    if links:
        lines.append("🔗 " + " | ".join(links))
        lines.append("")

    # Quick facts
    lines.append("**Quick Facts:**")
    for note in (hs.get("missing_note"), cs.get("missing_note")):
        if note:
            lines.append(f"• {note}")
    if cs["found"]:
        if cs.get("giving_note"):
            lines.append(f"• Giving history: {cs['giving_note']}")
        else:
            count = cs.get("donation_count")
            suffix = f" across {count} donations" if count else ""
            lines.append(
                f"• Lifetime giving: ${cs['lifetime_giving']:,.2f}{suffix}")
            if cs["last_donation"]:
                lines.append(f"• Last donation: {cs['last_donation']}")
            if cs.get("greatest_donation"):
                lines.append(
                    f"• Largest donation: "
                    f"${_money(cs['greatest_donation']):,.2f}"
                    + (f" ({cs['greatest_donation_date']})"
                       if cs.get("greatest_donation_date") else ""))
    if hs["found"]:
        lines.append(f"• Email: {hs['email'] or 'N/A'}")
        if hs["last_activity"]:
            lines.append(
                f"• Last contacted: {mirror_read.fmt_ts(hs['last_activity'])}")
    lines.append("")

    # Recent activity (condensed)
    activity_items = []
    if cs["donations"]:
        d = cs["donations"][0]
        activity_items.append(
            f"Latest donation: ${d['amount']} to {d['fund']} ({d['date']})")
    if cs["grants"]:
        g = cs["grants"][0]
        activity_items.append(f"Latest grant: ${g['amount']} to {g['vendor']} ({g['date']})")
    if hs["notes"]:
        n = hs["notes"][0]
        activity_items.append(f"Latest note: {n['body'][:80]}{'...' if len(n['body']) > 80 else ''}")

    if activity_items:
        lines.append("**Recent Activity:**")
        for item in activity_items:
            lines.append(f"• {item}")
        lines.append("")

    # Talking points
    lines.append("**Talking Points:**")
    lines.append(talking_points)
    lines.append("")

    # Open items
    if hs["tickets"]:
        lines.append("**Open Items:**")
        for t in hs["tickets"]:
            opened = f", opened {t['created']}" if t.get("created") else ""
            lines.append(f"• 🎫 {t['subject']} (stage {t['status']}{opened})")
        lines.append("")

    # Giving figures are mirrored, not live — say when they were taken.
    if cs["found"] and not cs.get("giving_note"):
        lines.append(mirror_read.as_of_line("donation_agg"))

    return "\n".join(lines)


def _money(value) -> float:
    """A stored money string as a float for formatting."""
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return 0.0