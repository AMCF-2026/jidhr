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

def can_handle(query: str, **kwargs) -> bool:
    q = query.lower().strip()
    return any(p in q for p in TRIGGER_PHRASES)


def handle(query: str, ctx) -> str:
    """
    Build a donor call-prep brief.

    Flow:
      1. Extract donor name from query
      2. Search HubSpot + CSuite for the person
      3. Gather engagement history, donations, grants, tickets
      4. Send everything to Claude to generate talking points
      5. Return formatted brief with deep links
    """
    name = _extract_donor_name(query)
    if not name:
        return (
            "❓ I need a donor name to prepare talking points. "
            'Try: *"Prepare talking points for a call with Ahmed"*'
        )

    logger.info(f"Preparing call prep for: {name}")

    # ----- Gather data from both systems -----
    hs_data = _gather_hubspot_data(name, ctx.services.hubspot)
    cs_data = _gather_csuite_data(name, ctx.services.csuite)

    if not hs_data["found"] and not cs_data["found"]:
        return (
            f"❓ I couldn't find **{name}** in HubSpot or CSuite. "
            "Double-check the spelling, or try a last name only."
        )

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
# HubSpot data gathering
# ---------------------------------------------------------------------------

def _gather_hubspot_data(name: str, hubspot) -> dict:
    """Search HubSpot and pull contact details + engagement history."""
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

    # Search for contact
    try:
        search = hubspot.search_contacts(name)
        results = search.get('results', [])
        if not results:
            return data

        contact = results[0]
        props = contact.get('properties', {})
        contact_id = contact.get('id')

        data.update({
            "found": True,
            "contact_id": contact_id,
            "email": props.get('email'),
            "phone": props.get('phone'),
            "company": props.get('company'),
            "last_activity": props.get('hs_last_activity_date') or props.get('lastmodifieddate'),
            "hubspot_link": f"https://app-na2.hubspot.com/contacts/243832852/contact/{contact_id}",
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

def _gather_csuite_data(name: str, csuite) -> dict:
    """Search CSuite and pull profile, donations, grants."""
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
        search = csuite.search_profiles(name)
        if not search.get('success') or not search.get('data'):
            return data

        results = search['data'].get('results', [])
        if not results:
            return data

        profile = results[0]
        profile_id = profile.get('profile_id')

        data.update({
            "found": True,
            "profile_id": profile_id,
            "address": profile.get('address'),
            "status": profile.get('status'),
            "csuite_link": Config.CSUITE_PROFILE_URL.format(profile_id=profile_id),
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
            f"Last activity: {hs['last_activity'] or 'unknown'}"
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
            lines.append(f"• Last contacted: {hs['last_activity']}")
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