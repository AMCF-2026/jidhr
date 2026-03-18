"""
Jidhr Context Gathering
=======================
Gathers relevant data from HubSpot and CSuite based on query keywords.

NOT a handler — does not produce final responses. Returns a context string
that gets injected into the Claude prompt so it can answer with real data.

Extracted from assistant.py lines 869-1043, with new triggers (v1.3)
and smarter lookups for existing triggers.
"""

import logging
import re
from config import Config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: extract a name-like phrase from a query
# ---------------------------------------------------------------------------

def _extract_name(query: str) -> str | None:
    """
    Try to pull a proper name out of a query.

    First strips common command prefixes, then looks for capitalised words.
    Returns the name string or None.
    """
    # Strip command prefixes to isolate the name
    _COMMAND_PREFIXES = [
        'pull up donor profile for', 'pull up profile for',
        'pull up donor for', 'pull up contact for',
        'donor profile for', 'contact profile for',
        'look up donor', 'look up contact', 'look up profile',
        'look up', 'pull up', 'search for', 'search up',
        'find donor', 'find contact', 'find profile', 'find',
        'show me donor', 'show me contact', 'show me profile',
        'show me', 'show donor', 'show contact', 'show profile',
        'get donor', 'get contact', 'get profile', 'get info on',
        'prep for my call with', 'prep for call with',
        'talking points for', 'call prep for',
        'who is', "who's",
    ]

    cleaned = query.strip()
    cleaned_lower = cleaned.lower()
    for prefix in sorted(_COMMAND_PREFIXES, key=len, reverse=True):
        if cleaned_lower.startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
            break

    STOP_WORDS = {
        'fund', 'balance', 'daf', 'endowment', 'grant', 'grants',
        'contact', 'donor', 'donors', 'email', 'person', 'who',
        'what', 'how', 'when', 'where', 'show', 'get', 'find',
        'list', 'tell', 'about', 'the', 'for', 'with', 'from',
        'look', 'up', 'search', 'check', 'csuite', 'hubspot',
        'donation', 'donations', 'profile', 'ticket', 'task',
        'recent', 'latest', 'last', 'all', 'any', 'many',
        'pull', 'me', 'my', 'a', 'an',
    }

    # Find sequences of capitalised words (2+ chars) that aren't stop words
    words = cleaned.split()
    name_parts = []
    for word in words:
        clean = re.sub(r'[^\w]', '', word)
        if clean and clean[0].isupper() and clean.lower() not in STOP_WORDS and len(clean) > 1:
            name_parts.append(clean)
        elif name_parts:
            break  # end of name sequence

    # Fallback: if prefix stripping left us with a clean name, use it
    if not name_parts and cleaned and len(cleaned) > 1:
        remaining = cleaned.strip().strip('"\'')
        if remaining and remaining[0].isupper():
            return remaining

    return ' '.join(name_parts) if name_parts else None


def _extract_id(query: str) -> str | None:
    """Extract a numeric ID from the query (e.g. 'fund 1234')."""
    match = re.search(r'\b(\d{2,})\b', query)
    return match.group(1) if match else None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def gather_context(query: str, hubspot, csuite) -> str:
    """
    Analyse the query for keywords and fetch relevant data.

    Args:
        query: The user's raw message
        hubspot: HubSpotClient instance
        csuite: CSuiteClient instance

    Returns:
        Context string (may be empty if no keywords matched)
    """
    context_parts = []
    query_lower = query.lower()

    logger.info(f"Gathering context for: {query_lower[:50]}...")

    # ------------------------------------------------------------------
    # FUND / BALANCE / DAF / ENDOWMENT / GRANT → CSuite
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['fund', 'balance', 'daf', 'endowment', 'grant']):
        context_parts += _gather_fund_context(query, query_lower, csuite)

    # ------------------------------------------------------------------
    # CONTACT / DONOR → HubSpot (+ CSuite cross-reference)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['contact', 'donor', 'email', 'person', 'who']):
        context_parts += _gather_contact_context(query, query_lower, hubspot, csuite)

    # ------------------------------------------------------------------
    # FORM / SUBMISSION / INQUIRY → HubSpot
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['form', 'submission', 'inquiry', 'submitted']):
        context_parts += _gather_form_context(query_lower, hubspot)

    # ------------------------------------------------------------------
    # SOCIAL / POST / PLATFORM → HubSpot
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['social', 'post', 'facebook', 'linkedin', 'schedule', 'channel']):
        context_parts += _gather_social_context(hubspot)

    # ------------------------------------------------------------------
    # EVENT → CSuite + HubSpot
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['event', 'symposium', 'webinar', 'registration', 'gala', 'dinner']):
        context_parts += _gather_event_context(csuite, hubspot)

    # ------------------------------------------------------------------
    # DONATION / GIFT → CSuite
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['donation', 'gift', 'gave', 'contributed', 'recent donations']):
        context_parts += _gather_donation_context(query, query_lower, csuite)

    # ------------------------------------------------------------------
    # TICKET / SUPPORT → HubSpot
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['ticket', 'support', 'issue', 'help desk', 'open tickets']):
        context_parts += _gather_ticket_context(hubspot)

    # ------------------------------------------------------------------
    # CLOSED TICKETS → HubSpot (Shazeen)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['closed tickets', 'closed ticket', 'resolved tickets',
                                       'which tickets are closed', 'what tickets are closed',
                                       'tickets are done', 'tickets closed']):
        context_parts += _gather_closed_ticket_context(hubspot)

    # ------------------------------------------------------------------
    # CAMPAIGN → HubSpot
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['campaign', 'marketing campaign']):
        context_parts += _gather_campaign_context(hubspot)

    # ------------------------------------------------------------------
    # TASK → HubSpot
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['task', 'tasks', 'to do', 'todo', 'my tasks']):
        context_parts += _gather_task_context(hubspot)

    # ------------------------------------------------------------------
    # FUND-ASSOCIATED CONTACTS → HubSpot (by csuite_fund_id)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['associated with', 'contacts for', 'contacts in fund', 'who is in', 'who\'s in']):
        context_parts += _gather_fund_contacts_context(query, query_lower, hubspot, csuite)

    # ------------------------------------------------------------------
    # NEW v1.3: CHECK / UNCASHED → CSuite (Muhi)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['check', 'cashed', 'uncashed', 'cleared']):
        context_parts += _gather_check_context(query_lower, csuite)

    # ------------------------------------------------------------------
    # NEW v1.3: FEE → CSuite (Muhi)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['fee', 'fees', 'admin fee']):
        context_parts += _gather_fee_context(csuite)

    # ------------------------------------------------------------------
    # NEW v1.3: VOUCHER / PAYMENT → CSuite
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['voucher', 'payment']):
        context_parts += _gather_voucher_context(csuite)

    # ------------------------------------------------------------------
    # NEW v1.3: PROFILE → CSuite
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['profile', 'profiles']):
        context_parts += _gather_profile_context(query, csuite)

    # ------------------------------------------------------------------
    # GIVING CIRCLE → HubSpot (Lisa)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['giving circle', 'gc member', 'gc status',
                                       'giving circle member', 'circle member']):
        context_parts += _gather_giving_circle_context(query_lower, hubspot)

    # ------------------------------------------------------------------
    # NEW v1.3: LAPSED / INACTIVE context hints (for reports module)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['lapsed', 'inactive', "haven't donated", 'dormant']):
        context_parts.append(
            "[Hint] This looks like a lapsed/inactive analysis request. "
            "The reports module can run full comparisons."
        )

    result = "\n\n".join(context_parts) if context_parts else ""
    logger.info(f"Total context gathered: {len(result)} chars")
    return result


# ---------------------------------------------------------------------------
# Per-category gatherers
# ---------------------------------------------------------------------------

def _gather_fund_context(query: str, query_lower: str, csuite) -> list:
    """Fund-related: search by name if possible, else list funds. Also grants."""
    parts = []
    name = _extract_name(query)
    fund_id = _extract_id(query)

    # Enhanced: search by name if a proper name is detected
    if name:
        logger.info(f"Searching CSuite funds for: {name}")
        try:
            search_data = csuite.search_funds(name)
            if search_data.get('success') and search_data.get('data'):
                results = search_data['data'].get('results', [])
                if results:
                    fund_list = [
                        f"{f.get('fund_name', 'Unknown')} (ID: {f.get('funit_id', 'N/A')}, Balance: ${f.get('balance', '0')})"
                        for f in results[:10]
                    ]
                    parts.append(f"CSuite Fund Search '{name}':\n" + "\n".join(fund_list))
                    logger.info(f"Found {len(fund_list)} matching funds")
        except Exception as e:
            logger.error(f"Error searching funds: {e}")

    # Enhanced: fetch specific fund details if an ID is present
    if fund_id:
        logger.info(f"Fetching CSuite fund details for ID: {fund_id}")
        try:
            fund_data = csuite.get_fund(fund_id)
            if fund_data.get('success') and fund_data.get('data'):
                f = fund_data['data']
                parts.append(
                    f"CSuite Fund Detail:\n"
                    f"Name: {f.get('fund_name', 'Unknown')}\n"
                    f"ID: {f.get('funit_id', 'N/A')}\n"
                    f"Balance: ${f.get('balance', '0')}\n"
                    f"Status: {f.get('status', 'Unknown')}"
                )
        except Exception as e:
            logger.error(f"Error fetching fund detail: {e}")

    # Fallback: generic fund list (only if no specific search produced results)
    if not parts:
        logger.info("Fetching CSuite funds (generic)...")
        try:
            funds_data = csuite.get_funds(limit=20)
            if funds_data.get('success') and funds_data.get('data'):
                results = funds_data['data'].get('results', [])
                fund_list = [
                    f"{f.get('fund_name', 'Unknown')} (ID: {f.get('funit_id', 'N/A')})"
                    for f in results[:10]
                ]
                parts.append(f"CSuite Funds:\n" + "\n".join(fund_list))
                logger.info(f"Found {len(fund_list)} funds")
        except Exception as e:
            logger.error(f"Error fetching funds: {e}")

    # Enhanced: grant-specific queries pull grants by fund
    if 'grant' in query_lower and fund_id:
        logger.info(f"Fetching grants for fund {fund_id}...")
        try:
            grants_data = csuite.get_grants_by_fund(fund_id, limit=10)
            if grants_data.get('success') and grants_data.get('data'):
                results = grants_data['data'].get('results', [])
                grant_list = [
                    f"${g.get('grant_amount', '0')} to {g.get('vendor_name', 'Unknown')} ({g.get('grant_date', 'No date')})"
                    for g in results[:10]
                ]
                parts.append(f"Grants for Fund {fund_id}:\n" + "\n".join(grant_list))
                logger.info(f"Found {len(grant_list)} grants")
        except Exception as e:
            logger.error(f"Error fetching grants by fund: {e}")

    return parts


def _gather_contact_context(query: str, query_lower: str, hubspot, csuite) -> list:
    """Contact-related: search by name if possible, else list recent contacts."""
    parts = []
    name = _extract_name(query)

    # Enhanced: search by name in HubSpot
    if name:
        logger.info(f"Searching HubSpot contacts for: {name}")
        try:
            search_data = hubspot.search_contacts(name)
            if 'results' in search_data and search_data['results']:
                contact_list = [
                    f"{c.get('properties', {}).get('firstname', '')} "
                    f"{c.get('properties', {}).get('lastname', '')} "
                    f"({c.get('properties', {}).get('email', 'No email')}) "
                    f"[ID: {c.get('id', 'N/A')}]"
                    for c in search_data['results'][:5]
                ]
                parts.append(f"HubSpot Contact Search '{name}':\n" + "\n".join(contact_list))
                logger.info(f"Found {len(contact_list)} matching contacts")
        except Exception as e:
            logger.error(f"Error searching contacts: {e}")

        # Enhanced: also search CSuite for cross-system context
        logger.info(f"Searching CSuite profiles for: {name}")
        try:
            profile_data = csuite.search_profiles(name)
            if profile_data.get('success') and profile_data.get('data'):
                results = profile_data['data'].get('results', [])
                if results:
                    profile_list = [
                        f"{p.get('name', 'Unknown')} (Profile ID: {p.get('profile_id', 'N/A')})"
                        for p in results[:5]
                    ]
                    parts.append(f"CSuite Profile Search '{name}':\n" + "\n".join(profile_list))
                    logger.info(f"Found {len(profile_list)} matching profiles")
        except Exception as e:
            logger.error(f"Error searching CSuite profiles: {e}")

    # Fallback: generic contact list
    if not parts:
        logger.info("Fetching HubSpot contacts (generic)...")
        try:
            contacts_data = hubspot.get_contacts(limit=10)
            if 'results' in contacts_data:
                contact_list = [
                    f"{c.get('properties', {}).get('firstname', '')} "
                    f"{c.get('properties', {}).get('lastname', '')} "
                    f"({c.get('properties', {}).get('email', 'No email')})"
                    for c in contacts_data['results'][:5]
                ]
                parts.append(f"HubSpot Contacts:\n" + "\n".join(contact_list))
                logger.info(f"Found {len(contact_list)} contacts")
        except Exception as e:
            logger.error(f"Error fetching contacts: {e}")

    return parts


def _gather_form_context(query_lower: str, hubspot) -> list:
    """Form-related: generic forms list + DAF/endowment submissions if relevant."""
    parts = []

    # Always fetch form list
    logger.info("Fetching HubSpot forms...")
    try:
        forms_data = hubspot.get_forms(limit=10)
        if 'results' in forms_data:
            form_list = [
                f"{f.get('name', 'Unknown')} (ID: {f.get('id', 'N/A')})"
                for f in forms_data['results'][:5]
            ]
            parts.append(f"HubSpot Forms:\n" + "\n".join(form_list))
            logger.info(f"Found {len(form_list)} forms")
    except Exception as e:
        logger.error(f"Error fetching forms: {e}")

    # Enhanced: pull recent DAF inquiry submissions
    if any(w in query_lower for w in ['daf', 'inquiry', 'submitted', 'submission']):
        logger.info("Fetching DAF inquiry submissions...")
        try:
            resp = hubspot.get_daf_inquiry_submissions(limit=5)
            subs = resp.get('results', []) if isinstance(resp, dict) else []
            if subs:
                sub_list = [
                    f"Submitted {s.get('submittedAt', 'Unknown date')}: "
                    + ", ".join(f"{v.get('name', '?')}={v.get('value', '')}" for v in s.get('values', [])[:4])
                    for s in subs[:5]
                ]
                parts.append(f"Recent DAF Inquiry Submissions:\n" + "\n".join(sub_list))
                logger.info(f"Found {len(sub_list)} DAF submissions")
        except Exception as e:
            logger.error(f"Error fetching DAF submissions: {e}")

    # Enhanced: pull recent endowment inquiry submissions
    if any(w in query_lower for w in ['endowment', 'inquiry', 'submitted', 'submission']):
        logger.info("Fetching endowment inquiry submissions...")
        try:
            resp = hubspot.get_endowment_inquiry_submissions(limit=5)
            subs = resp.get('results', []) if isinstance(resp, dict) else []
            if subs:
                sub_list = [
                    f"Submitted {s.get('submittedAt', 'Unknown date')}: "
                    + ", ".join(f"{v.get('name', '?')}={v.get('value', '')}" for v in s.get('values', [])[:4])
                    for s in subs[:5]
                ]
                parts.append(f"Recent Endowment Inquiry Submissions:\n" + "\n".join(sub_list))
                logger.info(f"Found {len(sub_list)} endowment submissions")
        except Exception as e:
            logger.error(f"Error fetching endowment submissions: {e}")

    return parts


def _gather_social_context(hubspot) -> list:
    """Social channels and recent broadcasts from HubSpot."""
    parts = []
    logger.info("Fetching HubSpot social context...")
    try:
        channels_data = hubspot.get_social_channels()
        if isinstance(channels_data, list):
            channel_list = [
                f"{c.get('name', 'Unknown')} ({c.get('channelType', 'Unknown')})"
                for c in channels_data[:5]
            ]
            parts.append(f"Social Channels:\n" + "\n".join(channel_list))
            logger.info(f"Found {len(channel_list)} channels")
    except Exception as e:
        logger.error(f"Error fetching social channels: {e}")

    # Fetch recent broadcasts for context
    try:
        broadcasts = hubspot.get_social_broadcasts(limit=5)
        if isinstance(broadcasts, list) and broadcasts:
            broadcast_list = []
            for b in broadcasts[:5]:
                status = b.get("status", "Unknown")
                created = b.get("createdAt", "")
                channel = b.get("channelKey", "")
                clicks = b.get("clicks", 0)
                interactions = b.get("interactions", 0)
                broadcast_list.append(
                    f"  - {channel} | Status: {status} | "
                    f"Clicks: {clicks} | Interactions: {interactions}"
                )
            parts.append("Recent Social Posts:\n" + "\n".join(broadcast_list))
        elif not broadcasts:
            parts.append(
                "Social Analytics Note: HubSpot does not provide a dedicated "
                "social performance metrics API via personal access tokens. "
                "For detailed social analytics, use the HubSpot Social dashboard directly."
            )
    except Exception as e:
        logger.error(f"Error fetching social broadcasts: {e}")
        parts.append(
            "Social Analytics Note: Could not fetch social data. "
            "For performance metrics, use the HubSpot Social dashboard directly."
        )

    return parts


def _gather_event_context(csuite, hubspot) -> list:
    """Events from both CSuite and HubSpot."""
    parts = []

    # CSuite Events
    logger.info("Fetching CSuite events...")
    try:
        csuite_events = csuite.get_event_dates(limit=10)
        if csuite_events.get('success') and csuite_events.get('data'):
            results = csuite_events['data'].get('results', [])
            event_list = [
                f"{e.get('event_description') or e.get('event_name', 'Unknown')} ({e.get('event_date', 'No date')})"
                for e in results[:5]
            ]
            parts.append(f"CSuite Events:\n" + "\n".join(event_list))
            logger.info(f"Found {len(event_list)} CSuite events")
    except Exception as e:
        logger.error(f"Error fetching CSuite events: {e}")

    # HubSpot Events
    logger.info("Fetching HubSpot marketing events...")
    try:
        hubspot_events = hubspot.get_marketing_events(limit=5)
        if 'results' in hubspot_events:
            event_list = [
                f"{e.get('eventName', 'Unknown')} ({e.get('startDateTime', 'No date')})"
                for e in hubspot_events['results'][:5]
            ]
            parts.append(f"HubSpot Marketing Events:\n" + "\n".join(event_list))
            logger.info(f"Found {len(event_list)} HubSpot events")
    except Exception as e:
        logger.error(f"Error fetching HubSpot events: {e}")

    return parts


def _gather_donation_context(query: str, query_lower: str, csuite) -> list:
    """Donation-related: profile-specific if possible, else recent donations."""
    parts = []
    profile_id = _extract_id(query)

    # Enhanced: donations for a specific profile
    if profile_id:
        logger.info(f"Fetching donations for profile {profile_id}...")
        try:
            donations_data = csuite.get_donations_by_profile(profile_id, limit=10)
            if donations_data.get('success') and donations_data.get('data'):
                results = donations_data['data'].get('results', [])
                donation_list = [
                    f"${d.get('donation_amount', '0')} to {d.get('fund_name', 'Unknown')} ({d.get('donation_date', 'No date')})"
                    for d in results[:10]
                ]
                parts.append(f"Donations for Profile {profile_id}:\n" + "\n".join(donation_list))
                logger.info(f"Found {len(donation_list)} donations for profile")
        except Exception as e:
            logger.error(f"Error fetching profile donations: {e}")

    # Fallback: recent donations
    if not parts:
        logger.info("Fetching CSuite donations (generic)...")
        try:
            donations_data = csuite.get_donations(limit=10)
            if donations_data.get('success') and donations_data.get('data'):
                results = donations_data['data'].get('results', [])
                donation_list = [
                    f"{d.get('name', 'Unknown')}: ${d.get('donation_amount', '0')} to {d.get('fund_name', 'Unknown')} ({d.get('donation_date', 'No date')})"
                    for d in results[:5]
                ]
                parts.append(f"CSuite Donations:\n" + "\n".join(donation_list))
                logger.info(f"Found {len(donation_list)} donations")
        except Exception as e:
            logger.error(f"Error fetching donations: {e}")

    return parts


def _gather_ticket_context(hubspot) -> list:
    """Tickets from HubSpot."""
    parts = []
    logger.info("Fetching HubSpot tickets...")
    try:
        tickets_data = hubspot.get_tickets(limit=10)
        if 'results' in tickets_data:
            ticket_list = []
            for t in tickets_data['results'][:10]:
                props = t.get('properties', {})
                subject = props.get('subject', 'No subject')
                status = props.get('hs_pipeline_stage', 'Unknown')
                ticket_list.append(f"{subject} (Status: {status})")
            if ticket_list:
                parts.append(f"HubSpot Tickets:\n" + "\n".join(ticket_list))
                logger.info(f"Found {len(ticket_list)} tickets")
    except Exception as e:
        logger.error(f"Error fetching tickets: {e}")
    return parts


def _gather_closed_ticket_context(hubspot) -> list:
    """Closed tickets from HubSpot (Shazeen)."""
    parts = []
    logger.info("Fetching closed HubSpot tickets...")
    try:
        tickets_data = hubspot.get_closed_tickets(limit=20)
        if 'results' in tickets_data:
            ticket_list = []
            for t in tickets_data['results']:
                props = t.get('properties', {})
                subject = props.get('subject', 'No subject')
                closed_date = (props.get('hs_lastmodifieddate') or '')[:10]
                ticket_list.append(f"{subject} (Closed: {closed_date})")
            if ticket_list:
                parts.append(f"Closed HubSpot Tickets ({len(ticket_list)}):\n" + "\n".join(ticket_list))
                logger.info(f"Found {len(ticket_list)} closed tickets")
            else:
                parts.append("No closed tickets found.")
    except Exception as e:
        logger.error(f"Error fetching closed tickets: {e}")
    return parts


def _gather_campaign_context(hubspot) -> list:
    """Campaigns from HubSpot."""
    parts = []
    logger.info("Fetching HubSpot campaigns...")
    try:
        campaigns_data = hubspot.get_campaigns(limit=10)
        if 'results' in campaigns_data:
            campaign_list = [
                f"Campaign ID: {c.get('id', 'Unknown')}"
                for c in campaigns_data['results'][:5]
            ]
            if campaign_list:
                parts.append(f"HubSpot Campaigns:\n" + "\n".join(campaign_list))
                logger.info(f"Found {len(campaign_list)} campaigns")
    except Exception as e:
        logger.error(f"Error fetching campaigns: {e}")
    return parts


def _gather_task_context(hubspot) -> list:
    """Tasks from HubSpot."""
    parts = []
    logger.info("Fetching HubSpot tasks...")
    try:
        tasks_data = hubspot.get_tasks(limit=10)
        if 'results' in tasks_data:
            task_list = []
            for t in tasks_data['results'][:10]:
                props = t.get('properties', {})
                subject = props.get('hs_task_subject', 'No subject')
                status = props.get('hs_task_status', 'Unknown')
                task_list.append(f"{subject} (Status: {status})")
            if task_list:
                parts.append(f"HubSpot Tasks:\n" + "\n".join(task_list))
                logger.info(f"Found {len(task_list)} tasks")
    except Exception as e:
        logger.error(f"Error fetching tasks: {e}")
    return parts


# ---------------------------------------------------------------------------
# NEW v1.3 gatherers
# ---------------------------------------------------------------------------

def _gather_check_context(query_lower: str, csuite) -> list:
    """Check/uncashed queries → CSuite (Muhi)."""
    parts = []

    if 'uncashed' in query_lower or "haven't cashed" in query_lower or 'not cashed' in query_lower:
        logger.info("Fetching uncashed checks...")
        try:
            checks = csuite.get_uncashed_checks()
            if checks:
                check_list = [
                    f"Check #{c.get('check_num', '?')}: ${c.get('amount', '0')} to {c.get('vendor_name', 'Unknown')} ({c.get('check_date', 'No date')})"
                    for c in checks[:10]
                ]
                parts.append(f"Uncashed Checks:\n" + "\n".join(check_list))
                logger.info(f"Found {len(check_list)} uncashed checks")
        except Exception as e:
            logger.error(f"Error fetching uncashed checks: {e}")
    else:
        logger.info("Fetching CSuite checks...")
        try:
            checks_data = csuite.get_checks(limit=10)
            if checks_data.get('success') and checks_data.get('data'):
                results = checks_data['data'].get('results', [])
                check_list = [
                    f"Check #{c.get('check_number', '?')}: ${c.get('amount', '0')} ({c.get('status', 'Unknown')})"
                    for c in results[:10]
                ]
                parts.append(f"CSuite Checks:\n" + "\n".join(check_list))
                logger.info(f"Found {len(check_list)} checks")
        except Exception as e:
            logger.error(f"Error fetching checks: {e}")

    return parts


def _gather_fee_context(csuite) -> list:
    """Fee queries → CSuite fund fee types (Muhi)."""
    parts = []
    logger.info("Fetching CSuite fund fee types...")
    try:
        fee_data = csuite.get_fund_fee_types()
        if fee_data.get('success') and fee_data.get('data'):
            results = fee_data['data'].get('results', [])
            fee_list = [
                f"{f.get('fee_name', 'Unknown')}: {f.get('fee_percent', '?')}% (min: ${f.get('min_fee', '0')})"
                for f in results[:10]
            ]
            parts.append(f"CSuite Fee Types:\n" + "\n".join(fee_list))
            logger.info(f"Found {len(fee_list)} fee types")
    except Exception as e:
        logger.error(f"Error fetching fee types: {e}")
    return parts


def _gather_voucher_context(csuite) -> list:
    """Voucher/payment queries → CSuite."""
    parts = []
    logger.info("Fetching CSuite vouchers...")
    try:
        voucher_data = csuite.get_vouchers(limit=10)
        if voucher_data.get('success') and voucher_data.get('data'):
            results = voucher_data['data'].get('results', [])
            voucher_list = [
                f"Voucher #{v.get('voucher_id', '?')}: ${v.get('amount', '0')} — {v.get('description', 'No description')} ({v.get('voucher_date', 'No date')})"
                for v in results[:10]
            ]
            parts.append(f"CSuite Vouchers:\n" + "\n".join(voucher_list))
            logger.info(f"Found {len(voucher_list)} vouchers")
    except Exception as e:
        logger.error(f"Error fetching vouchers: {e}")
    return parts


def _gather_profile_context(query: str, csuite) -> list:
    """Profile queries → CSuite. Search by name if possible."""
    parts = []
    name = _extract_name(query)

    if name:
        logger.info(f"Searching CSuite profiles for: {name}")
        try:
            profile_data = csuite.search_profiles(name)
            if profile_data.get('success') and profile_data.get('data'):
                results = profile_data['data'].get('results', [])
                if results:
                    profile_list = [
                        f"{p.get('name', 'Unknown')} (ID: {p.get('profile_id', 'N/A')}, Email: {p.get('email', 'N/A')})"
                        for p in results[:10]
                    ]
                    parts.append(f"CSuite Profiles matching '{name}':\n" + "\n".join(profile_list))
                    logger.info(f"Found {len(profile_list)} matching profiles")
        except Exception as e:
            logger.error(f"Error searching profiles: {e}")

    if not parts:
        logger.info("Fetching CSuite profiles (generic)...")
        try:
            profile_data = csuite.get_profiles(limit=10)
            if profile_data.get('success') and profile_data.get('data'):
                results = profile_data['data'].get('results', [])
                profile_list = [
                    f"{p.get('name', 'Unknown')} (ID: {p.get('profile_id', 'N/A')})"
                    for p in results[:10]
                ]
                parts.append(f"CSuite Profiles:\n" + "\n".join(profile_list))
                logger.info(f"Found {len(profile_list)} profiles")
        except Exception as e:
            logger.error(f"Error fetching profiles: {e}")

    return parts


def _gather_fund_contacts_context(query: str, query_lower: str, hubspot, csuite) -> list:
    """Find HubSpot contacts linked to a specific CSuite fund.

    Flow: extract fund name/ID → resolve to funit_id via CSuite if needed
          → search HubSpot contacts by csuite_fund_id property.
    """
    parts = []
    name = _extract_name(query)
    fund_id = _extract_id(query)

    # If no numeric ID, try to resolve fund name → funit_id via CSuite search
    if name and not fund_id:
        logger.info(f"Resolving fund name to ID for: {name}")
        try:
            search = csuite.search_funds(name)
            if search.get('success') and search.get('data'):
                results = search['data'].get('results', [])
                if results:
                    fund_id = str(results[0].get('funit_id', ''))
                    fund_display = results[0].get('fund_name', name)
                    logger.info(f"Resolved '{name}' to fund ID {fund_id}")
        except Exception as e:
            logger.error(f"Error resolving fund name: {e}")

    if not fund_id:
        return parts

    fund_display = fund_display if 'fund_display' in dir() else f"Fund {fund_id}"

    logger.info(f"Searching HubSpot contacts for fund ID: {fund_id}")
    try:
        contacts = hubspot.search_contacts_by_csuite_fund_id(fund_id)
        results = contacts.get('results', [])
        if results:
            contact_list = [
                f"{c.get('properties', {}).get('firstname', '')} "
                f"{c.get('properties', {}).get('lastname', '')} "
                f"({c.get('properties', {}).get('email', 'no email')})"
                for c in results
            ]
            parts.append(
                f"HubSpot Contacts associated with {fund_display} (ID: {fund_id}):\n"
                + "\n".join(contact_list)
            )
            logger.info(f"Found {len(contact_list)} contacts for fund {fund_id}")
        else:
            parts.append(
                f"No HubSpot contacts found linked to {fund_display} (ID: {fund_id}). "
                "Contacts are linked when a DAF is processed through Jidhr."
            )
    except Exception as e:
        logger.error(f"Error searching contacts by fund ID: {e}")

    return parts


# ---------------------------------------------------------------------------
# Giving Circle context (Lisa)
# ---------------------------------------------------------------------------

def _gather_giving_circle_context(query_lower: str, hubspot) -> list:
    """Fetch Giving Circle data from BOTH HubSpot lists. Returns raw data only — no analysis."""
    parts = []

    # --- List 126: AMCF Women's Giving Circle (Static, 130 members) ---
    try:
        members_126 = hubspot.get_giving_circle_member_details(limit=130)
        if members_126:
            lines = [
                f"**AMCF Women's Giving Circle** (List 126 — Static)",
                f"Members: {len(members_126)}",
                "",
            ]
            for c in members_126[:10]:
                props = c.get("properties", {})
                name = f"{props.get('firstname', '')} {props.get('lastname', '')}".strip()
                email = props.get("email", "no email")
                lines.append(f"  - {name} ({email})")
            if len(members_126) > 10:
                lines.append(f"  ...and {len(members_126) - 10} more")
            parts.append("\n".join(lines))
            logger.info(f"List 126: {len(members_126)} GC members")
        else:
            parts.append("List 126 (AMCF Women's Giving Circle): No members found.")
    except Exception as e:
        logger.exception(f"Error fetching List 126 (GC members): {e}")

    # --- List 31: Giving Circle Email List (Active, ~450 contacts) ---
    try:
        memberships_31 = hubspot._get(
            f"crm/v3/lists/{Config.GIVING_CIRCLE_EMAIL_LIST_ID}/memberships",
            {"limit": 250}
        )
        count_31 = len(memberships_31.get("results", [])) if memberships_31 else 0

        if count_31 > 0:
            # Fetch first 10 contact details for display
            record_ids = [str(m.get("recordId")) for m in memberships_31.get("results", [])[:10]]
            contacts_31 = []
            if record_ids:
                batch_result = hubspot._post("crm/v3/objects/contacts/batch/read", {
                    "inputs": [{"id": rid} for rid in record_ids],
                    "properties": ["firstname", "lastname", "email"]
                })
                if batch_result and "results" in batch_result:
                    contacts_31 = batch_result["results"]

            lines = [
                f"\n**Giving Circle Email List** (List 31 — Active)",
                f"Contacts: {count_31}+",
                f"Filter: GC Email form submission OR constituent code contains 'American Muslim Women's Giving Circle'",
                "",
            ]
            for c in contacts_31[:10]:
                props = c.get("properties", {})
                name = f"{props.get('firstname', '')} {props.get('lastname', '')}".strip()
                email = props.get("email", "no email")
                lines.append(f"  - {name} ({email})")
            if count_31 > 10:
                lines.append(f"  ...and {count_31 - 10} more")
            parts.append("\n".join(lines))
            logger.info(f"List 31: {count_31} GC email contacts")
        else:
            parts.append("\nList 31 (Giving Circle Email List): No contacts found.")
    except Exception as e:
        logger.exception(f"Error fetching List 31 (GC email list): {e}")

    return parts