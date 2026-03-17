"""
Jidhr DAF / Endowment Inquiry Workflow
=======================================
Multi-step conversational workflow that processes new DAF or endowment
inquiries from HubSpot form submissions into CSuite profiles and funds.

NEW in v1.3 — Survey priority: Kods (exact workflow), Muhi, Shazeen, Ola

Workflow steps:
  1. SHOW   — Pull latest unprocessed form submission, display for review
  2. CREATE — After confirmation, create CSuite profile + fund
  3. LINK   — Update HubSpot contact with CSuite IDs
  4. CLOSE  — Close associated ticket if any
  5. DONE   — Display confirmation with deep links
"""

import logging
from config import Config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Trigger keywords
# ---------------------------------------------------------------------------

TRIGGER_PHRASES = [
    'new daf', 'process daf', 'create daf', 'open a daf',
    'new endowment', 'process endowment', 'create endowment',
    'new fund inquiry', 'process inquiry', 'latest inquiry',
    'create profile from', 'create csuite profile',
    'daf inquiry form', 'endowment inquiry form',
    'process daf inquiry', 'process endowment inquiry',
]

# Phrases that should NOT trigger daf_workflow even if they contain trigger words
_EXCLUDE_PHRASES = [
    'summary', 'summarize', 'monthly', 'report', 'how many',
    'this month', 'last month', 'inquiries this', 'inquiry summary',
]


# ---------------------------------------------------------------------------
# Default workflow state (assistant.py holds this dict)
# ---------------------------------------------------------------------------

def default_workflow_state() -> dict:
    """Return a fresh workflow state. Called by assistant.__init__."""
    return {
        "active": False,
        "workflow_type": None,  # "daf" or "events"
        "type": None,        # "daf" or "endowment"
        "step": None,        # "confirm", "processing", "done"
        "submission_data": {},
        "profile_id": None,
        "funit_id": None,
        "ticket_id": None,
    }


def _reset_state(state: dict):
    """Reset workflow state to inactive."""
    state.update(default_workflow_state())


# ---------------------------------------------------------------------------
# Registry interface
# ---------------------------------------------------------------------------

def can_handle(query: str, workflow_state: dict = None, **kwargs) -> bool:
    """Match if trigger phrase detected OR DAF workflow is already active."""
    if workflow_state and workflow_state.get("active"):
        return workflow_state.get("workflow_type") == "daf"
    q = query.lower().strip()
    # Don't match summary/report queries that happen to contain "daf inquiry"
    if any(ex in q for ex in _EXCLUDE_PHRASES):
        return False
    return any(p in q for p in TRIGGER_PHRASES)


def handle(query: str, assistant) -> str:
    """
    Route to the appropriate workflow step.

    If workflow is not active, initiate it (show latest submission).
    If workflow is active, handle the current conversational step.
    """
    state = assistant.workflow_state
    hubspot = assistant.hubspot
    csuite = assistant.csuite

    # --- Active workflow: handle conversation ---
    if state.get("active"):
        return _handle_active_workflow(query, state, hubspot, csuite)

    # --- New workflow initiation ---
    return _initiate_workflow(query, state, hubspot)


# ---------------------------------------------------------------------------
# Initiation: pull latest submission and present for review
# ---------------------------------------------------------------------------

def _initiate_workflow(query: str, state: dict, hubspot) -> str:
    """Fetch the latest form submission and ask for confirmation."""
    q = query.lower()

    # Determine type
    if any(w in q for w in ['endowment']):
        wf_type = "endowment"
    else:
        wf_type = "daf"

    logger.info(f"Initiating {wf_type} inquiry workflow...")

    # Fetch submissions
    try:
        if wf_type == "daf":
            response = hubspot.get_daf_inquiry_submissions(limit=5)
        else:
            response = hubspot.get_endowment_inquiry_submissions(limit=5)
    except Exception as e:
        logger.error(f"Error fetching {wf_type} submissions: {e}")
        return f"❌ Failed to fetch {wf_type} inquiry submissions: {e}"

    if "error" in response:
        return f"❌ Failed to fetch {wf_type} inquiry submissions: {response['error']}"

    submissions = response.get("results", [])
    if not submissions:
        return f"📭 No pending {wf_type.upper()} inquiry submissions found."

    # Take the most recent submission
    sub = submissions[0]
    parsed = _parse_submission(sub)

    if not parsed.get("email"):
        return (
            f"⚠️ Latest {wf_type.upper()} submission is missing an email address. "
            "Cannot create a CSuite profile without one. Check HubSpot forms for details."
        )

    # Store in workflow state
    state.update({
        "active": True,
        "workflow_type": "daf",
        "type": wf_type,
        "step": "confirm",
        "submission_data": parsed,
        "profile_id": None,
        "funit_id": None,
        "ticket_id": None,
    })

    type_label = "DAF" if wf_type == "daf" else "Endowment"
    fund_name = parsed.get("fund_name", "Not specified")
    contribution = parsed.get("initial_contribution", "Not specified")

    return f"""📋 **New {type_label} Inquiry**

👤 **Name:** {parsed.get('first_name', '')} {parsed.get('last_name', '')}
📧 **Email:** {parsed.get('email', 'N/A')}
📱 **Phone:** {parsed.get('phone', 'N/A')}
💰 **Requested Fund Name:** {fund_name}
💵 **Initial Contribution:** {contribution}
📅 **Submitted:** {parsed.get('submitted_at', 'Unknown')}

---
**Shall I create the CSuite profile and fund?**
• Say *"Yes"* or *"Create it"* to proceed
• Say *"Skip"* or *"Cancel"* to abort"""


# ---------------------------------------------------------------------------
# Active workflow conversation handler
# ---------------------------------------------------------------------------

def _handle_active_workflow(query: str, state: dict, hubspot, csuite) -> str:
    """Route based on current workflow step."""
    q = query.lower().strip()
    step = state.get("step")

    # Cancel at any point
    if any(w in q for w in ['cancel', 'abort', 'stop', 'nevermind', 'forget it']):
        _reset_state(state)
        return "👍 Workflow cancelled."

    # Skip (move to next submission — for now just cancels)
    if any(w in q for w in ['skip', 'next']):
        _reset_state(state)
        return "⏭️ Skipped. Say *\"process daf inquiry\"* again to check for more submissions."

    if step == "confirm":
        return _step_create(q, state, hubspot, csuite)

    if step == "done":
        _reset_state(state)
        return "✅ Workflow complete. Let me know if you need anything else!"

    # Shouldn't reach here, but safety net
    _reset_state(state)
    return "⚠️ Workflow state was unclear — reset. Try starting again."


# ---------------------------------------------------------------------------
# Step: Create profile + fund + link + close ticket
# ---------------------------------------------------------------------------

def _step_create(query: str, state: dict, hubspot, csuite) -> str:
    """
    After user confirms, execute the full creation pipeline:
      1. Create CSuite profile
      2. Create CSuite fund
      3. Update HubSpot contact with CSuite IDs
      4. Close associated ticket (if found)
    """
    # Only proceed on affirmative
    affirmatives = ['yes', 'y', 'create', 'do it', 'go ahead', 'proceed', 'confirm', 'create it']
    if not any(w in query for w in affirmatives):
        return (
            "❓ I need a clear confirmation. Say *\"Yes\"* to create the profile and fund, "
            "or *\"Cancel\"* to abort."
        )

    state["step"] = "processing"
    data = state["submission_data"]
    wf_type = state["type"]
    type_label = "DAF" if wf_type == "daf" else "Endowment"

    results = {
        "profile_created": False,
        "fund_created": False,
        "hubspot_updated": False,
        "hubspot_created": False,
        "ticket_closed": False,
        "errors": [],
    }

    # --- 1. Create CSuite profile ---
    logger.info(f"Creating CSuite profile for {data.get('first_name')} {data.get('last_name')}...")
    try:
        profile_result = csuite.create_individual_profile(
            first_name=data.get("first_name", ""),
            last_name=data.get("last_name", ""),
            email=data.get("email", ""),
            phone=data.get("phone"),
        )
        if profile_result.get('success') and profile_result.get('data'):
            profile_id = profile_result['data'].get('profile_id')
            state["profile_id"] = profile_id
            results["profile_created"] = True
            logger.info(f"Profile created: {profile_id}")
        else:
            error = profile_result.get('error', 'Unknown error')
            results["errors"].append(f"Profile creation: {error}")
            logger.error(f"Profile creation failed: {error}")
    except Exception as e:
        results["errors"].append(f"Profile creation: {e}")
        logger.error(f"Profile creation error: {e}")

    # --- 2. Create CSuite fund ---
    if results["profile_created"]:
        fund_name = data.get("fund_name") or f"{data.get('last_name', 'New')} Family Fund"
        fgroup_id = Config.FUND_GROUP_DAF if wf_type == "daf" else Config.FUND_GROUP_ENDOWMENT

        logger.info(f"Creating CSuite fund: {fund_name}...")
        try:
            fund_result = csuite.create_fund(
                name=fund_name,
                fgroup_id=fgroup_id,
                cash_account_id=Config.DEFAULT_CASH_ACCOUNT_ID,
            )
            if fund_result.get('success') and fund_result.get('data'):
                funit_id = fund_result['data'].get('funit_id')
                state["funit_id"] = funit_id
                results["fund_created"] = True
                logger.info(f"Fund created: {funit_id}")
            else:
                error = fund_result.get('error', 'Unknown error')
                results["errors"].append(f"Fund creation: {error}")
                logger.error(f"Fund creation failed: {error}")
        except Exception as e:
            results["errors"].append(f"Fund creation: {e}")
            logger.error(f"Fund creation error: {e}")

    # --- 3. Update (or create) HubSpot contact with CSuite IDs ---
    if results["profile_created"] and data.get("email"):
        logger.info(f"Updating HubSpot contact for {data['email']}...")
        try:
            update_props = {
                "csuite_profile_id": str(state["profile_id"]),
            }
            if state.get("funit_id"):
                update_props["csuite_fund_id"] = str(state["funit_id"])

            hs_result = hubspot.update_contact_by_email(data["email"], update_props)
            if hs_result and "error" not in hs_result:
                results["hubspot_updated"] = True
                state["hubspot_contact_id"] = hs_result.get("id")
                logger.info("HubSpot contact updated")
            elif hs_result and "Contact not found" in hs_result.get("error", ""):
                # No existing contact — create one with CSuite IDs pre-populated
                logger.info(f"No HubSpot contact found — creating for {data['email']}...")
                create_props = {
                    "firstname": data.get("first_name", ""),
                    "lastname": data.get("last_name", ""),
                    "email": data["email"],
                    **update_props,
                }
                if data.get("phone"):
                    create_props["phone"] = data["phone"]
                create_result = hubspot.create_contact(create_props)
                if "id" in create_result:
                    results["hubspot_updated"] = True
                    results["hubspot_created"] = True
                    state["hubspot_contact_id"] = create_result["id"]
                    logger.info(f"Created HubSpot contact: {create_result['id']}")
                else:
                    error = create_result.get("error", "Unknown error")
                    results["errors"].append(f"HubSpot contact creation: {error}")
            else:
                error = hs_result.get('error', 'Unknown error') if hs_result else 'No response'
                results["errors"].append(f"HubSpot update: {error}")
        except Exception as e:
            results["errors"].append(f"HubSpot update: {e}")
            logger.error(f"HubSpot update error: {e}")

    # --- 4. Close associated ticket (best effort) ---
    try:
        tickets = hubspot.get_open_tickets()
        if 'results' in tickets:
            # Try to find a ticket mentioning this person's name or email
            search_terms = [
                data.get("email", "").lower(),
                data.get("last_name", "").lower(),
                data.get("first_name", "").lower(),
            ]
            for t in tickets['results']:
                subj = t.get('properties', {}).get('subject', '').lower()
                content = t.get('properties', {}).get('content', '').lower()
                combined = f"{subj} {content}"
                if any(term and term in combined for term in search_terms):
                    ticket_id = t.get('id')
                    hubspot.close_ticket(ticket_id)
                    state["ticket_id"] = ticket_id
                    results["ticket_closed"] = True
                    logger.info(f"Closed ticket {ticket_id}")
                    break
    except Exception as e:
        logger.error(f"Ticket lookup/close error: {e}")
        # Non-fatal — don't add to errors

    # --- Build confirmation ---
    state["step"] = "done"
    return _format_confirmation(data, state, results, type_label)


# ---------------------------------------------------------------------------
# Submission parser
# ---------------------------------------------------------------------------

# Common HubSpot form field names (may vary — we try multiple variants)
_FIELD_MAP = {
    "firstname": "first_name",
    "first_name": "first_name",
    "first name": "first_name",
    "lastname": "last_name",
    "last_name": "last_name",
    "last name": "last_name",
    "email": "email",
    "phone": "phone",
    "mobilephone": "phone",
    "fund_name": "fund_name",
    "fund name": "fund_name",
    "requested_fund_name": "fund_name",
    "initial_contribution": "initial_contribution",
    "contribution_amount": "initial_contribution",
    "amount": "initial_contribution",
}


def _parse_submission(submission: dict) -> dict:
    """Parse a HubSpot form submission into a normalised dict."""
    parsed = {
        "first_name": "",
        "last_name": "",
        "email": "",
        "phone": "",
        "fund_name": "",
        "initial_contribution": "",
        "submitted_at": submission.get("submittedAt", "Unknown"),
    }

    values = submission.get("values", [])
    for v in values:
        field = v.get("name", "").lower().strip()
        value = v.get("value", "").strip()
        mapped = _FIELD_MAP.get(field)
        if mapped and value:
            parsed[mapped] = value

    return parsed


# ---------------------------------------------------------------------------
# Confirmation formatter
# ---------------------------------------------------------------------------

def _format_confirmation(data: dict, state: dict, results: dict, type_label: str) -> str:
    """Format the workflow completion confirmation."""
    name = f"{data.get('first_name', '')} {data.get('last_name', '')}".strip()
    lines = []

    if results["errors"]:
        lines.append(f"⚠️ **{type_label} Created (with warnings)**")
    else:
        lines.append(f"✅ **{type_label} Created!**")

    lines.append("")

    # Profile
    if results["profile_created"]:
        profile_link = Config.CSUITE_PROFILE_URL.format(profile_id=state['profile_id'])
        lines.append(f"👤 Profile: {name} — [CSuite]({profile_link})")
    else:
        lines.append(f"❌ Profile: Failed to create")

    # Fund
    if results["fund_created"]:
        fund_name = data.get("fund_name") or f"{data.get('last_name', 'New')} Family Fund"
        fund_link = Config.CSUITE_FUND_URL.format(funit_id=state['funit_id'])
        lines.append(f"💰 Fund: {fund_name} — [CSuite]({fund_link})")
    elif results["profile_created"]:
        lines.append("❌ Fund: Failed to create")

    # HubSpot
    hs_contact_id = state.get("hubspot_contact_id")
    if results["hubspot_created"] and hs_contact_id:
        hs_link = Config.HUBSPOT_CONTACT_URL.format(contact_id=hs_contact_id)
        lines.append(f"🎯 HubSpot contact created — [View]({hs_link})")
    elif results["hubspot_updated"] and hs_contact_id:
        hs_link = Config.HUBSPOT_CONTACT_URL.format(contact_id=hs_contact_id)
        lines.append(f"🎯 HubSpot contact updated — [View]({hs_link})")
    elif results["hubspot_created"] or results["hubspot_updated"]:
        lines.append(f"🎯 HubSpot contact synced with CSuite IDs")
    elif results["profile_created"]:
        lines.append("⚠️ HubSpot contact: Could not update or create")

    # Ticket
    if results["ticket_closed"]:
        ticket_link = Config.HUBSPOT_TICKET_URL.format(ticket_id=state['ticket_id'])
        lines.append(f"📋 Ticket closed — [View]({ticket_link})")

    # Errors
    if results["errors"]:
        lines.append("")
        lines.append("**Issues:**")
        for err in results["errors"]:
            lines.append(f"• ⚠️ {err}")

    lines.append("")
    lines.append("Say anything to continue, or start a new workflow.")

    return "\n".join(lines)