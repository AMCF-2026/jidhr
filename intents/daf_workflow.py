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
from clients.audit import AuditUnavailable, record_write
from clients.csuite import mark_id, ui_url
from clients.hubspot import hubspot_writes_allowed
from config import Config

logger = logging.getLogger(__name__)

from intents import anchors


# ---------------------------------------------------------------------------
# Access control
# ---------------------------------------------------------------------------

# Nothing is donor-facing yet; every handler is staff-and-above.
ALLOWED_ROLES = frozenset({"admin", "staff"})

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

# An intake request is a COMMAND, not a document. "process the latest
# endowment inquiry" is six words; a newsletter brief pasted into chat is
# hundreds. On 2026-09-23 a brief containing "new endowment" in its prose
# opened this workflow twice, and the intake then scraped a name, an email
# and a phone number out of the newsletter and offered to create a CSuite
# profile and fund from them.
#
# A length ceiling is a blunt rule and it is the right one here: the cost
# of refusing a very long intake command is that someone retypes it
# shortly; the cost of accepting a very long anything-else is a workflow
# that proposes writing to CSuite on the strength of a word it read in a
# paragraph.
MAX_COMMAND_WORDS = 25

# Answers that mean "no". Matched as whole words, not substrings: "no"
# sits inside "nominate" and "know", and the previous affirmative list
# matched the bare letter "y" as a substring, so "not yet", "definitely
# not" and "absolutely not" all read as confirmation to create a profile
# and a fund.
#
# "not" is here as a bare token, which means "create it, why not" also
# cancels. That is the right direction to be wrong in: this step writes a
# profile and a fund to CSuite, so an unnecessary cancel costs one
# retyped word and an unnecessary create costs a record someone has to
# find and unpick.
NEGATIVES = frozenset({
    "no", "n", "not", "nope", "nah", "negative", "dont", "don't",
    "cancel", "abort", "stop", "nevermind", "never", "quit", "exit",
})

AFFIRMATIVES = frozenset({
    "yes", "y", "yeah", "yep", "yup", "ok", "okay", "sure", "create",
    "confirm", "proceed", "go", "do", "it",
})

# Multi-word confirmations, matched as phrases.
AFFIRMATIVE_PHRASES = ("do it", "go ahead", "create it", "sounds good",
                       "looks good", "yes please")


def _words(text: str) -> list:
    """Lowercase word tokens, punctuation stripped."""
    import re
    return re.findall(r"[a-z']+", (text or "").lower())


def says_no(query: str) -> bool:
    """True if this message is a refusal.

    Checked BEFORE the affirmative, so "no, don't create it" cancels
    rather than confirming on the word "create".
    """
    tokens = _words(query)
    if not tokens:
        return False
    return bool(NEGATIVES.intersection(tokens))


def says_yes(query: str) -> bool:
    """True if this message is a clear confirmation, and not a refusal."""
    if says_no(query):
        return False
    lowered = (query or "").lower()
    if any(phrase in lowered for phrase in AFFIRMATIVE_PHRASES):
        return True
    tokens = set(_words(query))
    # "do" and "it" alone mean nothing; they only count as "do it",
    # which the phrase list above already catches.
    return bool(tokens.intersection(AFFIRMATIVES - {"do", "it"}))


def _is_prose(query: str) -> bool:
    """True if this is a document someone pasted, not a command."""
    return len(_words(query)) > MAX_COMMAND_WORDS


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
    """Match on an explicit trigger phrase, or an already-active workflow.

    Never on a keyword buried in prose. This workflow proposes writing to
    CSuite — a profile and a fund — so it has to be asked for, not
    inferred from a word inside a paragraph someone pasted.
    """
    if workflow_state and workflow_state.get("active"):
        return workflow_state.get("workflow_type") == "daf"

    q = query.lower().strip()

    # An explicit content request always wins over a keyword intake. The
    # handler chain already puts content first, but stating it here means
    # the precedence survives someone reordering the chain.
    if anchors.yields_to_content(query):
        return False

    # Don't match summary/report queries that happen to contain "daf inquiry"
    if any(ex in q for ex in _EXCLUDE_PHRASES):
        return False

    if _is_prose(q):
        return False

    # Anchored as well as length-capped: the Step 11 ceiling stopped a
    # 5,000-character brief, and a 300-character one would still have
    # slipped through on a word in its last sentence.
    return anchors.anchored(q, TRIGGER_PHRASES)


def handle(query: str, ctx) -> str:
    """
    Route to the appropriate workflow step.

    If workflow is not active, initiate it (show latest submission).
    If workflow is active, handle the current conversational step.
    """
    state = ctx.workflow_state
    hubspot = ctx.services.hubspot
    csuite = ctx.services.csuite

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
    form_id = (Config.DAF_INQUIRY_FORM_ID if wf_type == "daf"
               else Config.ENDOWMENT_INQUIRY_FORM_ID)
    state.update({
        "active": True,
        "workflow_type": "daf",
        "type": wf_type,
        "step": "confirm",
        "form_id": form_id,
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

    # Cancel at any point. "No" means no: before this, it matched neither
    # the cancel list nor the affirmative list, so it fell through to
    # "I need a clear confirmation" and left the workflow open.
    if says_no(q) or "forget it" in q:
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

def _hubspot_contact_id(email: str, hubspot) -> str:
    """The contact's HubSpot id, or None. READ-ONLY, and never logs the email.

    crm/v3/objects/contacts/search is a POST that carries a query, which
    is_hubspot_write() classifies as a read — so this changes nothing.
    """
    if not email or hubspot is None:
        return None
    try:
        found = hubspot.search_contact_by_email(email)
    except Exception as e:
        logger.warning("could not resolve a HubSpot contact id: %s", e)
        return None
    if not isinstance(found, dict) or "error" in found:
        return None
    rows = found.get("results")
    if isinstance(rows, list) and rows:
        return rows[0].get("id")
    return None


def _log_skipped_create(data: dict, hubspot) -> str:
    """Record the skip against a HubSpot id. Returns the id, or None.

    The id, not the name or the email: this line goes to a log that is not
    the place for a donor's contact details. When the contact cannot be
    resolved the log says so, rather than going quiet — a skip nobody can
    trace back to a person is a skip nobody will action.
    """
    contact_id = _hubspot_contact_id(data.get("email"), hubspot)
    if contact_id:
        logger.warning(
            "CSuite profile create SKIPPED for HubSpot contact %s: "
            "CSUITE_DAF_CREATE_ENABLED is off. create_individual_profile "
            "sends input names CSuite does not recognise and drops the "
            "values without erroring. No profile was created.", contact_id)
    else:
        logger.warning(
            "CSuite profile create SKIPPED: CSUITE_DAF_CREATE_ENABLED is "
            "off, and no HubSpot contact id could be resolved for this "
            "submission. No profile was created.")
    return contact_id


# Words that identify what an open ticket is ABOUT.
#
# Needed because the ticket search filters on `hs_pipeline_stage == "1"` and
# NOTHING else — not pipeline, not ticket type, not the source form. Asset
# Transfer and DAF Inquiry share the DAF pipeline, so before 2026-10-02 a DAF
# inquiry could close an Asset Transfer ticket for the same donor; and because
# there is no pipeline filter either, it could close a stage-"1" ticket from an
# unrelated pipeline entirely.
#
# Matched against the ticket's own text, because that is what the search
# returns. A ticket PROPERTY identifying the type would be better and its name
# is unknown — see the report.
_INQUIRY_WORDS = {
    "daf": ("daf", "donor advised", "donor-advised"),
    "endowment": ("endowment", "endowed"),
}

# Other things the DAF pipeline carries. A ticket naming one of these is NOT an
# inquiry ticket, whatever else its text happens to say.
_OTHER_INQUIRY_WORDS = {
    "asset transfer": ("asset transfer", "asset donation", "stock transfer",
                       "in-kind", "in kind"),
    "investment request": ("investment request", "investment change",
                           "reallocat"),
}


def ticket_subject_kind(text: str):
    """What an open ticket appears to be about: a key, "other", or None.

    None means it cannot be told, and the caller treats that as a reason not to
    close it rather than a reason to try.
    """
    low = (text or "").lower()
    for label, words in _OTHER_INQUIRY_WORDS.items():
        if any(w in low for w in words):
            return "other"
    hits = [k for k, words in _INQUIRY_WORDS.items()
            if any(w in low for w in words)]
    if len(hits) == 1:
        return hits[0]
    return None             # none, or more than one — not determinable


# How many associated tickets to consider before giving up and saying so.
MAX_TICKETS_CONSIDERED = 200


def open_inquiry_tickets(hubspot, contact_id, wf_type):
    """(candidates, note) — this donor's open tickets for THIS inquiry type.

    Association-scoped, then filtered by pipeline and stage. Replaces "every
    open ticket in the portal, then look for the donor's email in the text",
    which could not work — all three measured against production 2026-10-02:

    * **177 open DAF-pipeline tickets exist and the old call fetched 10.** The
      right ticket was usually not among them, so "No matching ticket" was often
      false.
    * **Only 43 of 100 sampled tickets have any content at all**, and only those
      contain an email. 57 were titled "New ticket created from form submission"
      and 32 "DAF Form Submission -", with no donor name.
    * **Endowment tickets were unreachable.** They live in pipeline 1395576547;
      the filter asked for stage "1", which only the DAF Pipeline has.

    `clients/hubspot.get_contact_tickets` already does the association lookup —
    it was added after donor_prep showed five strangers' tickets as one donor's.
    This workflow had the same bug by a different route.
    """
    if not contact_id:
        return [], "no HubSpot contact, so no ticket could be identified"

    spec = Config.TICKET_PIPELINES.get(wf_type)
    if not spec:
        return [], f"no ticket pipeline is configured for {wf_type!r}"

    tickets = hubspot.get_contact_tickets(
        contact_id,
        properties=["subject", "content", "hs_pipeline", "hs_pipeline_stage"])
    if not tickets:
        return [], None

    note = None
    if len(tickets) > MAX_TICKETS_CONSIDERED:
        note = (f"this contact has {len(tickets)} tickets; only the first "
                f"{MAX_TICKETS_CONSIDERED} were considered")
        logger.warning("contact %s has %d tickets, considering %d",
                       contact_id, len(tickets), MAX_TICKETS_CONSIDERED)
        tickets = tickets[:MAX_TICKETS_CONSIDERED]

    candidates = []
    for ticket in tickets:
        props = ticket.get("properties") or {}
        if str(props.get("hs_pipeline")) != spec["pipeline"]:
            continue
        if str(props.get("hs_pipeline_stage")) != spec["new_stage"]:
            continue

        # Second guard, not the primary one. Association, pipeline and stage
        # already say this is an open inquiry ticket for this donor. The text is
        # only what tells an Asset Transfer Notification apart from a DAF Form
        # Submission — because NO ticket property distinguishes them, confirmed
        # 2026-10-02 by reading every ticket property definition in the portal.
        # So it EXCLUDES other request types and never requires a match.
        subject = props.get("subject") or ""
        blob = f"{subject} {props.get('content') or ''}"
        if ticket_subject_kind(blob) == "other":
            logger.info("ticket %s is in the right pipeline but reads as "
                        "another request type — not closing it",
                        ticket.get("id"))
            continue

        candidates.append({"id": ticket.get("id"),
                           "subject": subject or "(no subject)",
                           "pipeline": props.get("hs_pipeline")})
    return candidates, note


class _SkipTicketClose(Exception):
    """Internal: the ticket step is switched off."""


# matching_tickets() was removed on 2026-10-02. It searched every open ticket in
# the portal for the donor's email in the text, and production says that could
# not work: of 100 sampled open DAF-pipeline tickets, 57 were titled "New ticket
# created from form submission", 32 "DAF Form Submission -", and only 43 had any
# content at all. open_inquiry_tickets() uses the contact association instead,
# which is what actually identifies a donor's ticket.


def existing_hubspot_link(email: str, hubspot):
    """(contact_id, csuite_profile_id) for `email`. READ-ONLY.

    One search, both answers. Until 2026-10-01 the workflow created a profile
    and then PATCHed `csuite_profile_id` over whatever was there, having never
    read it — so a second inquiry from the same donor made a second CSuite
    profile and repointed HubSpot at it, with the old id gone and nothing
    recording what it had been. CSuite has no idempotency key, so the first
    profile stays.

    Returns (None, None) when the contact cannot be read. "Unknown" is not
    "absent": the caller treats a failed lookup as a reason to stop, not as
    permission to create.
    """
    if not email or hubspot is None:
        return None, None
    try:
        found = hubspot.search_contact_by_email(email)
    except Exception as e:
        logger.error("could not read the HubSpot contact for a duplicate "
                     "check: %s", e)
        return None, None
    if not isinstance(found, dict) or "error" in found:
        return None, None

    rows = found.get("results")
    if not (isinstance(rows, list) and rows):
        return None, None          # no contact yet; nothing to duplicate
    row = rows[0]
    props = row.get("properties") or {}
    existing = (props.get("csuite_profile_id") or "").strip() or None
    return row.get("id"), existing


class _SkipHubSpotUpdate(Exception):
    """Internal: leave the HubSpot contact alone and keep going."""


class DuplicateProfile(Exception):
    """Nothing should be created. `kind` says why, so the reply can differ.

    `profile_id` is the profile to treat as the donor's — the one a follow-up
    task links to — or None when there isn't one to trust.
    """

    def __init__(self, profile_id, source: str, kind: str = "duplicate"):
        self.profile_id = profile_id
        self.source = source
        self.kind = kind
        super().__init__(f"profile {profile_id} already exists ({source})")


# A stored id is not evidence that a profile exists.
PROFILE_EXISTS = "exists"
PROFILE_MISSING = "missing"
PROFILE_UNREADABLE = "unreadable"


def csuite_profile_state(csuite, profile_id):
    """(state, record) for a profile id. One READ; no write budget is touched.

    Three outcomes, because two of them lead to opposite decisions:

    * PROFILE_EXISTS — read back, with a profile_id in it.
    * PROFILE_MISSING — a clean "not found". The id is stale.
    * PROFILE_UNREADABLE — anything else. **Not** the same as missing: a
      transport fault or a 500 says nothing about whether the profile is there,
      and treating it as missing would invite creating a duplicate.
    """
    try:
        response = csuite._request("profile/display", {"profile_id": profile_id})
    except Exception as e:
        logger.error("could not read CSuite profile %s: %s", profile_id, e)
        return PROFILE_UNREADABLE, None

    if not isinstance(response, dict):
        return PROFILE_UNREADABLE, None

    record = response.get("data")
    if isinstance(record, list) and record:
        record = record[0]
    if isinstance(record, dict) and record.get("profile_id"):
        return PROFILE_EXISTS, record

    # CSuite answers a missing profile with success=0 and "Profile not found".
    # Anything else — a 5xx, a network fault, an unparseable body — is not a
    # statement that the profile is absent.
    error = str(response.get("error") or "")
    errors = " ".join(str(e) for e in (response.get("errors") or []))
    if "not found" in (error + " " + errors).lower():
        return PROFILE_MISSING, None
    return PROFILE_UNREADABLE, None


def already_in_csuite(data: dict, hubspot, csuite):
    """The existing CSuite profile id for this donor, or None. READ-ONLY.

    Two independent checks, because they fail differently:

    1. **The HubSpot contact's `csuite_profile_id`.** Cheap, and definitive
       when set — this workflow put it there.
    2. **A trusted `primary_email` search of CSuite.** Catches a profile
       created by hand, by an import, or by a run whose HubSpot PATCH failed.
       It goes through `filter_trust`, so a filter CSuite has decided to
       ignore raises instead of answering 0 — the 18,797-row lesson.

    Raises DuplicateProfile on a match, and also when the CSuite side cannot
    be trusted or is ambiguous. **Ambiguity stops the create.** The cost of
    stopping is a message; the cost of continuing is a duplicate donor record
    in a fund-accounting system, which nothing here can undo.
    """
    from sync.filter_trust import FilterNotTrusted, search_before_create

    contact_id, existing = existing_hubspot_link(data.get("email"), hubspot)
    email = (data.get("email") or "").strip().lower()

    def email_search():
        """(count, ids). Raises DuplicateProfile when it cannot be trusted."""
        if not email:
            return 0, []
        try:
            count, response = search_before_create(
                lambda endpoint, body: csuite._request(endpoint, body),
                "profile/list", "primary_email", email)
        except FilterNotTrusted as e:
            raise DuplicateProfile(
                None, f"CSuite could not be searched, so a duplicate cannot be "
                      f"ruled out: {e}", kind="unverifiable")
        except Exception as e:
            raise DuplicateProfile(
                None, f"the CSuite duplicate check failed, so a duplicate "
                      f"cannot be ruled out: {e}", kind="unverifiable")
        rows = (response.get("data") or {}).get("results") or []
        return count, [r.get("profile_id") for r in rows if r.get("profile_id")]

    # In a SANDBOX run the stored id is advisory and nothing more.
    #
    # HubSpot is production. Its csuite_profile_id values are PRODUCTION profile
    # ids, and checking one against sandbox CSuite compares two unrelated
    # numbering spaces: production profile 8443 is a real donor and sandbox 8443
    # is something else or nothing. Every such check would report "stale" and be
    # wrong about it.
    #
    # So only the CSuite primary_email search decides, against the environment
    # actually being used. The id is logged, because knowing it was there is
    # useful; it is not acted on.
    if existing and not hubspot_writes_allowed():
        logger.info("sandbox run: HubSpot's csuite_profile_id %s is a "
                    "PRODUCTION id and is not checked against sandbox CSuite; "
                    "the primary_email search decides", existing)
        existing = None

    # A stored csuite_profile_id is a claim, not a fact. 69 production contacts
    # carry ids that do not resolve in CSuite (measured 2026-10-01), and
    # trusting one meant the donor silently never got a profile while staff were
    # told they already had one — and a follow-up task was linked to nothing.
    if existing:
        state, _record = csuite_profile_state(csuite, existing)

        if state == PROFILE_UNREADABLE:
            raise DuplicateProfile(
                None, f"CSuite would not say whether profile {existing} exists, "
                      "so nothing was created or changed", kind="unverifiable")

        if state == PROFILE_EXISTS:
            count, ids = email_search()
            if count == 1 and len(ids) == 1 and \
                    str(ids[0]).strip() != str(existing).strip():
                raise DuplicateProfile(
                    existing,
                    f"HubSpot points at {existing}, CSuite email match is "
                    f"{mark_id(ids[0])} — two profiles, merge by hand.",
                    kind="conflict")
            # Same id, or no single email match to disagree with: as before.
            raise DuplicateProfile(
                existing, "HubSpot contact csuite_profile_id", kind="duplicate")

        # PROFILE_MISSING — the stored id is stale.
        count, ids = email_search()
        if count == 1 and len(ids) == 1:
            raise DuplicateProfile(
                ids[0],
                f"HubSpot's csuite_profile_id is stale; CSuite match is "
                f"{mark_id(ids[0])} — fix HubSpot by hand.",
                kind="stale_with_match")
        raise DuplicateProfile(
            None, f"HubSpot points at profile {existing}, which doesn't exist "
                  "in CSuite. No profile created — needs a human.",
            kind="stale_no_match")

    if not email:
        return contact_id          # nothing to search on; the create decides

    count, ids = email_search()
    if count == 0:
        return contact_id
    if count == 1 and len(ids) == 1:
        raise DuplicateProfile(ids[0], "CSuite primary_email search",
                               kind="duplicate")
    raise DuplicateProfile(
        None, f"{count} CSuite profiles already carry this email "
              f"({', '.join(mark_id(i) for i in ids) or 'ids not returned'})",
        kind="ambiguous")


def task_due_date(submitted_at=None, business_days: int = 2) -> str:
    """`business_days` working days from `submitted_at`, as YYYY-MM-DD.

    Saturdays and Sundays are skipped. Public holidays are NOT — this repo has
    no holiday calendar, and inventing one would be a guess that silently moves
    due dates. A task due on a holiday is late by a day; a task due on a
    Saturday is late by two and looks like a mistake.

    `submitted_at` may be a HubSpot epoch-milliseconds value, an ISO string, a
    date, or None for today.
    """
    from datetime import date, datetime, timedelta, timezone

    start = None
    if isinstance(submitted_at, datetime):
        start = submitted_at.date()
    elif isinstance(submitted_at, date):
        start = submitted_at
    elif isinstance(submitted_at, (int, float)):
        # HubSpot sends submittedAt in milliseconds.
        start = datetime.fromtimestamp(
                float(submitted_at) / 1000, timezone.utc).date()
    elif isinstance(submitted_at, str) and submitted_at.strip():
        text = submitted_at.strip()
        if text.isdigit():
            start = datetime.fromtimestamp(
                int(text) / 1000, timezone.utc).date()
        else:
            try:
                start = datetime.fromisoformat(text.replace("Z", "+00:00")).date()
            except ValueError:
                start = None
    if start is None:
        start = date.today()

    due, added = start, 0
    while added < business_days:
        due += timedelta(days=1)
        if due.weekday() < 5:          # Mon-Fri
            added += 1
    return due.isoformat()


def form_label(form_id) -> str:
    """A human name for a form id. The id tells a reader nothing."""
    return Config.FORM_LABELS.get(form_id) or (
        f"form {form_id}" if form_id else "an unknown form")


def task_assignee_for_form(form_id):
    """The employee a form's follow-up task goes to, or None.

    **Per form, and never a fallback to a default person.** A DAF inquiry and an
    endowment inquiry are different people's work, and a shared value sends one
    of them to the other silently. A fallback would be worse still: a form
    nobody has assigned would land in whoever's queue happens to be configured,
    and a task in the wrong queue looks exactly like a task in the right one.
    `Config.CSUITE_TASK_EMPLOYEE_ID` is deliberately NOT consulted.

    None means no task, and the caller names the form.
    """
    return {
        Config.DAF_INQUIRY_FORM_ID:
            Config.CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY,
        Config.ENDOWMENT_INQUIRY_FORM_ID:
            Config.CSUITE_TASK_EMPLOYEE_ID_ENDOWMENT_INQUIRY,
        # Asset Transfer and Investment Request are deliberately absent: this
        # workflow does not handle them, so no task is created for them.
    }.get(form_id)


def _create_followup_task(data, state, results, csuite, wf_type, type_label):
    """Create the CSuite follow-up task, or record why it was not created.

    Runs AFTER the profile step and never changes its result. A task is a
    reminder; a profile is a record. Nothing here may make a successful profile
    look like a failure.
    """
    profile_id = state.get("profile_id")
    label = "DAF" if wf_type == "daf" else "Endowment"

    if not Config.CSUITE_DAF_TASK_CREATE_ENABLED:
        results["task_skipped"] = "follow-up tasks are turned off"
        return
    if not profile_id:
        results["task_skipped"] = (
            "no CSuite profile to attach it to" if not results.get(
                "duplicate_reason")
            else f"the profile is ambiguous — {results['duplicate_reason']}")
        return

    form_id = state.get("form_id")
    assignee = task_assignee_for_form(form_id)
    if not assignee:
        results["task_skipped"] = (
            f"no assignee set for {form_label(form_id)}")
        return

    name = (data.get("first_name", "") + " " + data.get("last_name", "")).strip()
    email = (data.get("email") or "").strip()
    subject = (f"{label} inquiry follow-up: {name or 'unnamed'}"
               + (f" — {email}" if email else ""))
    due = task_due_date(data.get("submitted_at"))

    try:
        task_result = csuite.create_task(
            name=subject,
            employee_id=assignee,
            due_date=due,
            description=subject,
            linked_profile_id=profile_id,
            task_type_id=Config.CSUITE_TASK_TYPE_ID,
        )
    except Exception as e:
        # Never rolls back and never hides the profile. A reminder that was not
        # made is a person to tell, not a record to undo.
        results["task_failed"] = str(e)
        logger.error("CSuite follow-up task NOT created for profile %s: %s",
                     profile_id, e)
        return

    if task_result.get("success") and isinstance(task_result.get("data"), dict):
        results["task_id"] = task_result["data"].get("task_id")
        results["task_due"] = due
        results["task_subject"] = subject
        results["task_warning"] = task_result.get("task_warning")
        # The name when the read-back supplied one, the id otherwise. A
        # confirmation that says "1007" makes the reader look it up.
        results["task_assignee"] = (task_result.get("assignee_name")
                                    or str(assignee))
        results["csuite_api_base"] = state.get("csuite_api_base")
        results["task_donor"] = name or email or "this donor"
        logger.info("CSuite follow-up task %s created on profile %s, due %s",
                    results["task_id"], profile_id, due)
    else:
        results["task_failed"] = (task_result.get("error")
                                  or "CSuite returned no task id")
        logger.error("CSuite follow-up task NOT created for profile %s: %s",
                     profile_id, results["task_failed"])


def _backfill_hubspot_link(data, state, results, hubspot, profile_id):
    """Fill in a HubSpot contact's empty csuite_profile_id. One PATCH, or none.

    Runs ONLY on the duplicate path, where the guard stopped on exactly one
    existing profile. The step-3 PATCH block is not reused and the early return
    is not removed: that block falls back to `create_contact` when the contact
    is missing, which would create a contact for a donor who already has a
    CSuite profile — a worse outcome than the gap it would close.

    Writes only when the contact EXISTS and its value is EMPTY. A value already
    there is never overwritten, even when it disagrees with the match: the
    stored id is what staff and the donation sync have been using, and two
    profiles for one donor is a merge decision, not a field update.
    """
    if not Config.CSUITE_HUBSPOT_BACKFILL_ENABLED:
        results["backfill"] = "skipped: HubSpot backfill is turned off"
        return
    if not profile_id:
        results["backfill"] = "skipped: no single profile to link to"
        return

    contact_id, existing = existing_hubspot_link(data.get("email"), hubspot)
    if not contact_id:
        results["backfill"] = (
            "no HubSpot contact for this address, so nothing was linked — "
            "create the contact, or link it by hand")
        return
    if existing:
        if str(existing).strip() != str(profile_id).strip():
            results["backfill_conflict"] = (existing, profile_id)
            results["backfill"] = (
                f"HubSpot points at {existing}, CSuite match is "
                f"{mark_id(profile_id)} "
                "— nothing was changed. Merge them in CSuite.")
        else:
            results["backfill"] = f"already linked to {existing}"
        return

    try:
        patched = hubspot.update_contact_by_email(
            data["email"], {"csuite_profile_id": str(profile_id)})
    except Exception as e:
        results["backfill"] = f"HubSpot link NOT written ({e})"
        logger.error("backfill PATCH failed for contact %s: %s", contact_id, e)
        return

    if patched and patched.get("refused") == "sandbox_run":
        results["backfill"] = "HubSpot not updated (sandbox run)"
        return
    if patched and "error" not in patched:
        results["backfill_wrote"] = str(profile_id)
        results["backfill_contact_id"] = contact_id
        results["backfill"] = (
            f"HubSpot now linked to profile {mark_id(profile_id)}")
        logger.info("backfilled csuite_profile_id=%s onto HubSpot contact %s",
                    profile_id, contact_id)
    else:
        reason = (patched or {}).get("error") or "HubSpot returned no response"
        results["backfill"] = f"HubSpot link NOT written ({reason})"
        logger.error("backfill PATCH failed for contact %s: %s", contact_id,
                     reason)


def record_unprocessed_submission(state: dict, reason: str) -> bool:
    """Record WHICH submission could not be processed, so it can be replayed.

    Why this exists
    ---------------
    2026-10-01: with the write budget spent, a second inquiry's
    `profile/create/individual` was refused inside `_request` — before
    `reserve_write`, so correctly leaving **no audit row**, since nothing was
    sent. But nothing else recorded it either. The submission existed only in
    the chat session's workflow state, and `_initiate_workflow` always reads
    `submissions[0]`, the most recent. One newer submission and the refused
    one is unreachable: not lost from HubSpot, but no longer findable by this
    workflow, with nothing anywhere saying it had been seen.

    What is stored, and what is NOT
    ------------------------------
    The HubSpot **form id** and the **submission id** (its `conversionId`, or
    its `submittedAt` when HubSpot sends no conversionId). Those two locate
    the submission in HubSpot, which already holds the donor's details.

    **No name, email, phone or address is stored.** The row goes in
    `write_audit`, an existing table — no new store, no migration — and
    `payload_meta` keeps key names and id values only.

    Returns True when the row landed. Never raises: this runs on a path that
    has already failed, and a bookkeeping failure must not replace the real
    error. It returns False instead, and the caller says so out loud.
    """
    submission_id = (state.get("submission_data") or {}).get("submission_id")
    form_id = state.get("form_id")
    if not (submission_id or form_id):
        logger.error("a submission could not be processed (%s) and carries no "
                     "identifier, so it cannot be replayed", reason)
        return False

    try:
        record_write(
            "csuite", "POST", "profile/create/individual",
            target_id=submission_id or None,
            payload={"hubspot_form_id": form_id,
                     "hubspot_submission_id": submission_id},
            status="skipped",
            error=f"inquiry not processed, replayable from HubSpot: {reason}",
            duration_ms=0)
    except AuditUnavailable as e:
        logger.error("could not record unprocessed submission %s on form %s: "
                     "%s", submission_id, form_id, e)
        return False

    logger.warning("inquiry NOT processed and recorded for replay: "
                   "form=%s submission=%s reason=%s",
                   form_id, submission_id, reason)
    return True


def _step_create(query: str, state: dict, hubspot, csuite) -> str:
    """
    After user confirms, execute the full creation pipeline:
      1. Create CSuite profile
      2. Create CSuite fund
      3. Update HubSpot contact with CSuite IDs
      4. Close associated ticket (if found)
    """
    # Only proceed on a clear affirmative, matched as whole words.
    if not says_yes(query):
        return (
            "❓ I need a clear confirmation. Say *\"Yes\"* to create the profile and fund, "
            "or *\"Cancel\"* to abort."
        )

    state["step"] = "processing"
    # The API base URL of the client doing the work, so every UI link in the
    # confirmation points at the host that actually holds the record. Without
    # it the links go to production, where a sandbox id is a DIFFERENT real
    # donor — measured 2026-10-01.
    state["csuite_api_base"] = getattr(csuite, "base_url", None)
    data = state["submission_data"]
    wf_type = state["type"]
    type_label = "DAF" if wf_type == "daf" else "Endowment"

    results = {
        "profile_created": False,
        "fund_created": False,
        "hubspot_updated": False,
        "hubspot_created": False,
        # Set when HubSpot was not written to because this is a sandbox run.
        # Not a failure: nothing is wrong and nothing needs retrying.
        "hubspot_refused": False,
        "ticket_closed": False,
        # Set when a ticket was found and the close was attempted but did not
        # succeed. Distinct from ticket_closed being False because no ticket
        # matched — that is not a failure, and says nothing to the user.
        "ticket_close_failed": None,
        # Every open ticket carrying the donor's email. One is closed; more
        # than one is listed for a human to pick; none is reported as none.
        "ticket_matches": [],
        # "off" when the flag is down — which is not the same as "none found".
        # A note when the contact has more tickets than were considered.
        "ticket_skipped": None,
        "ticket_note": None,
        # Distinct from profile_created being False after a failed attempt:
        # nothing was sent. Reported to the user as skipped, not as failed,
        # because "Failed to create" would be a false statement.
        "profile_skipped": False,
        # Set when a CSuite profile already exists for this donor, or when a
        # duplicate could not be ruled out. Either way nothing is created and
        # nothing is PATCHed.
        "duplicate_of": None,
        "duplicate_reason": None,
        "duplicate_kind": None,
        # Set when CSuite would have refused the submitted phone number, so
        # it was left out and the profile made anyway. Shown to the user, not
        # only logged: a number dropped in silence is the failure this path
        # has been carrying since 2026-03-17.
        "phone_warning": None,
        # Set when funit/create succeeded but the fund did not read back
        # holding what was sent.
        "fund_warning": None,
        # Set when no fund was attempted because an inquiry creates a profile
        # only. Distinct from fund_created being False after a real failure.
        "fund_deferred": False,
        # True when a failed run was recorded for replay, False when even that
        # could not be stored. None when the run succeeded.
        "replay_recorded": None,
        # The follow-up task. Exactly one of these is set: task_id on success,
        # task_failed when a create was attempted and did not work,
        # task_skipped when none was attempted and why.
        "task_id": None,
        "task_due": None,
        "task_subject": None,
        "task_failed": None,
        "task_skipped": None,
        "task_warning": None,
        "task_assignee": None,
        "task_donor": None,
        "csuite_api_base": None,
        # The duplicate-path HubSpot backfill. `backfill` always carries a
        # sentence; the other two are set only when a PATCH landed or when the
        # two sides disagree.
        "backfill": None,
        "backfill_wrote": None,
        "backfill_contact_id": None,
        "backfill_conflict": None,
        # Set when the submission carried an address. CSuite's address input
        # name is unknown — nine candidates eliminated by sandbox write and
        # read-back — so it is not sent, and it is named to a human instead
        # of vanishing the way it has since 2026-03-17.
        "address_warning": None,
        "errors": [],
    }

    # --- 1. Create CSuite profile ---
    if not Config.CSUITE_DAF_CREATE_ENABLED:
        # Off by default. create_individual_profile has sent primary_email
        # since 2026-03-17 and CSuite does not recognise that input name —
        # it returns 200 with a profile_id and drops the value. A profile
        # created here would be missing its email, and probably its phone
        # and address, with nothing in the response to say so.
        _log_skipped_create(data, hubspot)
        results["profile_skipped"] = True
    else:
        # Search before create. CSuite has no idempotency key, so a duplicate
        # cannot be undone from here — and an ambiguous or unanswerable check
        # stops the create just as a match does.
        try:
            existing_contact_id = already_in_csuite(data, hubspot, csuite)
            state["hubspot_contact_id"] = existing_contact_id
        except DuplicateProfile as duplicate:
            results["duplicate_of"] = duplicate.profile_id
            results["duplicate_reason"] = duplicate.source
            results["duplicate_kind"] = duplicate.kind
            logger.warning("CSuite profile create SKIPPED: %s", duplicate)
            state["step"] = "done"
            if duplicate.profile_id:
                state["profile_id"] = duplicate.profile_id
            # A returning donor still needs following up, and the existing
            # profile is the right thing to link to. Two matches leave
            # profile_id unset, so the task is skipped with that as the reason.
            _create_followup_task(data, state, results, csuite, wf_type,
                                  type_label)
            # The link never heals on its own: a profile found by the CSuite
            # search means HubSpot has none, and this path returns before the
            # step-3 PATCH.
            # Not on a stale or conflicting link. The stored id is wrong and
            # a human has to fix it; writing over it would destroy the only
            # record of what it pointed at. The line already says so.
            if duplicate.kind in ("stale_with_match", "stale_no_match",
                                  "conflict"):
                results["backfill"] = (
                    "no change to HubSpot — the stored id needs a human")
            else:
                _backfill_hubspot_link(data, state, results, hubspot,
                                       duplicate.profile_id)
            return _format_confirmation(data, state, results, type_label)

        logger.info(f"Creating CSuite profile for {data.get('first_name')} {data.get('last_name')}...")
        try:
            profile_result = csuite.create_individual_profile(
                first_name=data.get("first_name", ""),
                last_name=data.get("last_name", ""),
                email=data.get("email", ""),
                phone=data.get("phone"),
                # The four parts the inquiry forms collect, all required on
                # both forms. Sent as a nested `address` object, which is the
                # confirmed create shape (2026-10-01, sentinel 21660).
                # address2 is parsed but never sent: not in the confirmed set.
                address_line=data.get("address_street"),
                city=data.get("address_city"),
                state=data.get("address_state"),
                zipcode=data.get("address_zip"),
            )
            if profile_result.get('success') and profile_result.get('data'):
                profile_id = profile_result['data'].get('profile_id')
                state["profile_id"] = profile_id
                results["profile_created"] = True
                logger.info(f"Profile created: {profile_id}")
                results["phone_warning"] = profile_result.get("phone_warning")
                # A complete address is now SENT and stored, so there is
                # nothing to warn about. The warning only survives for an
                # INCOMPLETE one, which CSuite would turn into a malformed
                # primary_address_string — build_address refuses to send a
                # partial object and says which parts it had.
                results["address_warning"] = profile_result.get(
                    "address_warning")
                if results["address_warning"]:
                    logger.warning("CSuite profile %s created without the "
                                   "submitted address", profile_id)
                # verify_writes is on, so a dropped field is reported rather
                # than assumed stored. Surfaced to the user, not only logged.
                if profile_result.get("nothing_stored"):
                    # Stronger than a per-field drop: CSuite did not write to
                    # the record at all, and still answered success.
                    results["errors"].append(
                        f"CSuite reported success on {profile_id} but stored "
                        "NOTHING — the record was not modified. Treat this "
                        "profile as empty.")
                    logger.error("CSuite stored nothing on %s", profile_id)
                elif profile_result.get("fields_dropped"):
                    dropped = sorted(profile_result["fields_dropped"])
                    results["errors"].append(
                        f"Profile created as {profile_id} but CSuite did not "
                        f"store: {', '.join(dropped)}")
                    logger.error("CSuite kept %s but dropped %s", profile_id,
                                 dropped)
            else:
                error = profile_result.get('error', 'Unknown error')
                results["errors"].append(f"Profile creation: {error}")
                logger.error(f"Profile creation failed: {error}")
        except Exception as e:
            results["errors"].append(f"Profile creation: {e}")
            logger.error(f"Profile creation error: {e}")

        if not results["profile_created"]:
            # Nothing was created, so the submission still needs processing.
            # Record which one, so it is replayable after a newer submission
            # has pushed it out of reach of submissions[0].
            results["replay_recorded"] = record_unprocessed_submission(
                state, "; ".join(results["errors"]) or "profile create failed")

    # --- 2. Create CSuite fund — OFF by default ---
    #
    # Decision 2026-10-01: an inquiry creates a profile only. A fund is opened
    # when the donor commits. With the flag off nothing is built and nothing is
    # sent — not even a payload — so there is no fund call to fail and nothing
    # to report as failed.
    if results["profile_created"] and Config.CSUITE_DAF_FUND_CREATE_ENABLED:
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
                # funit/create is read back now. A fund whose group or cash
                # account did not store is a finance problem, so it is named
                # to the user rather than left in a log.
                results["fund_warning"] = fund_result.get("fund_warning")
            else:
                error = fund_result.get('error', 'Unknown error')
                results["errors"].append(f"Fund creation: {error}")
                logger.error(f"Fund creation failed: {error}")
        except Exception as e:
            results["errors"].append(f"Fund creation: {e}")
            logger.error(f"Fund creation error: {e}")
    elif results["profile_created"]:
        # Deliberate, not a failure. Recorded so the reply can say so.
        results["fund_deferred"] = True
        logger.info("fund creation deferred: an inquiry creates a profile "
                    "only (CSUITE_DAF_FUND_CREATE_ENABLED is off)")

    # --- 2b. CSuite follow-up task ---
    _create_followup_task(data, state, results, csuite, wf_type, type_label)

    # --- 3. Update (or create) HubSpot contact with CSuite IDs ---
    if results["profile_created"] and data.get("email"):
        logger.info(f"Updating HubSpot contact for {data['email']}...")
        try:
            update_props = {
                "csuite_profile_id": str(state["profile_id"]),
            }
            # csuite_fund_id is OMITTED when no fund was created, never sent
            # as null or "". HubSpot treats an explicit empty value as an
            # instruction to clear the property, so a null here would wipe a
            # fund id a later commitment-stage run had set.
            if state.get("funit_id"):
                update_props["csuite_fund_id"] = str(state["funit_id"])

            # Never overwrite an id that is already there. The duplicate guard
            # above should have stopped this run, so reaching here with a value
            # set means the two disagree — and the stored id wins, because it
            # is the one staff and the donation sync have been using.
            _, already = existing_hubspot_link(data["email"], hubspot)
            if already:
                logger.error("HubSpot contact for this submission already has "
                             "csuite_profile_id %s; refusing to overwrite it "
                             "with %s", already, state["profile_id"])
                results["errors"].append(
                    f"HubSpot already links this contact to CSuite profile "
                    f"{already}, so it was NOT repointed at the new profile "
                    f"{mark_id(state['profile_id'])}. Two profiles now exist "
                    "for this donor — merge them in CSuite.")
                raise _SkipHubSpotUpdate

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
            elif hs_result and hs_result.get("refused") == "sandbox_run":
                # Refused structurally, not failed. Nothing is wrong and
                # nothing needs retrying.
                results["hubspot_refused"] = True
                logger.info("HubSpot not updated: sandbox run")
            else:
                error = hs_result.get('error', 'Unknown error') if hs_result else 'No response'
                results["errors"].append(f"HubSpot update: {error}")
        except _SkipHubSpotUpdate:
            pass            # already explained in results["errors"]
        except Exception as e:
            results["errors"].append(f"HubSpot update: {e}")
            logger.error(f"HubSpot update error: {e}")

    # --- 4. Close the associated ticket, on the email and nothing else ---
    #
    # This used to match the donor's FIRST NAME or LAST NAME as a substring of
    # a ticket's subject or content, and close the first hit. So an inquiry
    # from any Sarah closed the oldest open ticket with "sarah" anywhere in
    # it — a different donor's ticket, a vendor thread, "Sarah to follow up".
    # A first name is not an identifier.
    #
    # An email address is. It is matched case-insensitively and in full, and
    # if more than one open ticket carries it the workflow closes NOTHING and
    # lists them, because "which of these two" is a judgement and closing the
    # wrong one is not reversible by this workflow.
    try:
        if not Config.CSUITE_TICKET_CLOSE_ENABLED:
            results["ticket_skipped"] = "off"
            raise _SkipTicketClose

        matches, note = open_inquiry_tickets(
            hubspot, state.get("hubspot_contact_id"), wf_type)
        results["ticket_matches"] = matches
        results["ticket_note"] = note

        if not matches:
            logger.info("no open ticket carries the submitted email; closing "
                        "nothing")
        elif len(matches) > 1:
            # Ambiguous on purpose. A human picks.
            logger.warning("%d open tickets carry the submitted email (%s); "
                           "closing none", len(matches),
                           ", ".join(str(m["id"]) for m in matches))
        else:
            ticket_id = matches[0]["id"]
            state["ticket_id"] = ticket_id
            state["ticket_subject"] = matches[0]["subject"]

            # The return value used to be discarded and ticket_closed
            # set to True regardless, so a failed close still printed
            # "📋 Ticket closed".
            close_result = hubspot.close_ticket(ticket_id)
            if close_result and not close_result.get("error"):
                results["ticket_closed"] = True
                logger.info(f"Closed ticket {ticket_id}")
            else:
                reason = (
                    (close_result or {}).get("error")
                    or "HubSpot returned no confirmation"
                )
                results["ticket_close_failed"] = reason
                logger.warning(
                    f"Ticket {ticket_id} was NOT closed: {reason}")
    except _SkipTicketClose:
        logger.info("ticket close is off (CSUITE_TICKET_CLOSE_ENABLED)")
    except Exception as e:
        logger.error(f"Ticket lookup/close error: {e}", exc_info=True)
        if state.get("ticket_id") and not results["ticket_closed"]:
            # A close was attempted for a known ticket and blew up — the user
            # needs to know it is still open.
            results["ticket_close_failed"] = str(e)

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
    # The DAF Inquiry Form and the Endowment Inquiry Form both carry these
    # four as REQUIRED fields, so every submission has a full address —
    # verified against reports/hubspot_form_fields_2026-09-30.csv. They were
    # not mapped here before, so the address was discarded at the parse step
    # and nobody downstream could tell it had ever been submitted.
    #
    # They are mapped now so the address can be REPORTED. It is still not
    # sent to CSuite: no input name for it has been found, and nine
    # candidates have been eliminated by sandbox writes. See
    # clients/csuite.py::create_individual_profile.
    "address": "address_street",
    "street address": "address_street",
    "address2": "address_street2",
    "city": "address_city",
    "state": "address_state",
    "state/region": "address_state",
    "zip": "address_zip",
    "zipcode": "address_zip",
    "postal code": "address_zip",
}

# The parsed keys that together make up an address, in the order a person
# would write them.
_ADDRESS_PARTS = ("address_street", "address_street2", "address_city",
                  "address_state", "address_zip")


def submitted_address(data: dict) -> str:
    """The submitted address as one line, or "" if none was submitted.

    For showing to a person so they can enter it in CSuite by hand. Not for
    sending anywhere: CSuite's address input name is unknown.
    """
    street = " ".join(p for p in (data.get("address_street"),
                                  data.get("address_street2")) if p).strip()
    city = (data.get("address_city") or "").strip()
    state = (data.get("address_state") or "").strip()
    zipcode = (data.get("address_zip") or "").strip()

    tail = " ".join(p for p in (state, zipcode) if p)
    return ", ".join(p for p in (street, city, tail) if p)


def _parse_submission(submission: dict) -> dict:
    """Parse a HubSpot form submission into a normalised dict."""
    parsed = {
        # Identifiers, not donor data. They are what makes a submission
        # replayable from HubSpot after a run that could not complete —
        # see record_unprocessed_submission.
        "submission_id": str(submission.get("conversionId")
                             or submission.get("submittedAt") or ""),
        "first_name": "",
        "last_name": "",
        "email": "",
        "phone": "",
        "address_street": "",
        "address_street2": "",
        "address_city": "",
        "address_state": "",
        "address_zip": "",
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

def _task_lines(data: dict, results: dict) -> list:
    """The one follow-up-task line, whichever it is.

    Shared by the normal path and the duplicate-donor path so the two cannot
    drift — they had already drifted once, with the duplicate path missing the
    read-back warning.
    """
    if results.get("task_id"):
        task_id = results["task_id"]
        link = ui_url(Config.CSUITE_TASK_URL, results.get("csuite_api_base"),
                      task_id=task_id)
        donor = results.get("task_donor") or (
            (data.get("first_name", "") + " "
             + data.get("last_name", "")).strip() or "this donor")
        assignee = results.get("task_assignee") or "unassigned"
        out = [f"📝 Follow-up task {mark_id(task_id)} for {assignee} — "
               f"re: {donor} — due {results['task_due']} — [View]({link})"]
        if results.get("task_warning"):
            out.append(f"   {results['task_warning']}")
        return out
    if results.get("task_failed"):
        # Never presented as a failure of the profile, which succeeded.
        return [f"⚠️ Follow-up task NOT created ({results['task_failed']}) — "
                "add it by hand in CSuite."]
    if results.get("task_skipped"):
        # The endowment-specific "assignee not set" warning was removed on
        # 2026-10-02: CSUITE_TASK_EMPLOYEE_ID_ENDOWMENT_INQUIRY is now set, so
        # that gap is closed and the line would only ever appear if someone
        # unset it — which the generic line already covers.
        return [f"📝 No task: {results['task_skipped']}"]
    return []


def _format_confirmation(data: dict, state: dict, results: dict, type_label: str) -> str:
    """Format the workflow completion confirmation."""
    name = f"{data.get('first_name', '')} {data.get('last_name', '')}".strip()
    lines = []

    # A skip is not a creation and not a failure. Saying "Created" over a
    # run that created nothing is the same class of false success as the
    # 200 CSuite returns for a field it discarded.
    if results.get("duplicate_reason"):
        # Nothing was created and nothing was PATCHed.
        profile_id = results.get("duplicate_of")
        kind = results.get("duplicate_kind")

        if kind == "stale_no_match":
            # The donor has NO profile and nobody can safely make one here:
            # HubSpot's link is wrong and only a person can say what it meant.
            lines.append("🛑 **No profile created — HubSpot's CSuite link is "
                         "broken**")
            lines.append("")
            lines.append(f"⚠️ {results['duplicate_reason']}")
            lines.append("")
            lines.extend(_task_lines(data, results))
            lines.append("")
            lines.append("Say anything to continue, or start a new workflow.")
            return "\n".join(lines)

        if kind in ("stale_with_match", "conflict") and profile_id:
            heading = ("♻️ **Already in CSuite — no new profile created**"
                       if kind == "stale_with_match"
                       else "⚠️ **Two CSuite profiles — no new profile "
                            "created**")
            lines.append(heading)
            lines.append("")
            link = ui_url(Config.CSUITE_PROFILE_URL,
                          state.get("csuite_api_base"), profile_id=profile_id)
            lines.append(
                f"👤 Profile {mark_id(profile_id)} — [CSuite]({link})")
            lines.append(f"⚠️ {results['duplicate_reason']}")
            if results.get("backfill"):
                lines.append(f"🔗 {results['backfill']}")
            lines.extend(_task_lines(data, results))
            lines.append("")
            lines.append("Say anything to continue, or start a new workflow.")
            return "\n".join(lines)

        if profile_id:
            link = ui_url(Config.CSUITE_PROFILE_URL,
                          state.get("csuite_api_base"), profile_id=profile_id)
            lines.append(f"♻️ **Already in CSuite — no new profile created**")
            lines.append("")
            lines.append(
                f"👤 Profile {mark_id(profile_id)} — [CSuite]({link})")
            lines.append(f"   Found via: {results['duplicate_reason']}")
            if results.get("backfill"):
                icon = ("🔗" if results.get("backfill_wrote")
                        else "⚠️" if results.get("backfill_conflict")
                        else "🔗")
                lines.append(f"{icon} {results['backfill']}")
            lines.extend(_task_lines(data, results))
        else:
            lines.append("🛑 **No profile created — a duplicate could not be "
                         "ruled out**")
            lines.append("")
            lines.append(f"   {results['duplicate_reason']}")
            lines.append("   Nothing was created and nothing was changed. "
                         "CSuite has no way to merge two donor profiles from "
                         "here, so this stops rather than guesses.")
            if results.get("backfill"):
                lines.append(f"🔗 {results['backfill']}")
            lines.extend(_task_lines(data, results))
        lines.append("")
        lines.append("Say anything to continue, or start a new workflow.")
        return "\n".join(lines)

    if results.get("profile_skipped"):
        lines.append(f"⏸️ **{type_label} Not Created**")
    elif not results["profile_created"]:
        # Nothing was created. "Created (with warnings)" over a run that made
        # no record is the same overstatement as "Failed to create" over a run
        # that sent nothing — measured 2026-10-01, when a budget-refused
        # inquiry reported "⚠️ DAF Created (with warnings)".
        lines.append(f"❌ **{type_label} NOT Created**")
    elif results.get("fund_deferred"):
        # "DAF Created" over a run that opened no fund is the same overstatement
        # as "Failed to create" over a run that sent nothing. The profile was
        # created; the DAF was not.
        suffix = (" (with warnings)" if results["errors"]
                  or results.get("ticket_close_failed") else "")
        lines.append(f"✅ **{type_label} Inquiry — Profile Created**{suffix}")
    elif results["errors"] or results.get("ticket_close_failed"):
        lines.append(f"⚠️ **{type_label} Created (with warnings)**")
    else:
        lines.append(f"✅ **{type_label} Created!**")

    lines.append("")

    # Profile
    if results["profile_created"]:
        profile_link = ui_url(Config.CSUITE_PROFILE_URL,
                              state.get("csuite_api_base"),
                              profile_id=state['profile_id'])
        lines.append(f"👤 Profile: {name} — [CSuite]({profile_link})")
        if results.get("phone_warning"):
            # CSuite validates phone_number and rejects the whole create on a
            # value it dislikes, so a number it would refuse is left out and
            # named here. Never only in a log.
            lines.append(f"📱 Profile created. {results['phone_warning']}")
        if results.get("address_warning"):
            # Only an INCOMPLETE address warns now. Independent of the phone
            # warning — a submission can trip both.
            lines.append(results["address_warning"])
    elif results.get("profile_skipped"):
        lines.append(
            "⏸️ Profile: **not created — CSuite profile creation is "
            "turned off.** CSuite silently discards the email, phone and "
            "address this path sends, so a profile made now would be "
            "missing them. Create it in CSuite by hand, or set "
            "CSUITE_DAF_CREATE_ENABLED once the field names are fixed.")
    else:
        lines.append(f"❌ Profile: Failed to create")
        # Say whether the submission can still be picked up. A reply that
        # reports a failure without saying what happens to the donor's form is
        # a reply that invites someone to assume it was handled.
        if results.get("replay_recorded"):
            lines.append(
                "📥 The submission is recorded and can be re-processed from "
                "HubSpot — nothing was lost. Say *\"process DAF inquiry\"* "
                "again once the cause is cleared.")
        elif results.get("replay_recorded") is False:
            lines.append(
                "🚨 The submission could NOT be recorded for replay. Find it "
                "in HubSpot by hand before another submission arrives on the "
                "same form.")

    # Fund
    if results.get("fund_deferred"):
        lines.append("💰 Fund: not opened yet — a fund is created when the "
                     "donor commits, not at inquiry.")
    elif results["fund_created"]:
        fund_name = data.get("fund_name") or f"{data.get('last_name', 'New')} Family Fund"
        fund_link = ui_url(Config.CSUITE_FUND_URL,
                           state.get("csuite_api_base"),
                           funit_id=state['funit_id'])
        lines.append(f"💰 Fund: {fund_name} — [CSuite]({fund_link})")
        if results.get("fund_warning"):
            lines.append(results["fund_warning"])
    elif results["profile_created"]:
        lines.append("❌ Fund: Failed to create")
    # No profile means no fund was ever in question, so no fund line at all.

    # HubSpot
    hs_contact_id = state.get("hubspot_contact_id")
    if results.get("hubspot_refused"):
        # Structural, not a failure. Saying "Could not update" would read as
        # something to retry, and there is nothing to retry: the write was
        # refused because this deployment is pointed at sandbox CSuite.
        lines.append("🎯 HubSpot: not updated (sandbox run)")
    elif results["hubspot_created"] and hs_contact_id:
        hs_link = Config.HUBSPOT_CONTACT_URL.format(contact_id=hs_contact_id)
        lines.append(f"🎯 HubSpot contact created — [View]({hs_link})")
    elif results["hubspot_updated"] and hs_contact_id:
        hs_link = Config.HUBSPOT_CONTACT_URL.format(contact_id=hs_contact_id)
        lines.append(f"🎯 HubSpot contact updated — [View]({hs_link})")
    elif results["hubspot_created"] or results["hubspot_updated"]:
        lines.append(f"🎯 HubSpot contact synced with CSuite IDs")
    elif results["profile_created"]:
        lines.append("⚠️ HubSpot contact: Could not update or create")

    # Follow-up task. Exactly one line, and never silent: a reminder nobody
    # was told about is a reminder that does not exist.
    lines.extend(_task_lines(data, results))

    # Ticket. Always says which one, or that there was none — a bare
    # "Ticket closed" does not let anyone check it closed the right thing.
    matches = results.get("ticket_matches") or []
    if results.get("ticket_skipped") == "off":
        lines.append("🎫 Ticket close: off")
    elif results["ticket_closed"]:
        ticket_id = state["ticket_id"]
        ticket_link = Config.HUBSPOT_TICKET_URL.format(ticket_id=ticket_id)
        subject = state.get("ticket_subject") or "(no subject)"
        lines.append(f"📋 Ticket {ticket_id} closed — *{subject}* "
                     f"— [View]({ticket_link})")
    elif results.get("ticket_close_failed"):
        ticket_id = state.get("ticket_id", "unknown")
        ticket_link = Config.HUBSPOT_TICKET_URL.format(ticket_id=ticket_id)
        lines.append(
            f"⚠️ Ticket {ticket_id} was **NOT closed** "
            f"({results['ticket_close_failed']}) — close it by hand: "
            f"[View]({ticket_link})"
        )
    elif len(matches) > 1:
        # Closing the wrong one is not reversible by this workflow, so it
        # closes neither and hands the choice over with enough to decide on.
        lines.append(
            f"📋 **{len(matches)} open tickets** carry this email, so none was "
            "closed — pick one and close it by hand:")
        for match in matches:
            link = Config.HUBSPOT_TICKET_URL.format(ticket_id=match["id"])
            lines.append(f"   • Ticket {match['id']} — *{match['subject']}* "
                         f"— [View]({link})")
    else:
        lines.append("📋 No matching ticket — nothing was closed.")
    if results.get("ticket_note"):
        lines.append(f"   {results['ticket_note']}")

    # Errors
    if results["errors"]:
        lines.append("")
        lines.append("**Issues:**")
        for err in results["errors"]:
            lines.append(f"• ⚠️ {err}")

    lines.append("")
    lines.append("Say anything to continue, or start a new workflow.")

    return "\n".join(lines)