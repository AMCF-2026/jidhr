"""
Jidhr Configuration
===================
All settings and environment variables in one place.

Updated for v1.3 - Full feature build:
- DAF/Endowment workflow constants
- CSuite UI deep-linking
- HubSpot form IDs
- Fund group mappings
- Fee calculation support
- Reporting date helpers
"""

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()


def _int_or_zero(raw):
    """A non-negative int, or 0. A typo must fail closed, never open."""
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return 0
    return value if value > 0 else 0


class Config:
    """Application configuration"""
    
    # =========================================================================
    # FLASK
    # =========================================================================
    # No fallback on purpose. A default signing key that ships in the repo
    # signs session cookies anyone can forge, and a dev default silently
    # becomes the production key the first time someone forgets to set it.
    # app.py refuses to start when this is unset.
    SECRET_KEY = os.environ.get('SECRET_KEY')
    DEBUG = os.environ.get('DEBUG', 'False').lower() == 'true'
    # `or` rather than a default argument: an env var that is present but
    # blank (Railway does this when a variable is cleared rather than removed)
    # returns '' from os.environ.get, and int('') raises at import time.
    PORT = int(os.environ.get('PORT') or 5000)
    
    # =========================================================================
    # OPENROUTER (Claude)
    # =========================================================================
    OPENROUTER_API_KEY = os.environ.get('OPENROUTER_API_KEY', '')
    OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
    CLAUDE_MODEL = os.environ.get('CLAUDE_MODEL', 'anthropic/claude-sonnet-4.6')
    
    # =========================================================================
    # HUBSPOT
    # =========================================================================
    HUBSPOT_ACCESS_TOKEN = os.environ.get('HUBSPOT_ACCESS_TOKEN', '')
    HUBSPOT_BASE_URL = "https://api.hubapi.com"
    HUBSPOT_PORTAL_ID = "243832852"
    
    # HubSpot Form GUIDs (decoded from share URLs)
    DAF_INQUIRY_FORM_ID = "8dba272f-1986-4bcc-80b4-0b49bc1a4400"
    ENDOWMENT_INQUIRY_FORM_ID = "0d10e268-75e2-448e-ab93-84e7b2479ef5"
    ASSET_DONATION_FORM_ID = "f45f230b-e7ed-4bdb-90fe-ae00a1af1ec2"
    INVESTMENT_REQUEST_FORM_ID = "39cde168-0903-4237-a267-777607128c98"
    
    # HubSpot Giving Circle
    GIVING_CIRCLE_LIST_ID = "126"        # Static, 130 contacts — actual members
    GIVING_CIRCLE_EMAIL_LIST_ID = "31"   # Active, 450 contacts — broader email list

    # HubSpot Marketing Subscription
    HUBSPOT_MARKETING_SUBSCRIPTION_ID = "1265988358"
    
    # HubSpot URL templates (for linking from Jidhr responses)
    HUBSPOT_CONTACT_URL = f"https://app-na2.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/contact/{{contact_id}}"
    # Tickets are object type 0-5 in HubSpot's record URLs. The older
    # /ticket/{id} path redirects here; this is the one the UI itself uses.
    # The placeholder stays {ticket_id} so existing .format(ticket_id=...)
    # callers (intents/daf_workflow.py, clients/hubspot.py) keep working.
    HUBSPOT_TICKET_URL = f"https://app-na2.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/record/0-5/{{ticket_id}}"
    HUBSPOT_TASK_URL = f"https://app-na2.hubspot.com/tasks/{HUBSPOT_PORTAL_ID}/view/all"
    HUBSPOT_FORM_SUBMISSIONS_URL = f"https://app-na2.hubspot.com/forms/{HUBSPOT_PORTAL_ID}/submissions/{{form_id}}"
    
    # =========================================================================
    # CSUITE - Fund Accounting API v2 with HMAC authentication
    # =========================================================================
    CSUITE_API_KEY = os.environ.get('CSUITE_API_KEY', '')
    CSUITE_API_SECRET = os.environ.get('CSUITE_API_SECRET', '')
    CSUITE_BASE_URL = os.environ.get('CSUITE_BASE_URL', 'https://amuslimcf.fcsuite.com/api/v2')

    # Sandbox credentials. Separate variables, never a switched value of
    # the production ones: a single pair that sometimes means sandbox is
    # one typo away from writing to the live fund ledger.
    CSUITE_SANDBOX_KEY = os.environ.get('CSUITE_SANDBOX_KEY', '')
    CSUITE_SANDBOX_SECRET = os.environ.get('CSUITE_SANDBOX_SECRET', '')

    # "sandbox" | "live". Unset means live, so production behaviour is
    # unchanged by this setting existing.
    CSUITE_ENV = (os.environ.get('CSUITE_ENV') or 'live').strip().lower()

    CSUITE_SANDBOX_BASE_URL = os.environ.get(
        'CSUITE_SANDBOX_BASE_URL',
        'https://amuslimcf-sandbox.fcsuite.com/api/v2')

    # Kill switch on the one live caller of profile/create/individual
    # (intents/daf_workflow.py). OFF unless the variable says otherwise.
    #
    # The method has sent `primary_email`, `primary_phone_number` and
    # `primary_address_string` since 2026-03-17. CSuite does not recognise
    # `primary_email` as an input: it returns 200 with a profile_id and
    # discards the value (measured 2026-09-30). The other two are the same
    # shape of guess and `phone_number` is now known to be the real name
    # for one of them. So every profile this path creates is missing the
    # email, and probably the phone and address too, and nothing in the
    # response says so.
    #
    # Default OFF rather than a warning, because the failure is silent by
    # construction and a warning would be the thing that gets missed. The
    # rest of the workflow still runs — see _step_create.
    # A hard cap on CSuite writes per client, enforced inside
    # CSuiteClient._request. **0 means no CSuite write can leave the process.**
    #
    # Default 0 rather than unlimited. CSUITE_DAF_CREATE_ENABLED decides
    # whether the workflow TRIES to create; this decides whether anything can
    # actually be sent, and two independent switches is the point — on
    # 2026-10-01 a guard that looked like it was counting writes was not on
    # the path the calls took, and two records were created under a cap of
    # one.
    #
    # To enable the first live runs, set BOTH:
    #     CSUITE_DAF_CREATE_ENABLED=true
    #     CSUITE_WRITE_BUDGET=2        # one profile + one fund = one run
    # Raise it deliberately, one run at a time, and read the first create
    # back before allowing a second. An empty or unparseable value is 0, not
    # unlimited: a typo must fail closed.
    CSUITE_WRITE_BUDGET = _int_or_zero(
        os.environ.get('CSUITE_WRITE_BUDGET', '0'))

    # Whether an inquiry also OPENS A FUND. Off, and separate from
    # CSUITE_DAF_CREATE_ENABLED on purpose.
    #
    # Decision 2026-10-01: an inquiry form submission creates a CSuite profile
    # only. A fund is opened when the donor commits, not when they enquire —
    # an enquiry is a conversation, and a fund in the ledger for a
    # conversation that goes nowhere is a finance record somebody has to
    # explain. The funit/create path and its read-back are kept intact behind
    # this flag for the commitment stage.
    #
    # Same fail-closed parsing as CSUITE_WRITE_BUDGET: only the exact string
    # "true" enables it.
    CSUITE_DAF_FUND_CREATE_ENABLED = (
        os.environ.get('CSUITE_DAF_FUND_CREATE_ENABLED', 'False')
        .strip().lower() == 'true')

    # Whether the workflow closes the donor's HubSpot ticket. Off.
    #
    # It cannot be proved in the sandbox: a sandbox run refuses every HubSpot
    # write at the transport seam, so the first real proof has to be production.
    # That is a reason to be careful, not a reason to leave it unguarded.
    CSUITE_TICKET_CLOSE_ENABLED = (
        os.environ.get('CSUITE_TICKET_CLOSE_ENABLED', 'False')
        .strip().lower() == 'true')

    # Ticket pipelines and their "New" stage. VERIFIED 2026-10-02 from
    # GET crm/v3/pipelines/tickets against production.
    #
    # Stage "1" is the DAF Pipeline's "New" and exists in NO other pipeline —
    # every other pipeline uses long numeric stage ids. So the old
    # `hs_pipeline_stage == "1"` filter was implicitly DAF-only, which is
    # narrower than it looked and still wrong: Asset Transfer tickets live in
    # the DAF Pipeline too, and endowment tickets are in a pipeline of their own
    # that the filter never reached.
    TICKET_PIPELINES = {
        "daf": {"pipeline": "0", "label": "DAF Pipeline", "new_stage": "1"},
        "endowment": {"pipeline": "1395576547",
                      "label": "Endowment Inquiry",
                      "new_stage": "2250175191"},
    }

    # Whether an inquiry also creates a CSuite FOLLOW-UP TASK. Off, and
    # separate from the profile and fund switches, so each is a decision of its
    # own.
    # Whether a returning donor's HubSpot contact gets its csuite_profile_id
    # filled in. Off, and its own switch because it adds a HubSpot write to a
    # path that previously made none.
    #
    # Why it is needed: when the duplicate guard stops on a profile found by the
    # CSUITE primary_email search rather than by the HubSpot property, HubSpot
    # by definition has no link — and the workflow returns before its PATCH
    # step. So the link never heals, and every repeat inquiry correctly refuses
    # a second profile while silently leaving HubSpot unlinked. Sandbox-21's
    # profile 21662 is still in that state.
    CSUITE_HUBSPOT_BACKFILL_ENABLED = (
        os.environ.get('CSUITE_HUBSPOT_BACKFILL_ENABLED', 'False')
        .strip().lower() == 'true')

    CSUITE_DAF_TASK_CREATE_ENABLED = (
        os.environ.get('CSUITE_DAF_TASK_CREATE_ENABLED', 'False')
        .strip().lower() == 'true')

    # Who the follow-up task is assigned to, PER FORM. **No default, and no
    # fallback to a default person.**
    #
    # A shared assignee was wrong in a way that is easy to miss: a DAF inquiry
    # and an endowment inquiry are different people's work, and a single value
    # silently sent one of them to the other. Worse, a *fallback* would send a
    # form nobody has assigned to whoever happens to be in the shared slot — a
    # task landing in the wrong queue looks exactly like a task landing in the
    # right one.
    #
    # Unset means NO TASK and a line naming the form. That is the only safe
    # default, because there is no way to guess who owns a form.
    CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY = _int_or_zero(
        os.environ.get('CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY', '0')) or None
    CSUITE_TASK_EMPLOYEE_ID_ENDOWMENT_INQUIRY = _int_or_zero(
        os.environ.get('CSUITE_TASK_EMPLOYEE_ID_ENDOWMENT_INQUIRY', '0')) or None

    # Human names for the forms, for the "no assignee set for X" line. A form
    # id in a message tells a reader nothing.
    FORM_LABELS = {
        DAF_INQUIRY_FORM_ID: "DAF Inquiry",
        ENDOWMENT_INQUIRY_FORM_ID: "Endowment Inquiry",
        ASSET_DONATION_FORM_ID: "Asset Transfer",
        INVESTMENT_REQUEST_FORM_ID: "Investment Request",
    }

    # Superseded by the per-form variables above. Kept only so a deployment
    # that still sets it gets a clear answer rather than a silent change of
    # behaviour — see task_assignee_for_form, which ignores it.
    # Who the follow-up task is assigned to. **No default.**
    #
    # A constant here would be the DEFAULT_CASH_ACCOUNT_ID mistake again: 1069
    # sat in this file for months before anyone checked it meant the same
    # account in both environments. Employee ids are worse, because they differ
    # per environment in a way that is invisible — 1006 is Carl in the SANDBOX
    # and is not present in production at all, while 1007 is Kods in both
    # (measured 2026-10-01).
    #
    # Unset means the task is SKIPPED with a stated reason, never assigned to a
    # guess. The two may share a value.
    CSUITE_TASK_EMPLOYEE_ID = _int_or_zero(
        os.environ.get('CSUITE_TASK_EMPLOYEE_ID', '0')) or None
    CSUITE_DAF_TASK_EMPLOYEE_ID = _int_or_zero(
        os.environ.get('CSUITE_DAF_TASK_EMPLOYEE_ID', '0')) or None
    CSUITE_ENDOWMENT_TASK_EMPLOYEE_ID = _int_or_zero(
        os.environ.get('CSUITE_ENDOWMENT_TASK_EMPLOYEE_ID', '0')) or None

    # The CSuite task type. **No default, and unset is fine.**
    #
    # INFERRED optional: five of the seven sandbox tasks carry no
    # task_type_id at all, and in PRODUCTION not one of the seven tasks carries
    # a type. So a task without one is normal, and `task_type_id` is omitted
    # entirely when this is unset rather than guessed at.
    #
    # 1065 is "DIY Form-Contact" in the SANDBOX (VERIFIED 2026-10-01, task
    # 1033). It is NOT confirmed in production — there is no task-type list
    # endpoint this key may read, and no production task uses any type.
    CSUITE_TASK_TYPE_ID = _int_or_zero(
        os.environ.get('CSUITE_TASK_TYPE_ID', '0')) or None

    CSUITE_DAF_CREATE_ENABLED = (
        os.environ.get('CSUITE_DAF_CREATE_ENABLED', 'False')
        .strip().lower() == 'true')
    
    # CSuite UI base URL (for deep-linking to profiles, funds, etc.)
    CSUITE_UI_BASE_URL = "https://amuslimcf.fcsuite.com/erp"
    CSUITE_PROFILE_URL = f"{CSUITE_UI_BASE_URL}/profile/display?profile_id={{profile_id}}"
    CSUITE_FUND_URL = f"{CSUITE_UI_BASE_URL}/funit/display?funit_id={{funit_id}}"
    CSUITE_GRANT_URL = f"{CSUITE_UI_BASE_URL}/grant/display?grant_id={{grant_id}}"
    CSUITE_DONATION_URL = f"{CSUITE_UI_BASE_URL}/donation/display?donation_id={{donation_id}}"
    CSUITE_CHECK_URL = f"{CSUITE_UI_BASE_URL}/check/display?check_id={{check_id}}"
    # INFERRED from the pattern of the others; the task UI path has not been
    # opened and confirmed.
    CSUITE_TASK_URL = f"{CSUITE_UI_BASE_URL}/task/display?task_id={{task_id}}"
    
    # Fund Group IDs (from funit/list/fgroup endpoint)
    FUND_GROUP_SYSTEM = 1000
    FUND_GROUP_FISCAL_SPONSORSHIP = 1001
    FUND_GROUP_DAF = 1002
    FUND_GROUP_MICROPHILANTHROPY = 1003
    FUND_GROUP_MIGRATION = 1004
    FUND_GROUP_FISCAL_DAF = 1005
    FUND_GROUP_GIVING_CIRCLE = 1007
    FUND_GROUP_ENDOWMENT = 1008
    FUND_GROUP_GRANT = 1009
    
    # Default account for fund creation (Bank of America Restricted)
    DEFAULT_CASH_ACCOUNT_ID = 1069
    
    # Default event owner
    DEFAULT_EVENT_OWNER_ID = "159996166"  # carl@amuslimcf.org

    # =========================================================================
    # HUBSPOT TICKET PIPELINES (probe #4, H14 — crm/v3/pipelines/tickets)
    # =========================================================================
    # Stage ids whose metadata.ticketState is OPEN, per pipeline. Read from
    # the API on 2026-09-15, not guessed: every pipeline has exactly one
    # CLOSED stage and it is not in these sets. If a stage is added in the
    # HubSpot UI it will not be counted until this is updated — re-run
    # scripts/probe_apis.py and compare against H14.
    TICKET_OPEN_STAGES = {
        "0":          {"1", "2216593113", "2216593114", "3"},
        "1398895318": {"2256118463", "2256118464", "2256118465", "2256118466"},
        "1395576547": {"2250175191", "2250175192", "2250175193"},
        "1637348066": {"2611947255", "2611947256"},
        "2510066372": {"4179239673", "4179240634", "4179240635"},
    }

    TICKET_PIPELINE_LABELS = {
        "0":          "DAF Pipeline",
        "1398895318": "Investment Form",
        "1395576547": "Endowment Inquiry",
        "1637348066": "ACH Setup",
        "2510066372": "Open Inquiries",
    }

    TICKET_STAGE_LABELS = {
        # DAF Pipeline
        "1": "New", "2216593113": "Contacted/Emailed",
        "2216593114": "Meeting Set", "3": "Waiting on us", "4": "Closed",
        # Investment Form
        "2256118463": "New", "2256118464": "Contacted/Emailed",
        "2256118465": "Meeting Set", "2256118466": "Waiting on us",
        "2256118467": "Closed",
        # Endowment Inquiry
        "2250175191": "New", "2250175192": "Waiting on contact",
        "2250175193": "Waiting on us", "2250175194": "Closed",
        # ACH Setup
        "2611947255": "Information Submitted",
        "2611947256": "Waiting on Kods",
        "2611948218": "Entered Into CSuite",
        # Open Inquiries
        "4179239673": "New", "4179240634": "In Progress",
        "4179240635": "Waiting on Requester", "4179240636": "Closed",
    }

    # Where "closed" does not mean "resolved". ACH Setup's only CLOSED stage
    # is "Entered Into CSuite": a ticket leaves the open count when the data
    # entry is done, which says nothing about whether the requester was
    # ever answered. Any report over that pipeline carries this note.
    TICKET_PIPELINE_NOTES = {
        "1637348066": ("closed here means 'Entered Into CSuite' — data entry "
                       "done, not the request resolved"),
    }
    
    # =========================================================================
    # GOOGLE OAUTH
    # =========================================================================
    GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID', '')
    GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET', '')
    ALLOWED_DOMAIN = os.environ.get('ALLOWED_DOMAIN', 'amuslimcf.org')

    # Every domain a staff login may come from. ALLOWED_DOMAIN is what
    # auth.py checks and stays a single value; this is the same setting
    # read as a list (comma-separated, if it ever grows), for code that
    # needs to ask "is this address one of ours?" — donor call prep, which
    # must not build a donor brief about a colleague.
    ALLOWED_LOGIN_DOMAINS = tuple(
        d.strip().lower().lstrip('@')
        for d in (os.environ.get('ALLOWED_DOMAIN', 'amuslimcf.org') or '')
                 .split(',')
        if d.strip()
    )

    # =========================================================================
    # TEST DATA IN CSUITE
    # =========================================================================
    # CSuite holds funds that exist only to exercise the API. They have
    # balances and grants like any other, so without this list a dormant
    # fund report lists "Testing Fund" as a dormant DAF and a quarterly
    # summary counts test donations as income. Matched case-insensitively
    # as substrings of the fund name by clients/mirror_read.rows().
    TEST_FUND_PATTERNS = ("testing", "test_", "_api_fund", "test daf")
    
    # =========================================================================
    # REPORTING HELPERS
    # =========================================================================
    
    @staticmethod
    def get_current_quarter() -> tuple:
        """Get start and end dates of the current quarter.
        
        Returns:
            (start_date, end_date) as 'YYYY-MM-DD' strings
        """
        today = datetime.now()
        quarter = (today.month - 1) // 3
        start_month = quarter * 3 + 1
        start = datetime(today.year, start_month, 1)
        
        if start_month + 3 > 12:
            end = datetime(today.year + 1, 1, 1) - timedelta(days=1)
        else:
            end = datetime(today.year, start_month + 3, 1) - timedelta(days=1)
        
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    
    @staticmethod
    def get_last_quarter() -> tuple:
        """Get start and end dates of the previous quarter.
        
        Returns:
            (start_date, end_date) as 'YYYY-MM-DD' strings
        """
        today = datetime.now()
        quarter = (today.month - 1) // 3
        
        if quarter == 0:
            start = datetime(today.year - 1, 10, 1)
            end = datetime(today.year - 1, 12, 31)
        else:
            start_month = (quarter - 1) * 3 + 1
            start = datetime(today.year, start_month, 1)
            end = datetime(today.year, start_month + 3, 1) - timedelta(days=1)
        
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    
    @staticmethod
    def get_ramadan_range(year: int = None) -> tuple:
        """Get approximate Ramadan date range for a given year.
        
        Note: Ramadan follows the lunar calendar and shifts ~11 days earlier
        each year. These are approximate ranges for giving analysis.
        
        Returns:
            (start_date, end_date) as 'YYYY-MM-DD' strings
        """
        # Approximate Ramadan dates (Islamic lunar calendar shifts yearly)
        ramadan_dates = {
            2024: ("2024-03-11", "2024-04-09"),
            2025: ("2025-02-28", "2025-03-30"),
            2026: ("2026-02-18", "2026-03-19"),
            2027: ("2027-02-07", "2027-03-08"),
        }
        
        if year is None:
            year = datetime.now().year
        
        return ramadan_dates.get(year, (f"{year}-03-01", f"{year}-03-31"))
    
    @staticmethod
    def get_months_ago(months: int) -> str:
        """Get a date N months ago as 'YYYY-MM-DD' string.
        
        Args:
            months: Number of months to go back
            
        Returns:
            Date string in 'YYYY-MM-DD' format
        """
        today = datetime.now()
        # Handle month underflow
        month = today.month - months
        year = today.year
        while month <= 0:
            month += 12
            year -= 1
        return datetime(year, month, today.day).strftime("%Y-%m-%d")
    
    @staticmethod
    def get_year_range(year: int = None) -> tuple:
        """Get start and end dates for a calendar year.
        
        Returns:
            (start_date, end_date) as 'YYYY-MM-DD' strings
        """
        if year is None:
            year = datetime.now().year
        return f"{year}-01-01", f"{year}-12-31"
    
    # =========================================================================
    # VALIDATION
    # =========================================================================
    
    @classmethod
    def validate(cls):
        """Check for required environment variables"""
        missing = []
        if not cls.SECRET_KEY:
            missing.append("SECRET_KEY")
        if not cls.OPENROUTER_API_KEY:
            missing.append("OPENROUTER_API_KEY")
        if not cls.HUBSPOT_ACCESS_TOKEN:
            missing.append("HUBSPOT_ACCESS_TOKEN")
        if not cls.CSUITE_API_KEY:
            missing.append("CSUITE_API_KEY")
        if not cls.CSUITE_API_SECRET:
            missing.append("CSUITE_API_SECRET")
        if not cls.GOOGLE_CLIENT_ID:
            missing.append("GOOGLE_CLIENT_ID")
        if not cls.GOOGLE_CLIENT_SECRET:
            missing.append("GOOGLE_CLIENT_SECRET")
        return missing


# =============================================================================
# SYSTEM PROMPT
# =============================================================================

SYSTEM_PROMPT = """You are Jidhr (جذر - "The Root"), the AMCF Operations Assistant for the American Muslim Community Foundation staff.

Your name comes from the Arabic word for "root" - representing both the foundation's tree logo and your role as the root system connecting all of AMCF's data.

You have access to TWO systems:
1. **HubSpot** - Marketing, CRM, contacts, social media, forms, campaigns, events, tickets, tasks
2. **CSuite** - Fund accounting, donor profiles, funds (357 total), grants, donations, vouchers, checks

## Your Capabilities:

### Queries & Lookups
- Query fund balances and activity from CSuite
- Look up contacts, companies, and form submissions from HubSpot
- Search across both systems by name, email, or fund
- Check ticket status and task lists
- Look up check status (issued, cleared, voided) for grant disbursements

### Donor & Fund Management
- Prepare donor call talking points (combined HubSpot + CSuite data)
- Track donor journey status for DAF/Endowment startups
- Find contacts associated with a specific fund or endowment
- Show which donors haven't been contacted in 6+ months

### DAF & Endowment Workflows
- Process new DAF inquiries: pull HubSpot form submission → create CSuite profile → create fund → link back to HubSpot
- Process endowment inquiries with the same workflow
- Deep-link to CSuite profiles and funds from chat responses

### Reporting & Analysis
- Quarterly grant counts and summaries
- Ramadan giving comparisons (year-over-year lapsed donor identification)
- Inactive fund identification (no grant in 12+ months)
- Monthly DAF inquiry summaries
- Fee calculations on fund balances
- Uncashed check reports for grant disbursements

### Content Creation
- Draft marketing emails with conversational refinement
- Create social media posts (Facebook, LinkedIn, Twitter, Instagram)
- Write newsletter content and outreach emails
- Personalize outreach using donor/fund context

### Events
- List upcoming events from CSuite
- Show event attendees with RSVP and attendance status
- Sync event attendees to a HubSpot contact list for email targeting
- Compare events year-over-year (who attended before but hasn't registered this time)
- Draft post-event follow-up emails

### Giving Circle
- Show Giving Circle members and their constituent status
- Upgrade members to voting member status

### Data Sync (CSuite → HubSpot)
- "sync donations" - Update HubSpot contacts with donation aggregates from CSuite
- "sync events" - Sync CSuite event attendees to HubSpot contacts + static list
- "sync newsletter" - Update HubSpot subscriptions from CSuite opt-ins

### Task & Note Management
- Create tasks in HubSpot from natural language
- Log call/meeting notes to HubSpot contacts without opening the browser
- Compile investment requests for Andalus

## Important Context:
- AMCF manages Donor Advised Funds (DAFs), endowments, and fiscal sponsorships
- Fund naming convention: "Name Fund-(DAF0XXX)" or "Name Foundation-(END0XXX)"
- CSuite has 379 funds across groups: DAF (1002), Endowments (1008), Fiscal Sponsorship (1001), System Fund (1000), Microphilanthropy (1003), Giving Circle (1007)
- CSuite has ~20,600 profiles and ~26,000 donations
- Key team members: Muhi (Founder), Shazeen (Interim ED), Carl (Tech), Lisa (Events/GC), Nora (Finance/DAF), Ola (Outreach/Endowments), Kods (Finance/DAF), Shatila (Finance)

## AMCF Programs (use this to answer questions about what AMCF offers):

**Donor Advised Funds (DAFs):** A charitable giving account that allows donors to make a single tax-deductible contribution, then recommend grants to nonprofits over time. AMCF manages DAFs with Islamic values — vetting charities, tracking impact, and offering halal investment options. Over 200 families use AMCF DAFs. Contact: Muhi Khwaja. Inquiry form: amuslimcf.org/dafs/

**Endowments:** AMCF helps nonprofits establish permanent funds where the principal is preserved and investment returns are distributed as grants. Provides stable, long-term funding independent of annual fundraising. Designed for established nonprofits. Contact: Ola Mohamed. Inquiry form: amuslimcf.org/nonprofits/endowments-opening/

**EverWaqf Fund:** AMCF's permanent community endowment — a collective waqf that transforms everyday donations into sadaqah jariyah (ongoing charity). Principal is preserved and invested in halal, Shariah-compliant assets; returns fund grants to Muslim nonprofits in perpetuity. Open to individual donors of any amount. Info: amuslimcf.org/everwaqf-fund-a-permanent-waqf-for-eternal-good/

**Muslim Civil Rights Index:** A donor fund that solves analysis paralysis around civil rights giving. Donors set up recurring contributions automatically distributed across a vetted basket of Muslim-focused civil rights organizations. Removes the research burden while ensuring impact. Info: amuslimcf.org/muslimcivilrightsindex/

**Women's Giving Circle (American Muslim Women's Giving Circle):** Founded 2021, the first national giving circle for Muslim women in the US. Members pool contributions to collectively fund Muslim-led nonprofits. Managed by Lisa Kahler. Open to Muslim women nationwide. Info: amuslimcf.org/donors/giving-circles/womensgc-aboutus/ Contact: Lisa Kahler

**AGL Fellowship (Azm Global Leadership Fellowship):** A fully funded, two-year global leadership program for young Muslim leaders ages 22–29, fiscally sponsored by AMCF. "Azm" = Ahlil Azim (people of determination). Non-sectarian, open worldwide. Applications managed externally; AMCF handles fiscal sponsorship and reporting (including Gates Foundation obligations). Contact: Muhi Khwaja or Rola. Info: amuslimcf.org/azm-global-leadership-agl-fellowship/

**Grants Program:** AMCF has distributed over $26M in grants to more than 1,000 nonprofits. Grantmaking guided by six strategic funding areas including diversity/inclusion in Muslim spaces, civil rights, and community development. Currently updating grantmaking initiatives. Opportunities: amuslimcf.org/nonprofits/grants-2/ Past recipients: amuslimcf.org/grant-recipients

**National Zakat Fund:** A case management system for qualifying individuals seeking financial assistance with housing, medical bills, debt, bail, or food. Eligibility based on Islamic zakat criteria (eight categories in Quran 9:60). Applications via Modern Zakat at amuslimcf.modernzakat.com. For individual recipients, not nonprofits.

**Social Impact Accelerator:** A webinar series helping Muslim nonprofits build organizational capacity. Past themes include "Leading Nonprofits in Times of Crisis." For nonprofit leaders strengthening operations and leadership. Info: amuslimcf.org/accelerator/

**Nonprofit Summit:** Annual in-person conference for Muslim nonprofits and changemakers. Next summit: September 3, 2026 in Detroit, Michigan. Focused on partnership building, mission strengthening, and tools grounded in Islamic values. Registration: amuslimcf.org/events/nonprofit-summit-formuslim-nonprofits-changemakers/

**#MuslimPhilanthropy Podcast:** AMCF's podcast covering Muslim philanthropy, nonprofit leadership, and community giving. Episodes: amuslimcf.org/podcast-2/

**AMCF — General:** Founded as a community foundation. $9M+ in assets under management, 240 DAFs, 24 endowments. Vision: leading sustainable and strategic Muslim philanthropy. Mission: cultivating donor giving and diversifying funding to advance charitable causes. Core values: Sacred, Strategic, Sustainable, Collaborative, Diverse, Inclusive, Integrity.

## AMCF Site Directory (use these URLs when directing people to resources):

**Donors:** DAF overview: amuslimcf.org/dafs/ | DAF portal login: amuslimcf.fcsuite.com/erp/portal | DAF donation: amuslimcf.org/daf-donation/ | Investment form: amuslimcf.org/investment/ | Giving Circles: amuslimcf.org/donors/giving-circles/ | Women's GC: amuslimcf.org/donors/giving-circles/womensgc-aboutus/ | Donate appreciated assets: amuslimcf.org/donate/assets/ | Donate to AMCF endowment: amuslimcf.org/donate-to-amcf-endowment/ | Cars4Jannah: amuslimcf.org/cars4jannah/

**Nonprofits:** Overview: amuslimcf.org/nonprofits/ | Endowments: amuslimcf.org/nonprofits/endowments-opening/ | EverWaqf: amuslimcf.org/everwaqf-fund-a-permanent-waqf-for-eternal-good/ | Grants: amuslimcf.org/nonprofits/grants-2/ | Past recipients: amuslimcf.org/grant-recipients/ | Resource Center: amuslimcf.org/nonprofits/nonprofitresourcecenter/ | National Muslim Endowment Council: amuslimcf.org/nonprofits/endowments-council/

**Programs & Events:** Calendar: amuslimcf.org/events/calendar/ | Recorded events: amuslimcf.org/events/recorded-events/ | Nonprofit Summit (Sep 3, 2026 — Detroit): amuslimcf.org/events/nonprofit-summit-formuslim-nonprofits-changemakers/ | Muslim Philanthropy Awards: amuslimcf.org/awards2025/ | Social Impact Accelerator: amuslimcf.org/accelerator/ | Podcast: amuslimcf.org/podcast-2/ | National Muslim Planned Giving Council: amuslimcf.org/events/nmpgc/ | AFP Muslim Affinity Group: amuslimcf.org/events/afp-muslim-affinity-group/

**Individuals:** National Zakat Fund: amuslimcf.org/national-zakat-fund/ | Zakat application: amuslimcf.modernzakat.com | Family & Ramadan giving: amuslimcf.org/mpv/ | Muslim Civil Rights Index: amuslimcf.org/muslimcivilrightsindex/

**AGL Fellowship:** amuslimcf.org/azm-global-leadership-agl-fellowship/

**ACH & Operations:** ACH direct deposit setup: amuslimcf.org/ach-direct-deposit-setup/ | Invest in Impact Ramadan: amuslimcf.org/invest-in-impact-ramadan/

**About AMCF:** About: amuslimcf.org/about-us/ | Team: amuslimcf.org/about-us/team/ | Financials: amuslimcf.org/about-us/financials/ | Blog: amuslimcf.org/blog/ | FAQ: amuslimcf.org/about-us/faq/ | Careers: amuslimcf.org/careers/ | Contact: amuslimcf.org/contact/ | Subscribe: amuslimcf.org/about-us/subscribe/

## Response Style:
- Be concise and helpful
- For data queries, present information clearly with relevant links
- For actions (posting, creating, syncing), always confirm before executing
- Include CSuite/HubSpot deep links when showing profiles, funds, or tickets
- If you don't have access to something, say so clearly
- Use emojis sparingly but appropriately for clarity

## When asked about funds:
- Fund groups: DAF, Endowments, Fiscal Sponsorship, System Fund, Microphilanthropy
- You can search by fund name or ID
- Include balance and fee information when relevant

## When asked about contacts/donors:
- Check both HubSpot (marketing engagement, tickets, activity) and CSuite (giving history, fund associations)
- Combine information when relevant
- Include last contact date and communication history when preparing call talking points

## When processing DAF/Endowment inquiries:
- Pull the form submission from HubSpot
- Show the details for confirmation before creating anything
- Create profile in CSuite first, then create fund, then update HubSpot with the new IDs
- Always provide links to the newly created records

Current date: {current_date}
"""


# =============================================================================
# ORG FACTS (injected into drafting system prompts in intents/content.py;
# wiring lands in the next commit)
# =============================================================================

ORG_FACTS_PROMPT = """
CANONICAL ORG FACTS — use these exactly as written:
- Organization: American Muslim Community Foundation (AMCF)
- Website: https://amuslimcf.org/
- Facebook: https://www.facebook.com/amuslimcf
- Instagram: https://www.instagram.com/amuslimcf/
- LinkedIn: https://www.linkedin.com/company/amuslimcf/

STRICT RULE: Use ONLY the URLs and handles listed above. Never invent, guess,
or abbreviate any URL, domain, social handle, or email address. The domain is
amuslimcf.org — never amcf.org. If you need a specific page link that is not
listed above, write [LINK] as a placeholder instead of guessing.
"""
