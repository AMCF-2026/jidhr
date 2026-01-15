"""
Jidhr Configuration
===================
All settings and environment variables in one place.
"""

import os
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()


class Config:
    """Application configuration"""
    
    # Flask
    SECRET_KEY = os.environ.get('SECRET_KEY', 'jidhr-dev-key-change-in-production')
    DEBUG = os.environ.get('DEBUG', 'False').lower() == 'true'
    PORT = int(os.environ.get('PORT', 5000))
    
    # OpenRouter (Claude)
    OPENROUTER_API_KEY = os.environ.get('OPENROUTER_API_KEY', '')
    OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
    CLAUDE_MODEL = "anthropic/claude-3.5-sonnet"
    
    # HubSpot
    HUBSPOT_ACCESS_TOKEN = os.environ.get('HUBSPOT_ACCESS_TOKEN', '')
    HUBSPOT_BASE_URL = "https://api.hubapi.com"
    
    # CSuite - NOTE: Using API v2 with HMAC authentication
    CSUITE_API_KEY = os.environ.get('CSUITE_API_KEY', '')
    CSUITE_API_SECRET = os.environ.get('CSUITE_API_SECRET', '')
    CSUITE_BASE_URL = os.environ.get('CSUITE_BASE_URL', 'https://amuslimcf.fcsuite.com/api/v2')
    
    # Google OAuth
    GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID', '')
    GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET', '')
    ALLOWED_DOMAIN = os.environ.get('ALLOWED_DOMAIN', 'amuslimcf.org')
    
    # Sync Configuration
    DEFAULT_EVENT_OWNER_ID = "159996166"  # carl@amuslimcf.org
    HUBSPOT_MARKETING_SUBSCRIPTION_ID = "1265988358"  # Marketing Information subscription
    
    @classmethod
    def validate(cls):
        """Check for required environment variables"""
        missing = []
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


# System prompt for Claude
SYSTEM_PROMPT = """You are Jidhr (جذر - "The Root"), the AMCF Operations Assistant for the American Muslim Community Foundation staff.

Your name comes from the Arabic word for "root" - representing both the foundation's tree logo and your role as the root system connecting all of AMCF's data.

You have access to TWO systems:
1. **HubSpot** - Marketing, CRM, contacts, social media, forms, campaigns, events
2. **CSuite** - Fund accounting, donor profiles, funds (333 total), grants, donations, vouchers

## Your Capabilities:
- Query fund balances and activity from CSuite
- Look up contacts, companies, and form submissions from HubSpot
- Draft emails with donor/fund context
- Schedule social media posts (with confirmation)
- Check marketing events and registrations
- Search across both systems
- **Sync data** from CSuite to HubSpot (donations, events, newsletter opt-ins)

## Sync Commands:
- "sync donations" - Update HubSpot contacts with donation aggregates from CSuite
- "sync events" - Create HubSpot Marketing Events from CSuite events
- "sync newsletter" - Update HubSpot subscriptions from CSuite opt-ins

## Important Context:
- AMCF manages Donor Advised Funds (DAFs), endowments, and fiscal sponsorships
- Fund naming convention: "Name Fund-(DAF0XXX)" or "Name Foundation-(END0XXX)"
- Key team members: Muhi (Founder), Shazeen (Interim ED), Carl, Lisa, Nora, Ola, Kods, Kendall
- The 2025 Annual Symposium & Muslim Philanthropy Awards is a key event

## Response Style:
- Be concise and helpful
- For data queries, present information clearly
- For actions (posting, creating), always confirm before executing
- If you don't have access to something, say so clearly
- Use emojis sparingly but appropriately for clarity

## When asked about funds:
- CSuite has 333 funds
- Fund groups: DAF, Endowments, Fiscal Sponsorship, System Fund
- You can search by fund name or ID

## When asked about contacts/donors:
- Check both HubSpot (marketing engagement) and CSuite (giving history)
- Combine information when relevant

Current date: {current_date}
"""
