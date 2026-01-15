"""
HubSpot Client
==============
Client for HubSpot CRM and Marketing APIs.

Jidhr v1.2 - Added:
- Marketing Email Drafts
- Social Media Posts (create/schedule)
- Tasks (list)
"""

import logging
import requests
from datetime import datetime
from config import Config

logger = logging.getLogger(__name__)


class HubSpotClient:
    """Client for HubSpot API"""
    
    # =========================================================================
    # TEMPLATE & CHANNEL MAPPINGS
    # =========================================================================
    
    # Email templates discovered via API
    EMAIL_TEMPLATES = {
        "amcf": "EMAIL_DND_TEMPLATE/AMFC Emails.html",
        "amfc": "EMAIL_DND_TEMPLATE/AMFC Emails.html",  # alias
        "newsletter": "EMAIL_DND_TEMPLATE/AMFC Emails.html",  # alias
        "giving circle": "EMAIL_DND_TEMPLATE/Giving Circle Email.html",
        "giving_circle": "EMAIL_DND_TEMPLATE/Giving Circle Email.html",  # alias
    }
    
    # Social channel mapping (friendly name -> channel key pattern)
    # Actual channel keys will be fetched dynamically
    SOCIAL_PLATFORMS = {
        "twitter": "TwitterChannel",
        "x": "TwitterChannel",
        "facebook": "FacebookPage",
        "linkedin": "LinkedInCompanyPage",
        "instagram": "InstagramBusinessProfile",
    }
    
    def __init__(self):
        self.access_token = Config.HUBSPOT_ACCESS_TOKEN
        self.base_url = Config.HUBSPOT_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        # Cache for social channels (populated on first use)
        self._social_channels_cache = None
    
    # =========================================================================
    # HTTP METHODS
    # =========================================================================
    
    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Make a GET request to HubSpot API"""
        if not self.access_token:
            logger.error("HubSpot access token not configured")
            return {"error": "HubSpot access token not configured"}
        
        url = f"{self.base_url}/{endpoint}"
        logger.info(f"HubSpot GET: {endpoint} | params: {params}")
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            logger.info(f"HubSpot Response: {response.status_code}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"HubSpot Error: {str(e)}")
            return {"error": str(e)}
    
    def _post(self, endpoint: str, data: dict = None) -> dict:
        """Make a POST request to HubSpot API"""
        if not self.access_token:
            logger.error("HubSpot access token not configured")
            return {"error": "HubSpot access token not configured"}
        
        url = f"{self.base_url}/{endpoint}"
        logger.info(f"HubSpot POST: {endpoint}")
        
        try:
            response = requests.post(
                url,
                headers=self.headers,
                json=data,
                timeout=30
            )
            logger.info(f"HubSpot Response: {response.status_code}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"HubSpot Error: {str(e)}")
            return {"error": str(e)}
    
    def _patch(self, endpoint: str, data: dict = None) -> dict:
        """Make a PATCH request to HubSpot API"""
        if not self.access_token:
            logger.error("HubSpot access token not configured")
            return {"error": "HubSpot access token not configured"}
        
        url = f"{self.base_url}/{endpoint}"
        logger.info(f"HubSpot PATCH: {endpoint}")
        
        try:
            response = requests.patch(
                url,
                headers=self.headers,
                json=data,
                timeout=30
            )
            logger.info(f"HubSpot Response: {response.status_code}")
            
            # PATCH may return 204 No Content on success
            if response.status_code == 204:
                return {"success": True}
            
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"HubSpot Error: {str(e)}")
            return {"error": str(e)}
    
    # =========================================================================
    # CONTACTS
    # =========================================================================
    
    def get_contacts(self, limit: int = 10) -> dict:
        """Get contacts list"""
        return self._get("crm/v3/objects/contacts", {"limit": limit})
    
    def get_contact(self, contact_id: str, properties: list = None) -> dict:
        """Get contact by ID with optional properties"""
        params = {}
        if properties:
            params["properties"] = ",".join(properties)
        return self._get(f"crm/v3/objects/contacts/{contact_id}", params)
    
    def search_contacts(self, query: str) -> dict:
        """Search contacts by query string"""
        return self._post("crm/v3/objects/contacts/search", {
            "query": query,
            "limit": 10
        })
    
    def search_contact_by_email(self, email: str) -> dict:
        """Search for a contact by exact email match"""
        return self._post("crm/v3/objects/contacts/search", {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "email",
                    "operator": "EQ",
                    "value": email
                }]
            }],
            "limit": 1
        })
    
    def update_contact(self, contact_id: str, properties: dict) -> dict:
        """Update contact properties by ID"""
        return self._patch(f"crm/v3/objects/contacts/{contact_id}", {
            "properties": properties
        })
    
    def update_contact_by_email(self, email: str, properties: dict) -> dict:
        """Find contact by email and update properties"""
        # First, find the contact
        search_result = self.search_contact_by_email(email)
        
        if "error" in search_result:
            return search_result
        
        results = search_result.get("results", [])
        if not results:
            return {"error": f"Contact not found: {email}"}
        
        contact_id = results[0]["id"]
        return self.update_contact(contact_id, properties)
    
    # =========================================================================
    # COMPANIES
    # =========================================================================
    
    def get_companies(self, limit: int = 10) -> dict:
        """Get companies list"""
        return self._get("crm/v3/objects/companies", {"limit": limit})
    
    # =========================================================================
    # FORMS
    # =========================================================================
    
    def get_forms(self, limit: int = 10) -> dict:
        """Get forms list"""
        return self._get("marketing/v3/forms", {"limit": limit})
    
    def get_form_submissions(self, form_id: str) -> dict:
        """Get form submissions"""
        return self._get(f"form-integrations/v1/submissions/forms/{form_id}")
    
    # =========================================================================
    # MARKETING EVENTS
    # =========================================================================
    
    def get_marketing_events(self, limit: int = 10) -> dict:
        """Get marketing events"""
        return self._get("marketing/v3/marketing-events", {"limit": limit})
    
    def get_marketing_event(self, event_id: str) -> dict:
        """Get specific marketing event"""
        return self._get(f"marketing/v3/marketing-events/{event_id}")
    
    def create_marketing_event(self, event_data: dict) -> dict:
        """Create a marketing event
        
        Required fields:
        - eventName: str
        - eventOrganizer: str (HubSpot owner ID)
        - externalEventId: str (unique ID for deduplication)
        
        Optional fields:
        - eventDescription: str
        - eventUrl: str
        - eventType: str
        - startDateTime: str (ISO 8601)
        - endDateTime: str (ISO 8601)
        - customProperties: list of {name, value}
        """
        return self._post("marketing/v3/marketing-events", event_data)
    
    def search_marketing_event_by_external_id(self, external_id: str) -> dict:
        """Search for marketing event by external ID"""
        return self._get(f"marketing/v3/marketing-events/external/{external_id}")
    
    # =========================================================================
    # MARKETING EMAILS (NEW - Jidhr v1.2)
    # =========================================================================
    
    def get_marketing_emails(self, limit: int = 20) -> dict:
        """Get list of marketing emails
        
        Args:
            limit: Number of emails to return (default 20)
            
        Returns:
            dict with 'results' containing email objects
        """
        return self._get("marketing/v3/emails", {"limit": limit})
    
    def get_marketing_email(self, email_id: str) -> dict:
        """Get a specific marketing email by ID"""
        return self._get(f"marketing/v3/emails/{email_id}")
    
    def create_marketing_email_draft(
        self,
        name: str,
        subject: str,
        body_html: str,
        template: str = "amcf"
    ) -> dict:
        """Create a marketing email draft
        
        Args:
            name: Internal name for the email (e.g., "DAF Portal Launch - Jan 2026")
            subject: Email subject line
            body_html: HTML content for the email body
            template: Template key - "amcf", "newsletter", or "giving circle"
            
        Returns:
            dict with created email details including 'id' for editing URL
        
        Example:
            result = client.create_marketing_email_draft(
                name="Ramadan Campaign Draft",
                subject="Prepare for Ramadan Giving",
                body_html="<p>Ramadan is approaching...</p>",
                template="amcf"
            )
            # Edit URL: https://app-na2.hubspot.com/email/243832852/edit/{result['id']}/content
        """
        # Resolve template path
        template_key = template.lower().strip()
        template_path = self.EMAIL_TEMPLATES.get(template_key)
        
        if not template_path:
            available = ", ".join(self.EMAIL_TEMPLATES.keys())
            return {"error": f"Unknown template '{template}'. Available: {available}"}
        
        # Build the email payload
        # Using the v3 API content.widgets structure discovered from community
        payload = {
            "name": name,
            "subject": subject,
            "templatePath": template_path,
            "content": {
                "widgets": {
                    "hs_email_body": {
                        "body": {
                            "html": body_html
                        },
                        "id": "hs_email_body",
                        "label": "Main body",
                        "name": "hs_email_body",
                        "type": "rich_text"
                    }
                }
            }
        }
        
        logger.info(f"Creating email draft: {name} with template: {template_path}")
        result = self._post("marketing/v3/emails", payload)
        
        # Add helpful edit URL if successful
        if "id" in result:
            result["edit_url"] = f"https://app-na2.hubspot.com/email/243832852/edit/{result['id']}/content"
        
        return result
    
    # =========================================================================
    # SUBSCRIPTIONS (Email/Newsletter)
    # =========================================================================
    
    def get_subscription_status(self, email: str) -> dict:
        """Get email subscription status for a contact"""
        return self._get(f"communication-preferences/v3/status/email/{email}")
    
    def subscribe_contact(self, email: str, subscription_id: str, legal_basis: str = "LEGITIMATE_INTEREST_CLIENT") -> dict:
        """Subscribe a contact to a subscription type
        
        Args:
            email: Contact's email address
            subscription_id: HubSpot subscription ID (e.g., "1265988358" for Marketing Information)
            legal_basis: Legal basis for subscription (LEGITIMATE_INTEREST_CLIENT, CONSENT_WITH_NOTICE, etc.)
        """
        return self._post("communication-preferences/v3/subscribe", {
            "emailAddress": email,
            "subscriptionId": subscription_id,
            "legalBasis": legal_basis,
            "legalBasisExplanation": "Opted in via CSuite donor portal"
        })
    
    def unsubscribe_contact(self, email: str, subscription_id: str) -> dict:
        """Unsubscribe a contact from a subscription type"""
        return self._post("communication-preferences/v3/unsubscribe", {
            "emailAddress": email,
            "subscriptionId": subscription_id
        })
    
    # =========================================================================
    # SOCIAL MEDIA (ENHANCED - Jidhr v1.2)
    # =========================================================================
    
    def get_social_channels(self) -> dict:
        """Get connected social media channels
        
        Returns list of channels with channelId, channelType, name, etc.
        """
        return self._get("broadcast/v1/channels/setting/publish/current")
    
    def _get_channel_key(self, platform: str) -> str:
        """Get the channel key for a platform
        
        Args:
            platform: Friendly name like "facebook", "twitter", "linkedin", "instagram"
            
        Returns:
            Channel key like "FacebookPage:1159312454102818" or None if not found
        """
        # Fetch channels if not cached
        if self._social_channels_cache is None:
            channels_response = self.get_social_channels()
            if isinstance(channels_response, list):
                self._social_channels_cache = channels_response
            else:
                self._social_channels_cache = []
        
        # Find matching channel
        platform_lower = platform.lower().strip()
        channel_type = self.SOCIAL_PLATFORMS.get(platform_lower)
        
        if not channel_type:
            return None
        
        for channel in self._social_channels_cache:
            if channel.get("channelType") == channel_type:
                channel_id = channel.get("channelId")
                return f"{channel_type}:{channel_id}"
        
        return None
    
    def get_social_broadcasts(self, limit: int = 10) -> dict:
        """Get social media broadcasts"""
        return self._get("broadcast/v1/broadcasts", {"limit": limit})
    
    def create_social_broadcast(self, data: dict) -> dict:
        """Create a social media broadcast (raw API)"""
        return self._post("broadcast/v1/broadcasts", data)
    
    def create_social_post(
        self,
        platform: str,
        content: str,
        link_url: str = None,
        photo_url: str = None,
        schedule_time: datetime = None,
        campaign_guid: str = None
    ) -> dict:
        """Create a social media post (draft, scheduled, or publish now)
        
        Args:
            platform: "facebook", "twitter", "linkedin", or "instagram"
            content: The post text/message
            link_url: Optional URL to include in the post
            photo_url: Optional image URL (must be publicly accessible)
            schedule_time: Optional datetime to schedule (None = draft, "now" for immediate)
            campaign_guid: Optional HubSpot campaign GUID
            
        Returns:
            dict with broadcast details
            
        Example - Create draft:
            result = client.create_social_post(
                platform="facebook",
                content="Check out our new DAF portal!",
                link_url="https://amuslimcf.org/daf"
            )
            
        Example - Schedule post:
            from datetime import datetime, timedelta
            schedule = datetime.now() + timedelta(days=1)
            result = client.create_social_post(
                platform="linkedin",
                content="Year-end giving strategies...",
                schedule_time=schedule
            )
        """
        # Get channel key
        channel_key = self._get_channel_key(platform)
        if not channel_key:
            available = ", ".join(self.SOCIAL_PLATFORMS.keys())
            return {"error": f"Channel not found for '{platform}'. Available: {available}"}
        
        # Build broadcast payload
        payload = {
            "channelKeys": [channel_key],
            "content": {
                "body": content
            }
        }
        
        # Add optional fields
        if link_url:
            payload["content"]["linkUrl"] = link_url
        
        if photo_url:
            payload["content"]["photoUrl"] = photo_url
        
        if campaign_guid:
            payload["campaignGuid"] = campaign_guid
        
        # Handle scheduling
        if schedule_time:
            if isinstance(schedule_time, datetime):
                # Convert to milliseconds timestamp
                timestamp_ms = int(schedule_time.timestamp() * 1000)
                payload["triggerAt"] = timestamp_ms
            elif schedule_time == "now":
                # Publish immediately (triggerAt in the past or very soon)
                payload["triggerAt"] = int(datetime.now().timestamp() * 1000)
        # If no schedule_time, it creates a draft
        
        logger.info(f"Creating social post for {platform}: {content[:50]}...")
        return self._post("broadcast/v1/broadcasts", payload)
    
    def get_available_social_platforms(self) -> list:
        """Get list of connected social platforms
        
        Returns:
            List of platform names that are connected (e.g., ["facebook", "twitter", "linkedin", "instagram"])
        """
        channels_response = self.get_social_channels()
        
        if not isinstance(channels_response, list):
            return []
        
        # Map channel types back to friendly names
        type_to_name = {v: k for k, v in self.SOCIAL_PLATFORMS.items()}
        
        connected = []
        seen_types = set()
        
        for channel in channels_response:
            channel_type = channel.get("channelType")
            if channel_type and channel_type not in seen_types:
                seen_types.add(channel_type)
                name = type_to_name.get(channel_type, channel_type)
                # Prefer standard names
                if name == "x":
                    name = "twitter"
                connected.append(name)
        
        return connected
    
    # =========================================================================
    # CAMPAIGNS
    # =========================================================================
    
    def get_campaigns(self, limit: int = 10) -> dict:
        """Get campaigns"""
        return self._get("marketing/v3/campaigns", {"limit": limit})
    
    # =========================================================================
    # TASKS (ENHANCED - Jidhr v1.2)
    # =========================================================================
    
    def get_tasks(self, limit: int = 20, owner_id: str = None) -> dict:
        """Get tasks list
        
        Args:
            limit: Number of tasks to return
            owner_id: Optional owner ID to filter by
            
        Returns:
            dict with 'results' containing task objects
        """
        params = {
            "limit": limit,
            "properties": "hs_task_subject,hs_task_body,hs_task_status,hs_task_priority,hs_timestamp,hubspot_owner_id"
        }
        
        if owner_id:
            # Use search endpoint for filtering
            return self._post("crm/v3/objects/tasks/search", {
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "hubspot_owner_id",
                        "operator": "EQ",
                        "value": owner_id
                    }]
                }],
                "properties": params["properties"].split(","),
                "limit": limit
            })
        
        return self._get("crm/v3/objects/tasks", params)
    
    def create_task(self, properties: dict) -> dict:
        """Create a task
        
        Args:
            properties: Task properties dict. Common properties:
                - hs_task_subject: Task title (required)
                - hs_task_body: Task description
                - hs_task_status: "NOT_STARTED", "IN_PROGRESS", "COMPLETED"
                - hs_task_priority: "LOW", "MEDIUM", "HIGH"
                - hs_timestamp: Due date (Unix timestamp in milliseconds)
                - hubspot_owner_id: Assigned owner ID
                
        Example:
            client.create_task({
                "hs_task_subject": "Follow up with donor",
                "hs_task_body": "Discuss DAF contribution",
                "hs_task_priority": "HIGH",
                "hs_task_status": "NOT_STARTED"
            })
        """
        return self._post("crm/v3/objects/tasks", {"properties": properties})
    
    def create_task_simple(
        self,
        subject: str,
        body: str = None,
        priority: str = "MEDIUM",
        due_date: datetime = None,
        owner_id: str = None
    ) -> dict:
        """Create a task with simple parameters
        
        Args:
            subject: Task title
            body: Optional task description
            priority: "LOW", "MEDIUM", or "HIGH"
            due_date: Optional due date
            owner_id: Optional owner to assign to
            
        Returns:
            Created task object
        """
        properties = {
            "hs_task_subject": subject,
            "hs_task_status": "NOT_STARTED",
            "hs_task_priority": priority.upper()
        }
        
        if body:
            properties["hs_task_body"] = body
        
        if due_date:
            properties["hs_timestamp"] = int(due_date.timestamp() * 1000)
        
        if owner_id:
            properties["hubspot_owner_id"] = owner_id
        
        return self.create_task(properties)
    
    # =========================================================================
    # TICKETS
    # =========================================================================
    
    def get_tickets(self, limit: int = 10, properties: list = None) -> dict:
        """Get tickets list
        
        Args:
            limit: Number of tickets to return
            properties: List of properties to include (default: basic props)
        """
        if properties is None:
            properties = ['subject', 'content', 'hs_pipeline', 'hs_pipeline_stage',
                         'hs_ticket_priority', 'createdate', 'hs_lastmodifieddate']
        
        return self._get("crm/v3/objects/tickets", {
            "limit": limit,
            "properties": ",".join(properties)
        })
    
    def get_open_tickets(self, limit: int = 10) -> dict:
        """Get open tickets only"""
        return self._post("crm/v3/objects/tickets/search", {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "hs_pipeline_stage",
                    "operator": "EQ",
                    "value": "1"  # Stage 1 is typically "New/Open"
                }]
            }],
            "properties": ['subject', 'content', 'hs_pipeline_stage', 'hs_ticket_priority', 'createdate'],
            "limit": limit
        })
    
    # =========================================================================
    # OWNERS
    # =========================================================================
    
    def get_owners(self) -> dict:
        """Get list of owners"""
        return self._get("crm/v3/owners")
