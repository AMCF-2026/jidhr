"""
HubSpot Client
==============
Client for HubSpot CRM and Marketing APIs.
"""

import logging
import requests
from config import Config

logger = logging.getLogger(__name__)


class HubSpotClient:
    """Client for HubSpot API"""
    
    def __init__(self):
        self.access_token = Config.HUBSPOT_ACCESS_TOKEN
        self.base_url = Config.HUBSPOT_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
    
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
    # SOCIAL MEDIA
    # =========================================================================
    
    def get_social_channels(self) -> dict:
        """Get connected social media channels"""
        return self._get("broadcast/v1/channels/setting/publish/current")
    
    def get_social_broadcasts(self, limit: int = 10) -> dict:
        """Get social media broadcasts"""
        return self._get("broadcast/v1/broadcasts", {"limit": limit})
    
    def create_social_broadcast(self, data: dict) -> dict:
        """Create a social media broadcast"""
        return self._post("broadcast/v1/broadcasts", data)
    
    # =========================================================================
    # CAMPAIGNS
    # =========================================================================
    
    def get_campaigns(self, limit: int = 10) -> dict:
        """Get campaigns"""
        return self._get("marketing/v3/campaigns", {"limit": limit})
    
    # =========================================================================
    # TASKS
    # =========================================================================
    
    def create_task(self, properties: dict) -> dict:
        """Create a task"""
        return self._post("crm/v3/objects/tasks", {"properties": properties})
    
    # =========================================================================
    # OWNERS
    # =========================================================================
    
    def get_owners(self) -> dict:
        """Get list of owners"""
        return self._get("crm/v3/owners")
