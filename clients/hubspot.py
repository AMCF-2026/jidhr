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
    
    # =========================================================================
    # CONTACTS
    # =========================================================================
    
    def get_contacts(self, limit: int = 10) -> dict:
        """Get contacts list"""
        return self._get("crm/v3/objects/contacts", {"limit": limit})
    
    def search_contacts(self, query: str) -> dict:
        """Search contacts"""
        return self._post("crm/v3/objects/contacts/search", {
            "query": query,
            "limit": 10
        })
    
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
    
    def create_marketing_event(self, event_data: dict) -> dict:
        """Create a marketing event"""
        return self._post("marketing/v3/marketing-events", event_data)
    
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
