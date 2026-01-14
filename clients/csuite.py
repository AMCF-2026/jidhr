"""
CSuite Client
=============
Client for CSuite Fund Accounting API.
"""

import logging
import requests
from config import Config

logger = logging.getLogger(__name__)


class CSuiteClient:
    """Client for CSuite API"""
    
    def __init__(self):
        self.api_key = Config.CSUITE_API_KEY
        self.api_secret = Config.CSUITE_API_SECRET
        self.base_url = Config.CSUITE_BASE_URL
        self.headers = {
            "x-api-key": self.api_key,
            "x-api-secret": self.api_secret,
            "Content-Type": "application/json"
        }
    
    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Make a GET request to CSuite API"""
        if not self.api_key or not self.api_secret:
            logger.error("CSuite API credentials not configured")
            return {"error": "CSuite API credentials not configured"}
        
        url = f"{self.base_url}/{endpoint}"
        logger.info(f"CSuite GET: {endpoint} | params: {params}")
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            logger.info(f"CSuite Response: {response.status_code}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"CSuite Error: {str(e)}")
            return {"error": str(e)}
    
    # =========================================================================
    # FUNDS
    # =========================================================================
    
    def get_funds(self, limit: int = 100) -> dict:
        """Get list of funds"""
        return self._get("fund/list", {"view_limit": limit})
    
    def get_fund(self, fund_id: int) -> dict:
        """Get specific fund details"""
        return self._get("fund/display", {"funit_id": fund_id})
    
    def search_funds(self, query: str) -> dict:
        """Search funds"""
        return self._get("search", {"q": query, "type": "funit"})
    
    # =========================================================================
    # PROFILES
    # =========================================================================
    
    def get_profiles(self, limit: int = 100) -> dict:
        """Get profiles (donors, vendors, etc.)"""
        return self._get("profile/list", {"view_limit": limit})
    
    def search_profiles(self, query: str) -> dict:
        """Search profiles"""
        return self._get("search", {"q": query, "type": "profile"})
    
    # =========================================================================
    # GRANTS
    # =========================================================================
    
    def get_grants(self, limit: int = 100) -> dict:
        """Get grants list"""
        return self._get("grant/list", {"view_limit": limit})
    
    # =========================================================================
    # DONATIONS
    # =========================================================================
    
    def get_donations(self, limit: int = 100) -> dict:
        """Get donations list"""
        return self._get("donation/list", {"view_limit": limit})
    
    # =========================================================================
    # VOUCHERS
    # =========================================================================
    
    def get_vouchers(self, limit: int = 100) -> dict:
        """Get vouchers list"""
        return self._get("voucher/list", {"view_limit": limit})
    
    # =========================================================================
    # EVENTS
    # =========================================================================
    
    def get_events(self, limit: int = 100) -> dict:
        """Get events list"""
        return self._get("event/list", {"view_limit": limit})
    
    def get_event(self, event_id: int) -> dict:
        """Get specific event details"""
        return self._get("event/display", {"event_id": event_id})
    
    def get_event_dates(self, limit: int = 100) -> dict:
        """Get event dates"""
        return self._get("event_date/list", {"view_limit": limit})
