"""
CSuite Client
=============
Client for CSuite Fund Accounting API with HMAC-SHA256 authentication.
"""

import hashlib
import hmac
import base64
import json
import time
import logging
import requests
from config import Config

logger = logging.getLogger(__name__)


class CSuiteClient:
    """Client for CSuite API with proper HMAC authentication"""
    
    def __init__(self):
        self.api_key = Config.CSUITE_API_KEY
        self.api_secret = Config.CSUITE_API_SECRET
        self.base_url = Config.CSUITE_BASE_URL
        self.env = "live"
        self.session = requests.Session()
    
    def _generate_signature(self, body: str) -> str:
        """Generate HMAC-SHA256 Base64 signature"""
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            body.encode('utf-8'),
            hashlib.sha256
        )
        return base64.b64encode(signature.digest()).decode('utf-8')
    
    def _build_payload(self, data: dict = None) -> dict:
        """Build request payload with required fields"""
        payload = {
            "env": self.env,
            "epoch": int(time.time())
        }
        if data:
            payload.update(data)
        return payload
    
    def _request(self, endpoint: str, data: dict = None) -> dict:
        """Make authenticated POST request to CSuite API"""
        if not self.api_key or not self.api_secret:
            logger.error("CSuite API credentials not configured")
            return {"error": "CSuite API credentials not configured"}
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        payload = self._build_payload(data)
        body = json.dumps(payload)
        
        headers = {
            "Content-Type": "application/json",
            "SIGNER": self.api_key,
            "SIGNATURE": self._generate_signature(body)
        }
        
        logger.info(f"CSuite POST: {endpoint} | data keys: {list((data or {}).keys())}")
        
        try:
            response = self.session.post(
                url,
                data=body,
                headers=headers,
                timeout=30
            )
            logger.info(f"CSuite Response: {response.status_code}")
            
            try:
                json_response = response.json()
                
                # CSuite returns success: 1 or 0
                if json_response.get("success") == 1:
                    return {
                        "success": True,
                        "data": json_response.get("data"),
                        "messages": json_response.get("messages", [])
                    }
                else:
                    errors = json_response.get("errors", [])
                    logger.warning(f"CSuite API error: {errors}")
                    return {
                        "success": False,
                        "error": errors[0] if errors else "Unknown error",
                        "errors": errors
                    }
                    
            except json.JSONDecodeError as e:
                logger.error(f"CSuite JSON decode error: {str(e)}")
                return {"error": f"Invalid JSON response: {str(e)}"}
                
        except requests.exceptions.RequestException as e:
            logger.error(f"CSuite Request error: {str(e)}")
            return {"error": str(e)}
    
    # =========================================================================
    # PROFILES
    # =========================================================================
    
    def get_profiles(self, limit: int = 100, offset: int = 0) -> dict:
        """Get profiles (donors, vendors, etc.)"""
        return self._request("profile/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_profile(self, profile_id: int) -> dict:
        """Get specific profile details"""
        return self._request("profile/display", {"profile_id": profile_id})
    
    def search_profiles(self, query: str) -> dict:
        """Search profiles by name"""
        return self._request("profile/list/search", {"q": query})
    
    def get_all_profiles(self, max_iterations: int = 200) -> list:
        """Get all profiles across all pages using offset pagination"""
        all_profiles = []
        offset = 0
        limit = 100
        
        for _ in range(max_iterations):
            result = self.get_profiles(limit=limit, offset=offset)
            
            if not result.get("success"):
                logger.error(f"Failed to get profiles at offset {offset}")
                break
            
            data = result.get("data", {})
            profiles = data.get("results", [])
            
            if not profiles:
                break
            
            all_profiles.extend(profiles)
            
            # Check if we got fewer than limit (last page)
            if len(profiles) < limit:
                break
            
            offset += limit
        
        logger.info(f"Retrieved {len(all_profiles)} total profiles")
        return all_profiles
    
    # =========================================================================
    # FUNDS
    # =========================================================================
    
    def get_funds(self, limit: int = 100, offset: int = 0) -> dict:
        """Get list of funds"""
        return self._request("funit/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_fund(self, fund_id: int) -> dict:
        """Get specific fund details"""
        return self._request("funit/display", {"funit_id": fund_id})
    
    def search_funds(self, query: str) -> dict:
        """Search funds"""
        return self._request("funit/list/search", {"q": query})
    
    def get_fund_groups(self) -> dict:
        """Get fund groups"""
        return self._request("funit/list/fgroup")
    
    # =========================================================================
    # DONATIONS
    # =========================================================================
    
    def get_donations(self, limit: int = 100, offset: int = 0) -> dict:
        """Get donations list"""
        return self._request("donation/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_donations_by_profile(self, profile_id: int) -> dict:
        """Get donations for a specific profile"""
        return self._request("donation/list", {"profile_id": profile_id})
    
    def get_all_donations(self, max_iterations: int = 100) -> list:
        """Get all donations across all pages using offset pagination"""
        all_donations = []
        offset = 0
        limit = 100
        
        for _ in range(max_iterations):
            result = self.get_donations(limit=limit, offset=offset)
            
            if not result.get("success"):
                logger.error(f"Failed to get donations at offset {offset}")
                break
            
            data = result.get("data", {})
            donations = data.get("results", [])
            
            if not donations:
                break
            
            all_donations.extend(donations)
            
            # Check if we got fewer than limit (last page)
            if len(donations) < limit:
                break
            
            offset += limit
        
        logger.info(f"Retrieved {len(all_donations)} total donations")
        return all_donations
    
    # =========================================================================
    # GRANTS
    # =========================================================================
    
    def get_grants(self, limit: int = 100, offset: int = 0) -> dict:
        """Get grants list"""
        return self._request("grant/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    # =========================================================================
    # EVENTS
    # =========================================================================
    
    def get_event_dates(self, limit: int = 100) -> dict:
        """Get event dates list (campaigns)"""
        return self._request("event/list/dates", {"view_limit": limit})
    
    def get_event_date(self, event_date_id: int) -> dict:
        """Get specific event date details including attendees"""
        return self._request("event/display/eventdate", {"event_date_id": event_date_id})
    
    def get_event(self, event_id: int) -> dict:
        """Get specific event details"""
        return self._request("event/display", {"event_id": event_id})
    
    # =========================================================================
    # VOUCHERS
    # =========================================================================
    
    def get_vouchers(self, limit: int = 100, offset: int = 0) -> dict:
        """Get vouchers list"""
        return self._request("voucher/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    # =========================================================================
    # ACCOUNTS
    # =========================================================================
    
    def get_accounts(self, limit: int = 100) -> dict:
        """Get accounts list"""
        return self._request("account/list", {"view_limit": limit})
    
    # =========================================================================
    # TASKS
    # =========================================================================
    
    def get_tasks(self, limit: int = 100) -> dict:
        """Get tasks list"""
        return self._request("task/list", {"view_limit": limit})
