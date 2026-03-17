"""
CSuite Client
=============
Client for CSuite Fund Accounting API with HMAC-SHA256 authentication.

Jidhr v1.3 - Complete client covering:
- Profile CRUD (Kods' DAF workflow)
- Fund CRUD + fee types (Muhi's fee calculations)
- Grant queries with date filtering (quarterly reporting)
- Donation queries with date filtering (Ramadan comparisons)
- Check tracking (Muhi's uncashed check reports)
- Voucher lookups (grant disbursement tracking)
- Event management (Lisa's event workflows)
- Task management (CSuite-side tasks)
- Account + investment strategy lookups
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
    
    # =========================================================================
    # AUTHENTICATION & HTTP
    # =========================================================================
    
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
        """Make authenticated POST request to CSuite API
        
        All CSuite API calls are POST with HMAC-SHA256 signature.
        
        Returns:
            dict with keys: success (bool), data (dict/None), error (str/None),
                           errors (list), messages (list)
        """
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
    # PAGINATION HELPER
    # =========================================================================
    
    def _get_all_pages(self, endpoint: str, data: dict = None,
                       max_iterations: int = 200, batch_size: int = 100) -> list:
        """Fetch all pages of a paginated endpoint.
        
        CSuite uses view_offset (not cur_page) for pagination.
        
        Args:
            endpoint: API endpoint
            data: Additional request data (filters, etc.)
            max_iterations: Safety limit to prevent infinite loops
            batch_size: Records per page
            
        Returns:
            list of all result objects across all pages
        """
        all_results = []
        offset = 0
        base_data = data or {}
        
        for _ in range(max_iterations):
            request_data = {
                **base_data,
                "view_limit": batch_size,
                "view_offset": offset
            }
            
            result = self._request(endpoint, request_data)
            
            if not result.get("success"):
                logger.error(f"Pagination failed at offset {offset}: {result.get('error')}")
                break
            
            results = result.get("data", {}).get("results", [])
            if not results:
                break
            
            all_results.extend(results)
            
            if len(results) < batch_size:
                break
            
            offset += batch_size
            
            # Log progress every 500 records
            if len(all_results) % 500 == 0:
                logger.info(f"Fetched {len(all_results)} records from {endpoint}...")
        
        logger.info(f"Retrieved {len(all_results)} total records from {endpoint}")
        return all_results
    
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
        """Search profiles by name
        
        Note: Returns mixed results - profiles AND funds matching the query.
        Filter by result['object'] == 'profile' for profiles only.
        """
        return self._request("profile/list/search", {"q": query})
    
    def get_all_profiles(self, max_iterations: int = 200) -> list:
        """Get all profiles across all pages"""
        return self._get_all_pages("profile/list", max_iterations=max_iterations)
    
    def create_individual_profile(self, first_name: str, last_name: str,
                                   email: str = None, phone: str = None,
                                   address: str = None, **kwargs) -> dict:
        """Create an individual profile in CSuite.
        
        Used by: DAF/Endowment inquiry workflow (Kods)
        
        Args:
            first_name: First name (required)
            last_name: Last name (required)
            email: Primary email
            phone: Primary phone number
            address: Primary address
            **kwargs: Additional profile fields
            
        Returns:
            dict with 'data': {'profile_id': int} on success
        """
        data = {
            "first_name": first_name,
            "last_name": last_name,
        }
        if email:
            data["primary_email"] = email
        if phone:
            data["primary_phone_number"] = phone
        if address:
            data["primary_address_string"] = address
        data.update(kwargs)
        
        logger.info(f"Creating individual profile: {first_name} {last_name}")
        return self._request("profile/create/individual", data)
    
    def create_org_profile(self, organization: str, email: str = None,
                           phone: str = None, **kwargs) -> dict:
        """Create an organization profile in CSuite.
        
        Used by: Nonprofit/org onboarding workflows (Ola)
        
        Args:
            organization: Organization name (required)
            email: Primary email
            phone: Primary phone number
            **kwargs: Additional profile fields
            
        Returns:
            dict with 'data': {'profile_id': int} on success
        """
        data = {"organization": organization}
        if email:
            data["primary_email"] = email
        if phone:
            data["primary_phone_number"] = phone
        data.update(kwargs)
        
        logger.info(f"Creating org profile: {organization}")
        return self._request("profile/create/org", data)
    
    def create_household_profile(self, household: str, **kwargs) -> dict:
        """Create a household profile in CSuite.
        
        Args:
            household: Household name (required)
            **kwargs: Additional profile fields
            
        Returns:
            dict with 'data': {'profile_id': int} on success
        """
        data = {"household": household}
        data.update(kwargs)
        
        logger.info(f"Creating household profile: {household}")
        return self._request("profile/create/household", data)
    
    def edit_profile(self, profile_id: int, **kwargs) -> dict:
        """Edit an existing profile.
        
        Args:
            profile_id: CSuite profile ID
            **kwargs: Fields to update (e.g., primary_email, primary_phone_number)
            
        Returns:
            dict with success status
        """
        data = {"profile_id": profile_id, **kwargs}
        logger.info(f"Editing profile {profile_id}: {list(kwargs.keys())}")
        return self._request("profile/edit", data)
    
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
        """Get specific fund details including balance"""
        return self._request("funit/display", {"funit_id": fund_id})
    
    def search_funds(self, query: str) -> dict:
        """Search funds by name"""
        return self._request("funit/list/search", {"q": query})
    
    def get_all_funds(self, max_iterations: int = 10) -> list:
        """Get all funds across all pages"""
        return self._get_all_pages("funit/list", max_iterations=max_iterations)
    
    def create_fund(self, name: str, fgroup_id: int,
                    cash_account_id: int = None, **kwargs) -> dict:
        """Create a new fund in CSuite.
        
        Used by: DAF/Endowment inquiry workflow (Kods)
        
        Args:
            name: Fund name (required) - e.g., "Smith Family Fund-(DAF0XXX)"
            fgroup_id: Fund group ID (required) - 1002 for DAF, use Config.FUND_GROUP_*
            cash_account_id: Cash account (defaults to Config.DEFAULT_CASH_ACCOUNT_ID)
            **kwargs: Additional fund fields (e.g., fund_type_id, invest_id)
            
        Returns:
            dict with 'data': {'funit_id': int} on success
        """
        data = {
            "name": name,
            "fgroup_id": fgroup_id,
            "cash_account_id": cash_account_id or Config.DEFAULT_CASH_ACCOUNT_ID,
        }
        data.update(kwargs)
        
        logger.info(f"Creating fund: {name} (group: {fgroup_id})")
        return self._request("funit/create", data)
    
    def get_fund_groups(self) -> dict:
        """Get fund groups (DAF, Endowment, Fiscal Sponsorship, etc.)"""
        return self._request("funit/list/fgroup")
    
    def get_fund_types(self) -> dict:
        """Get fund types (Permanently Restricted, Temporarily Restricted, etc.)"""
        return self._request("funit/list/fundtype")
    
    def get_fund_fee_types(self) -> dict:
        """Get fund admin fee types and schedules.
        
        Used by: Fee calculation on fund balances (Muhi)
        
        Returns fee structure including:
        - admin_fee_type_name: e.g., "Fund Admin Fees"
        - admin_fee_apply_fee: "quarterly", "annually", etc.
        - admin_fee_min_fee: Minimum fee amount
        - admin_fee_percent: Fee percentage (if flat rate)
        - admin_fee_type_type: "percent_range", "flat", etc.
        - admin_fee_ladder: Whether fees are tiered
        - admin_fee_use_adb: Whether to use average daily balance
        """
        return self._request("funit/feetype")
    
    def get_fund_subgroups(self) -> dict:
        """Get fund subgroups"""
        return self._request("funit/list/fsubgroup")
    
    # =========================================================================
    # DONATIONS
    # =========================================================================
    
    def get_donations(self, limit: int = 100, offset: int = 0) -> dict:
        """Get donations list"""
        return self._request("donation/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_donation(self, donation_id: int) -> dict:
        """Get specific donation details"""
        return self._request("donation/display", {"donation_id": donation_id})
    
    def get_donations_by_profile(self, profile_id: int) -> dict:
        """Get donations for a specific profile"""
        return self._request("donation/list", {"profile_id": profile_id})
    
    def get_donations_by_fund(self, funit_id: int, limit: int = 100, offset: int = 0) -> dict:
        """Get donations for a specific fund"""
        return self._request("donation/list", {
            "funit_id": funit_id,
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_all_donations(self, max_iterations: int = 300) -> list:
        """Get all donations across all pages (24,910+ records)
        
        Warning: This fetches a LOT of data. Use sparingly.
        For targeted queries, use get_donations_by_profile() or get_donations_by_fund().
        """
        return self._get_all_pages("donation/list", max_iterations=max_iterations)
    
    def get_donations_with_limit(self, limit: int = None) -> list:
        """Get donations with optional cap on total records.
        
        Used by: Donation sync, Ramadan comparisons, reporting
        
        Args:
            limit: Max total donations to fetch (None = all)
        """
        all_donations = []
        offset = 0
        batch_size = 100
        
        while True:
            result = self.get_donations(limit=batch_size, offset=offset)
            
            if not result.get("success"):
                logger.error(f"Failed to get donations at offset {offset}")
                break
            
            data = result.get("data", {})
            donations = data.get("results", [])
            
            if not donations:
                break
            
            all_donations.extend(donations)
            
            if limit and len(all_donations) >= limit:
                all_donations = all_donations[:limit]
                break
            
            if len(donations) < batch_size:
                break
            
            offset += batch_size
            
            if offset % 500 == 0:
                logger.info(f"Fetched {offset} donations so far...")
        
        logger.info(f"Retrieved {len(all_donations)} donations")
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
    
    def get_grant(self, grant_id: int) -> dict:
        """Get specific grant details"""
        return self._request("grant/display", {"grant_id": grant_id})
    
    def get_grants_by_fund(self, funit_id: int = None, fund_name_link_id: int = None,
                           limit: int = 100, offset: int = 0) -> dict:
        """Get grants for a specific fund.
        
        Used by: Fund activity summaries, grant reporting
        
        Args:
            funit_id: Fund unit ID
            fund_name_link_id: Fund name link ID (sometimes used instead of funit_id)
            limit: Records per page
            offset: Pagination offset
        """
        data = {
            "view_limit": limit,
            "view_offset": offset
        }
        if funit_id:
            data["funit_id"] = funit_id
        if fund_name_link_id:
            data["fund_name_link_id"] = fund_name_link_id
        return self._request("grant/list", data)
    
    def get_grants_by_profile(self, profile_id: int, limit: int = 100, offset: int = 0) -> dict:
        """Get grants associated with a specific profile"""
        return self._request("grant/list", {
            "profile_id": profile_id,
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_all_grants(self, max_iterations: int = 100) -> list:
        """Get all grants across all pages (5,338+ records)
        
        Used by: Quarterly grant reports, inactive fund analysis
        """
        return self._get_all_pages("grant/list", max_iterations=max_iterations)
    
    # =========================================================================
    # CHECKS
    # =========================================================================
    
    def get_checks(self, limit: int = 100, offset: int = 0) -> dict:
        """Get checks list.
        
        Used by: Uncashed check reports (Muhi)
        
        Check fields include:
        - check_id, check_num, check_date, amount
        - cleared (0/1): Whether the check has been cashed
        - voided (0/1), void_date, void_reason
        - account_name, account_id
        - is_electronic (0/1), memo
        """
        return self._request("check/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_check(self, check_id: int) -> dict:
        """Get specific check details"""
        return self._request("check/display", {"check_id": check_id})
    
    def get_all_checks(self, max_iterations: int = 60) -> list:
        """Get all checks across all pages (5,324+ records)"""
        return self._get_all_pages("check/list", max_iterations=max_iterations)
    
    def get_uncashed_checks(self, limit: int = 200) -> list:
        """Get checks that haven't been cleared (not cashed yet).
        
        Used by: Muhi's "which charities have cashed their checks" query
        
        Returns:
            list of check dicts where cleared == 0 and voided == 0
        """
        all_checks = self._get_all_pages("check/list", batch_size=200)
        
        uncashed = [
            c for c in all_checks
            if c.get("cleared") == 0 and c.get("voided") == 0
            and not c.get("unused", 0)
        ]
        
        logger.info(f"Found {len(uncashed)} uncashed checks out of {len(all_checks)} total")
        return uncashed
    
    # =========================================================================
    # VOUCHERS
    # =========================================================================
    
    def get_vouchers(self, limit: int = 100, offset: int = 0) -> dict:
        """Get vouchers list"""
        return self._request("voucher/list", {
            "view_limit": limit,
            "view_offset": offset
        })
    
    def get_voucher(self, voucher_id: int) -> dict:
        """Get specific voucher details"""
        return self._request("voucher/display", {"voucher_id": voucher_id})
    
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
    
    def create_event_date(self, event_id: int, **kwargs) -> dict:
        """Create a new event date.
        
        Args:
            event_id: Parent event ID (required)
            **kwargs: event_date, start_time, location, event_description, etc.
        """
        data = {"event_id": event_id, **kwargs}
        logger.info(f"Creating event date for event {event_id}")
        return self._request("event/create/eventdate", data)
    
    def edit_event_date(self, event_date_id: int, **kwargs) -> dict:
        """Edit an existing event date.
        
        Args:
            event_date_id: Event date ID (required)
            **kwargs: Fields to update
        """
        data = {"event_date_id": event_date_id, **kwargs}
        return self._request("event/edit/eventdate", data)
    
    # =========================================================================
    # TASKS
    # =========================================================================
    
    def get_tasks(self, limit: int = 100) -> dict:
        """Get CSuite tasks list"""
        return self._request("task/list", {"view_limit": limit})
    
    def get_task(self, task_id: int) -> dict:
        """Get specific task details"""
        return self._request("task/display", {"task_id": task_id})
    
    def create_task(self, name: str, employee_id: int, due_date: str = None,
                    description: str = None, **kwargs) -> dict:
        """Create a task in CSuite.
        
        Args:
            name: Task name (required)
            employee_id: Assigned employee's name_link_id (required)
            due_date: Due date in YYYY-MM-DD format
            description: Task description
            **kwargs: Additional task fields
        """
        data = {"name": name, "employee_id": employee_id}
        if due_date:
            data["due_date"] = due_date
        if description:
            data["task_description"] = description
        data.update(kwargs)
        
        logger.info(f"Creating CSuite task: {name}")
        return self._request("task/create", data)
    
    def complete_task(self, task_id: int = None, task_guid: str = None) -> dict:
        """Mark a CSuite task as complete.
        
        Args:
            task_id: Task ID (use one or the other)
            task_guid: Task GUID (use one or the other)
        """
        data = {}
        if task_id:
            data["task_id"] = task_id
        if task_guid:
            data["task_guid"] = task_guid
        return self._request("task/edit/complete", data)
    
    # =========================================================================
    # ACCOUNTS
    # =========================================================================
    
    def get_accounts(self, limit: int = 100) -> dict:
        """Get accounts list"""
        return self._request("account/list", {"view_limit": limit})
    
    def get_investment_strategies(self) -> dict:
        """Get investment strategies (e.g., Saturna)"""
        return self._request("account/list/strategy")
    
    # =========================================================================
    # ACCOUNTS PAYABLE
    # =========================================================================
    
    def get_ap_summary(self) -> dict:
        """Get accounts payable summary by vendor.
        
        Returns AP and SP (scholarship payable) totals with aging buckets
        (30/60/90/91+ days).
        """
        return self._request("ap/list")
    
    def get_ap_open_vouchers(self) -> dict:
        """Get open vouchers that can be paid"""
        return self._request("ap/list/openvouchers")
    
    # =========================================================================
    # VENDORS & GRANTEES
    # =========================================================================
    
    def make_vendor(self, profile_id: int) -> dict:
        """Make a profile a vendor (required before creating vouchers for them)"""
        return self._request("vendor/create", {"profile_id": profile_id})
    
    def make_grantee(self, profile_id: int) -> dict:
        """Make a profile a grantee (required before creating grants for them)"""
        return self._request("grantee/create", {"profile_id": profile_id})
    
    # =========================================================================
    # GRANT TYPES & DISTRIBUTION TYPES
    # =========================================================================
    
    def get_grant_types(self) -> dict:
        """Get grant types (NTEE categories: Education, Human Services, etc.)"""
        return self._request("grant_type/list")
    
    def get_distribution_types(self) -> dict:
        """Get distribution types"""
        return self._request("distribution/list/type")
