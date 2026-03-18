"""
HubSpot Client
==============
Client for HubSpot CRM and Marketing APIs.

Jidhr v1.3 - Complete client covering:
- Contact CRUD + search + activity timeline (donor prep, journey tracking)
- Notes/Engagements (call/meeting note logging)
- Form submissions (DAF/Endowment inquiry processing)
- Tickets (status tracking, closing, deep links - Kods/Shazeen)
- Marketing Emails (draft creation with templates)
- Social Media (post/schedule across platforms)
- Tasks (create/list)
- Marketing Events (sync from CSuite)
- Subscriptions (newsletter sync)
- Companies
- Campaigns
- Owners
"""

import json
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
    
    def _parse_response(self, response, method: str, endpoint: str) -> dict:
        """Parse a HubSpot API response safely.

        Handles empty bodies, non-JSON responses, and error status codes
        without crashing the worker.
        """
        status = response.status_code
        logger.info(f"HubSpot {method} {endpoint}: {status}")

        if status >= 400:
            body = response.text[:300] if response.text else "(empty)"
            logger.warning(f"HubSpot {method} {endpoint} failed: {status} | {body}")

        if not response.text or not response.text.strip():
            if 200 <= status < 300:
                return {"status_code": status}
            return {"error": f"HubSpot returned {status} with empty body", "status_code": status}

        try:
            return response.json()
        except (ValueError, json.JSONDecodeError):
            snippet = response.text[:200]
            logger.error(f"HubSpot {method} {endpoint}: non-JSON response ({status}): {snippet}")
            return {"error": f"Non-JSON response ({status}): {snippet}", "status_code": status}

    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Make a GET request to HubSpot API"""
        if not self.access_token:
            logger.error("HubSpot access token not configured")
            return {"error": "HubSpot access token not configured"}

        url = f"{self.base_url}/{endpoint}"
        logger.info(f"HubSpot GET: {endpoint} | params: {params}")

        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            return self._parse_response(response, "GET", endpoint)
        except requests.exceptions.RequestException as e:
            logger.error(f"HubSpot GET {endpoint} error: {e}")
            return {"error": str(e)}

    def _post(self, endpoint: str, data: dict = None) -> dict:
        """Make a POST request to HubSpot API"""
        if not self.access_token:
            logger.error("HubSpot access token not configured")
            return {"error": "HubSpot access token not configured"}

        url = f"{self.base_url}/{endpoint}"
        logger.info(f"HubSpot POST: {endpoint}")

        try:
            response = requests.post(url, headers=self.headers, json=data, timeout=30)
            return self._parse_response(response, "POST", endpoint)
        except requests.exceptions.RequestException as e:
            logger.error(f"HubSpot POST {endpoint} error: {e}")
            return {"error": str(e)}

    def _put(self, endpoint: str, data: dict = None) -> dict:
        """Make a PUT request to HubSpot API"""
        if not self.access_token:
            logger.error("HubSpot access token not configured")
            return {"error": "HubSpot access token not configured"}

        url = f"{self.base_url}/{endpoint}"
        logger.info(f"HubSpot PUT: {endpoint}")

        try:
            response = requests.put(url, headers=self.headers, json=data, timeout=30)
            return self._parse_response(response, "PUT", endpoint)
        except requests.exceptions.RequestException as e:
            logger.error(f"HubSpot PUT {endpoint} error: {e}")
            return {"error": str(e)}

    def _patch(self, endpoint: str, data: dict = None) -> dict:
        """Make a PATCH request to HubSpot API"""
        if not self.access_token:
            logger.error("HubSpot access token not configured")
            return {"error": "HubSpot access token not configured"}

        url = f"{self.base_url}/{endpoint}"
        logger.info(f"HubSpot PATCH: {endpoint}")

        try:
            response = requests.patch(url, headers=self.headers, json=data, timeout=30)
            return self._parse_response(response, "PATCH", endpoint)
        except requests.exceptions.RequestException as e:
            logger.error(f"HubSpot PATCH {endpoint} error: {e}")
            return {"error": str(e)}
    
    def _delete(self, endpoint: str) -> dict:
        """Make a DELETE request to HubSpot API"""
        if not self.access_token:
            logger.error("HubSpot access token not configured")
            return {"error": "HubSpot access token not configured"}

        url = f"{self.base_url}/{endpoint}"
        logger.info(f"HubSpot DELETE: {endpoint}")

        try:
            response = requests.delete(url, headers=self.headers, timeout=30)
            return self._parse_response(response, "DELETE", endpoint)
        except requests.exceptions.RequestException as e:
            logger.error(f"HubSpot DELETE {endpoint} error: {e}")
            return {"error": str(e)}
    
    # =========================================================================
    # CONTACTS
    # =========================================================================
    
    def get_contacts(self, limit: int = 10, properties: list = None) -> dict:
        """Get contacts list with optional properties.
        
        Args:
            limit: Number of contacts to return
            properties: List of property names to include
        """
        params = {"limit": limit}
        if properties:
            params["properties"] = ",".join(properties)
        return self._get("crm/v3/objects/contacts", params)
    
    def get_contact(self, contact_id: str, properties: list = None) -> dict:
        """Get contact by ID with optional properties"""
        params = {}
        if properties:
            params["properties"] = ",".join(properties)
        return self._get(f"crm/v3/objects/contacts/{contact_id}", params)
    
    def search_contacts(self, query: str, limit: int = 10) -> dict:
        """Search contacts by query string (searches name, email, phone, etc.)"""
        return self._post("crm/v3/objects/contacts/search", {
            "query": query,
            "limit": limit
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

    def search_contacts_by_csuite_fund_id(self, fund_id: str, limit: int = 20) -> dict:
        """Search contacts by CSuite fund ID (custom property).

        Used by: Fund-associated contacts lookup
        ("Find all contacts associated with the Smith Family Fund")

        Args:
            fund_id: CSuite funit_id as a string
            limit: Max contacts to return

        Returns:
            dict with 'results' containing contact objects with
            firstname, lastname, email, csuite_profile_id, csuite_fund_id
        """
        return self._post("crm/v3/objects/contacts/search", {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "csuite_fund_id",
                    "operator": "EQ",
                    "value": str(fund_id)
                }]
            }],
            "properties": [
                "firstname", "lastname", "email", "phone",
                "csuite_profile_id", "csuite_fund_id"
            ],
            "limit": limit
        })
    
    def create_contact(self, properties: dict) -> dict:
        """Create a new contact in HubSpot.
        
        Used by: DAF/Endowment inquiry workflow, contact creation from CSuite
        
        Args:
            properties: Contact properties dict. Common properties:
                - firstname, lastname, email, phone
                - company, jobtitle
                - csuite_profile_id (custom property for linking)
                
        Returns:
            Created contact object with 'id'
            
        Example:
            result = client.create_contact({
                "firstname": "Jane",
                "lastname": "Doe",
                "email": "jane@example.com",
                "csuite_profile_id": "19879"
            })
        """
        return self._post("crm/v3/objects/contacts", {"properties": properties})
    
    def update_contact(self, contact_id: str, properties: dict) -> dict:
        """Update contact properties by ID"""
        return self._patch(f"crm/v3/objects/contacts/{contact_id}", {
            "properties": properties
        })
    
    def update_contact_by_email(self, email: str, properties: dict) -> dict:
        """Find contact by email and update properties.
        
        Used by: Donation sync, DAF workflow linking
        
        Returns error dict if contact not found.
        """
        search_result = self.search_contact_by_email(email)
        
        if "error" in search_result:
            return search_result
        
        results = search_result.get("results", [])
        if not results:
            return {"error": f"Contact not found: {email}"}
        
        contact_id = results[0]["id"]
        return self.update_contact(contact_id, properties)
    
    # =========================================================================
    # CONTACT ACTIVITY & ENGAGEMENTS
    # =========================================================================
    
    def get_contact_engagements(self, contact_id: str, limit: int = 20) -> dict:
        """Get engagement history for a contact (emails, calls, meetings, notes).
        
        Used by: Donor call prep (Muhi/Shazeen/Ola), "donors not contacted in 6+ months"
        
        Args:
            contact_id: HubSpot contact ID
            limit: Number of engagements to return
            
        Returns:
            dict with 'results' containing engagement objects
        """
        return self._get(
            f"crm/v3/objects/contacts/{contact_id}/associations/engagements",
            {"limit": limit}
        )
    
    def get_contact_notes(self, contact_id: str, limit: int = 10) -> dict:
        """Get notes associated with a contact.
        
        Used by: Donor call prep, reviewing past interactions
        """
        return self._post("crm/v3/objects/notes/search", {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "associations.contact",
                    "operator": "EQ",
                    "value": contact_id
                }]
            }],
            "properties": ["hs_note_body", "hs_timestamp", "hubspot_owner_id"],
            "sorts": [{"propertyName": "hs_timestamp", "direction": "DESCENDING"}],
            "limit": limit
        })
    
    def get_contact_emails(self, contact_id: str, limit: int = 10) -> dict:
        """Get email engagements associated with a contact.
        
        Used by: Donor call prep, communication history
        """
        return self._post("crm/v3/objects/emails/search", {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "associations.contact",
                    "operator": "EQ",
                    "value": contact_id
                }]
            }],
            "properties": ["hs_email_subject", "hs_email_text", "hs_email_direction",
                          "hs_timestamp", "hs_email_status"],
            "sorts": [{"propertyName": "hs_timestamp", "direction": "DESCENDING"}],
            "limit": limit
        })
    
    def get_recently_modified_contacts(self, limit: int = 50, properties: list = None) -> dict:
        """Get recently modified contacts.
        
        Used by: Quick overview of recent activity
        """
        params = {"limit": limit, "sort": "-hs_lastmodifieddate"}
        if properties:
            params["properties"] = ",".join(properties)
        return self._get("crm/v3/objects/contacts", params)
    
    # =========================================================================
    # NOTES (Call/Meeting Note Logging)
    # =========================================================================
    
    def create_note(self, body: str, contact_id: str = None,
                    owner_id: str = None, timestamp: datetime = None) -> dict:
        """Create a note, optionally associated with a contact.
        
        Used by: Note logging intent (Muhi/Shazeen/Ola) -
                 "Log my call notes without opening HubSpot"
        
        Args:
            body: Note content (plain text or HTML)
            contact_id: Optional HubSpot contact ID to associate with
            owner_id: Optional HubSpot owner ID (who wrote the note)
            timestamp: Optional timestamp (defaults to now)
            
        Returns:
            Created note object with 'id'
            
        Example:
            result = client.create_note(
                body="Called about DAF contribution. Will follow up next week.",
                contact_id="12345"
            )
        """
        ts = timestamp or datetime.now()
        ts_ms = str(int(ts.timestamp() * 1000))
        
        properties = {
            "hs_note_body": body,
            "hs_timestamp": ts_ms,
        }
        
        if owner_id:
            properties["hubspot_owner_id"] = owner_id
        
        # Create the note
        result = self._post("crm/v3/objects/notes", {"properties": properties})
        
        # Associate with contact if provided
        if contact_id and "id" in result:
            note_id = result["id"]
            self._associate_objects("notes", note_id, "contacts", contact_id)
            logger.info(f"Created note {note_id} associated with contact {contact_id}")
        
        return result
    
    def create_call_note(self, body: str, contact_id: str = None,
                         owner_id: str = None, duration_ms: int = None) -> dict:
        """Create a call engagement record.
        
        Used by: "Log my call with [donor name]"
        
        Args:
            body: Call notes/description
            contact_id: HubSpot contact ID to associate with
            owner_id: HubSpot owner ID
            duration_ms: Call duration in milliseconds
        """
        ts_ms = str(int(datetime.now().timestamp() * 1000))
        
        properties = {
            "hs_call_body": body,
            "hs_timestamp": ts_ms,
            "hs_call_status": "COMPLETED",
            "hs_call_direction": "OUTBOUND",
        }
        
        if owner_id:
            properties["hubspot_owner_id"] = owner_id
        if duration_ms:
            properties["hs_call_duration"] = str(duration_ms)
        
        result = self._post("crm/v3/objects/calls", {"properties": properties})
        
        if contact_id and "id" in result:
            call_id = result["id"]
            self._associate_objects("calls", call_id, "contacts", contact_id)
            logger.info(f"Created call {call_id} associated with contact {contact_id}")
        
        return result
    
    def create_meeting_note(self, title: str, body: str, contact_id: str = None,
                            owner_id: str = None, start_time: datetime = None,
                            end_time: datetime = None) -> dict:
        """Create a meeting engagement record.
        
        Used by: "Log my meeting with [donor name]"
        
        Args:
            title: Meeting title
            body: Meeting notes
            contact_id: HubSpot contact ID
            owner_id: HubSpot owner ID
            start_time: Meeting start time
            end_time: Meeting end time
        """
        now = datetime.now()
        start = start_time or now
        end = end_time or now
        
        properties = {
            "hs_meeting_title": title,
            "hs_meeting_body": body,
            "hs_timestamp": str(int(start.timestamp() * 1000)),
            "hs_meeting_start_time": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "hs_meeting_end_time": end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "hs_meeting_outcome": "COMPLETED",
        }
        
        if owner_id:
            properties["hubspot_owner_id"] = owner_id
        
        result = self._post("crm/v3/objects/meetings", {"properties": properties})
        
        if contact_id and "id" in result:
            meeting_id = result["id"]
            self._associate_objects("meetings", meeting_id, "contacts", contact_id)
            logger.info(f"Created meeting {meeting_id} associated with contact {contact_id}")
        
        return result
    
    def _associate_objects(self, from_type: str, from_id: str,
                           to_type: str, to_id: str) -> dict:
        """Associate two HubSpot objects (e.g., note ↔ contact).
        
        Uses the v4 associations API.
        """
        return self._post(
            f"crm/v4/objects/{from_type}/{from_id}/associations/{to_type}/{to_id}",
            [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 0}]
        )
    
    # =========================================================================
    # COMPANIES
    # =========================================================================
    
    def get_companies(self, limit: int = 10) -> dict:
        """Get companies list"""
        return self._get("crm/v3/objects/companies", {"limit": limit})
    
    def search_companies(self, query: str) -> dict:
        """Search companies by name"""
        return self._post("crm/v3/objects/companies/search", {
            "query": query,
            "limit": 10
        })
    
    # =========================================================================
    # FORMS & SUBMISSIONS
    # =========================================================================
    
    def get_forms(self, limit: int = 10) -> dict:
        """Get forms list"""
        return self._get("marketing/v3/forms", {"limit": limit})
    
    def get_form_submissions(self, form_id: str, limit: int = 50) -> dict:
        """Get form submissions for a specific form.
        
        Used by: DAF/Endowment inquiry processing (Kods),
                 Monthly DAF inquiry summaries, Asset donation tracking
        
        Args:
            form_id: HubSpot form GUID (use Config.DAF_INQUIRY_FORM_ID, etc.)
            limit: Number of submissions to return
            
        Returns:
            dict with 'results' containing submission objects.
            Each submission has 'values' dict with field name → value mappings.
        """
        return self._get(
            f"form-integrations/v1/submissions/forms/{form_id}",
            {"limit": limit}
        )
    
    def get_daf_inquiry_submissions(self, limit: int = 50) -> dict:
        """Get DAF Inquiry form submissions.
        
        Convenience method using the configured form ID.
        
        Used by: Kods' DAF creation workflow, monthly summaries
        """
        return self.get_form_submissions(Config.DAF_INQUIRY_FORM_ID, limit)
    
    def get_endowment_inquiry_submissions(self, limit: int = 50) -> dict:
        """Get Endowment Inquiry form submissions.
        
        Convenience method using the configured form ID.
        
        Used by: Ola's endowment workflows
        """
        return self.get_form_submissions(Config.ENDOWMENT_INQUIRY_FORM_ID, limit)
    
    def get_asset_donation_submissions(self, limit: int = 50) -> dict:
        """Get Asset Donation form submissions.
        
        Convenience method using the configured form ID.
        """
        return self.get_form_submissions(Config.ASSET_DONATION_FORM_ID, limit)

    def get_investment_request_submissions(self, limit: int = 50) -> dict:
        """Get Investment Request form submissions (Andalus).

        Used by: Nora's investment request compilation
        """
        return self.get_form_submissions(Config.INVESTMENT_REQUEST_FORM_ID, limit)

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
        """Create or update a marketing event via PUT.

        HubSpot Marketing Events API requires PUT with externalAccountId
        and externalEventId in the URL path. POST does not work.

        Required in event_data:
        - eventName: str
        - externalEventId: str (unique ID for deduplication)

        Optional:
        - eventDescription, eventUrl, eventType
        - startDateTime, endDateTime (ISO 8601)
        - eventOrganizer: str
        - customProperties: list of {name, value}
        """
        external_account_id = "jidhr-amcf"
        external_event_id = event_data.get("externalEventId")
        if not external_event_id:
            return {"error": "externalEventId is required"}

        # PUT requires externalEventId in both URL and body
        endpoint = f"marketing/v3/marketing-events/events/{external_event_id}"
        event_data["externalAccountId"] = external_account_id
        event_data["externalEventId"] = external_event_id

        return self._put(endpoint, event_data)
    
    def search_marketing_event_by_external_id(self, external_id: str) -> dict:
        """Search for marketing event by external ID"""
        return self._get(f"marketing/v3/marketing-events/external/{external_id}")
    
    # =========================================================================
    # MARKETING EMAILS
    # =========================================================================
    
    def get_marketing_emails(self, limit: int = 20) -> dict:
        """Get list of marketing emails"""
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
        """Create a marketing email draft in HubSpot.

        Uses a two-step approach for DnD templates:
        1. Create the email with the template (initializes layout)
        2. PATCH the email to inject the body content into the main widget

        If no template is found, falls back to a regular (non-template) email
        with the body content set directly.

        Args:
            name: Internal name (e.g., "DAF Portal Launch - Jan 2026")
            subject: Email subject line
            body_html: HTML content for the email body
            template: Template key - "amcf", "newsletter", or "giving circle"

        Returns:
            dict with created email details including 'id' and 'edit_url'
        """
        template_key = template.lower().strip()
        template_path = self.EMAIL_TEMPLATES.get(template_key)

        # Step 1: Create the email
        if template_path:
            # DnD template — create with template, patch body after
            create_payload = {
                "name": name,
                "subject": subject,
                "templatePath": template_path,
            }
            logger.info(f"Creating email draft: {name} with template: {template_path}")
        else:
            # No template — create a regular email with body inline
            create_payload = {
                "name": name,
                "subject": subject,
                "subcategory": "regular_email",
                "content": {
                    "body": body_html,
                },
            }
            logger.info(f"Creating email draft: {name} (no template)")

        logger.info(f"Email create payload keys: {list(create_payload.keys())}")
        result = self._post("marketing/v3/emails", create_payload)

        if "error" in result or "id" not in result:
            logger.error(f"Email create failed: {result}")
            return result

        email_id = result["id"]

        # Step 2: PATCH the body content into the email.
        # For DnD templates, we first GET the email to discover the actual
        # widget IDs, then patch the correct widget. If that fails, we try
        # direct body fields as fallback.
        patch_endpoint = f"marketing/v3/emails/{email_id}"
        body_set = False

        if template_path:
            # Try to discover widget IDs from the created email
            try:
                email_detail = self._get(patch_endpoint)
                widgets = (email_detail.get("content") or {}).get("widgets") or {}
                logger.info(f"Email {email_id} widget IDs: {list(widgets.keys())}")

                if widgets:
                    # Find the first rich_text widget (that's the body)
                    target_widget_id = None
                    for wid, wdata in widgets.items():
                        if isinstance(wdata, dict) and wdata.get("type") == "rich_text":
                            target_widget_id = wid
                            break
                    # Fall back to hs_email_body or first widget
                    if not target_widget_id:
                        target_widget_id = "hs_email_body" if "hs_email_body" in widgets else list(widgets.keys())[0]

                    patch_payload = {
                        "content": {
                            "widgets": {
                                target_widget_id: {
                                    "body": {"html": body_html},
                                    "type": "rich_text",
                                }
                            }
                        }
                    }
                    logger.info(f"Patching widget '{target_widget_id}' with {len(body_html)} chars")
                    patch_result = self._patch(patch_endpoint, patch_payload)
                    body_set = patch_result and "error" not in patch_result
                    if body_set:
                        logger.info(f"Email {email_id} body patched via widget '{target_widget_id}'")
                    else:
                        logger.warning(f"Widget patch failed: {patch_result}")
            except Exception as e:
                logger.warning(f"Widget discovery failed for {email_id}: {e}")

        # Fallback approaches if widget patch didn't work
        if not body_set:
            for field_name in ["body", "htmlBody"]:
                fallback_payload = {"content": {field_name: body_html}}
                logger.info(f"Trying fallback: content.{field_name} ({len(body_html)} chars)")
                patch_result = self._patch(patch_endpoint, fallback_payload)
                if patch_result and "error" not in patch_result:
                    logger.info(f"Email {email_id} body set via content.{field_name}")
                    body_set = True
                    break
                else:
                    logger.warning(f"Fallback content.{field_name} failed: {patch_result}")

        if not body_set:
            logger.error(f"Could not set body content for email {email_id} — all approaches failed")

        result["edit_url"] = (
            f"https://app-na2.hubspot.com/email/"
            f"{Config.HUBSPOT_PORTAL_ID}/edit/{email_id}/content"
        )

        return result
    
    # =========================================================================
    # SUBSCRIPTIONS (Email/Newsletter)
    # =========================================================================
    
    def get_subscription_status(self, email: str) -> dict:
        """Get email subscription status for a contact"""
        return self._get(f"communication-preferences/v3/status/email/{email}")
    
    def subscribe_contact(self, email: str, subscription_id: str,
                          legal_basis: str = "LEGITIMATE_INTEREST_CLIENT") -> dict:
        """Subscribe a contact to a subscription type.
        
        Args:
            email: Contact's email address
            subscription_id: HubSpot subscription ID
            legal_basis: Legal basis for subscription
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
    
    def _get_channel_key(self, platform: str) -> str:
        """Get the channel key for a platform.
        
        Args:
            platform: Friendly name like "facebook", "twitter", "linkedin", "instagram"
            
        Returns:
            Channel key like "FacebookPage:1159312454102818" or None
        """
        if self._social_channels_cache is None:
            channels_response = self.get_social_channels()
            if isinstance(channels_response, list):
                self._social_channels_cache = channels_response
            else:
                self._social_channels_cache = []
        
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
        """Create a social media post (draft, scheduled, or publish now).
        
        Args:
            platform: "facebook", "twitter", "linkedin", or "instagram"
            content: The post text/message
            link_url: Optional URL to include
            photo_url: Optional image URL (must be publicly accessible)
            schedule_time: datetime to schedule (None = draft, "now" = immediate)
            campaign_guid: Optional HubSpot campaign GUID
        """
        channel_key = self._get_channel_key(platform)
        if not channel_key:
            available = ", ".join(self.SOCIAL_PLATFORMS.keys())
            return {"error": f"Channel not found for '{platform}'. Available: {available}"}
        
        payload = {
            "channelKeys": [channel_key],
            "content": {
                "body": content
            }
        }
        
        if link_url:
            payload["content"]["linkUrl"] = link_url
        if photo_url:
            payload["content"]["photoUrl"] = photo_url
        if campaign_guid:
            payload["campaignGuid"] = campaign_guid
        
        if schedule_time:
            if isinstance(schedule_time, datetime):
                timestamp_ms = int(schedule_time.timestamp() * 1000)
                payload["triggerAt"] = timestamp_ms
            elif schedule_time == "now":
                payload["triggerAt"] = int(datetime.now().timestamp() * 1000)
        
        logger.info(f"Creating social post for {platform}: {content[:50]}...")
        return self._post("broadcast/v1/broadcasts", payload)
    
    def get_available_social_platforms(self) -> list:
        """Get list of connected social platforms.
        
        Returns:
            List of platform names (e.g., ["facebook", "twitter", "linkedin"])
        """
        channels_response = self.get_social_channels()
        
        if not isinstance(channels_response, list):
            return []
        
        type_to_name = {v: k for k, v in self.SOCIAL_PLATFORMS.items()}
        
        connected = []
        seen_types = set()
        
        for channel in channels_response:
            channel_type = channel.get("channelType")
            if channel_type and channel_type not in seen_types:
                seen_types.add(channel_type)
                name = type_to_name.get(channel_type, channel_type)
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
    # TASKS
    # =========================================================================
    
    def get_tasks(self, limit: int = 20, owner_id: str = None) -> dict:
        """Get tasks list.
        
        Args:
            limit: Number of tasks to return
            owner_id: Optional owner ID to filter by
        """
        params = {
            "limit": limit,
            "properties": "hs_task_subject,hs_task_body,hs_task_status,hs_task_priority,hs_timestamp,hubspot_owner_id"
        }
        
        if owner_id:
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
        """Create a task (raw properties)"""
        return self._post("crm/v3/objects/tasks", {"properties": properties})
    
    def create_task_simple(
        self,
        subject: str,
        body: str = None,
        priority: str = "MEDIUM",
        due_date: datetime = None,
        owner_id: str = None
    ) -> dict:
        """Create a task with simple parameters.
        
        Args:
            subject: Task title
            body: Optional description
            priority: "LOW", "MEDIUM", or "HIGH"
            due_date: Optional due date
            owner_id: Optional owner to assign to
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
        """Get tickets list.
        
        Args:
            limit: Number of tickets to return
            properties: List of properties to include
        """
        if properties is None:
            properties = ['subject', 'content', 'hs_pipeline', 'hs_pipeline_stage',
                         'hs_ticket_priority', 'createdate', 'hs_lastmodifieddate']
        
        return self._get("crm/v3/objects/tickets", {
            "limit": limit,
            "properties": ",".join(properties)
        })
    
    def get_ticket(self, ticket_id: str, properties: list = None) -> dict:
        """Get a specific ticket by ID.
        
        Args:
            ticket_id: HubSpot ticket ID
            properties: List of property names to include
        """
        params = {}
        if properties:
            params["properties"] = ",".join(properties)
        return self._get(f"crm/v3/objects/tickets/{ticket_id}", params)
    
    def get_open_tickets(self, limit: int = 10) -> dict:
        """Get open tickets only.

        Used by: Shazeen's "what tickets are closed" query,
                 Kods' workflow to close tickets after DAF creation
        """
        return self._post("crm/v3/objects/tickets/search", {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "hs_pipeline_stage",
                    "operator": "EQ",
                    "value": "1"
                }]
            }],
            "properties": ['subject', 'content', 'hs_pipeline_stage',
                          'hs_ticket_priority', 'createdate'],
            "limit": limit
        })

    def get_closed_tickets(self, limit: int = 20) -> dict:
        """Get closed tickets only (pipeline stage 4).

        Used by: Shazeen's "what tickets are closed" query

        Args:
            limit: Number of closed tickets to return
        """
        return self._post("crm/v3/objects/tickets/search", {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "hs_pipeline_stage",
                    "operator": "EQ",
                    "value": "4"
                }]
            }],
            "properties": ['subject', 'content', 'hs_pipeline_stage',
                          'hs_ticket_priority', 'createdate', 'hs_lastmodifieddate'],
            "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "DESCENDING"}],
            "limit": limit
        })
    
    def update_ticket(self, ticket_id: str, properties: dict) -> dict:
        """Update a ticket's properties.
        
        Used by: Closing tickets after DAF workflow completion (Kods)
        
        Args:
            ticket_id: HubSpot ticket ID
            properties: Properties to update (e.g., {"hs_pipeline_stage": "4"} to close)
        """
        return self._patch(f"crm/v3/objects/tickets/{ticket_id}", {
            "properties": properties
        })
    
    def close_ticket(self, ticket_id: str) -> dict:
        """Close a ticket by setting its pipeline stage to closed.
        
        Used by: Kods' DAF creation workflow (final step)
        
        Note: Pipeline stage "4" is typically "Closed" in default HubSpot pipelines.
        Adjust if AMCF uses a custom pipeline.
        """
        return self.update_ticket(ticket_id, {"hs_pipeline_stage": "4"})
    
    def get_ticket_associations(self, ticket_id: str, to_type: str = "contacts") -> dict:
        """Get objects associated with a ticket (e.g., the contact who submitted it).
        
        Used by: Linking tickets to CSuite profiles (Kods)
        """
        return self._get(
            f"crm/v3/objects/tickets/{ticket_id}/associations/{to_type}"
        )
    
    # =========================================================================
    # OWNERS
    # =========================================================================
    
    def get_owners(self) -> dict:
        """Get list of owners (staff members with HubSpot access)"""
        return self._get("crm/v3/owners")
    
    def get_owner_by_email(self, email: str) -> dict:
        """Get owner by email address.
        
        Used by: Mapping logged-in Jidhr user to HubSpot owner for note/task assignment
        """
        result = self._get("crm/v3/owners", {"email": email})
        if "results" in result and result["results"]:
            return result["results"][0]
        return {"error": f"Owner not found: {email}"}
    
    # =========================================================================
    # URL HELPERS
    # =========================================================================
    
    # -----------------------------------------------------------------
    # CONTACT LISTS
    # -----------------------------------------------------------------

    def create_contact_list(self, name: str) -> dict:
        """Create a static (manual) contact list.

        Used by: events.py to create targetable lists for email outreach.

        Args:
            name: List name (e.g., "Event: AGL Fellowship - 2026-03-03")

        Returns:
            Dict with nested 'list' key containing 'listId'.
        """
        return self._post("crm/v3/lists", {
            "name": name,
            "objectTypeId": "0-1",
            "processingType": "MANUAL"
        })

    def add_contacts_to_list(self, list_id: str, contact_ids: list) -> dict:
        """Add contacts to a static list by HubSpot contact IDs.

        Uses PUT (not POST) per HubSpot API spec.

        Args:
            list_id: The HubSpot list ID
            contact_ids: List of integer contact IDs
        """
        url = f"{self.base_url}/crm/v3/lists/{list_id}/memberships/add"
        headers = {**self.headers}
        try:
            response = requests.put(url, json=contact_ids, headers=headers)
            return response.json() if response.ok else {"error": response.text[:200]}
        except Exception as e:
            logger.error(f"Error adding contacts to list {list_id}: {e}")
            return {"error": str(e)}

    # -----------------------------------------------------------------
    # GIVING CIRCLE
    # -----------------------------------------------------------------

    def get_giving_circle_members(self) -> dict:
        """Get all contacts in the Giving Circle list (ID 126).

        Returns:
            Dict with 'results' containing member record IDs.
        """
        return self._get(
            f"crm/v3/lists/{Config.GIVING_CIRCLE_LIST_ID}/memberships",
            {"limit": 250}
        )

    def get_giving_circle_member_details(self, limit: int = 50) -> list:
        """Get Giving Circle members with contact details.

        Fetches list memberships, then batch-loads contact properties.

        Returns:
            List of contact dicts with name, email, and giving_circle_status.
        """
        memberships = self.get_giving_circle_members()
        if not memberships or "results" not in memberships:
            return []

        record_ids = [str(m.get("recordId")) for m in memberships.get("results", [])]
        if not record_ids:
            return []

        # Fetch contact details in batches of 50
        contacts = []
        for i in range(0, min(len(record_ids), limit), 50):
            batch = record_ids[i:i + 50]
            batch_result = self._post("crm/v3/objects/contacts/batch/read", {
                "inputs": [{"id": rid} for rid in batch],
                "properties": ["firstname", "lastname", "email", "constituent_codes"]
            })
            if batch_result and "results" in batch_result:
                contacts.extend(batch_result["results"])

        return contacts

    def update_giving_circle_status(self, contact_id: str, status: str) -> dict:
        """Update a contact's Giving Circle constituent code.

        Args:
            contact_id: HubSpot contact ID
            status: constituent_codes value (e.g. 'GC Member', 'GC Voting Member')
        """
        return self._patch(f"crm/v3/objects/contacts/{contact_id}", {
            "properties": {"constituent_codes": status}
        })

    @staticmethod
    def get_contact_url(contact_id: str) -> str:
        """Get the HubSpot UI URL for a contact"""
        return Config.HUBSPOT_CONTACT_URL.format(contact_id=contact_id)
    
    @staticmethod
    def get_ticket_url(ticket_id: str) -> str:
        """Get the HubSpot UI URL for a ticket"""
        return Config.HUBSPOT_TICKET_URL.format(ticket_id=ticket_id)
    
    @staticmethod
    def get_task_url() -> str:
        """Get the HubSpot UI URL for the tasks view"""
        return Config.HUBSPOT_TASK_URL
