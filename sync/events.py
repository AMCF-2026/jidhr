"""
Events Sync
===========
Syncs events from CSuite to HubSpot Marketing Events.

Mapping:
- event_description → eventName (CSuite event_name is generic like "Event - Other")
- event_date + start_time → startDateTime
- event_date + 2 hours → endDateTime (default duration)
- event_date_id → externalEventId (for deduplication)
- location → customProperties
"""

import logging
from datetime import datetime, timedelta
from clients.csuite import CSuiteClient
from clients.hubspot import HubSpotClient
from config import Config

logger = logging.getLogger(__name__)


class EventSync:
    """Sync events from CSuite to HubSpot"""
    
    def __init__(self):
        self.csuite = CSuiteClient()
        self.hubspot = HubSpotClient()
        self.default_owner_id = Config.DEFAULT_EVENT_OWNER_ID
    
    def format_datetime(self, date_str: str, time_str: str = None) -> str:
        """Convert CSuite date/time to ISO 8601 format
        
        Args:
            date_str: Date in YYYY-MM-DD format
            time_str: Optional time in HH:MM format
        
        Returns:
            ISO 8601 datetime string
        """
        if not date_str:
            return None
        
        try:
            if time_str:
                dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
            else:
                # Default to 10:00 AM if no time specified
                dt = datetime.strptime(f"{date_str} 10:00", "%Y-%m-%d %H:%M")
            
            return dt.strftime("%Y-%m-%dT%H:%M:00.000Z")
        except ValueError as e:
            logger.warning(f"Invalid date/time: {date_str} {time_str} - {e}")
            return None
    
    def calculate_end_time(self, start_datetime: str, duration_hours: int = 2) -> str:
        """Calculate end time by adding duration to start time"""
        if not start_datetime:
            return None
        
        try:
            dt = datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M:00.000Z")
            end_dt = dt + timedelta(hours=duration_hours)
            return end_dt.strftime("%Y-%m-%dT%H:%M:00.000Z")
        except ValueError:
            return None
    
    def map_event_type(self, csuite_type: str) -> str:
        """Map CSuite event type to HubSpot event type"""
        type_mapping = {
            "event": "Conference",
            "webinar": "Webinar",
            "fundraiser": "Charity & Causes",
            "gala": "Charity & Causes",
            "workshop": "Workshop",
            "meeting": "Meeting",
        }
        return type_mapping.get(csuite_type.lower() if csuite_type else "", "Other")
    
    def build_hubspot_event(self, csuite_event: dict) -> dict:
        """Convert CSuite event to HubSpot Marketing Event format"""
        
        # Use description as name (event_name in CSuite is generic)
        event_name = csuite_event.get("event_description") or csuite_event.get("event_name") or "Unnamed Event"
        
        # Build start/end times
        event_date = csuite_event.get("event_date")
        start_time = csuite_event.get("start_time")  # May be None
        
        start_datetime = self.format_datetime(event_date, start_time)
        end_datetime = self.calculate_end_time(start_datetime)
        
        # External ID for deduplication
        external_id = f"csuite-{csuite_event.get('event_date_id', csuite_event.get('event_id', 'unknown'))}"
        
        hubspot_event = {
            "eventName": event_name,
            "eventOrganizer": self.default_owner_id,
            "externalEventId": external_id,
            "eventType": self.map_event_type(csuite_event.get("event_type_code", "")),
        }
        
        # Add optional fields
        if start_datetime:
            hubspot_event["startDateTime"] = start_datetime
        if end_datetime:
            hubspot_event["endDateTime"] = end_datetime
        
        # Add location as custom property if available
        location = csuite_event.get("location")
        if location:
            hubspot_event["customProperties"] = [
                {"name": "location", "value": location}
            ]
        
        return hubspot_event
    
    def event_exists(self, external_id: str) -> bool:
        """Check if event already exists in HubSpot by external ID"""
        try:
            result = self.hubspot.search_marketing_event_by_external_id(external_id)
            # If we get a valid response without error, event exists
            return "error" not in result and result.get("eventName")
        except Exception:
            return False
    
    def sync(self, dry_run: bool = False, skip_archived: bool = True, future_only: bool = True) -> dict:
        """Run the full event sync
        
        Args:
            dry_run: If True, don't actually create events
            skip_archived: Skip archived events
            future_only: Only sync future events
        
        Returns:
            dict: Sync results with stats
        """
        results = {
            'created': 0,
            'skipped_exists': 0,
            'skipped_archived': 0,
            'skipped_past': 0,
            'errors': 0,
            'details': []
        }
        
        logger.info("Starting event sync...")
        
        # Step 1: Get events from CSuite
        logger.info("Step 1: Getting events from CSuite...")
        csuite_result = self.csuite.get_event_dates(limit=100)
        
        if not csuite_result.get("success"):
            error_msg = csuite_result.get("error", "Unknown error")
            logger.error(f"Failed to get CSuite events: {error_msg}")
            results['errors'] += 1
            results['details'].append(f"CSuite error: {error_msg}")
            return results
        
        events = csuite_result.get("data", {}).get("results", [])
        logger.info(f"Found {len(events)} events in CSuite")
        
        if not events:
            results['details'].append("No events found in CSuite")
            return results
        
        # Step 2: Process each event
        today = datetime.now().strftime("%Y-%m-%d")
        
        for event in events:
            event_name = event.get("event_description") or event.get("event_name", "Unknown")
            event_date = event.get("event_date", "")
            
            # Skip archived
            if skip_archived and event.get("archived"):
                results['skipped_archived'] += 1
                logger.debug(f"Skipping archived event: {event_name}")
                continue
            
            # Skip past events
            if future_only and event_date and event_date < today:
                results['skipped_past'] += 1
                logger.debug(f"Skipping past event: {event_name} ({event_date})")
                continue
            
            # Build HubSpot event
            hubspot_event = self.build_hubspot_event(event)
            external_id = hubspot_event.get("externalEventId")
            
            # Check if exists
            if self.event_exists(external_id):
                results['skipped_exists'] += 1
                logger.debug(f"Event already exists: {event_name}")
                continue
            
            if dry_run:
                logger.info(f"[DRY RUN] Would create event: {event_name}")
                results['created'] += 1
                results['details'].append(f"Would create: {event_name}")
                continue
            
            # Create in HubSpot
            create_result = self.hubspot.create_marketing_event(hubspot_event)
            
            if "error" in create_result:
                results['errors'] += 1
                logger.error(f"Failed to create event {event_name}: {create_result['error']}")
                results['details'].append(f"Error creating {event_name}: {create_result['error']}")
            else:
                results['created'] += 1
                logger.info(f"Created event: {event_name}")
                results['details'].append(f"Created: {event_name}")
        
        # Summary
        logger.info(f"Event sync complete: {results['created']} created, "
                   f"{results['skipped_exists']} already existed, "
                   f"{results['skipped_past']} past events skipped, "
                   f"{results['errors']} errors")
        
        return results


def run_event_sync(dry_run: bool = False) -> dict:
    """Convenience function to run event sync"""
    sync = EventSync()
    return sync.sync(dry_run=dry_run)