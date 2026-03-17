"""
Sync Module
===========
Data synchronization between CSuite and HubSpot.

Available syncs:
- donations: Aggregate donation data to HubSpot contact properties
- events: CSuite events to HubSpot Marketing Events
- newsletter: Newsletter opt-ins to HubSpot subscriptions
"""

from sync.donations import DonationSync, run_donation_sync
from sync.events import EventSync, run_event_sync
from sync.newsletter import NewsletterSync, run_newsletter_sync

__all__ = [
    'DonationSync',
    'EventSync', 
    'NewsletterSync',
    'run_donation_sync',
    'run_event_sync',
    'run_newsletter_sync',
]