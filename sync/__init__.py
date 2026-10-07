"""
Sync Module
===========
Data synchronization between CSuite and HubSpot.

Available syncs:
- donations: Aggregate donation data to HubSpot contact properties
- event_apply: CSuite event dates to HubSpot marketing events (replaced
  sync/events.py on 2026-10-06; see that module's docstring for why)
- newsletter: Newsletter opt-ins to HubSpot subscriptions
"""

from sync.donations import (DonationSync, DonationSyncDisabled,
                            PartialReadRefused,
                            donation_sync_allowed, run_donation_sync)
from sync.registrations import (PhaseOnePreviewOnly,
                                RegistrationsSyncDisabled,
                                registrations_sync_allowed)
from sync.newsletter import (NewsletterSync,
                             NewsletterSyncDisabled,
                             newsletter_sync_allowed,
                             run_newsletter_sync)

__all__ = [
    'DonationSync',
    'DonationSyncDisabled',
    'PartialReadRefused',
    'donation_sync_allowed',
    'EventSync', 
    'PhaseOnePreviewOnly',
    'RegistrationsSyncDisabled',
    'registrations_sync_allowed',
    'NewsletterSync',
    'NewsletterSyncDisabled',
    'newsletter_sync_allowed',
    'run_donation_sync',
    'run_newsletter_sync',
]