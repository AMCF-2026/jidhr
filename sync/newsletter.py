"""
Newsletter Sync
===============
Syncs newsletter opt-in status from CSuite to HubSpot communication preferences.

CSuite: newsletter field on profiles (0 or 1)
HubSpot: Communication Preferences subscription status

Note: This requires CSuite profiles to have a 'newsletter' field.
Check with CSuite API to confirm field availability.
"""

import logging
from clients.csuite import CSuiteClient
from clients.hubspot import HubSpotClient
from config import Config

logger = logging.getLogger(__name__)


class NewsletterSync:
    """Sync newsletter opt-in from CSuite to HubSpot"""
    
    def __init__(self):
        self.csuite = CSuiteClient()
        self.hubspot = HubSpotClient()
        self.subscription_id = Config.HUBSPOT_MARKETING_SUBSCRIPTION_ID
    
    def get_opted_in_profiles(self, limit: int = None) -> list:
        """Get CSuite profiles with newsletter opt-in
        
        Args:
            limit: Max profiles to check (None = all)
        
        Returns:
            list: Profiles with email and newsletter=1
        """
        opted_in = []
        offset = 0
        batch_size = 100
        profiles_checked = 0
        
        while True:
            result = self.csuite.get_profiles(limit=batch_size, offset=offset)
            
            if not result.get("success"):
                logger.error(f"Failed to get profiles at offset {offset}")
                break
            
            data = result.get("data", {})
            profiles = data.get("results", [])
            
            if not profiles:
                break
            
            for profile in profiles:
                email = profile.get("primary_email")
                newsletter = profile.get("newsletter", 0)
                
                # Only include profiles with email and newsletter opt-in
                if email and newsletter == 1:
                    opted_in.append({
                        'profile_id': profile.get("profile_id"),
                        'email': email.lower().strip(),
                        'name': profile.get("name", "")
                    })
            
            profiles_checked += len(profiles)
            
            # Check if we've hit the limit
            if limit and profiles_checked >= limit:
                break
            
            # Check if we got fewer than batch_size (last page)
            if len(profiles) < batch_size:
                break
            
            offset += batch_size
        
        logger.info(f"Checked {profiles_checked} profiles, found {len(opted_in)} opted in to newsletter")
        return opted_in
    
    def sync(self, dry_run: bool = False, quick: bool = False) -> dict:
        """Run the newsletter sync
        
        Args:
            dry_run: If True, don't actually update HubSpot
            quick: If True, only check sample profiles (faster for testing)
        
        Returns:
            dict: Sync results with stats
        """
        results = {
            'subscribed': 0,
            'already_subscribed': 0,
            'not_found': 0,
            'errors': 0,
            'details': []
        }
        
        # Use limit for dry run or quick mode
        profile_limit = 500 if (dry_run or quick) else None
        
        logger.info(f"Starting newsletter sync... (dry_run={dry_run}, quick={quick})")
        
        # Step 1: Get opted-in profiles from CSuite
        logger.info("Step 1: Getting newsletter opt-ins from CSuite...")
        opted_in = self.get_opted_in_profiles(limit=profile_limit)
        
        if not opted_in:
            logger.warning("No newsletter opt-ins found in CSuite")
            results['details'].append("No profiles with newsletter opt-in found")
            return results
        
        # Step 2: Subscribe each contact in HubSpot
        logger.info(f"Step 2: Subscribing {len(opted_in)} contacts in HubSpot...")
        
        for profile in opted_in:
            email = profile['email']
            
            if dry_run:
                logger.info(f"[DRY RUN] Would subscribe: {email}")
                results['subscribed'] += 1
                continue
            
            # Check current status first (optional, for accurate reporting)
            status_result = self.hubspot.get_subscription_status(email)
            
            if "error" not in status_result:
                # Check if already subscribed
                subscriptions = status_result.get("subscriptionStatuses", [])
                already_subscribed = any(
                    s.get("id") == self.subscription_id and s.get("status") == "SUBSCRIBED"
                    for s in subscriptions
                )
                
                if already_subscribed:
                    results['already_subscribed'] += 1
                    logger.debug(f"Already subscribed: {email}")
                    continue
            
            # Subscribe the contact
            subscribe_result = self.hubspot.subscribe_contact(email, self.subscription_id)
            
            if "error" in subscribe_result:
                error_msg = subscribe_result.get("error", "Unknown error")
                
                if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                    results['not_found'] += 1
                    logger.debug(f"Contact not found in HubSpot: {email}")
                else:
                    results['errors'] += 1
                    logger.error(f"Failed to subscribe {email}: {error_msg}")
            else:
                results['subscribed'] += 1
                logger.debug(f"Subscribed: {email}")
        
        # Summary
        logger.info(f"Newsletter sync complete: {results['subscribed']} subscribed, "
                   f"{results['already_subscribed']} already subscribed, "
                   f"{results['not_found']} not in HubSpot, "
                   f"{results['errors']} errors")
        
        return results


def run_newsletter_sync(dry_run: bool = False, quick: bool = False) -> dict:
    """Convenience function to run newsletter sync
    
    Args:
        dry_run: Preview changes without applying them
        quick: Use sample data for faster testing (500 profiles)
    """
    sync = NewsletterSync()
    return sync.sync(dry_run=dry_run, quick=quick)
