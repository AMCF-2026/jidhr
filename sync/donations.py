"""
Donations Sync
==============
Syncs donation aggregates from CSuite to HubSpot contact properties.

Matching: CSuite profile.primary_email → HubSpot contact.email

HubSpot Properties Updated:
- lifetime_giving: Total donation amount
- last_donation_date: Most recent donation date
- last_donation_amount: Most recent donation amount  
- donation_count: Number of donations
- csuite_profile_id: CSuite profile ID (for future direct linking)
"""

import logging
from datetime import datetime
from collections import defaultdict
from clients.csuite import CSuiteClient
from clients.hubspot import HubSpotClient

logger = logging.getLogger(__name__)


class DonationSync:
    """Sync donation data from CSuite to HubSpot"""
    
    def __init__(self):
        self.csuite = CSuiteClient()
        self.hubspot = HubSpotClient()
    
    def get_profile_emails(self) -> dict:
        """Get mapping of profile_id → email from CSuite"""
        profile_emails = {}
        
        # Use the built-in method that handles pagination
        profiles = self.csuite.get_all_profiles()
        
        for profile in profiles:
            profile_id = profile.get("profile_id")
            email = profile.get("primary_email")
            if profile_id and email:
                profile_emails[profile_id] = email.lower().strip()
        
        logger.info(f"Found {len(profile_emails)} profiles with emails")
        return profile_emails
    
    def aggregate_donations(self, donations: list) -> dict:
        """Aggregate donations by profile_id
        
        Returns:
            dict: {profile_id: {
                'total': float,
                'count': int,
                'last_date': str,
                'last_amount': float
            }}
        """
        aggregates = defaultdict(lambda: {
            'total': 0.0,
            'count': 0,
            'last_date': None,
            'last_amount': 0.0,
            'donations': []
        })
        
        for donation in donations:
            profile_id = donation.get("profile_id")
            if not profile_id:
                continue
            
            amount_str = donation.get("donation_amount", "0")
            try:
                amount = float(amount_str)
            except (ValueError, TypeError):
                amount = 0.0
            
            date_str = donation.get("donation_date", "")
            
            agg = aggregates[profile_id]
            agg['total'] += amount
            agg['count'] += 1
            agg['donations'].append({
                'amount': amount,
                'date': date_str
            })
        
        # Calculate last donation for each profile
        for profile_id, agg in aggregates.items():
            if agg['donations']:
                # Sort by date descending
                sorted_donations = sorted(
                    agg['donations'],
                    key=lambda x: x['date'] or '',
                    reverse=True
                )
                agg['last_date'] = sorted_donations[0]['date']
                agg['last_amount'] = sorted_donations[0]['amount']
            
            # Clean up - don't need full list anymore
            del agg['donations']
        
        return dict(aggregates)
    
    def format_date_for_hubspot(self, date_str: str) -> str:
        """Convert CSuite date to HubSpot format (midnight UTC)"""
        if not date_str:
            return None
        
        try:
            # CSuite format: YYYY-MM-DD
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            # HubSpot wants midnight UTC
            return dt.strftime("%Y-%m-%dT00:00:00.000Z")
        except ValueError:
            logger.warning(f"Invalid date format: {date_str}")
            return None
    
    def sync(self, dry_run: bool = False) -> dict:
        """Run the full donation sync
        
        Args:
            dry_run: If True, don't actually update HubSpot
        
        Returns:
            dict: Sync results with stats
        """
        results = {
            'updated': 0,
            'skipped_no_email': 0,
            'skipped_not_found': 0,
            'errors': 0,
            'details': []
        }
        
        logger.info("Starting donation sync...")
        
        # Step 1: Get profile email mapping
        logger.info("Step 1: Getting profile emails from CSuite...")
        profile_emails = self.get_profile_emails()
        
        if not profile_emails:
            logger.error("No profile emails found")
            results['details'].append("No profiles with emails found in CSuite")
            return results
        
        # Step 2: Get all donations
        logger.info("Step 2: Getting donations from CSuite...")
        donations = self.csuite.get_all_donations()
        
        if not donations:
            logger.warning("No donations found")
            results['details'].append("No donations found in CSuite")
            return results
        
        logger.info(f"Found {len(donations)} donations")
        
        # Step 3: Aggregate by profile
        logger.info("Step 3: Aggregating donations by profile...")
        aggregates = self.aggregate_donations(donations)
        logger.info(f"Aggregated donations for {len(aggregates)} profiles")
        
        # Step 4: Update HubSpot contacts
        logger.info("Step 4: Updating HubSpot contacts...")
        
        for profile_id, agg in aggregates.items():
            email = profile_emails.get(profile_id)
            
            if not email:
                results['skipped_no_email'] += 1
                continue
            
            # Build HubSpot properties
            properties = {
                'lifetime_giving': str(round(agg['total'], 2)),
                'donation_count': str(agg['count']),
                'last_donation_amount': str(round(agg['last_amount'], 2)),
                'csuite_profile_id': str(profile_id)
            }
            
            # Add last donation date if available
            formatted_date = self.format_date_for_hubspot(agg['last_date'])
            if formatted_date:
                properties['last_donation_date'] = formatted_date
            
            if dry_run:
                logger.info(f"[DRY RUN] Would update {email}: {properties}")
                results['updated'] += 1
                continue
            
            # Update HubSpot
            update_result = self.hubspot.update_contact_by_email(email, properties)
            
            if "error" in update_result:
                if "not found" in update_result["error"].lower():
                    results['skipped_not_found'] += 1
                    logger.debug(f"Contact not found in HubSpot: {email}")
                else:
                    results['errors'] += 1
                    logger.error(f"Error updating {email}: {update_result['error']}")
            else:
                results['updated'] += 1
                logger.debug(f"Updated {email}: ${agg['total']:.2f} lifetime")
        
        # Summary
        logger.info(f"Sync complete: {results['updated']} updated, "
                   f"{results['skipped_no_email']} skipped (no email), "
                   f"{results['skipped_not_found']} skipped (not in HubSpot), "
                   f"{results['errors']} errors")
        
        return results


def run_donation_sync(dry_run: bool = False) -> dict:
    """Convenience function to run donation sync"""
    sync = DonationSync()
    return sync.sync(dry_run=dry_run)
