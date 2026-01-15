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
    
    def get_profile_emails(self, limit: int = None) -> dict:
        """Get mapping of profile_id → email from CSuite
        
        Args:
            limit: Max number of profiles to fetch (None = all)
        """
        profile_emails = {}
        offset = 0
        batch_size = 100
        total_fetched = 0
        
        while True:
            result = self.csuite.get_profiles(limit=batch_size, offset=offset)
            
            if not result.get("success"):
                logger.error(f"Failed to get profiles at offset {offset}")
                break
            
            data = result.get("data", {})
            profiles = data.get("results", [])
            
            if not profiles:
                break
            
            total_fetched += len(profiles)
            
            for profile in profiles:
                profile_id = profile.get("profile_id")
                email = profile.get("primary_email")
                if profile_id and email:
                    profile_emails[profile_id] = email.lower().strip()
            
            # Check if we've hit the limit on TOTAL profiles fetched
            if limit and total_fetched >= limit:
                break
            
            # Check if we got fewer than batch_size (last page)
            if len(profiles) < batch_size:
                break
            
            offset += batch_size
        
        logger.info(f"Fetched {total_fetched} profiles, {len(profile_emails)} have emails")
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
    
    def sync(self, dry_run: bool = False, quick: bool = False) -> dict:
        """Run the full donation sync
        
        Args:
            dry_run: If True, don't actually update HubSpot
            quick: If True, only process a sample (faster for testing)
        
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
        
        # Use limits for dry run or quick mode
        profile_limit = 500 if (dry_run or quick) else None
        donation_limit = 500 if (dry_run or quick) else None
        
        logger.info(f"Starting donation sync... (dry_run={dry_run}, quick={quick})")
        
        # Step 1: Get profile email mapping
        logger.info("Step 1: Getting profile emails from CSuite...")
        profile_emails = self.get_profile_emails(limit=profile_limit)
        
        if not profile_emails:
            logger.error("No profile emails found")
            results['details'].append("No profiles with emails found in CSuite")
            return results
        
        # Step 2: Get donations
        logger.info("Step 2: Getting donations from CSuite...")
        donations = self.get_donations_with_limit(limit=donation_limit)
        
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
                logger.debug(f"[DRY RUN] Would update {email}: {properties}")
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
        mode = "[DRY RUN] " if dry_run else ""
        mode += "[QUICK] " if quick else ""
        logger.info(f"{mode}Sync complete: {results['updated']} updated, "
                   f"{results['skipped_no_email']} skipped (no email), "
                   f"{results['skipped_not_found']} skipped (not in HubSpot), "
                   f"{results['errors']} errors")
        
        return results
    
    def get_donations_with_limit(self, limit: int = None) -> list:
        """Get donations with optional limit"""
        all_donations = []
        offset = 0
        batch_size = 100
        
        while True:
            result = self.csuite.get_donations(limit=batch_size, offset=offset)
            
            if not result.get("success"):
                logger.error(f"Failed to get donations at offset {offset}")
                break
            
            data = result.get("data", {})
            donations = data.get("results", [])
            
            if not donations:
                break
            
            all_donations.extend(donations)
            
            # Check if we've hit the limit
            if limit and len(all_donations) >= limit:
                all_donations = all_donations[:limit]
                break
            
            # Check if we got fewer than batch_size (last page)
            if len(donations) < batch_size:
                break
            
            offset += batch_size
            
            # Log progress every 500 donations
            if offset % 500 == 0:
                logger.info(f"Fetched {offset} donations so far...")
        
        logger.info(f"Retrieved {len(all_donations)} donations")
        return all_donations


def run_donation_sync(dry_run: bool = False, quick: bool = False) -> dict:
    """Convenience function to run donation sync
    
    Args:
        dry_run: Preview changes without applying them
        quick: Use sample data for faster testing (500 profiles, 500 donations)
    """
    sync = DonationSync()
    return sync.sync(dry_run=dry_run, quick=quick)
