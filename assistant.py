"""
Jidhr Assistant
===============
The main brain that orchestrates queries across HubSpot and CSuite.
"""

import logging
from datetime import datetime
from config import SYSTEM_PROMPT
from clients import OpenRouterClient, HubSpotClient, CSuiteClient
from sync import run_donation_sync, run_event_sync, run_newsletter_sync

logger = logging.getLogger(__name__)


class JidhrAssistant:
    """Main assistant that orchestrates queries across systems"""
    
    def __init__(self):
        logger.info("Initializing Jidhr Assistant")
        self.claude = OpenRouterClient()
        self.hubspot = HubSpotClient()
        self.csuite = CSuiteClient()
        self.conversation_history = []
    
    def get_system_prompt(self) -> str:
        """Get system prompt with current date"""
        return SYSTEM_PROMPT.format(
            current_date=datetime.now().strftime("%B %d, %Y")
        )
    
    def process_query(self, user_message: str) -> str:
        """
        Process a user query and return response.
        
        Args:
            user_message: The user's question or request
            
        Returns:
            Jidhr's response
        """
        logger.info(f"Processing query: {user_message[:50]}...")
        
        # Check for sync commands FIRST (before adding to history)
        sync_response = self._handle_sync_commands(user_message)
        if sync_response:
            # Add to history for context
            self.conversation_history.append({"role": "user", "content": user_message})
            self.conversation_history.append({"role": "assistant", "content": sync_response})
            return sync_response
        
        # Add user message to history
        self.conversation_history.append({
            "role": "user",
            "content": user_message
        })
        
        # Check for specific intents and gather data
        context = self._gather_context(user_message)
        
        # Build the message with context
        enhanced_message = user_message
        if context:
            enhanced_message = f"{user_message}\n\n[System Context - Real Data]\n{context}"
            self.conversation_history[-1]["content"] = enhanced_message
            logger.info(f"Added context: {len(context)} chars")
        
        # Get response from Claude
        response = self.claude.chat(
            messages=self.conversation_history,
            system_prompt=self.get_system_prompt()
        )
        
        # Add assistant response to history
        self.conversation_history.append({
            "role": "assistant",
            "content": response
        })
        
        # Keep history manageable (last 20 exchanges)
        if len(self.conversation_history) > 40:
            self.conversation_history = self.conversation_history[-40:]
            logger.info("Trimmed conversation history")
        
        return response
    
    def _handle_sync_commands(self, query: str) -> str:
        """
        Handle sync commands directly without going to Claude.
        
        Returns:
            Response string if sync command detected, None otherwise
        """
        query_lower = query.lower().strip()
        
        # Sync donations command
        if any(phrase in query_lower for phrase in ['sync donations', 'sync donation', 'update donations']):
            logger.info("Running donation sync...")
            
            # Check for dry run
            dry_run = 'dry run' in query_lower or 'test' in query_lower
            
            try:
                # Use quick mode for dry runs to avoid timeout
                results = run_donation_sync(dry_run=dry_run, quick=dry_run)
                return self._format_donation_sync_results(results, dry_run)
            except Exception as e:
                logger.error(f"Donation sync error: {str(e)}")
                return f"❌ Donation sync failed: {str(e)}"
        
        # Sync events command
        if any(phrase in query_lower for phrase in ['sync events', 'sync event', 'update events']):
            logger.info("Running event sync...")
            
            dry_run = 'dry run' in query_lower or 'test' in query_lower
            
            try:
                results = run_event_sync(dry_run=dry_run)
                return self._format_event_sync_results(results, dry_run)
            except Exception as e:
                logger.error(f"Event sync error: {str(e)}")
                return f"❌ Event sync failed: {str(e)}"
        
        # Sync newsletter command
        if any(phrase in query_lower for phrase in ['sync newsletter', 'sync newsletters', 'update newsletter', 'sync subscriptions']):
            logger.info("Running newsletter sync...")
            
            dry_run = 'dry run' in query_lower or 'test' in query_lower
            
            try:
                # Use quick mode for dry runs to avoid timeout
                results = run_newsletter_sync(dry_run=dry_run, quick=dry_run)
                return self._format_newsletter_sync_results(results, dry_run)
            except Exception as e:
                logger.error(f"Newsletter sync error: {str(e)}")
                return f"❌ Newsletter sync failed: {str(e)}"
        
        # Sync all command
        if query_lower in ['sync all', 'sync everything', 'run all syncs']:
            logger.info("Running all syncs...")
            return self._run_all_syncs()
        
        return None
    
    def _format_donation_sync_results(self, results: dict, dry_run: bool) -> str:
        """Format donation sync results for display"""
        prefix = "🧪 **DRY RUN (Sample)** - " if dry_run else ""
        
        response = f"""{prefix}✅ **Donation Sync Complete**

📊 **Results:**
• **{results['updated']}** contacts {"would be updated" if dry_run else "updated"} with donation data
• **{results['skipped_no_email']}** profiles skipped (no email in CSuite)
• **{results['skipped_not_found']}** profiles skipped (not found in HubSpot)
• **{results['errors']}** errors

💡 Fields: `lifetime_giving`, `last_donation_date`, `last_donation_amount`, `donation_count`, `csuite_profile_id`"""
        
        if dry_run:
            response += "\n\n⚡ *This dry run used sample data (500 profiles, 500 donations). Run `sync donations` without 'dry run' for full sync.*"
        
        return response
    
    def _format_event_sync_results(self, results: dict, dry_run: bool) -> str:
        """Format event sync results for display"""
        prefix = "🧪 **DRY RUN** - " if dry_run else ""
        
        response = f"""{prefix}✅ **Event Sync Complete**

📊 **Results:**
• **{results['created']}** events created in HubSpot
• **{results['skipped_exists']}** events skipped (already exist)
• **{results['skipped_past']}** events skipped (past events)
• **{results['skipped_archived']}** events skipped (archived)
• **{results['errors']}** errors"""
        
        if results.get('details'):
            response += "\n\n📝 **Details:**\n"
            for detail in results['details'][:10]:  # Limit to 10
                response += f"• {detail}\n"
        
        if dry_run:
            response += "\n*Run without 'dry run' to apply changes.*"
        
        return response
    
    def _format_newsletter_sync_results(self, results: dict, dry_run: bool) -> str:
        """Format newsletter sync results for display"""
        prefix = "🧪 **DRY RUN (Sample)** - " if dry_run else ""
        
        response = f"""{prefix}✅ **Newsletter Sync Complete**

📊 **Results:**
• **{results['subscribed']}** contacts {"would be subscribed" if dry_run else "subscribed"} to marketing emails
• **{results['already_subscribed']}** contacts already subscribed
• **{results['not_found']}** contacts not found in HubSpot
• **{results['errors']}** errors"""
        
        if dry_run:
            response += "\n\n⚡ *This dry run used sample data (500 profiles). Run `sync newsletter` without 'dry run' for full sync.*"
        
        return response
    
    def _run_all_syncs(self) -> str:
        """Run all sync operations"""
        responses = []
        
        # Donations
        try:
            donation_results = run_donation_sync(dry_run=False)
            responses.append(f"**Donations:** {donation_results['updated']} updated, {donation_results['errors']} errors")
        except Exception as e:
            responses.append(f"**Donations:** ❌ Failed - {str(e)}")
        
        # Events
        try:
            event_results = run_event_sync(dry_run=False)
            responses.append(f"**Events:** {event_results['created']} created, {event_results['errors']} errors")
        except Exception as e:
            responses.append(f"**Events:** ❌ Failed - {str(e)}")
        
        # Newsletter
        try:
            newsletter_results = run_newsletter_sync(dry_run=False)
            responses.append(f"**Newsletter:** {newsletter_results['subscribed']} subscribed, {newsletter_results['errors']} errors")
        except Exception as e:
            responses.append(f"**Newsletter:** ❌ Failed - {str(e)}")
        
        return "✅ **All Syncs Complete**\n\n" + "\n".join(responses)
    
    def _gather_context(self, query: str) -> str:
        """
        Gather relevant context from HubSpot/CSuite based on query.
        
        Analyzes the query for keywords and fetches relevant data
        to help Claude give better answers.
        """
        context_parts = []
        query_lower = query.lower()
        
        logger.info(f"Gathering context for: {query_lower[:50]}...")
        
        # Fund-related queries → CSuite
        if any(word in query_lower for word in ['fund', 'balance', 'daf', 'endowment', 'grant']):
            logger.info("Fetching CSuite funds...")
            try:
                funds_data = self.csuite.get_funds(limit=20)
                if funds_data.get('success') and funds_data.get('data'):
                    results = funds_data['data'].get('results', [])
                    fund_list = [
                        f"{f.get('fund_name', 'Unknown')} (ID: {f.get('funit_id', 'N/A')})"
                        for f in results[:10]
                    ]
                    context_parts.append(f"CSuite Funds:\n" + "\n".join(fund_list))
                    logger.info(f"Found {len(fund_list)} funds")
            except Exception as e:
                logger.error(f"Error fetching funds: {str(e)}")
        
        # Contact-related queries → HubSpot
        if any(word in query_lower for word in ['contact', 'donor', 'email', 'person', 'who']):
            logger.info("Fetching HubSpot contacts...")
            try:
                contacts_data = self.hubspot.get_contacts(limit=10)
                if 'results' in contacts_data:
                    contact_list = [
                        f"{c.get('properties', {}).get('firstname', '')} {c.get('properties', {}).get('lastname', '')} ({c.get('properties', {}).get('email', 'No email')})"
                        for c in contacts_data['results'][:5]
                    ]
                    context_parts.append(f"HubSpot Contacts:\n" + "\n".join(contact_list))
                    logger.info(f"Found {len(contact_list)} contacts")
            except Exception as e:
                logger.error(f"Error fetching contacts: {str(e)}")
        
        # Form-related queries → HubSpot
        if any(word in query_lower for word in ['form', 'submission', 'inquiry', 'submitted']):
            logger.info("Fetching HubSpot forms...")
            try:
                forms_data = self.hubspot.get_forms(limit=10)
                if 'results' in forms_data:
                    form_list = [
                        f"{f.get('name', 'Unknown')} (ID: {f.get('id', 'N/A')})"
                        for f in forms_data['results'][:5]
                    ]
                    context_parts.append(f"HubSpot Forms:\n" + "\n".join(form_list))
                    logger.info(f"Found {len(form_list)} forms")
            except Exception as e:
                logger.error(f"Error fetching forms: {str(e)}")
        
        # Social media queries → HubSpot
        if any(word in query_lower for word in ['social', 'post', 'facebook', 'linkedin', 'schedule', 'channel']):
            logger.info("Fetching HubSpot social channels...")
            try:
                channels_data = self.hubspot.get_social_channels()
                if isinstance(channels_data, list):
                    channel_list = [
                        f"{c.get('name', 'Unknown')} ({c.get('channelType', 'Unknown')})"
                        for c in channels_data[:5]
                    ]
                    context_parts.append(f"Social Channels:\n" + "\n".join(channel_list))
                    logger.info(f"Found {len(channel_list)} channels")
            except Exception as e:
                logger.error(f"Error fetching social channels: {str(e)}")
        
        # Event-related queries → BOTH CSuite AND HubSpot
        if any(word in query_lower for word in ['event', 'symposium', 'webinar', 'registration', 'gala', 'dinner']):
            # CSuite Events
            logger.info("Fetching CSuite events...")
            try:
                csuite_events = self.csuite.get_event_dates(limit=10)
                if csuite_events.get('success') and csuite_events.get('data'):
                    results = csuite_events['data'].get('results', [])
                    event_list = [
                        f"{e.get('event_description') or e.get('event_name', 'Unknown')} ({e.get('event_date', 'No date')})"
                        for e in results[:5]
                    ]
                    context_parts.append(f"CSuite Events:\n" + "\n".join(event_list))
                    logger.info(f"Found {len(event_list)} CSuite events")
            except Exception as e:
                logger.error(f"Error fetching CSuite events: {str(e)}")
            
            # HubSpot Events
            logger.info("Fetching HubSpot marketing events...")
            try:
                hubspot_events = self.hubspot.get_marketing_events(limit=5)
                if 'results' in hubspot_events:
                    event_list = [
                        f"{e.get('eventName', 'Unknown')} ({e.get('startDateTime', 'No date')})"
                        for e in hubspot_events['results'][:5]
                    ]
                    context_parts.append(f"HubSpot Marketing Events:\n" + "\n".join(event_list))
                    logger.info(f"Found {len(event_list)} HubSpot events")
            except Exception as e:
                logger.error(f"Error fetching HubSpot events: {str(e)}")
        
        # Donation-related queries → CSuite
        if any(word in query_lower for word in ['donation', 'gift', 'gave', 'contributed', 'recent donations']):
            logger.info("Fetching CSuite donations...")
            try:
                donations_data = self.csuite.get_donations(limit=10)
                if donations_data.get('success') and donations_data.get('data'):
                    results = donations_data['data'].get('results', [])
                    donation_list = [
                        f"{d.get('name', 'Unknown')}: ${d.get('donation_amount', '0')} to {d.get('fund_name', 'Unknown')} ({d.get('donation_date', 'No date')})"
                        for d in results[:5]
                    ]
                    context_parts.append(f"CSuite Donations:\n" + "\n".join(donation_list))
                    logger.info(f"Found {len(donation_list)} donations")
            except Exception as e:
                logger.error(f"Error fetching donations: {str(e)}")
        
        # Ticket-related queries → HubSpot
        if any(word in query_lower for word in ['ticket', 'support', 'issue', 'help desk', 'open tickets']):
            logger.info("Fetching HubSpot tickets...")
            try:
                tickets_data = self.hubspot.get_tickets(limit=10)
                if 'results' in tickets_data:
                    ticket_list = []
                    for t in tickets_data['results'][:10]:
                        props = t.get('properties', {})
                        subject = props.get('subject', 'No subject')
                        status = props.get('hs_pipeline_stage', 'Unknown')
                        ticket_list.append(f"{subject} (Status: {status})")
                    if ticket_list:
                        context_parts.append(f"HubSpot Tickets:\n" + "\n".join(ticket_list))
                        logger.info(f"Found {len(ticket_list)} tickets")
            except Exception as e:
                logger.error(f"Error fetching tickets: {str(e)}")
        
        # Campaign-related queries → HubSpot
        if any(word in query_lower for word in ['campaign', 'marketing campaign']):
            logger.info("Fetching HubSpot campaigns...")
            try:
                campaigns_data = self.hubspot.get_campaigns(limit=10)
                if 'results' in campaigns_data:
                    campaign_list = [
                        f"Campaign ID: {c.get('id', 'Unknown')}"
                        for c in campaigns_data['results'][:5]
                    ]
                    if campaign_list:
                        context_parts.append(f"HubSpot Campaigns:\n" + "\n".join(campaign_list))
                        logger.info(f"Found {len(campaign_list)} campaigns")
            except Exception as e:
                logger.error(f"Error fetching campaigns: {str(e)}")
        
        result = "\n\n".join(context_parts) if context_parts else ""
        logger.info(f"Total context gathered: {len(result)} chars")
        return result
    
    def clear_history(self):
        """Clear conversation history"""
        logger.info("Clearing conversation history")
        self.conversation_history = []


# Singleton instance
_assistant = None

def get_assistant() -> JidhrAssistant:
    """Get or create the assistant instance"""
    global _assistant
    if _assistant is None:
        _assistant = JidhrAssistant()
    return _assistant
