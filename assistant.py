"""
Jidhr Assistant
===============
The main brain that orchestrates queries across HubSpot and CSuite.
"""

import logging
from datetime import datetime
from config import SYSTEM_PROMPT
from clients import OpenRouterClient, HubSpotClient, CSuiteClient

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
            funds_data = self.csuite.get_funds(limit=20)
            if 'results' in funds_data:
                fund_list = [
                    f"{f.get('fund_name', 'Unknown')} (ID: {f.get('funit_id', 'N/A')})" 
                    for f in funds_data['results'][:10]
                ]
                context_parts.append(f"CSuite Funds:\n" + "\n".join(fund_list))
                logger.info(f"Found {len(fund_list)} funds")
        
        # Contact-related queries → HubSpot
        if any(word in query_lower for word in ['contact', 'donor', 'email', 'person', 'who']):
            logger.info("Fetching HubSpot contacts...")
            contacts_data = self.hubspot.get_contacts(limit=10)
            if 'results' in contacts_data:
                contact_list = [
                    f"{c.get('properties', {}).get('firstname', '')} {c.get('properties', {}).get('lastname', '')} ({c.get('properties', {}).get('email', 'No email')})"
                    for c in contacts_data['results'][:5]
                ]
                context_parts.append(f"HubSpot Contacts:\n" + "\n".join(contact_list))
                logger.info(f"Found {len(contact_list)} contacts")
        
        # Form-related queries → HubSpot
        if any(word in query_lower for word in ['form', 'submission', 'inquiry', 'submitted']):
            logger.info("Fetching HubSpot forms...")
            forms_data = self.hubspot.get_forms(limit=10)
            if 'results' in forms_data:
                form_list = [
                    f"{f.get('name', 'Unknown')} (ID: {f.get('id', 'N/A')})"
                    for f in forms_data['results'][:5]
                ]
                context_parts.append(f"HubSpot Forms:\n" + "\n".join(form_list))
                logger.info(f"Found {len(form_list)} forms")
        
        # Social media queries → HubSpot
        if any(word in query_lower for word in ['social', 'post', 'facebook', 'linkedin', 'schedule', 'channel']):
            logger.info("Fetching HubSpot social channels...")
            channels_data = self.hubspot.get_social_channels()
            if isinstance(channels_data, list):
                channel_list = [
                    f"{c.get('name', 'Unknown')} ({c.get('channelType', 'Unknown')})"
                    for c in channels_data[:5]
                ]
                context_parts.append(f"Social Channels:\n" + "\n".join(channel_list))
                logger.info(f"Found {len(channel_list)} channels")
        
        # Event-related queries → BOTH CSuite AND HubSpot
        if any(word in query_lower for word in ['event', 'symposium', 'webinar', 'registration', 'gala', 'dinner']):
            # CSuite Events
            logger.info("Fetching CSuite events...")
            csuite_events = self.csuite.get_events(limit=10)
            if 'results' in csuite_events:
                event_list = [
                    f"{e.get('event_name', 'Unknown')} ({e.get('event_date', 'No date')})"
                    for e in csuite_events['results'][:5]
                ]
                context_parts.append(f"CSuite Events:\n" + "\n".join(event_list))
                logger.info(f"Found {len(event_list)} CSuite events")
            
            # HubSpot Events
            logger.info("Fetching HubSpot marketing events...")
            hubspot_events = self.hubspot.get_marketing_events(limit=5)
            if 'results' in hubspot_events:
                event_list = [
                    f"{e.get('eventName', 'Unknown')} ({e.get('startDateTime', 'No date')})"
                    for e in hubspot_events['results'][:5]
                ]
                context_parts.append(f"HubSpot Marketing Events:\n" + "\n".join(event_list))
                logger.info(f"Found {len(event_list)} HubSpot events")
        
        # Donation-related queries → CSuite
        if any(word in query_lower for word in ['donation', 'gift', 'gave', 'contributed', 'recent donations']):
            logger.info("Fetching CSuite donations...")
            donations_data = self.csuite.get_donations(limit=10)
            if 'results' in donations_data:
                donation_list = [
                    f"{d.get('name', 'Unknown')}: ${d.get('donation_amount', '0')} to {d.get('fund_name', 'Unknown')} ({d.get('donation_date', 'No date')})"
                    for d in donations_data['results'][:5]
                ]
                context_parts.append(f"CSuite Donations:\n" + "\n".join(donation_list))
                logger.info(f"Found {len(donation_list)} donations")
        
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
