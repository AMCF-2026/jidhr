"""
Jidhr Assistant
===============
The main brain that orchestrates queries across HubSpot and CSuite.
"""

from datetime import datetime
from config import SYSTEM_PROMPT
from clients import OpenRouterClient, HubSpotClient, CSuiteClient


class JidhrAssistant:
    """Main assistant that orchestrates queries across systems"""
    
    def __init__(self):
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
            enhanced_message = f"{user_message}\n\n[System Context]\n{context}"
            self.conversation_history[-1]["content"] = enhanced_message
        
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
        
        return response
    
    def _gather_context(self, query: str) -> str:
        """
        Gather relevant context from HubSpot/CSuite based on query.
        
        Analyzes the query for keywords and fetches relevant data
        to help Claude give better answers.
        """
        context_parts = []
        query_lower = query.lower()
        
        # Fund-related queries → CSuite
        if any(word in query_lower for word in ['fund', 'balance', 'daf', 'endowment', 'grant']):
            funds_data = self.csuite.get_funds(limit=20)
            if 'results' in funds_data:
                fund_list = [
                    f"{f.get('fund_name', 'Unknown')} (ID: {f.get('funit_id', 'N/A')})" 
                    for f in funds_data['results'][:10]
                ]
                context_parts.append(f"Recent funds from CSuite:\n" + "\n".join(fund_list))
        
        # Contact-related queries → HubSpot
        if any(word in query_lower for word in ['contact', 'donor', 'email', 'person', 'who']):
            contacts_data = self.hubspot.get_contacts(limit=10)
            if 'results' in contacts_data:
                contact_list = [
                    f"{c.get('properties', {}).get('firstname', '')} {c.get('properties', {}).get('lastname', '')} ({c.get('properties', {}).get('email', 'No email')})"
                    for c in contacts_data['results'][:5]
                ]
                context_parts.append(f"Recent contacts from HubSpot:\n" + "\n".join(contact_list))
        
        # Form-related queries → HubSpot
        if any(word in query_lower for word in ['form', 'submission', 'inquiry', 'submitted']):
            forms_data = self.hubspot.get_forms(limit=10)
            if 'results' in forms_data:
                form_list = [
                    f"{f.get('name', 'Unknown')} (ID: {f.get('id', 'N/A')})"
                    for f in forms_data['results'][:5]
                ]
                context_parts.append(f"Available forms from HubSpot:\n" + "\n".join(form_list))
        
        # Social media queries → HubSpot
        if any(word in query_lower for word in ['social', 'post', 'facebook', 'linkedin', 'schedule', 'channel']):
            channels_data = self.hubspot.get_social_channels()
            if isinstance(channels_data, list):
                channel_list = [
                    f"{c.get('name', 'Unknown')} ({c.get('channelType', 'Unknown')})"
                    for c in channels_data[:5]
                ]
                context_parts.append(f"Connected social channels:\n" + "\n".join(channel_list))
        
        # Event-related queries → HubSpot
        if any(word in query_lower for word in ['event', 'symposium', 'webinar', 'registration']):
            events_data = self.hubspot.get_marketing_events(limit=5)
            if 'results' in events_data:
                event_list = [
                    f"{e.get('eventName', 'Unknown')} ({e.get('startDateTime', 'No date')})"
                    for e in events_data['results'][:5]
                ]
                context_parts.append(f"Marketing events:\n" + "\n".join(event_list))
        
        # Donation-related queries → CSuite
        if any(word in query_lower for word in ['donation', 'gift', 'gave', 'contributed', 'recent donations']):
            donations_data = self.csuite.get_donations(limit=10)
            if 'results' in donations_data:
                donation_list = [
                    f"{d.get('name', 'Unknown')}: ${d.get('donation_amount', '0')} to {d.get('fund_name', 'Unknown')} ({d.get('donation_date', 'No date')})"
                    for d in donations_data['results'][:5]
                ]
                context_parts.append(f"Recent donations from CSuite:\n" + "\n".join(donation_list))
        
        return "\n\n".join(context_parts) if context_parts else ""
    
    def clear_history(self):
        """Clear conversation history"""
        self.conversation_history = []


# Singleton instance
_assistant = None

def get_assistant() -> JidhrAssistant:
    """Get or create the assistant instance"""
    global _assistant
    if _assistant is None:
        _assistant = JidhrAssistant()
    return _assistant
