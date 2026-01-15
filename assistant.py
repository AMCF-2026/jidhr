"""
Jidhr Assistant
===============
The main brain that orchestrates queries across HubSpot and CSuite.

Jidhr v1.2 - Added:
- Email draft creation (conversational)
- Social media post creation (conversational)
- Task creation
"""

import logging
import re
from datetime import datetime, timedelta
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
        
        # Draft state for conversational content creation (v1.2)
        self.draft_state = {
            "active": False,
            "type": None,  # "email" or "social"
            "subject": None,
            "body": None,
            "platform": None,  # for social: facebook, twitter, etc.
            "template": None,  # for email: amcf, giving circle
            "link_url": None,
            "photo_url": None,
        }
    
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
        
        # Check for content creation commands (v1.2)
        content_response = self._handle_content_commands(user_message)
        if content_response:
            self.conversation_history.append({"role": "user", "content": user_message})
            self.conversation_history.append({"role": "assistant", "content": content_response})
            return content_response
        
        # Check for draft refinement (if draft is active)
        if self.draft_state["active"]:
            draft_response = self._handle_draft_conversation(user_message)
            if draft_response:
                self.conversation_history.append({"role": "user", "content": user_message})
                self.conversation_history.append({"role": "assistant", "content": draft_response})
                return draft_response
        
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
    
    # =========================================================================
    # CONTENT CREATION HANDLERS (v1.2)
    # =========================================================================
    
    def _handle_content_commands(self, query: str) -> str:
        """
        Handle content creation commands (email drafts, social posts, tasks).
        
        Returns:
            Response string if content command detected, None otherwise
        """
        query_lower = query.lower().strip()
        
        # Task creation (simple, immediate)
        if self._is_task_creation(query_lower):
            return self._handle_task_creation(query)
        
        # Email draft initiation
        if self._is_email_draft_request(query_lower):
            return self._initiate_email_draft(query)
        
        # Social post initiation
        if self._is_social_post_request(query_lower):
            return self._initiate_social_post(query)
        
        return None
    
    def _is_task_creation(self, query: str) -> bool:
        """Check if query is a task creation request"""
        task_patterns = [
            'create a task', 'create task', 'add a task', 'add task',
            'new task', 'make a task', 'remind me to', 'set a reminder'
        ]
        return any(pattern in query for pattern in task_patterns)
    
    def _is_email_draft_request(self, query: str) -> bool:
        """Check if query is an email draft request"""
        email_patterns = [
            'draft an email', 'draft email', 'write an email', 'write email',
            'create an email', 'create email', 'compose an email', 'compose email',
            'email draft', 'marketing email', 'newsletter about'
        ]
        return any(pattern in query for pattern in email_patterns)
    
    def _is_social_post_request(self, query: str) -> bool:
        """Check if query is a social post request"""
        social_patterns = [
            'draft a post', 'write a post', 'create a post',
            'facebook post', 'linkedin post', 'twitter post', 'instagram post',
            'social media post', 'social post', 'draft a facebook', 'draft a linkedin',
            'draft a twitter', 'draft an instagram', 'write a facebook', 'write a linkedin'
        ]
        return any(pattern in query for pattern in social_patterns)
    
    def _handle_task_creation(self, query: str) -> str:
        """Create a task from natural language"""
        logger.info("Creating task from query...")
        
        # Use Claude to extract task details
        extraction_prompt = f"""Extract task details from this request. Return ONLY a JSON object with these fields:
- subject: The task title (required, be concise)
- body: Task description/details (optional, can be null)
- priority: LOW, MEDIUM, or HIGH (default MEDIUM)

Request: "{query}"

JSON:"""
        
        try:
            extraction = self.claude.chat(
                messages=[{"role": "user", "content": extraction_prompt}],
                system_prompt="You are a JSON extractor. Return only valid JSON, no markdown."
            )
            
            # Parse the JSON
            import json
            # Clean up potential markdown
            extraction = extraction.strip()
            if extraction.startswith("```"):
                extraction = extraction.split("```")[1]
                if extraction.startswith("json"):
                    extraction = extraction[4:]
            extraction = extraction.strip()
            
            task_data = json.loads(extraction)
            
            # Create the task
            result = self.hubspot.create_task_simple(
                subject=task_data.get("subject", "New Task"),
                body=task_data.get("body"),
                priority=task_data.get("priority", "MEDIUM")
            )
            
            if "error" in result:
                return f"❌ Failed to create task: {result['error']}"
            
            task_id = result.get("id", "Unknown")
            return f"""✅ **Task Created**

📋 **{task_data.get('subject', 'New Task')}**
• Priority: {task_data.get('priority', 'MEDIUM')}
• Status: Not Started

View in HubSpot: https://app-na2.hubspot.com/tasks/243832852/view/all"""
            
        except Exception as e:
            logger.error(f"Task creation error: {str(e)}")
            return f"❌ Failed to create task: {str(e)}"
    
    def _initiate_email_draft(self, query: str) -> str:
        """Start the email drafting conversation"""
        logger.info("Initiating email draft...")
        
        # Extract the topic from the query
        topic = self._extract_topic(query, "email")
        
        # Generate initial draft using Claude
        draft_prompt = f"""Write a marketing email for AMCF (American Muslim Community Foundation) about: {topic}

AMCF is a national community foundation that advances charitable giving through Donor-Advised Funds, Giving Circles, endowments, and fiscal sponsorships for the Muslim community.

Write:
1. A compelling subject line
2. The email body (2-3 paragraphs, warm but professional tone)

Format your response as:
SUBJECT: [subject line]

BODY:
[email body - can include basic HTML like <p>, <strong>, <a>]"""

        try:
            draft = self.claude.chat(
                messages=[{"role": "user", "content": draft_prompt}],
                system_prompt="You are a nonprofit marketing copywriter. Write warm, engaging emails."
            )
            
            # Parse subject and body
            subject, body = self._parse_email_draft(draft)
            
            # Store in draft state
            self.draft_state = {
                "active": True,
                "type": "email",
                "subject": subject,
                "body": body,
                "platform": None,
                "template": "amcf",  # default
                "link_url": None,
                "photo_url": None,
            }
            
            return f"""📧 **Email Draft**

**Subject:** {subject}

**Body:**
{self._html_to_display(body)}

---
💬 **What would you like to do?**
• Request changes: *"Make it shorter"*, *"Add more urgency"*, *"Include a call to action"*
• Save to HubSpot: *"Save this to the AMCF template"* or *"Save to Giving Circle template"*
• Start over: *"Start over"* or *"Cancel"*"""
            
        except Exception as e:
            logger.error(f"Email draft error: {str(e)}")
            return f"❌ Failed to generate email draft: {str(e)}"
    
    def _initiate_social_post(self, query: str) -> str:
        """Start the social post drafting conversation"""
        logger.info("Initiating social post draft...")
        
        # Detect platform from query
        platform = self._detect_platform(query)
        topic = self._extract_topic(query, "social")
        
        # Get character limits
        char_limits = {
            "twitter": 280,
            "facebook": 500,
            "linkedin": 700,
            "instagram": 450
        }
        limit = char_limits.get(platform, 500)
        
        # Generate initial draft using Claude
        draft_prompt = f"""Write a {platform or 'social media'} post for AMCF (American Muslim Community Foundation) about: {topic}

AMCF advances charitable giving through Donor-Advised Funds, Giving Circles, and endowments for the Muslim community.

Requirements:
- Keep it under {limit} characters
- Engaging, warm tone
- Include relevant hashtags for {platform or 'social media'}
- Include a call to action if appropriate

Write just the post content, nothing else."""

        try:
            draft = self.claude.chat(
                messages=[{"role": "user", "content": draft_prompt}],
                system_prompt="You are a social media manager for a nonprofit. Write engaging posts."
            )
            
            content = draft.strip()
            
            # Store in draft state
            self.draft_state = {
                "active": True,
                "type": "social",
                "subject": None,
                "body": content,
                "platform": platform,
                "template": None,
                "link_url": None,
                "photo_url": None,
            }
            
            # Get available platforms
            available = self.hubspot.get_available_social_platforms()
            platform_list = ", ".join(available) if available else "facebook, twitter, linkedin, instagram"
            
            platform_display = platform.title() if platform else "Social Media"
            
            return f"""📱 **{platform_display} Post Draft**

{content}

📊 Character count: {len(content)}

---
💬 **What would you like to do?**
• Request changes: *"Make it shorter"*, *"Add emojis"*, *"More professional tone"*
• Add link: *"Add link to [URL]"*
• Change platform: *"Switch to LinkedIn"* (Available: {platform_list})
• Schedule: *"Schedule for tomorrow at 5pm"*
• Post now: *"Post this now"* or *"Create as draft"*
• Start over: *"Start over"* or *"Cancel"*"""
            
        except Exception as e:
            logger.error(f"Social post draft error: {str(e)}")
            return f"❌ Failed to generate social post draft: {str(e)}"
    
    def _handle_draft_conversation(self, query: str) -> str:
        """Handle ongoing draft refinement conversation"""
        query_lower = query.lower().strip()
        
        # Cancel/start over
        if any(word in query_lower for word in ['cancel', 'start over', 'nevermind', 'forget it']):
            self._clear_draft_state()
            return "👍 Draft cancelled. Let me know if you'd like to start something new!"
        
        # Save email to HubSpot
        if self.draft_state["type"] == "email" and any(word in query_lower for word in ['save', 'create', 'done', 'looks good', 'that works']):
            return self._save_email_draft(query)
        
        # Post/schedule social
        if self.draft_state["type"] == "social":
            if any(word in query_lower for word in ['post now', 'publish', 'schedule', 'create as draft', 'save as draft', 'done', 'looks good']):
                return self._save_social_post(query)
            
            # Add link
            if 'add link' in query_lower or 'include link' in query_lower:
                return self._add_link_to_draft(query)
            
            # Change platform
            if 'switch to' in query_lower or 'change to' in query_lower:
                return self._change_platform(query)
        
        # Refinement request - use Claude to revise
        return self._refine_draft(query)
    
    def _refine_draft(self, feedback: str) -> str:
        """Refine the current draft based on user feedback"""
        draft_type = self.draft_state["type"]
        current_content = self.draft_state["body"]
        current_subject = self.draft_state.get("subject", "")
        
        if draft_type == "email":
            refine_prompt = f"""Revise this marketing email based on the feedback.

Current Subject: {current_subject}
Current Body:
{current_content}

Feedback: {feedback}

Return the revised email in this format:
SUBJECT: [revised subject line]

BODY:
[revised body]"""
        else:
            platform = self.draft_state.get("platform", "social media")
            refine_prompt = f"""Revise this {platform} post based on the feedback.

Current Post:
{current_content}

Feedback: {feedback}

Return only the revised post content, nothing else."""
        
        try:
            revised = self.claude.chat(
                messages=[{"role": "user", "content": refine_prompt}],
                system_prompt="You are a marketing copywriter. Make the requested changes."
            )
            
            if draft_type == "email":
                subject, body = self._parse_email_draft(revised)
                self.draft_state["subject"] = subject
                self.draft_state["body"] = body
                
                return f"""📧 **Revised Email Draft**

**Subject:** {subject}

**Body:**
{self._html_to_display(body)}

---
💬 How's this? You can request more changes or say *"Save to AMCF template"* when ready."""
            
            else:
                content = revised.strip()
                self.draft_state["body"] = content
                
                return f"""📱 **Revised Post**

{content}

📊 Character count: {len(content)}

---
💬 How's this? Request more changes, or say *"Post now"*, *"Schedule for [time]"*, or *"Create as draft"*."""
                
        except Exception as e:
            logger.error(f"Draft refinement error: {str(e)}")
            return f"❌ Failed to revise draft: {str(e)}"
    
    def _save_email_draft(self, query: str) -> str:
        """Save the email draft to HubSpot"""
        # Detect template from query
        template = "amcf"  # default
        query_lower = query.lower()
        if 'giving circle' in query_lower:
            template = "giving circle"
        
        subject = self.draft_state["subject"]
        body = self.draft_state["body"]
        
        # Convert plain text body to HTML if needed
        if not body.startswith("<"):
            body = f"<p>{body.replace(chr(10)+chr(10), '</p><p>').replace(chr(10), '<br>')}</p>"
        
        # Generate internal name
        name = f"{subject[:50]} - {datetime.now().strftime('%Y-%m-%d')}"
        
        try:
            result = self.hubspot.create_marketing_email_draft(
                name=name,
                subject=subject,
                body_html=body,
                template=template
            )
            
            if "error" in result:
                return f"❌ Failed to save email: {result['error']}"
            
            email_id = result.get("id", "Unknown")
            edit_url = result.get("edit_url", f"https://app-na2.hubspot.com/email/243832852/edit/{email_id}/content")
            
            self._clear_draft_state()
            
            template_display = "AMCF Emails" if template == "amcf" else "Giving Circle Email"
            
            return f"""✅ **Email Draft Saved to HubSpot!**

📧 **{subject}**
• Template: {template_display}
• Status: Draft (not sent)

✏️ **Edit in HubSpot:** {edit_url}

*You can now add images, adjust formatting, select recipients, and schedule/send from HubSpot.*"""
            
        except Exception as e:
            logger.error(f"Email save error: {str(e)}")
            return f"❌ Failed to save email: {str(e)}"
    
    def _save_social_post(self, query: str) -> str:
        """Save/schedule the social post to HubSpot"""
        query_lower = query.lower()
        platform = self.draft_state.get("platform", "facebook")
        content = self.draft_state["body"]
        link_url = self.draft_state.get("link_url")
        photo_url = self.draft_state.get("photo_url")
        
        # Determine schedule time
        schedule_time = None
        if 'schedule' in query_lower:
            schedule_time = self._parse_schedule_time(query)
        elif 'post now' in query_lower or 'publish now' in query_lower:
            schedule_time = datetime.now()
        # else: creates as draft (no schedule_time)
        
        try:
            result = self.hubspot.create_social_post(
                platform=platform,
                content=content,
                link_url=link_url,
                photo_url=photo_url,
                schedule_time=schedule_time
            )
            
            if "error" in result:
                return f"❌ Failed to create post: {result['error']}"
            
            self._clear_draft_state()
            
            # Format response based on action taken
            if schedule_time and schedule_time > datetime.now():
                time_str = schedule_time.strftime("%B %d at %I:%M %p")
                return f"""✅ **Post Scheduled!**

📱 **{platform.title()}**
📅 Scheduled for: {time_str}

{content[:100]}{'...' if len(content) > 100 else ''}

*View and manage in HubSpot Social.*"""
            
            elif schedule_time:
                return f"""✅ **Post Published!**

📱 **{platform.title()}**

{content[:100]}{'...' if len(content) > 100 else ''}"""
            
            else:
                return f"""✅ **Post Draft Created!**

📱 **{platform.title()}** (Draft)

{content[:100]}{'...' if len(content) > 100 else ''}

*Edit and schedule in HubSpot Social.*"""
            
        except Exception as e:
            logger.error(f"Social post save error: {str(e)}")
            return f"❌ Failed to create post: {str(e)}"
    
    def _add_link_to_draft(self, query: str) -> str:
        """Add a link to the current social post draft"""
        # Extract URL from query
        url_match = re.search(r'https?://[^\s]+', query)
        if url_match:
            url = url_match.group(0)
            self.draft_state["link_url"] = url
            return f"""✅ Link added: {url}

📱 **Current Draft:**

{self.draft_state['body']}

🔗 Link: {url}

---
💬 Ready to post? Say *"Post now"*, *"Schedule for tomorrow"*, or *"Create as draft"*."""
        else:
            return "❓ I didn't see a URL in your message. Try: *\"Add link https://amuslimcf.org/...\"*"
    
    def _change_platform(self, query: str) -> str:
        """Change the target platform for the social post"""
        new_platform = self._detect_platform(query)
        if new_platform:
            self.draft_state["platform"] = new_platform
            return f"""✅ Switched to **{new_platform.title()}**

📱 **Current Draft:**

{self.draft_state['body']}

📊 Character count: {len(self.draft_state['body'])}

---
💬 Ready? Say *"Post now"*, *"Schedule for [time]"*, or request changes."""
        else:
            available = self.hubspot.get_available_social_platforms()
            return f"❓ Which platform? Available: {', '.join(available)}"
    
    def _parse_schedule_time(self, query: str) -> datetime:
        """Parse a schedule time from natural language"""
        query_lower = query.lower()
        now = datetime.now()
        
        # Tomorrow
        if 'tomorrow' in query_lower:
            target = now + timedelta(days=1)
        elif 'next week' in query_lower:
            target = now + timedelta(weeks=1)
        else:
            target = now + timedelta(hours=1)  # default: 1 hour from now
        
        # Parse time
        time_match = re.search(r'(\d{1,2})(?::(\d{2}))?\s*(am|pm)?', query_lower)
        if time_match:
            hour = int(time_match.group(1))
            minute = int(time_match.group(2) or 0)
            period = time_match.group(3)
            
            if period == 'pm' and hour < 12:
                hour += 12
            elif period == 'am' and hour == 12:
                hour = 0
            
            target = target.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        return target
    
    def _extract_topic(self, query: str, context: str) -> str:
        """Extract the topic/subject from a content creation request"""
        # Remove common prefixes
        patterns = [
            r'draft an? (?:email|post) (?:about|for|on)\s+',
            r'write an? (?:email|post) (?:about|for|on)\s+',
            r'create an? (?:email|post) (?:about|for|on)\s+',
            r'(?:facebook|linkedin|twitter|instagram) post (?:about|for|on)\s+',
        ]
        
        result = query
        for pattern in patterns:
            result = re.sub(pattern, '', result, flags=re.IGNORECASE)
        
        return result.strip() or f"AMCF {context}"
    
    def _detect_platform(self, query: str) -> str:
        """Detect social media platform from query"""
        query_lower = query.lower()
        
        if 'facebook' in query_lower or 'fb' in query_lower:
            return 'facebook'
        elif 'linkedin' in query_lower:
            return 'linkedin'
        elif 'twitter' in query_lower or ' x ' in query_lower:
            return 'twitter'
        elif 'instagram' in query_lower or 'insta' in query_lower:
            return 'instagram'
        
        return 'facebook'  # default
    
    def _parse_email_draft(self, draft: str) -> tuple:
        """Parse subject and body from Claude's email draft response"""
        lines = draft.strip().split('\n')
        subject = ""
        body_lines = []
        in_body = False
        
        for line in lines:
            if line.upper().startswith('SUBJECT:'):
                subject = line.split(':', 1)[1].strip()
            elif line.upper().startswith('BODY:'):
                in_body = True
            elif in_body:
                body_lines.append(line)
        
        body = '\n'.join(body_lines).strip()
        
        # Fallback if parsing fails
        if not subject:
            subject = "AMCF Update"
        if not body:
            body = draft
        
        return subject, body
    
    def _html_to_display(self, html: str) -> str:
        """Convert HTML to displayable text for chat"""
        # Simple conversion for display
        text = html
        text = re.sub(r'<p>', '', text)
        text = re.sub(r'</p>', '\n\n', text)
        text = re.sub(r'<br\s*/?>', '\n', text)
        text = re.sub(r'<strong>', '**', text)
        text = re.sub(r'</strong>', '**', text)
        text = re.sub(r'<[^>]+>', '', text)  # remove other tags
        return text.strip()
    
    def _clear_draft_state(self):
        """Clear the draft state"""
        self.draft_state = {
            "active": False,
            "type": None,
            "subject": None,
            "body": None,
            "platform": None,
            "template": None,
            "link_url": None,
            "photo_url": None,
        }
    
    # =========================================================================
    # SYNC COMMAND HANDLERS (existing)
    # =========================================================================
    
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
            response += "\n\n📅 **Events:**"
            for detail in results['details'][:5]:
                response += f"\n• {detail}"
        
        return response
    
    def _format_newsletter_sync_results(self, results: dict, dry_run: bool) -> str:
        """Format newsletter sync results for display"""
        prefix = "🧪 **DRY RUN (Sample)** - " if dry_run else ""
        
        response = f"""{prefix}✅ **Newsletter Sync Complete**

📊 **Results:**
• **{results['subscribed']}** contacts {"would be subscribed" if dry_run else "subscribed"}
• **{results['already_subscribed']}** already subscribed
• **{results['skipped_not_found']}** not found in HubSpot
• **{results['errors']}** errors"""
        
        if dry_run:
            response += "\n\n⚡ *This dry run used sample data. Run `sync newsletter` without 'dry run' for full sync.*"
        
        return response
    
    def _run_all_syncs(self) -> str:
        """Run all sync operations"""
        responses = []
        
        try:
            donation_results = run_donation_sync(dry_run=False)
            responses.append(f"✅ Donations: {donation_results['updated']} updated")
        except Exception as e:
            responses.append(f"❌ Donations: {str(e)}")
        
        try:
            event_results = run_event_sync(dry_run=False)
            responses.append(f"✅ Events: {event_results['created']} created")
        except Exception as e:
            responses.append(f"❌ Events: {str(e)}")
        
        try:
            newsletter_results = run_newsletter_sync(dry_run=False)
            responses.append(f"✅ Newsletter: {newsletter_results['subscribed']} subscribed")
        except Exception as e:
            responses.append(f"❌ Newsletter: {str(e)}")
        
        return "✅ **All Syncs Complete**\n\n" + "\n".join(responses)
    
    # =========================================================================
    # CONTEXT GATHERING (existing)
    # =========================================================================
    
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
        
        # Task-related queries → HubSpot (NEW in v1.2)
        if any(word in query_lower for word in ['task', 'tasks', 'to do', 'todo', 'my tasks']):
            logger.info("Fetching HubSpot tasks...")
            try:
                tasks_data = self.hubspot.get_tasks(limit=10)
                if 'results' in tasks_data:
                    task_list = []
                    for t in tasks_data['results'][:10]:
                        props = t.get('properties', {})
                        subject = props.get('hs_task_subject', 'No subject')
                        status = props.get('hs_task_status', 'Unknown')
                        task_list.append(f"{subject} (Status: {status})")
                    if task_list:
                        context_parts.append(f"HubSpot Tasks:\n" + "\n".join(task_list))
                        logger.info(f"Found {len(task_list)} tasks")
            except Exception as e:
                logger.error(f"Error fetching tasks: {str(e)}")
        
        result = "\n\n".join(context_parts) if context_parts else ""
        logger.info(f"Total context gathered: {len(result)} chars")
        return result
    
    def clear_history(self):
        """Clear conversation history and draft state"""
        logger.info("Clearing conversation history and draft state")
        self.conversation_history = []
        self._clear_draft_state()


# Singleton instance
_assistant = None

def get_assistant() -> JidhrAssistant:
    """Get or create the assistant instance"""
    global _assistant
    if _assistant is None:
        _assistant = JidhrAssistant()
    return _assistant
