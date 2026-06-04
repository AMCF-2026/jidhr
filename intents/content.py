"""
Jidhr Content Creation
======================
Handles email drafting, social media posts, and task creation.
Owns the draft_state lifecycle (email/social conversational flow).

Extracted from assistant.py lines 120-721 — logic unchanged.
"""

import logging
import re
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dateutil import parser as dateutil_parser

from config import ORG_FACTS_PROMPT
from content.content_analysis import find_topic_matches
from content.queue_check import check_schedule, get_queue, suggest_slot

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Trigger patterns
# ---------------------------------------------------------------------------

TASK_PATTERNS = [
    'create a task', 'create task', 'add a task', 'add task',
    'new task', 'make a task', 'remind me to', 'set a reminder',
]

EMAIL_PATTERNS = [
    'draft an email', 'draft email', 'write an email', 'write email',
    'create an email', 'create email', 'compose an email', 'compose email',
    'email draft', 'marketing email', 'newsletter about',
    'write a newsletter', 'write the newsletter', 'draft a newsletter',
    'draft the newsletter', 'create a newsletter', 'write newsletter',
    'draft newsletter', 'newsletter draft', 'newsletter blurb',
    'weekly newsletter', 'monthly newsletter', 'amcf newsletter',
]

SOCIAL_PATTERNS = [
    'draft a post', 'write a post', 'create a post',
    'facebook post', 'linkedin post', 'twitter post', 'instagram post',
    'social media post', 'social post', 'draft a facebook', 'draft a linkedin',
    'draft a twitter', 'draft an instagram', 'write a facebook', 'write a linkedin',
]

# Follow-up commands that should resolve against pending content, not start new intents
FOLLOWUP_PATTERNS = [
    'create as draft', 'save as draft', 'post now', 'publish now',
    'schedule it', 'schedule for', 'make it shorter', 'make it longer',
    'save draft', 'post it', 'publish it', 'looks good', 'that works',
    'send it', 'send now', 'done with it',
    'save to template', 'save this to', 'save to amcf', 'save to giving circle',
    'add link', 'include link', 'switch to', 'change to',
    'more professional', 'add emojis', 'less formal', 'more formal',
]


# ---------------------------------------------------------------------------
# Registry interface
# ---------------------------------------------------------------------------

def can_handle(query: str, draft_state: dict = None, **kwargs) -> bool:
    """
    Check if query is a content-creation command OR if a draft is active.
    """
    q = query.lower().strip()

    # New content initiation
    if _is_task_creation(q):
        return True
    if _is_email_draft_request(q):
        return True
    if _is_social_post_request(q):
        return True

    # Active draft conversation (refinement, save, cancel, etc.)
    if draft_state and draft_state.get("active"):
        return True

    # Follow-up commands — only match if there's a pending draft
    if _is_followup_command(q):
        return True

    return False


def handle(query: str, assistant) -> str:
    """
    Route to the appropriate content handler.

    Args:
        query: The user's message
        assistant: JidhrAssistant instance (provides .claude, .hubspot,
                   .draft_state and helper access)

    Returns:
        Response string
    """
    q = query.lower().strip()

    # Task creation (immediate, no draft flow)
    if _is_task_creation(q):
        return _handle_task_creation(query, assistant)

    # Email draft initiation
    if _is_email_draft_request(q):
        return _initiate_email_draft(query, assistant)

    # Social post initiation
    if _is_social_post_request(q):
        return _initiate_social_post(query, assistant)

    # Active draft — route to conversational handler
    if assistant.draft_state.get("active"):
        return _handle_draft_conversation(query, assistant)

    # Follow-up command with no active draft — tell user clearly
    if _is_followup_command(q):
        return (
            "I don't have a recent draft to act on — my draft context "
            "may have been lost between requests. Please start fresh:\n\n"
            "- \"Draft a LinkedIn post about our upcoming event\"\n"
            "- \"Write an email to thank Ramadan donors\"\n"
            "- \"Create a social post about EverWaqf\"\n\n"
            "Once I generate a draft, you can use commands like "
            "\"Post now\", \"Create as draft\", \"Save to AMCF template\", etc."
        )

    return "❌ Content command not recognised."


# ---------------------------------------------------------------------------
# Detection helpers
# ---------------------------------------------------------------------------

def _is_task_creation(query: str) -> bool:
    return any(p in query for p in TASK_PATTERNS)


def _is_email_draft_request(query: str) -> bool:
    return any(p in query for p in EMAIL_PATTERNS)


def _is_social_post_request(query: str) -> bool:
    return any(p in query for p in SOCIAL_PATTERNS)


def _is_followup_command(query: str) -> bool:
    return any(p in query for p in FOLLOWUP_PATTERNS)


# ---------------------------------------------------------------------------
# Task creation (immediate)
# ---------------------------------------------------------------------------

def _handle_task_creation(query: str, assistant) -> str:
    """Create a task from natural language."""
    logger.info("Creating task from query...")

    extraction_prompt = f"""Extract task details from this request. Return ONLY a JSON object with these fields:
- subject: The task title (required, be concise)
- body: Task description/details (optional, can be null)
- priority: LOW, MEDIUM, or HIGH (default MEDIUM)

Request: "{query}"

JSON:"""

    try:
        extraction = assistant.claude.chat(
            messages=[{"role": "user", "content": extraction_prompt}],
            system_prompt="You are a JSON extractor. Return only valid JSON, no markdown.",
        )

        # Clean up potential markdown fences
        extraction = extraction.strip()
        if extraction.startswith("```"):
            extraction = extraction.split("```")[1]
            if extraction.startswith("json"):
                extraction = extraction[4:]
        extraction = extraction.strip()

        task_data = json.loads(extraction)

        result = assistant.hubspot.create_task_simple(
            subject=task_data.get("subject", "New Task"),
            body=task_data.get("body"),
            priority=task_data.get("priority", "MEDIUM"),
        )

        if "error" in result:
            return f"❌ Failed to create task: {result['error']}"

        return f"""✅ **Task Created**

📋 **{task_data.get('subject', 'New Task')}**
• Priority: {task_data.get('priority', 'MEDIUM')}
• Status: Not Started

View in HubSpot: https://app-na2.hubspot.com/tasks/243832852/view/all"""

    except Exception as e:
        logger.error(f"Task creation error: {e}")
        return f"❌ Failed to create task: {e}"


# ---------------------------------------------------------------------------
# Repetition heads-up (pre-draft check via content.content_analysis)
# ---------------------------------------------------------------------------

# Channel abbreviations mirror content_report's style so the pre-draft
# heads-up reads consistently with the broader content report view.
_REPETITION_CHANNEL_ABBR = {
    "facebook":        "FB",
    "instagram":       "IG",
    "linkedin":        "LI",
    "youtube":         "YT",
    "twitter":         "TW",
    "giving_circle":   "GC",
    "amcf_newsletter": "NL",
    "email":           "EM",
    "unknown":         "?",
}


def _abbr_channel_for_note(c) -> str:
    if not isinstance(c, str) or not c:
        return "?"
    return _REPETITION_CHANNEL_ABBR.get(c.lower(), c[:2].upper())


def _repetition_note(topic) -> str | None:
    """Build a one-line heads-up if the topic was covered in the last 6 weeks.

    Returns None on no matches, empty/invalid topic, or any exception —
    drafting must NEVER fail because of this informational helper.
    """
    try:
        if not isinstance(topic, str) or not topic.strip():
            return None
        matches = find_topic_matches(topic, days=42)
        if not matches:
            return None

        per_channel: dict[str, int] = {}
        for m in matches:
            ch = m.get("channel") or "unknown"
            per_channel[ch] = per_channel.get(ch, 0) + 1
        channel_pairs = sorted(per_channel.items(), key=lambda kv: (-kv[1], kv[0]))
        channel_str = ", ".join(
            f"{_abbr_channel_for_note(c)} {n}" for c, n in channel_pairs
        )

        timestamps = [m.get("sent_at") for m in matches if m.get("sent_at") is not None]
        if timestamps:
            days_ago = (datetime.utcnow() - max(timestamps)).days
            recency = f"most recent {days_ago}d ago"
        else:
            recency = "recent"

        return (
            f"ℹ️ Heads-up: this topic appeared {len(matches)}× in the last "
            f"6 weeks ({channel_str} — {recency})."
        )
    except Exception as e:
        logger.warning(f"_repetition_note failed for topic={topic!r}: {e}")
        return None


def _queue_note(topic) -> str | None:
    """One-line heads-up if a queued (WAITING) post mentions the topic.

    Substring containment AFTER lowercasing+stripping punctuation —
    deliberately NOT queue_check._text_similarity, which guards against
    inputs with < 4 words. Topics are often 2–3 words ("EverWaqf launch",
    "Ramadan giving"); we want those to fire.

    Same contract as _repetition_note: returns None on no matches,
    empty/invalid topic, or any exception. Drafting must NEVER fail
    because of this informational helper.
    """
    try:
        if not isinstance(topic, str) or not topic.strip():
            return None
        norm_topic = re.sub(r"[^a-z0-9\s]", " ", topic.lower())
        norm_topic = re.sub(r"\s+", " ", norm_topic).strip()
        if not norm_topic:
            return None

        queue = get_queue()
        if not queue:
            return None

        matches = []
        for item in queue:
            body = item.get("body")
            if not isinstance(body, str):
                continue
            norm_body = re.sub(r"[^a-z0-9\s]", " ", body.lower())
            norm_body = re.sub(r"\s+", " ", norm_body).strip()
            if norm_topic in norm_body:
                matches.append(item)

        if not matches:
            return None

        matches.sort(key=lambda m: m.get("trigger_at_et") or datetime.max)
        parts = []
        for m in matches:
            dt = m.get("trigger_at_et")
            if dt is None:
                continue
            parts.append(
                f"{_abbr_channel_for_note(m.get('channel'))} {dt.month}/{dt.day}"
            )
        if not parts:
            return None

        return f"📅 Already in the queue: {topic} — {', '.join(parts)}."
    except Exception as e:
        logger.warning(f"_queue_note failed for topic={topic!r}: {e}")
        return None


# ---------------------------------------------------------------------------
# Email draft lifecycle
# ---------------------------------------------------------------------------

def _initiate_email_draft(query: str, assistant) -> str:
    """Start the email drafting conversation."""
    logger.info("Initiating email draft...")

    topic = _extract_topic(query, "email")

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
        draft = assistant.claude.chat(
            messages=[{"role": "user", "content": draft_prompt}],
            system_prompt="You are a nonprofit marketing copywriter. Write warm, engaging emails." + ORG_FACTS_PROMPT,
        )

        subject, body = _parse_email_draft(draft)

        assistant.draft_state.update({
            "active": True,
            "type": "email",
            "subject": subject,
            "body": body,
            "platform": None,
            "template": "amcf",
            "link_url": None,
            "photo_url": None,
        })

        response = f"""📧 **Email Draft**

**Subject:** {subject}

**Body:**
{_html_to_display(body)}

---
💬 **What would you like to do?**
• Request changes: *"Make it shorter"*, *"Add more urgency"*, *"Include a call to action"*
• Save to HubSpot: *"Save this to the AMCF template"* or *"Save to Giving Circle template"*
• Start over: *"Start over"* or *"Cancel"*"""

        parts = []
        rep_note = _repetition_note(topic)
        if rep_note:
            parts.append(rep_note)
        q_note = _queue_note(topic)
        if q_note:
            parts.append(q_note)
        parts.append(response)
        return "\n\n".join(parts)

    except Exception as e:
        logger.error(f"Email draft error: {e}")
        return f"❌ Failed to generate email draft: {e}"


def _save_email_draft(query: str, assistant) -> str:
    """Save the email draft to HubSpot."""
    template = "amcf"
    if 'giving circle' in query.lower():
        template = "giving circle"

    subject = assistant.draft_state["subject"]
    body = assistant.draft_state["body"]

    # Convert plain text body to HTML if needed
    if not body.startswith("<"):
        body = f"<p>{body.replace(chr(10)+chr(10), '</p><p>').replace(chr(10), '<br>')}</p>"

    name = f"{subject[:50]} - {datetime.now().strftime('%Y-%m-%d')}"

    try:
        result = assistant.hubspot.create_marketing_email_draft(
            name=name,
            subject=subject,
            body_html=body,
            template=template,
        )

        if "error" in result:
            return f"❌ Failed to save email: {result['error']}"

        email_id = result.get("id", "Unknown")
        edit_url = result.get(
            "edit_url",
            f"https://app-na2.hubspot.com/email/243832852/edit/{email_id}/content",
        )

        _clear_draft_state(assistant)

        template_display = "AMCF Emails" if template == "amcf" else "Giving Circle Email"

        return f"""✅ **Email Draft Saved to HubSpot!**

📧 **{subject}**
• Template: {template_display}
• Status: Draft (not sent)

✏️ **Edit in HubSpot:** {edit_url}

*You can now add images, adjust formatting, select recipients, and schedule/send from HubSpot.*"""

    except Exception as e:
        logger.error(f"Email save error: {e}")
        return f"❌ Failed to save email: {e}"


# ---------------------------------------------------------------------------
# Social post lifecycle
# ---------------------------------------------------------------------------

def _initiate_social_post(query: str, assistant) -> str:
    """Start the social post drafting conversation."""
    logger.info("Initiating social post draft...")

    platform = _detect_platform(query)
    topic = _extract_topic(query, "social")

    char_limits = {
        "twitter": 280,
        "facebook": 500,
        "linkedin": 700,
        "instagram": 450,
    }
    limit = char_limits.get(platform, 500)

    draft_prompt = f"""Write a {platform or 'social media'} post for AMCF (American Muslim Community Foundation) about: {topic}

AMCF advances charitable giving through Donor-Advised Funds, Giving Circles, and endowments for the Muslim community.

Requirements:
- Keep it under {limit} characters
- Engaging, warm tone
- Include relevant hashtags for {platform or 'social media'}
- Include a call to action if appropriate

Write just the post content, nothing else."""

    try:
        draft = assistant.claude.chat(
            messages=[{"role": "user", "content": draft_prompt}],
            system_prompt="You are a social media manager for a nonprofit. Write engaging posts." + ORG_FACTS_PROMPT,
        )

        content = draft.strip()

        assistant.draft_state.update({
            "active": True,
            "type": "social",
            "subject": None,
            "body": content,
            "platform": platform,
            "template": None,
            "link_url": None,
            "photo_url": None,
        })

        available = assistant.hubspot.get_available_social_platforms()
        platform_list = ", ".join(available) if available else "facebook, twitter, linkedin, instagram"
        platform_display = platform.title() if platform else "Social Media"

        response = f"""📱 **{platform_display} Post Draft**

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

        parts = []
        rep_note = _repetition_note(topic)
        if rep_note:
            parts.append(rep_note)
        q_note = _queue_note(topic)
        if q_note:
            parts.append(q_note)
        parts.append(response)
        return "\n\n".join(parts)

    except Exception as e:
        logger.error(f"Social post draft error: {e}")
        return f"❌ Failed to generate social post draft: {e}"


_RULE_LABEL = {
    "slot_collision":         "Slot collision",
    "same_day_cross_network": "Same-day cross-network",
}


def _format_cadence_violations(violations, platform, requested_dt) -> str:
    """Render the blocking cadence-violation message."""
    lines = [
        "🚧 **Cadence check flagged conflicts — post not sent yet.**",
        "",
    ]
    for v in violations:
        label = _RULE_LABEL.get(v.get("rule"), v.get("rule") or "?")
        ch = v.get("channel") or "?"
        et = v.get("trigger_at_et") or "?"
        matched = v.get("matched_on") or "?"
        excerpt = (v.get("excerpt") or "").strip()
        lines.append(f"• **{label}** ({matched}): {_abbr_channel_for_note(ch)} @ {et}")
        if excerpt:
            lines.append(f"  > {excerpt}")

    sug = None
    if isinstance(requested_dt, datetime):
        try:
            sug = suggest_slot(platform, requested_dt)
        except Exception as e:
            logger.warning(f"suggest_slot failed (display only): {e}")
            sug = None
    if sug is not None:
        lines.append("")
        lines.append(f"💡 Suggested clean slot: **{sug.strftime('%B %d at %I:%M %p')} ET**")

    lines.append("")
    lines.append('Reply **"schedule anyway"** to override, or give a new time.')
    return "\n".join(lines)


def _execute_override(assistant) -> str:
    """Bypass the cadence gate using the stashed pending_schedule.

    Called from _handle_draft_conversation when the user types
    "schedule anyway" / "override and schedule" after a violation
    message. Always pops pending_schedule (success or failure).
    """
    pending = assistant.draft_state.get("pending_schedule") or {}
    platform = pending.get("platform", "facebook")
    body = pending.get("content", "")
    link_url = pending.get("link_url")
    photo_url = pending.get("photo_url")
    schedule_iso = pending.get("schedule_time_iso")

    try:
        result = assistant.hubspot.create_social_post(
            platform=platform,
            content=body,
            link_url=link_url,
            photo_url=photo_url,
            schedule_time=schedule_iso,  # V5.6 _coerce_trigger_at_ms parses ISO
        )
    except Exception as e:
        logger.error(f"Override save error: {e}")
        assistant.draft_state.pop("pending_schedule", None)
        return f"❌ Failed to create post: {e}"

    assistant.draft_state.pop("pending_schedule", None)

    if "error" in result:
        return f"❌ Failed to create post: {result['error']}"

    _clear_draft_state(assistant)
    excerpt = body[:100] + ("..." if len(body) > 100 else "")

    # Echo the stashed schedule so the operator sees what they're confirming.
    schedule_echo = ""
    if isinstance(schedule_iso, str):
        try:
            schedule_echo = _format_et_schedule(datetime.fromisoformat(schedule_iso))
        except (ValueError, TypeError):
            schedule_echo = ""
    schedule_line = f"\n📅 Scheduled for: **{schedule_echo}**" if schedule_echo else ""

    return f"""✅ **Post Scheduled (cadence override)!**

📱 **{platform.title()}**{schedule_line}

{excerpt}

*Rule-1/rule-2 conflicts were bypassed at your request. View in HubSpot Social.*"""


def _save_social_post(query: str, assistant) -> str:
    """Save/schedule the social post to HubSpot."""
    query_lower = query.lower()
    platform = assistant.draft_state.get("platform", "facebook")
    content = assistant.draft_state["body"]
    link_url = assistant.draft_state.get("link_url")
    photo_url = assistant.draft_state.get("photo_url")

    # Determine schedule time
    schedule_time = None
    parse_failed = False
    if 'schedule' in query_lower:
        parsed = _parse_schedule_time(query)
        if parsed is None:
            parse_failed = True
        else:
            schedule_time = parsed
    elif 'post now' in query_lower or 'publish now' in query_lower:
        # ET-aware now so the echo + V5.6 _coerce_trigger_at_ms agree on tz.
        schedule_time = datetime.now(_ET_TZ).replace(tzinfo=None)
    # else: creates as draft (no schedule_time)

    if parse_failed:
        # Keep draft + pending_schedule intact; ask the user to restate.
        return (
            "⏰ I couldn't parse the time from that. Try formats like:\n"
            '• *"Schedule for tomorrow at 5pm"*\n'
            '• *"Schedule for June 21 at 10am"*\n'
            '• *"Schedule for Monday 10am"*\n'
            '• *"Schedule for 2026-06-21 10:00"*\n\n'
            "Your draft is still here — say a new time when ready."
        )

    # Cadence gate — runs only when there's an effective trigger time.
    # Drafts (no schedule_time) skip the gate by design.
    # Fail-open: any check error logs + proceeds. The rules are advisory.
    if schedule_time is not None:
        try:
            violations = check_schedule(
                body=content,
                link=link_url,
                channel=platform,
                trigger_at=schedule_time,
            )
        except Exception as e:
            logger.error(f"check_schedule failed (fail-open): {e}", exc_info=True)
            violations = []

        if violations:
            schedule_iso = (
                schedule_time.isoformat()
                if isinstance(schedule_time, datetime)
                else str(schedule_time)
            )
            assistant.draft_state["pending_schedule"] = {
                "platform":          platform,
                "content":           content,
                "link_url":          link_url,
                "photo_url":         photo_url,
                "schedule_time_iso": schedule_iso,
            }
            return _format_cadence_violations(violations, platform, schedule_time)

    # Clean: clear any stale pending_schedule from a previous gate hit.
    assistant.draft_state.pop("pending_schedule", None)

    try:
        result = assistant.hubspot.create_social_post(
            platform=platform,
            content=content,
            link_url=link_url,
            photo_url=photo_url,
            schedule_time=schedule_time,
        )

        if "error" in result:
            return f"❌ Failed to create post: {result['error']}"

        _clear_draft_state(assistant)

        # Compare schedule_time (naive ET) against a naive-ET "now".
        # Mixing naive-ET with datetime.now() (naive UTC on Railway) would
        # miscategorise a 5pm-ET post as "already past" by 4–5 hours.
        now_et_naive = datetime.now(_ET_TZ).replace(tzinfo=None)

        if schedule_time and schedule_time > now_et_naive:
            schedule_echo = _format_et_schedule(schedule_time)
            return f"""✅ **Post Scheduled!**

📱 **{platform.title()}**
📅 Scheduled for: **{schedule_echo}**

{content[:100]}{'...' if len(content) > 100 else ''}

*View and manage in HubSpot Social.*"""

        elif schedule_time:
            schedule_echo = _format_et_schedule(schedule_time)
            return f"""✅ **Post Published!**

📱 **{platform.title()}**
🕒 At: **{schedule_echo}**

{content[:100]}{'...' if len(content) > 100 else ''}"""

        else:
            return f"""✅ **Post Draft Created!**

📱 **{platform.title()}** (Draft)

{content[:100]}{'...' if len(content) > 100 else ''}

*Edit and schedule in HubSpot Social.*"""

    except Exception as e:
        logger.error(f"Social post save error: {e}")
        return f"❌ Failed to create post: {e}"


def _add_link_to_draft(query: str, assistant) -> str:
    """Add a link to the current social post draft."""
    url_match = re.search(r'https?://[^\s]+', query)
    if url_match:
        url = url_match.group(0)
        assistant.draft_state["link_url"] = url
        return f"""✅ Link added: {url}

📱 **Current Draft:**

{assistant.draft_state['body']}

🔗 Link: {url}

---
💬 Ready to post? Say *"Post now"*, *"Schedule for tomorrow"*, or *"Create as draft"*."""
    else:
        return '❓ I didn\'t see a URL in your message. Try: *"Add link https://amuslimcf.org/..."*'


def _change_platform(query: str, assistant) -> str:
    """Change the target platform for the social post."""
    new_platform = _detect_platform(query)
    if new_platform:
        assistant.draft_state["platform"] = new_platform
        return f"""✅ Switched to **{new_platform.title()}**

📱 **Current Draft:**

{assistant.draft_state['body']}

📊 Character count: {len(assistant.draft_state['body'])}

---
💬 Ready? Say *"Post now"*, *"Schedule for [time]"*, or request changes."""
    else:
        available = assistant.hubspot.get_available_social_platforms()
        return f"❓ Which platform? Available: {', '.join(available)}"


# ---------------------------------------------------------------------------
# Draft conversation router
# ---------------------------------------------------------------------------

def _handle_draft_conversation(query: str, assistant) -> str:
    """Handle ongoing draft refinement conversation."""
    query_lower = query.lower().strip()

    # Cancel / start over
    if any(w in query_lower for w in ['cancel', 'start over', 'nevermind', 'forget it']):
        _clear_draft_state(assistant)
        return "👍 Draft cancelled. Let me know if you'd like to start something new!"

    # Save email to HubSpot
    if assistant.draft_state["type"] == "email" and any(
        w in query_lower for w in ['save', 'create', 'done', 'looks good', 'that works']
    ):
        return _save_email_draft(query, assistant)

    # Cadence override — must run BEFORE the generic "schedule" matcher
    # below, since "schedule anyway" contains "schedule".
    if assistant.draft_state.get("type") == "social" and (
        "schedule anyway" in query_lower
        or "override and schedule" in query_lower
    ):
        if assistant.draft_state.get("pending_schedule"):
            return _execute_override(assistant)
        # No stashed pending → fall through to normal handling.

    # Post/schedule social
    if assistant.draft_state["type"] == "social":
        if any(w in query_lower for w in [
            'post now', 'publish', 'schedule', 'create as draft',
            'save as draft', 'done', 'looks good',
        ]):
            return _save_social_post(query, assistant)

        if 'add link' in query_lower or 'include link' in query_lower:
            return _add_link_to_draft(query, assistant)

        if 'switch to' in query_lower or 'change to' in query_lower:
            return _change_platform(query, assistant)

    # Refinement request — use Claude to revise
    return _refine_draft(query, assistant)


# ---------------------------------------------------------------------------
# Draft refinement
# ---------------------------------------------------------------------------

def _refine_draft(feedback: str, assistant) -> str:
    """Refine the current draft based on user feedback."""
    draft_type = assistant.draft_state["type"]
    current_content = assistant.draft_state["body"]
    current_subject = assistant.draft_state.get("subject", "")

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
        platform = assistant.draft_state.get("platform", "social media")
        refine_prompt = f"""Revise this {platform} post based on the feedback.

Current Post:
{current_content}

Feedback: {feedback}

Return only the revised post content, nothing else."""

    try:
        revised = assistant.claude.chat(
            messages=[{"role": "user", "content": refine_prompt}],
            system_prompt="You are a marketing copywriter. Make the requested changes." + ORG_FACTS_PROMPT,
        )

        if draft_type == "email":
            subject, body = _parse_email_draft(revised)
            assistant.draft_state["subject"] = subject
            assistant.draft_state["body"] = body

            return f"""📧 **Revised Email Draft**

**Subject:** {subject}

**Body:**
{_html_to_display(body)}

---
💬 How's this? You can request more changes or say *"Save to AMCF template"* when ready."""

        else:
            content = revised.strip()
            assistant.draft_state["body"] = content

            return f"""📱 **Revised Post**

{content}

📊 Character count: {len(content)}

---
💬 How's this? Request more changes, or say *"Post now"*, *"Schedule for [time]"*, or *"Create as draft"*."""

    except Exception as e:
        logger.error(f"Draft refinement error: {e}")
        return f"❌ Failed to revise draft: {e}"


# ---------------------------------------------------------------------------
# Utilities (used only by this module)
# ---------------------------------------------------------------------------

_ET_TZ = ZoneInfo("America/New_York")

_WEEKDAY_WORDS = (
    "monday", "tuesday", "wednesday", "thursday",
    "friday", "saturday", "sunday",
)


def _parse_schedule_time(query: str):
    """Parse a schedule time from natural language to a naive ET datetime.

    Uses dateutil.parser.parse(fuzzy=True) so command words like
    "schedule for facebook" don't poison the parse. The default anchor
    is "now in America/New_York" (HubSpot portal default); date-only
    inputs inherit the anchor's time, time-only inputs inherit the
    anchor's date. Anchor shifts to tomorrow/next-week when those words
    appear (dateutil can't interpret those natively).

    Roll-forward heuristics for past results:
      - any weekday name in input  → +7 days (next occurrence of weekday)
      - otherwise                  → +1 day  (bare time, assume tomorrow)

    Returns:
        naive datetime in ET wall-clock time on success;
        None on parse failure, OR if still in the past after roll-forward
        (caller must NOT guess — ask the user to restate).
    """
    q = query.lower()
    now_et = datetime.now(_ET_TZ)

    anchor = now_et
    if "tomorrow" in q:
        anchor = now_et + timedelta(days=1)
    elif "next week" in q:
        anchor = now_et + timedelta(weeks=1)

    # Zero-anchor: drop tz and snap to noon so bare "10am" means 10:00 (not
    # inheriting the current minute), and date-only inputs default to noon.
    default_naive = anchor.replace(
        tzinfo=None, hour=12, minute=0, second=0, microsecond=0,
    )
    try:
        parsed = dateutil_parser.parse(query, fuzzy=True, default=default_naive)
    except (ValueError, OverflowError, TypeError):
        return None

    # If the input carried an explicit tz (e.g. "2026-06-21T10:00:00Z"),
    # convert to ET; otherwise treat the naive result as ET wall time.
    if parsed.tzinfo is not None:
        parsed_et = parsed.astimezone(_ET_TZ)
    else:
        parsed_et = parsed.replace(tzinfo=_ET_TZ)

    grace = timedelta(seconds=60)
    if parsed_et < now_et - grace:
        if any(w in q for w in _WEEKDAY_WORDS):
            parsed_et = parsed_et + timedelta(days=7)
        else:
            parsed_et = parsed_et + timedelta(days=1)

    if parsed_et < now_et - grace:
        return None

    return parsed_et.replace(tzinfo=None)


def _format_et_schedule(dt) -> str:
    """Format a naive ET datetime as 'Weekday, Month Day at H:MM AM/PM ET'.

    The echo is what catches parse bugs — operator sees exactly what
    will be scheduled before HubSpot acts.
    """
    if not isinstance(dt, datetime):
        return "(unknown time)"
    weekday = dt.strftime("%A")
    month = dt.strftime("%B")
    hour_12 = (dt.hour % 12) or 12
    period = "AM" if dt.hour < 12 else "PM"
    return f"{weekday}, {month} {dt.day} at {hour_12}:{dt.minute:02d} {period} ET"


def _extract_topic(query: str, context: str) -> str:
    """Extract the topic/subject from a content creation request."""
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


def _detect_platform(query: str) -> str:
    """Detect social media platform from query."""
    q = query.lower()

    if 'facebook' in q or 'fb' in q:
        return 'facebook'
    elif 'linkedin' in q:
        return 'linkedin'
    elif 'twitter' in q or ' x ' in q:
        return 'twitter'
    elif 'instagram' in q or 'insta' in q:
        return 'instagram'

    return 'facebook'


def _parse_email_draft(draft: str) -> tuple:
    """Parse subject and body from Claude's email draft response."""
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

    if not subject:
        subject = "AMCF Update"
    if not body:
        body = draft

    return subject, body


def _html_to_display(html: str) -> str:
    """Convert HTML to displayable text for chat."""
    text = html
    text = re.sub(r'<p>', '', text)
    text = re.sub(r'</p>', '\n\n', text)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<strong>', '**', text)
    text = re.sub(r'</strong>', '**', text)
    text = re.sub(r'<[^>]+>', '', text)
    return text.strip()


def _clear_draft_state(assistant):
    """Reset the draft state to inactive."""
    assistant.draft_state.update({
        "active": False,
        "type": None,
        "subject": None,
        "body": None,
        "platform": None,
        "template": None,
        "link_url": None,
        "photo_url": None,
    })
    # pending_schedule isn't a fixed key on the default draft_state dict —
    # remove it entirely so it doesn't haunt the next draft session.
    assistant.draft_state.pop("pending_schedule", None)