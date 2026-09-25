"""
Trigger Anchoring
=================
A keyword intake fires on what the message ASKS FOR, not on a word that
happens to appear somewhere inside it.

Why this module exists
----------------------
Every handler in the chain matched its trigger phrases with a plain
substring scan over the whole message::

    return any(p in q for p in TRIGGER_PHRASES)

That is fine for "show me open tickets" and wrong for a 5,000-character
newsletter someone pasted into chat. Twice in production:

    2026-09-23  a brief containing "new endowment" in its prose opened
                the CSuite intake, which then scraped a name, an email
                and a phone number out of the newsletter and offered to
                create a profile and a fund from them.
    2026-09-25  a brief containing "investment requests" in its prose
                returned a report of 34 investment requests instead of
                drafting the email that was asked for.

In both cases the first line said plainly what was wanted. Nothing read
it.

The rule
--------
A trigger counts only when it LEADS the message: within the first
TRIGGER_WINDOW_CHARS characters, in a message no longer than
MAX_COMMAND_CHARS. A command is short and starts with the command; a
document is long and starts with whatever it starts with.

Both numbers are deliberately generous. "Can you pull together the
dormant fund report for me please" is 56 characters and still anchors.
A brief is thousands.

The cost of being wrong in each direction is what sets them: refusing a
long-winded command costs someone one retyped sentence, while accepting
a keyword found in prose costs a report nobody asked for, or a write to
CSuite nobody asked for.
"""

# How far into a message a trigger may appear and still count as leading
# it. Long enough for "Hi Jidhr, could you show me the open tickets" —
# short enough that a keyword in the third paragraph never counts.
TRIGGER_WINDOW_CHARS = 80

# Beyond this, the message is a document rather than a command, whatever
# it contains.
MAX_COMMAND_CHARS = 400


def is_document(query) -> bool:
    """True if this is something pasted, not something asked."""
    return len(query or "") > MAX_COMMAND_CHARS


def first_line(query) -> str:
    """The first non-empty line, lowercased. Where the ask lives."""
    for line in (query or "").splitlines():
        if line.strip():
            return line.strip().lower()
    return ""


def anchored(query, phrases) -> bool:
    """True if one of `phrases` leads the message.

    Not merely appears in it. See the module docstring for the two
    production failures that were each a plain `in` away.
    """
    text = (query or "").lower().strip()
    if not text or is_document(text):
        return False
    window = text[:TRIGGER_WINDOW_CHARS]
    return any(phrase in window for phrase in phrases)


def yields_to_content(query) -> bool:
    """True if this message is an explicit content request.

    Every keyword intake defers to it. The handler chain already puts
    content first, but stating it in each intake means the precedence
    survives someone reordering the chain — and the chain order was
    never what failed. can_handle returning False was.
    """
    from intents import content

    try:
        if not content.can_handle(query):
            return False
        return not content.claims_by_draft_only(query)
    except Exception:  # pragma: no cover - a broken matcher must not block
        return False
