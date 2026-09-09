"""
Jidhr Context Gathering
=======================
Gathers relevant data from HubSpot and CSuite based on query keywords.

NOT a handler — does not produce final responses. Returns a context string
that gets injected into the Claude prompt so it can answer with real data.

Keyword-dispatched read-only gatherers, one per data domain
(funds, contacts, forms, social, events, donations, tickets, ...).
"""

import logging
import re
from config import Config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Access control
# ---------------------------------------------------------------------------

# queries.py is not in HANDLER_CHAIN — it is the fallback context gatherer,
# reached only after every handler declines. It still declares who may reach
# it, because the fallback path returns real donor data and a donor-role actor
# must not be handed it by default.
ALLOWED_ROLES = frozenset({"admin", "staff"})


# ---------------------------------------------------------------------------
# Helper: extract a name-like phrase from a query
# ---------------------------------------------------------------------------

_NAME_STOP_WORDS = {
    'fund', 'balance', 'daf', 'endowment', 'grant', 'grants',
    'contact', 'donor', 'donors', 'email', 'person', 'who',
    'what', 'how', 'when', 'where', 'show', 'get', 'find',
    'list', 'tell', 'about', 'the', 'for', 'with', 'from',
    'look', 'up', 'search', 'check', 'csuite', 'hubspot',
    'donation', 'donations', 'profile', 'ticket', 'task',
    'recent', 'latest', 'last', 'all', 'any', 'many',
    'pull', 'me', 'my', 'a', 'an',
}

# Nearly every fund is named "<Something> Fund" or "<Something> Endowment",
# so the general stop list — which drops those words — truncated the name to
# "Tanvir Family" and no exact match could ever succeed.
_FUND_NAME_STOP_WORDS = _NAME_STOP_WORDS - {
    'fund', 'endowment', 'daf', 'grant', 'grants',
}


def _extract_name(query: str, stop_words: set | None = None) -> str | None:
    """
    Try to pull a proper name out of a query.

    First strips common command prefixes, then looks for capitalised words.
    Returns the name string or None.

    Args:
        stop_words: Override the default stop list. The fund path passes a
                    variant that keeps "Fund"/"Endowment" as part of a name.
    """
    # Strip command prefixes to isolate the name
    _COMMAND_PREFIXES = [
        'pull up donor profile for', 'pull up profile for',
        'pull up donor for', 'pull up contact for',
        'donor profile for', 'contact profile for',
        'look up donor', 'look up contact', 'look up profile',
        'look up', 'pull up', 'search for', 'search up',
        'find donor', 'find contact', 'find profile', 'find',
        'show me donor', 'show me contact', 'show me profile',
        'show me', 'show donor', 'show contact', 'show profile',
        'get donor', 'get contact', 'get profile', 'get info on',
        'prep for my call with', 'prep for call with',
        'talking points for', 'call prep for',
        'who is', "who's",
    ]

    cleaned = query.strip()
    cleaned_lower = cleaned.lower()
    for prefix in sorted(_COMMAND_PREFIXES, key=len, reverse=True):
        if cleaned_lower.startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
            break

    STOP_WORDS = stop_words if stop_words is not None else _NAME_STOP_WORDS

    # Find sequences of capitalised words (2+ chars) that aren't stop words
    words = cleaned.split()
    name_parts = []
    for word in words:
        clean = re.sub(r'[^\w]', '', word)
        if clean and clean[0].isupper() and clean.lower() not in STOP_WORDS and len(clean) > 1:
            name_parts.append(clean)
        elif name_parts:
            break  # end of name sequence

    # Fallback: if prefix stripping left us with a clean name, use it
    if not name_parts and cleaned and len(cleaned) > 1:
        remaining = cleaned.strip().strip('"\'')
        if remaining and remaining[0].isupper():
            return remaining

    return ' '.join(name_parts) if name_parts else None


# ---------------------------------------------------------------------------
# Numeric reference extraction (shared by the fund and profile paths)
# ---------------------------------------------------------------------------

_PUNCT_STRIP = "#.,;:!?()[]{}<>\"'"

_DIGITS_RE = re.compile(r'\d+')


def _extract_bare_number(query: str, keywords: set) -> int | None:
    r"""Read a numeric id out of a query, or None.

    A token counts only when BOTH hold:

      1. the whole token is digits — never a number spliced out of the
         middle of something longer, and
      2. the preceding word introduces an id (one of `keywords`, or a
         leading '#'), or the number is the last word of the query.

    Rule 1 alone is not enough: "200 Muslim Women Who Care" is a fund NAME
    that begins with a number, and the old \b(\d{2,})\b search pulled 200
    out of it and looked up an unrelated record. As a further guard, a
    number immediately followed by a Capitalised word is read as the start
    of a name rather than an id.
    """
    if not query:
        return None

    tokens = query.split()
    cleaned = [t.strip(_PUNCT_STRIP) for t in tokens]

    for index, token in enumerate(cleaned):
        if not token or not _DIGITS_RE.fullmatch(token):
            continue

        previous = cleaned[index - 1].lower().lstrip('#') if index else ""
        introduced = previous in keywords or tokens[index].startswith('#')
        is_last = index == len(cleaned) - 1
        if not (introduced or is_last):
            continue

        following = cleaned[index + 1] if index + 1 < len(cleaned) else ""
        if following[:1].isupper():
            # "profile 200 Muslim Women Who Care" — a name, not an id.
            continue

        return int(token)

    return None


# Words that introduce a profile/contact id.
_PROFILE_ID_KEYWORDS = {"id", "profile", "profile_id", "donor", "contact", "#"}


def _extract_id(query: str) -> str | None:
    """Extract a profile/contact id from the query (e.g. 'profile 19879').

    Same rule as extract_fund_ref, shared via _extract_bare_number. Returns
    a string because the CSuite client takes ids as strings here.
    """
    number = _extract_bare_number(query, _PROFILE_ID_KEYWORDS)
    return str(number) if number is not None else None


# ---------------------------------------------------------------------------
# Fund reference extraction
# ---------------------------------------------------------------------------

# A fund code looks like END0026 or DAF0123: letters then digits, no space.
_FUND_CODE_RE = re.compile(r'^[A-Za-z]{2,4}\d{3,}$')

# A bare number is only a fund id when something says so. "200 Muslim Women
# Who Care" is a fund NAME that starts with a number, and the old
# \b(\d{2,})\b search happily pulled 200 out of it and looked up an
# unrelated fund.
_FUND_ID_KEYWORDS = {"fund", "funit", "fund_id", "funit_id", "id", "#"}


def extract_fund_ref(query: str) -> dict | None:
    """Pull a fund reference out of a query.

    Returns one of:
        {"code": "END0026"}  — a fund code, matched anywhere in the query
        {"id": 1046}         — a numeric fund id
        None                 — no fund reference; treat the query as a name

    The numeric half follows _extract_bare_number's rule: a bare number is
    an id only when introduced by a fund keyword or final, and not followed
    by a Capitalised word. Codes need no such guard — the shape is
    distinctive enough on its own — so they are matched anywhere.
    """
    if not query:
        return None

    # Codes win: they are unambiguous wherever they appear.
    for token in (t.strip(_PUNCT_STRIP) for t in query.split()):
        if token and _FUND_CODE_RE.fullmatch(token):
            return {"code": token.upper()}

    number = _extract_bare_number(query, _FUND_ID_KEYWORDS)
    return {"id": number} if number is not None else None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def gather_context(query: str, hubspot, csuite,
                   workflow_state: dict | None = None) -> str:
    """
    Analyse the query for keywords and fetch relevant data.

    Args:
        query: The user's raw message
        hubspot: HubSpotClient instance
        csuite: CSuiteClient instance
        workflow_state: The request's workflow state. Optional so existing
            callers keep working, but WITHOUT it a numbered fund pick cannot
            be remembered between messages — see take_pending_fund_pick.

    Returns:
        Context string (may be empty if no keywords matched)
    """
    context_parts = []
    query_lower = query.lower()

    logger.info(f"Gathering context for: {query_lower[:50]}...")

    # A bare digit answering a fund list has no keywords of its own, so it
    # has to be checked before keyword dispatch or it would match nothing.
    if workflow_state is not None and workflow_state.get("pending_fund_pick"):
        if _FUND_PICK_RE.match(query.strip()):
            return "\n\n".join(
                _gather_fund_context(query, query_lower, csuite, workflow_state))
        # Any other message means the user moved on.
        workflow_state.pop("pending_fund_pick", None)

    # ------------------------------------------------------------------
    # FUND / BALANCE / DAF / ENDOWMENT / GRANT → CSuite
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['fund', 'balance', 'daf', 'endowment', 'grant']):
        context_parts += _gather_fund_context(
            query, query_lower, csuite, workflow_state)

    # ------------------------------------------------------------------
    # CONTACT / DONOR → HubSpot (+ CSuite cross-reference)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['contact', 'donor', 'email', 'person', 'who']):
        context_parts += _gather_contact_context(query, query_lower, hubspot, csuite)

    # ------------------------------------------------------------------
    # FORM / SUBMISSION / INQUIRY → HubSpot
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['form', 'submission', 'inquiry', 'submitted']):
        context_parts += _gather_form_context(query_lower, hubspot)

    # ------------------------------------------------------------------
    # SOCIAL / POST / PLATFORM → HubSpot
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['social', 'post', 'facebook', 'linkedin', 'schedule', 'channel']):
        context_parts += _gather_social_context(hubspot)

    # ------------------------------------------------------------------
    # EVENT → CSuite + HubSpot
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['event', 'symposium', 'webinar', 'registration', 'gala', 'dinner']):
        context_parts += _gather_event_context(csuite, hubspot)

    # ------------------------------------------------------------------
    # DONATION / GIFT → CSuite
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['donation', 'gift', 'gave', 'contributed', 'recent donations']):
        context_parts += _gather_donation_context(query, query_lower, csuite)

    # ------------------------------------------------------------------
    # TICKET / SUPPORT → HubSpot
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['ticket', 'support', 'issue', 'help desk', 'open tickets']):
        context_parts += _gather_ticket_context(hubspot)

    # ------------------------------------------------------------------
    # CLOSED TICKETS → HubSpot (Shazeen)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['closed tickets', 'closed ticket', 'resolved tickets',
                                       'which tickets are closed', 'what tickets are closed',
                                       'tickets are done', 'tickets closed']):
        context_parts += _gather_closed_ticket_context(hubspot)

    # ------------------------------------------------------------------
    # CAMPAIGN → HubSpot
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['campaign', 'marketing campaign']):
        context_parts += _gather_campaign_context(hubspot)

    # ------------------------------------------------------------------
    # TASK → HubSpot
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['task', 'tasks', 'to do', 'todo', 'my tasks']):
        context_parts += _gather_task_context(hubspot)

    # ------------------------------------------------------------------
    # FUND-ASSOCIATED CONTACTS → HubSpot (by csuite_fund_id)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['associated with', 'contacts for', 'contacts in fund', 'who is in', 'who\'s in']):
        context_parts += _gather_fund_contacts_context(query, query_lower, hubspot, csuite)

    # ------------------------------------------------------------------
    # NEW v1.3: CHECK / UNCASHED → CSuite (Muhi)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['check', 'cashed', 'uncashed', 'cleared']):
        context_parts += _gather_check_context(query_lower, csuite)

    # ------------------------------------------------------------------
    # NEW v1.3: FEE → CSuite (Muhi)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['fee', 'fees', 'admin fee']):
        context_parts += _gather_fee_context(csuite)

    # ------------------------------------------------------------------
    # NEW v1.3: VOUCHER / PAYMENT → CSuite
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['voucher', 'payment']):
        context_parts += _gather_voucher_context(csuite)

    # ------------------------------------------------------------------
    # NEW v1.3: PROFILE → CSuite
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['profile', 'profiles']):
        context_parts += _gather_profile_context(query, csuite)

    # ------------------------------------------------------------------
    # GIVING CIRCLE → HubSpot (Lisa)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['giving circle', 'gc member', 'gc status',
                                       'giving circle member', 'circle member']):
        context_parts += _gather_giving_circle_context(query_lower, hubspot)

    # ------------------------------------------------------------------
    # NEW v1.3: LAPSED / INACTIVE context hints (for reports module)
    # ------------------------------------------------------------------
    if any(w in query_lower for w in ['lapsed', 'inactive', "haven't donated", 'dormant']):
        context_parts.append(
            "[Hint] This looks like a lapsed/inactive analysis request. "
            "The reports module can run full comparisons."
        )

    result = "\n\n".join(context_parts) if context_parts else ""
    logger.info(f"Total context gathered: {len(result)} chars")
    return result


# ---------------------------------------------------------------------------
# Per-category gatherers
# ---------------------------------------------------------------------------

# Fund group ids that are worth naming in context. From config.py's
# FUND_GROUP_* constants; anything else is reported as the bare id.
_FUND_GROUP_LABELS = {
    1002: "DAF",
    1008: "Endowment",
}


def _fund_row_id(row: dict):
    """funit id from a row of either shape.

    funit/list returns `funit_id`; funit/list/search returns `id`. Reading
    only one of them is why fund search used to resolve to nothing.
    """
    for key in ("funit_id", "id", "fund_id"):
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


# CSuite carries the fund code inside the name, not in a field of its own:
#   "200 Muslim Women Who Care Endowment Fund-(END0026)"
# Seen also as " (END0026)" and trailing "-END0026".
_FUND_NAME_CODE_RE = re.compile(
    r"""(?:
            \s*-?\s*\(\s*([A-Za-z]{2,4}\d{3,})\s*\)   # -(END0026) / (END0026)
          | \s*-\s*([A-Za-z]{2,4}\d{3,})                 # -END0026
        )\s*$""",
    re.VERBOSE,
)


def split_fund_name(raw) -> tuple:
    """Split a CSuite fund name into (clean_name, code).

    >>> split_fund_name("200 Muslim Women Who Care Endowment Fund-(END0026)")
    ('200 Muslim Women Who Care Endowment Fund', 'END0026')

    Returns (clean_name, None) when there is no code suffix. Whitespace is
    collapsed so a name that differs only in spacing still compares equal.
    """
    if not raw:
        return "", None

    text = " ".join(str(raw).split())
    match = _FUND_NAME_CODE_RE.search(text)
    if not match:
        return text, None

    code = (match.group(1) or match.group(2) or "").upper()
    clean = " ".join(text[:match.start()].split()).rstrip(" -")
    return clean, (code or None)


def _norm_for_match(text) -> str:
    """Case-insensitive, whitespace-collapsed form used for name equality."""
    return " ".join(str(text or "").split()).strip().lower()


def _fund_row_raw_names(row: dict) -> list:
    """Every name-ish string a row carries, exactly as CSuite sent it."""
    return [
        str(row[key]) for key in ("fund_name", "name", "fullname", "public_name")
        if row.get(key)
    ]


def _fund_row_names(row: dict) -> list:
    """Display names for a row, with any code suffix stripped."""
    names = []
    for raw in _fund_row_raw_names(row):
        clean, _ = split_fund_name(raw)
        names.append(clean or raw)
    return names


def _fund_row_code(row: dict) -> str | None:
    """The fund code, parsed out of the name or read from short_name."""
    for raw in _fund_row_raw_names(row):
        _, code = split_fund_name(raw)
        if code:
            return code
    short = row.get("short_name")
    return str(short).strip().upper() if short else None


def _format_currency(value) -> str:
    """Format a CSuite money string as currency, or hand it back untouched."""
    try:
        return f"${float(str(value).replace(',', '').strip()):,.2f}"
    except (TypeError, ValueError):
        return str(value)


# Lead-ins a fund question opens with. Stripping these leaves the fund name
# whole, which _extract_name cannot do: it stops at the first stop word, so
# "200 Muslim Women Who Care Endowment Fund" was truncated to "Muslim Women"
# — it drops the leading number and halts on "Who".
_FUND_LEAD_IN_RE = re.compile(
    r"""^\s*(?:
          what(?:'s|\s+is)\s+(?:the\s+)?(?:current\s+)?(?:fund\s+)?
              balance\s+(?:of|for|in)\s+
        | how\s+much\s+is\s+(?:in|left\s+in)\s+
        | (?:fund\s+)?balance\s+(?:of|for|in)\s+
        | (?:show|tell|give)\s+me\s+(?:the\s+)?(?:balance\s+(?:of|for|in)\s+)?
        | (?:calculate\s+)?fees?\s+for\s+
        | look\s+up\s+
        | pull\s+up\s+
        )\s*(?:the\s+)?""",
    re.VERBOSE | re.IGNORECASE,
)


def extract_fund_name_phrase(query: str) -> str | None:
    """The fund name a query is asking about, as a whole phrase.

    Strips a leading question form and returns the rest verbatim. Falls back
    to the capitalised-words heuristic when nothing recognisable leads.
    """
    if not query:
        return None

    text = " ".join(str(query).split())
    stripped = _FUND_LEAD_IN_RE.sub("", text, count=1).strip()
    stripped = stripped.strip('"\'').rstrip("?.!,;:").strip()

    if stripped and stripped.lower() != text.lower():
        return stripped

    return _extract_name(query, stop_words=_FUND_NAME_STOP_WORDS)


def resolve_fund_id(csuite, query: str):
    """Resolve a query to one fund id.

    Returns (fund_id, rows, message):
        fund_id  — the id to display, or None
        rows     — the search rows, when a choice is needed
        message  — an error string, when the search itself failed

    A caller that gets no id and no message is looking at an ambiguous
    result and should offer `rows` as a choice rather than guess.
    """
    ref = extract_fund_ref(query)
    if ref and "id" in ref:
        return ref["id"], [], None

    term = ref["code"] if ref else extract_fund_name_phrase(query)
    if not term:
        return None, [], None

    rows, error = _search_funds(csuite, term)
    if error:
        return None, [], error
    if not rows:
        return None, [], f"CSuite fund search '{term}' returned no funds."

    chosen = _choose_fund(rows, term)
    if chosen is not None:
        return _fund_row_id(chosen), rows, None

    return None, rows, None


def _search_funds(csuite, term: str):
    """Return (rows, error_text). Exactly one of them is meaningful."""
    try:
        data = csuite.search_funds(term)
    except Exception as e:
        logger.error(f"Error searching funds for {term!r}: {e}")
        return [], f"CSuite fund search for '{term}' failed: {e}"

    if not data.get("success"):
        error = data.get("error") or "unknown error"
        return [], f"CSuite fund search for '{term}' failed: {error}"

    return (data.get("data") or {}).get("results", []) or [], None


def _choose_fund(rows: list, term: str):
    """Pick the one fund a query means, or None if it is ambiguous.

    Order: exact name match, then exact code match, then a lone result.
    Anything else is ambiguous on purpose — guessing between two funds and
    reporting one balance as fact is worse than asking.

    The name comparison runs against both the code-stripped name and the raw
    one, so "200 Muslim Women Who Care Endowment Fund" matches a row whose
    name is "200 Muslim Women Who Care Endowment Fund-(END0026)", and a user
    pasting the full raw string still matches too.
    """
    wanted = _norm_for_match(term)
    if not wanted:
        return rows[0] if len(rows) == 1 else None

    for row in rows:
        for raw in _fund_row_raw_names(row):
            clean, _ = split_fund_name(raw)
            if wanted in (_norm_for_match(clean), _norm_for_match(raw)):
                return row

    wanted_code = wanted.upper()
    for row in rows:
        code = _fund_row_code(row)
        if code and code.upper() == wanted_code:
            return row

    if len(rows) == 1:
        return rows[0]

    return None


# Picks are single digits, matching the event pick.
_MAX_FUND_CHOICES = 9

_FUND_PICK_RE = re.compile(r"^\s*([1-9])\s*$")


def _fund_choice_line(index: int, row: dict) -> str:
    names = _fund_row_names(row)
    name = names[0] if names else "Unknown"
    code = _fund_row_code(row)
    suffix = f", code: {code}" if code else ""
    return f"{index}. {name} (id: {_fund_row_id(row)}{suffix})"


def _format_fund_candidates(term: str, rows: list,
                            workflow_state: dict | None = None) -> str:
    """List the candidates and stop. No display call, no guessing.

    2–9 candidates are numbered and remembered, so the next message can be
    just "1". More than that is not a list worth printing.
    """
    if len(rows) > _MAX_FUND_CHOICES:
        return (
            f"CSuite fund search '{term}' matched {len(rows)} funds — too many "
            "to list. Ask the user for more of the fund's name, or its code "
            "(for example END0026)."
        )

    lines = [
        f"CSuite fund search '{term}' matched {len(rows)} funds. "
        f"Ask the user which one is meant — do not guess:"
    ]
    for index, row in enumerate(rows, 1):
        lines.append(_fund_choice_line(index, row))
    lines.append(
        "The user can reply with just the number to pick one.")

    if workflow_state is not None:
        workflow_state["pending_fund_pick"] = {
            "term": term,
            "funds": rows,
        }
    return "\n".join(lines)


def take_pending_fund_pick(query: str, workflow_state: dict):
    """Resolve a bare 1-9 against a stored fund list.

    Returns the chosen row, or None. The pending list is cleared either way:
    a pick consumes it, and any other message means the user moved on.
    """
    pending = (workflow_state or {}).get("pending_fund_pick")
    if not pending:
        return None

    match = _FUND_PICK_RE.match(query or "")
    if not match:
        workflow_state.pop("pending_fund_pick", None)
        return None

    index = int(match.group(1)) - 1
    rows = pending.get("funds") or []
    workflow_state.pop("pending_fund_pick", None)

    if 0 <= index < len(rows):
        return rows[index]
    return None


def _fund_detail_context(csuite, fund_id) -> str:
    """Labelled detail lines for one fund, or the literal CSuite error."""
    try:
        data = csuite.get_fund(fund_id)
    except Exception as e:
        logger.error(f"Error fetching fund {fund_id}: {e}")
        return f"CSuite fund lookup for id {fund_id} failed: {e}"

    if not data.get("success"):
        # The literal error, so Claude reports it rather than inventing a
        # plausible-sounding next step.
        error = data.get("error") or "unknown error"
        return f"CSuite fund lookup for id {fund_id} failed: {error}"

    fund = data.get("data") or {}
    if not fund:
        return f"CSuite returned no detail for fund id {fund_id}."

    clean_name, code = split_fund_name(fund.get("fund_name"))

    lines = ["CSuite Fund Detail:"]
    lines.append(f"Fund name: {clean_name or 'Unknown'}")
    lines.append(f"Fund id: {fund.get('funit_id', fund_id)}")

    # The code lives inside the name; short_name is the fallback.
    if not code and fund.get("short_name"):
        code = str(fund["short_name"]).strip().upper()
    if code:
        lines.append(f"Fund code: {code}")

    group_id = fund.get("fgroup_id")
    if group_id is not None:
        label = _FUND_GROUP_LABELS.get(group_id)
        lines.append(
            f"Fund group id: {group_id}" + (f" ({label})" if label else "")
        )

    # current_fundbalance, NOT "balance" — there is no `balance` field on
    # funit/display; reading it returned the default and reported $0.
    if fund.get("current_fundbalance") is not None:
        lines.append(
            f"Current balance: {_format_currency(fund['current_fundbalance'])}"
        )

    for key in sorted(fund):
        if key.endswith("_date") and fund.get(key):
            lines.append(f"{key}: {fund[key]}")

    return "\n".join(lines)


def _gather_fund_context(query: str, query_lower: str, csuite,
                         workflow_state: dict | None = None) -> list:
    """Fund-related: resolve one fund and report it, or list the candidates.

    Sequence: a pending numbered pick wins; then a fund code searches by
    code, a numeric id goes straight to funit/display, and anything else
    searches on the extracted name phrase.
    """
    parts = []

    # A bare "1" answering a previous list of candidates.
    if workflow_state is not None and workflow_state.get("pending_fund_pick"):
        picked = take_pending_fund_pick(query, workflow_state)
        if picked is not None:
            fund_id = _fund_row_id(picked)
            logger.info(f"Fund pick resolved to id {fund_id}")
            parts.append(_fund_detail_context(csuite, fund_id))
            return parts

    fund_id, rows, error = resolve_fund_id(csuite, query)

    if error:
        parts.append(error)
        return parts

    if fund_id is None and rows:
        # Ambiguous: report the candidates and stop here.
        term = extract_fund_ref(query)
        term = term.get("code") if term else None
        term = term or extract_fund_name_phrase(query) or query.strip()
        parts.append(_format_fund_candidates(term, rows, workflow_state))
        return parts

    if fund_id is not None:
        logger.info(f"Fetching CSuite fund details for id {fund_id}")
        parts.append(_fund_detail_context(csuite, fund_id))

    # Fallback: generic fund list (only if nothing above produced context)
    if not parts:
        logger.info("Fetching CSuite funds (generic)...")
        try:
            funds_data = csuite.get_funds(limit=20)
            if funds_data.get('success') and funds_data.get('data'):
                results = funds_data['data'].get('results', [])
                fund_list = [
                    f"{(_fund_row_names(f) or ['Unknown'])[0]} "
                    f"(ID: {_fund_row_id(f) or 'N/A'})"
                    for f in results[:10]
                ]
                parts.append(f"CSuite Funds:\n" + "\n".join(fund_list))
                logger.info(f"Found {len(fund_list)} funds")
        except Exception as e:
            logger.error(f"Error fetching funds: {e}")

    # Enhanced: grant-specific queries pull grants by fund
    if 'grant' in query_lower and fund_id:
        logger.info(f"Fetching grants for fund {fund_id}...")
        try:
            grants_data = csuite.get_grants_by_fund(fund_id, limit=10)
            if grants_data.get('success') and grants_data.get('data'):
                results = grants_data['data'].get('results', [])
                grant_list = [
                    f"${g.get('grant_amount', '0')} to {g.get('vendor_name', 'Unknown')} ({g.get('grant_date', 'No date')})"
                    for g in results[:10]
                ]
                parts.append(f"Grants for Fund {fund_id}:\n" + "\n".join(grant_list))
                logger.info(f"Found {len(grant_list)} grants")
        except Exception as e:
            logger.error(f"Error fetching grants by fund: {e}")

    return parts


def _gather_contact_context(query: str, query_lower: str, hubspot, csuite) -> list:
    """Contact-related: search by name if possible, else list recent contacts."""
    parts = []
    name = _extract_name(query)

    # Enhanced: search by name in HubSpot
    if name:
        logger.info(f"Searching HubSpot contacts for: {name}")
        try:
            search_data = hubspot.search_contacts(name)
            if 'results' in search_data and search_data['results']:
                contact_list = [
                    f"{c.get('properties', {}).get('firstname', '')} "
                    f"{c.get('properties', {}).get('lastname', '')} "
                    f"({c.get('properties', {}).get('email', 'No email')}) "
                    f"[ID: {c.get('id', 'N/A')}]"
                    for c in search_data['results'][:5]
                ]
                parts.append(f"HubSpot Contact Search '{name}':\n" + "\n".join(contact_list))
                logger.info(f"Found {len(contact_list)} matching contacts")
        except Exception as e:
            logger.error(f"Error searching contacts: {e}")

        # Enhanced: also search CSuite for cross-system context
        logger.info(f"Searching CSuite profiles for: {name}")
        try:
            profile_data = csuite.search_profiles(name)
            if profile_data.get('success') and profile_data.get('data'):
                results = profile_data['data'].get('results', [])
                if results:
                    profile_list = [
                        f"{p.get('name', 'Unknown')} (Profile ID: {p.get('profile_id', 'N/A')})"
                        for p in results[:5]
                    ]
                    parts.append(f"CSuite Profile Search '{name}':\n" + "\n".join(profile_list))
                    logger.info(f"Found {len(profile_list)} matching profiles")
        except Exception as e:
            logger.error(f"Error searching CSuite profiles: {e}")

    # Fallback: generic contact list
    if not parts:
        logger.info("Fetching HubSpot contacts (generic)...")
        try:
            contacts_data = hubspot.get_contacts(limit=10)
            if 'results' in contacts_data:
                contact_list = [
                    f"{c.get('properties', {}).get('firstname', '')} "
                    f"{c.get('properties', {}).get('lastname', '')} "
                    f"({c.get('properties', {}).get('email', 'No email')})"
                    for c in contacts_data['results'][:5]
                ]
                parts.append(f"HubSpot Contacts:\n" + "\n".join(contact_list))
                logger.info(f"Found {len(contact_list)} contacts")
        except Exception as e:
            logger.error(f"Error fetching contacts: {e}")

    return parts


def _gather_form_context(query_lower: str, hubspot) -> list:
    """Form-related: generic forms list + DAF/endowment submissions if relevant."""
    parts = []

    # Always fetch form list
    logger.info("Fetching HubSpot forms...")
    try:
        forms_data = hubspot.get_forms(limit=10)
        if 'results' in forms_data:
            form_list = [
                f"{f.get('name', 'Unknown')} (ID: {f.get('id', 'N/A')})"
                for f in forms_data['results'][:5]
            ]
            parts.append(f"HubSpot Forms:\n" + "\n".join(form_list))
            logger.info(f"Found {len(form_list)} forms")
    except Exception as e:
        logger.error(f"Error fetching forms: {e}")

    # Enhanced: pull recent DAF inquiry submissions
    if any(w in query_lower for w in ['daf', 'inquiry', 'submitted', 'submission']):
        logger.info("Fetching DAF inquiry submissions...")
        try:
            resp = hubspot.get_daf_inquiry_submissions(limit=5)
            subs = resp.get('results', []) if isinstance(resp, dict) else []
            if subs:
                sub_list = [
                    f"Submitted {s.get('submittedAt', 'Unknown date')}: "
                    + ", ".join(f"{v.get('name', '?')}={v.get('value', '')}" for v in s.get('values', [])[:4])
                    for s in subs[:5]
                ]
                parts.append(f"Recent DAF Inquiry Submissions:\n" + "\n".join(sub_list))
                logger.info(f"Found {len(sub_list)} DAF submissions")
        except Exception as e:
            logger.error(f"Error fetching DAF submissions: {e}")

    # Enhanced: pull recent endowment inquiry submissions
    if any(w in query_lower for w in ['endowment', 'inquiry', 'submitted', 'submission']):
        logger.info("Fetching endowment inquiry submissions...")
        try:
            resp = hubspot.get_endowment_inquiry_submissions(limit=5)
            subs = resp.get('results', []) if isinstance(resp, dict) else []
            if subs:
                sub_list = [
                    f"Submitted {s.get('submittedAt', 'Unknown date')}: "
                    + ", ".join(f"{v.get('name', '?')}={v.get('value', '')}" for v in s.get('values', [])[:4])
                    for s in subs[:5]
                ]
                parts.append(f"Recent Endowment Inquiry Submissions:\n" + "\n".join(sub_list))
                logger.info(f"Found {len(sub_list)} endowment submissions")
        except Exception as e:
            logger.error(f"Error fetching endowment submissions: {e}")

    return parts


def _gather_social_context(hubspot) -> list:
    """Social channels and recent broadcasts from HubSpot."""
    parts = []
    logger.info("Fetching HubSpot social context...")
    try:
        channels_data = hubspot.get_social_channels()
        if isinstance(channels_data, list):
            channel_list = [
                f"{c.get('name', 'Unknown')} ({c.get('channelType', 'Unknown')})"
                for c in channels_data[:5]
            ]
            parts.append(f"Social Channels:\n" + "\n".join(channel_list))
            logger.info(f"Found {len(channel_list)} channels")
    except Exception as e:
        logger.error(f"Error fetching social channels: {e}")

    # Fetch recent broadcasts for context
    try:
        broadcasts = hubspot.get_social_broadcasts(limit=5)
        if isinstance(broadcasts, list) and broadcasts:
            broadcast_list = []
            for b in broadcasts[:5]:
                status = b.get("status", "Unknown")
                created = b.get("createdAt", "")
                channel = b.get("channelKey", "")
                clicks = b.get("clicks", 0)
                interactions = b.get("interactions", 0)
                broadcast_list.append(
                    f"  - {channel} | Status: {status} | "
                    f"Clicks: {clicks} | Interactions: {interactions}"
                )
            parts.append("Recent Social Posts:\n" + "\n".join(broadcast_list))
        elif not broadcasts:
            parts.append(
                "Social Analytics Note: HubSpot does not provide a dedicated "
                "social performance metrics API via personal access tokens. "
                "For detailed social analytics, use the HubSpot Social dashboard directly."
            )
    except Exception as e:
        logger.error(f"Error fetching social broadcasts: {e}")
        parts.append(
            "Social Analytics Note: Could not fetch social data. "
            "For performance metrics, use the HubSpot Social dashboard directly."
        )

    return parts


def _gather_event_context(csuite, hubspot) -> list:
    """Events from both CSuite and HubSpot."""
    parts = []

    # CSuite Events
    logger.info("Fetching CSuite events...")
    try:
        csuite_events = csuite.get_event_dates(limit=10)
        if csuite_events.get('success') and csuite_events.get('data'):
            results = csuite_events['data'].get('results', [])
            event_list = [
                f"{e.get('event_description') or e.get('event_name', 'Unknown')} ({e.get('event_date', 'No date')})"
                for e in results[:5]
            ]
            parts.append(f"CSuite Events:\n" + "\n".join(event_list))
            logger.info(f"Found {len(event_list)} CSuite events")
    except Exception as e:
        logger.error(f"Error fetching CSuite events: {e}")

    # HubSpot Events
    logger.info("Fetching HubSpot marketing events...")
    try:
        hubspot_events = hubspot.get_marketing_events(limit=5)
        if 'results' in hubspot_events:
            event_list = [
                f"{e.get('eventName', 'Unknown')} ({e.get('startDateTime', 'No date')})"
                for e in hubspot_events['results'][:5]
            ]
            parts.append(f"HubSpot Marketing Events:\n" + "\n".join(event_list))
            logger.info(f"Found {len(event_list)} HubSpot events")
    except Exception as e:
        logger.error(f"Error fetching HubSpot events: {e}")

    return parts


def _gather_donation_context(query: str, query_lower: str, csuite) -> list:
    """Donation-related: profile-specific if possible, else recent donations."""
    parts = []
    profile_id = _extract_id(query)

    # Enhanced: donations for a specific profile
    if profile_id:
        logger.info(f"Fetching donations for profile {profile_id}...")
        try:
            donations_data = csuite.get_donations_by_profile(profile_id, limit=10)
            if donations_data.get('success') and donations_data.get('data'):
                results = donations_data['data'].get('results', [])
                donation_list = [
                    f"${d.get('donation_amount', '0')} to {d.get('fund_name', 'Unknown')} ({d.get('donation_date', 'No date')})"
                    for d in results[:10]
                ]
                parts.append(f"Donations for Profile {profile_id}:\n" + "\n".join(donation_list))
                logger.info(f"Found {len(donation_list)} donations for profile")
        except Exception as e:
            logger.error(f"Error fetching profile donations: {e}")

    # Fallback: recent donations
    if not parts:
        logger.info("Fetching CSuite donations (generic)...")
        try:
            donations_data = csuite.get_donations(limit=10)
            if donations_data.get('success') and donations_data.get('data'):
                results = donations_data['data'].get('results', [])
                donation_list = [
                    f"{d.get('name', 'Unknown')}: ${d.get('donation_amount', '0')} to {d.get('fund_name', 'Unknown')} ({d.get('donation_date', 'No date')})"
                    for d in results[:5]
                ]
                parts.append(f"CSuite Donations:\n" + "\n".join(donation_list))
                logger.info(f"Found {len(donation_list)} donations")
        except Exception as e:
            logger.error(f"Error fetching donations: {e}")

    return parts


def _gather_ticket_context(hubspot) -> list:
    """Tickets from HubSpot."""
    parts = []
    logger.info("Fetching HubSpot tickets...")
    try:
        tickets_data = hubspot.get_tickets(limit=10)
        if 'results' in tickets_data:
            ticket_list = []
            for t in tickets_data['results'][:10]:
                props = t.get('properties', {})
                subject = props.get('subject', 'No subject')
                status = props.get('hs_pipeline_stage', 'Unknown')
                ticket_list.append(f"{subject} (Status: {status})")
            if ticket_list:
                parts.append(f"HubSpot Tickets:\n" + "\n".join(ticket_list))
                logger.info(f"Found {len(ticket_list)} tickets")
    except Exception as e:
        logger.error(f"Error fetching tickets: {e}")
    return parts


def _gather_closed_ticket_context(hubspot) -> list:
    """Closed tickets from HubSpot (Shazeen)."""
    parts = []
    logger.info("Fetching closed HubSpot tickets...")
    try:
        tickets_data = hubspot.get_closed_tickets(limit=20)
        if 'results' in tickets_data:
            ticket_list = []
            for t in tickets_data['results']:
                props = t.get('properties', {})
                subject = props.get('subject', 'No subject')
                closed_date = (props.get('hs_lastmodifieddate') or '')[:10]
                ticket_list.append(f"{subject} (Closed: {closed_date})")
            if ticket_list:
                parts.append(f"Closed HubSpot Tickets ({len(ticket_list)}):\n" + "\n".join(ticket_list))
                logger.info(f"Found {len(ticket_list)} closed tickets")
            else:
                parts.append("No closed tickets found.")
    except Exception as e:
        logger.error(f"Error fetching closed tickets: {e}")
    return parts


def _gather_campaign_context(hubspot) -> list:
    """Campaigns from HubSpot."""
    parts = []
    logger.info("Fetching HubSpot campaigns...")
    try:
        campaigns_data = hubspot.get_campaigns(limit=10)
        if 'results' in campaigns_data:
            campaign_list = [
                f"Campaign ID: {c.get('id', 'Unknown')}"
                for c in campaigns_data['results'][:5]
            ]
            if campaign_list:
                parts.append(f"HubSpot Campaigns:\n" + "\n".join(campaign_list))
                logger.info(f"Found {len(campaign_list)} campaigns")
    except Exception as e:
        logger.error(f"Error fetching campaigns: {e}")
    return parts


def _gather_task_context(hubspot) -> list:
    """Tasks from HubSpot."""
    parts = []
    logger.info("Fetching HubSpot tasks...")
    try:
        tasks_data = hubspot.get_tasks(limit=10)
        if 'results' in tasks_data:
            task_list = []
            for t in tasks_data['results'][:10]:
                props = t.get('properties', {})
                subject = props.get('hs_task_subject', 'No subject')
                status = props.get('hs_task_status', 'Unknown')
                task_list.append(f"{subject} (Status: {status})")
            if task_list:
                parts.append(f"HubSpot Tasks:\n" + "\n".join(task_list))
                logger.info(f"Found {len(task_list)} tasks")
    except Exception as e:
        logger.error(f"Error fetching tasks: {e}")
    return parts


# ---------------------------------------------------------------------------
# NEW v1.3 gatherers
# ---------------------------------------------------------------------------

def _gather_check_context(query_lower: str, csuite) -> list:
    """Check/uncashed queries → CSuite (Muhi)."""
    parts = []

    if 'uncashed' in query_lower or "haven't cashed" in query_lower or 'not cashed' in query_lower:
        logger.info("Fetching uncashed checks...")
        try:
            checks = csuite.get_uncashed_checks()
            if checks:
                check_list = [
                    f"Check #{c.get('check_num', '?')}: ${c.get('amount', '0')} to {c.get('vendor_name', 'Unknown')} ({c.get('check_date', 'No date')})"
                    for c in checks[:10]
                ]
                parts.append(f"Uncashed Checks:\n" + "\n".join(check_list))
                logger.info(f"Found {len(check_list)} uncashed checks")
        except Exception as e:
            logger.error(f"Error fetching uncashed checks: {e}")
    else:
        logger.info("Fetching CSuite checks...")
        try:
            checks_data = csuite.get_checks(limit=10)
            if checks_data.get('success') and checks_data.get('data'):
                results = checks_data['data'].get('results', [])
                check_list = [
                    f"Check #{c.get('check_number', '?')}: ${c.get('amount', '0')} ({c.get('status', 'Unknown')})"
                    for c in results[:10]
                ]
                parts.append(f"CSuite Checks:\n" + "\n".join(check_list))
                logger.info(f"Found {len(check_list)} checks")
        except Exception as e:
            logger.error(f"Error fetching checks: {e}")

    return parts


def _gather_fee_context(csuite) -> list:
    """Fee queries → CSuite fund fee types (Muhi)."""
    parts = []
    logger.info("Fetching CSuite fund fee types...")
    try:
        fee_data = csuite.get_fund_fee_types()
        if fee_data.get('success') and fee_data.get('data'):
            results = fee_data['data'].get('results', [])
            fee_list = [
                f"{f.get('fee_name', 'Unknown')}: {f.get('fee_percent', '?')}% (min: ${f.get('min_fee', '0')})"
                for f in results[:10]
            ]
            parts.append(f"CSuite Fee Types:\n" + "\n".join(fee_list))
            logger.info(f"Found {len(fee_list)} fee types")
    except Exception as e:
        logger.error(f"Error fetching fee types: {e}")
    return parts


def _gather_voucher_context(csuite) -> list:
    """Voucher/payment queries → CSuite."""
    parts = []
    logger.info("Fetching CSuite vouchers...")
    try:
        voucher_data = csuite.get_vouchers(limit=10)
        if voucher_data.get('success') and voucher_data.get('data'):
            results = voucher_data['data'].get('results', [])
            voucher_list = [
                f"Voucher #{v.get('voucher_id', '?')}: ${v.get('amount', '0')} — {v.get('description', 'No description')} ({v.get('voucher_date', 'No date')})"
                for v in results[:10]
            ]
            parts.append(f"CSuite Vouchers:\n" + "\n".join(voucher_list))
            logger.info(f"Found {len(voucher_list)} vouchers")
    except Exception as e:
        logger.error(f"Error fetching vouchers: {e}")
    return parts


def _gather_profile_context(query: str, csuite) -> list:
    """Profile queries → CSuite. Search by name if possible."""
    parts = []
    name = _extract_name(query)

    if name:
        logger.info(f"Searching CSuite profiles for: {name}")
        try:
            profile_data = csuite.search_profiles(name)
            if profile_data.get('success') and profile_data.get('data'):
                results = profile_data['data'].get('results', [])
                if results:
                    profile_list = [
                        f"{p.get('name', 'Unknown')} (ID: {p.get('profile_id', 'N/A')}, Email: {p.get('email', 'N/A')})"
                        for p in results[:10]
                    ]
                    parts.append(f"CSuite Profiles matching '{name}':\n" + "\n".join(profile_list))
                    logger.info(f"Found {len(profile_list)} matching profiles")
        except Exception as e:
            logger.error(f"Error searching profiles: {e}")

    if not parts:
        logger.info("Fetching CSuite profiles (generic)...")
        try:
            profile_data = csuite.get_profiles(limit=10)
            if profile_data.get('success') and profile_data.get('data'):
                results = profile_data['data'].get('results', [])
                profile_list = [
                    f"{p.get('name', 'Unknown')} (ID: {p.get('profile_id', 'N/A')})"
                    for p in results[:10]
                ]
                parts.append(f"CSuite Profiles:\n" + "\n".join(profile_list))
                logger.info(f"Found {len(profile_list)} profiles")
        except Exception as e:
            logger.error(f"Error fetching profiles: {e}")

    return parts


def _gather_fund_contacts_context(query: str, query_lower: str, hubspot, csuite) -> list:
    """Find HubSpot contacts linked to a specific CSuite fund.

    Flow: extract fund name/ID → resolve to funit_id via CSuite if needed
          → search HubSpot contacts by csuite_fund_id property.
    """
    parts = []
    name = _extract_name(query, stop_words=_FUND_NAME_STOP_WORDS)
    # Fund path: the same strict reference rules as the balance gatherer.
    ref = extract_fund_ref(query)
    fund_id = str(ref["id"]) if ref and "id" in ref else None
    if ref and "code" in ref:
        name = ref["code"]

    # If no numeric ID, try to resolve fund name → funit_id via CSuite search
    if name and not fund_id:
        logger.info(f"Resolving fund name to ID for: {name}")
        try:
            search = csuite.search_funds(name)
            if search.get('success') and search.get('data'):
                results = search['data'].get('results', [])
                if results:
                    # funit/list/search rows key the id as `id`, not `funit_id`.
                    fund_id = str(_fund_row_id(results[0]) or '')
                    names = _fund_row_names(results[0])
                    fund_display = names[0] if names else name
                    logger.info(f"Resolved '{name}' to fund ID {fund_id}")
        except Exception as e:
            logger.error(f"Error resolving fund name: {e}")

    if not fund_id:
        return parts

    fund_display = fund_display if 'fund_display' in dir() else f"Fund {fund_id}"

    logger.info(f"Searching HubSpot contacts for fund ID: {fund_id}")
    try:
        contacts = hubspot.search_contacts_by_csuite_fund_id(fund_id)
        results = contacts.get('results', [])
        if results:
            contact_list = [
                f"{c.get('properties', {}).get('firstname', '')} "
                f"{c.get('properties', {}).get('lastname', '')} "
                f"({c.get('properties', {}).get('email', 'no email')})"
                for c in results
            ]
            parts.append(
                f"HubSpot Contacts associated with {fund_display} (ID: {fund_id}):\n"
                + "\n".join(contact_list)
            )
            logger.info(f"Found {len(contact_list)} contacts for fund {fund_id}")
        else:
            parts.append(
                f"No HubSpot contacts found linked to {fund_display} (ID: {fund_id}). "
                "Contacts are linked when a DAF is processed through Jidhr."
            )
    except Exception as e:
        logger.error(f"Error searching contacts by fund ID: {e}")

    return parts


# ---------------------------------------------------------------------------
# Giving Circle context (Lisa)
# ---------------------------------------------------------------------------

def _gather_giving_circle_context(query_lower: str, hubspot) -> list:
    """Fetch Giving Circle data from BOTH HubSpot lists. Returns raw data only — no analysis."""
    parts = []

    # --- List 126: AMCF Women's Giving Circle (Static, 130 members) ---
    try:
        members_126 = hubspot.get_giving_circle_member_details(limit=130)
        if members_126:
            lines = [
                f"**AMCF Women's Giving Circle** (List 126 — Static)",
                f"Members: {len(members_126)}",
                "",
            ]
            for c in members_126[:10]:
                props = c.get("properties", {})
                name = f"{props.get('firstname', '')} {props.get('lastname', '')}".strip()
                email = props.get("email", "no email")
                lines.append(f"  - {name} ({email})")
            if len(members_126) > 10:
                lines.append(f"  ...and {len(members_126) - 10} more")
            parts.append("\n".join(lines))
            logger.info(f"List 126: {len(members_126)} GC members")
        else:
            parts.append("List 126 (AMCF Women's Giving Circle): No members found.")
    except Exception as e:
        logger.exception(f"Error fetching List 126 (GC members): {e}")

    # --- List 31: Giving Circle Email List (Active, ~450 contacts) ---
    try:
        memberships_31 = hubspot._get(
            f"crm/v3/lists/{Config.GIVING_CIRCLE_EMAIL_LIST_ID}/memberships",
            {"limit": 250}
        )
        count_31 = len(memberships_31.get("results", [])) if memberships_31 else 0

        if count_31 > 0:
            # Fetch first 10 contact details for display
            record_ids = [str(m.get("recordId")) for m in memberships_31.get("results", [])[:10]]
            contacts_31 = []
            if record_ids:
                batch_result = hubspot._post("crm/v3/objects/contacts/batch/read", {
                    "inputs": [{"id": rid} for rid in record_ids],
                    "properties": ["firstname", "lastname", "email"]
                })
                if batch_result and "results" in batch_result:
                    contacts_31 = batch_result["results"]

            lines = [
                f"\n**Giving Circle Email List** (List 31 — Active)",
                f"Contacts: {count_31}+",
                f"Filter: GC Email form submission OR constituent code contains 'American Muslim Women's Giving Circle'",
                "",
            ]
            for c in contacts_31[:10]:
                props = c.get("properties", {})
                name = f"{props.get('firstname', '')} {props.get('lastname', '')}".strip()
                email = props.get("email", "no email")
                lines.append(f"  - {name} ({email})")
            if count_31 > 10:
                lines.append(f"  ...and {count_31 - 10} more")
            parts.append("\n".join(lines))
            logger.info(f"List 31: {count_31} GC email contacts")
        else:
            parts.append("\nList 31 (Giving Circle Email List): No contacts found.")
    except Exception as e:
        logger.exception(f"Error fetching List 31 (GC email list): {e}")

    return parts