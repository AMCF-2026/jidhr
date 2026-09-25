"""A pasted brief is routed by what it asks for, not by a word inside it.

Two production failures, one fix:

    2026-09-23  "Format it for a HubSpot email:" + brief -> the CSuite
                endowment intake, which scraped a name, an email and a
                phone number out of the newsletter.
    2026-09-25  "Format email for HubSpot:" + brief -> a report of 34
                investment requests, because the prose contained
                "investment requests". General chat then echoed the brief
                back styled as a draft, so the save found nothing.

Both messages said plainly on their first line what was wanted. Nothing
read it.

No network. Routing and matching only.
"""

import pytest

from intents import HANDLER_CHAIN, anchors, content


# ---------------------------------------------------------------------------
# The two messages, as sent
# ---------------------------------------------------------------------------

SEPT_23 = "Format it for a HubSpot email:\n\n" + (
    "Salaam Dear Supporter, It's a full and exciting week at AMCF as we wrap "
    "up applications for the 2026 Women's Giving Circle. Questions? Reach "
    "Amina Yusuf at amina@example.org or (555) 010-9922. AMCF advances "
    "charitable giving through Donor-Advised Funds, Giving Circles, "
    "endowments and fiscal sponsorships, and this autumn we are opening a "
    "new endowment for youth programs. " * 4)

SEPT_25 = "Format email for HubSpot:\n\n" + (
    "Investment values differ from one organization to the next. Submit your "
    "investment requests by Friday, and reach out with any inquiry about a "
    "new fund or an endowment distribution. " * 12)


def claims(query, draft_state=None, workflow_state=None):
    """Every handler that would take this message, in chain order."""
    taken = []
    for name, module in HANDLER_CHAIN:
        try:
            matched = module.can_handle(query, draft_state=draft_state,
                                        workflow_state=workflow_state)
        except TypeError:
            matched = module.can_handle(query)
        if matched:
            taken.append(name)
    return taken


@pytest.mark.parametrize("message, label", [
    (SEPT_23, "2026-09-23"), (SEPT_25, "2026-09-25")])
def test_both_production_messages_route_to_content(message, label):
    taken = claims(message)
    assert taken and taken[0] == "content", \
        f"{label} routed to {taken or 'nothing'}"
    assert "reports" not in taken
    assert "daf_workflow" not in taken


@pytest.mark.parametrize("word", ["investment requests", "endowment",
                                  "new fund inquiry", "latest inquiry",
                                  "distribution schedule", "open tickets",
                                  "dormant funds", "log a note"])
def test_a_trigger_phrase_in_prose_enters_no_intake(word):
    """Padded past the document ceiling, which is what a brief is."""
    brief = ("Format it for a HubSpot email:\n\nThis week at AMCF we cover "
             f"a great deal, including {word}, alongside our usual updates. "
             + "Thank you for your continued support of the foundation. " * 12)
    taken = claims(brief)
    assert taken == ["content"], f"{word!r} pulled in {taken}"


@pytest.mark.parametrize("command, expected", [
    ("show me open tickets", "tickets"),
    ("dormant funds report", "reports"),
    ("investment requests", "reports"),
    ("process endowment inquiry", "daf_workflow"),
])
def test_a_real_command_still_reaches_its_handler(command, expected):
    """The anchor must not break the handlers it protects."""
    assert expected in claims(command)


# ---------------------------------------------------------------------------
# The anchor rule itself
# ---------------------------------------------------------------------------

def test_a_trigger_beyond_the_window_does_not_count():
    lead = "x" * (anchors.TRIGGER_WINDOW_CHARS + 5)
    assert not anchors.anchored(lead + " open tickets", ["open tickets"])
    assert anchors.anchored("open tickets", ["open tickets"])


def test_a_trigger_inside_a_document_does_not_count():
    long = "open tickets " + ("filler " * 200)
    assert len(long) > anchors.MAX_COMMAND_CHARS
    assert not anchors.anchored(long, ["open tickets"])


def test_a_polite_preamble_still_anchors():
    assert anchors.anchored(
        "Hi Jidhr, could you show me the open tickets please",
        ["open tickets"])


def test_first_line_skips_blank_lines():
    assert anchors.first_line("\n\n  Format email for HubSpot:  \nbody") == \
        "format email for hubspot:"
    assert anchors.first_line("") == ""


def test_every_handler_defers_to_an_explicit_content_request():
    """Stated in each intake, not left to chain order.

    Chain order was never what failed — content.can_handle returning
    False was — but an intake that re-derives the precedence cannot lose
    it to a reordering either.
    """
    import inspect
    from intents import (content_report, daf_workflow, donor_prep, events,
                         notes, reports, social_sync, sync_commands, tickets)

    for module in (sync_commands, social_sync, content_report, daf_workflow,
                   events, notes, donor_prep, tickets, reports):
        source = inspect.getsource(module.can_handle)
        assert "yields_to_content" in source, module.__name__


# ---------------------------------------------------------------------------
# content claims by the shape of the ask
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("opening", [
    "Format email for HubSpot:",
    "Format it for a HubSpot email:",
    "FORMAT EMAIL FOR HUBSPOT",
    "Please format this as a newsletter:",
    "turn this into an email",
    "Make this an email for HubSpot",
    "write the newsletter",
    "Draft an email:",
])
def test_the_first_line_is_what_claims_the_message(opening):
    assert content.can_handle(opening + "\n\n" + ("body text. " * 200))


def test_a_long_brief_whose_first_line_only_says_newsletter():
    """No verb at all — but nothing else looks like this."""
    assert content.can_handle("Newsletter copy:\n\n" + ("word " * 400))
    # The same line, short, is just a remark.
    assert not content.can_handle("Newsletter copy:")


@pytest.mark.parametrize("query", [
    "what is the fund balance for END0026",
    "show me open tickets",
    "how many investment requests came in this month",
    "log a note about the call with Ahmed",
])
def test_ordinary_questions_are_not_claimed_as_email_requests(query):
    assert not content._is_email_draft_request(query.lower())
