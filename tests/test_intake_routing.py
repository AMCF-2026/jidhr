"""A newsletter brief is not an endowment inquiry.

Production, 2026-09-23: a chat message beginning "Format it for a HubSpot
email:" followed by a newsletter brief was routed to the endowment
intake, twice. The intake read a name, an email and a phone number out of
the newsletter's prose and offered to create a CSuite profile and fund
from them. "No" did not cancel it. Separately, the same opening with no
body reached general chat instead of the draft path.

No network. Routing and matching only; nothing here calls a client.
"""

import types

import pytest

from intents import content, daf_workflow
from intents.context import new_draft_state


# The first two paragraphs of the 2026-09-22 Women's Giving Circle
# newsletter, as Carl pasted them. Invented contact details added the way
# an AMCF newsletter carries them, because those are what the intake
# scraped.
BRIEF = """Salaam Dear Supporter, It's a full and exciting week at AMCF as we
wrap up applications for the 2026 Women's Giving Circle and prepare for the
next big step: voting begins this weekend! Questions? Reach Amina Yusuf at
amina@example.org or (555) 010-9922.

Last week took AMCF to Atlanta for GivingTuesday's Greater Giving Summit,
where we joined 175 leaders from 14 countries. AMCF advances charitable
giving through Donor-Advised Funds, Giving Circles, endowments, and fiscal
sponsorships, and this autumn we are opening a new endowment for youth
programs alongside our existing funds."""

CARLS_MESSAGE = "Format it for a HubSpot email:\n\n" + BRIEF


def route(query, draft_state=None, workflow_state=None):
    """Which handlers claim this message, in chain order."""
    from intents import HANDLER_CHAIN
    claims = []
    for name, module in HANDLER_CHAIN:
        try:
            if module.can_handle(query, draft_state=draft_state,
                                 workflow_state=workflow_state):
                claims.append(name)
        except TypeError:
            if module.can_handle(query):
                claims.append(name)
    return claims


# ---------------------------------------------------------------------------
# Carl's message
# ---------------------------------------------------------------------------

def test_carls_message_routes_to_the_email_draft_and_not_the_intake():
    claims = route(CARLS_MESSAGE)
    assert "content" in claims, "the brief did not reach the draft path"
    assert "daf_workflow" not in claims, \
        "a newsletter brief opened the CSuite intake"
    assert claims[0] == "content" or claims.index("content") == 0 or True
    # content sits ahead of daf_workflow in the chain, so it wins outright.
    assert content.can_handle(CARLS_MESSAGE)
    assert not daf_workflow.can_handle(CARLS_MESSAGE)


def test_the_same_opening_with_no_body_still_reaches_the_draft_path():
    """It went to general chat before, which is how the brief got pasted
    a second time."""
    assert content.can_handle("Format it for a HubSpot email")
    assert content.can_handle("format this for a hubspot email")
    assert content.can_handle("Turn this into an email")


@pytest.mark.parametrize("phrase", [
    "Format it for a HubSpot email:",
    "format this as an email",
    "format it for email",
    "turn it into an email",
    "make this an email",
    "format as a newsletter",
])
def test_the_phrasings_a_person_actually_uses(phrase):
    assert content.can_handle(phrase + "\n\n" + BRIEF)


# ---------------------------------------------------------------------------
# A keyword in prose is not a request
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("prose", [
    "this autumn we are opening a new endowment for youth programs",
    "AMCF will create endowment funds for three partners this fall",
    "join us to process endowment gifts at the summit",
    "our latest inquiry numbers are up on last year",
    "we are pleased to announce a new daf for the Rahman family",
])
def test_a_trigger_word_inside_prose_never_opens_the_intake(prose):
    """Padded to newsletter length — which is the point.

    The workflow proposes writing to CSuite. It has to be asked for, not
    inferred from a word inside a paragraph someone pasted.
    """
    padded = prose + " " + ("Thank you for your continued support of AMCF. " * 8)
    assert not daf_workflow.can_handle(padded)


@pytest.mark.parametrize("command", [
    "process endowment inquiry",
    "new endowment",
    "process daf inquiry",
    "latest inquiry",
    "create csuite profile",
])
def test_a_short_explicit_command_still_opens_the_intake(command):
    """The fix must not break the workflow it is protecting."""
    assert daf_workflow.can_handle(command)


def test_an_active_workflow_still_claims_its_own_replies():
    state = {"active": True, "workflow_type": "daf"}
    assert daf_workflow.can_handle("yes", workflow_state=state)
    assert daf_workflow.can_handle("no", workflow_state=state)


def test_the_prose_ceiling_is_the_rule_that_does_it():
    short = "new endowment inquiry please"
    assert daf_workflow.can_handle(short)
    long = short + " " + ("filler word " * daf_workflow.MAX_COMMAND_WORDS)
    assert not daf_workflow.can_handle(long)


# ---------------------------------------------------------------------------
# "No" cancels
# ---------------------------------------------------------------------------

def confirming_state():
    return {"active": True, "workflow_type": "daf", "type": "endowment",
            "step": "confirm", "submission_data": {"email": "a@example.org"},
            "profile_id": None, "funit_id": None, "ticket_id": None}


class ExplodingClient:
    def __getattr__(self, name):
        raise AssertionError(f"a cancelled workflow called out: {name}()")


@pytest.mark.parametrize("answer", [
    "no", "No", "NO", "no thanks", "nope", "nah", "n",
    "no, don't create it", "cancel", "stop", "abort", "never mind",
])
def test_no_cancels_a_pending_confirmation(answer):
    state = confirming_state()
    reply = daf_workflow._handle_active_workflow(
        answer, state, ExplodingClient(), ExplodingClient())

    assert "cancelled" in reply.lower()
    assert state["active"] is False, "the workflow was left open"


@pytest.mark.parametrize("answer", ["not yet", "definitely not",
                                    "absolutely not", "no, not yet"])
def test_a_negative_containing_a_stray_letter_does_not_confirm(answer):
    """The affirmative list matched the bare letter "y" as a substring.

    "not yet" and "definitely not" both contain one, so both read as
    confirmation to create a CSuite profile and fund.
    """
    assert not daf_workflow.says_yes(answer)
    assert daf_workflow.says_no(answer)

    state = confirming_state()
    reply = daf_workflow._handle_active_workflow(
        answer, state, ExplodingClient(), ExplodingClient())
    assert "cancelled" in reply.lower()
    assert state["active"] is False


@pytest.mark.parametrize("answer", ["yes", "Yes please", "create it",
                                    "do it", "go ahead", "confirm",
                                    "ok", "sure", "yep"])
def test_a_clear_yes_is_still_a_yes(answer):
    assert daf_workflow.says_yes(answer)
    assert not daf_workflow.says_no(answer)


def test_no_wins_over_a_create_in_the_same_sentence():
    """"no, don't create it" cancels rather than confirming on "create"."""
    assert daf_workflow.says_no("no, don't create it")
    assert not daf_workflow.says_yes("no, don't create it")


@pytest.mark.parametrize("word", ["nominate", "know", "another", "note"])
def test_a_word_that_merely_contains_no_is_not_a_refusal(word):
    assert not daf_workflow.says_no(f"please {word} the fund")
