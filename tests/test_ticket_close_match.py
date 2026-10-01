"""A ticket is closed on the donor's email address and nothing else.

Until 2026-10-01 the workflow matched the donor's FIRST NAME or LAST NAME as
a substring of a ticket's subject or content, and closed the first hit. So an
inquiry from any Sarah closed the oldest open ticket with "sarah" anywhere in
it — another donor's thread, a vendor's name, "Sarah to follow up". A given
name is not an identifier.

An address is. Matched in full, case-insensitively, and when more than one
open ticket carries it the workflow closes NOTHING: "which of these two" is a
judgement, and closing the wrong one is not something this workflow can undo.

No network.
"""

import pytest

from config import Config
from intents import daf_workflow
from intents.daf_workflow import matching_tickets

EMAIL = "sarah.ahmed@example.invalid"


def ticket(tid, subject="", content=""):
    return {"id": tid, "properties": {"subject": subject, "content": content}}


def tickets(*rows):
    return {"results": list(rows)}


# ---------------------------------------------------------------------------
# matching_tickets
# ---------------------------------------------------------------------------

def test_the_email_in_the_subject_matches():
    found = matching_tickets(tickets(ticket("T1", subject=f"DAF for {EMAIL}")),
                             EMAIL)
    assert [m["id"] for m in found] == ["T1"]


def test_the_email_in_the_content_matches():
    found = matching_tickets(
        tickets(ticket("T1", subject="New inquiry",
                       content=f"Reply to {EMAIL} please")), EMAIL)
    assert [m["id"] for m in found] == ["T1"]


@pytest.mark.parametrize("stored,submitted", [
    (EMAIL.upper(), EMAIL),
    (EMAIL, EMAIL.upper()),
    (f"  {EMAIL}  ", EMAIL),
    (EMAIL, f"  {EMAIL.title()}  "),
])
def test_the_match_is_case_insensitive_and_trimmed(stored, submitted):
    found = matching_tickets(tickets(ticket("T1", subject=stored)), submitted)
    assert [m["id"] for m in found] == ["T1"]


def test_a_first_name_no_longer_matches():
    """The behaviour this change exists to remove."""
    found = matching_tickets(
        tickets(ticket("T1", subject="Call Sarah about the gala"),
                ticket("T2", content="sarah to follow up on the grant")),
        EMAIL)
    assert found == []


def test_a_last_name_only_match_closes_nothing():
    found = matching_tickets(
        tickets(ticket("T1", subject="Ahmed family endowment"),
                ticket("T2", content="ahmed asked about fees")),
        EMAIL)
    assert found == []


def test_a_partial_address_does_not_match():
    """A truncated address is a different address."""
    for near in ("sarah.ahmed@example", "ahmed@example.invalid",
                 "sarah.ahmed@example.invalid.uk"):
        found = matching_tickets(tickets(ticket("T1", subject=near)), EMAIL)
        assert found == [] or near.startswith(EMAIL), near


def test_no_email_on_the_submission_matches_nothing():
    """Nothing to match on means nothing to close, not "close the first one"."""
    for email in (None, "", "   "):
        assert matching_tickets(
            tickets(ticket("T1", subject="anything at all")), email) == []


def test_a_missing_or_odd_ticket_payload_is_survivable():
    assert matching_tickets({}, EMAIL) == []
    assert matching_tickets({"results": None}, EMAIL) == []
    assert matching_tickets(None, EMAIL) == []
    assert matching_tickets({"results": [{"id": "T1"}]}, EMAIL) == []
    assert matching_tickets(
        {"results": [{"id": "T1", "properties": {"subject": None,
                                                 "content": None}}]},
        EMAIL) == []


def test_a_ticket_with_no_subject_is_still_identifiable():
    found = matching_tickets(
        tickets(ticket("T1", subject="", content=EMAIL)), EMAIL)
    assert found[0]["subject"] == "(no subject)"


# ---------------------------------------------------------------------------
# Through the workflow
# ---------------------------------------------------------------------------

class CSuite:
    def create_individual_profile(self, **kwargs):
        return {"success": True, "data": {"profile_id": 21680}}


class HubSpot:
    def __init__(self, open_tickets):
        self.open_tickets = open_tickets
        self.closed = []

    def search_contact_by_email(self, email):
        return {"results": [{"id": "70123"}]}

    def update_contact_by_email(self, email, properties):
        return {"id": "70123"}

    def get_open_tickets(self):
        return self.open_tickets

    def close_ticket(self, ticket_id):
        self.closed.append(ticket_id)
        return {"id": ticket_id}


def run(monkeypatch, open_tickets, email=EMAIL):
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    hubspot = HubSpot(open_tickets)
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": {"first_name": "Sarah", "last_name": "Ahmed",
                                 "email": email},
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, hubspot, CSuite())
    return reply, hubspot, state


def test_two_tickets_share_the_first_name_and_only_the_email_one_closes(
        monkeypatch):
    """The brief's first case, and the exact bug being fixed."""
    reply, hubspot, state = run(monkeypatch, tickets(
        ticket("T-NAME", subject="Sarah — gala seating"),
        ticket("T-EMAIL", subject="DAF inquiry", content=f"from {EMAIL}"),
        ticket("T-NAME2", content="sarah asked about fees")))

    assert hubspot.closed == ["T-EMAIL"], "only the email match may close"
    assert state["ticket_id"] == "T-EMAIL"
    assert "📋 Ticket T-EMAIL closed" in reply
    assert "DAF inquiry" in reply
    assert "T-NAME" not in reply


def test_a_last_name_only_match_closes_nothing(monkeypatch):
    """The brief's second case."""
    reply, hubspot, _ = run(monkeypatch, tickets(
        ticket("T-LAST", subject="Ahmed family endowment"),
        ticket("T-LAST2", content="ahmed wants a call")))

    assert hubspot.closed == []
    assert "No matching ticket" in reply
    assert "nothing was closed" in reply


def test_two_email_matches_close_nothing_and_both_are_listed(monkeypatch):
    """The brief's third case. Closing the wrong one is not reversible here."""
    reply, hubspot, state = run(monkeypatch, tickets(
        ticket("T-A", subject="DAF inquiry", content=EMAIL),
        ticket("T-B", subject=f"Follow-up for {EMAIL}")))

    assert hubspot.closed == [], "ambiguity must not be resolved by guessing"
    assert state["ticket_id"] is None
    assert "2 open tickets" in reply
    assert "none was closed" in reply
    for tid, subject in (("T-A", "DAF inquiry"),
                         ("T-B", f"Follow-up for {EMAIL}")):
        assert tid in reply and subject in reply


def test_no_open_tickets_at_all_says_so(monkeypatch):
    reply, hubspot, _ = run(monkeypatch, tickets())

    assert hubspot.closed == []
    assert "No matching ticket" in reply


def test_a_submission_without_an_email_closes_nothing(monkeypatch):
    reply, hubspot, _ = run(monkeypatch, tickets(
        ticket("T1", subject="Sarah Ahmed DAF")), email="")

    assert hubspot.closed == []
    assert "No matching ticket" in reply


def test_a_failed_close_still_reports_the_ticket_as_open(monkeypatch):
    class Refusing(HubSpot):
        def close_ticket(self, ticket_id):
            return {"error": "insufficient scopes"}

    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    hubspot = Refusing(tickets(ticket("T-E", subject=EMAIL)))
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": {"first_name": "Sarah", "last_name": "Ahmed",
                                 "email": EMAIL},
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, hubspot, CSuite())

    assert "NOT closed" in reply
    assert "insufficient scopes" in reply
    assert "T-E" in reply


def test_the_closed_line_always_names_the_ticket(monkeypatch):
    """A bare "Ticket closed" does not let anyone check it closed the right
    thing — which, until this change, it often had not."""
    reply, _, _ = run(monkeypatch, tickets(
        ticket("T-E", subject="Ahmed DAF inquiry", content=EMAIL)))

    assert "T-E" in reply
    assert "Ahmed DAF inquiry" in reply
