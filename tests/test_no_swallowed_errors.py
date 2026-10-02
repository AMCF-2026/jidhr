"""An exception must never read as business as usual.

Two handlers turned a raise into a normal-looking reply:

* **The ticket step.** A crash during LOOKUP, before any ticket was chosen, left
  `ticket_matches` empty and the reply said
  `📋 No matching ticket — nothing was closed.` That is a normal outcome. Nobody
  knew whether a ticket had matched.
* **The duplicate guard.** `existing_hubspot_link` returned `(None, None)` both
  when a contact was absent and when the lookup FAILED, so a HubSpot outage read
  as "this donor has no stored link", the guard fell through to the CSuite search
  alone, and `✅ Profile Created` was printed over a duplicate check that had run
  half of itself. Its docstring claimed the caller stopped on a failed lookup;
  the caller could not, because it could not tell.

Everything else already said something distinct, and is asserted here too so a
later change cannot quietly flatten one of them.

No network.
"""

import pytest

from config import Config
from intents import daf_workflow
from intents.daf_workflow import existing_hubspot_link
from tests.csuite_doubles import NoDuplicates, contact

DAF = Config.TICKET_PIPELINES["daf"]
EMAIL = "s@example.invalid"


class CSuite(NoDuplicates):
    base_url = "https://amuslimcf.fcsuite.com/api/v2"

    def __init__(self, fail_on=None):
        self.fail_on = fail_on or set()
        self.calls = []

    def create_individual_profile(self, **kwargs):
        self.calls.append("profile")
        if "profile" in self.fail_on:
            raise RuntimeError("csuite exploded")
        return {"success": True, "data": {"profile_id": 21900}}

    def create_task(self, **kwargs):
        self.calls.append("task")
        if "task" in self.fail_on:
            raise RuntimeError("task exploded")
        return {"success": True, "data": {"task_id": 1099}, "verified": True}


class HubSpot:
    def __init__(self, raise_on=None, tickets=None):
        self.raise_on = raise_on or set()
        self.tickets = tickets or []
        self.closed = []

    def search_contact_by_email(self, email, properties=None):
        if "search" in self.raise_on:
            raise RuntimeError("hubspot search exploded")
        return contact("70123")

    def update_contact_by_email(self, email, properties):
        if "patch" in self.raise_on:
            raise RuntimeError("hubspot patch exploded")
        return {"id": "70123"}

    def get_contact_tickets(self, contact_id, properties=None):
        if "tickets" in self.raise_on:
            raise RuntimeError("hubspot tickets exploded")
        return self.tickets

    def close_ticket(self, ticket_id):
        if "close" in self.raise_on:
            raise RuntimeError("hubspot close exploded")
        self.closed.append(ticket_id)
        return {"id": ticket_id}


def run(monkeypatch, csuite=None, hubspot=None, task=False, ticket=True):
    monkeypatch.setattr("config.Config.CSUITE_ENV", "live")
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", task)
    monkeypatch.setattr(Config, "CSUITE_HUBSPOT_BACKFILL_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_TICKET_CLOSE_ENABLED", ticket)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", 1004)
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", None)
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": {"first_name": "S", "last_name": "A",
                                 "email": EMAIL},
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, hubspot or HubSpot(),
                                      csuite or CSuite())
    return reply, state


def ticket_row(tid="T1", subject="DAF Form Submission - S A"):
    return {"id": tid, "properties": {
        "subject": subject, "content": "",
        "hs_pipeline": DAF["pipeline"], "hs_pipeline_stage": DAF["new_stage"]}}


# ---------------------------------------------------------------------------
# The ticket step
# ---------------------------------------------------------------------------

def test_a_raise_during_ticket_LOOKUP_is_not_reported_as_none_found(monkeypatch):
    """The bug. Before this, the reply read "No matching ticket — nothing was
    closed", which is what a clean run with no candidates says."""
    reply, _ = run(monkeypatch, hubspot=HubSpot(raise_on={"tickets"}))

    assert "⚠️ Ticket: ERROR — not closed, see log" in reply
    assert "hubspot tickets exploded" in reply
    assert "No matching ticket" not in reply
    assert "Ticket close: off" not in reply


def test_a_raise_during_the_CLOSE_still_names_the_ticket(monkeypatch):
    """A ticket was chosen, so the user needs to know which one is still open —
    this path already worked and must keep working."""
    hs = HubSpot(raise_on={"close"}, tickets=[ticket_row("T-E")])
    reply, _ = run(monkeypatch, hubspot=hs)

    assert "NOT closed" in reply
    assert "T-E" in reply
    assert "hubspot close exploded" in reply


def test_a_clean_run_with_no_candidates_still_says_none_found(monkeypatch):
    """The normal outcome is unchanged — that is the point of the distinction."""
    reply, _ = run(monkeypatch, hubspot=HubSpot(tickets=[]))

    assert "📋 No matching ticket — nothing was closed." in reply
    assert "ERROR" not in reply


def test_the_flag_being_off_is_still_its_own_line(monkeypatch):
    reply, _ = run(monkeypatch, ticket=False)

    assert "🎫 Ticket close: off" in reply
    assert "ERROR" not in reply
    assert "No matching ticket" not in reply


def test_there_is_only_ever_one_ticket_line(monkeypatch):
    for kwargs in ({"hubspot": HubSpot(raise_on={"tickets"})},
                   {"hubspot": HubSpot(tickets=[])},
                   {"hubspot": HubSpot(tickets=[ticket_row()])},
                   {"ticket": False}):
        reply, _ = run(monkeypatch, **kwargs)
        lines = [l for l in reply.splitlines()
                 if l.startswith(("📋", "🎫")) or "Ticket" in l]
        assert len(lines) == 1, (kwargs, lines)


# ---------------------------------------------------------------------------
# The duplicate guard
# ---------------------------------------------------------------------------

def test_a_failed_hubspot_lookup_stops_the_create(monkeypatch):
    """Half the duplicate check became unavailable. Creating now is how a second
    profile for the same donor gets made, and CSuite has no idempotency key."""
    csuite = CSuite()
    reply, state = run(monkeypatch, csuite=csuite,
                       hubspot=HubSpot(raise_on={"search"}))

    assert "profile" not in csuite.calls, "nothing may be created"
    assert state["profile_id"] is None
    assert "🛑 **No profile created — a duplicate could not be ruled out**" in reply
    assert "the HubSpot contact could not be read" in reply
    assert "Profile Created" not in reply


def test_an_absent_contact_still_lets_the_create_proceed(monkeypatch):
    """The distinction cuts both ways: absence is not failure."""
    class NoContact(HubSpot):
        def search_contact_by_email(self, email, properties=None):
            return {"results": []}

    csuite = CSuite()
    reply, state = run(monkeypatch, csuite=csuite, hubspot=NoContact())

    assert "profile" in csuite.calls
    assert state["profile_id"] == 21900


@pytest.mark.parametrize("response", [{"error": "nope"}, "not a dict", None, 42])
def test_an_error_shaped_response_counts_as_a_failed_read(response):
    class Odd:
        def search_contact_by_email(self, email, properties=None):
            return response

    assert existing_hubspot_link(EMAIL, Odd()) == (None, None, False)


def test_nothing_to_look_up_is_not_a_failure():
    """No email means no lookup, which must not stop the create."""
    assert existing_hubspot_link("", None) == (None, None, True)
    assert existing_hubspot_link(None, object()) == (None, None, True)


# ---------------------------------------------------------------------------
# The handlers that were already distinct — pinned so they stay that way
# ---------------------------------------------------------------------------

def test_a_raise_in_the_profile_create_says_failed_not_created(monkeypatch):
    csuite = CSuite(fail_on={"profile"})
    reply, state = run(monkeypatch, csuite=csuite)

    assert state["profile_id"] is None
    assert "❌ **DAF NOT Created**" in reply
    assert "❌ Profile: Failed to create" in reply
    assert "csuite exploded" in reply
    assert "Profile Created" not in reply


def test_a_raise_in_the_task_create_says_NOT_created(monkeypatch):
    csuite = CSuite(fail_on={"task"})
    reply, state = run(monkeypatch, csuite=csuite, task=True)

    assert state["profile_id"] == 21900, "the profile still succeeded"
    assert "⚠️ Follow-up task NOT created (task exploded)" in reply
    assert "add it by hand in CSuite" in reply
    assert "📝 No task:" not in reply, "a raise is not a skip"


def test_a_raise_in_the_hubspot_patch_says_could_not_update(monkeypatch):
    reply, state = run(monkeypatch, hubspot=HubSpot(raise_on={"patch"}))

    assert state["profile_id"] == 21900
    assert "⚠️ HubSpot contact: Could not update or create" in reply
    assert "hubspot patch exploded" in reply
    assert "HubSpot contact updated" not in reply
    assert "sandbox run" not in reply, "a raise is not a refusal"


def test_a_failed_relink_check_withholds_the_patch_and_says_why(monkeypatch):
    """The second lookup, the one guarding against overwriting a stored id."""
    calls = {"n": 0}

    class FlakyOnSecond(HubSpot):
        def search_contact_by_email(self, email, properties=None):
            calls["n"] += 1
            if calls["n"] >= 2:
                raise RuntimeError("second lookup exploded")
            return contact("70123")

    reply, state = run(monkeypatch, hubspot=FlakyOnSecond())

    assert state["profile_id"] == 21900
    assert "could not be re-read before the update" in reply
    assert "verify the link by hand" in reply
    assert "HubSpot contact updated" not in reply
