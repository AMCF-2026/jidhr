"""The live CSuite profile create is off until the field names are fixed.

intents/daf_workflow.py has called create_individual_profile since
2026-03-17. That method sends `primary_email`, which CSuite does not
recognise as an input: it answers HTTP 200 with a profile_id and discards
the value. `primary_phone_number` and `primary_address_string` are the same
shape of guess, and `phone_number` is now known to be the real name for one
of them.

So a profile created by this path is missing its email, and probably its
phone and address, and the response says nothing about it. The switch is
off by default, and off means nothing is sent.

No network.
"""

import logging

import pytest

from tests.csuite_doubles import NoDuplicates, contact

from config import Config
from intents import daf_workflow


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------

class CSuiteNeverCalled:
    """Any CSuite call at all is the failure this test exists to catch."""

    def __getattr__(self, name):
        raise AssertionError(
            f"the create is disabled and CSuite.{name}() was still called")


class CSuiteCreating(NoDuplicates):
    def __init__(self, response=None):
        self.calls = []
        self.response = response or {"success": True,
                                     "data": {"profile_id": 31337}}

    def create_individual_profile(self, **kwargs):
        self.calls.append(kwargs)
        return self.response

    def create_fund(self, **kwargs):
        return {"success": True, "data": {"funit_id": 9001}}


class HubSpot:
    """Read-only. A write attempt fails the test."""

    def __init__(self, contact_id="70123", found=True):
        self.contact_id = contact_id
        self.found = found
        self.searches = 0
        self.ticket_sweeps = 0

    def search_contact_by_email(self, email):
        self.searches += 1
        if not self.found:
            return {"results": []}
        return {"results": [{"id": self.contact_id}]}

    def update_contact_by_email(self, email, properties):
        return {"id": self.contact_id}

    def get_open_tickets(self):
        self.ticket_sweeps += 1
        return {"results": []}

    def __getattr__(self, name):
        raise AssertionError(f"unexpected HubSpot call: {name}()")


SUBMISSION = {"first_name": "Testy", "last_name": "McTest",
              "email": "testy@example.invalid", "phone": "703-555-0100",
              "fund_name": "Test Family Fund"}


def confirming_state():
    return {"active": True, "workflow_type": "daf", "type": "daf",
            "step": "confirm", "submission_data": dict(SUBMISSION),
            "profile_id": None, "funit_id": None, "ticket_id": None}


def run(monkeypatch, enabled, csuite, hubspot=None):
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", enabled)
    state = confirming_state()
    reply = daf_workflow._step_create("yes", state, hubspot or HubSpot(),
                                      csuite)
    return reply, state


# ---------------------------------------------------------------------------
# The default
# ---------------------------------------------------------------------------

def test_the_switch_is_off_unless_the_environment_says_otherwise():
    assert Config.CSUITE_DAF_CREATE_ENABLED is False


@pytest.mark.parametrize("value,expected", [
    ("true", True), ("True", True), ("TRUE", True), (" true ", True),
    ("false", False), ("", False), ("1", False), ("yes", False),
    ("on", False), ("anything", False),
])
def test_only_the_word_true_turns_it_on(monkeypatch, value, expected):
    """A flag that reads "1" or "yes" as on is a flag that turns itself on
    by accident. Exactly one spelling enables a live write."""
    monkeypatch.setenv("CSUITE_DAF_CREATE_ENABLED", value)
    import importlib

    import config
    importlib.reload(config)
    try:
        assert config.Config.CSUITE_DAF_CREATE_ENABLED is expected
    finally:
        monkeypatch.delenv("CSUITE_DAF_CREATE_ENABLED", raising=False)
        importlib.reload(config)


# ---------------------------------------------------------------------------
# Off
# ---------------------------------------------------------------------------

def test_off_sends_nothing_to_csuite(monkeypatch):
    reply, state = run(monkeypatch, False, CSuiteNeverCalled())

    assert state["profile_id"] is None
    assert "not created" in reply.lower()


def test_off_reports_a_skip_not_a_failure(monkeypatch):
    """"Failed to create" over a run that never sent anything is the same
    false statement as CSuite's 200 over a discarded field."""
    reply, _ = run(monkeypatch, False, CSuiteNeverCalled())

    assert "Failed to create" not in reply
    assert "Created!" not in reply
    assert "turned off" in reply


def test_off_names_the_hubspot_contact_id_and_no_pii(monkeypatch, caplog):
    hubspot = HubSpot(contact_id="70123")
    with caplog.at_level(logging.WARNING, logger="intents.daf_workflow"):
        run(monkeypatch, False, CSuiteNeverCalled(), hubspot)

    skipped = [r for r in caplog.records if "SKIPPED" in r.getMessage()]
    assert len(skipped) == 1, "the skip must be logged exactly once"
    message = skipped[0].getMessage()
    assert "70123" in message
    assert skipped[0].levelno == logging.WARNING

    for private in ("testy@example.invalid", "Testy", "McTest",
                    "703-555-0100"):
        assert private not in message, f"{private!r} leaked into the log"


def test_off_says_so_when_no_contact_can_be_resolved(monkeypatch, caplog):
    """Silence would make the skip untraceable to a person."""
    with caplog.at_level(logging.WARNING, logger="intents.daf_workflow"):
        run(monkeypatch, False, CSuiteNeverCalled(), HubSpot(found=False))

    skipped = [r for r in caplog.records if "SKIPPED" in r.getMessage()]
    assert len(skipped) == 1
    assert "no HubSpot contact id" in skipped[0].getMessage()


def test_a_broken_hubspot_lookup_does_not_break_the_skip(monkeypatch, caplog):
    class Broken(HubSpot):
        def search_contact_by_email(self, email):
            raise RuntimeError("boom")

    with caplog.at_level(logging.WARNING, logger="intents.daf_workflow"):
        reply, _ = run(monkeypatch, False, CSuiteNeverCalled(), Broken())

    assert "not created" in reply.lower()


def test_off_leaves_the_rest_of_the_workflow_alone(monkeypatch):
    """Each downstream step keeps its own precondition.

    No profile means no fund and no id to write to HubSpot — loosening that
    would create a fund with nothing attached. The ticket sweep still runs.
    """
    hubspot = HubSpot()
    reply, state = run(monkeypatch, False, CSuiteNeverCalled(), hubspot)

    assert state["funit_id"] is None
    assert hubspot.ticket_sweeps == 1, "step 4 still runs"
    assert "Failed" not in reply, "nothing failed; nothing was attempted"


# ---------------------------------------------------------------------------
# On
# ---------------------------------------------------------------------------

def test_on_still_creates(monkeypatch):
    csuite = CSuiteCreating()
    reply, state = run(monkeypatch, True, csuite)

    assert len(csuite.calls) == 1
    assert csuite.calls[0]["email"] == "testy@example.invalid"
    assert state["profile_id"] == 31337
    assert "not created" not in reply.lower()


def test_on_surfaces_a_dropped_field_to_the_user(monkeypatch):
    """verify_writes annotates rather than raising, so somebody has to read
    the annotation. A 200 with a profile_id is not a successful write."""
    csuite = CSuiteCreating({
        "success": True, "data": {"profile_id": 31337},
        "verified": False,
        "fields_dropped": {"primary_email": ("a@b.invalid", None)}})
    reply, state = run(monkeypatch, True, csuite)

    assert state["profile_id"] == 31337
    assert "primary_email" in reply
    assert "did not store" in reply.lower()


def test_on_reports_a_real_failure_as_a_failure(monkeypatch):
    csuite = CSuiteCreating({"success": False, "error": "nope",
                             "http_status": 400})
    reply, state = run(monkeypatch, True, csuite)

    assert state["profile_id"] is None
    assert "Failed to create" in reply
