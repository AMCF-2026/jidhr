"""Endowment has its own switch, and the DAF switches no longer reach it.

Step 0 discovery, 2026-10-14: "new endowment" ran behind
CSUITE_DAF_CREATE_ENABLED and CSUITE_DAF_TASK_CREATE_ENABLED, both on in
production. The path it ran would have made an INDIVIDUAL profile for the
contact person, put the ORGANISATION's address on it, and dropped the org
name and the EIN.

V5.81 Part 1: CSUITE_ENDOWMENT_CREATE_ENABLED, default off, strict parsing.
Off means the endowment path refuses before anything is fetched.

No network, no database. All CSuite and HubSpot objects here are test
doubles.
"""

import importlib

import pytest

from config import Config
from intents import daf_workflow

from tests.test_daf_create_switch import (CSuiteCreating, CSuiteNeverCalled,
                                          HubSpot, SUBMISSION)


class HubSpotNeverCalled:
    """Any HubSpot call is the failure: off means nothing is even read."""

    def __getattr__(self, name):
        raise AssertionError(f"endowment is off and HubSpot.{name}() ran")


def _all_daf_switches_on(monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", True)


def _endowment(monkeypatch, raw):
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_RAW", raw)
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_ENABLED",
                        raw.strip().lower() == "true")


def _confirming(wf_type):
    return {"active": True, "workflow_type": "daf", "type": wf_type,
            "step": "confirm", "submission_data": dict(SUBMISSION),
            "form_id": (Config.DAF_INQUIRY_FORM_ID if wf_type == "daf"
                        else Config.ENDOWMENT_INQUIRY_FORM_ID),
            "profile_id": None, "funit_id": None, "ticket_id": None}


# --- the default and the parsing --------------------------------------------

def test_endowment_is_off_unless_the_environment_says_otherwise():
    assert Config.CSUITE_ENDOWMENT_CREATE_ENABLED is False


@pytest.mark.parametrize("value,expected", [
    ("true", True), ("True", True), (" TRUE ", True),
    ("false", False), ("", False), ("1", False), ("yes", False),
    ("on", False), ("anything", False),
])
def test_only_the_word_true_turns_it_on(monkeypatch, value, expected):
    monkeypatch.setenv("CSUITE_ENDOWMENT_CREATE_ENABLED", value)
    import config
    importlib.reload(config)
    try:
        assert config.Config.CSUITE_ENDOWMENT_CREATE_ENABLED is expected
    finally:
        monkeypatch.delenv("CSUITE_ENDOWMENT_CREATE_ENABLED", raising=False)
        importlib.reload(config)


@pytest.mark.parametrize("value", ["1", "yes", "on", "enabled", "tru"])
def test_a_bad_value_is_off_and_the_reply_says_so(monkeypatch, value):
    _endowment(monkeypatch, value)
    state = daf_workflow.default_workflow_state()

    reply = daf_workflow._initiate_workflow("new endowment", state,
                                            HubSpotNeverCalled())

    assert "turned off" in reply
    assert "not understood" in reply
    assert state["active"] is False


@pytest.mark.parametrize("value", ["", "false", "False"])
def test_an_unset_or_false_value_is_off_without_a_parse_warning(monkeypatch,
                                                                 value):
    _endowment(monkeypatch, value)
    reply = daf_workflow._initiate_workflow(
        "new endowment", daf_workflow.default_workflow_state(),
        HubSpotNeverCalled())

    assert "turned off" in reply
    assert "not understood" not in reply


# --- DAF switches on, endowment off -----------------------------------------

@pytest.mark.parametrize("phrase", ["new endowment",
                                    "process endowment inquiry",
                                    "create endowment",
                                    "endowment inquiry form"])
def test_daf_switches_on_do_not_enable_endowment(monkeypatch, phrase):
    _all_daf_switches_on(monkeypatch)
    _endowment(monkeypatch, "")
    state = daf_workflow.default_workflow_state()

    assert daf_workflow.can_handle(phrase)
    reply = daf_workflow._initiate_workflow(phrase, state,
                                            HubSpotNeverCalled())

    assert "Endowment inquiries are turned off" in reply
    assert "CSUITE_ENDOWMENT_CREATE_ENABLED" in reply
    assert state["active"] is False


def test_a_workflow_opened_before_the_switch_went_off_cannot_create(
        monkeypatch):
    _all_daf_switches_on(monkeypatch)
    _endowment(monkeypatch, "")
    state = _confirming("endowment")

    reply = daf_workflow._step_create("yes", state, HubSpot(),
                                      CSuiteNeverCalled())

    assert "turned off" in reply
    assert state["active"] is False


def test_daf_is_unchanged_with_endowment_off(monkeypatch):
    """The DAF create still runs, exactly as before, with endowment off."""
    _all_daf_switches_on(monkeypatch)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    _endowment(monkeypatch, "")
    csuite = CSuiteCreating()
    state = _confirming("daf")

    reply = daf_workflow._step_create("yes", state, HubSpot(), csuite)

    assert len(csuite.calls) == 1
    assert state["profile_id"] == 31337
    assert "DAF Inquiry — Profile Created" in reply
    assert "turned off" not in reply.split("Profile Created")[0]


def test_switched_on_still_refuses_the_individual_create(monkeypatch):
    """Part 1 alone has no organisation path. On must not fall through to
    create_individual_profile."""
    _all_daf_switches_on(monkeypatch)
    _endowment(monkeypatch, "true")
    state = _confirming("endowment")

    reply = daf_workflow._step_create("yes", state, HubSpot(),
                                      CSuiteNeverCalled())

    assert "not available in this version" in reply
    assert state["active"] is False
