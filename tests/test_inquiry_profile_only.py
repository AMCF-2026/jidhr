"""An inquiry creates a profile. A fund waits for the donor to commit.

Decision 2026-10-01. An enquiry is a conversation; a fund in the ledger for a
conversation that went nowhere is a finance record somebody has to explain.
The funit/create path and its read-back are kept intact behind
CSUITE_DAF_FUND_CREATE_ENABLED for the commitment stage.

Also here: what happens to a submission the write budget refuses. Before
2026-10-01 the answer was "nothing" — no audit row (correctly, since nothing
was sent), no other record, and `_initiate_workflow` only ever reads
`submissions[0]`, so one newer submission put the refused one out of reach
with nothing anywhere saying it had been seen.

No network.
"""

import logging

import pytest

from clients.audit import AuditUnavailable
from clients.csuite import CSuiteClient
from config import Config
from intents import daf_workflow
from intents.daf_workflow import _parse_submission
from sync.sandbox_writes import WriteBudget


def submission(**fields):
    base = {"firstname": "HUBSYNC", "lastname": "SENTINEL",
            "email": "s@example.invalid", "phone": "(703) 555-0199",
            "address": "91 Test Way", "city": "Reston", "state": "VA",
            "zip": "20190"}
    base.update(fields)
    return {"submittedAt": 1759300000000, "conversionId": "conv-abc-123",
            "values": [{"name": k, "value": v} for k, v in base.items() if v]}


class CSuiteSpy:
    def __init__(self, fail=None):
        self.calls = []
        self.fail = fail

    def create_individual_profile(self, **kwargs):
        self.calls.append(("create_individual_profile", kwargs))
        if self.fail:
            raise self.fail
        return {"success": True, "data": {"profile_id": 21670}}

    def create_fund(self, **kwargs):
        self.calls.append(("create_fund", kwargs))
        return {"success": True, "data": {"funit_id": 1570}}

    @property
    def endpoints(self):
        return [name for name, _ in self.calls]


class HubSpot:
    def __init__(self, found=True):
        self.found = found
        self.patched = []

    def search_contact_by_email(self, email):
        return {"results": [{"id": "70123"}]} if self.found else {"results": []}

    def update_contact_by_email(self, email, properties):
        self.patched.append(dict(properties))
        if not self.found:
            return {"error": "Contact not found: x"}
        return {"id": "70123"}

    def create_contact(self, properties):
        self.patched.append(dict(properties))
        return {"id": "70199"}

    def get_open_tickets(self):
        return {"results": []}


def run(monkeypatch, fund=False, csuite=None, hubspot=None, sub=None,
        recorded=None):
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", fund)
    if recorded is not None:
        monkeypatch.setattr(daf_workflow, "record_write",
                            recorded if callable(recorded)
                            else (lambda *a, **kw: True))
    parsed = _parse_submission(sub or submission())
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": parsed,
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, hubspot or HubSpot(),
                                      csuite or CSuiteSpy())
    return reply, state


# ---------------------------------------------------------------------------
# The flag
# ---------------------------------------------------------------------------

def test_the_fund_flag_is_off_by_default():
    assert Config.CSUITE_DAF_FUND_CREATE_ENABLED is False


@pytest.mark.parametrize("value,expected", [
    ("true", True), ("True", True), (" TRUE ", True),
    ("false", False), ("", False), ("1", False), ("yes", False), ("on", False),
])
def test_only_the_word_true_enables_the_fund(monkeypatch, value, expected):
    """Same fail-closed parsing as CSUITE_WRITE_BUDGET."""
    import importlib

    import config
    monkeypatch.setenv("CSUITE_DAF_FUND_CREATE_ENABLED", value)
    try:
        importlib.reload(config)
        assert config.Config.CSUITE_DAF_FUND_CREATE_ENABLED is expected
    finally:
        monkeypatch.delenv("CSUITE_DAF_FUND_CREATE_ENABLED", raising=False)
        importlib.reload(config)


def test_the_fund_code_is_still_there_for_the_commitment_stage():
    """Gated, not deleted."""
    from sync.readback import verify_fund

    assert callable(CSuiteClient.create_fund)
    assert callable(verify_fund)


# ---------------------------------------------------------------------------
# Off: one CSuite write per inquiry
# ---------------------------------------------------------------------------

def test_an_inquiry_makes_exactly_one_csuite_call(monkeypatch):
    csuite = CSuiteSpy()
    run(monkeypatch, fund=False, csuite=csuite)

    assert csuite.endpoints == ["create_individual_profile"]


def test_no_fund_payload_is_even_built(monkeypatch):
    """Not just unsent — not constructed. A spy that raises on create_fund
    would catch a call; this catches a call that was prepared and dropped."""
    csuite = CSuiteSpy()
    _, state = run(monkeypatch, fund=False, csuite=csuite)

    assert state["funit_id"] is None
    assert "create_fund" not in csuite.endpoints


def test_the_reply_never_says_the_fund_failed(monkeypatch):
    reply, _ = run(monkeypatch, fund=False)

    assert "Fund: Failed" not in reply
    assert "❌ Fund" not in reply
    assert "not opened yet" in reply
    assert "when the donor commits" in reply


def test_the_heading_does_not_claim_a_daf_was_created(monkeypatch):
    """A profile is not a DAF. No fund exists."""
    reply, _ = run(monkeypatch, fund=False)

    assert "Inquiry — Profile Created" in reply
    assert "DAF Created!" not in reply


def test_a_real_write_budget_of_one_is_enough_for_an_inquiry(monkeypatch):
    """The arming instruction is CSUITE_WRITE_BUDGET=1, so one must suffice."""
    budget = WriteBudget(1)
    budget.spend("profile/create/individual")
    assert budget.remaining == 0, "an inquiry needs exactly one write"


# ---------------------------------------------------------------------------
# HubSpot: csuite_fund_id is omitted, never nulled
# ---------------------------------------------------------------------------

def test_the_patch_omits_csuite_fund_id_entirely(monkeypatch):
    """HubSpot treats an explicit empty value as "clear this property", so a
    null here would wipe a fund id a later commitment-stage run had set."""
    hubspot = HubSpot()
    run(monkeypatch, fund=False, hubspot=hubspot)

    assert len(hubspot.patched) == 1
    props = hubspot.patched[0]
    assert props["csuite_profile_id"] == "21670"
    assert "csuite_fund_id" not in props
    assert None not in props.values()
    assert "" not in props.values()


def test_a_new_contact_is_created_without_a_fund_id_either(monkeypatch):
    hubspot = HubSpot(found=False)
    run(monkeypatch, fund=False, hubspot=hubspot)

    assert all("csuite_fund_id" not in p for p in hubspot.patched)


# ---------------------------------------------------------------------------
# On: unchanged
# ---------------------------------------------------------------------------

def test_with_the_flag_on_the_fund_is_created_as_before(monkeypatch):
    csuite = CSuiteSpy()
    reply, state = run(monkeypatch, fund=True, csuite=csuite)

    assert csuite.endpoints == ["create_individual_profile", "create_fund"]
    assert state["funit_id"] == 1570
    assert "not opened yet" not in reply
    assert "DAF Created!" in reply


def test_with_the_flag_on_the_patch_carries_both_ids(monkeypatch):
    hubspot = HubSpot()
    run(monkeypatch, fund=True, hubspot=hubspot)

    props = hubspot.patched[0]
    assert props["csuite_profile_id"] == "21670"
    assert props["csuite_fund_id"] == "1570"


# ---------------------------------------------------------------------------
# A refused submission is recoverable
# ---------------------------------------------------------------------------

REFUSED = RuntimeError("this run is capped at 1 CSuite write(s) and has used 1")


def test_the_submission_id_is_parsed_from_the_conversion_id():
    parsed = _parse_submission(submission())
    assert parsed["submission_id"] == "conv-abc-123"


def test_submitted_at_is_the_fallback_identifier():
    sub = submission()
    del sub["conversionId"]
    assert _parse_submission(sub)["submission_id"] == "1759300000000"


def test_a_refused_inquiry_records_the_form_and_submission_id(monkeypatch):
    rows = []
    monkeypatch.setattr(daf_workflow, "record_write",
                        lambda *a, **kw: rows.append((a, kw)) or True)

    reply, _ = run(monkeypatch, fund=False, csuite=CSuiteSpy(fail=REFUSED))

    assert len(rows) == 1
    _, kw = rows[0]
    assert kw["target_id"] == "conv-abc-123"
    assert kw["payload"]["hubspot_form_id"] == Config.DAF_INQUIRY_FORM_ID
    assert kw["payload"]["hubspot_submission_id"] == "conv-abc-123"
    assert kw["status"] == "skipped"
    assert "replayable from HubSpot" in kw["error"]


def test_no_donor_data_is_stored_with_the_replay_record(monkeypatch):
    rows = []
    monkeypatch.setattr(daf_workflow, "record_write",
                        lambda *a, **kw: rows.append((a, kw)) or True)
    run(monkeypatch, fund=False, csuite=CSuiteSpy(fail=REFUSED))

    blob = repr(rows[0])
    for private in ("HUBSYNC", "SENTINEL", "s@example.invalid",
                    "703) 555-0199", "91 Test Way", "Reston", "20190"):
        assert private not in blob, f"{private!r} reached the audit row"


def test_the_user_is_told_the_submission_can_be_re_processed(monkeypatch):
    reply, _ = run(monkeypatch, fund=False, csuite=CSuiteSpy(fail=REFUSED),
                   recorded=True)

    assert "nothing was lost" in reply
    assert "re-processed from HubSpot" in reply


def test_a_failed_recording_is_shouted_about_not_swallowed(monkeypatch):
    def boom(*a, **kw):
        raise AuditUnavailable("no database")

    monkeypatch.setattr(daf_workflow, "record_write", boom)
    reply, _ = run(monkeypatch, fund=False, csuite=CSuiteSpy(fail=REFUSED))

    assert "🚨" in reply
    assert "could NOT be recorded" in reply
    assert "by hand" in reply


def test_a_bookkeeping_failure_does_not_replace_the_real_error(monkeypatch):
    def boom(*a, **kw):
        raise AuditUnavailable("no database")

    monkeypatch.setattr(daf_workflow, "record_write", boom)
    reply, _ = run(monkeypatch, fund=False, csuite=CSuiteSpy(fail=REFUSED))

    assert "capped at 1 CSuite write" in reply, "the cause must still be shown"


def test_the_heading_says_NOT_created_when_nothing_was(monkeypatch):
    """Measured 2026-10-01: a budget-refused inquiry reported
    "⚠️ DAF Created (with warnings)" having created nothing at all."""
    reply, state = run(monkeypatch, fund=False, csuite=CSuiteSpy(fail=REFUSED),
                       recorded=True)

    assert "NOT Created" in reply
    assert "Created (with warnings)" not in reply
    assert "Created!" not in reply
    assert state["profile_id"] is None


def test_a_successful_run_records_nothing_for_replay(monkeypatch):
    rows = []
    monkeypatch.setattr(daf_workflow, "record_write",
                        lambda *a, **kw: rows.append(kw) or True)
    reply, _ = run(monkeypatch, fund=False)

    assert rows == []
    assert "re-processed from HubSpot" not in reply


def test_a_submission_with_no_identifier_at_all_is_logged_loudly(
        monkeypatch, caplog):
    rows = []
    monkeypatch.setattr(daf_workflow, "record_write",
                        lambda *a, **kw: rows.append(kw) or True)
    bare = {"values": [{"name": "email", "value": "a@b.invalid"}]}

    with caplog.at_level(logging.ERROR, logger="intents.daf_workflow"):
        monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
        monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
        parsed = _parse_submission(bare)
        state = {"active": True, "workflow_type": "daf", "type": "daf",
                 "step": "confirm", "form_id": None,
                 "submission_data": parsed, "profile_id": None,
                 "funit_id": None, "ticket_id": None}
        reply = daf_workflow._step_create("yes", state, HubSpot(),
                                          CSuiteSpy(fail=REFUSED))

    assert rows == [], "nothing to record it by"
    assert any("cannot be replayed" in r.getMessage() for r in caplog.records)
    assert "🚨" in reply
