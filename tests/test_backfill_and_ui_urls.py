"""UI links point at the host that holds the record, and a returning donor's
HubSpot link gets filled in.

**The URL bug was not cosmetic.** `Config.CSUITE_UI_BASE_URL` is a hardcoded
production host, so every UI link this repo printed went to production —
including the ones in sandbox confirmations. Measured 2026-10-01 against
production: profile **21662** exists and is an unrelated real ORG, 21626 is an
unrelated real individual, and task 1034 is a real task from 2025-08-25. A
sandbox link opened by staff lands on a different donor's record, and an edit
made there believing it was the sentinel would be real damage done by a
confirmation line.

**The backfill** exists because the guard's two sides disagree in one case: a
profile found by the CSuite `primary_email` search means HubSpot has no link —
and the duplicate path returns before the step-3 PATCH. So the link never heals,
and every repeat inquiry refuses a second profile while leaving HubSpot
unlinked.

No network.
"""

import pytest

from clients.csuite import ui_url
from config import Config
from intents import daf_workflow
from intents.daf_workflow import _parse_submission
from tests.csuite_doubles import HasDuplicate, NoDuplicates, contact

SANDBOX = "https://amuslimcf-sandbox.fcsuite.com/api/v2"
PRODUCTION = "https://amuslimcf.fcsuite.com/api/v2"
EMAIL = "hubsync-sentinel-100@example.com"


# ---------------------------------------------------------------------------
# STEP 1 — the host follows the client
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("template,ids", [
    (Config.CSUITE_PROFILE_URL, {"profile_id": 21663}),
    (Config.CSUITE_TASK_URL, {"task_id": 1035}),
    (Config.CSUITE_FUND_URL, {"funit_id": 1564}),
])
def test_a_sandbox_write_links_to_the_sandbox_host(template, ids):
    url = ui_url(template, SANDBOX, **ids)
    assert "amuslimcf-sandbox.fcsuite.com" in url
    assert "//amuslimcf.fcsuite.com" not in url


@pytest.mark.parametrize("template,ids", [
    (Config.CSUITE_PROFILE_URL, {"profile_id": 21663}),
    (Config.CSUITE_TASK_URL, {"task_id": 1035}),
])
def test_a_production_write_links_to_production(template, ids):
    url = ui_url(template, PRODUCTION, **ids)
    assert "//amuslimcf.fcsuite.com/" in url
    assert "sandbox" not in url


def test_the_path_and_query_are_untouched():
    """The path stays INFERRED — no CSuite UI path has been opened and
    confirmed, and swapping the host does not change that."""
    url = ui_url(Config.CSUITE_PROFILE_URL, SANDBOX, profile_id=21663)
    assert url.endswith("/erp/profile/display?profile_id=21663")


def test_an_unknown_environment_falls_back_rather_than_guessing():
    """A caller that cannot say where it was gets the old behaviour, not a
    silently wrong host."""
    for base in (None, "", "   "):
        assert ui_url(Config.CSUITE_PROFILE_URL, base, profile_id=1) == \
            Config.CSUITE_PROFILE_URL.format(profile_id=1)


# ---------------------------------------------------------------------------
# Through the workflow
# ---------------------------------------------------------------------------

class CSuite(NoDuplicates):
    base_url = SANDBOX

    def __init__(self):
        self.calls = []

    def create_individual_profile(self, **kwargs):
        self.calls.append("profile")
        return {"success": True, "data": {"profile_id": 21663}}

    def create_task(self, **kwargs):
        self.calls.append("task")
        return {"success": True, "data": {"task_id": 1040}, "verified": True}


class DuplicateCSuite(HasDuplicate, CSuite):
    pass


class HubSpot:
    def __init__(self, row=None, patch_result=None, raises=None):
        self.row = row if row is not None else contact()
        self.patch_result = patch_result
        self.raises = raises
        self.patched = []

    def search_contact_by_email(self, email, properties=None):
        return self.row

    def update_contact_by_email(self, email, properties):
        if self.raises:
            raise self.raises
        self.patched.append(dict(properties))
        return (self.patch_result if self.patch_result is not None
                else {"id": "561059265217"})

    def get_open_tickets(self):
        return {"results": []}


def run(monkeypatch, csuite=None, hubspot=None, backfill=False, task=False):
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", task)
    monkeypatch.setattr(Config, "CSUITE_HUBSPOT_BACKFILL_ENABLED", backfill)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID", 1006)
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", None)
    sub = {"submittedAt": "2026-10-01", "values": [
        {"name": "firstname", "value": "HUBSYNC SENTINEL 100"},
        {"name": "lastname", "value": "SANDBOX ONLY"},
        {"name": "email", "value": EMAIL}]}
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": _parse_submission(sub),
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, hubspot or HubSpot(),
                                      csuite or CSuite())
    return reply, state


def test_the_confirmation_links_to_the_sandbox_when_the_client_is_sandbox(
        monkeypatch):
    reply, _ = run(monkeypatch, task=True)

    assert "amuslimcf-sandbox.fcsuite.com/erp/profile/display?profile_id=21663" \
        in reply
    assert "amuslimcf-sandbox.fcsuite.com/erp/task/display?task_id=1040" in reply
    assert "//amuslimcf.fcsuite.com/erp" not in reply


def test_the_confirmation_links_to_production_when_the_client_is_production(
        monkeypatch):
    class Prod(CSuite):
        base_url = PRODUCTION

    reply, _ = run(monkeypatch, csuite=Prod())
    assert "//amuslimcf.fcsuite.com/erp/profile/display?profile_id=21663" in reply
    assert "sandbox" not in reply


def test_the_duplicate_path_links_to_the_right_host_too(monkeypatch):
    reply, _ = run(monkeypatch, csuite=DuplicateCSuite())
    assert "amuslimcf-sandbox.fcsuite.com/erp/profile/display?profile_id=19999" \
        in reply


# ---------------------------------------------------------------------------
# STEP 2 — the backfill
# ---------------------------------------------------------------------------

def test_the_backfill_flag_is_off_by_default():
    assert Config.CSUITE_HUBSPOT_BACKFILL_ENABLED is False


def test_off_means_no_patch_and_a_stated_reason(monkeypatch):
    hubspot = HubSpot()
    reply, _ = run(monkeypatch, csuite=DuplicateCSuite(), hubspot=hubspot,
                   backfill=False)

    assert hubspot.patched == []
    assert "🔗 skipped: HubSpot backfill is turned off" in reply


def test_an_empty_property_is_filled_in(monkeypatch):
    hubspot = HubSpot(contact("561059265217"))
    reply, _ = run(monkeypatch, csuite=DuplicateCSuite(), hubspot=hubspot,
                   backfill=True)

    assert hubspot.patched == [{"csuite_profile_id": "19999"}]
    assert "🔗 HubSpot now linked to profile 19999" in reply


def test_a_non_empty_matching_value_is_left_alone(monkeypatch):
    hubspot = HubSpot(contact("561059265217", csuite_profile_id="19999"))
    reply, _ = run(monkeypatch, csuite=DuplicateCSuite(), hubspot=hubspot,
                   backfill=True)

    assert hubspot.patched == [], "no write when it is already right"
    assert "already linked to 19999" in reply


def test_a_DISAGREEING_value_is_never_overwritten_and_is_warned_about(
        monkeypatch):
    """Two profiles for one donor is a merge decision, not a field update. The
    stored id is what staff and the donation sync have been using.

    Called directly, because this branch is NOT reachable through the workflow —
    see the test below. It is kept because it is correct if the guard's order
    ever changes, and because getting it wrong would mean overwriting a link.
    """
    from intents.daf_workflow import _backfill_hubspot_link

    monkeypatch.setattr(Config, "CSUITE_HUBSPOT_BACKFILL_ENABLED", True)
    hubspot = HubSpot(contact("561059265217", csuite_profile_id="12345"))
    results = {}
    _backfill_hubspot_link({"email": EMAIL}, {}, results, hubspot, 19999)

    assert hubspot.patched == []
    assert results["backfill_conflict"] == ("12345", 19999)
    assert results["backfill"] == (
        "HubSpot points at 12345, CSuite match is 19999 — nothing was changed. "
        "Merge them in CSuite.")


def test_the_guard_short_circuits_so_a_disagreement_is_never_SEEN(monkeypatch):
    """A finding, pinned.

    `already_in_csuite` checks the HubSpot property FIRST and raises on it, so
    when HubSpot holds an id the CSuite primary_email search is never run. The
    workflow therefore stops on HubSpot's id and never learns that CSuite's
    match is a different profile.

    So the conflict branch above cannot fire from here, and more importantly
    **the workflow does not detect that kind of disagreement at all.** Noticing
    it would mean running both checks rather than returning on the first.
    """
    hubspot = HubSpot(contact("561059265217", csuite_profile_id="12345"))
    reply, state = run(monkeypatch, csuite=DuplicateCSuite(), hubspot=hubspot,
                       backfill=True)

    # Stopped on HubSpot's 12345, not on CSuite's 19999.
    assert state["profile_id"] == "12345"
    assert "HubSpot contact csuite_profile_id" in reply
    assert "19999" not in reply, "the CSuite search was never run"
    assert hubspot.patched == []
    assert "already linked to 12345" in reply


def test_no_contact_means_no_write_and_no_contact_is_created(monkeypatch):
    """The step-3 block is deliberately NOT reused: it falls back to
    create_contact, which would make a contact for a donor who already has a
    CSuite profile."""
    class NoContact(HubSpot):
        def create_contact(self, properties):
            raise AssertionError("a contact must never be created here")

    hubspot = NoContact({"results": []})
    reply, _ = run(monkeypatch, csuite=DuplicateCSuite(), hubspot=hubspot,
                   backfill=True)

    assert hubspot.patched == []
    assert "no HubSpot contact for this address" in reply
    assert "link it by hand" in reply


def test_two_matches_write_nothing(monkeypatch):
    class Two(CSuite):
        duplicate_ids = (19999, 20001)

    hubspot = HubSpot()
    reply, _ = run(monkeypatch, csuite=Two(), hubspot=hubspot, backfill=True)

    assert hubspot.patched == []
    assert "🔗 skipped: no single profile to link to" in reply


def test_a_guard_error_writes_nothing(monkeypatch):
    class Broken(CSuite):
        def _request(self, endpoint, data=None):
            raise RuntimeError("CSuite is down")

    hubspot = HubSpot()
    reply, _ = run(monkeypatch, csuite=Broken(), hubspot=hubspot, backfill=True)

    assert hubspot.patched == []
    assert "could not be ruled out" in reply
    assert "🔗 skipped: no single profile to link to" in reply


def test_a_failed_patch_is_reported_not_swallowed(monkeypatch):
    hubspot = HubSpot(patch_result={"error": "insufficient scopes"})
    reply, _ = run(monkeypatch, csuite=DuplicateCSuite(), hubspot=hubspot,
                   backfill=True)

    assert "HubSpot link NOT written (insufficient scopes)" in reply


def test_an_exception_in_the_patch_does_not_sink_the_reply(monkeypatch):
    hubspot = HubSpot(raises=RuntimeError("boom"))
    reply, _ = run(monkeypatch, csuite=DuplicateCSuite(), hubspot=hubspot,
                   backfill=True)

    assert "HubSpot link NOT written (boom)" in reply
    assert "Already in CSuite" in reply


def test_the_create_path_is_untouched_by_the_backfill(monkeypatch):
    """A new donor still goes through the step-3 PATCH, once."""
    hubspot = HubSpot()
    reply, state = run(monkeypatch, hubspot=hubspot, backfill=True)

    assert state["profile_id"] == 21663
    assert hubspot.patched == [{"csuite_profile_id": "21663"}]
    assert "🔗" not in reply, "the backfill line belongs to the duplicate path"


def test_there_is_exactly_one_backfill_line_on_the_duplicate_path(monkeypatch):
    for backfill in (True, False):
        reply, _ = run(monkeypatch, csuite=DuplicateCSuite(),
                       hubspot=HubSpot(contact("561059265217")),
                       backfill=backfill)
        assert len([l for l in reply.splitlines()
                    if l.startswith("🔗") or l.startswith("⚠️ HubSpot points")]) == 1
