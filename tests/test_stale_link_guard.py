"""A stored csuite_profile_id is a claim, not a fact.

**68 production HubSpot contacts carry ids that do not resolve in CSuite**
(measured 2026-09-30). Until 2026-10-01 the guard took the stored id on faith
and never read CSuite at all, so for every one of those donors:

* no CSuite profile was created — they silently never got one;
* staff were told **"♻️ Already in CSuite"**; and
* a follow-up task was created **linked to a profile that does not exist**.

All three VERIFIED by tracing the old code with fakes before the change.

The id is now read back, with three outcomes, because two of them lead to
opposite decisions: exists, cleanly-missing, and unreadable. Unreadable is not
missing — a 500 says nothing about whether the profile is there, and treating it
as missing would invite a duplicate.

No network.
"""

import pytest

from config import Config
from intents import daf_workflow
from intents.daf_workflow import (PROFILE_EXISTS, PROFILE_MISSING,
                                  PROFILE_UNREADABLE, DuplicateProfile,
                                  already_in_csuite, csuite_profile_state)
from tests.csuite_doubles import (NoDuplicates, StaleLink, StaleLinkWithMatch,
                                  UnreadableProfile, contact)

EMAIL = "stale@example.invalid"
DATA = {"first_name": "Stale", "last_name": "Donor", "email": EMAIL}


class HubSpot:
    def __init__(self, row=None):
        self.row = row if row is not None else contact("70999")
        self.patched = []

    def search_contact_by_email(self, email, properties=None):
        return self.row

    def update_contact_by_email(self, email, properties):
        self.patched.append(dict(properties))
        return {"id": "70999"}

    def get_open_tickets(self):
        return {"results": []}


# ---------------------------------------------------------------------------
# csuite_profile_state — three outcomes, not two
# ---------------------------------------------------------------------------

def test_a_profile_that_reads_back_exists():
    state, record = csuite_profile_state(NoDuplicates(), 21663)
    assert state == PROFILE_EXISTS
    assert record["profile_id"] == 21663


def test_a_clean_not_found_is_missing():
    state, record = csuite_profile_state(StaleLink(), 99999)
    assert state == PROFILE_MISSING
    assert record is None


@pytest.mark.parametrize("response", [
    {"success": False, "error": "Internal server error", "http_status": 500},
    {"success": False, "error": "timed out", "http_status": None},
    {"success": True, "data": None},
    {"success": True, "data": {}},
    "not a dict",
    None,
])
def test_anything_that_is_not_a_clean_not_found_is_UNREADABLE(response):
    """A 500 says nothing about whether the profile is there. Reading it as
    missing would invite creating a duplicate."""
    class Odd:
        def _request(self, endpoint, data=None):
            return response

    state, _ = csuite_profile_state(Odd(), 99999)
    assert state == PROFILE_UNREADABLE


def test_a_raising_client_is_unreadable_not_missing():
    class Broken:
        def _request(self, endpoint, data=None):
            raise RuntimeError("socket closed")

    assert csuite_profile_state(Broken(), 99999)[0] == PROFILE_UNREADABLE


# ---------------------------------------------------------------------------
# (a) the id exists
# ---------------------------------------------------------------------------

def test_an_existing_id_that_agrees_with_the_email_search_stops_as_before():
    class Agrees(NoDuplicates):
        live_profile_ids = (19999,)
        duplicate_ids = (19999,)

    with pytest.raises(DuplicateProfile) as caught:
        already_in_csuite(DATA, HubSpot(contact(csuite_profile_id="19999")),
                          Agrees())
    assert caught.value.kind == "duplicate"
    assert caught.value.profile_id == "19999"


def test_an_existing_id_that_DISAGREES_is_a_conflict():
    class Disagrees(NoDuplicates):
        live_profile_ids = (12345, 19999)
        duplicate_ids = (19999,)

    with pytest.raises(DuplicateProfile) as caught:
        already_in_csuite(DATA, HubSpot(contact(csuite_profile_id="12345")),
                          Disagrees())
    assert caught.value.kind == "conflict"
    assert "HubSpot points at 12345, CSuite email match is 19999" in \
        caught.value.source
    assert "merge by hand" in caught.value.source


def test_an_existing_id_with_no_single_email_match_still_stops():
    """Two email matches, or none, leave nothing to disagree with."""
    class Many(NoDuplicates):
        live_profile_ids = (12345,)
        duplicate_ids = (19999, 20001)

    with pytest.raises(DuplicateProfile) as caught:
        already_in_csuite(DATA, HubSpot(contact(csuite_profile_id="12345")),
                          Many())
    assert caught.value.kind == "duplicate"
    assert caught.value.profile_id == "12345"


# ---------------------------------------------------------------------------
# (b) the id is stale
# ---------------------------------------------------------------------------

def test_a_stale_id_with_one_email_match_stops_on_the_MATCH():
    with pytest.raises(DuplicateProfile) as caught:
        already_in_csuite(DATA, HubSpot(contact(csuite_profile_id="99999")),
                          StaleLinkWithMatch())
    assert caught.value.kind == "stale_with_match"
    assert caught.value.profile_id == 21663, "the real profile, not the stale id"
    assert "stale" in caught.value.source
    assert "CSuite match is 21663" in caught.value.source
    assert "fix HubSpot by hand" in caught.value.source


def test_a_stale_id_with_no_email_match_creates_NOTHING():
    with pytest.raises(DuplicateProfile) as caught:
        already_in_csuite(DATA, HubSpot(contact(csuite_profile_id="99999")),
                          StaleLink())
    assert caught.value.kind == "stale_no_match"
    assert caught.value.profile_id is None, "nothing may be linked to"
    assert "which doesn't exist in CSuite" in caught.value.source
    assert "needs a human" in caught.value.source


# ---------------------------------------------------------------------------
# (c) unreadable
# ---------------------------------------------------------------------------

def test_an_unreadable_id_stops_without_creating_anything():
    with pytest.raises(DuplicateProfile) as caught:
        already_in_csuite(DATA, HubSpot(contact(csuite_profile_id="99999")),
                          UnreadableProfile())
    assert caught.value.kind == "unverifiable"
    assert caught.value.profile_id is None
    assert "would not say whether profile 99999 exists" in caught.value.source


# ---------------------------------------------------------------------------
# Through the workflow
# ---------------------------------------------------------------------------

class Creating:
    base_url = "https://amuslimcf-sandbox.fcsuite.com/api/v2"

    def __init__(self, inner):
        self._inner = inner
        self.created = []
        self.tasks = []

    def _request(self, endpoint, data=None):
        return self._inner._request(endpoint, data)

    def create_individual_profile(self, **kwargs):
        self.created.append(kwargs)
        return {"success": True, "data": {"profile_id": 31000}}

    def create_task(self, **kwargs):
        self.tasks.append(kwargs)
        return {"success": True, "data": {"task_id": 2001}, "verified": True}


def run(monkeypatch, inner, hubspot, backfill=True):
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_HUBSPOT_BACKFILL_ENABLED", backfill)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", 1006)
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", None)
    csuite = Creating(inner)
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": dict(DATA),
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, hubspot, csuite)
    return reply, state, csuite


def test_stale_with_no_match_creates_nothing_and_no_task(monkeypatch):
    hubspot = HubSpot(contact("70999", csuite_profile_id="99999"))
    reply, state, csuite = run(monkeypatch, StaleLink(), hubspot)

    assert csuite.created == [], "no profile"
    assert csuite.tasks == [], "no task — there is nothing to link it to"
    assert hubspot.patched == [], "the broken id must not be overwritten"
    assert state["profile_id"] is None
    assert "🛑 **No profile created — HubSpot's CSuite link is broken**" in reply
    assert "⚠️ HubSpot points at profile 99999, which doesn't exist in " \
        "CSuite. No profile created — needs a human." in reply
    assert "Already in CSuite" not in reply, "it is NOT already in CSuite"


def test_stale_with_a_match_links_the_task_to_the_real_profile(monkeypatch):
    hubspot = HubSpot(contact("70999", csuite_profile_id="99999"))
    reply, state, csuite = run(monkeypatch, StaleLinkWithMatch(), hubspot)

    assert csuite.created == []
    assert len(csuite.tasks) == 1
    assert csuite.tasks[0]["linked_profile_id"] == 21663
    assert state["profile_id"] == 21663
    assert hubspot.patched == [], "never overwrite a non-empty value"
    assert "stale" in reply and "CSuite match is 21663" in reply
    assert "no change to HubSpot — the stored id needs a human" in reply
    assert "profile_id=21663" in reply, "the link points at the real profile"


def test_an_unreadable_id_creates_nothing_and_no_task(monkeypatch):
    hubspot = HubSpot(contact("70999", csuite_profile_id="99999"))
    reply, state, csuite = run(monkeypatch, UnreadableProfile(), hubspot)

    assert csuite.created == [] and csuite.tasks == []
    assert hubspot.patched == []
    assert "a duplicate could not be ruled out" in reply
    assert "would not say whether profile 99999 exists" in reply


def test_a_conflict_creates_nothing_and_warns(monkeypatch):
    class Disagrees(NoDuplicates):
        live_profile_ids = (12345, 19999)
        duplicate_ids = (19999,)

    hubspot = HubSpot(contact("70999", csuite_profile_id="12345"))
    reply, state, csuite = run(monkeypatch, Disagrees(), hubspot)

    assert csuite.created == []
    assert hubspot.patched == []
    assert "Two CSuite profiles" in reply
    assert "HubSpot points at 12345, CSuite email match is 19999" in reply


def test_a_clean_new_donor_is_unaffected(monkeypatch):
    hubspot = HubSpot(contact("70999"))      # no stored id
    reply, state, csuite = run(monkeypatch, NoDuplicates(), hubspot)

    assert len(csuite.created) == 1
    assert state["profile_id"] == 31000
    assert "Profile Created" in reply


@pytest.mark.parametrize("inner,label", [
    (StaleLink(), "stale, no match"),
    (StaleLinkWithMatch(), "stale, with match"),
    (UnreadableProfile(), "unreadable"),
])
def test_no_branch_ever_overwrites_a_stored_id(monkeypatch, inner, label):
    """The one invariant across every branch."""
    hubspot = HubSpot(contact("70999", csuite_profile_id="99999"))
    run(monkeypatch, inner, hubspot)
    assert hubspot.patched == [], label
