"""The inquiry workflow searches before it creates.

Until 2026-10-01 it did not. It created a CSuite profile, then PATCHed
`csuite_profile_id` over whatever HubSpot already held — having never read it.
So a second inquiry from the same donor made a second CSuite profile and
repointed HubSpot at it, the old id gone and nothing recording what it had
been. CSuite has no idempotency key, so the first profile stays.

Two checks now, because they fail differently: the HubSpot contact's stored id
(cheap, definitive when set, since this workflow put it there) and a trusted
`primary_email` search of CSuite (catches a profile made by hand, by an import,
or by a run whose PATCH failed).

**Ambiguity stops the create.** The cost of stopping is a message; the cost of
continuing is a duplicate donor record in a fund-accounting system.

No network.
"""

import pytest

from config import Config
from intents import daf_workflow
from intents.daf_workflow import (DuplicateProfile, already_in_csuite,
                                  existing_hubspot_link)
from tests.csuite_doubles import (EmptyTable, HasDuplicate, NoDuplicates,
                                  contact)

DATA = {"first_name": "Sarah", "last_name": "Ahmed",
        "email": "sarah.ahmed@example.invalid"}


class HubSpot:
    def __init__(self, result=None, raises=None):
        self.result = result if result is not None else contact()
        self.raises = raises
        self.searches = 0
        self.patched = []

    def search_contact_by_email(self, email, properties=None):
        self.searches += 1
        if self.raises:
            raise self.raises
        return self.result

    def update_contact_by_email(self, email, properties):
        self.patched.append(dict(properties))
        return {"id": "70123"}

    def get_open_tickets(self):
        return {"results": []}


# ---------------------------------------------------------------------------
# existing_hubspot_link
# ---------------------------------------------------------------------------

def test_the_search_now_returns_the_csuite_property():
    from clients.hubspot import HubSpotClient

    assert "csuite_profile_id" in HubSpotClient.CONTACT_SEARCH_PROPERTIES


def test_a_stored_id_is_read():
    cid, existing = existing_hubspot_link(
        DATA["email"], HubSpot(contact("70123", csuite_profile_id="19999")))
    assert (cid, existing) == ("70123", "19999")


@pytest.mark.parametrize("stored", [None, "", "   "])
def test_an_empty_property_reads_as_no_link(stored):
    row = contact("70123")
    if stored is not None:
        row["results"][0]["properties"]["csuite_profile_id"] = stored
    _, existing = existing_hubspot_link(DATA["email"], HubSpot(row))
    assert existing is None


def test_no_contact_is_not_a_link():
    assert existing_hubspot_link(DATA["email"],
                                 HubSpot({"results": []})) == (None, None)


def test_a_failed_lookup_returns_unknown_not_absent():
    """"Unknown" and "absent" lead to opposite decisions."""
    assert existing_hubspot_link(
        DATA["email"], HubSpot(raises=RuntimeError("boom"))) == (None, None)
    assert existing_hubspot_link(
        DATA["email"], HubSpot({"error": "nope"})) == (None, None)


# ---------------------------------------------------------------------------
# already_in_csuite
# ---------------------------------------------------------------------------

def test_a_hubspot_link_stops_the_create_without_touching_csuite():
    """The cheap check first, and it is definitive."""
    class Exploding(NoDuplicates):
        def _request(self, endpoint, data=None):
            raise AssertionError("CSuite must not be searched; HubSpot knew")

    with pytest.raises(DuplicateProfile) as caught:
        already_in_csuite(DATA, HubSpot(contact(csuite_profile_id="19999")),
                          Exploding())
    assert caught.value.profile_id == "19999"
    assert "HubSpot" in caught.value.source


def test_a_csuite_match_stops_the_create():
    with pytest.raises(DuplicateProfile) as caught:
        already_in_csuite(DATA, HubSpot(), HasDuplicate())
    assert caught.value.profile_id == 19999
    assert "primary_email" in caught.value.source


def test_no_match_anywhere_lets_the_create_proceed():
    assert already_in_csuite(DATA, HubSpot(), NoDuplicates()) == "70123"


def test_an_untrustworthy_filter_stops_the_create():
    """An empty table makes every filter look like it works, so filter_trust
    refuses to answer — and a check that cannot answer is not a green light."""
    with pytest.raises(DuplicateProfile) as caught:
        already_in_csuite(DATA, HubSpot(), EmptyTable())
    assert caught.value.profile_id is None
    assert "cannot be ruled out" in caught.value.source


def test_a_broken_csuite_stops_the_create():
    class Broken(NoDuplicates):
        def _request(self, endpoint, data=None):
            raise RuntimeError("CSuite is down")

    with pytest.raises(DuplicateProfile) as caught:
        already_in_csuite(DATA, HubSpot(), Broken())
    assert "cannot be ruled out" in caught.value.source


def test_two_csuite_matches_are_ambiguous_and_stop_the_create():
    class Two(NoDuplicates):
        duplicate_ids = (19999, 20001)

    with pytest.raises(DuplicateProfile) as caught:
        already_in_csuite(DATA, HubSpot(), Two())
    assert caught.value.profile_id is None, "no id may be guessed from two"
    assert "19999" in caught.value.source and "20001" in caught.value.source


def test_no_email_means_no_csuite_search():
    class Exploding(NoDuplicates):
        def _request(self, endpoint, data=None):
            raise AssertionError("nothing to search on")

    assert already_in_csuite({"email": ""}, HubSpot({"results": []}),
                             Exploding()) is None


# ---------------------------------------------------------------------------
# Through the workflow
# ---------------------------------------------------------------------------

class CSuiteSpy(NoDuplicates):
    def __init__(self):
        self.created = []

    def create_individual_profile(self, **kwargs):
        self.created.append(kwargs)
        return {"success": True, "data": {"profile_id": 21690}}


def run(monkeypatch, hubspot, csuite):
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": dict(DATA),
             "profile_id": None, "funit_id": None, "ticket_id": None}
    return daf_workflow._step_create("yes", state, hubspot, csuite), state


def test_an_existing_link_creates_nothing_and_patches_nothing(monkeypatch):
    hubspot = HubSpot(contact(csuite_profile_id="19999"))
    csuite = CSuiteSpy()
    reply, state = run(monkeypatch, hubspot, csuite)

    assert csuite.created == [], "no profile may be created"
    assert hubspot.patched == [], "no PATCH may be sent"
    assert "Already in CSuite — no new profile created" in reply
    assert "19999" in reply
    assert "/profile/display?profile_id=19999" in reply
    assert state["profile_id"] == "19999"


def test_a_csuite_match_creates_nothing_and_says_where_it_was_found(monkeypatch):
    hubspot = HubSpot()
    csuite = HasDuplicate()
    csuite.created = []
    csuite.create_individual_profile = lambda **kw: csuite.created.append(kw)
    reply, _ = run(monkeypatch, hubspot, csuite)

    assert csuite.created == []
    assert hubspot.patched == []
    assert "Already in CSuite" in reply
    assert "19999" in reply
    assert "primary_email" in reply


def test_an_unanswerable_check_creates_nothing_and_explains(monkeypatch):
    hubspot = HubSpot()
    reply, _ = run(monkeypatch, hubspot, EmptyTable())

    assert hubspot.patched == []
    assert "a duplicate could not be ruled out" in reply
    assert "Nothing was created and nothing was changed" in reply
    assert "Already in CSuite" not in reply


def test_a_clean_check_still_creates(monkeypatch):
    hubspot = HubSpot()
    csuite = CSuiteSpy()
    reply, state = run(monkeypatch, hubspot, csuite)

    assert len(csuite.created) == 1
    assert state["profile_id"] == 21690
    assert "Profile Created" in reply


# ---------------------------------------------------------------------------
# The PATCH never overwrites
# ---------------------------------------------------------------------------

def test_the_patch_is_withheld_if_an_id_appeared_mid_run(monkeypatch):
    """The guard ran clean, so a value here means the two disagree — and the
    stored id wins, because it is what staff and the donation sync use."""
    hubspot = HubSpot()
    calls = {"n": 0}

    def search(email, properties=None):
        calls["n"] += 1
        # clean on the pre-create check, set by the time the PATCH is built
        return contact() if calls["n"] == 1 else contact(
            csuite_profile_id="19999")

    hubspot.search_contact_by_email = search
    csuite = CSuiteSpy()
    reply, state = run(monkeypatch, hubspot, csuite)

    assert len(csuite.created) == 1, "the create had already been cleared"
    assert hubspot.patched == [], "the stored id must not be overwritten"
    assert "NOT repointed" in reply
    assert "19999" in reply and "21690" in reply
    assert "merge them in CSuite" in reply


def test_an_empty_stored_value_is_patched_normally(monkeypatch):
    hubspot = HubSpot()
    csuite = CSuiteSpy()
    run(monkeypatch, hubspot, csuite)

    assert hubspot.patched == [{"csuite_profile_id": "21690"}]


# ---------------------------------------------------------------------------
# What task/create's 400 proved
# ---------------------------------------------------------------------------

def test_due_date_is_recorded_as_the_wrong_input_name():
    """2026-10-01: `due_date` was in the payload and CSuite still answered
    HTTP 400 `due_ts: due_ts is required`. So due_date did not satisfy the
    requirement — it is the output name only."""
    from clients.csuite import KNOWN_INVALID_INPUT_FIELDS

    assert "due_date" in KNOWN_INVALID_INPUT_FIELDS
    assert "due_ts" in KNOWN_INVALID_INPUT_FIELDS["due_date"]


def test_name_and_due_ts_graduated_once_a_task_was_created():
    """They were in the holding pen on 2026-10-01 while CSuite had only
    DEMANDED them. Task 1034 was then created with both and read back, so they
    are confirmed — endpoint-scoped, because `name` means something else
    everywhere else."""
    from clients.csuite import (ENDPOINT_CONFIRMED_FIELDS,
                                RECOGNISED_UNCONFIRMED_FIELDS)

    assert RECOGNISED_UNCONFIRMED_FIELDS == frozenset()
    for field in ("name", "due_ts"):
        assert ENDPOINT_CONFIRMED_FIELDS[field] == ("task/create",)


def test_create_task_now_sends_due_ts():
    from clients.csuite import CSuiteClient

    class Client(CSuiteClient):
        def __init__(self):
            self.sent = []

        def _request(self, endpoint, data=None):
            self.sent.append(dict(data or {}))
            return {"success": True, "data": {}}

    client = Client()
    client.create_task("A task", 1006, due_date="2026-10-05")
    payload = client.sent[0]

    assert payload["due_ts"] == "2026-10-05"
    assert "due_date" not in payload
    assert payload["name"] == "A task", "CSuite requires name; it is not dead"


def test_name_is_required_by_csuite_so_it_stays_a_required_argument():
    """The brief's condition for removing it was a create that SUCCEEDED
    without it. The create was refused FOR it."""
    import inspect

    from clients.csuite import CSuiteClient

    params = inspect.signature(CSuiteClient.create_task).parameters
    assert params["name"].default is inspect.Parameter.empty


# ---------------------------------------------------------------------------
# create_task links a task to a profile
# ---------------------------------------------------------------------------

class TaskClient:
    """Captures the task/create payload. No network."""

    def __init__(self):
        from clients.csuite import CSuiteClient
        self.sent = []
        self._create = CSuiteClient.create_task

    def _request(self, endpoint, data=None):
        self.sent.append((endpoint, dict(data or {})))
        return {"success": True, "data": {"task_id": 1034,
                                          "task_guid": "g"}}

    def create_task(self, *a, **kw):
        return self._create(self, *a, **kw)

    @property
    def payload(self):
        return self.sent[0][1]


def test_a_linked_task_sends_o_and_id():
    """VERIFIED 2026-10-01 on sandbox task 1034."""
    client = TaskClient()
    client.create_task("SENTINEL", 1006, due_date="2026-10-05",
                       description="SENTINEL", linked_profile_id=21661,
                       task_type_id=1065)

    assert client.payload == {
        "name": "SENTINEL", "employee_id": 1006, "due_ts": "2026-10-05",
        "task_description": "SENTINEL", "task_type_id": 1065,
        "o": "profile", "id": 21661}
    assert "due_date" not in client.payload
    assert "profile_id" not in client.payload


def test_an_unlinked_task_sends_neither_o_nor_id():
    client = TaskClient()
    client.create_task("SENTINEL", 1006, due_date="2026-10-05")

    assert "o" not in client.payload
    assert "id" not in client.payload


def test_profile_zero_is_still_a_link():
    """`if linked_profile_id is not None`, not a truthiness test."""
    client = TaskClient()
    client.create_task("SENTINEL", 1006, due_date="2026-10-05",
                       linked_profile_id=0)
    assert client.payload["id"] == 0


def test_a_caller_cannot_pass_profile_id_through_kwargs():
    from clients.csuite import UnconfirmedField

    client = TaskClient()
    with pytest.raises(UnconfirmedField):
        client.create_task("SENTINEL", 1006, due_date="2026-10-05",
                           profile_id=21661)
    assert client.sent == []


def test_the_docstring_warns_that_1006_and_1065_are_sandbox_only():
    """Production has 7 tasks, none Carl's and none carrying a type. That is
    unknown, not absent — the sandbox-17 mistake, written down."""
    from clients.csuite import CSuiteClient

    doc = CSuiteClient.create_task.__doc__
    # Asserted on normalised whitespace: the warnings are wrapped across
    # lines, and a test that breaks on re-wrapping tests the formatting.
    flat = " ".join(doc.split())
    assert "1007 is confirmed in production" in flat
    assert "1006 is NOT" in flat
    assert "no production task carries any type at all" in flat
