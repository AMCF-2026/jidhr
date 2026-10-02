"""A client call on the inquiry path never turns a failure into an empty result.

sandbox-30 made the workflow's own handlers honest. It could not fix what never
reaches a handler: a client that catches its own failure and answers with the
same value it uses for "nothing found". Three on the go-live path did.

* **hubspot.get_contact_tickets** returned `[]` for a failed association read,
  a failed batch read, AND a contact with no tickets. Its docstring said so and
  called it safe, which it is for donor_prep — no "Open Items" section. For this
  workflow `[]` is a sentence: *"📋 No matching ticket — nothing was closed."*
  So the lookup raises TicketLookupFailed now and the caller chooses.
* **csuite._verify_write** set `verified = None` for "could not check" and told
  nobody. The workflow reads `nothing_stored` and `fields_dropped` and never
  `verified`, so a profile create whose read-back failed printed a clean
  `✅ Profile Created` — the same reply as a create proven to hold the donor's
  email, phone and address.
* **csuite.create_task / create_fund** caught ReadBackUnavailable and
  FieldDropped and nothing else, so any other read-back error left the method.
  The workflow's task handler caught it and said `Follow-up task NOT created`
  about a task that exists in CSuite — so nobody looks for it, and the next run
  makes a second one.

No network.
"""

import pytest

from clients.csuite import CSuiteClient
from clients.hubspot import HubSpotClient, TicketLookupFailed
from config import Config
from intents import daf_workflow
from intents.daf_workflow import open_inquiry_tickets
from sync.readback import ReadBackUnavailable
from sync.sandbox_writes import WriteBudget
from tests.csuite_doubles import NoDuplicates, contact

DAF = Config.TICKET_PIPELINES["daf"]
EMAIL = "distinct@example.invalid"


# ---------------------------------------------------------------------------
# 1. The ticket lookup
# ---------------------------------------------------------------------------

TICKETS = {
    "301": {"id": "301", "properties": {
        "subject": "DAF Form Submission - S A", "content": "",
        "hs_pipeline": DAF["pipeline"], "hs_pipeline_stage": DAF["new_stage"]}},
}


class TicketHubSpot:
    """A HubSpot client stubbed at the HTTP seam, not the method seam."""

    def __init__(self, associations=None, assoc_error=None, batch_error=None,
                 pages=None):
        self.client = HubSpotClient.__new__(HubSpotClient)
        self.client.access_token = "test-token"
        self.client.base_url = "https://api.example.invalid"
        self.client.headers = {}
        self.associations = ([{"toObjectId": 301}] if associations is None
                             else associations)
        self.assoc_error = assoc_error
        self.batch_error = batch_error
        self.pages = pages or {}
        self.posts = []
        self.client._get = self._get
        self.client._post = self._post

    def _get(self, endpoint, params=None):
        if self.assoc_error:
            return {"error": self.assoc_error}
        page = self.pages.get((params or {}).get("after") or "first")
        if page is not None:
            return page
        return {"results": list(self.associations)}

    def _post(self, endpoint, data=None):
        self.posts.append(data)
        if self.batch_error:
            return {"error": self.batch_error}
        wanted = [i["id"] for i in data["inputs"]]
        return {"results": [TICKETS[i] for i in wanted if i in TICKETS]}


def test_a_failed_association_read_raises_when_the_caller_asks():
    hub = TicketHubSpot(assoc_error="HubSpot returned 500")

    with pytest.raises(TicketLookupFailed) as caught:
        hub.client.get_contact_tickets("701", raise_on_failure=True)

    assert "701" in str(caught.value)
    assert hub.posts == [], "no batch read after the associations failed"


def test_a_failed_batch_read_raises_rather_than_returning_a_short_list():
    """The donor's ticket can be in the page that failed, and a short list is
    indistinguishable from a complete one."""
    hub = TicketHubSpot(batch_error="HubSpot returned 500")

    with pytest.raises(TicketLookupFailed):
        hub.client.get_contact_tickets("701", raise_on_failure=True)


def test_a_contact_with_genuinely_no_tickets_is_still_an_empty_list():
    """The point is to separate the two, not to make absence an error."""
    hub = TicketHubSpot(associations=[])

    assert hub.client.get_contact_tickets("701", raise_on_failure=True) == []


def test_donor_preps_reading_is_unchanged_by_default():
    """No Open Items is the cautious reading there, and that caller is not
    being changed. The default must behave exactly as it did."""
    assert TicketHubSpot(assoc_error="500").client.get_contact_tickets(
        "701") == []
    assert TicketHubSpot(batch_error="500").client.get_contact_tickets(
        "701") == []
    assert TicketHubSpot(associations=[]).client.get_contact_tickets(
        "701") == []


def test_open_inquiry_tickets_asks_for_the_raise():
    """If it ever stops asking, the failure is silent again."""
    asked = {}

    class HS:
        def get_contact_tickets(self, contact_id, properties=None,
                                raise_on_failure=False):
            asked["raise_on_failure"] = raise_on_failure
            return []

    open_inquiry_tickets(HS(), "70123", "daf")
    assert asked["raise_on_failure"] is True


def test_a_failed_lookup_reaches_the_workflow_as_an_error_line(monkeypatch):
    """End to end: the client's raise becomes the ticket step's ERROR line,
    not the line a clean run prints."""
    hub = TicketHubSpot(assoc_error="HubSpot returned 500")
    reply, _ = run(monkeypatch, hubspot=WorkflowHubSpot(client=hub.client))

    assert "⚠️ Ticket: ERROR — not closed, see log" in reply
    assert "No matching ticket" not in reply


# ---------------------------------------------------------------------------
# 2. The profile create's read-back
# ---------------------------------------------------------------------------

class ProfileClient(CSuiteClient):
    """A client whose only real part is the read-back. _verify_write is called
    from inside _request, so the fake has to sit under it, not over it."""

    def __init__(self, display=None, display_raises=None):
        self.sent = []
        self.verify_writes = True
        self.write_budget = WriteBudget(10)
        self._display = display
        self._display_raises = display_raises

    def _request(self, endpoint, data=None):
        self.sent.append((endpoint, dict(data or {})))
        if self._display_raises:
            raise self._display_raises
        if self._display is None:
            return {"success": False, "error": "CSuite returned 503"}
        return {"success": True, "data": self._display}


SENT = {"first_name": "S", "last_name": "A", "email": EMAIL}


def stored(**overrides):
    base = {"profile_id": 21900, "first_name": "S", "last_name": "A",
            "primary_email": EMAIL}
    base.update(overrides)
    return base


def verify(client, data=None):
    """create_individual_profile's 200 response, then the read-back."""
    return client._verify_write(
        "profile/create/individual", SENT,
        {"success": True, "data": data if data is not None
         else {"profile_id": 21900}, "http_status": 200})


def test_an_unreadable_profile_says_so_on_the_response():
    result = verify(ProfileClient())

    assert result["success"] is True, "the profile exists; this is not a failure"
    assert result["verified"] is None
    assert "could not be read back" in result["verify_warning"]
    assert "21900" in result["verify_warning"]


def test_a_read_back_that_explodes_says_so_too():
    result = verify(ProfileClient(display_raises=RuntimeError("exploded")))

    assert result["success"] is True
    assert result["verified"] is None
    assert result.get("verify_warning")


def test_a_create_with_no_profile_id_back_says_so():
    result = verify(ProfileClient(display=stored()), data={})

    assert result["verified"] is None
    assert "returned no profile_id" in result["verify_warning"]


def test_a_verified_profile_carries_no_warning():
    """The warning has to mean something, so it cannot be always on."""
    result = verify(ProfileClient(display=stored()))

    assert result["verified"] is True
    assert "verify_warning" not in result


def test_a_dropped_field_is_still_dropped_not_merely_unverified():
    """The new warning must not have displaced the stronger finding."""
    result = verify(ProfileClient(display=stored(primary_email=None)))

    assert result["verified"] is False
    assert "email" in result["fields_dropped"]
    assert "verify_warning" not in result


def test_the_unverified_warning_reaches_the_reply(monkeypatch):
    class CSuite(NoDuplicates):
        def create_individual_profile(self, **kwargs):
            return {"success": True, "data": {"profile_id": 21900},
                    "verified": None,
                    "verify_warning": "⚠️ profile_id 21900 was written but "
                                      "could not be read back"}

    reply, _ = run(monkeypatch, csuite=CSuite(), ticket=False)

    assert "could not be read back" in reply
    assert "(with warnings)" in reply, \
        "a create nobody could verify is not a clean success"


# ---------------------------------------------------------------------------
# 3. The task and fund read-backs
# ---------------------------------------------------------------------------

class TaskClient(CSuiteClient):
    """Real create_task, fake transport."""

    def __init__(self, display_raises=None):
        self.verify_writes = True
        self.write_budget = WriteBudget(10)
        self._display_raises = display_raises

    def _request(self, endpoint, data=None):
        if endpoint == "task/display":
            raise self._display_raises
        return {"success": True, "data": {"task_id": 1099},
                "http_status": 200}


def test_a_task_read_back_that_explodes_is_not_a_failed_create():
    client = TaskClient(display_raises=RuntimeError("display exploded"))

    result = client.create_task("Follow up", 1004, due_date="2026-10-05",
                                linked_profile_id=21900)

    assert result["success"] is True, "the task exists"
    assert result["verified"] is None
    assert "was created but the read-back failed" in result["task_warning"]
    assert "1099" in result["task_warning"]


def test_a_task_read_back_error_does_not_escape_as_an_exception():
    """It used to, and the workflow then said "Follow-up task NOT created"
    about a task CSuite had already made."""
    client = TaskClient(display_raises=TypeError("unexpected record shape"))

    result = client.create_task("Follow up", 1004)

    assert result["success"] is True
    assert result["verified"] is None


def test_the_task_read_back_still_distinguishes_unavailable_from_dropped():
    """The new broad handler must not have swallowed the two specific ones."""
    client = TaskClient(display_raises=ReadBackUnavailable("no task came back"))

    result = client.create_task("Follow up", 1004)

    assert result["verified"] is None
    assert "could not be read back" in result["task_warning"]


class FundClient(CSuiteClient):
    def __init__(self, display_raises=None):
        self.verify_writes = True
        self.write_budget = WriteBudget(10)
        self._display_raises = display_raises

    def _request(self, endpoint, data=None):
        if endpoint == "funit/display":
            raise self._display_raises
        return {"success": True, "data": {"funit_id": 1599},
                "http_status": 200}


def test_a_fund_read_back_that_explodes_is_not_a_failed_create():
    """Fund creation is deferred and the flag is off, but the method is wired
    into this workflow and had the same gap."""
    result = FundClient(
        display_raises=RuntimeError("display exploded")
    ).create_fund("SENTINEL Fund", 1002, cash_account_id=1069)

    assert result["success"] is True
    assert result["verified"] is None
    assert "the read-back failed" in result["fund_warning"]


# ---------------------------------------------------------------------------
# 4. The duplicate guard does not create on a failed search
# ---------------------------------------------------------------------------

class SearchFails(NoDuplicates):
    """CSuite answers the duplicate search with an error."""

    base_url = "https://amuslimcf.fcsuite.com/api/v2"

    def __init__(self):
        self.created = []

    def _request(self, endpoint, data=None):
        if endpoint == "profile/list":
            return {"success": False, "error": "CSuite returned 503",
                    "http_status": 503}
        return {"success": True, "data": {}}

    def create_individual_profile(self, **kwargs):
        self.created.append(kwargs)
        return {"success": True, "data": {"profile_id": 21901}}


def test_the_guard_does_not_create_when_the_csuite_search_fails(monkeypatch):
    csuite = SearchFails()
    reply, _ = run(monkeypatch, csuite=csuite, ticket=False)

    assert csuite.created == [], "a duplicate could not be ruled out"
    assert "🛑" in reply
    assert "Profile Created" not in reply


def test_the_guard_does_not_create_when_the_hubspot_read_fails(monkeypatch):
    """sandbox-30's fix, re-pinned from the client side."""
    class HubSpot(WorkflowHubSpot):
        def search_contact_by_email(self, email, properties=None):
            return {"error": "HubSpot returned 500"}

    csuite = SearchFails()
    reply, _ = run(monkeypatch, csuite=csuite, hubspot=HubSpot(),
                   ticket=False)

    assert csuite.created == []
    assert "🛑" in reply


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------

class WorkflowHubSpot:
    """Method-seam double, optionally delegating tickets to a real client."""

    def __init__(self, client=None):
        self._client = client
        self.closed = []

    def search_contact_by_email(self, email, properties=None):
        return contact("70123")

    def update_contact_by_email(self, email, properties):
        return {"id": "70123"}

    def get_contact_tickets(self, contact_id, properties=None,
                            raise_on_failure=False):
        if self._client is None:
            return []
        return self._client.get_contact_tickets(
            contact_id, properties=properties,
            raise_on_failure=raise_on_failure)

    def close_ticket(self, ticket_id):
        self.closed.append(ticket_id)
        return {"id": ticket_id}


class PlainCSuite(NoDuplicates):
    base_url = "https://amuslimcf.fcsuite.com/api/v2"

    def create_individual_profile(self, **kwargs):
        return {"success": True, "data": {"profile_id": 21900},
                "verified": True}


def run(monkeypatch, csuite=None, hubspot=None, ticket=True):
    monkeypatch.setattr("config.Config.CSUITE_ENV", "live")
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_HUBSPOT_BACKFILL_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_TICKET_CLOSE_ENABLED", ticket)
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": {"first_name": "S", "last_name": "A",
                                 "email": EMAIL},
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create(
        "yes", state, hubspot if hubspot is not None else WorkflowHubSpot(),
        csuite if csuite is not None else PlainCSuite())
    return reply, state
