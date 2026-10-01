"""A sandbox run cannot write to production HubSpot.

There is ONE HubSpot portal and TWO CSuite environments, and nothing in this
codebase knew that until 2026-10-01 — when an authorised PATCH wrote **sandbox**
profile 21663 onto a **live** HubSpot contact, taking production's count of
broken `csuite_profile_id` links from 68 to 69. The write did exactly what it was
told.

So the refusal is **structural**: `CSUITE_ENV` decides it, at the transport seam
every HubSpot write funnels through. Not a feature flag — a flag can be set by
whoever wants the write, and the thing being prevented is a write somebody
wanted. Not a rule in a brief either, for the same reason: the 69th link was
created under a brief that forbade everything except that one PATCH.

No network.
"""

import pytest

from clients.hubspot import (HubSpotClient, csuite_env, hubspot_writes_allowed,
                             is_hubspot_write)
from config import Config
from intents import daf_workflow
from tests.csuite_doubles import NoDuplicates, StaleLink, contact


def client(monkeypatch, env):
    monkeypatch.setattr(Config, "CSUITE_ENV", env)
    monkeypatch.setattr("clients.hubspot.reserve_write",
                        lambda *a, **kw: {"id": 1})
    monkeypatch.setattr("clients.hubspot.complete_write", lambda *a, **kw: None)
    monkeypatch.setattr("clients.hubspot.record_write", lambda *a, **kw: True)

    hs = HubSpotClient()
    hs.access_token = "token"
    sent = []

    import requests as real_requests

    class Requests:
        # The real exception namespace: the client catches
        # requests.exceptions.RequestException, and shadowing that would turn a
        # deliberate AssertionError into an AttributeError.
        exceptions = real_requests.exceptions

        def __getattr__(self, name):
            def send(url, **kwargs):
                sent.append((name.upper(), url))
                raise AssertionError(
                    f"a {name.upper()} left the process: {url}")
            return send

    monkeypatch.setattr("clients.hubspot.requests", Requests())
    return hs, sent


# ---------------------------------------------------------------------------
# STEP 1 — the seam refuses every write
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("env", ["sandbox", "SANDBOX", " sandbox ", "", "staging",
                                 None])
def test_anything_but_live_refuses_writes(monkeypatch, env):
    monkeypatch.setattr(Config, "CSUITE_ENV", env)
    assert hubspot_writes_allowed() is False


def test_live_allows_writes(monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_ENV", "live")
    assert hubspot_writes_allowed() is True
    assert csuite_env() == "live"


@pytest.mark.parametrize("method,endpoint,data", [
    ("PATCH", "crm/v3/objects/contacts/701", {"properties": {"x": "1"}}),
    ("POST", "crm/v3/objects/contacts", {"properties": {"email": "a@b.org"}}),
    ("POST", "crm/v3/objects/notes", {"properties": {"hs_note_body": "x"}}),
    ("PUT", "crm/v3/objects/contacts/701", {}),
    ("DELETE", "crm/v3/objects/contacts/701", None),
])
def test_every_write_method_is_refused_in_sandbox(monkeypatch, method,
                                                  endpoint, data):
    hs, sent = client(monkeypatch, "sandbox")
    result, status = hs._send_with_status(method, endpoint, data)

    assert sent == [], "nothing may leave the process"
    assert result["refused"] == "sandbox_run"
    assert "one HubSpot portal, two CSuite environments" in result["error"]
    assert "Nothing was sent" in result["error"]
    assert status is None


@pytest.mark.parametrize("endpoint", [
    "crm/v3/objects/contacts/search", "crm/v3/objects/contacts/batch/read",
])
def test_read_shaped_posts_are_NOT_refused(monkeypatch, endpoint):
    """A sandbox run still has to be able to look things up."""
    hs, sent = client(monkeypatch, "sandbox")
    assert is_hubspot_write("POST", endpoint) is False

    with pytest.raises(AssertionError, match="left the process"):
        hs._send_with_status("POST", endpoint, {"filterGroups": []})
    assert sent, "the read was attempted, which is correct"


def test_the_refusal_is_recorded_as_skipped_not_failed(monkeypatch):
    rows = []
    monkeypatch.setattr(Config, "CSUITE_ENV", "sandbox")
    monkeypatch.setattr("clients.hubspot.record_write",
                        lambda *a, **kw: rows.append(kw) or True)
    hs = HubSpotClient()
    hs.access_token = "token"
    hs._send_with_status("PATCH", "crm/v3/objects/contacts/701",
                         {"properties": {"x": "1"}})

    assert len(rows) == 1
    assert rows[0]["status"] == "skipped"
    assert "refused" in rows[0]["error"] or "refused" in str(rows[0])


def test_it_is_not_a_flag(monkeypatch):
    """A flag can be set by whoever wants the write, and the thing being
    prevented is a write somebody wanted."""
    import clients.hubspot as mod

    assert not hasattr(Config, "HUBSPOT_WRITES_ENABLED")
    assert not hasattr(Config, "ALLOW_HUBSPOT_WRITES")
    source = __import__("inspect").getsource(mod.hubspot_writes_allowed)
    assert "CSUITE_ENV" in source or "csuite_env" in source


# ---------------------------------------------------------------------------
# Through the workflow
# ---------------------------------------------------------------------------

class CSuite(NoDuplicates):
    base_url = "https://amuslimcf-sandbox.fcsuite.com/api/v2"

    def __init__(self, inner=None):
        self._inner = inner
        self.created = []

    def _request(self, endpoint, data=None):
        if self._inner is not None:
            return self._inner._request(endpoint, data)
        return super()._request(endpoint, data)

    def create_individual_profile(self, **kwargs):
        self.created.append(kwargs)
        return {"success": True, "data": {"profile_id": 31500}}


class HubSpot:
    """Returns the real refusal shape when writes are not allowed."""

    def __init__(self, row=None):
        self.row = row if row is not None else contact("70700")
        self.attempted = []

    def search_contact_by_email(self, email, properties=None):
        return self.row

    def _refuse_or_record(self, properties):
        self.attempted.append(dict(properties))
        if not hubspot_writes_allowed():
            return {"error": "refused", "refused": "sandbox_run"}
        return {"id": "70700"}

    def update_contact_by_email(self, email, properties):
        return self._refuse_or_record(properties)

    def create_contact(self, properties):
        return self._refuse_or_record(properties)

    def get_open_tickets(self):
        return {"results": []}


def run(monkeypatch, env, csuite=None, hubspot=None, backfill=True):
    monkeypatch.setattr(Config, "CSUITE_ENV", env)
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_HUBSPOT_BACKFILL_ENABLED", backfill)
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": {"first_name": "S", "last_name": "A",
                                 "email": "s@example.invalid"},
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, hubspot or HubSpot(),
                                      csuite or CSuite())
    return reply, state


def test_the_confirmation_says_sandbox_run(monkeypatch):
    hubspot = HubSpot()
    reply, state = run(monkeypatch, "sandbox", hubspot=hubspot)

    assert "🎯 HubSpot: not updated (sandbox run)" in reply
    assert "Could not update" not in reply, "refused is not failed"
    assert state["profile_id"] == 31500, "the CSuite side still works"
    assert "**Issues:**" not in reply, "nothing went wrong"


def test_the_backfill_path_says_sandbox_run_too(monkeypatch):
    class Dup(CSuite):
        duplicate_ids = (19999,)
        live_profile_ids = (19999,)

    hubspot = HubSpot(contact("70700"))
    reply, _ = run(monkeypatch, "sandbox", csuite=Dup(), hubspot=hubspot)

    assert "Already in CSuite" in reply
    assert "🔗 HubSpot not updated (sandbox run)" in reply
    assert "now linked to" not in reply


# ---------------------------------------------------------------------------
# STEP 2 — in sandbox the stored id is advisory
# ---------------------------------------------------------------------------

def test_a_production_id_in_hubspot_gives_NO_stale_warning_in_sandbox(
        monkeypatch, caplog):
    """HubSpot's ids are PRODUCTION ids. Checking one against sandbox CSuite
    compares two unrelated numbering spaces, so every such check would report
    "stale" and be wrong about it."""
    import logging

    hubspot = HubSpot(contact("70700", csuite_profile_id="8443"))
    with caplog.at_level(logging.INFO, logger="intents.daf_workflow"):
        reply, state = run(monkeypatch, "sandbox", csuite=CSuite(StaleLink()),
                           hubspot=hubspot)

    assert "stale" not in reply.lower()
    assert "doesn't exist in CSuite" not in reply
    assert "needs a human" not in reply
    assert state["profile_id"] == 31500, "the email search decided, and it was clear"
    assert any("is a PRODUCTION id and is not checked" in r.getMessage()
               for r in caplog.records), "it is logged, not acted on"


def test_in_sandbox_only_the_email_search_decides(monkeypatch):
    """A stored id that would have stopped the create does not."""
    class CleanSearch(CSuite):
        live_profile_ids = ()        # nothing exists, so a check would say stale
        duplicate_ids = ()           # and the email search finds nothing

    hubspot = HubSpot(contact("70700", csuite_profile_id="8443"))
    reply, state = run(monkeypatch, "sandbox", csuite=CleanSearch(),
                       hubspot=hubspot)

    assert state["profile_id"] == 31500, "created, because the search was clear"
    assert "Already in CSuite" not in reply


def test_in_sandbox_the_email_search_can_still_stop_the_create(monkeypatch):
    class Match(CSuite):
        duplicate_ids = (21663,)
        live_profile_ids = (21663,)

    csuite = Match()
    hubspot = HubSpot(contact("70700", csuite_profile_id="8443"))
    reply, state = run(monkeypatch, "sandbox", csuite=csuite, hubspot=hubspot)

    assert csuite.created == []
    assert state["profile_id"] == 21663
    assert "CSuite primary_email search" in reply


# ---------------------------------------------------------------------------
# STEP 3 — production is unchanged
# ---------------------------------------------------------------------------

def test_in_production_the_patch_happens(monkeypatch):
    hubspot = HubSpot()
    reply, state = run(monkeypatch, "live", hubspot=hubspot)

    assert hubspot.attempted == [{"csuite_profile_id": "31500"}]
    assert "🎯 HubSpot contact updated" in reply
    assert "sandbox run" not in reply


def test_in_production_a_stale_id_is_still_caught(monkeypatch):
    """The whole sandbox-24 behaviour, intact."""
    hubspot = HubSpot(contact("70700", csuite_profile_id="99999"))
    reply, state = run(monkeypatch, "live", csuite=CSuite(StaleLink()),
                       hubspot=hubspot)

    assert "HubSpot's CSuite link is broken" in reply
    assert "doesn't exist in CSuite" in reply
    assert "needs a human" in reply
    assert hubspot.attempted == [], "and nothing was overwritten"


def test_in_production_a_conflict_is_still_caught(monkeypatch):
    class Disagrees(CSuite):
        live_profile_ids = (12345, 19999)
        duplicate_ids = (19999,)

    hubspot = HubSpot(contact("70700", csuite_profile_id="12345"))
    reply, _ = run(monkeypatch, "live", csuite=Disagrees(), hubspot=hubspot)

    assert "Two CSuite profiles" in reply
    assert "HubSpot points at 12345, CSuite email match is 19999" in reply
