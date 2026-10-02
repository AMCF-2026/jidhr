"""A CSuite id shown to a person says which system it came from.

Sandbox-23 made the UI links point at the right host, so clicking one lands in
the right place. The NUMBER was the remaining hole: a staff member reading
`Profile 21663` in a sandbox confirmation and typing it into production would be
acting on a different record.

That is not hypothetical. Sandbox-24 found 21663 had already reached the live
HubSpot portal as a broken link, and found production profile 21662 — printed
five times in sandbox reports — is a real unrelated org.

Marked only when it would otherwise mislead: **live output is byte-identical to
before**, and the whole suite passing unchanged after this landed is the evidence
for that.

No network.
"""

import pytest

from clients.csuite import mark_id
from config import Config
from intents import daf_workflow
from tests.csuite_doubles import NoDuplicates, StaleLink, contact


# ---------------------------------------------------------------------------
# mark_id
# ---------------------------------------------------------------------------

def test_live_is_unmarked():
    assert mark_id(21663, "live") == "21663"


@pytest.mark.parametrize("env", ["sandbox", "SANDBOX", " sandbox ", "",
                                 "staging"])
def test_anything_but_live_is_marked(env):
    assert mark_id(21663, env) == "21663 (sandbox)"


def test_passing_None_means_read_the_config_not_not_live(monkeypatch):
    """`None` is the "no opinion" value, so it defers. An explicit "" is a
    value, and an empty CSUITE_ENV is not live."""
    monkeypatch.setattr("config.Config.CSUITE_ENV", "live")
    assert mark_id(21663, None) == "21663"
    assert mark_id(21663, "") == "21663 (sandbox)"


def test_it_reads_the_config_when_no_env_is_passed(monkeypatch):
    monkeypatch.setattr("config.Config.CSUITE_ENV", "sandbox")
    assert mark_id(1035) == "1035 (sandbox)"
    monkeypatch.setattr("config.Config.CSUITE_ENV", "live")
    assert mark_id(1035) == "1035"


@pytest.mark.parametrize("value", [None, ""])
def test_nothing_is_not_marked(value):
    """"None (sandbox)" would read as an id."""
    assert mark_id(value, "sandbox") == str(value)


def test_strings_and_ints_mark_the_same():
    assert mark_id("21663", "sandbox") == mark_id(21663, "sandbox")


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
        return {"success": True, "data": {"profile_id": 21663}}

    def create_task(self, **kwargs):
        return {"success": True, "data": {"task_id": 1035}, "verified": True}


class HubSpot:
    def __init__(self, row=None):
        self.row = row if row is not None else contact("70700")

    def search_contact_by_email(self, email, properties=None):
        return self.row

    def update_contact_by_email(self, email, properties):
        return {"id": "70700"}

    def get_open_tickets(self):
        return {"results": []}


def run(monkeypatch, env, csuite=None, hubspot=None, task=True):
    monkeypatch.setattr("config.Config.CSUITE_ENV", env)
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", task)
    monkeypatch.setattr(Config, "CSUITE_HUBSPOT_BACKFILL_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", 1006)
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", None)
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": {"first_name": "S", "last_name": "A",
                                 "email": "s@example.invalid"},
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, hubspot or HubSpot(),
                                      csuite or CSuite())
    return reply, state


def test_the_task_line_is_marked_in_sandbox(monkeypatch):
    reply, _ = run(monkeypatch, "sandbox")
    assert "📝 Follow-up task 1035 (sandbox) for" in reply


def test_the_task_line_is_clean_in_live(monkeypatch):
    reply, _ = run(monkeypatch, "live")
    assert "📝 Follow-up task 1035 for" in reply
    assert "(sandbox)" not in reply


def test_a_duplicate_profile_line_is_marked(monkeypatch):
    class Dup(CSuite):
        duplicate_ids = (19999,)
        live_profile_ids = (19999,)

    reply, _ = run(monkeypatch, "sandbox", csuite=Dup())
    assert "👤 Profile 19999 (sandbox) — [CSuite](" in reply


def test_the_same_line_is_clean_in_live(monkeypatch):
    class Dup(CSuite):
        duplicate_ids = (19999,)
        live_profile_ids = (19999,)

    reply, _ = run(monkeypatch, "live", csuite=Dup())
    assert "👤 Profile 19999 — [CSuite](" in reply
    assert "(sandbox)" not in reply


def test_a_stale_match_warning_marks_the_match(monkeypatch):
    class Stale(CSuite):
        live_profile_ids = (21663,)
        duplicate_ids = (21663,)

    hubspot = HubSpot(contact("70700", csuite_profile_id="99999"))
    # live, so the stale branch actually runs — in sandbox the stored id is
    # advisory and never checked.
    reply, _ = run(monkeypatch, "live", csuite=Stale(), hubspot=hubspot)
    assert "CSuite match is 21663 — fix HubSpot by hand" in reply
    assert "(sandbox)" not in reply


def test_an_ambiguous_match_marks_every_id(monkeypatch):
    class Two(CSuite):
        duplicate_ids = (19999, 20001)
        live_profile_ids = (19999, 20001)

    reply, _ = run(monkeypatch, "sandbox", csuite=Two())
    assert "19999 (sandbox)" in reply
    assert "20001 (sandbox)" in reply


def test_the_backfill_line_is_marked(monkeypatch):
    class Dup(CSuite):
        duplicate_ids = (19999,)
        live_profile_ids = (19999,)

    # live, because a sandbox run refuses the HubSpot write entirely
    reply, _ = run(monkeypatch, "live", csuite=Dup(),
                   hubspot=HubSpot(contact("70700")))
    assert "HubSpot now linked to profile 19999" in reply
    assert "(sandbox)" not in reply


def test_a_fund_read_back_warning_is_marked(monkeypatch):
    from clients.csuite import CSuiteClient

    monkeypatch.setattr("config.Config.CSUITE_ENV", "sandbox")

    class Client(CSuiteClient):
        verify_writes = True

        def __init__(self):
            pass

        def _request(self, endpoint, data=None):
            if endpoint == "funit/display":
                return {"success": True, "data": {
                    "funit_id": 1564, "fund_name": "F", "fgroup_id": 1002,
                    "account_id": 9999}}
            return {"success": True, "data": {"funit_id": 1564}}

    result = Client().create_fund("F", 1002, 1069)
    assert "Fund 1564 (sandbox) was created" in result["fund_warning"]


def test_a_task_read_back_warning_is_marked(monkeypatch):
    from clients.csuite import CSuiteClient

    monkeypatch.setattr("config.Config.CSUITE_ENV", "sandbox")

    class Client(CSuiteClient):
        verify_writes = True

        def __init__(self):
            pass

        def _request(self, endpoint, data=None):
            if endpoint == "task/display":
                return {"success": False, "error": "boom"}
            return {"success": True, "data": {"task_id": 1040}}

    result = Client().create_task("T", 1006, due_date="2026-10-05")
    assert "Task 1040 (sandbox) was created" in result["task_warning"]


def test_a_full_sandbox_confirmation_marks_every_id_it_prints(monkeypatch):
    """The whole point: nothing a person could retype is unmarked."""
    import re

    class Dup(CSuite):
        duplicate_ids = (19999,)
        live_profile_ids = (19999,)

    reply, _ = run(monkeypatch, "sandbox", csuite=Dup())

    # Every bare id in the TEXT (not inside a URL) carries the marker.
    text = "\n".join(l for l in reply.splitlines() if "](" not in l)
    for number in re.findall(r"\b(1\d{4}|\d{4})\b", text):
        assert f"{number} (sandbox)" in reply, (number, text)


# ---------------------------------------------------------------------------
# The assignee is a NAME, through the real create_task + read-back
# ---------------------------------------------------------------------------

def test_the_live_line_names_the_assignee_not_a_bare_employee_id(monkeypatch):
    """The sandbox-26 report printed "for 1006". That was a demo artifact — its
    double returned `create_task` directly, so no read-back ran and no name was
    available. Sandbox-21's real run printed "for Dodge, Carl".

    This exercises the REAL create_task, faked only at _request, so the name
    comes from the read-back the way it does in production.
    """
    from clients.csuite import CSuiteClient

    monkeypatch.setattr("config.Config.CSUITE_ENV", "live")
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_HUBSPOT_BACKFILL_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", 1007)
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", None)

    class Real(CSuiteClient):
        """The real create_individual_profile and create_task."""

        base_url = "https://amuslimcf.fcsuite.com/api/v2"
        verify_writes = True

        def __init__(self):
            self.endpoints = []
            self.task_sent = {}

        def _request(self, endpoint, data=None):
            self.endpoints.append(endpoint)
            if endpoint == "profile/list":
                return NoDuplicates()._request(endpoint, data)
            if endpoint == "profile/display":
                return {"success": True, "data": {
                    "profile_id": 21700, "first_name": "S", "last_name": "A",
                    "primary_email": "s@example.invalid"}}
            if endpoint == "task/display":
                # Echoes what was sent, under its display names. Hardcoding a
                # due_date made this test fail the day the clock rolled over —
                # task_due_date is computed from today, so a fixed date in a
                # fake is a date that goes stale.
                from sync.readback import TASK_SENT_TO_STORED
                record = {
                    "task_id": 1050,
                    "assigned_employee": {"employee_id": 1007,
                                          "employee_profile_id": 1039,
                                          "employee_name": "Zouita, Kods"}}
                for field, target in TASK_SENT_TO_STORED.items():
                    if field in self.task_sent:
                        record[target] = self.task_sent[field]
                return {"success": True, "data": record}
            if endpoint == "profile/create/individual":
                return {"success": True, "data": {"profile_id": 21700}}
            if endpoint == "task/create":
                self.task_sent = dict(data or {})
                return {"success": True, "data": {"task_id": 1050}}
            raise AssertionError(endpoint)

    csuite = Real()
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": {"first_name": "S", "last_name": "A",
                                 "email": "s@example.invalid"},
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, HubSpot(), csuite)

    assert "📝 Follow-up task 1050 for Zouita, Kods — re: S A" in reply
    assert " for 1007 " not in reply, "a bare employee id makes a reader look it up"
    assert "(sandbox)" not in reply, "live output carries no marker"
    assert "task/display" in csuite.endpoints, "the name comes from the read-back"


def test_without_a_read_back_name_it_falls_back_to_the_id(monkeypatch):
    """Honest degradation: an id is worse than a name and better than nothing."""
    from clients.csuite import CSuiteClient

    monkeypatch.setattr("config.Config.CSUITE_ENV", "live")
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_HUBSPOT_BACKFILL_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", 1007)
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", None)

    class NoName(CSuite):
        def create_task(self, **kwargs):
            return {"success": True, "data": {"task_id": 1050},
                    "verified": True}          # no assignee_name

    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": {"first_name": "S", "last_name": "A",
                                 "email": "s@example.invalid"},
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, HubSpot(), NoName())

    assert "📝 Follow-up task 1050 for 1007 — re: S A" in reply
