"""The inquiry workflow creates a CSuite follow-up task, linked to the profile.

Every input name was proven on 2026-10-01 against sandbox task 1034:
`name`, `task_description`, `employee_id`, `due_ts` (YYYY-MM-DD, read back as
`due_date`), `task_type_id`, and the link `o="profile"` + `id=<profile_id>`.

Three rules this file enforces:

* **A task never changes the profile result.** A profile is a record; a task is
  a reminder. A reminder that was not made is a person to tell, not a record to
  undo.
* **Nothing is guessed.** No assignee and no task type are hardcoded. Unset
  means skipped with a stated reason, because 1006 is Carl in the SANDBOX and
  is not present in production at all.
* **The line is never silent.** A reminder nobody was told about is a reminder
  that does not exist.

No network.
"""

from datetime import date

import pytest

from config import Config
from intents import daf_workflow
from intents.daf_workflow import (_parse_submission, form_label,
                                  task_assignee_for_form,
                                  task_due_date)
from tests.csuite_doubles import HasDuplicate, NoDuplicates, contact

EMAIL = "sarah.ahmed@example.invalid"


def submission(**fields):
    base = {"firstname": "Sarah", "lastname": "Ahmed", "email": EMAIL,
            "phone": "(703) 555-0177", "address": "1 Test Way",
            "city": "Reston", "state": "VA", "zip": "20190"}
    base.update(fields)
    return {"submittedAt": "2026-10-01", "conversionId": "conv-x",
            "values": [{"name": k, "value": v} for k, v in base.items() if v]}


class CSuite(NoDuplicates):
    """Profile create plus task create, both recorded."""

    def __init__(self, task_response=None, task_raises=None):
        self.calls = []
        self.task_response = task_response
        self.task_raises = task_raises

    def create_individual_profile(self, **kwargs):
        self.calls.append(("profile", kwargs))
        return {"success": True, "data": {"profile_id": 21700}}

    def create_task(self, **kwargs):
        self.calls.append(("task", kwargs))
        if self.task_raises:
            raise self.task_raises
        if self.task_response is not None:
            return self.task_response
        return {"success": True, "data": {"task_id": 1040}, "verified": True}

    @property
    def kinds(self):
        return [kind for kind, _ in self.calls]

    @property
    def task_kwargs(self):
        return next(kw for kind, kw in self.calls if kind == "task")


class DuplicateCSuite(HasDuplicate, CSuite):
    pass


class HubSpot:
    def __init__(self, row=None):
        self.row = row if row is not None else contact()

    def search_contact_by_email(self, email, properties=None):
        return self.row

    def update_contact_by_email(self, email, properties):
        return {"id": "70123"}

    def get_open_tickets(self):
        return {"results": []}


def run(monkeypatch, csuite=None, hubspot=None, enabled=True, assignee=1007,
        task_type=None, wf_type="daf", sub=None):
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", enabled)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY",
                        assignee)
    monkeypatch.setattr(Config,
                        "CSUITE_TASK_EMPLOYEE_ID_ENDOWMENT_INQUIRY",
                        assignee)
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", task_type)
    state = {"active": True, "workflow_type": "daf", "type": wf_type,
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": _parse_submission(sub or submission()),
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, hubspot or HubSpot(),
                                      csuite or CSuite())
    return reply, state


# ---------------------------------------------------------------------------
# STEP 2 — config, no constants
# ---------------------------------------------------------------------------

def test_the_task_flag_is_off_by_default():
    assert Config.CSUITE_DAF_TASK_CREATE_ENABLED is False


def test_no_assignee_and_no_task_type_are_hardcoded():
    """A constant here would be the DEFAULT_CASH_ACCOUNT_ID mistake again —
    1069 sat in config for months before anyone checked it meant the same
    account in both environments. 1006 is Carl in the SANDBOX and is not
    present in production at all."""
    assert Config.CSUITE_TASK_EMPLOYEE_ID is None
    assert Config.CSUITE_DAF_TASK_EMPLOYEE_ID is None
    assert Config.CSUITE_ENDOWMENT_TASK_EMPLOYEE_ID is None
    assert Config.CSUITE_TASK_TYPE_ID is None


@pytest.mark.parametrize("raw", ["", "  ", "none", "-1", "0", "abc"])
def test_an_unparseable_assignee_is_none_not_zero(monkeypatch, raw):
    import importlib

    import config
    monkeypatch.setenv("CSUITE_TASK_EMPLOYEE_ID", raw)
    try:
        importlib.reload(config)
        assert config.Config.CSUITE_TASK_EMPLOYEE_ID is None
    finally:
        monkeypatch.delenv("CSUITE_TASK_EMPLOYEE_ID", raising=False)
        importlib.reload(config)


def test_each_form_has_its_own_assignee(monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", 1004)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_ENDOWMENT_INQUIRY", 1009)

    assert task_assignee_for_form(Config.DAF_INQUIRY_FORM_ID) == 1004
    assert task_assignee_for_form(Config.ENDOWMENT_INQUIRY_FORM_ID) == 1009


def test_there_is_NO_fallback_to_a_default_person(monkeypatch):
    """A task in the wrong queue looks exactly like a task in the right one."""
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID", 1007)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", None)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_ENDOWMENT_INQUIRY", None)

    assert task_assignee_for_form(Config.DAF_INQUIRY_FORM_ID) is None
    assert task_assignee_for_form(Config.ENDOWMENT_INQUIRY_FORM_ID) is None


@pytest.mark.parametrize("form_id,label", [
    (Config.ASSET_DONATION_FORM_ID, "Asset Transfer"),
    (Config.INVESTMENT_REQUEST_FORM_ID, "Investment Request"),
])
def test_an_unhandled_form_has_no_assignee_and_a_readable_label(form_id, label):
    assert task_assignee_for_form(form_id) is None
    assert form_label(form_id) == label


def test_an_unknown_form_id_still_reads_sensibly():
    assert form_label(None) == "an unknown form"
    assert form_label("abc-123") == "form abc-123"


# ---------------------------------------------------------------------------
# due_ts — two business days
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("submitted,expected", [
    ("2026-10-01", "2026-10-05"),   # Thu -> Mon, skipping the weekend
    ("2026-10-02", "2026-10-06"),   # Fri -> Tue
    ("2026-10-03", "2026-10-06"),   # Sat -> Tue
    ("2026-10-04", "2026-10-06"),   # Sun -> Tue
    ("2026-10-05", "2026-10-07"),   # Mon -> Wed
    ("2026-10-06", "2026-10-08"),   # Tue -> Thu
    ("2026-10-07", "2026-10-09"),   # Wed -> Fri
])
def test_two_business_days_skips_saturday_and_sunday(submitted, expected):
    assert task_due_date(submitted) == expected


def test_the_due_date_is_never_a_weekend():
    for day in range(1, 29):
        due = task_due_date(f"2026-10-{day:02d}")
        assert date.fromisoformat(due).weekday() < 5, due


def test_a_hubspot_epoch_milliseconds_value_is_understood():
    # 2026-10-01 00:00:00 UTC
    assert task_due_date(1790812800000) == "2026-10-05"
    assert task_due_date("1790812800000") == "2026-10-05"


def test_an_unreadable_submitted_at_falls_back_to_today():
    for value in (None, "", "Unknown", "not a date", {}):
        due = date.fromisoformat(task_due_date(value))
        assert due > date.today()
        assert due.weekday() < 5


# ---------------------------------------------------------------------------
# STEP 3 — what gets sent
# ---------------------------------------------------------------------------

def test_the_task_links_to_the_new_profile(monkeypatch):
    csuite = CSuite()
    _, state = run(monkeypatch, csuite=csuite)

    assert csuite.kinds == ["profile", "task"]
    kw = csuite.task_kwargs
    assert kw["linked_profile_id"] == 21700 == state["profile_id"]
    assert kw["employee_id"] == 1007
    assert kw["due_date"] == "2026-10-05"
    assert kw["task_type_id"] is None, "unset means omitted, not guessed"


def test_name_and_description_are_identical_text(monkeypatch):
    csuite = CSuite()
    run(monkeypatch, csuite=csuite)
    kw = csuite.task_kwargs

    assert kw["name"] == kw["description"]
    assert kw["name"] == f"DAF inquiry follow-up: Sarah Ahmed — {EMAIL}"


def test_an_endowment_inquiry_says_endowment(monkeypatch):
    csuite = CSuite()
    run(monkeypatch, csuite=csuite, wf_type="endowment")

    assert csuite.task_kwargs["name"].startswith(
        "Endowment inquiry follow-up: ")


def test_a_configured_task_type_is_sent(monkeypatch):
    csuite = CSuite()
    run(monkeypatch, csuite=csuite, task_type=1065)

    assert csuite.task_kwargs["task_type_id"] == 1065


def test_a_returning_donor_gets_a_task_on_the_EXISTING_profile(monkeypatch):
    """The duplicate guard stopped the create. They still need following up,
    and the existing profile is the right thing to link to."""
    csuite = DuplicateCSuite()
    reply, state = run(monkeypatch, csuite=csuite)

    assert "profile" not in csuite.kinds, "no profile may be created"
    assert csuite.task_kwargs["linked_profile_id"] == 19999
    assert "Already in CSuite" in reply
    assert "re: Sarah Ahmed" in reply
    assert "📝 Follow-up task 1040 for" in reply


def test_two_matches_mean_no_task_and_the_reason_is_the_ambiguity(monkeypatch):
    class Two(CSuite):
        duplicate_ids = (19999, 20001)

    csuite = Two()
    reply, state = run(monkeypatch, csuite=csuite)

    assert "task" not in csuite.kinds
    assert state["profile_id"] is None
    assert "📝 No task:" in reply
    assert "ambiguous" in reply or "19999" in reply


# ---------------------------------------------------------------------------
# STEP 4 — the three lines
# ---------------------------------------------------------------------------

def test_the_success_line_names_the_task_the_assignee_and_the_donor(monkeypatch):
    """A line that says only "Follow-up task created" does not let anyone check
    it went to the right person about the right donor."""
    reply, _ = run(monkeypatch)

    assert "📝 Follow-up task 1040 for 1007 — re: Sarah Ahmed — due " \
        "2026-10-05 — [View](" in reply
    assert "task_id=1040" in reply


def test_the_line_prefers_the_assignee_NAME_when_the_read_back_gave_one(
        monkeypatch):
    """"1007" makes the reader look it up. The name is only known after the
    read-back — task/create returns task_id and task_guid and nothing else."""
    csuite = CSuite(task_response={
        "success": True, "data": {"task_id": 1040}, "verified": True,
        "assignee_name": "Zouita, Kods"})
    reply, _ = run(monkeypatch, csuite=csuite)

    assert "📝 Follow-up task 1040 for Zouita, Kods — re: Sarah Ahmed" in reply
    assert " for 1007 " not in reply


def test_the_failure_line_never_blames_the_profile(monkeypatch):
    csuite = CSuite(task_response={"success": False, "error": "no permission"})
    reply, state = run(monkeypatch, csuite=csuite)

    assert "⚠️ Follow-up task NOT created (no permission) — add it by hand " \
        "in CSuite." in reply
    assert state["profile_id"] == 21700, "the profile still succeeded"
    assert "Profile Created" in reply
    assert "❌ Profile" not in reply


def test_an_exception_in_the_task_does_not_sink_the_profile(monkeypatch):
    csuite = CSuite(task_raises=RuntimeError("CSuite is down"))
    reply, state = run(monkeypatch, csuite=csuite)

    assert state["profile_id"] == 21700
    assert "Follow-up task NOT created (CSuite is down)" in reply
    assert "Profile Created" in reply


@pytest.mark.parametrize("kwargs,fragment", [
    ({"enabled": False}, "follow-up tasks are turned off"),
    ({"assignee": None}, "no assignee set for DAF Inquiry"),
])
def test_the_skipped_line_states_the_reason(monkeypatch, kwargs, fragment):
    csuite = CSuite()
    reply, _ = run(monkeypatch, csuite=csuite, **kwargs)

    assert "task" not in csuite.kinds
    assert f"📝 No task: {fragment}" in reply
    assert "NOT created" not in reply, "skipped is not failed"


def test_a_read_back_warning_is_surfaced_under_the_task_line(monkeypatch):
    csuite = CSuite(task_response={
        "success": True, "data": {"task_id": 1040}, "verified": False,
        "task_warning": "⚠️ Task 1040 was created but CSuite did not store: o."})
    reply, _ = run(monkeypatch, csuite=csuite)

    assert "📝 Follow-up task 1040 for" in reply
    assert "did not store: o" in reply


def test_there_is_always_exactly_one_task_line(monkeypatch):
    for kwargs in ({}, {"enabled": False}, {"assignee": None}):
        reply, _ = run(monkeypatch, csuite=CSuite(), **kwargs)
        lines = [l for l in reply.splitlines()
                 if l.startswith("📝") or "Follow-up task" in l]
        assert len(lines) == 1, (kwargs, lines)


# ---------------------------------------------------------------------------
# STEP 1 — the read-back
# ---------------------------------------------------------------------------

def test_a_task_read_back_clean_is_verified():
    from clients.csuite import CSuiteClient

    class Client(CSuiteClient):
        verify_writes = True

        def __init__(self):
            self.sent = []

        def _request(self, endpoint, data=None):
            self.sent.append(endpoint)
            if endpoint == "task/display":
                return {"success": True, "data": {
                    "task_id": 1040, "task_description": "S",
                    "employee_id": 1007, "due_date": "2026-10-05",
                    "task_type_id": 1065, "o": "profile", "id": 21700}}
            return {"success": True, "data": {"task_id": 1040}}

    client = Client()
    result = client.create_task("S", 1007, due_date="2026-10-05",
                                description="S", linked_profile_id=21700,
                                task_type_id=1065)
    assert result["verified"] is True
    assert "task_warning" not in result
    assert client.sent == ["task/create", "task/display"]


@pytest.mark.parametrize("field,stored", [
    ("o", None), ("id", 99999), ("due_date", "2026-12-25"),
    ("employee_id", 1), ("task_type_id", 9), ("task_description", "wrong"),
])
def test_a_mismatch_on_any_verified_field_warns(field, stored):
    from clients.csuite import CSuiteClient

    good = {"task_id": 1040, "task_description": "S", "employee_id": 1007,
            "due_date": "2026-10-05", "task_type_id": 1065, "o": "profile",
            "id": 21700}
    good[field] = stored

    class Client(CSuiteClient):
        verify_writes = True

        def __init__(self):
            pass

        def _request(self, endpoint, data=None):
            if endpoint == "task/display":
                return {"success": True, "data": good}
            return {"success": True, "data": {"task_id": 1040}}

    result = Client().create_task("S", 1007, due_date="2026-10-05",
                                  description="S", linked_profile_id=21700,
                                  task_type_id=1065)
    assert result["verified"] is False
    assert "did not store" in result["task_warning"]
    assert "1040" in result["task_warning"]


def test_an_unreadable_task_is_unconfirmed_not_wrong():
    from clients.csuite import CSuiteClient

    class Client(CSuiteClient):
        verify_writes = True

        def __init__(self):
            pass

        def _request(self, endpoint, data=None):
            if endpoint == "task/display":
                return {"success": False, "error": "boom"}
            return {"success": True, "data": {"task_id": 1040}}

    result = Client().create_task("S", 1007, due_date="2026-10-05")
    assert result["verified"] is None
    assert "could not be read back" in result["task_warning"]


def test_name_is_not_compared_because_it_is_ambiguous():
    """`name` and `task_description` were sent with the same text on task
    1034, so which populated task_description is unknown. Comparing name
    against it would pass for the wrong reason."""
    from sync.readback import TASK_SENT_TO_STORED

    assert "name" not in TASK_SENT_TO_STORED
    assert TASK_SENT_TO_STORED["due_ts"] == "due_date"
    assert TASK_SENT_TO_STORED["o"] == "o"
    assert TASK_SENT_TO_STORED["id"] == "id"


def test_an_omitted_optional_field_cannot_be_reported_as_dropped():
    from sync.readback import compare_task

    sent = {"task_description": "S", "employee_id": 1007}
    stored = {"task_id": 1040, "task_description": "S", "employee_id": 1007}
    assert compare_task(sent, stored) == {}


def test_the_duplicate_path_shows_the_read_back_warning_too(monkeypatch):
    """The two paths render the same line through one function, because they
    had already drifted once — the duplicate path was missing this warning."""
    class Dup(DuplicateCSuite):
        def create_task(self, **kwargs):
            self.calls.append(("task", kwargs))
            return {"success": True, "data": {"task_id": 1041},
                    "verified": False,
                    "task_warning": "⚠️ Task 1041 … did not store: id."}

    reply, _ = run(monkeypatch, csuite=Dup())

    assert "Already in CSuite" in reply
    assert "📝 Follow-up task 1041 for" in reply
    assert "did not store: id" in reply


# ---------------------------------------------------------------------------
# Per-form assignee, through the workflow
# ---------------------------------------------------------------------------

def test_an_unmapped_form_names_the_form_and_makes_no_task(monkeypatch):
    """Asset Transfer and Investment Request are not handled by this workflow."""
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", 1004)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_ENDOWMENT_INQUIRY", 1009)
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", None)

    csuite = CSuite()
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.ASSET_DONATION_FORM_ID,
             "submission_data": _parse_submission(submission()),
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, HubSpot(), csuite)

    assert "task" not in csuite.kinds, "no task for a form nobody owns"
    assert "📝 No task: no assignee set for Asset Transfer" in reply


def test_a_configured_form_still_gets_its_own_person(monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", 1004)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_ENDOWMENT_INQUIRY", 1009)
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", None)

    for form_id, expected in ((Config.DAF_INQUIRY_FORM_ID, 1004),
                              (Config.ENDOWMENT_INQUIRY_FORM_ID, 1009)):
        csuite = CSuite()
        state = {"active": True, "workflow_type": "daf", "type": "daf",
                 "step": "confirm", "form_id": form_id,
                 "submission_data": _parse_submission(submission()),
                 "profile_id": None, "funit_id": None, "ticket_id": None}
        daf_workflow._step_create("yes", state, HubSpot(), csuite)
        assert csuite.task_kwargs["employee_id"] == expected, form_id


def test_the_shared_variable_is_ignored_entirely(monkeypatch):
    """It is superseded. A deployment still setting it must not get a task
    assigned to that person by accident."""
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID", 1007)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", None)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_ENDOWMENT_INQUIRY", None)

    csuite = CSuite()
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": _parse_submission(submission()),
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, HubSpot(), csuite)

    assert "task" not in csuite.kinds
    assert "no assignee set for DAF Inquiry" in reply


def test_an_endowment_inquiry_with_no_assignee_says_so_plainly(monkeypatch):
    """Today's normal endowment outcome, not an oversight: Ola has no CSuite
    employee_id — no production or sandbox task names her, and there is no
    employee list endpoint to look her up in (2026-10-02). A bare "No task: no
    assignee set for Endowment Inquiry" reads like something that failed."""
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", 1004)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_ENDOWMENT_INQUIRY", None)
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", None)

    csuite = CSuite()
    state = {"active": True, "workflow_type": "daf", "type": "endowment",
             "step": "confirm", "form_id": Config.ENDOWMENT_INQUIRY_FORM_ID,
             "submission_data": _parse_submission(submission()),
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, HubSpot(), csuite)

    assert "⚠️ Endowment inquiry: profile created, NO task — assignee not " \
        "set." in reply
    assert state["profile_id"] == 21700, "the profile WAS created"
    assert "task" not in csuite.kinds
    assert "No task: no assignee" not in reply, "the plain line replaces it"


def test_a_DAF_inquiry_with_no_assignee_keeps_the_generic_line(monkeypatch):
    """Only the endowment case is spelled out; the others stay uniform."""
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", None)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_ENDOWMENT_INQUIRY", None)
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", None)

    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "form_id": Config.DAF_INQUIRY_FORM_ID,
             "submission_data": _parse_submission(submission()),
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, HubSpot(), CSuite())

    assert "📝 No task: no assignee set for DAF Inquiry" in reply
    assert "Endowment inquiry:" not in reply
