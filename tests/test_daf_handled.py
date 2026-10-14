"""DAF "handled" = profile OR task OR reconcile link.

Measured 2026-10-14, before any of this was written:

* processed_submissions counted only a live profile/create/individual. The
  returning-donor path (daf_workflow, the DuplicateProfile branch) finds the
  existing profile, makes a follow-up task and returns — no profile create —
  and the task's audit row carried no submission id. So a handled returning
  donor was offered again in "new daf" forever.
* Live: 50 newest fetched, 5 processed. Task 1033 belongs to submission
  62ed3c5d (2026-09-29, unprocessed). Task 1036 matched TWO submissions —
  6a31606e (2026-09-15, processed; it made profile 21699 and task 1036) and
  6fff2fa0 (2026-09-03), one donor who submitted twice. Carl decided:
  6fff2fa0 links as earlier_submission_same_donor.
* HubSpot returns submissions NEWEST FIRST: all 90, both pages, 2026-10-08
  down to 2025-10-14, no reversal.
* write_audit's target_system CHECK admits only 'hubspot' and 'csuite'; the
  local link rows need migrations/007.

No network, no database.
"""

import json

import pytest

from clients import audit
from clients.csuite import CSuiteClient
from config import Config
from intents import daf_workflow, sync_commands
from intents.daf_workflow import _initiate_workflow, is_newest_first
from sync import daf_reconcile as dr
from sync.sandbox_writes import WriteBudget

FORM = Config.DAF_INQUIRY_FORM_ID


# ---------------------------------------------------------------------------
# CHANGE 1 — the task carries the submission; the body does not change
# ---------------------------------------------------------------------------

class TaskClient(CSuiteClient):
    """The REAL _request, with the transport and the audit store faked."""

    def __init__(self):
        self.api_key = "k"
        self.api_secret = "s"
        self.base_url = "https://amuslimcf.fcsuite.com/api/v2"
        self.env = "live"
        self.verify_writes = False
        self.write_budget = WriteBudget(5)
        self.sent = []
        outer = self

        class Session:
            def post(_self, url, data=None, **kwargs):
                outer.sent.append(data)

                class R:
                    status_code = 200
                    text = '{"success":1,"data":{"task_id":1039}}'

                    def json(self):
                        return {"success": 1, "data": {"task_id": 1039}}
                return R()

        self.session = Session()


@pytest.fixture
def audited(monkeypatch):
    seen = []

    def reserve(system, method, endpoint, target_id=None, payload=None,
                sync_run_id=None):
        seen.append({"endpoint": endpoint, "payload": payload,
                     "meta": audit.payload_meta(payload)})
        return {"id": len(seen)}

    import clients.csuite as mod
    monkeypatch.setattr(mod, "reserve_write", reserve)
    monkeypatch.setattr(mod, "complete_write", lambda *a, **kw: None)
    return seen


TASK = dict(name="DAF inquiry follow-up", employee_id=1012,
            due_date="2026-10-20", description="DAF inquiry follow-up",
            linked_profile_id=21699, task_type_id=1065)
META = {"hubspot_form_id": FORM, "hubspot_submission_id": "6fff2fa0",
        "linked_profile_id": 21699}


def test_the_task_audit_row_carries_the_submission(audited):
    TaskClient().create_task(**TASK, audit_meta=META)

    ids = audited[0]["meta"]["ids"]
    assert audited[0]["endpoint"] == "task/create"
    assert ids["hubspot_form_id"] == FORM
    assert ids["hubspot_submission_id"] == "6fff2fa0"
    assert ids["linked_profile_id"] == 21699


def test_the_csuite_request_body_is_byte_identical(audited):
    """Only the AUDIT ROW changes. What reaches CSuite is exactly what it was
    in V5.78."""
    with_meta, without = TaskClient(), TaskClient()

    with_meta.create_task(**TASK, audit_meta=META)
    without.create_task(**TASK)

    assert with_meta.sent == without.sent, "the body must not change"
    body = json.loads(with_meta.sent[0])
    for key in ("hubspot_form_id", "hubspot_submission_id",
                "linked_profile_id"):
        assert key not in body, f"{key} must never reach CSuite"


def test_a_task_with_no_audit_meta_is_unchanged(audited):
    TaskClient().create_task(**TASK)

    assert "hubspot_submission_id" not in audited[0]["meta"]["ids"]


def test_a_narrow_request_override_still_works_without_audit_meta():
    """The profile create's rule, copied: audit_meta is passed only when
    there is something to record, so a double that does not know about it
    keeps working."""
    calls = []

    class Narrow(TaskClient):
        def _request(self, endpoint, data=None):
            calls.append(endpoint)
            return {"success": True, "data": {"task_id": 1}}

    Narrow().create_task(**TASK)
    assert calls == ["task/create"]


def test_the_daf_workflow_passes_the_submission_to_the_task(monkeypatch):
    seen = {}

    class Recorder:
        def create_task(self, **kwargs):
            seen.update(kwargs)
            return {"success": True, "data": {"task_id": 1039}}

    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", 1012)
    state = {"form_id": FORM, "profile_id": 21699}
    results = {}
    daf_workflow._create_followup_task(
        {"submission_id": "6fff2fa0", "first_name": "A", "last_name": "B",
         "email": "a@example.invalid", "submitted_at": 1790000000000},
        state, results, Recorder(), "daf", "DAF")

    assert seen.get("audit_meta") == {"hubspot_form_id": FORM,
                                      "hubspot_submission_id": "6fff2fa0",
                                      "linked_profile_id": 21699}


# ---------------------------------------------------------------------------
# processed = profile OR task OR link
# ---------------------------------------------------------------------------

def _capture_query(monkeypatch, rows):
    seen = {}
    monkeypatch.setattr(audit, "_csuite_env_column_exists", lambda: True)
    monkeypatch.setattr("clients.database.is_configured", lambda: True)

    def query(sql, params=None, fetch=True):
        seen["sql"] = " ".join(sql.split())
        seen["params"] = params
        return rows

    monkeypatch.setattr("clients.database.execute_query", query)
    return seen


def test_the_query_counts_profile_task_and_link(monkeypatch):
    seen = _capture_query(monkeypatch, [])
    audit.processed_submissions(FORM)

    assert "endpoint = ANY(%s)" in seen["sql"]
    assert seen["params"][1] == ["profile/create/individual", "task/create"]
    assert seen["params"][2] == "jidhr"
    assert seen["params"][3] == "submission/link/%"
    assert "csuite_env = 'live'" in seen["sql"]
    assert "status = 'success'" in seen["sql"]


def test_all_three_kinds_read_as_handled(monkeypatch):
    _capture_query(monkeypatch, [
        {"submission_id": "s-profile", "target_id": "21700",
         "endpoint": "profile/create/individual"},
        {"submission_id": "s-task", "target_id": "21699",
         "endpoint": "task/create"},
        {"submission_id": "s-link", "target_id": "21696",
         "endpoint": "submission/link/task_for_profile"},
    ])

    assert audit.processed_submissions(FORM) == {
        "s-profile": "21700", "s-task": "21699", "s-link": "21696"}


def test_the_profile_reported_is_the_donors_not_the_tasks(monkeypatch):
    """A task row's target_id is the TASK. linked_profile_id names the
    donor, and the query reports that."""
    seen = _capture_query(monkeypatch, [])
    audit.processed_submissions(FORM)

    assert ("COALESCE(payload_meta->'ids'->>'linked_profile_id', target_id)"
            in seen["sql"])


def test_first_row_wins_means_the_earliest_row(monkeypatch):
    seen = _capture_query(monkeypatch, [
        {"submission_id": "s", "target_id": "21699",
         "endpoint": "profile/create/individual"},
        {"submission_id": "s", "target_id": "99999",
         "endpoint": "task/create"}])

    assert audit.processed_submissions(FORM) == {"s": "21699"}
    assert "ORDER BY id" in seen["sql"], "first row must mean earliest"


def test_link_rows_are_not_tagged_as_csuite_writes():
    """sync/sandbox_writes counts write_audit rows per target_system to
    reconcile a write budget. A link row tagged 'csuite' would read as a
    breach."""
    assert audit.LINK_TARGET_SYSTEM == "jidhr"
    assert audit.LINK_TARGET_SYSTEM not in ("csuite", "hubspot")


# ---------------------------------------------------------------------------
# The "new daf" flow no longer re-offers a tasked submission
# ---------------------------------------------------------------------------

class SubmissionsHub:
    def __init__(self, submissions):
        self._subs = submissions

    def get_daf_inquiry_submissions(self, limit=50):
        return {"results": self._subs[:limit]}

    def get_form_submissions(self, form_id, limit=50, after=None):
        # One page: every fixture here is shorter than a page.
        return {"results": self._subs[:limit]}


def sub(conv, first, submitted):
    return {"conversionId": conv, "submittedAt": submitted,
            "values": [{"name": "firstname", "value": first},
                       {"name": "lastname", "value": "X"},
                       {"name": "email",
                        "value": f"{first.lower()}@example.invalid"}]}


def test_the_chat_flow_skips_a_submission_that_has_a_task(monkeypatch):
    """62ed3c5d is the newest unprocessed submission, and its donor already
    has task 1033. Once the task counts as handled, the flow must offer the
    next one instead — offering it would make a SECOND task."""
    tasked = sub("62ed3c5d", "Tasked", 1790704953446)       # newest
    untasked = sub("aaaa1111", "Fresh", 1790000000000)
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: {"62ed3c5d": "21696"})

    state = dict(daf_workflow.default_workflow_state())
    reply = _initiate_workflow("process daf inquiry", state,
                               SubmissionsHub([tasked, untasked]))

    # The one OFFERED is the untasked one; the tasked one is named only as
    # already handled, which is what tells a reviewer why it was skipped.
    assert state["submission_data"]["submission_id"] == "aaaa1111"
    assert "**Name:** Fresh X" in reply
    assert "**Name:** Tasked X" not in reply
    assert "Tasked X is profile 21696" in reply


def test_before_the_fix_the_tasked_one_would_have_been_offered(monkeypatch):
    """The regression, pinned: with only profile creates counted, the
    tasked submission IS the newest unprocessed and gets offered."""
    tasked = sub("62ed3c5d", "Tasked", 1790704953446)
    untasked = sub("aaaa1111", "Fresh", 1790000000000)
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: {})

    state = dict(daf_workflow.default_workflow_state())
    _initiate_workflow("process daf inquiry", state,
                       SubmissionsHub([tasked, untasked]))

    assert state["submission_data"]["submission_id"] == "62ed3c5d"


# ---------------------------------------------------------------------------
# CHANGE 3 — newest first is assumed, and pinned
# ---------------------------------------------------------------------------

# The real order of the five newest DAF submissions, 2026-10-14, by
# submittedAt only — no personal data. The flow treats index 0 as newest.
REAL_NEWEST_FIVE = [1791946968174, 1790704953446, 1789670000000,
                    1789660000000, 1789500000000]


def parsed(times):
    return [{"submitted_at": t} for t in times]


def test_the_real_order_is_newest_first():
    assert is_newest_first(parsed(REAL_NEWEST_FIVE))


def test_a_flipped_order_is_caught():
    """If HubSpot ever returns oldest first, "the newest" becomes the oldest
    and a months-old submission is offered as new. This must fail."""
    assert not is_newest_first(parsed(list(reversed(REAL_NEWEST_FIVE))))


def test_one_out_of_place_row_is_caught():
    shuffled = [REAL_NEWEST_FIVE[1], REAL_NEWEST_FIVE[0]] + \
        REAL_NEWEST_FIVE[2:]
    assert not is_newest_first(parsed(shuffled))


def test_equal_times_and_unreadable_rows_are_not_false_alarms():
    assert is_newest_first(parsed([5, 5, 4]))
    assert is_newest_first(parsed([5, "not a time", 4]))
    assert is_newest_first([])


def test_a_flipped_page_is_logged_loudly(monkeypatch, caplog):
    import logging

    old = sub("old", "Old", 1700000000000)
    new = sub("new", "New", 1790000000000)
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: {})

    with caplog.at_level(logging.ERROR, logger="intents.daf_workflow"):
        _initiate_workflow("process daf inquiry",
                           dict(daf_workflow.default_workflow_state()),
                           SubmissionsHub([old, new]))

    assert any("did NOT come back newest first" in r.getMessage()
               for r in caplog.records)


def test_the_reconcile_does_no_paging():
    """The RECONCILE reads page 1 only, by decision: 50, no cursor. (The chat
    flow pages fully since V5.81 — tests/test_daf_unchanged.)"""
    import inspect

    assert "after" not in inspect.getsource(dr.page_one)
    assert "limit=50" in inspect.getsource(dr.page_one)


# ---------------------------------------------------------------------------
# CHANGE 2 — the reconcile
# ---------------------------------------------------------------------------

EMAIL_TASKED = "tasked@example.invalid"
EMAIL_EARLY = "twice@example.invalid"
EMAIL_NOPROF = "nobody@example.invalid"
EMAIL_NOTASK = "loner@example.invalid"


def subs_fixture():
    """09-29 tasked (rule A), 09-15 processed + 09-03 earlier from the same
    donor (rule B), and two leftovers."""
    def s(conv, email, ms):
        return {"conversionId": conv, "submittedAt": ms,
                "values": [{"name": "email", "value": email},
                           {"name": "firstname", "value": "A"},
                           {"name": "lastname", "value": "B"}]}
    return [s("62ed3c5d", EMAIL_TASKED, 1790704953446),
            s("6a31606e", EMAIL_EARLY, 1789670000000),
            s("6fff2fa0", EMAIL_EARLY, 1788480000000),
            s("loner001", EMAIL_NOTASK, 1788000000000),
            s("nobody01", EMAIL_NOPROF, 1787000000000)]


class ReconcileHub:
    def __init__(self, submissions):
        self.subs = submissions
        self.calls = []

    def get_form_submissions(self, form_id, limit=50):
        self.calls.append(("GET", form_id, limit))
        return {"results": self.subs[:limit]}

    def __getattr__(self, name):
        # Any OTHER HubSpot method is a write this reconcile must not make.
        raise AssertionError(f"reconcile called HubSpot.{name}")


class ReconcileCSuite:
    """task/display and the trusted profile/list search. Nothing else."""

    PROFILES = {EMAIL_TASKED: ["21696"], EMAIL_EARLY: ["21699"],
                EMAIL_NOTASK: ["30000"], EMAIL_NOPROF: []}
    TASKS = {"1033": "21696", "1036": "21699"}

    def __init__(self, profiles=None):
        self.profiles = profiles or dict(self.PROFILES)
        self.calls = []

    def _request(self, endpoint, data=None, **kwargs):
        self.calls.append(endpoint)
        if endpoint == "task/display":
            profile = self.TASKS.get(str(data["task_id"]))
            return {"success": True,
                    "data": {"o": "profile", "id": profile}}
        raise AssertionError(f"reconcile asked CSuite for {endpoint}")


@pytest.fixture
def reconcile(monkeypatch):
    """Wire the reconcile to fakes: tasks, processed history, search."""
    state = {"links": [], "processed": {"6a31606e": "21699"},
             "csuite": ReconcileCSuite()}

    monkeypatch.setattr(dr, "legacy_tasks", lambda form_id: ["1033", "1036"])
    monkeypatch.setattr(dr, "tasked_submissions", lambda form_id: set())
    monkeypatch.setattr(dr, "_pace", lambda: None)
    monkeypatch.setattr(dr, "link_rows_allowed", lambda: True)
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: dict(state["processed"]))
    monkeypatch.setattr(
        dr, "profiles_for",
        lambda csuite, email: (list(state["csuite"].profiles.get(email, [])),
                               None))

    def record(target_system, method, endpoint, target_id=None, payload=None,
               status="success", **kwargs):
        state["links"].append({"target_system": target_system,
                               "method": method, "endpoint": endpoint,
                               "target_id": target_id,
                               "meta": audit.payload_meta(payload)})
        # A recorded link is handled from then on.
        state["processed"][target_id] = (payload or {}).get(
            "linked_profile_id")
        return True

    monkeypatch.setattr(audit, "record_write", record)
    return state


def run_reconcile(state, dry_run):
    hub = ReconcileHub(subs_fixture())
    out = dr.run(hubspot=hub, csuite=state["csuite"], dry_run=dry_run,
                 form_id=FORM)
    return out, hub


def test_the_dry_run_proposes_exactly_the_two_decided_links(reconcile):
    out, _hub = run_reconcile(reconcile, dry_run=True)

    links = {l["submission_id"]: l for l in out["links"]}
    assert set(links) == {"62ed3c5d", "6fff2fa0"}
    assert links["62ed3c5d"]["reason"] == dr.LINK_TASK_FOR_PROFILE
    assert links["62ed3c5d"]["task"] == "1033"
    assert links["6fff2fa0"]["reason"] == dr.LINK_EARLIER_SUBMISSION
    assert links["6fff2fa0"]["linked_from"] == "6a31606e"
    assert links["6fff2fa0"]["profile"] == "21699"
    assert links["6fff2fa0"]["task"] == "1036"


def test_the_dry_run_writes_nothing(reconcile):
    out, _hub = run_reconcile(reconcile, dry_run=True)

    assert reconcile["links"] == []
    assert out["written"] == []


def test_the_live_run_writes_only_write_audit_link_rows(reconcile):
    out, hub = run_reconcile(reconcile, dry_run=False)

    assert len(reconcile["links"]) == 2
    for row in reconcile["links"]:
        assert row["target_system"] == "jidhr"
        assert row["method"] == "LINK"
        assert row["endpoint"].startswith("submission/link/")
    # Reads only, on both systems.
    assert all(c[0] == "GET" for c in hub.calls)
    assert set(reconcile["csuite"].calls) <= {"task/display"}


def test_the_reason_is_in_the_endpoint_not_a_dropped_payload_value(
        reconcile):
    """payload_meta keeps key names and id values only. A reason held as a
    payload value would be thrown away."""
    run_reconcile(reconcile, dry_run=False)

    endpoints = sorted(r["endpoint"] for r in reconcile["links"])
    assert endpoints == [
        "submission/link/earlier_submission_same_donor",
        "submission/link/task_for_profile"]


def test_the_link_row_records_its_references(reconcile):
    run_reconcile(reconcile, dry_run=False)

    early = next(r for r in reconcile["links"]
                 if r["endpoint"].endswith(dr.LINK_EARLIER_SUBMISSION))
    ids = early["meta"]["ids"]
    assert ids["hubspot_submission_id"] == "6fff2fa0"
    assert ids["hubspot_form_id"] == FORM
    assert ids["linked_profile_id"] == "21699"
    assert ids["linked_task_id"] == "1036"
    assert ids["linked_from_submission_id"] == "6a31606e"


def test_a_second_live_run_links_nothing(reconcile):
    run_reconcile(reconcile, dry_run=False)
    second, _hub = run_reconcile(reconcile, dry_run=False)

    assert second["links"] == []
    assert len(reconcile["links"]) == 2, "no new rows"


def test_processed_rises_by_exactly_the_links(reconcile):
    before = len(reconcile["processed"])
    out, _hub = run_reconcile(reconcile, dry_run=False)

    assert len(reconcile["processed"]) - before == len(out["written"]) == 2


def test_the_leftovers_are_bucketed_and_never_linked(reconcile):
    out, _hub = run_reconcile(reconcile, dry_run=True)

    whys = {u["submission_id"]: u["why"] for u in out["unlinked"]}
    assert whys["nobody01"] == dr.NO_PROFILE
    assert whys["loner001"] == dr.NO_TASK


def test_more_than_one_profile_is_never_linked(reconcile):
    reconcile["csuite"].profiles[EMAIL_TASKED] = ["21696", "21697"]

    out, _hub = run_reconcile(reconcile, dry_run=True)

    assert "62ed3c5d" not in {l["submission_id"] for l in out["links"]}
    assert any(u["submission_id"] == "62ed3c5d" and
               u["why"] == dr.MANY_PROFILES for u in out["unlinked"])


def test_a_task_claimed_by_two_submissions_links_neither(reconcile):
    """The stop condition, enforced at run time."""
    reconcile["csuite"].profiles[EMAIL_NOTASK] = ["21696"]   # also 1033's

    out, _hub = run_reconcile(reconcile, dry_run=True)

    linked = {l["submission_id"] for l in out["links"]}
    assert "62ed3c5d" not in linked and "loner001" not in linked
    assert sum(1 for u in out["unlinked"]
               if u["why"] == dr.AMBIGUOUS_TASK) == 2


def test_a_search_failure_is_reported_not_linked(reconcile, monkeypatch):
    monkeypatch.setattr(dr, "profiles_for",
                        lambda csuite, email: ([], "HTTP 500"))

    out, _hub = run_reconcile(reconcile, dry_run=True)

    assert out["links"] == []
    assert all(dr.SEARCH_FAILED in u["why"] for u in out["unlinked"])


# --- the earlier-submission rule, only under its conditions -----------------

def test_a_later_submission_from_a_handled_donor_is_not_suppressed(
        reconcile):
    """A LATER submission may be a new request. Linking it would hide real
    work, so it is reported instead."""
    subs = subs_fixture()
    subs[2]["submittedAt"] = 1791000000000          # 6fff2fa0 now AFTER

    out = dr.run(hubspot=ReconcileHub(subs), csuite=reconcile["csuite"],
                 dry_run=True, form_id=FORM)

    assert "6fff2fa0" not in {l["submission_id"] for l in out["links"]}
    assert any(u["submission_id"] == "6fff2fa0" and
               u["why"] == dr.LATER_SUBMISSION for u in out["unlinked"])


def test_no_earlier_link_without_a_processed_submission(reconcile):
    reconcile["processed"].clear()

    out, _hub = run_reconcile(reconcile, dry_run=True)

    assert not any(l["reason"] == dr.LINK_EARLIER_SUBMISSION
                   for l in out["links"])


def test_no_earlier_link_when_the_handled_donor_has_no_task(reconcile,
                                                           monkeypatch):
    """Processed but never followed up: not "handled" in the sense the rule
    needs."""
    monkeypatch.setattr(dr, "legacy_tasks", lambda form_id: ["1033"])

    out, _hub = run_reconcile(reconcile, dry_run=True)

    assert "6fff2fa0" not in {l["submission_id"] for l in out["links"]}


def test_no_earlier_link_when_the_order_cannot_be_told(reconcile):
    """The handled submission is not on page 1, so which came first is
    unknown."""
    subs = [s for s in subs_fixture() if s["conversionId"] != "6a31606e"]

    out = dr.run(hubspot=ReconcileHub(subs), csuite=reconcile["csuite"],
                 dry_run=True, form_id=FORM)

    assert any(u["submission_id"] == "6fff2fa0" and
               u["why"] == dr.NOT_ORDERABLE for u in out["unlinked"])


def test_no_earlier_link_with_more_than_one_profile(reconcile):
    reconcile["csuite"].profiles[EMAIL_EARLY] = ["21699", "21700"]

    out, _hub = run_reconcile(reconcile, dry_run=True)

    assert "6fff2fa0" not in {l["submission_id"] for l in out["links"]}


# --- the guard against migration 007 not being run --------------------------

def test_the_live_run_refuses_before_migration_007(reconcile, monkeypatch):
    """Checked first, not discovered as a CheckViolation part way through —
    the lesson of migration 006."""
    monkeypatch.setattr(dr, "link_rows_allowed", lambda: False)

    with pytest.raises(dr.ReconcileRefused) as refused:
        run_reconcile(reconcile, dry_run=False)

    assert "migrations/007" in str(refused.value)
    assert reconcile["links"] == []


def test_the_dry_run_needs_no_migration(reconcile, monkeypatch):
    monkeypatch.setattr(dr, "link_rows_allowed", lambda: False)

    out, _hub = run_reconcile(reconcile, dry_run=True)

    assert len(out["links"]) == 2


def test_the_constraint_check_reads_the_real_definition(monkeypatch):
    monkeypatch.setattr(
        "clients.database.execute_query",
        lambda *a, **k: [{"def": "CHECK ((target_system = ANY (ARRAY["
                                 "'hubspot'::text, 'csuite'::text, "
                                 "'jidhr'::text])))"}])
    assert dr.link_rows_allowed() is True

    monkeypatch.setattr(
        "clients.database.execute_query",
        lambda *a, **k: [{"def": "CHECK ((target_system = ANY (ARRAY["
                                 "'hubspot'::text, 'csuite'::text])))"}])
    assert dr.link_rows_allowed() is False


# --- the chat command --------------------------------------------------------

def test_the_command_is_recognised_and_dry_run_is_explicit(monkeypatch):
    seen = {}
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(dr, "run", lambda **k: seen.update(k) or {
        "dry_run": k["dry_run"], "links": [], "unlinked": [], "fetched": 0,
        "processed_before": 0, "unprocessed": 0, "written": []})

    sync_commands.handle("reconcile daf tasks dry run", None)
    assert seen["dry_run"] is True

    sync_commands.handle("reconcile daf tasks", None)
    assert seen["dry_run"] is False


def test_the_munira_list_masks_every_address(monkeypatch):
    out = {"dry_run": True, "fetched": 50, "processed_before": 5,
           "unprocessed": 1, "links": [], "written": [], "csuite_reads": 1,
           "unlinked": [{"submission_id": "x", "submitted_at": 1790000000000,
                         "email": "donor.person@private.invalid",
                         "why": dr.NO_PROFILE}]}

    reply = sync_commands._format_daf_reconcile(out)

    assert "donor.person@private.invalid" not in reply
    assert "d***@private.invalid" in reply
    assert "Not sent anywhere" in reply
    assert "0 CSuite writes, 0 HubSpot writes" in reply


def test_sync_all_cannot_reach_the_reconcile():
    for phrase in sync_commands.ALL_SYNC_PHRASES:
        assert not any(p in phrase
                       for p in sync_commands.DAF_RECONCILE_PHRASES)


# ---------------------------------------------------------------------------
# What did not move
# ---------------------------------------------------------------------------

def test_the_protected_modules_have_zero_diff():
    import subprocess

    found = subprocess.run(
        ["git", "diff", "--stat", "4cfe93a", "--",
         "sync/registrations.py", "sync/contact_create.py",
         "clients/notify.py", "sync/scheduled.py",
         "jobs/handlers/registrations_sync.py"],
        capture_output=True, text=True)
    assert found.returncode == 0, found.stderr
    assert found.stdout.strip() == "", found.stdout


def test_no_daf_create_was_automated():
    """CHANGE 1 records an id; it adds no trigger. Nothing outside the chat
    flow calls the profile or task create."""
    import subprocess

    found = subprocess.run(
        ["grep", "-rln", "--include=*.py", "-e", "create_individual_profile(",
         "-e", "create_task(", "jobs/", "sync/"],
        capture_output=True, text=True)
    assert found.stdout.strip() == "", found.stdout
