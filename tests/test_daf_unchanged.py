"""V5.81 Part 2: DAF moves onto Config.INQUIRY_FORMS row 1 and does not change.

The golden file tests/fixtures/daf_v580_golden.json was recorded by
tests/daf_snapshot.py against the V5.80 DAF path, before Part 2 touched it.
Every scenario here is replayed against the current code and compared:

* every reply, character for character, including the known "Not
  specified" fund name / contribution (hotfix-63, deliberately not fixed);
* every CSuite request body, BYTE for byte, through the real _request;
* every audit reservation's endpoint and payload_meta;
* the "reconcile daf tasks dry run" reply on the V5.79 two-link fixture.

What is ALLOWED to change is how the submissions are fetched: every page now,
not limit=5. That is asserted separately.

No network, no database. Every client below is a test double, apart from
CSuiteClient._request, which is real with its transport faked.
"""

import ast
import json
import subprocess
from pathlib import Path

import pytest

from clients import audit
from config import Config
from intents import daf_workflow, sync_commands
from sync import daf_reconcile as dr
from tests import daf_snapshot

GOLDEN = json.loads(
    (Path(__file__).parent / "fixtures" / "daf_v580_golden.json").read_text())
FORM = Config.DAF_INQUIRY_FORM_ID
BASE = "de92baa"                         # V5.80


@pytest.fixture(scope="module")
def recorded():
    mp = pytest.MonkeyPatch()
    try:
        return daf_snapshot.record(mp)
    finally:
        mp.undo()


SCENARIOS = ("new_donor", "returning_donor", "create_off", "task_off")


# --- byte-identical bodies, replies and audit rows --------------------------

@pytest.mark.parametrize("scenario", SCENARIOS)
def test_every_csuite_request_body_is_byte_identical(recorded, scenario):
    now = [(s["endpoint"], s["raw"]) for s in recorded[scenario]["sent"]]
    then = [(s["endpoint"], s["raw"]) for s in GOLDEN[scenario]["sent"]]
    assert now == then


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_every_reply_is_identical(recorded, scenario):
    assert recorded[scenario]["replies"] == GOLDEN[scenario]["replies"]


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_every_audit_reservation_is_identical(recorded, scenario):
    assert recorded[scenario]["audit"] == GOLDEN[scenario]["audit"]


def test_the_profile_and_task_bodies_are_the_ones_expected():
    """Spelled out once, so the golden file is not the only statement of
    what DAF sends."""
    bodies = {s["endpoint"]: s["body"] for s in GOLDEN["new_donor"]["sent"]}
    assert sorted(bodies["profile/create/individual"]) == [
        "address", "email", "env", "epoch", "first_name", "last_name",
        "phone_number"]
    assert sorted(bodies["task/create"]) == [
        "due_ts", "employee_id", "env", "epoch", "id", "name", "o",
        "task_description", "task_type_id"]
    assert bodies["task/create"]["task_type_id"] == 1065
    assert bodies["task/create"]["employee_id"] == 1012


def test_the_known_not_specified_bug_is_still_there(recorded):
    """hotfix-63, not V5.81. Pinned so a fix is a deliberate change."""
    offer = recorded["new_donor"]["replies"][0]
    assert "💰 **Requested Fund Name:** Not specified" in offer
    assert "💵 **Initial Contribution:** Not specified" in offer


# --- the new daf reply -------------------------------------------------------

def test_the_new_daf_reply_is_the_v580_reply(recorded):
    assert recorded["new_donor"]["replies"][0] == \
        GOLDEN["new_donor"]["replies"][0]
    assert recorded["new_donor"]["replies"][0].startswith(
        "📋 **New DAF Inquiry**")


# --- processed_submissions ---------------------------------------------------

def test_the_chat_flow_asks_for_daf_history_exactly_as_before(monkeypatch):
    seen = []
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda *a, **k: seen.append((a, k)) or {})
    daf_workflow._initiate_workflow("new daf",
                                    daf_workflow.default_workflow_state(),
                                    daf_snapshot.Hub())
    assert seen == [((FORM,), {})]


def test_the_history_query_sends_the_same_params_for_daf(monkeypatch):
    captured = []
    monkeypatch.setattr("clients.database.is_configured", lambda: True)
    monkeypatch.setattr(audit, "_csuite_env_column_exists", lambda: True)
    monkeypatch.setattr("clients.database.execute_query",
                        lambda sql, params, fetch=True:
                        captured.append((sql, params)) or [])

    audit.processed_submissions(FORM)

    sql, params = captured[0]
    assert params == (FORM, ["profile/create/individual", "task/create"],
                      "jidhr", "submission/link/%")
    assert sql == audit._PROCESSED_SQL


def test_the_history_sql_is_the_v580_sql():
    then = subprocess.run(["git", "show", f"{BASE}:clients/audit.py"],
                          capture_output=True, text=True, check=True).stdout
    namespace = {}
    start = then.index("_PROCESSED_SQL = ")
    exec(then[start:then.index('"""', then.index('"""', start) + 3) + 3],
         namespace)
    assert namespace["_PROCESSED_SQL"] == audit._PROCESSED_SQL


def test_the_reconcile_asks_for_daf_history_exactly_as_before(monkeypatch):
    seen = []
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda *a, **k: seen.append((a, k)) or {})
    monkeypatch.setattr(dr, "legacy_tasks", lambda form_id: [])
    monkeypatch.setattr(dr, "tasked_submissions", lambda form_id: set())
    dr.plan(daf_snapshot.Hub([]), None, FORM)
    assert seen == [((FORM,), {})]


# --- the reconcile -----------------------------------------------------------

def test_the_reconcile_dry_run_reply_is_identical(monkeypatch):
    now = daf_snapshot.record_reconcile(monkeypatch)
    assert now == GOLDEN["reconcile_dry_run"]
    assert "| `62ed3c5d` |" in now["reply"]
    assert "| `6fff2fa0` |" in now["reply"]


def test_reconcile_inquiries_daf_is_the_same_run(monkeypatch):
    """The alias and the general command run the same thing; only the
    heading and the "say ..." line name the command that was typed."""
    seen = []
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(dr, "run", lambda **k: seen.append(k) or {
        "dry_run": k["dry_run"], "form_id": k["form_id"], "links": [],
        "unlinked": [], "fetched": 0, "processed_before": 0,
        "unprocessed": 0, "written": []})

    sync_commands.handle("reconcile daf tasks dry run", None)
    sync_commands.handle("reconcile inquiries daf dry run", None)
    sync_commands.handle("reconcile inquiries daf", None)

    assert seen == [{"dry_run": True, "form_id": FORM},
                    {"dry_run": True, "form_id": FORM},
                    {"dry_run": False, "form_id": FORM}]


def test_reconcile_inquiries_needs_a_known_form(monkeypatch):
    monkeypatch.setattr(dr, "run", lambda **k: pytest.fail("ran"))
    reply = sync_commands.handle("reconcile inquiries nonsense dry run", None)
    assert "Which form?" in reply and "`daf`" in reply


def test_both_reconcile_commands_are_routed():
    for phrase in ("reconcile daf tasks dry run",
                   "reconcile inquiries daf dry run"):
        assert sync_commands.can_handle(phrase)
    for phrase in sync_commands.ALL_SYNC_PHRASES:
        assert not any(p in phrase
                       for p in sync_commands.INQUIRY_RECONCILE_PHRASES)


def test_the_legacy_task_query_is_filtered_by_form(monkeypatch):
    captured = []
    monkeypatch.setattr("clients.database.execute_query",
                        lambda sql, params, fetch=True:
                        captured.append((sql, params)) or [])

    dr.legacy_tasks(FORM)
    dr.legacy_tasks(Config.ENDOWMENT_INQUIRY_FORM_ID)
    dr.tasked_submissions(FORM)

    assert "hubspot_form_id" in captured[0][0]
    # Untagged (pre-V5.79) rows are DAF's: the COALESCE default is DAF.
    assert captured[0][1] == (FORM, FORM)
    assert captured[1][1] == (FORM, Config.ENDOWMENT_INQUIRY_FORM_ID)
    assert "hubspot_form_id" in captured[2][0]
    assert captured[2][1] == (FORM,)


# --- the switches ------------------------------------------------------------

def test_the_daf_row_names_the_v580_switches_and_values():
    row = daf_workflow.inquiry_form("daf")
    assert row["form_id"] == FORM
    assert row["create_flag"] == "CSUITE_DAF_CREATE_ENABLED"
    assert row["task_flag"] == "CSUITE_DAF_TASK_CREATE_ENABLED"
    assert row["assignee_env"] == "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY"
    assert row["task_type_env"] == "CSUITE_TASK_TYPE_ID"
    assert row["task_type_id"] is None
    assert row["fund_group"] == Config.FUND_GROUP_DAF == 1002
    assert row["profile_kind"] == "individual"
    assert row["profile_endpoint"] == "profile/create/individual"
    assert daf_workflow.handled_endpoints(row) is None


@pytest.mark.parametrize("create,task", [(True, True), (True, False),
                                         (False, True), (False, False)])
def test_the_daf_switches_gate_daf_as_before(monkeypatch, create, task):
    out = daf_snapshot._create(monkeypatch, "fresh001-bbbb", create=create,
                               task=task)
    endpoints = [s["endpoint"] for s in out["sent"]]
    assert ("profile/create/individual" in endpoints) is create
    assert ("task/create" in endpoints) is (create and task)


def test_the_task_type_is_read_when_the_task_is_made(monkeypatch):
    row = daf_workflow.inquiry_form("daf")
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", None)
    assert daf_workflow.form_task_type(row) is None
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", 1065)
    assert daf_workflow.form_task_type(row) == 1065


# --- what DID change: every page is read ---------------------------------------

def _many(n):
    return [daf_snapshot.submission(f"s{i:04d}", f"P{i}", "X",
                                    f"p{i}@example.invalid",
                                    1900000000000 - i * 1000)
            for i in range(n)]


def test_every_page_is_read_newest_first(monkeypatch):
    subs = _many(90)
    processed = {s["conversionId"]: "1" for s in subs[:60]}
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: dict(processed))
    hub = daf_snapshot.Hub(subs)
    state = daf_workflow.default_workflow_state()

    reply = daf_workflow._initiate_workflow("new daf", state, hub)

    assert [c[3] for c in hub.calls] == [None, "50"]
    assert all(c[:3] == ("form", FORM, 50) for c in hub.calls)
    assert state["submission_index"] == 60
    # Submission 61 is on page 2; limit=5 could never have reached it.
    assert state["submission_data"]["submission_id"] == "s0060"
    assert "**Name:** P60 X" in reply


def test_a_page_error_is_an_error_not_a_short_list(monkeypatch):
    class Failing(daf_snapshot.Hub):
        def get_form_submissions(self, form_id, limit=50, after=None):
            if after:
                return {"error": "HTTP 502"}
            return super().get_form_submissions(form_id, limit, after)

    reply = daf_workflow._initiate_workflow(
        "new daf", daf_workflow.default_workflow_state(), Failing(_many(90)))
    assert reply == "❌ Failed to fetch daf inquiry submissions: HTTP 502"


def test_a_cursor_that_never_ends_is_stopped_and_said(monkeypatch):
    class Endless(daf_snapshot.Hub):
        def get_form_submissions(self, form_id, limit=50, after=None):
            n = int(after or 0)
            return {"results": [daf_snapshot.submission(
                        f"e{n}", "E", "X", "e@example.invalid",
                        1900000000000 - n)],
                    "paging": {"next": {"after": str(n + 1)}}}

    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: {})
    reply = daf_workflow._initiate_workflow(
        "new daf", daf_workflow.default_workflow_state(), Endless([]))
    assert f"Only the newest {daf_workflow.MAX_SUBMISSION_PAGES}" in reply


def test_the_first_page_request_is_unchanged():
    """No cursor means no `after` parameter at all."""
    from clients.hubspot import HubSpotClient

    seen = []

    class Client(HubSpotClient):
        def __init__(self):
            pass

        def _get(self, path, params=None):
            seen.append((path, params))
            return {}

    Client().get_form_submissions(FORM, limit=50)
    Client().get_form_submissions(FORM, limit=50, after="abc")
    assert seen[0][1] == {"limit": 50}
    assert seen[1][1] == {"limit": 50, "after": "abc"}


# --- git-diff pin: the DAF-only code paths ---------------------------------------

def _functions(source: str) -> dict:
    tree = ast.parse(source)
    found = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            found.setdefault(node.name, ast.dump(node))
    return found


def _at_base(path):
    return subprocess.run(["git", "show", f"{BASE}:{path}"],
                          capture_output=True, text=True, check=True).stdout


# Functions the DAF path runs that V5.81 must not change. Compared as ASTs,
# so a comment can change and code cannot.
PINNED = {
    "intents/daf_workflow.py": [
        "already_in_csuite", "existing_hubspot_link", "_backfill_hubspot_link",
        "_format_confirmation", "_task_lines", "open_inquiry_tickets",
        "task_due_date", "record_unprocessed_submission", "says_yes",
        "says_no", "_render_submission", "_skipped_note",
        "_all_processed_message", "_next_unprocessed", "_is_processed",
        "is_newest_first", "format_submitted_at", "submitted_address",
        "_log_skipped_create", "_hubspot_contact_id",
        # _skip_to_next is NOT pinned: it re-reads the list since V5.81
        # (the session is a cookie). Its replies are pinned by the golden
        # returning_donor scenario, which skips once.
    ],
    "clients/csuite.py": ["create_individual_profile", "create_task",
                          "build_address", "check_input_fields"],
    "clients/audit.py": ["processed_submissions", "payload_meta",
                         "record_write"],
    "sync/daf_reconcile.py": ["task_profile", "profiles_for"],
}


@pytest.mark.parametrize("path", sorted(PINNED))
def test_the_daf_only_functions_have_not_changed(path):
    then = _functions(_at_base(path))
    now = _functions(Path(path).read_text())
    changed = [name for name in PINNED[path] if then[name] != now[name]]
    assert changed == [], f"{path}: {changed} changed since {BASE}"


def test_the_daf_dependencies_have_zero_diff():
    found = subprocess.run(
        ["git", "diff", "--stat", BASE, "--", "sync/filter_trust.py",
         "sync/profile_state.py", "sync/readback.py", "sync/sandbox_writes.py"],
        capture_output=True, text=True)
    assert found.returncode == 0, found.stderr
    assert found.stdout.strip() == "", found.stdout


# --- the session is a cookie ---------------------------------------------------

def _realistic(n):
    import random
    import string
    import uuid

    rnd = random.Random(7)

    def w(k):
        return "".join(rnd.choice(string.ascii_lowercase) for _ in range(k))

    return [{"conversionId": str(uuid.UUID(int=rnd.getrandbits(128))),
             "submittedAt": 1900000000000 - i * 86400000,
             "values": [{"name": k, "value": v} for k, v in {
                 "firstname": w(7).title(), "lastname": w(9).title(),
                 "email": f"{w(10)}@{w(8)}.invalid",
                 "phone": "".join(rnd.choice("0123456789")
                                  for _ in range(10)),
                 "address": f"{rnd.randint(1, 9999)} {w(8).title()} St",
                 "city": w(8).title(), "state": "VA",
                 "zip": str(rnd.randint(10000, 99999))}.items()]}
            for i in range(n)]


def test_ninety_submissions_still_fit_in_the_session_cookie(monkeypatch):
    """The workflow state is saved in the Flask session cookie, and a browser
    drops one over 4,096 bytes silently. Keeping the paged list in state
    measured 12,419 bytes for 90 realistic submissions."""
    from flask import Flask
    from flask.sessions import SecureCookieSessionInterface

    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: {})
    state = daf_workflow.default_workflow_state()
    daf_workflow._initiate_workflow("new daf", state,
                                    daf_snapshot.Hub(_realistic(90)))

    app = Flask(__name__)
    app.secret_key = "k" * 64
    signer = SecureCookieSessionInterface().get_signing_serializer(app)
    cookie = signer.dumps({"workflow_state": state, "_user_id": "1",
                           "_fresh": True, "csrf_token": "t" * 40})
    assert state["active"] is True
    assert state["submissions"] == []
    assert len(cookie) < 1500


def test_skip_re_reads_and_finds_the_shown_one_by_id(monkeypatch):
    """A submission that arrives between the offer and the skip moves every
    index down by one. Skip finds the shown submission by id."""
    subs = _many(3)
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: {})
    hub = daf_snapshot.Hub(subs)
    state = daf_workflow.default_workflow_state()
    daf_workflow._initiate_workflow("new daf", state, hub)
    assert state["submission_data"]["submission_id"] == "s0000"

    hub.subs = [daf_snapshot.submission("brandnew", "N", "X",
                                        "n@example.invalid",
                                        1900000005000)] + subs
    reply = daf_workflow._handle_active_workflow("skip", state, hub, None)

    assert state["submission_data"]["submission_id"] == "s0001"
    assert "Submission 3 of 4." in reply


def test_skip_skips_one_handled_since_the_offer(monkeypatch):
    processed = {}
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: dict(processed))
    hub = daf_snapshot.Hub(_many(3))
    state = daf_workflow.default_workflow_state()
    daf_workflow._initiate_workflow("new daf", state, hub)

    processed["s0001"] = "21700"          # handled in another session
    daf_workflow._handle_active_workflow("skip", state, hub, None)

    assert state["submission_data"]["submission_id"] == "s0002"


def test_a_failed_re_read_skips_nothing(monkeypatch):
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: {})
    hub = daf_snapshot.Hub(_many(3))
    state = daf_workflow.default_workflow_state()
    daf_workflow._initiate_workflow("new daf", state, hub)

    class Down(daf_snapshot.Hub):
        def get_form_submissions(self, *a, **k):
            return {"error": "HTTP 503"}

    reply = daf_workflow._handle_active_workflow("skip", state, Down([]),
                                                 None)
    assert "Nothing was skipped" in reply
    assert state["active"] is True
    assert state["submission_data"]["submission_id"] == "s0000"


# --- V5.82: the reconcile honours each form's own switch ----------------------

@pytest.mark.parametrize("command", ["reconcile daf tasks dry run",
                                     "reconcile daf tasks",
                                     "reconcile inquiries daf dry run",
                                     "reconcile inquiries daf"])
def test_the_daf_reconcile_refuses_when_daf_is_off(monkeypatch, command):
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", False)
    monkeypatch.setattr(dr, "run", lambda **k: pytest.fail("it ran"))
    reply = sync_commands.handle(command, None)
    assert "DAF inquiries are turned off" in reply
    assert "CSUITE_DAF_CREATE_ENABLED" in reply


@pytest.mark.parametrize("command", ["reconcile inquiries endowment dry run",
                                     "reconcile inquiries endowment"])
def test_the_endowment_reconcile_obeys_the_kill_switch(monkeypatch, command):
    """V5.81 gap: the kill switch could be walked round through this
    command. Off refuses before anything is read — even with DAF on."""
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_RAW", "")
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_ENABLED", False)
    monkeypatch.setattr(dr, "run", lambda **k: pytest.fail("it ran"))
    reply = sync_commands.handle(command, None)
    assert "Endowment inquiries are turned off" in reply


def test_the_endowment_reconcile_runs_when_switched_on(monkeypatch):
    seen = []
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_RAW", "true")
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_ENABLED", True)
    monkeypatch.setattr(dr, "run", lambda **k: seen.append(k) or {
        "dry_run": k["dry_run"], "form_id": k["form_id"], "links": [],
        "unlinked": [], "fetched": 0, "processed_before": 0,
        "unprocessed": 0, "written": []})
    sync_commands.handle("reconcile inquiries endowment dry run", None)
    assert seen == [{"dry_run": True,
                     "form_id": Config.ENDOWMENT_INQUIRY_FORM_ID}]
