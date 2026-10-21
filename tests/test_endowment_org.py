"""V5.81 Part 3: the endowment row makes an ORGANISATION profile.

Every value here is invented. The EIN, goal and essays are obviously fake
markers, so a test can search logs and audit rows for them. The CSuite client
is the REAL CSuiteClient._request with its transport faked; HubSpot, the
mirror and the audit store are test doubles. No network, no database.
"""

import json
import logging
from datetime import datetime, timezone

import pytest

from clients import audit
from clients.csuite import CSuiteClient
from config import Config
from intents import daf_workflow, endowment_inquiry, mark_handled
from sync import org_match
from sync.sandbox_writes import WriteBudget

FORM = Config.ENDOWMENT_INQUIRY_FORM_ID
DAF = Config.DAF_INQUIRY_FORM_ID
EIN = "00-FAKE-EIN-0042"
GOAL = "FAKEGOAL-250000"
ESSAYS = {"what_is_your_organization_s_mission_and_core_services_programs_":
          "FAKEMISSION " * 20,
          "what_prompted_your_organization_s_interest_in_starting_an_"
          "endowment_": "FAKEINTEREST " * 20,
          "what_are_your_organization_s_main_endowment_goals_":
          "FAKEGOALS " * 20}
SECRETS = (EIN, GOAL, "FAKEMISSION", "FAKEINTEREST", "FAKEGOALS")
FIXED_EPOCH = 1800000000


def endowment_submission(conv="endw0001-aaaa-bbbb", org="Test Peace Academy",
                         email="director@example.invalid",
                         ms=1900000000000, **overrides):
    contact = {"firstname": "Dana", "lastname": "Director", "email": email,
               "phone": "(703) 555-0123",
               "how_did_you_hear_about_us_": "FAKEHEARD"}
    company = {"name": org, "organization_ein": EIN,
               "address": "9 Org Plaza", "city": "Herndon", "state": "VA",
               "zip": "20170", "website": "https://org.example.invalid",
               "what_is_your_endowment_starting_goal_": GOAL, **ESSAYS}
    company.update(overrides)
    values = ([{"name": k, "value": v, "objectTypeId": "0-1"}
               for k, v in contact.items()]
              + [{"name": k, "value": v, "objectTypeId": "0-2"}
                 for k, v in company.items()])
    return {"conversionId": conv, "submittedAt": ms, "values": values}


class Hub:
    """HubSpot double: per-form submission lists, one page each."""

    def __init__(self, by_form=None, contact_profile=None):
        self.by_form = by_form if by_form is not None else {
            FORM: [endowment_submission()], DAF: []}
        self.contact_profile = contact_profile
        self.calls = []

    def get_form_submissions(self, form_id, limit=50, after=None):
        self.calls.append(form_id)
        return {"results": list(self.by_form.get(form_id, []))}

    def search_contact_by_email(self, email, properties=None):
        props = ({"csuite_profile_id": self.contact_profile}
                 if self.contact_profile else {})
        return {"results": [{"id": "70999", "properties": props}]}

    def __getattr__(self, name):
        raise AssertionError(f"unexpected HubSpot call: {name}()")


class Client(CSuiteClient):
    """The real _request. `existing` maps an email to [(profile_id, ptype)]."""

    def __init__(self, existing=None, fedids=None, unreadable=()):
        self.api_key, self.api_secret = "k", "s"
        self.base_url = "https://amuslimcf.fcsuite.com/api/v2"
        self.env = "live"
        self.verify_writes = False
        self.write_budget = WriteBudget(5)
        self.sent = []
        outer, existing = self, existing or {}
        ptypes = {str(pid): pt for rows in existing.values()
                  for pid, pt in rows}
        # What profile/display says each profile's fedid is. Updated by a
        # successful profile/edit, as CSuite would.
        self.fedids = {str(k): v for k, v in (fedids or {}).items()}
        self.notes = []
        self.note_reply = {"note_id": 303200, "success": 0}
        unreadable = {str(u) for u in unreadable}

        class Session:
            def post(_self, url, data=None, **kwargs):
                endpoint = url.split("/api/v2/", 1)[-1]
                body = json.loads(data)
                outer.sent.append({"endpoint": endpoint, "body": body,
                                   "raw": data})
                if endpoint == "profile/list":
                    email = body.get("primary_email")
                    if email is None:
                        reply = {"count": 18797, "results": []}
                    elif str(email).startswith("filter-probe-"):
                        reply = {"count": 0, "results": []}
                    else:
                        rows = existing.get(email, [])
                        reply = {"count": len(rows), "results": [
                            {"profile_id": pid} for pid, _pt in rows]}
                    payload = {"success": 1, "data": reply}
                elif endpoint == "profile/display":
                    pid = str(body.get("profile_id"))
                    if pid in unreadable:
                        payload = {"success": 0,
                                   "errors": ["Internal server error"]}
                    else:
                        payload = {"success": 1, "data": {
                            "profile_id": int(pid),
                            "ptype": ptypes.get(pid, "org"),
                            "fedid": outer.fedids.get(pid)}}
                elif endpoint == "profile/edit":
                    outer.fedids[str(body["profile_id"])] = body.get("fedid")
                    payload = {"success": 1,
                               "data": {"profile_id": body["profile_id"]}}
                elif endpoint == "profile/create/org":
                    payload = {"success": 1, "data": {"profile_id": 40001}}
                elif endpoint == "task/create":
                    payload = {"success": 1, "data": {"task_id": 2001}}
                elif endpoint == "note/create":
                    # CSuite's real reply for a note it DID create (sandbox,
                    # 2026-10-15): a note_id, and success 0.
                    outer.notes.append(body)
                    payload = outer.note_reply
                else:
                    raise AssertionError(f"unexpected CSuite {endpoint}")

                class R:
                    status_code = 200
                    text = json.dumps(payload)

                    def json(self):
                        return payload
                return R()

        self.session = Session()

    def endpoints(self):
        return [s["endpoint"] for s in self.sent]

    def body(self, endpoint):
        return next(s["body"] for s in self.sent if s["endpoint"] == endpoint)


MIRROR_SYNCED = datetime(2026, 9, 23, 19, 52, tzinfo=timezone.utc)


@pytest.fixture
def world(monkeypatch):
    """Switch on, assignee confirmed, empty history, a mirror with orgs."""
    import clients.csuite as mod

    state = {"audit": [], "records": [], "processed": {FORM: {}, DAF: {}},
             "tasks": {FORM: {}, DAF: {}},
             "mirror": [{"profile_id": "9648", "organization":
                         "Unrelated Charity Trust", "ptype": "org"}]}
    monkeypatch.setattr(Config, "CSUITE_ENV", "live")
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_RAW", "true")
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_ASSIGNEE", 1002)
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", True)

    def reserve(system, method, endpoint, target_id=None, payload=None,
                sync_run_id=None):
        state["audit"].append({"endpoint": endpoint,
                               "meta": audit.payload_meta(payload)})
        return {"id": len(state["audit"])}

    monkeypatch.setattr(mod, "reserve_write", reserve)
    monkeypatch.setattr(mod, "complete_write", lambda *a, **kw: None)
    monkeypatch.setattr(mod.time, "time", lambda: FIXED_EPOCH)
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: dict(state["processed"][form_id]))
    monkeypatch.setattr(audit, "handled_tasks",
                        lambda form_id: dict(state["tasks"][form_id]))
    monkeypatch.setattr(org_match, "mirror_org_rows",
                        lambda: (list(state["mirror"]), MIRROR_SYNCED))

    def record(system, method, endpoint, target_id=None, payload=None,
               status="success", **kwargs):
        state["records"].append({"system": system, "endpoint": endpoint,
                                 "meta": audit.payload_meta(payload)})
        ids = (payload or {})
        state["processed"][ids["hubspot_form_id"]][
            ids["hubspot_submission_id"]] = None
        return True

    monkeypatch.setattr(audit, "record_write", record)
    monkeypatch.setattr("sync.daf_reconcile.link_rows_allowed", lambda: True)
    return state


def offer_and_create(hub=None, client=None, second="yes"):
    """Review, first "yes" (the check), and — when it asks — `second`.

    Returns the LAST reply. The plan shown between the two is on
    client.plan_reply, and the CSuite requests made before the second
    answer on client.before_second.
    """
    hub = hub or Hub()
    client = client or Client()
    state = daf_workflow.default_workflow_state()
    review = daf_workflow._initiate_workflow("new endowment", state, hub)
    reply = daf_workflow._handle_active_workflow("yes", state, hub, client)
    client.plan_reply, client.before_second = None, list(client.endpoints())
    if state.get("step") == "confirm_write" and second is not None:
        client.plan_reply = reply
        reply = daf_workflow._handle_active_workflow(second, state, hub,
                                                     client)
    return review, reply, state, client


WRITES = ("profile/create/org", "profile/create/individual", "task/create",
          "note/create", "profile/edit")


# --- the profile is an ORGANISATION, with the organisation's address --------

def test_the_org_profile_body(world):
    _review, reply, state, client = offer_and_create()

    # The check (3 reads) at the first yes, again at the second, then the
    # two writes.
    check = ["profile/list", "profile/list", "profile/list"]
    assert client.endpoints() == check + check + ["profile/create/org",
                                                  "note/create",
                                                  "task/create"]
    body = client.body("profile/create/org")
    assert body == {
        "env": "live", "epoch": FIXED_EPOCH,
        "organization": "Test Peace Academy",
        "email": "director@example.invalid",
        "phone_number": "7035550123",
        "address": {"address": "9 Org Plaza", "city": "Herndon",
                    "state": "VA", "zipcode": "20170"},
        "website": "https://org.example.invalid",
        "fedid": EIN}
    assert state["profile_id"] == 40001
    assert "Organisation Profile Created" in reply


def test_no_individual_profile_call_ever(world):
    for existing in ({}, {"director@example.invalid": [(555, "indiv")]},
                     {"director@example.invalid": [(556, "org")]}):
        _r, _reply, _s, client = offer_and_create(client=Client(existing))
        assert "profile/create/individual" not in client.endpoints()


def test_the_address_is_the_organisations_and_never_the_persons(world):
    parsed = daf_workflow._parse_submission(
        endowment_submission(),
        daf_workflow.inquiry_form("endowment")["field_map"])
    assert parsed["org_address_street"] == "9 Org Plaza"
    assert parsed["org_address_zip"] == "20170"
    for person_key in ("address_street", "address_city", "address_state",
                       "address_zip"):
        assert parsed[person_key] == ""


def test_a_contact_field_with_a_company_name_is_not_read_as_the_company():
    """The map names the object. A contact-object "address" is not the
    organisation's address."""
    sub = endowment_submission()
    sub["values"] = [v for v in sub["values"] if v["name"] != "address"]
    sub["values"].append({"name": "address", "value": "1 Person Rd",
                          "objectTypeId": "0-1"})
    parsed = daf_workflow._parse_submission(
        sub, daf_workflow.inquiry_form("endowment")["field_map"])
    assert parsed["org_address_street"] == ""


TITLE = ("Endowment inquiry: Test Peace Academy — EIN 00-FAKE-EIN-0042 — "
         "goal FAKEGOAL-250000 — Dana Director (director@example.invalid)")


def test_the_task(world):
    _review, reply, _state, client = offer_and_create()
    body = client.body("task/create")

    assert body["employee_id"] == 1002
    assert body["task_type_id"] == 1065
    assert body["o"] == "profile" and body["id"] == 40001
    assert "📝 Follow-up task 2001 for 1002" in reply


def test_the_task_text_is_the_title_in_both_fields(world):
    """A CSuite task keeps ONE text: task_description is ignored when name
    is sent (sandbox, 2026-10-15). The same text goes in both, as DAF."""
    _review, _reply, _state, client = offer_and_create()
    body = client.body("task/create")

    assert body["name"] == TITLE
    assert body["task_description"] == TITLE
    for essay in ("FAKEMISSION", "FAKEINTEREST", "FAKEGOALS", "FAKEHEARD"):
        assert essay not in body["name"], "the essays stay in HubSpot"


def _data(**overrides):
    return daf_workflow._parse_submission(
        endowment_submission(**overrides),
        daf_workflow.inquiry_form("endowment")["field_map"])


def test_a_short_title_is_not_cut():
    assert endowment_inquiry.task_title(_data()) == TITLE


def test_a_long_goal_is_cut_to_40_first():
    goal = "We hope to raise " + "a great deal " * 30
    title = endowment_inquiry.task_title(_data(
        what_is_your_endowment_starting_goal_=goal))

    assert len(title) <= 255
    cut = title.split(" — goal ")[1].split(" — ")[0]
    assert len(cut) == 40 and cut.endswith("…")
    assert "Test Peace Academy" in title, "the org is not cut while goal can be"
    assert "EIN 00-FAKE-EIN-0042" in title
    assert "(director@example.invalid)" in title


def test_then_the_organisation_name_is_cut(world):
    org = "The Very Long Named Organisation " * 10
    title = endowment_inquiry.task_title(_data(
        name=org, what_is_your_endowment_starting_goal_="g" * 100))

    assert len(title) == 255
    assert title.split(" — goal ")[1].startswith("g" * 39 + "…")
    assert title.split(" — EIN ")[0].endswith("…")
    assert "EIN 00-FAKE-EIN-0042" in title
    assert title.endswith("Dana Director (director@example.invalid)")


@pytest.mark.parametrize("org_len,goal_len", [(400, 400), (1000, 5),
                                              (230, 0), (5, 1000)])
def test_the_ein_and_email_are_never_cut(org_len, goal_len):
    title = endowment_inquiry.task_title(_data(
        name="O" * org_len,
        what_is_your_endowment_starting_goal_="G" * goal_len))

    assert len(title) <= 255
    assert "— EIN 00-FAKE-EIN-0042 —" in title
    assert title.endswith("(director@example.invalid)")


def test_missing_values_are_said_not_dropped():
    title = endowment_inquiry.task_title(_data(
        organization_ein="", what_is_your_endowment_starting_goal_=""))
    assert "— EIN not given — goal not given —" in title


# --- EIN, goal and essays: never logged, never audited, never in the cookie ----

def test_ein_goal_and_essays_are_absent_from_logs_and_audit(world, caplog):
    with caplog.at_level(logging.DEBUG):
        offer_and_create()

    logged = "\n".join(r.getMessage() for r in caplog.records)
    audited = json.dumps(world["audit"])
    assert world["audit"], "the audit rows were captured"
    for secret in SECRETS:
        assert secret not in logged, f"{secret} reached a log line"
        assert secret not in audited, f"{secret} reached write_audit"


def test_the_session_never_holds_the_ein_goal_or_essays(world):
    hub = Hub()
    state = daf_workflow.default_workflow_state()
    daf_workflow._initiate_workflow("new endowment", state, hub)

    cookie = json.dumps(state)
    for secret in SECRETS:
        assert secret not in cookie
    assert state["submission_data"]["organization"] == "Test Peace Academy"


def test_the_audit_rows_carry_the_submission(world):
    offer_and_create()
    by_endpoint = {row["endpoint"]: row["meta"]["ids"]
                   for row in world["audit"]}
    assert by_endpoint["profile/create/org"]["hubspot_submission_id"] == \
        "endw0001-aaaa-bbbb"
    assert by_endpoint["task/create"]["linked_profile_id"] == 40001
    assert by_endpoint["task/create"]["hubspot_form_id"] == FORM


# --- the duplicate check -------------------------------------------------------

def test_an_exact_name_match_is_existing_and_gets_only_a_task(world):
    world["mirror"].append({"profile_id": "18075",
                            "organization": "The Test Peace Academy, Inc.",
                            "ptype": "org"})
    _review, reply, state, client = offer_and_create()

    assert "profile/create/org" not in client.endpoints()
    assert client.body("task/create")["id"] == 18075
    assert "Already in CSuite" in reply


def test_a_close_name_match_is_ambiguous_and_writes_nothing(world):
    world["mirror"].append({"profile_id": "18076",
                            "organization": "Test Peace Academy of Virginia",
                            "ptype": "org"})
    review, reply, state, client = offer_and_create()

    assert "AMBIGUOUS" in review
    assert "Test Peace Academy of Virginia (profile 18076)" in review
    assert not [e for e in client.endpoints() if "create" in e]
    assert "No profile and no task created" in reply
    assert "never linked automatically" in reply


def test_two_exact_name_matches_are_ambiguous(world):
    world["mirror"] += [{"profile_id": "1", "organization":
                         "Test Peace Academy", "ptype": "org"},
                        {"profile_id": "2", "organization":
                         "Test Peace Academy Foundation", "ptype": "org"}]
    _review, reply, _state, client = offer_and_create()
    assert not [e for e in client.endpoints() if "create" in e]


def test_an_individual_with_the_same_employer_name_is_not_compared():
    rows = [{"profile_id": "7", "organization": "Test Peace Academy",
             "ptype": "indiv"}]
    # mirror_org_rows asks for ptype 'org' only; the matcher is given what
    # it is given, so the SQL is what keeps individuals out.
    import inspect
    assert "'ptype' = %s" in inspect.getsource(org_match.mirror_org_rows)
    assert org_match.match_org_name("Test Peace Academy", rows)["exact"]


def test_an_email_on_an_individual_profile_is_ambiguous(world):
    client = Client({"director@example.invalid": [(555, "indiv")]})
    _review, reply, _state, client = offer_and_create(client=client)

    assert not [e for e in client.endpoints() if "create" in e]
    assert "INDIVIDUAL" in reply


def test_an_email_on_an_org_profile_is_existing(world):
    client = Client({"director@example.invalid": [(556, "org")]})
    _review, reply, _state, client = offer_and_create(client=client)

    assert "profile/create/org" not in client.endpoints()
    assert client.body("task/create")["id"] == 556


def test_an_unreadable_mirror_stops_the_create(world, monkeypatch):
    def down():
        raise org_match.MirrorUnavailable("the CSuite mirror could not be "
                                          "read: boom")
    monkeypatch.setattr(org_match, "mirror_org_rows", down)
    _review, reply, _state, client = offer_and_create()

    assert not [e for e in client.endpoints() if "create" in e]
    assert "could not be ruled out" in reply


def test_the_reply_says_how_old_the_mirror_is(world):
    review, reply, _state, _client = offer_and_create()
    for text in (review, reply):
        assert "Mirror data from Sep 23, 2026" in text
        assert "days old" in text


@pytest.mark.parametrize("a,b,same", [
    ("The Peace Foundation, Inc.", "peace", True),
    ("Peace & Light", "peace and light", True),
    ("Peace Academy", "Peace Academy Inc", True),
    ("Peace Academy", "Grace Academy", False),
])
def test_name_normalisation(a, b, same):
    assert (org_match.normalise_org_name(a)
            == org_match.normalise_org_name(b)) is same


# --- cross-form ----------------------------------------------------------------

def test_a_daf_inquiry_from_the_same_email_is_noted_not_blocking(world):
    daf_sub = {"conversionId": "daf00001", "submittedAt": 1890000000000,
               "values": [{"name": "email",
                           "value": "Director@example.invalid"}]}
    world["processed"][DAF] = {"daf00001": "21699"}
    world["tasks"][DAF] = {"daf00001": "1036"}
    hub = Hub({FORM: [endowment_submission()], DAF: [daf_sub]})

    review, reply, _state, client = offer_and_create(hub=hub)

    assert "Also a DAF inquiry" in review and "task 1036" in review
    assert "Also a DAF inquiry" in reply
    assert "profile/create/org" in client.endpoints(), "noted, not blocking"


# --- refusals ------------------------------------------------------------------

def test_refuses_when_the_assignee_is_unset(world, monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_ASSIGNEE", None)
    review, reply, state, client = offer_and_create()

    assert "Assignee not confirmed" in review
    assert "assignee not confirmed" in reply
    assert client.sent == []
    assert state["active"] is False


def test_refuses_when_the_flag_is_off(world, monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_RAW", "")
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_ENABLED", False)
    hub = Hub()
    state = daf_workflow.default_workflow_state()

    reply = daf_workflow._initiate_workflow("new endowment", state, hub)

    assert "turned off" in reply and hub.calls == []


def test_the_retired_assignee_variable_does_not_unlock_create(world,
                                                              monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_ASSIGNEE", None)
    monkeypatch.setenv("CSUITE_TASK_EMPLOYEE_ID_ENDOWMENT_INQUIRY", "1002")
    _review, reply, _state, client = offer_and_create()
    assert "assignee not confirmed" in reply and client.sent == []


# --- skip ------------------------------------------------------------------------

def test_skip_renders_the_next_organisation(world):
    hub = Hub({FORM: [endowment_submission(),
                      endowment_submission("endw0002-cccc", "Second Org",
                                           "two@example.invalid",
                                           1899000000000)], DAF: []})
    state = daf_workflow.default_workflow_state()
    daf_workflow._initiate_workflow("new endowment", state, hub)

    reply = daf_workflow._handle_active_workflow("skip", state, hub, None)

    assert "🏛️ **Organization:** Second Org" in reply
    assert "Submission 2 of 2." in reply
    assert EIN not in json.dumps(state)


# --- mark inquiry handled ----------------------------------------------------------

def _mark(state, hub, text):
    return daf_workflow.handle(text, _Ctx(state, hub))


class _Ctx:
    def __init__(self, state, hub):
        from intents.context import Actor, Services
        self.workflow_state = state
        self.services = Services(hubspot=hub, csuite=None, claude=None)
        self.actor = Actor(user_id=7, email="staff@example.invalid",
                           role="staff")


def test_mark_handled_writes_one_row_with_ids_only(world):
    hub = Hub()
    state = daf_workflow.default_workflow_state()
    ask = _mark(state, hub, "mark inquiry handled endowment endw0001 "
                            "Ola replied by email")
    assert "Mark this Endowment submission handled" in ask
    assert "NOT stored" in ask and "Ola replied by email" in ask
    assert world["records"] == [], "nothing before the yes"

    done = _mark(state, hub, "yes")

    assert "recorded as handled" in done
    assert len(world["records"]) == 1
    row = world["records"][0]
    assert row["system"] == "jidhr"
    assert row["endpoint"] == "submission/link/marked_handled"
    assert row["meta"]["ids"] == {"hubspot_form_id": FORM,
                                  "hubspot_submission_id": "endw0001-aaaa-bbbb",
                                  "actor_user_id": 7}
    assert row["meta"]["keys"] == ["actor_user_id", "hubspot_form_id",
                                   "hubspot_submission_id"]


def test_mark_handled_is_idempotent(world):
    hub = Hub()
    state = daf_workflow.default_workflow_state()
    _mark(state, hub, "mark inquiry handled endowment endw0001-aaaa-bbbb")
    _mark(state, hub, "yes")

    again = _mark(state, hub, "mark inquiry handled endowment "
                              "endw0001-aaaa-bbbb")

    assert "already handled" in again
    assert len(world["records"]) == 1


def test_a_second_yes_does_not_write_twice(world):
    hub = Hub()
    state = daf_workflow.default_workflow_state()
    _mark(state, hub, "mark inquiry handled endowment endw0001-aaaa-bbbb")
    # Someone else marks it between the question and the answer.
    world["processed"][FORM]["endw0001-aaaa-bbbb"] = None

    reply = _mark(state, hub, "yes")

    assert "already handled" in reply
    assert world["records"] == []


def test_a_marked_submission_is_not_offered_again(world):
    hub = Hub({FORM: [endowment_submission(),
                      endowment_submission("endw0002-cccc", "Second Org",
                                           "two@example.invalid",
                                           1899000000000)], DAF: []})
    state = daf_workflow.default_workflow_state()
    _mark(state, hub, "mark inquiry handled endowment endw0001-aaaa-bbbb")
    _mark(state, hub, "yes")

    review = daf_workflow._initiate_workflow("new endowment", state, hub)

    assert "Second Org" in review
    assert "none (marked handled, no task)" in review
    assert "profile None" not in review


def test_mark_handled_refuses_when_endowment_is_off(world, monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_RAW", "")
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_ENABLED", False)
    hub = Hub()
    reply = _mark(daf_workflow.default_workflow_state(), hub,
                  "mark inquiry handled endowment endw0001-aaaa-bbbb")
    assert "turned off" in reply and hub.calls == []


@pytest.mark.parametrize("text,needle", [
    ("mark inquiry handled", "where <form> is one of"),
    ("mark inquiry handled nonsense abc12345", "where <form> is one of"),
    ("mark inquiry handled endowment", "Which submission?"),
    ("mark inquiry handled endowment endw", "at least its first 8"),
    ("mark inquiry handled endowment zzzzzzzz", "no submission on this form"),
])
def test_mark_handled_needs_a_form_and_a_real_submission(world, text,
                                                         needle):
    reply = _mark(daf_workflow.default_workflow_state(), Hub(), text)
    assert needle in reply
    assert world["records"] == []


def test_mark_handled_is_routed_to_this_workflow():
    from intents import route_intent
    from tests.test_context import make_ctx

    for text in ("mark inquiry handled endowment endw0001",
                 "mark inquiry handled daf 6fff2fa0 replied, see report"):
        name, _handle = route_intent(text, make_ctx())
        assert name == "daf_workflow", text


# --- the 06:15 run is untouched ------------------------------------------------------

def test_nothing_the_scheduled_run_executes_has_changed():
    """V5.81 must not change the runner or what it runs. Everything under
    jobs/, the runner script and the scheduled chain's modules are
    byte-identical to V5.80 (de92baa). railway.toml changed deliberately in
    V5.84 (role switch, no healthcheck) and is pinned by
    tests/test_railway_jobs_config.py instead."""
    import subprocess

    found = subprocess.run(
        # jobs/, scripts/jobs_run.py and clients/notify.py left this list in
        # V5.90, which adds the daf_sync job, its seeding and send_report.
        # The runner itself and the two existing handlers stay pinned.
        ["git", "diff", "--stat", "de92baa", "--", "jobs/runner.py",
         "jobs/handlers/registrations_sync.py",
         "jobs/handlers/mirror_refresh.py",
         "sync/scheduled.py", "sync/registrations.py",
         "sync/contact_create.py", "sync/mirror.py",
         "clients/database.py", "clients/mirror_read.py"],
        capture_output=True, text=True)
    assert found.returncode == 0, found.stderr
    assert found.stdout.strip() == "", found.stdout


# --- V5.85: a second confirmation before any CSuite write ------------------------

def test_the_first_yes_reads_only_and_shows_a_new_organisation(world):
    _review, plan, state, client = offer_and_create(second=None)

    assert not [e for e in client.endpoints() if e in WRITES]
    assert state["step"] == "confirm_write"
    assert "nothing has been written yet" in plan
    assert "**Outcome: NEW organisation.**" in plan
    assert "will make 3 CSuite writes" in plan
    assert "1. `profile/create/org`" in plan
    assert "2. `note/create` on the new organisation profile" in plan
    assert "3. `task/create`" in plan
    assert "Assigned to employee 1002, task type 1065" in plan
    assert "3. `task/create` — the new organisation profile:" in plan
    assert f"*{TITLE}*" in plan
    assert f"({len(TITLE)} of 255 characters" in plan
    assert "The submission itself stays in HubSpot" in plan
    assert "the three essays" in plan, "the note carries them now"
    # The EIN and the goal ARE shown: both are written now (the EIN to the
    # profile, both in the task title), and the reviewer approves them.
    assert f"EIN {EIN} (as fedid)" in plan
    for secret in SECRETS:
        if secret not in (EIN, GOAL):
            assert secret not in plan, "the essays are never shown"


def test_the_first_yes_shows_an_existing_organisation(world):
    world["mirror"].append({"profile_id": "18075",
                            "organization": "Test Peace Academy",
                            "ptype": "org"})
    _review, plan, _state, client = offer_and_create(second=None)

    assert "**Outcome: EXISTING organisation — profile 18075.**" in plan
    # 18075 has no EIN in the double, so: EIN edit, note, task.
    assert "will make 3 CSuite writes:" in plan
    assert "`profile/edit` on profile 18075 — sets ONLY fedid" in plan
    assert "profile/create/org" not in plan
    assert "`task/create` — profile 18075:" in plan
    assert not [e for e in client.endpoints() if e in WRITES]


def test_the_second_yes_writes_what_was_shown(world):
    _review, reply, _state, client = offer_and_create()

    assert not [e for e in client.before_second if e in WRITES]
    assert [e for e in client.endpoints() if e in WRITES] == [
        "profile/create/org", "note/create", "task/create"]
    assert "Organisation Profile Created" in reply


def test_a_stopped_outcome_never_asks_a_second_time(world):
    world["mirror"].append({"profile_id": "18076", "organization":
                            "Test Peace Academy of Virginia",
                            "ptype": "org"})
    _review, reply, state, client = offer_and_create(second=None)

    assert client.plan_reply is None
    assert state["step"] == "done"
    assert "No profile and no task created" in reply
    assert not [e for e in client.endpoints() if e in WRITES]


@pytest.mark.parametrize("answer", ["cancel", "no", "not yet"])
def test_saying_no_at_the_second_step_writes_nothing(world, answer):
    _review, reply, state, client = offer_and_create(second=answer)

    assert "cancelled" in reply.lower()
    assert state["active"] is False
    assert not [e for e in client.endpoints() if e in WRITES]


@pytest.mark.parametrize("answer", ["hmm", "what will it write?", "maybe"])
def test_anything_else_at_the_second_step_writes_nothing_and_waits(world,
                                                                   answer):
    _review, reply, state, client = offer_and_create(second=answer)

    assert "Nothing has been written" in reply
    assert state["step"] == "confirm_write"
    assert not [e for e in client.endpoints() if e in WRITES]


def test_a_changed_check_at_the_second_yes_writes_nothing(world):
    """Approved as NEW; by the second yes the organisation exists."""
    hub, client = Hub(), Client()
    state = daf_workflow.default_workflow_state()
    daf_workflow._initiate_workflow("new endowment", state, hub)
    plan = daf_workflow._handle_active_workflow("yes", state, hub, client)
    assert "NEW organisation" in plan

    world["mirror"].append({"profile_id": "19999",
                            "organization": "Test Peace Academy",
                            "ptype": "org"})
    reply = daf_workflow._handle_active_workflow("yes", state, hub, client)

    assert "changed since you approved it" in reply
    assert "Approved: new" in reply and "Now: existing (profile 19999)" in reply
    assert not [e for e in client.endpoints() if e in WRITES]


def test_the_switch_going_off_between_the_two_yeses_writes_nothing(
        world, monkeypatch):
    hub, client = Hub(), Client()
    state = daf_workflow.default_workflow_state()
    daf_workflow._initiate_workflow("new endowment", state, hub)
    daf_workflow._handle_active_workflow("yes", state, hub, client)

    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_RAW", "")
    monkeypatch.setattr(Config, "CSUITE_ENDOWMENT_CREATE_ENABLED", False)
    reply = daf_workflow._handle_active_workflow("yes", state, hub, client)

    assert "turned off" in reply
    assert not [e for e in client.endpoints() if e in WRITES]


def test_a_plan_for_another_submission_is_not_used(world):
    """Skipping after the plan was shown must not let a second yes write
    the skipped submission's plan onto the next one."""
    hub = Hub({FORM: [endowment_submission(),
                      endowment_submission("endw0002-cccc", "Second Org",
                                           "two@example.invalid",
                                           1899000000000)], DAF: []})
    client = Client()
    state = daf_workflow.default_workflow_state()
    daf_workflow._initiate_workflow("new endowment", state, hub)
    daf_workflow._handle_active_workflow("yes", state, hub, client)
    state["submission_data"]["submission_id"] = "endw0002-cccc"
    state["step"] = "confirm_write"

    reply = daf_workflow._handle_active_workflow("yes", state, hub, client)

    assert "no approved plan" in reply
    assert not [e for e in client.endpoints() if e in WRITES]


def test_the_plan_in_the_cookie_holds_ids_only(world):
    import json

    _review, _plan, state, _client = offer_and_create(second=None)
    plan = state["endowment_plan"]
    assert plan == {"submission_id": "endw0001-aaaa-bbbb", "outcome": "new",
                    "profile_id": None, "ein_action": None}
    for secret in SECRETS:
        assert secret not in json.dumps(state)


# --- V5.86: the EIN goes into CSuite's fedid --------------------------------------

def _existing(world, pid="18075"):
    world["mirror"].append({"profile_id": pid,
                            "organization": "Test Peace Academy",
                            "ptype": "org"})


def test_a_new_organisation_without_an_ein_sends_no_fedid(world):
    hub = Hub({FORM: [endowment_submission(organization_ein="")], DAF: []})
    _review, _reply, _state, client = offer_and_create(hub=hub)
    assert "fedid" not in client.body("profile/create/org")


def test_existing_with_no_ein_gets_one_guarded_edit_setting_only_fedid(world):
    _existing(world)
    _review, reply, _state, client = offer_and_create(client=Client())

    writes = [e for e in client.endpoints()
              if e in WRITES or e == "profile/edit"]
    assert writes == ["profile/edit", "note/create", "task/create"]
    body = client.body("profile/edit")
    assert {k: v for k, v in body.items() if k not in ("env", "epoch")} \
        == {"profile_id": 18075, "fedid": EIN}
    assert f"EIN {EIN} set on profile 18075 (it had none)" in reply


@pytest.mark.parametrize("stored", [EIN, "00FAKEEIN0042", " 00-FAKE-EIN-0042 "])
def test_existing_with_the_same_ein_is_not_written(world, stored):
    """The same digits in any formatting count as the same EIN."""
    _existing(world)
    client = Client(fedids={18075: stored})
    _review, reply, _state, client = offer_and_create(client=client)

    assert "profile/edit" not in client.endpoints()
    assert "already holds" in client.plan_reply
    assert "will make 2 CSuite writes:" in client.plan_reply
    assert client.body("task/create")["id"] == 18075


def test_existing_with_a_different_ein_is_never_overwritten(world):
    _existing(world)
    client = Client(fedids={18075: "99-9999999"})
    _review, reply, _state, client = offer_and_create(client=client)

    assert "profile/edit" not in client.endpoints()
    assert client.fedids["18075"] == "99-9999999"
    for text in (client.plan_reply, reply):
        assert "EIN MISMATCH" in text and "99-9999999" in text
    assert "will make 2 CSuite writes:" in client.plan_reply
    assert client.body("task/create")["id"] == 18075, "the task still goes"


def test_an_unreadable_existing_profile_gets_no_ein_write(world):
    _existing(world)
    client = Client(unreadable=(18075,))
    _review, _reply, _state, client = offer_and_create(client=client)

    assert "profile/edit" not in client.endpoints()
    assert "could not be read" in client.plan_reply


def test_existing_and_no_ein_on_the_form_gets_no_edit(world):
    _existing(world)
    hub = Hub({FORM: [endowment_submission(organization_ein="")], DAF: []})
    _review, _reply, _state, client = offer_and_create(hub=hub)

    assert "profile/edit" not in client.endpoints()
    assert "the form has none" in client.plan_reply


def test_an_ein_appearing_between_the_two_yeses_stops_the_write(world):
    """Approved to SET an empty EIN; by the second yes the profile has one.
    Never overwrite: nothing at all is written."""
    _existing(world)
    hub, client = Hub(), Client()
    state = daf_workflow.default_workflow_state()
    daf_workflow._initiate_workflow("new endowment", state, hub)
    plan = daf_workflow._handle_active_workflow("yes", state, hub, client)
    assert "sets ONLY fedid" in plan

    client.fedids["18075"] = "11-1111111"          # someone set it meanwhile
    reply = daf_workflow._handle_active_workflow("yes", state, hub, client)

    assert "changed since you approved it" in reply
    assert "EIN step approved: set; now: mismatch" in reply
    assert [e for e in client.endpoints()
            if e in WRITES or e == "profile/edit"] == []


def test_the_ein_never_reaches_logs_or_audit_on_the_edit_path(world, caplog):
    _existing(world)
    with caplog.at_level(logging.DEBUG):
        offer_and_create(client=Client())

    logged = "\n".join(r.getMessage() for r in caplog.records)
    audited = json.dumps(world["audit"])
    assert any(row["endpoint"] == "profile/edit" for row in world["audit"])
    for secret in SECRETS:
        assert secret not in logged
        assert secret not in audited


def test_the_field_gate_allows_fedid_only_where_it_was_verified():
    from clients.csuite import UnconfirmedField, check_input_fields

    check_input_fields(["fedid"], "profile/create/org")
    check_input_fields(["fedid"], "profile/edit")
    with pytest.raises(UnconfirmedField):
        check_input_fields(["fedid"], "profile/create/individual")


@pytest.mark.parametrize("name", ["org_contact_name", "is_nonprofit"])
def test_the_display_only_names_are_refused_by_the_gate(name):
    """Probed 2026-10-15: CSuite drops them. The gate refuses them before a
    request is built, so nothing pretends to set them."""
    from clients.csuite import ORG_DISPLAY_ONLY, UnconfirmedField, \
        check_input_fields

    assert name in ORG_DISPLAY_ONLY
    for endpoint in ("profile/create/org", "profile/edit"):
        with pytest.raises(UnconfirmedField):
            check_input_fields([name], endpoint)


# --- V5.87: the note ------------------------------------------------------------

def test_the_note_goes_on_the_profile_with_the_essays_and_the_link(world):
    _review, reply, _state, client = offer_and_create()

    assert len(client.notes) == 1
    body = client.notes[0]
    assert body["ref"] == "profile" and body["ref_id"] == 40001
    text = body["note_text"]
    assert text.splitlines()[0] == \
        "Added by Jidhr from HubSpot submission endw0001-aaaa-bbbb"
    for part in ("FAKEMISSION", "FAKEINTEREST", "FAKEGOALS", GOAL,
                 "FAKEHEARD", "(submission endw0001-aaaa-bbbb)",
                 "[Mission and core services/programs]",
                 "[Main endowment goals]"):
        assert part in text
    assert "🗒️ Note 303200 added to the profile" in reply
    assert "Not read back through the API" in reply


def test_the_note_is_made_before_the_task_and_on_an_existing_profile(world):
    _existing(world)
    client = Client(fedids={18075: EIN})
    _review, _reply, _state, client = offer_and_create(client=client)

    writes = [e for e in client.endpoints() if e in WRITES]
    assert writes == ["note/create", "task/create"]
    assert client.notes[0]["ref_id"] == 18075


def test_success_zero_with_a_note_id_is_a_created_note(world):
    """CSuite's own reply, 2026-10-15: {"note_id": 303140, "success": 0}."""
    _review, reply, _state, client = offer_and_create()
    assert "Note 303200 added" in reply
    note_rows = [r for r in world["audit"] if r["endpoint"] == "note/create"]
    assert len(note_rows) == 1


@pytest.mark.parametrize("reply_body,shown", [
    ({"success": 0, "errors": ["Invalid or missing note_text"]},
     "Invalid or missing note_text"),
    ({"success": 0}, "CSuite returned no note id"),
    ({"note_id": 303201, "success": 0, "errors": ["something"]}, "something"),
])
def test_a_failed_note_is_reported_never_retried_and_the_task_still_goes(
        world, reply_body, shown):
    client = Client()
    client.note_reply = reply_body
    _review, reply, _state, client = offer_and_create(client=client)

    assert [e for e in client.endpoints() if e == "note/create"] == \
        ["note/create"], "exactly one attempt"
    assert "Note NOT added" in reply and shown in reply
    assert "task/create" in client.endpoints()


def test_a_note_whose_outcome_is_unknown_is_not_retried(world, monkeypatch):
    calls = []

    def boom(self, profile_id, text, audit_meta=None):
        calls.append(profile_id)
        raise TimeoutError("read timed out")

    monkeypatch.setattr(Client, "create_note", boom)
    _review, reply, _state, client = offer_and_create()

    assert calls == [40001]
    assert "may or may not exist" in reply and "NOT retried" in reply
    assert "task/create" in client.endpoints()


def test_the_note_text_never_reaches_logs_or_audit(world, caplog):
    with caplog.at_level(logging.DEBUG):
        offer_and_create()
    logged = "\n".join(r.getMessage() for r in caplog.records)
    audited = json.dumps(world["audit"])
    for secret in SECRETS:
        assert secret not in logged and secret not in audited


def test_the_client_rule_is_note_create_only():
    from clients.csuite import note_created

    assert note_created("note/create", {"note_id": 5, "success": 0})
    assert not note_created("note/create", {"success": 0})
    assert not note_created("note/create", {"note_id": 5, "errors": ["x"]})
    assert not note_created("task/create", {"note_id": 5, "success": 0})
    assert not note_created("profile/create/org", {"note_id": 5})


def test_the_audit_row_records_the_note_id_as_its_target():
    from clients.audit import target_id_from_response

    assert target_id_from_response(
        {"success": 1, "data": {"note_id": 303140}}) == "303140"


# --- V5.88: no angle brackets in the title ----------------------------------------

def test_the_email_is_in_parentheses_not_angle_brackets():
    """CSuite's task page renders HTML: production task 1067 stored
    "<abanth@...>" and the page showed nothing after the name."""
    title = endowment_inquiry.task_title(_data())
    assert title.endswith("— Dana Director (director@example.invalid)")
    assert "<" not in title and ">" not in title


def test_angle_brackets_are_removed_from_every_field():
    title = endowment_inquiry.task_title(_data(
        name="<b>Peace</b> Academy", organization_ein="<00-1>",
        what_is_your_endowment_starting_goal_="> $25,000 <script>",
        firstname="<Dana>", email="<director@example.invalid>"))

    assert "<" not in title and ">" not in title
    assert title == ("Endowment inquiry: bPeace/b Academy — EIN 00-1 — "
                     "goal $25,000 script — Dana Director "
                     "(director@example.invalid)")
