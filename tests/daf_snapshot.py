"""A fixed DAF run, recorded end to end, for the V5.81 DAF-unchanged tests.

Everything here is a TEST DOUBLE and every value is invented. The CSuite
client is the REAL CSuiteClient with the real `_request`; only the HTTP
session, the audit store and the clock are faked, so what is recorded is the
exact body that would leave the process.

`record()` runs the scenarios and returns plain data. The golden copy in
tests/fixtures/daf_v580_golden.json was produced by this module against
V5.80 code (Part 1 had changed nothing on the DAF path), before Part 2
touched anything.
"""

import json

from clients import audit
from clients.csuite import CSuiteClient
from config import Config
from intents import daf_workflow
from sync.sandbox_writes import WriteBudget

FORM = Config.DAF_INQUIRY_FORM_ID
FIXED_EPOCH = 1800000000          # 2027-01-15; only stamps the body
EMAIL_NEW = "fresh.donor@example.invalid"
EMAIL_BACK = "returning.donor@example.invalid"


def submission(conv, first, last, email, ms, **extra):
    values = {"firstname": first, "lastname": last, "email": email,
              "phone": "(703) 555-0177", "address": "1 Test Way",
              "city": "Reston", "state": "VA", "zip": "20190",
              "how_did_you_hear_about_us_": "Friend",
              "what_would_you_like_to_name_your_fund_": "Fresh Family Fund",
              "estimated_initial_contribution_amount": "25000"}
    values.update(extra)
    return {"conversionId": conv, "submittedAt": ms,
            "values": [{"name": k, "value": v} for k, v in values.items()]}


# Submitted in 2030 so task_due_date's max(submitted, today) is the
# submission date on any day this suite runs, and the due date is fixed.
def fixture_submissions():
    return [
        submission("done0001-aaaa", "Handled", "Already", "h@example.invalid",
                   1900000000000),
        submission("fresh001-bbbb", "Fresh", "Donor", EMAIL_NEW,
                   1899990000000),
        submission("back0001-cccc", "Returning", "Donor", EMAIL_BACK,
                   1899980000000),
    ]


PROCESSED = {"done0001-aaaa": "21000"}


class Hub:
    """HubSpot double. Answers both the V5.80 call (limit=5) and a paged
    get_form_submissions, and records which one was used."""

    def __init__(self, subs=None):
        self.subs = subs if subs is not None else fixture_submissions()
        self.calls = []

    def get_daf_inquiry_submissions(self, limit=50):
        self.calls.append(("daf", limit))
        return {"results": self.subs[:limit]}

    def get_form_submissions(self, form_id, limit=50, after=None):
        self.calls.append(("form", form_id, limit, after))
        start = int(after or 0)
        page = self.subs[start:start + limit]
        out = {"results": page}
        if start + limit < len(self.subs):
            out["paging"] = {"next": {"after": str(start + limit)}}
        return out

    def search_contact_by_email(self, email, properties=None):
        return {"results": [{"id": "70123", "properties": {}}]}

    def update_contact_by_email(self, email, properties):
        return {"id": "70123"}

    def __getattr__(self, name):
        raise AssertionError(f"unexpected HubSpot call: {name}()")


class RealRequestClient(CSuiteClient):
    """The real _request. `existing` is what the email search finds."""

    def __init__(self, existing=None):
        self.api_key = "k"
        self.api_secret = "s"
        self.base_url = "https://amuslimcf.fcsuite.com/api/v2"
        self.env = "live"
        self.verify_writes = False
        self.write_budget = WriteBudget(5)
        self.sent = []
        outer = self
        existing = existing or {}

        class Session:
            def post(_self, url, data=None, **kwargs):
                endpoint = url.split("/api/v2/", 1)[-1]
                body = json.loads(data)
                outer.sent.append({"endpoint": endpoint, "body": body, "raw": data})
                if endpoint == "profile/list":
                    email = body.get("primary_email")
                    if email is None:
                        reply = {"count": 18797, "results": []}
                    elif str(email).startswith("filter-probe-"):
                        reply = {"count": 0, "results": []}
                    else:
                        ids = existing.get(email, [])
                        reply = {"count": len(ids),
                                 "results": [{"profile_id": i} for i in ids]}
                    payload = {"success": 1, "data": reply}
                elif endpoint == "profile/display":
                    payload = {"success": 1, "data": {
                        "profile_id": body.get("profile_id"),
                        "ptype": "indiv"}}
                elif endpoint == "profile/create/individual":
                    payload = {"success": 1, "data": {"profile_id": 31337}}
                elif endpoint == "task/create":
                    payload = {"success": 1, "data": {"task_id": 1099}}
                else:
                    raise AssertionError(f"unexpected CSuite {endpoint}")

                class R:
                    status_code = 200
                    text = json.dumps(payload)

                    def json(self):
                        return payload
                return R()

        self.session = Session()


def _flags(monkeypatch, create=True, task=True):
    monkeypatch.setattr(Config, "CSUITE_ENV", "live")
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", create)
    monkeypatch.setattr(Config, "CSUITE_DAF_TASK_CREATE_ENABLED", task)
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_HUBSPOT_BACKFILL_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_TICKET_CLOSE_ENABLED", False)
    monkeypatch.setattr(Config, "CSUITE_TASK_EMPLOYEE_ID_DAF_INQUIRY", 1012)
    monkeypatch.setattr(Config, "CSUITE_TASK_TYPE_ID", 1065)


def _wire(monkeypatch, audit_rows):
    import clients.csuite as mod

    def reserve(system, method, endpoint, target_id=None, payload=None,
                sync_run_id=None):
        audit_rows.append({"system": system, "endpoint": endpoint,
                           "meta": audit.payload_meta(payload)})
        return {"id": len(audit_rows)}

    monkeypatch.setattr(mod, "reserve_write", reserve)
    monkeypatch.setattr(mod, "complete_write", lambda *a, **kw: None)
    monkeypatch.setattr(mod.time, "time", lambda: FIXED_EPOCH)
    # filter_trust's known-absent probe is a fresh uuid per search; pinned
    # so the probe request body is comparable byte for byte.
    import uuid

    import sync.filter_trust as trust
    monkeypatch.setattr(trust.uuid, "uuid4",
                        lambda: uuid.UUID(int=0x5EED))
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: dict(PROCESSED))


def _create(monkeypatch, pick, existing=None, create=True, task=True):
    """Open "new daf", skip to `pick`, say yes. Returns everything seen."""
    _flags(monkeypatch, create=create, task=task)
    rows = []
    _wire(monkeypatch, rows)
    hub = Hub()
    client = RealRequestClient(existing=existing)
    state = daf_workflow.default_workflow_state()
    opened = daf_workflow._initiate_workflow("new daf", state, hub)
    replies = [opened]
    while state["submission_data"]["submission_id"] != pick:
        replies.append(daf_workflow._handle_active_workflow(
            "skip", state, hub, client))
    replies.append(daf_workflow._handle_active_workflow(
        "yes", state, hub, client))
    return {"replies": replies, "sent": client.sent, "audit": rows}


def record(monkeypatch) -> dict:
    """Every scenario, as plain JSON-able data."""
    out = {}
    out["new_donor"] = _create(monkeypatch, "fresh001-bbbb")
    out["returning_donor"] = _create(monkeypatch, "back0001-cccc",
                                     existing={EMAIL_BACK: [21999]})
    out["create_off"] = _create(monkeypatch, "fresh001-bbbb", create=False)
    out["task_off"] = _create(monkeypatch, "fresh001-bbbb", task=False)
    return json.loads(json.dumps(out, default=str))


def record_reconcile(monkeypatch) -> dict:
    """"reconcile daf tasks dry run", end to end through the chat command,
    on the V5.79 fixture (two decided links). Doubles only."""
    from intents import sync_commands
    from sync import daf_reconcile as dr
    from tests import test_daf_handled as h

    processed = {"6a31606e": "21699"}
    csuite = h.ReconcileCSuite()
    monkeypatch.setattr(dr, "legacy_tasks", lambda *a, **k: ["1033", "1036"])
    monkeypatch.setattr(dr, "tasked_submissions", lambda *a, **k: set())
    monkeypatch.setattr(dr, "_pace", lambda: None)
    monkeypatch.setattr(dr, "link_rows_allowed", lambda: True)
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: dict(processed))
    monkeypatch.setattr(
        dr, "profiles_for",
        lambda client, email: (list(csuite.profiles.get(email, [])), None))
    hub = h.ReconcileHub(h.subs_fixture())
    import clients.csuite
    import clients.hubspot
    monkeypatch.setattr(clients.hubspot, "HubSpotClient", lambda: hub)
    monkeypatch.setattr(clients.csuite, "CSuiteClient", lambda: csuite)
    reply = sync_commands.handle("reconcile daf tasks dry run", None)
    return {"reply": reply, "hub_calls": [list(c) for c in hub.calls]}
