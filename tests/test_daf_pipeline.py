"""sync/daf_pipeline.py and the CSuite client's opportunity calls.

Shapes are the sandbox's own (2026-10-28, opportunities 1033-1035). No
network, no database.
"""

import json

import pytest

from clients import audit
from clients.csuite import (CSuiteClient, UnconfirmedField,
                            check_input_fields, opportunity_created)
from sync import daf_pipeline as dp

ROW = {"op_id": 1000, "otype_id": 1003, "profile_id": 8916,
       "opportunity_status": "open", "stage_name": "Inquiry"}


class Csuite:
    """Records calls; answers list/create/display."""

    def __init__(self, rows=(), count=None, list_ok=True, create=None):
        self.rows, self.count = [dict(r) for r in rows], count
        self.list_ok, self.create = list_ok, create
        self.calls = []

    def list_opportunities(self, limit=1000):
        self.calls.append("list")
        if not self.list_ok:
            return {"success": False, "error": "HTTP 503"}
        n = len(self.rows) if self.count is None else self.count
        return {"success": True, "data": {"count": n, "results": self.rows}}

    def create_opportunity(self, profile_id, otype_id, audit_meta=None):
        self.calls.append(("create", profile_id, otype_id))
        if self.create is not None:
            return self.create
        return {"success": True, "data": {"op_id": 1036}}

    def get_opportunity(self, op_id):
        self.calls.append(("display", op_id))
        return {"success": True, "data": {"op_id": op_id,
                                          "stage_name": "Outreach",
                                          "assigned_employee_id": None}}


def test_a_new_donor_needs_no_read_to_know_none_exists():
    c = Csuite()
    assert dp.plan(c, None, {}) == {"outcome": dp.WOULD}
    assert c.calls == []


@pytest.mark.parametrize("count,expected", [(5, "returned 1 of 5"),
                                            ("x", "no count")])
def test_a_partial_or_uncounted_list_is_could_not_check(count, expected):
    c = Csuite([ROW], count=count)
    out = dp.ensure(c, 8916, {})
    assert out["outcome"] == dp.UNCHECKED and expected in out["reason"]
    assert [x for x in c.calls if x != "list"] == [], "nothing created"


def test_an_unreadable_list_creates_nothing():
    c = Csuite(list_ok=False)
    assert dp.ensure(c, 77, {})["outcome"] == dp.UNCHECKED
    assert c.calls == ["list"]


def test_one_list_read_serves_the_run_and_learns_its_own_creates():
    c, cache = Csuite(), {}
    first = dp.ensure(c, 77, cache)
    second = dp.ensure(c, 77, cache)
    assert first == {"outcome": dp.CREATED, "op_id": 1036,
                     "stage": "Outreach", "owner": None}
    assert second == {"outcome": dp.EXISTING, "op_id": 1036}
    assert c.calls.count("list") == 1
    assert [x for x in c.calls if x and x[0] == "create"] == \
        [("create", 77, 1003)]


def test_a_refused_create_is_failed_never_raised():
    c = Csuite(create={"success": False, "error": "server error"})
    assert dp.ensure(c, 77, {})["outcome"] == dp.FAILED

    class Boom(Csuite):
        def create_opportunity(self, *a, **k):
            raise RuntimeError("socket closed")
    assert dp.ensure(Boom(), 77, {}) == {"outcome": dp.FAILED,
                                         "reason": "RuntimeError"}


@pytest.mark.parametrize("result,line", [
    ({"outcome": dp.WOULD},
     "Would create pipeline record at Outreach (unowned)."),
    ({"outcome": dp.CREATED, "op_id": 1036, "stage": "Outreach",
      "owner": None}, "Pipeline record 1036 created at Outreach (unowned)."),
    ({"outcome": dp.EXISTING, "op_id": 1000},
     "Already has open pipeline record 1000; no second one."),
])
def test_the_report_lines(result, line):
    assert dp.line(result) == line


# --- the client -----------------------------------------------------------------

def test_only_otype_id_and_profile_id_may_be_sent():
    check_input_fields({"otype_id": 1003, "profile_id": 1},
                       "opportunity/create")
    for ignored in ("stage_name", "assigned_employee_id"):
        with pytest.raises(UnconfirmedField):
            check_input_fields({ignored: 1}, "opportunity/create")


def test_the_top_level_op_id_is_kept_and_audited(monkeypatch):
    """The sandbox's own answer: the id beside `success`, no `data`."""
    reserved, completed = [], []
    monkeypatch.setattr("clients.csuite.reserve_write",
                        lambda *a, **k: reserved.append((a, k)) or 1)
    monkeypatch.setattr("clients.csuite.complete_write",
                        lambda *a, **k: completed.append(k))

    client = CSuiteClient.__new__(CSuiteClient)
    client.api_key, client.api_secret = "k", "s"
    client.base_url = "https://amuslimcf-sandbox.fcsuite.com/api/v2"
    client.env, client.verify_writes, client.write_budget = \
        "sandbox", False, None
    answer = {"op_id": 1034, "success": 1, "opportunity_guid": "g-1034"}

    class Session:
        def post(self, url, data=None, **kw):
            class R:
                status_code = 200
                text = json.dumps(answer)

                def json(self):
                    return answer
            return R()
    client.session = Session()

    out = client.create_opportunity(21889, 1003)
    assert out["success"] and out["data"]["op_id"] == 1034
    assert completed[0]["target_id"] == "1034"


def test_op_id_is_a_created_id_for_the_audit():
    assert audit.target_id_from_response({"data": {"op_id": 1034}}) == "1034"


@pytest.mark.parametrize("endpoint,body,expected", [
    ("opportunity/create", {"op_id": 1, "success": 1}, True),
    ("opportunity/create", {"op_id": 1, "success": 0}, False),
    ("opportunity/create", {"success": 1, "data": {"x": 1}, "op_id": 1}, False),
    ("task/create", {"op_id": 1, "success": 1}, False),
])
def test_only_an_opportunity_create_success_is_rewrapped(endpoint, body,
                                                         expected):
    assert opportunity_created(endpoint, body) is expected
