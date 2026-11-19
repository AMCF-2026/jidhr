"""Sandbox proof of the nightly endowment step (sync/endowment_sync.py).

SANDBOX ONLY. CSuite is the real client, checked by sync.sandbox_writes.
assert_sandbox before every case, with a WriteBudget equal to the case's
expected writes (a write over it raises). Nothing else is real:

  * HubSpot is an in-memory double holding invented submissions; the real
    HubSpotClient cannot be built (it raises).
  * Postgres is never touched: clients.database raises on any call. The
    audit store, the processed/held history and the mirror are in memory.
    (processed_submissions only counts LIVE creates anyway, so a sandbox
    create could never mark a submission handled in the real table.)

    python scripts/sandbox_endowment_nightly.py --discover
        reads only: finds an org profile with no EIN and one with an EIN,
        neither with an open task, for cases 3 and 4.
    python scripts/sandbox_endowment_nightly.py --run NOFEDID_ID FEDID_ID
        the six cases. Prints a JSON summary.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, ".")

from config import Config  # noqa: E402

FORBIDDEN = RuntimeError


def _no(*_a, **_k):
    raise FORBIDDEN("this sandbox run must not reach Postgres or HubSpot")


def lock_down():
    """No database, no real HubSpot, an in-memory audit and history."""
    import clients.audit as audit
    import clients.csuite as csuite_mod
    import clients.database as database
    import clients.hubspot as hubspot
    import sync.daf_reconcile as reconcile

    database.execute_query = _no
    database.get_connection = _no
    hubspot.HubSpotClient = _no

    state = {"processed": {}, "held": {}, "audit": [], "records": []}

    def reserve(system, method, endpoint, target_id=None, payload=None,
                sync_run_id=None):
        state["audit"].append({"endpoint": endpoint,
                               "meta": audit.payload_meta(payload)})
        return {"endpoint": endpoint,
                "sid": (payload or {}).get("hubspot_submission_id")}

    def complete(reservation, status="success", **kwargs):
        if status == "success" and reservation.get("sid") and \
                reservation["endpoint"] in ("profile/create/org",
                                            "task/create"):
            state["processed"][reservation["sid"]] = reservation["endpoint"]
        return True

    def record(system, method, endpoint, target_id=None, payload=None,
               status="success", **kwargs):
        state["records"].append({"endpoint": endpoint,
                                 "meta": audit.payload_meta(payload)})
        sid = payload["hubspot_submission_id"]
        if endpoint.startswith("submission/held/"):
            state["held"][sid] = endpoint.rsplit("/", 1)[-1]
        if endpoint.startswith("submission/link/"):
            state["processed"][sid] = endpoint
        return True

    csuite_mod.reserve_write = reserve
    csuite_mod.complete_write = complete
    audit.processed_submissions = (lambda form_id, endpoints=None, **k:
                                   dict(state["processed"]))
    audit.held_submissions = lambda form_id: dict(state["held"])
    audit.record_write = record
    reconcile.link_rows_allowed = lambda: True
    return state


def sandbox_client(budget):
    from clients.csuite import CSuiteClient
    from sync.sandbox_writes import WriteBudget, assert_sandbox

    client = CSuiteClient()
    assert_sandbox(client)
    client.write_budget = WriteBudget(budget)
    return client


# --- discovery (reads only) ----------------------------------------------------

def discover():
    from sync.open_tasks import load_open_tasks

    lock_down()
    client = sandbox_client(0)
    tasks, error = load_open_tasks(client)
    print(json.dumps({"task_list": error or f"{len(tasks)} profiles with "
                      "open tasks"}))
    if error:
        return
    found = {"no_fedid": None, "fedid": None}
    listing = client.get_profiles(limit=200, offset=0)
    ids = [r.get("profile_id") for r in
           ((listing.get("data") or {}).get("results") or [])]
    looked = 0
    for pid in ids:
        if all(found.values()) or looked >= 80:
            break
        looked += 1
        record = (client.get_profile(pid).get("data") or {})
        if str(record.get("ptype") or "").lower() != "org":
            continue
        if str(pid) in tasks:
            continue
        name = record.get("organization") or record.get("name")
        if not name:
            continue
        key = "fedid" if str(record.get("fedid") or "").strip() else \
            "no_fedid"
        if not found[key]:
            found[key] = {"profile_id": pid, "organization": name,
                          "has_fedid": key == "fedid"}
    print(json.dumps({"looked_at": looked, "found": found}, indent=2))


# --- the six cases -------------------------------------------------------------

def submission(conv, org, email, ein, ms):
    contact = {"firstname": "Sandbox", "lastname": "Contact", "email": email,
               "phone": "", "how_did_you_hear_about_us_": "sandbox test"}
    company = {"name": org, "organization_ein": ein, "address": "",
               "city": "", "state": "", "zip": "", "website": "",
               "what_is_your_endowment_starting_goal_": "sandbox test",
               "what_is_your_organization_s_mission_and_core_services_"
               "programs_": "Sandbox test of the nightly endowment step.",
               "what_prompted_your_organization_s_interest_in_starting_an_"
               "endowment_": "Sandbox test.",
               "what_are_your_organization_s_main_endowment_goals_":
                   "Sandbox test."}
    values = ([{"name": k, "value": v, "objectTypeId": "0-1"}
               for k, v in contact.items() if v]
              + [{"name": k, "value": v, "objectTypeId": "0-2"}
                 for k, v in company.items() if v])
    return {"conversionId": conv, "submittedAt": ms, "values": values}


class Hub:
    def __init__(self, subs):
        self.subs = subs

    def get_form_submissions(self, form_id, limit=50, after=None):
        if form_id != Config.ENDOWMENT_INQUIRY_FORM_ID:
            return {"results": []}
        return {"results": list(self.subs)}

    def search_contact_by_email(self, email, properties=None):
        return {"results": [{"id": "sandbox-contact", "properties": {}}]}

    def __getattr__(self, name):
        raise FORBIDDEN(f"unexpected HubSpot call: {name}()")


def profile_snapshot(client, pid):
    data = (client.get_profile(pid).get("data") or {})
    return {k: data.get(k) for k in sorted(data)}


def run_cases(no_fedid_id, fedid_id):
    from sync import endowment_sync, org_match

    state = lock_down()
    stamp = datetime.now(timezone.utc).strftime("%m%d%H%M")
    now_ms = int(time.time() * 1000)
    today = datetime.now(timezone.utc).date().isoformat()
    for name, value in (("JIDHR_ENDOWMENT_SINCE", today),
                        ("CSUITE_ENDOWMENT_CREATE_RAW", "true"),
                        ("CSUITE_ENDOWMENT_CREATE_ENABLED", True),
                        ("CSUITE_ENDOWMENT_ASSIGNEE", 1002),
                        ("CSUITE_WRITE_BUDGET", 10),
                        ("CSUITE_WRITE_BUDGET_RAW", "10")):
        setattr(Config, name, value)

    reader = sandbox_client(0)
    no_fedid = profile_snapshot(reader, no_fedid_id)
    with_fedid = profile_snapshot(reader, fedid_id)
    mirror = [{"profile_id": str(no_fedid_id), "ptype": "org",
               "organization": no_fedid.get("organization")},
              {"profile_id": str(fedid_id), "ptype": "org",
               "organization": with_fedid.get("organization")}]
    org_match.mirror_org_rows = lambda: (list(mirror),
                                         datetime.now(timezone.utc))

    def case(label, subs, budget, budget_left=None):
        client = sandbox_client(budget)
        out = endowment_sync.run(hubspot=Hub(subs), csuite=client,
                                 run_mode="live",
                                 budget_left=(budget if budget_left is None
                                              else budget_left))
        writes = [e for e in client.write_budget.log]
        row = {"case": label, "writes": writes, "used": len(writes),
               "added": [{k: e.get(k) for k in
                          ("submission_id", "plan", "profile_id",
                           "profile_created", "note_id", "task_id", "ein",
                           "problems")} for e in out["added"]],
               "held": [{k: e.get(k) for k in ("submission_id", "reason")}
                        for e in out["held"]],
               "deferred": [{k: e.get(k) for k in ("submission_id",
                                                   "reason")}
                            for e in out["deferred"]],
               "existing_task": [e["submission_id"]
                                 for e in out["existing_task"]],
               "skipped_handled": out["skipped_handled"],
               "skipped_held": out["skipped_held"]}
        return row, out

    rows, sections = [], []
    a = submission(f"sbx{stamp}-a", f"Jidhr Sandbox Endowment {stamp} One",
                   f"jidhr-sbx-{stamp}-a@example.invalid", "98-7654321",
                   now_ms)
    r, out = case("1 new org", [a], 3)
    rows.append(r)
    sections.append(out)
    new_pid = r["added"][0]["profile_id"] if r["added"] else None
    if new_pid:
        r["readback"] = {k: profile_snapshot(reader, new_pid).get(k)
                         for k in ("profile_id", "ptype", "organization",
                                   "fedid")}

    r, _out = case("2 same submission again", [a], 3)
    rows.append(r)

    c = submission(f"sbx{stamp}-c", no_fedid.get("organization"),
                   f"jidhr-sbx-{stamp}-c@example.invalid", "98-1111111",
                   now_ms + 1000)
    r, out = case("3 matched org, profile has no EIN", [c], 2)
    after = profile_snapshot(reader, no_fedid_id)
    r["profile_unchanged"] = {
        "fedid_before": no_fedid.get("fedid"),
        "fedid_after": after.get("fedid"),
        "changed_fields": sorted(k for k in set(no_fedid) | set(after)
                                 if no_fedid.get(k) != after.get(k))}
    rows.append(r)
    sections.append(out)

    d = submission(f"sbx{stamp}-d", with_fedid.get("organization"),
                   f"jidhr-sbx-{stamp}-d@example.invalid",
                   str(with_fedid.get("fedid") or ""), now_ms + 2000)
    r, out = case("4 matched org, same EIN", [d], 2)
    rows.append(r)
    sections.append(out)

    mirror.append({"profile_id": "999999001", "ptype": "org",
                   "organization": f"Jidhr Sandbox Endowment {stamp} Five "
                                   "of Virginia"})
    e = submission(f"sbx{stamp}-e", f"Jidhr Sandbox Endowment {stamp} Five",
                   f"jidhr-sbx-{stamp}-e@example.invalid", "", now_ms + 3000)
    r, out = case("5 ambiguous match", [e], 0)
    rows.append(r)
    sections.append(out)

    f = submission(f"sbx{stamp}-f", f"Jidhr Sandbox Endowment {stamp} Six",
                   f"jidhr-sbx-{stamp}-f@example.invalid", "", now_ms + 4000)
    r, out = case("6 budget 2, new org", [f], 2, budget_left=2)
    rows.append(r)
    sections.append(out)

    from sync import daf_sync
    print(json.dumps({"stamp": stamp, "cases": rows,
                      "csuite_writes_total": sum(r["used"] for r in rows),
                      "jidhr_rows": [x["endpoint"] for x in state["records"]],
                      "database_touched": False}, indent=2, default=str))
    print("----- the endowment block, as the morning email would show it")
    merged = {"form": "endowment", "label": "Endowment", "mode": "live",
              "added": [x for s in sections for x in s["added"]],
              "held": [x for s in sections for x in s["held"]],
              "deferred": [x for s in sections for x in s["deferred"]],
              "existing_task": [x for s in sections
                                for x in s["existing_task"]],
              "budget": 10, "budget_needed": 0, "budget_allowed": 0}
    print(daf_sync.email_text([merged]))


# --- round 16: whose open tasks count -------------------------------------------

USED = {"21762", "21856", "21955"}


def discover_owner():
    """Reads only: two org profiles with no open task (not used before), and
    every task type the sandbox's tasks carry, from task/display."""
    from sync.open_tasks import load_open_tasks

    lock_down()
    client = sandbox_client(0)
    tasks, error = load_open_tasks(client, with_employee=True)
    if error:
        print(json.dumps({"task_list": error}))
        return
    rows = ((client._request("task/list", {"view_limit": 1000})
             .get("data") or {}).get("results") or [])
    types = {}
    for row in rows:
        data = (client._request("task/display",
                                {"task_id": row.get("task_id")})
                .get("data") or {})
        kind = (data.get("task_type") or {})
        types.setdefault(str(kind.get("task_type_id")),
                         kind.get("task_type_name"))
    orgs = []
    listing = client.get_profiles(limit=200, offset=0)
    for row in ((listing.get("data") or {}).get("results") or []):
        pid = str(row.get("profile_id"))
        if len(orgs) >= 2 or pid in USED or pid in tasks:
            continue
        record = client.get_profile(pid).get("data") or {}
        if str(record.get("ptype") or "").lower() == "org" and \
                record.get("organization"):
            orgs.append({"profile_id": pid,
                         "organization": record.get("organization")})
    print(json.dumps({"task_types_seen": types, "orgs": orgs}, indent=2))


def counting(client):
    """Log every CSuite request the client makes, reads included."""
    calls = []
    inner = client._request

    def _request(endpoint, data=None, **kwargs):
        calls.append(endpoint)
        return inner(endpoint, data, **kwargs)
    client._request = _request
    return calls


def run_owner_cases(other_type_org, other_assignee_org, other_type):
    """Setup: one task on each org (listed apart). Then three matched
    submissions: 21762 (open task 1199, Ola, 1065), an org whose only open
    task is 1065 for another assignee, an org whose only open task is Ola's
    of another type."""
    from intents.daf_workflow import form_task_type, inquiry_form
    from sync import endowment_sync, org_match

    state = lock_down()
    stamp = datetime.now(timezone.utc).strftime("%m%d%H%M")
    now_ms = int(time.time() * 1000)
    today = datetime.now(timezone.utc).date().isoformat()
    for name, value in (("JIDHR_ENDOWMENT_SINCE", today),
                        ("CSUITE_ENDOWMENT_CREATE_RAW", "true"),
                        ("CSUITE_ENDOWMENT_CREATE_ENABLED", True),
                        ("CSUITE_ENDOWMENT_ASSIGNEE", 1002),
                        ("CSUITE_WRITE_BUDGET", 10),
                        ("CSUITE_WRITE_BUDGET_RAW", "10")):
        setattr(Config, name, value)
    endowment_type = form_task_type(inquiry_form("endowment"))

    reader = sandbox_client(0)
    names = {pid: profile_snapshot(reader, pid).get("organization")
             for pid in ("21762", other_assignee_org, other_type_org)}
    mirror = [{"profile_id": pid, "ptype": "org", "organization": name}
              for pid, name in names.items()]
    org_match.mirror_org_rows = lambda: (list(mirror),
                                         datetime.now(timezone.utc))

    # --- setup writes, counted apart from the cases
    setup = sandbox_client(2)
    made = []
    for pid, employee, kind in ((other_assignee_org, 1007, endowment_type),
                                (other_type_org, 1002, int(other_type))):
        response = setup.create_task(
            name=f"Jidhr sandbox setup {stamp}: open task, employee "
                 f"{employee}, type {kind}",
            employee_id=employee, due_date=today,
            description="Sandbox setup for the endowment open-task filter.",
            linked_profile_id=int(pid), task_type_id=kind,
            audit_meta={"sandbox_setup": stamp})
        made.append({"profile_id": pid, "employee_id": employee,
                     "task_type_id": kind,
                     "task_id": (response.get("data") or {}).get("task_id"),
                     "success": bool(response.get("success"))})

    def case(label, pid, letter, budget):
        sub = submission(f"sbx{stamp}-{letter}", names[pid],
                         f"jidhr-sbx-{stamp}-{letter}@example.invalid", "",
                         now_ms + ord(letter))
        client = sandbox_client(budget)
        calls = counting(client)
        out = endowment_sync.run(hubspot=Hub([sub]), csuite=client,
                                 run_mode="live", budget_left=budget)
        reads = [c for c in calls if c not in ("note/create", "task/create")]
        return {"case": label, "profile_id": pid,
                "writes": list(client.write_budget.log),
                "reads": {e: reads.count(e) for e in sorted(set(reads))},
                "reads_total": len(reads),
                "added": [{k: e.get(k) for k in ("plan", "profile_id",
                                                 "note_id", "task_id")}
                          for e in out["added"]],
                "existing_task": [
                    {"task_id": e["open_task"]["task_id"],
                     "recorded": e.get("recorded")}
                    for e in out["existing_task"]],
                "held": [e["reason"] for e in out["held"]],
                "deferred": [e["reason"] for e in out["deferred"]]}, out

    rows, sections = [], []
    for label, pid, letter, budget in (
            ("A 21762: open task 1199, Ola, endowment type", "21762", "g", 2),
            ("B only open task: endowment type, another assignee",
             other_assignee_org, "h", 2),
            ("C only open task: Ola, another type", other_type_org, "i", 2)):
        row, out = case(label, pid, letter, budget)
        rows.append(row)
        sections.append(out)

    from sync import daf_sync
    print(json.dumps({"stamp": stamp, "endowment_task_type": endowment_type,
                      "setup_writes": made, "cases": rows,
                      "case_writes_total": sum(len(r["writes"])
                                               for r in rows),
                      "jidhr_rows": [x["endpoint"] for x in state["records"]],
                      "database_touched": False}, indent=2, default=str))
    print("----- the endowment block")
    merged = {"form": "endowment", "label": "Endowment", "mode": "live",
              "added": [x for s in sections for x in s["added"]],
              "held": [x for s in sections for x in s["held"]],
              "deferred": [x for s in sections for x in s["deferred"]],
              "existing_task": [x for s in sections
                                for x in s["existing_task"]],
              "budget": 10, "budget_needed": 0, "budget_allowed": 0}
    print("\n".join(endowment_sync.section_text(merged)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--discover", action="store_true")
    parser.add_argument("--run", nargs=2, metavar=("NOFEDID_ID", "FEDID_ID"))
    parser.add_argument("--discover-owner", action="store_true")
    parser.add_argument("--run-owner", nargs=3,
                        metavar=("OTHER_TYPE_ORG", "OTHER_ASSIGNEE_ORG",
                                 "OTHER_TYPE"))
    args = parser.parse_args()
    if args.discover_owner:
        discover_owner()
    elif args.run_owner:
        run_owner_cases(*args.run_owner)
    elif args.discover:
        discover()
    elif args.run:
        run_cases(*args.run)
    else:
        parser.print_help()
