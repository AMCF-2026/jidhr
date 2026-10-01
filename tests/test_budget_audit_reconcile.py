"""The budget cannot detect its own bypass. The audit table can.

2026-10-01: a run wrapped the CSuite client in a proxy whose `_request` was
never reached, because `proxy.create_individual_profile` returned the inner
client's bound method. The budget counted 0 while two records were created,
and the run printed "budget 0 of 1" and looked clean.

A count kept in this process cannot notice that it was skipped. A count kept
in Postgres by the code that actually sends the requests can. So both are
kept, and at the end of a write run they must agree.

No network, no database.
"""

import pytest

from clients.csuite import (CONFIRMED_INPUT_FIELDS, INCONCLUSIVE_PROBES,
                            KNOWN_INVALID_INPUT_FIELDS, UnconfirmedField,
                            check_input_fields)
from sync.sandbox_writes import (BudgetAuditMismatch, WriteBudget,
                                 assert_budget_matches_audit)

SINCE = "2026-10-01 00:00:00"


def audit(*endpoints):
    """A fake execute_query returning one row per endpoint."""
    rows = [{"endpoint": e, "status": "success"} for e in endpoints]
    return lambda sql, params=None: rows


def spent(n):
    budget = WriteBudget(10)
    for _ in range(n):
        budget.spend("profile/edit")
    return budget


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("n", [0, 1, 2, 3])
def test_agreement_returns_the_count(n):
    assert assert_budget_matches_audit(
        spent(n), SINCE, query=audit(*["profile/edit"] * n)) == n


def test_more_audited_than_counted_is_a_breached_cap():
    """The 2026-10-01 failure, as a test."""
    with pytest.raises(BudgetAuditMismatch) as caught:
        assert_budget_matches_audit(
            spent(0), SINCE,
            query=audit("profile/create/individual", "funit/create"))

    message = str(caught.value)
    assert "the cap did not hold" in message
    assert "budget.used=0" in message
    assert "funit/create" in message, "name what went out uncounted"


def test_fewer_audited_than_counted_is_a_failing_ledger():
    """The other direction matters just as much.

    A pre-flight audit that misses rows is the one thing this repository
    relies on to know what it did.
    """
    with pytest.raises(BudgetAuditMismatch) as caught:
        assert_budget_matches_audit(spent(2), SINCE,
                                    query=audit("profile/edit"))

    message = str(caught.value)
    assert "missing rows" in message
    assert "budget.used=2" in message


def test_no_rows_at_all_is_handled_like_zero():
    assert assert_budget_matches_audit(
        spent(0), SINCE, query=lambda sql, params=None: None) == 0
    with pytest.raises(BudgetAuditMismatch):
        assert_budget_matches_audit(spent(1), SINCE,
                                    query=lambda sql, params=None: None)


# ---------------------------------------------------------------------------
# INCONCLUSIVE_PROBES is a record, not a gate
# ---------------------------------------------------------------------------

def test_address_city_is_confirmed_and_its_open_question_is_on_record():
    """It was never invalid: sent with the other three keys it stores.

    Sent ALONE it stores nothing — twice, against a profile with no address
    and against one with a full address. The entry stays in
    INCONCLUSIVE_PROBES because what is still unknown is the smallest working
    set, and deleting the measurement would invite someone to re-run the same
    probe and draw the same wrong conclusion sandbox-12 drew.
    """
    assert "address.city" not in KNOWN_INVALID_INPUT_FIELDS
    assert "address.city" in CONFIRMED_INPUT_FIELDS
    assert "smallest working set is untested" in \
        INCONCLUSIVE_PROBES["address.city"]


def test_an_inconclusive_probe_never_blocks_a_request():
    """"We learned nothing" is not "this is wrong", and only the second may
    stop a call. The gate does not consult this list at all."""
    for name in INCONCLUSIVE_PROBES:
        assert name not in KNOWN_INVALID_INPUT_FIELDS

    # A name that is ONLY in the inconclusive list is refused for not being
    # confirmed yet, and never carries the "proven not to work" evidence.
    with pytest.raises(UnconfirmedField) as caught:
        check_input_fields(["address.line1"], "profile/edit")
    assert "Proven not to work" not in str(caught.value)


def test_nothing_is_both_inconclusive_and_proven_wrong():
    """The invariant that matters: a probe that proved nothing must never sit
    beside a measurement that proved something wrong.

    Overlap with CONFIRMED is fine and expected — a resolved probe keeps its
    record, which is how the dependency behind it stays written down.
    """
    assert not (set(INCONCLUSIVE_PROBES) & set(KNOWN_INVALID_INPUT_FIELDS))


def test_the_nested_and_plain_string_address_stay_proven_wrong_on_edit():
    """Neither is the documented EDIT shape, and both were measured alone."""
    from clients.csuite import ENDPOINT_CONFIRMED_FIELDS

    assert "address" in KNOWN_INVALID_INPUT_FIELDS
    evidence = KNOWN_INVALID_INPUT_FIELDS["address"]
    assert "plain string" in evidence and "nested" in evidence
    assert "profile/EDIT" in evidence

    # One name, two answers. The gate asks the endpoint.
    with pytest.raises(UnconfirmedField):
        check_input_fields(["address"], "profile/edit")
    check_input_fields(["address"], "profile/create/individual")
    assert "profile/create/individual" in ENDPOINT_CONFIRMED_FIELDS["address"]


def test_the_create_no_longer_refuses_an_address_at_all():
    """It sends one. The refusal message that called the documented shapes
    dead is gone, because the shape was confirmed on 2026-10-01."""
    from clients.csuite import CSuiteClient

    class Client(CSuiteClient):
        def __init__(self):
            self.sent = []

        def _request(self, endpoint, data=None):
            self.sent.append((endpoint, dict(data or {})))
            return {"success": True, "data": {"profile_id": 1}}

    client = Client()
    client.create_individual_profile("A", "B", address_line="1 Test Way",
                                     city="Fairfax", state="VA",
                                     zipcode="22031")
    assert client.sent[0][1]["address"]["city"] == "Fairfax"


# ---------------------------------------------------------------------------
# The window comes from the database's clock, not the machine's
# ---------------------------------------------------------------------------

def test_the_window_start_is_read_from_the_audit_database():
    """write_audit.created_at is UTC; a developer machine is not.

    2026-10-01: a local datetime.now() opened the window four hours in the
    past, swept in eight rows from three earlier tasks, and reported a
    breached cap on a run that had reconciled perfectly. A false alarm on a
    cap is not harmless — the next person to see one assumes this one was
    false too.
    """
    from sync.sandbox_writes import audit_now

    asked = []

    def query(sql, params=None):
        asked.append(sql)
        return [{"db_now": "2026-10-01 15:00:03+00:00"}]

    assert audit_now(query=query) == "2026-10-01 15:00:03+00:00"
    assert "now()" in asked[0], "the clock must come from the database"


def test_an_unreadable_clock_is_a_failure_not_a_default():
    from sync.sandbox_writes import audit_now

    with pytest.raises(BudgetAuditMismatch) as caught:
        audit_now(query=lambda sql, params=None: [])
    assert "no write window can be opened" in str(caught.value)


def test_no_window_is_refused_rather_than_counting_everything():
    with pytest.raises(BudgetAuditMismatch) as caught:
        assert_budget_matches_audit(spent(1), None, query=audit("x"))
    assert "every write ever audited" in str(caught.value)


def test_a_run_split_across_processes_reconciles_its_own_window():
    """budget.used can be carried in; the window cannot.

    2026-10-01: step 2 ran as a separate process seeded with used=1 from
    step 1, so used=2 was compared against a window holding step 2's single
    row and the check reported a missing ledger. `expected` names the delta
    the window actually covers.
    """
    carried = spent(2)          # 1 from an earlier process, 1 from this one
    assert assert_budget_matches_audit(
        carried, SINCE, query=audit("profile/create/individual"),
        expected=1) == 1

    with pytest.raises(BudgetAuditMismatch):
        assert_budget_matches_audit(
            carried, SINCE, query=audit("profile/create/individual"))
