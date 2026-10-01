"""funit/create is read back, and a production client cannot write by default.

Two gaps sandbox-11 left open.

**The fund.** It created fund 1564 with no read-back of any kind. What that
fund actually holds is known only because I chose to look afterwards. A fund
pointed at the wrong cash account is a finance problem, not a data-entry one.

**The budget.** The cap was 1 and two writes went out, because the guard was
on a proxy the calls never traversed. It lives on the client now, and
Config.CSUITE_WRITE_BUDGET defaults to 0 — so a client nobody deliberately
armed cannot send a write at all.

No network.
"""

import pytest

from tests.csuite_doubles import NoDuplicates, contact

from clients.csuite import CSuiteClient
from config import Config
from sync.readback import FieldDropped, ReadBackUnavailable, verify_fund
from sync.sandbox_writes import WriteBudget, WriteBudgetExceeded

# The fund create exactly as the DAF workflow sends it.
SENT = {"name": "SENTINEL Family Fund", "fgroup_id": 1002,
        "cash_account_id": 1069}


def fund_display(**overrides):
    """funit/display as CSuite returns it — note `fund_name` and `account_id`."""
    base = {"funit_id": 1564, "fund_name": "SENTINEL Family Fund",
            "fgroup_id": 1002, "account_id": 1069, "fund_open": True,
            "fund_open_date": "2026-10-01"}
    base.update(overrides)
    return base


def reader(record):
    def read(endpoint, body):
        return {"success": True, "data": record}
    return read


class Client(CSuiteClient):
    """Real create_fund, fake transport, explicit budget."""

    _DEFAULT = object()

    def __init__(self, create_response=None, display=_DEFAULT, budget=10):
        self.sent = []
        self.verify_writes = True
        self.write_budget = WriteBudget(budget)
        # A sentinel, so display=None means "the fund cannot be read" rather
        # than "use the default".
        self._display = (fund_display() if display is Client._DEFAULT
                         else display)
        self._create_response = create_response or {
            "success": True, "data": {"funit_id": 1564}, "http_status": 200}

    def _request(self, endpoint, data=None):
        self.sent.append((endpoint, dict(data or {})))
        if endpoint == "funit/display":
            return {"success": True, "data": self._display}
        return dict(self._create_response)


# ---------------------------------------------------------------------------
# STEP 3 — the read-back itself
# ---------------------------------------------------------------------------

def test_a_fund_that_holds_what_was_sent_verifies():
    assert verify_fund(reader(fund_display()), SENT, 1564)["funit_id"] == 1564


def test_the_name_is_read_back_from_fund_name():
    """CSuite takes `name` and returns `fund_name`. Comparing like for like
    would report every fund as wrong."""
    dropped_reader = reader(fund_display(fund_name="Something Else"))
    with pytest.raises(FieldDropped) as caught:
        verify_fund(dropped_reader, SENT, 1564)
    assert "name" in caught.value.dropped


def test_the_cash_account_is_read_back_from_account_id():
    """`cash_account_id` goes in and `account_id` comes out — the same
    mismatch that recorded fund 1564's cash account as its target_id."""
    with pytest.raises(FieldDropped) as caught:
        verify_fund(reader(fund_display(account_id=9999)), SENT, 1564)
    assert "cash_account_id" in caught.value.dropped


def test_a_wrong_fund_group_is_caught():
    with pytest.raises(FieldDropped) as caught:
        verify_fund(reader(fund_display(fgroup_id=1008)), SENT, 1564)
    assert "fgroup_id" in caught.value.dropped


@pytest.mark.parametrize("record", [None, {}, {"funit_id": None}, "nope", []])
def test_a_missing_fund_is_unavailable_not_a_mismatch(record):
    """"Could not check" and "is wrong" are different claims."""
    with pytest.raises(ReadBackUnavailable):
        verify_fund(reader(record), SENT, 1564)


# ---------------------------------------------------------------------------
# STEP 3 — through create_fund, and visible to the user
# ---------------------------------------------------------------------------

def test_a_good_fund_create_is_marked_verified():
    client = Client()
    result = client.create_fund("SENTINEL Family Fund", 1002, 1069)

    assert result["verified"] is True
    assert "fund_warning" not in result
    assert [e for e, _ in client.sent] == ["funit/create", "funit/display"]


def test_a_mismatched_fund_warns_without_claiming_the_create_failed():
    """The fund exists. Saying it failed would invite a second one, and
    CSuite has no idempotency key."""
    client = Client(display=fund_display(account_id=9999))
    result = client.create_fund("SENTINEL Family Fund", 1002, 1069)

    assert result["success"] is True
    assert result["verified"] is False
    assert "cash_account_id" in result["fields_dropped"]
    assert "1564" in result["fund_warning"]
    assert "cash account" in result["fund_warning"]


def test_an_unreadable_fund_is_unverified_not_wrong():
    client = Client(display=None)
    result = client.create_fund("SENTINEL Family Fund", 1002, 1069)

    assert result["verified"] is None
    assert "could not be read back" in result["fund_warning"]
    assert "fields_dropped" not in result


def test_no_funit_id_means_unverified():
    client = Client(create_response={"success": True, "data": None,
                                     "http_status": 200})
    result = client.create_fund("X", 1002, 1069)

    assert result["verified"] is None
    assert [e for e, _ in client.sent] == ["funit/create"]


def test_a_failed_create_is_not_read_back():
    client = Client(create_response={"success": False, "error": "nope",
                                     "http_status": 400})
    result = client.create_fund("X", 1002, 1069)

    assert "verified" not in result
    assert [e for e, _ in client.sent] == ["funit/create"]


def test_the_fund_warning_reaches_the_confirmation_text(monkeypatch):
    from intents import daf_workflow

    class CSuite(NoDuplicates):
        def create_individual_profile(self, **kwargs):
            return {"success": True, "data": {"profile_id": 21662}}

        def create_fund(self, **kwargs):
            return {"success": True, "data": {"funit_id": 1565},
                    "verified": False,
                    "fund_warning": "⚠️ Fund 1565 was created but CSuite did "
                                    "not store: cash_account_id. Check its "
                                    "fund group and cash account in CSuite "
                                    "before using it."}

    class HubSpot:
        def search_contact_by_email(self, email):
            return contact(self.contact_id if hasattr(self, "contact_id") else "70123")

        def update_contact_by_email(self, email, properties):
            return {"id": "70123"}

        def get_open_tickets(self):
            return {"results": []}

    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    # The fund path is off by default from 2026-10-01; this test is about the
    # fund read-back, so it arms the commitment-stage flag explicitly.
    monkeypatch.setattr(Config, "CSUITE_DAF_FUND_CREATE_ENABLED", True)
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm",
             "submission_data": {"first_name": "A", "last_name": "B",
                                 "email": "a@b.invalid"},
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, HubSpot(), CSuite())

    assert "Fund 1565 was created but CSuite did not store" in reply
    assert "cash_account_id" in reply


# ---------------------------------------------------------------------------
# STEP 4 — the production budget
# ---------------------------------------------------------------------------

def test_the_config_budget_defaults_to_zero():
    assert Config.CSUITE_WRITE_BUDGET == 0


@pytest.mark.parametrize("raw,expected", [
    ("0", 0), ("", 0), ("  ", 0), ("two", 0), ("-1", 0), ("1.5", 0),
    ("1", 1), ("2", 2), (" 3 ", 3),
])
def test_an_unparseable_budget_fails_closed(raw, expected):
    """A typo must mean "no writes", never "unlimited"."""
    import importlib

    import config
    monkey = __import__("os")
    monkey.environ["CSUITE_WRITE_BUDGET"] = raw
    try:
        importlib.reload(config)
        assert config.Config.CSUITE_WRITE_BUDGET == expected
    finally:
        del monkey.environ["CSUITE_WRITE_BUDGET"]
        importlib.reload(config)


def test_a_client_built_from_config_cannot_write_by_default(monkeypatch):
    """The production default. Nothing leaves without a deliberate budget."""
    import clients.csuite as mod

    monkeypatch.setattr(Config, "CSUITE_WRITE_BUDGET", 0)
    monkeypatch.setattr(mod, "reserve_write", lambda *a, **kw: {"id": 1})
    monkeypatch.setattr(mod, "complete_write", lambda *a, **kw: None)

    client = CSuiteClient.__new__(CSuiteClient)
    client.api_key = "k"
    client.api_secret = "s"
    client.base_url = "https://amuslimcf.fcsuite.com/api/v2"
    client.env = "live"
    client.verify_writes = False
    client.write_budget = client._budget_from_config()

    posted = []

    class Session:
        def post(_self, url, **kwargs):
            posted.append(url)
            raise AssertionError("a write escaped a zero budget")

    client.session = Session()

    assert client.write_budget.limit == 0
    with pytest.raises(WriteBudgetExceeded):
        client._request("profile/create/individual", {"first_name": "A"})
    assert posted == []


def test_a_raised_config_budget_allows_exactly_that_many(monkeypatch):
    monkeypatch.setattr(Config, "CSUITE_WRITE_BUDGET", 2)
    client = CSuiteClient.__new__(CSuiteClient)
    budget = client._budget_from_config()

    assert budget.limit == 2
    budget.spend("profile/create/individual")
    budget.spend("funit/create")
    with pytest.raises(WriteBudgetExceeded):
        budget.spend("profile/edit")


def test_reads_are_never_blocked_by_a_zero_budget(monkeypatch):
    """Step 2 of this task read production with write_budget=0."""
    client = CSuiteClient.__new__(CSuiteClient)
    client.api_key = "k"
    client.api_secret = "s"
    client.base_url = "https://amuslimcf.fcsuite.com/api/v2"
    client.env = "live"
    client.verify_writes = False
    client.write_budget = WriteBudget(0)

    class Session:
        def post(_self, url, **kwargs):
            class R:
                status_code = 200
                text = '{"success":1,"data":{"results":[]}}'

                def json(self):
                    return {"success": 1, "data": {"results": []}}
            return R()

    client.session = Session()
    assert client._request("account/list", {"view_limit": 5})["success"] is True
    assert client.write_budget.used == 0
