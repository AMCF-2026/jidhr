"""A phone number CSuite would refuse is left out, loudly.

CSuite VALIDATES `phone_number` and rejects the whole create on a value it
dislikes — measured 2026-09-30: `phone_number: phone [5550100] is not
valid`, HTTP 400, no profile created. Its predecessor
`primary_phone_number` was never validated because it was never
recognised, so a bad number used to be dropped in silence.

That means fixing the field name turned one bad digit in an unvalidated
HubSpot form field into a blocked profile. The number is checked here
instead: the profile is always created, and a number CSuite would refuse is
named to a human. Losing a phone costs one profile/edit; losing the profile
costs the workflow.

No network.
"""

import pytest

from clients.csuite import CSuiteClient, normalize_phone
from config import Config
from intents import daf_workflow


# ---------------------------------------------------------------------------
# normalize_phone
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("703-555-0100", "7035550100"),
    ("(703) 555-0100", "7035550100"),
    ("1-703-555-0100", "7035550100"),      # US country code dropped
    ("+1 703 555 0100", "7035550100"),     # same, written differently
])
def test_a_ten_digit_number_comes_back_clean_and_unwarned(raw, expected):
    value, warning = normalize_phone(raw)
    assert value == expected
    assert warning is None


@pytest.mark.parametrize("raw", [
    "555-0100",             # seven digits: the exact value CSuite 400'd on
    "n/a",                  # free text
    "703-555-0100 ext 4",   # eleven digits that are not a country code
    "+44 20 7946 0958",     # international
    "",
    None,
])
def test_anything_else_is_refused_with_a_warning(raw):
    value, warning = normalize_phone(raw)
    assert value is None
    assert warning, "a dropped number without a warning is a silent loss"
    assert "isn't a 10-digit US number" in warning
    assert "Enter it manually" in warning


def test_the_warning_quotes_the_raw_value_so_it_can_be_acted_on():
    _, warning = normalize_phone("703-555-0100 ext 4")
    assert "703-555-0100 ext 4" in warning


def test_an_eleven_digit_number_not_starting_with_one_is_not_trimmed():
    """Trimming any eleven-digit value would turn a typo into a wrong
    number that looks right, which is worse than refusing it."""
    value, warning = normalize_phone("70355501004")
    assert value is None
    assert warning


def test_the_result_is_never_silent():
    for raw in ("703-555-0100", "555-0100", "", None, "n/a", "+44 1234 5678"):
        value, warning = normalize_phone(raw)
        assert (value is None) == (warning is not None)


# ---------------------------------------------------------------------------
# The create keeps going
# ---------------------------------------------------------------------------

class Client(CSuiteClient):
    def __init__(self):
        self.sent = []

    def _request(self, endpoint, data=None):
        self.sent.append((endpoint, dict(data or {})))
        return {"success": True, "data": {"profile_id": 21659},
                "http_status": 200, "outcome": "ok"}

    @property
    def payload(self):
        return self.sent[0][1]


def test_a_good_number_is_sent_as_ten_bare_digits():
    client = Client()
    result = client.create_individual_profile("A", "B", phone="(703) 555-0100")

    assert client.payload["phone_number"] == "7035550100"
    assert "phone_warning" not in result


def test_a_bad_number_is_omitted_and_the_profile_is_still_created():
    client = Client()
    result = client.create_individual_profile("A", "B", email="a@b.invalid",
                                              phone="555-0100")

    assert "phone_number" not in client.payload
    assert client.payload["email"] == "a@b.invalid", "the create went ahead"
    assert result["success"] is True
    assert "555-0100" in result["phone_warning"]


def test_no_phone_at_all_warns_about_nothing():
    """A blank form field is not a malformed number.

    normalize_phone("") returns a warning, as specified — but the create
    only consults it when a phone was actually submitted. Warning on every
    submission without a phone would train people to ignore the warning
    that matters.
    """
    client = Client()
    result = client.create_individual_profile("A", "B", phone=None)

    assert "phone_number" not in client.payload
    assert "phone_warning" not in result


# ---------------------------------------------------------------------------
# It reaches the user
# ---------------------------------------------------------------------------

class CSuiteWithBadPhone:
    def create_individual_profile(self, **kwargs):
        number, warning = normalize_phone(kwargs.get("phone"))
        response = {"success": True, "data": {"profile_id": 21659}}
        if warning:
            response["phone_warning"] = warning
        return response

    def create_fund(self, **kwargs):
        return {"success": True, "data": {"funit_id": 9001}}


class HubSpot:
    def search_contact_by_email(self, email):
        return {"results": [{"id": "70123"}]}

    def update_contact_by_email(self, email, properties):
        return {"id": "70123"}

    def get_open_tickets(self):
        return {"results": []}


def run(monkeypatch, phone):
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm",
             "submission_data": {"first_name": "Testy", "last_name": "McTest",
                                 "email": "testy@example.invalid",
                                 "phone": phone},
             "profile_id": None, "funit_id": None, "ticket_id": None}
    return daf_workflow._step_create("yes", state, HubSpot(),
                                     CSuiteWithBadPhone())


def test_the_warning_reaches_the_confirmation_text(monkeypatch):
    reply = run(monkeypatch, "555-0100")

    assert "Profile created." in reply
    assert "555-0100" in reply
    assert "isn't a 10-digit US number" in reply
    assert "Enter it manually" in reply


def test_a_good_number_adds_no_noise_to_the_confirmation(monkeypatch):
    reply = run(monkeypatch, "703-555-0100")

    assert "isn't a 10-digit US number" not in reply
    assert "Phone not stored" not in reply
