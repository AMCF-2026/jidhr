"""The submitted address is reported, never sent, and never silent.

CSuite's address INPUT name is unknown. Nine candidates have been
eliminated, each by a sandbox write and a read-back:
primary_address_string, primary_address, primary_city, primary_state,
primary_zipcode, address.city, and `address` both as a plain string and as a
nested object — every one accepted with HTTP 200 and stored nothing.

Meanwhile the DAF Inquiry Form and the Endowment Inquiry Form both carry
address, city, state and zip as REQUIRED fields, so every submission has a
full address. It was not mapped in _FIELD_MAP, so it was discarded at the
parse step and nobody downstream could tell it had been submitted at all.

It is mapped now so it can be shown to a person. It is still not sent.

No network.
"""

import pytest

from clients.csuite import CSuiteClient, normalize_phone
from config import Config
from intents import daf_workflow
from intents.daf_workflow import _parse_submission, submitted_address


def submission(**fields):
    base = {"firstname": "HUBSYNC", "lastname": "SENTINEL 4 - SANDBOX ONLY",
            "email": "hubsync-sentinel-4@example.invalid",
            "phone": "(703) 555-0122", "address": "51 Test Way",
            "city": "Fairfax", "state": "VA", "zip": "22031"}
    base.update(fields)
    return {"values": [{"name": k, "value": v} for k, v in base.items() if v]}


# ---------------------------------------------------------------------------
# The address is parsed at all
# ---------------------------------------------------------------------------

def test_the_four_required_form_fields_are_parsed():
    """They were dropped on the floor before this. Both inquiry forms mark
    all four required, so this was every submission."""
    parsed = _parse_submission(submission())

    assert parsed["address_street"] == "51 Test Way"
    assert parsed["address_city"] == "Fairfax"
    assert parsed["address_state"] == "VA"
    assert parsed["address_zip"] == "22031"


def test_the_address_reads_as_one_line_a_person_can_retype():
    assert submitted_address(_parse_submission(submission())) == \
        "51 Test Way, Fairfax, VA 22031"


def test_a_second_street_line_is_kept():
    parsed = _parse_submission(submission(address2="Suite 300"))
    assert submitted_address(parsed) == \
        "51 Test Way Suite 300, Fairfax, VA 22031"


@pytest.mark.parametrize("partial,expected", [
    ({"address_city": "Fairfax"}, "Fairfax"),
    ({"address_street": "51 Test Way"}, "51 Test Way"),
    ({"address_state": "VA", "address_zip": "22031"}, "VA 22031"),
])
def test_a_partial_address_is_still_reported(partial, expected):
    """Half an address is still something to retype."""
    assert submitted_address(partial) == expected


def test_no_address_is_an_empty_string_not_a_stray_comma():
    assert submitted_address({}) == ""
    assert submitted_address({"address_street": "", "address_city": ""}) == ""


# ---------------------------------------------------------------------------
# It is never sent
# ---------------------------------------------------------------------------

class Client(CSuiteClient):
    def __init__(self):
        self.sent = []

    def _request(self, endpoint, data=None):
        self.sent.append((endpoint, dict(data or {})))
        return {"success": True, "data": {"profile_id": 21660},
                "http_status": 200, "outcome": "ok"}


class CSuiteSpy:
    """Stands in for the client; records the kwargs the workflow passes."""

    def __init__(self, phone_warning=None):
        self.calls = []
        self.phone_warning = phone_warning

    def create_individual_profile(self, **kwargs):
        self.calls.append(kwargs)
        number, warning = normalize_phone(kwargs.get("phone"))
        response = {"success": True, "data": {"profile_id": 21660}}
        if warning:
            response["phone_warning"] = warning
        return response

    def create_fund(self, **kwargs):
        return {"success": True, "data": {"funit_id": 9002}}


class HubSpot:
    def __init__(self):
        self.calls = []

    def search_contact_by_email(self, email):
        self.calls.append("search_contact_by_email")
        return {"results": [{"id": "70123"}]}

    def update_contact_by_email(self, email, properties):
        self.calls.append(f"update_contact_by_email {sorted(properties)}")
        return {"id": "70123"}

    def get_open_tickets(self):
        self.calls.append("get_open_tickets")
        return {"results": []}


def run(monkeypatch, sub=None, csuite=None, hubspot=None):
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    parsed = _parse_submission(sub or submission())
    state = {"active": True, "workflow_type": "daf", "type": "daf",
             "step": "confirm", "submission_data": parsed,
             "profile_id": None, "funit_id": None, "ticket_id": None}
    reply = daf_workflow._step_create("yes", state, hubspot or HubSpot(),
                                      csuite or CSuiteSpy())
    return reply, state


def test_the_workflow_does_not_pass_address_to_the_create(monkeypatch):
    csuite = CSuiteSpy()
    run(monkeypatch, csuite=csuite)

    assert len(csuite.calls) == 1
    assert set(csuite.calls[0]) == {"first_name", "last_name", "email",
                                    "phone"}
    assert "address" not in csuite.calls[0]


def test_the_create_itself_refuses_an_address_if_anyone_adds_one_later():
    """Belt and braces: the method raises rather than dropping it, so
    re-adding the argument here cannot reintroduce a silent loss."""
    with pytest.raises(ValueError):
        Client().create_individual_profile("A", "B", address="51 Test Way")


# ---------------------------------------------------------------------------
# It reaches the user
# ---------------------------------------------------------------------------

def test_the_address_warning_reaches_the_confirmation_text(monkeypatch):
    reply, _ = run(monkeypatch)

    assert "🏠 Address not stored in CSuite yet: '51 Test Way, Fairfax, " \
           "VA 22031'. Enter it manually." in reply


def test_no_address_submitted_means_no_address_line(monkeypatch):
    bare = {"values": [{"name": "firstname", "value": "HUBSYNC"},
                       {"name": "lastname", "value": "SENTINEL"},
                       {"name": "email", "value": "a@b.invalid"}]}
    reply, _ = run(monkeypatch, sub=bare)

    assert "Address not stored" not in reply
    assert "🏠" not in reply


def test_a_good_phone_adds_no_phone_line_but_the_address_line_stays(monkeypatch):
    reply, _ = run(monkeypatch)

    assert "Phone not stored" not in reply
    assert "Address not stored" in reply


def test_a_bad_phone_and_an_address_both_warn_and_the_profile_is_created(
        monkeypatch):
    """The two warnings are independent, and neither blocks the create."""
    csuite = CSuiteSpy()
    reply, state = run(monkeypatch, sub=submission(phone="555-0122"),
                       csuite=csuite)

    assert state["profile_id"] == 21660, "the profile was still created"
    assert len(csuite.calls) == 1
    assert "📱 Profile created. Phone not stored: '555-0122' isn't a " \
           "10-digit US number. Enter it manually." in reply
    assert "🏠 Address not stored in CSuite yet: '51 Test Way, Fairfax, " \
           "VA 22031'. Enter it manually." in reply
    assert "Failed to create" not in reply


def test_neither_warning_is_ever_only_in_a_log(monkeypatch, caplog):
    reply, _ = run(monkeypatch, sub=submission(phone="n/a"))

    for fragment in ("Phone not stored", "Address not stored"):
        assert fragment in reply, f"{fragment} must be visible to the user"
