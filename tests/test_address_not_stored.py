"""The submitted address is sent when it is complete, and named when it is not.

A complete address now reaches CSuite as a nested `address` object of
`address`, `city`, `state`, `zipcode` — the confirmed create shape, VERIFIED
2026-10-01 on sentinel 21660, where all four stored and CSuite derived
primary_citystatezip, primary_address_string and primary_country itself.

An INCOMPLETE address is not sent at all. CSuite derives those three fields
from the parts it is given, so a half-filled object puts a malformed address
on the record, and an address nobody can trust is worse than one a person is
asked to enter.

The DAF Inquiry Form and the Endowment Inquiry Form both carry address, city,
state and zip as REQUIRED fields, so a complete address is the normal case.
Until 2026-10-01 none of it was even parsed.

No network.
"""

import pytest

from clients.csuite import (CSuiteClient, build_address,
                            normalize_phone)
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
        response = {"success": True, "data": {"profile_id": 21660}}
        _, phone_warning = normalize_phone(kwargs.get("phone"))
        if phone_warning:
            response["phone_warning"] = phone_warning
        _, address_warning = build_address(
            kwargs.get("address_line"), kwargs.get("city"),
            kwargs.get("state"), kwargs.get("zipcode"))
        if address_warning:
            response["address_warning"] = address_warning
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


def test_the_workflow_passes_the_four_parsed_address_parts(monkeypatch):
    csuite = CSuiteSpy()
    run(monkeypatch, csuite=csuite)

    assert len(csuite.calls) == 1
    call = csuite.calls[0]
    assert set(call) == {"first_name", "last_name", "email", "phone",
                         "address_line", "city", "state", "zipcode"}
    assert call["address_line"] == "51 Test Way"
    assert call["city"] == "Fairfax"
    assert call["state"] == "VA"
    assert call["zipcode"] == "22031"
    assert "address2" not in call, "not in the confirmed set"


def test_the_create_sends_the_confirmed_nested_shape():
    client = Client()
    client.create_individual_profile("A", "B", address_line="51 Test Way",
                                     city="Fairfax", state="VA",
                                     zipcode="22031")
    sent = client.sent[0][1]
    assert sent["address"] == {"address": "51 Test Way", "city": "Fairfax",
                               "state": "VA", "zipcode": "22031"}


def test_a_partial_address_is_never_sent_as_a_partial_object():
    """CSuite derives primary_address_string from the parts, so half an
    address becomes a malformed one on the record."""
    client = Client()
    result = client.create_individual_profile(
        "A", "B", address_line="51 Test Way", city="Fairfax", state="VA")

    assert "address" not in client.sent[0][1]
    assert "Address incomplete" in result["address_warning"]
    assert "51 Test Way, Fairfax, VA" in result["address_warning"]


# ---------------------------------------------------------------------------
# It reaches the user
# ---------------------------------------------------------------------------

def test_a_complete_address_produces_no_warning_at_all(monkeypatch):
    """It is stored now. There is nothing to tell anyone."""
    reply, _ = run(monkeypatch)

    assert "Address not stored" not in reply
    assert "Address incomplete" not in reply
    assert "🏠" not in reply


def test_an_incomplete_address_warning_reaches_the_confirmation_text(
        monkeypatch):
    reply, _ = run(monkeypatch, sub=submission(zip=None))

    assert "🏠 Address incomplete, not stored: '51 Test Way, Fairfax, VA'. " \
           "Enter it manually." in reply


def test_no_address_submitted_means_no_address_line(monkeypatch):
    bare = {"values": [{"name": "firstname", "value": "HUBSYNC"},
                       {"name": "lastname", "value": "SENTINEL"},
                       {"name": "email", "value": "a@b.invalid"}]}
    reply, _ = run(monkeypatch, sub=bare)

    assert "Address not stored" not in reply
    assert "🏠" not in reply


def test_a_complete_submission_adds_no_lines_at_all(monkeypatch):
    reply, _ = run(monkeypatch)

    assert "Phone not stored" not in reply
    assert "Address" not in reply


def test_a_bad_phone_with_a_COMPLETE_address_warns_about_the_phone_only(
        monkeypatch):
    """The address is stored, so only the phone has anything to report."""
    csuite = CSuiteSpy()
    reply, state = run(monkeypatch, sub=submission(phone="555-0122"),
                       csuite=csuite)

    assert state["profile_id"] == 21660, "the profile was still created"
    assert "📱 Profile created. Phone not stored: '555-0122' isn't a " \
           "10-digit US number. Enter it manually." in reply
    assert "Address" not in reply
    assert "Failed to create" not in reply


def test_a_bad_phone_and_an_incomplete_address_both_warn(monkeypatch):
    """The two warnings are independent, and neither blocks the create."""
    csuite = CSuiteSpy()
    reply, state = run(monkeypatch,
                       sub=submission(phone="555-0122", zip=None),
                       csuite=csuite)

    assert state["profile_id"] == 21660
    assert len(csuite.calls) == 1
    assert "Phone not stored: '555-0122'" in reply
    assert "🏠 Address incomplete, not stored: '51 Test Way, Fairfax, VA'." \
        in reply
    assert "Failed to create" not in reply


def test_neither_warning_is_ever_only_in_a_log(monkeypatch, caplog):
    reply, _ = run(monkeypatch, sub=submission(phone="n/a", state=None))

    for fragment in ("Phone not stored", "Address incomplete"):
        assert fragment in reply, f"{fragment} must be visible to the user"
