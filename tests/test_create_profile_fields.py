"""create_individual_profile sends the names CSuite actually takes.

From 2026-03-17 to 2026-10-01 it sent `primary_email`,
`primary_phone_number` and `primary_address_string`. All three are valid
`profile/display` OUTPUT names and none is an input name: CSuite answered
HTTP 200 with a profile_id and discarded every one of them.

Confirmed by read-back, and what the method sends now:
  email        -> primary_email           (create + edit, 2026-09-30)
  phone_number -> primary_phone_number    (edit, 2026-09-30)

The address is not sent at all, because its input name is still unknown.

No network.
"""

import pytest

from clients.csuite import (CONFIRMED_INPUT_FIELDS, CSuiteClient,
                            KNOWN_INVALID_INPUT_FIELDS, UnconfirmedField)


class Client(CSuiteClient):
    """Captures the payload; never touches a socket."""

    def __init__(self, response=None):
        self.sent = []
        self.response = response or {"success": True,
                                     "data": {"profile_id": 21627},
                                     "http_status": 200, "outcome": "ok"}

    def _request(self, endpoint, data=None):
        self.sent.append((endpoint, dict(data or {})))
        return self.response

    @property
    def payload(self):
        return self.sent[0][1]


# ---------------------------------------------------------------------------
# The two renames
# ---------------------------------------------------------------------------

def test_the_create_sends_email_not_primary_email():
    client = Client()
    client.create_individual_profile("HUBSYNC", "SENTINEL",
                                     email="a@b.invalid")

    assert client.payload["email"] == "a@b.invalid"
    assert "primary_email" not in client.payload


def test_the_create_sends_phone_number_not_primary_phone_number():
    client = Client()
    client.create_individual_profile("HUBSYNC", "SENTINEL",
                                     phone="7035550100")

    assert client.payload["phone_number"] == "7035550100"
    assert "primary_phone_number" not in client.payload


def test_the_full_payload_is_exactly_the_confirmed_names():
    client = Client()
    client.create_individual_profile("HUBSYNC", "SENTINEL 3 - SANDBOX ONLY",
                                     email="a@b.invalid", phone="7035550100")

    assert client.sent[0][0] == "profile/create/individual"
    assert set(client.payload) == {"first_name", "last_name", "email",
                                   "phone_number"}
    assert set(client.payload) <= set(CONFIRMED_INPUT_FIELDS)


@pytest.mark.parametrize("name", [
    "primary_email", "primary_phone_number", "primary_address_string",
])
def test_no_proven_wrong_name_is_sent_any_more(name):
    client = Client()
    client.create_individual_profile("A", "B", email="a@b.invalid",
                                     phone="7035550100")

    assert name in KNOWN_INVALID_INPUT_FIELDS
    assert name not in client.payload


def test_an_empty_email_or_phone_is_simply_absent():
    client = Client()
    client.create_individual_profile("A", "B", email="", phone=None)

    assert set(client.payload) == {"first_name", "last_name"}


# ---------------------------------------------------------------------------
# The address is refused, not dropped
# ---------------------------------------------------------------------------

def test_a_complete_address_is_sent_as_the_confirmed_nested_object():
    """VERIFIED 2026-10-01 on sentinel 21660: all four parts stored."""
    client = Client()
    client.create_individual_profile("A", "B", address_line="1 Test Way",
                                     city="Fairfax", state="VA",
                                     zipcode="22031")

    assert client.payload["address"] == {
        "address": "1 Test Way", "city": "Fairfax", "state": "VA",
        "zipcode": "22031"}


def test_no_address_at_all_is_not_an_incomplete_address():
    """A blank form is not a malformed one, and warns about nothing."""
    client = Client()
    result = client.create_individual_profile("A", "B")

    assert "address" not in client.payload
    assert "address_warning" not in result


# ---------------------------------------------------------------------------
# The kwargs gate still bites
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field", ["primary_email", "primary_city", "notes"])
def test_an_unconfirmed_kwarg_is_still_refused_before_the_request(field):
    client = Client()
    with pytest.raises(UnconfirmedField):
        client.create_individual_profile("A", "B", **{field: "x"})
    assert client.sent == []


# ---------------------------------------------------------------------------
# create_org_profile: same renames, and honest about never having run
# ---------------------------------------------------------------------------

def test_the_org_create_got_the_same_two_renames():
    client = Client()
    client.create_org_profile("Some Org", email="a@b.invalid",
                              phone="7035550100")

    assert client.sent[0][0] == "profile/create/org"
    assert client.payload["email"] == "a@b.invalid"
    assert client.payload["phone_number"] == "7035550100"
    assert "primary_email" not in client.payload
    assert "primary_phone_number" not in client.payload


def test_the_org_create_is_marked_unverified():
    """It has no caller in any commit and the endpoint has never been
    called. The renames are carried over from the individual endpoint, and
    carrying over is not confirming — CSuite's vocabularies differ per
    field, so they may differ per endpoint."""
    doc = CSuiteClient.create_org_profile.__doc__

    assert "UNVERIFIED" in doc
    assert "never been called" in doc


def test_the_dotted_address_keys_are_confirmed_together():
    """2026-10-01, profile/edit on 21626: all four sent together, all four
    stored, and CSuite derived primary_citystatezip, primary_address_string
    and primary_country by itself.

    One key ALONE stores nothing — measured twice, against a profile with no
    address and against one with a full address, nothing blanked either time.
    So profile/edit does not act on a single address key, and the earlier
    guess that it was a missing-row precondition was wrong. The smallest
    working set is still unknown.
    """
    from clients.csuite import INCONCLUSIVE_PROBES

    for key in ("address.address", "address.city", "address.state",
                "address.zipcode"):
        assert key in CONFIRMED_INPUT_FIELDS
        assert key not in KNOWN_INVALID_INPUT_FIELDS
    assert "ALONE" in INCONCLUSIVE_PROBES["address.city"]

    client = Client()
    client.edit_profile(21626, **{"address.city": "Fairfax"})
    assert client.payload["address.city"] == "Fairfax"


@pytest.mark.parametrize("shape", ["41 Test Way, Fairfax, VA 22031",
                                   {"city": "Vienna"}])
def test_the_address_key_is_wrong_whatever_shape_it_takes(shape):
    """2026-10-01, two isolated single-key edits of 21626: `address` as a
    plain string and `address` as a nested object. Both returned 200,
    success: true, 0 of 81 fields changed, modified_ts unchanged."""
    assert KNOWN_INVALID_INPUT_FIELDS["address"].startswith("dropped")
    with pytest.raises(UnconfirmedField):
        Client().edit_profile(21626, address=shape)


@pytest.mark.parametrize("missing", ["address_line", "city", "state",
                                    "zipcode"])
def test_any_missing_part_means_no_address_object_is_sent(missing):
    """CSuite derives primary_address_string and primary_citystatezip from
    the parts, so a partial object writes a malformed address."""
    parts = {"address_line": "1 Test Way", "city": "Fairfax", "state": "VA",
             "zipcode": "22031"}
    parts[missing] = None

    client = Client()
    result = client.create_individual_profile("A", "B", **parts)

    assert "address" not in client.payload
    assert "Address incomplete" in result["address_warning"]
    assert client.sent, "the profile is still created"


def test_the_payload_is_still_only_confirmed_names():
    client = Client()
    client.create_individual_profile("A", "B", email="a@b.invalid",
                                     phone="7035550100",
                                     address_line="1 Test Way", city="Fairfax",
                                     state="VA", zipcode="22031")

    assert set(client.payload) == {"first_name", "last_name", "email",
                                  "phone_number", "address"}
    assert set(client.payload["address"]) == {"address", "city", "state",
                                              "zipcode"}
    assert "address2" not in client.payload["address"]


def test_a_single_address_key_is_recorded_as_storing_nothing():
    """Measured twice, 2026-10-01: against a profile with NO address
    (sandbox-9) and against 21626 holding a full one (sandbox-13). Both times
    200, 0 of 81 fields changed, modified_ts unchanged, nothing blanked.

    Which is why build_address sends all four or none — not only because a
    partial object derives a malformed primary_address_string, but because a
    partial set is not acted on at all.
    """
    from clients.csuite import INCONCLUSIVE_PROBES, build_address

    evidence = INCONCLUSIVE_PROBES["address.city"]
    assert "nothing blanked" in evidence
    assert "smallest working set is untested" in evidence

    for parts in ({"city": "Vienna"},
                  {"address_line": "81 Test Way", "city": "Vienna"},
                  {"address_line": "81 Test Way", "city": "Vienna",
                   "state": "VA"}):
        address, warning = build_address(parts.get("address_line"),
                                        parts.get("city"), parts.get("state"),
                                        parts.get("zipcode"))
        assert address is None, "a partial set is never sent"
        assert warning and "Address incomplete" in warning
