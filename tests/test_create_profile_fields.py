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

def test_an_address_raises_rather_than_being_quietly_discarded():
    """Accepting a value and dropping it is the bug being fixed here.

    Doing it ourselves would be worse than CSuite doing it, because at
    least CSuite does not claim to support the field.
    """
    client = Client()
    with pytest.raises(ValueError) as caught:
        client.create_individual_profile("A", "B", address="1 Test Way")

    assert "Nothing was sent" in str(caught.value)
    assert client.sent == [], "the request must not leave the process"


def test_no_address_is_not_an_address():
    client = Client()
    client.create_individual_profile("A", "B", address=None)
    client.create_individual_profile("A", "B", address="")
    assert len(client.sent) == 2


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


def test_the_single_dotted_key_is_recorded_as_dropped():
    """2026-10-01, profile/edit on 21626, `address.city` ALONE: HTTP 200,
    success: true, 0 of 81 fields changed, modified_ts unchanged.

    Which also settles the 500 of 2026-09-30: one dotted key does not fault,
    so the fault came from nine conflicting keys, not from the dot.
    """
    assert KNOWN_INVALID_INPUT_FIELDS["address.city"].startswith("dropped")
    with pytest.raises(UnconfirmedField) as caught:
        Client().edit_profile(21626, **{"address.city": "Fairfax"})
    assert "Proven not to work" in str(caught.value)


@pytest.mark.parametrize("shape", ["41 Test Way, Fairfax, VA 22031",
                                   {"city": "Vienna"}])
def test_the_address_key_is_wrong_whatever_shape_it_takes(shape):
    """2026-10-01, two isolated single-key edits of 21626: `address` as a
    plain string and `address` as a nested object. Both returned 200,
    success: true, 0 of 81 fields changed, modified_ts unchanged."""
    assert KNOWN_INVALID_INPUT_FIELDS["address"].startswith("dropped")
    with pytest.raises(UnconfirmedField):
        Client().edit_profile(21626, address=shape)


def test_the_address_refusal_names_what_has_been_eliminated():
    """So the next person does not re-spend a capped write on a dead name."""
    with pytest.raises(ValueError) as caught:
        Client().create_individual_profile("A", "B", address="1 Test Way")
    message = str(caught.value)
    for spent in ("primary_address_string", "primary_city", "address.city",
                  "nested"):
        assert spent in message
