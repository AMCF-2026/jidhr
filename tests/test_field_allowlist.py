"""Unconfirmed field names are refused, and every write is read back.

CSuite returns HTTP 200 and a profile_id whether it stored a field or
discarded it. Two consequences, both tested here:

  * a name that no read-back has confirmed must not be sent at all, since
    the response would not tell us it was ignored; and
  * a name that IS sent has to be checked against the stored record,
    because the method's own hard-coded fields are not gated and two of
    them are known-wrong.

No network.
"""

import pytest

from clients.csuite import (CONFIRMED_INPUT_FIELDS, CSuiteClient,
                            UnconfirmedField, check_input_fields)


# ---------------------------------------------------------------------------
# A client whose only I/O is a dict lookup
# ---------------------------------------------------------------------------

class Client(CSuiteClient):
    """Records every endpoint it was asked to call; answers from `replies`."""

    def __init__(self, replies=None):
        self.sent = []
        self.replies = replies or {}

    def _request(self, endpoint, data=None):
        self.sent.append((endpoint, dict(data or {})))
        return self.replies.get(
            endpoint,
            {"success": True, "data": {"profile_id": 21626},
             "http_status": 200, "outcome": "ok"})

    @property
    def endpoints(self):
        return [endpoint for endpoint, _ in self.sent]


def display(**fields):
    base = {"profile_id": 21626, "first_name": None, "last_name": None,
            "primary_email": None, "website": None, "name": "derived",
            "ptype": "indiv"}
    base.update(fields)
    return base


# ---------------------------------------------------------------------------
# The allowlist itself
# ---------------------------------------------------------------------------

def test_the_allowlist_holds_only_read_back_confirmed_names():
    """Every entry was observed on a record, not read off a doc.

    This is the list that decides what may be sent, so it is pinned:
    a name is added by a sandbox write and a read-back, never by
    plausibility.
    """
    assert set(CONFIRMED_INPUT_FIELDS) == {
        "first_name", "last_name", "email", "website", "phone_number",
        "env", "profile_id"}


@pytest.mark.parametrize("field", sorted(CONFIRMED_INPUT_FIELDS))
def test_a_confirmed_name_passes(field):
    check_input_fields([field], "profile/edit")


def test_nothing_to_check_is_not_an_error():
    check_input_fields([], "profile/edit")
    check_input_fields(None, "profile/edit")


@pytest.mark.parametrize("field", [
    "primary_email",          # a valid DISPLAY name, proven invalid as input
    "primary_phone_number",
    "primary_address_string",
    "phone",
    "address",
    "primary_city",
    "notes",
    "",
])
def test_an_unconfirmed_name_raises(field):
    with pytest.raises(UnconfirmedField):
        check_input_fields([field], "profile/edit")


def test_the_refusal_names_the_field_and_says_nothing_was_sent():
    with pytest.raises(UnconfirmedField) as caught:
        check_input_fields(["notes", "primary_email"], "profile/edit")
    message = str(caught.value)
    assert "notes" in message and "primary_email" in message
    assert "Nothing was sent" in message
    assert caught.value.unknown == ["notes", "primary_email"]
    assert caught.value.endpoint == "profile/edit"


def test_a_confirmed_name_alongside_an_unconfirmed_one_still_raises():
    """All or nothing. A partial send is the silent drop by another route."""
    with pytest.raises(UnconfirmedField):
        check_input_fields(["email", "notes"], "profile/edit")


# ---------------------------------------------------------------------------
# The gate on every profile / fund / event write method
# ---------------------------------------------------------------------------

GATED = [
    ("create_individual_profile", ("A", "B"), "profile/create/individual"),
    ("create_org_profile", ("Org",), "profile/create/org"),
    ("create_household_profile", ("House",), "profile/create/household"),
    ("edit_profile", (21626,), "profile/edit"),
    ("create_fund", ("Fund", 1002), "funit/create"),
    ("create_event_date", (77,), "event/create/eventdate"),
    ("edit_event_date", (88,), "event/edit/eventdate"),
]


@pytest.mark.parametrize("method,args,endpoint", GATED)
def test_an_unconfirmed_kwarg_is_refused_before_anything_is_sent(
        method, args, endpoint):
    client = Client()
    with pytest.raises(UnconfirmedField) as caught:
        getattr(client, method)(*args, notes="please store this")
    assert caught.value.endpoint == endpoint
    assert client.sent == [], "the request must not leave the process"


@pytest.mark.parametrize("method,args,endpoint", GATED)
def test_a_confirmed_kwarg_reaches_the_endpoint(method, args, endpoint):
    """`website` rather than `email`: two of these methods take email as a
    named parameter and rewrite it to primary_email, which is the bug the
    gate does not cover and the read-back does."""
    client = Client()
    getattr(client, method)(*args, website="https://example.invalid")
    assert client.endpoints[0] == endpoint
    assert client.sent[0][1]["website"] == "https://example.invalid"


def test_the_gate_does_not_block_a_methods_own_fields():
    """create_org_profile sends `organization`, which is not on the list.

    The gate covers **kwargs, which is where a caller can introduce a name
    nobody checked. A method's own declared fields are its contract; two of
    them are known-wrong and fixing them is a separate change, so
    verify_writes is what catches those.
    """
    client = Client()
    client.create_org_profile("Some Org", email="a@b.invalid")
    assert client.sent[0][1]["organization"] == "Some Org"


def test_task_create_is_documented_as_ungated():
    """No task input name is confirmed, so gating it would refuse `name`."""
    client = Client()
    client.create_task("Do the thing", 1007, anything_at_all="x")
    assert client.endpoints == ["task/create"]


# ---------------------------------------------------------------------------
# verify_writes
# ---------------------------------------------------------------------------

def test_read_back_is_on_by_default():
    assert CSuiteClient.verify_writes is True


def test_a_stored_field_is_marked_verified():
    client = Client({"profile/display": {
        "success": True, "data": display(primary_email="a@b.invalid")}})
    result = client._verify_write(
        "profile/create/individual", {"email": "a@b.invalid"},
        {"success": True, "data": {"profile_id": 21626}})
    assert result["verified"] is True
    assert "fields_dropped" not in result
    assert client.endpoints == ["profile/display"]


def test_a_dropped_field_is_reported_without_raising():
    """Annotated, not raised.

    Raising after a create that succeeded would tell the caller the write
    failed while a record exists, and CSuite has no idempotency key — so
    the obvious response, try again, would make a second profile.
    """
    client = Client({"profile/display": {
        "success": True, "data": display(primary_email=None)}})
    result = client._verify_write(
        "profile/create/individual", {"email": "a@b.invalid"},
        {"success": True, "data": {"profile_id": 21626}})
    assert result["success"] is True, "the record exists; do not retry"
    assert result["verified"] is False
    assert "email" in result["fields_dropped"]


def test_an_edit_is_read_back_by_the_id_it_was_sent():
    client = Client({"profile/display": {
        "success": True, "data": display(website="https://example.invalid")}})
    result = client._verify_write(
        "profile/edit", {"profile_id": 21626,
                         "website": "https://example.invalid"},
        {"success": True, "data": None})
    assert result["verified"] is True
    assert client.sent[0][1] == {"profile_id": 21626}


def test_an_endpoint_with_no_known_read_back_is_left_alone():
    client = Client()
    result = client._verify_write("funit/create", {"name": "F"},
                                  {"success": True, "data": {"funit_id": 9}})
    assert "verified" not in result
    assert client.sent == []


def test_no_id_means_unverified_not_verified():
    client = Client()
    result = client._verify_write("profile/create/individual",
                                  {"email": "a@b.invalid"},
                                  {"success": True, "data": None})
    assert result["verified"] is None
    assert client.sent == []


def test_a_failed_read_back_is_unverified_not_a_pass():
    client = Client({"profile/display": {"success": False,
                                         "error": "boom", "data": None}})
    result = client._verify_write(
        "profile/create/individual", {"email": "a@b.invalid"},
        {"success": True, "data": {"profile_id": 21626}})
    assert result["verified"] is None
    assert result.get("fields_dropped") is None


def test_the_read_back_never_repeats_the_write():
    client = Client({"profile/display": {
        "success": True, "data": display(primary_email="a@b.invalid")}})
    client._verify_write("profile/create/individual",
                         {"email": "a@b.invalid"},
                         {"success": True, "data": {"profile_id": 21626}})
    assert "profile/create/individual" not in client.endpoints


def test_recognised_and_confirmed_never_overlap():
    """The holding pen is for names CSuite parses but has not yet stored.

    phone_number spent a task in it — validated by a 400 on create, with no
    value ever read back — and left it on 2026-09-30 when profile/edit
    stored one. Empty now, and it must never share a name with the
    allowlist, or a field would be both sendable and unconfirmed.
    """
    from clients.csuite import RECOGNISED_UNCONFIRMED_FIELDS

    assert not (RECOGNISED_UNCONFIRMED_FIELDS & CONFIRMED_INPUT_FIELDS)


def test_phone_number_may_now_be_sent():
    """Confirmed by read-back: sent 7035550100, stored 703-555-0100."""
    from sync.readback import SENT_TO_STORED

    check_input_fields(["phone_number"], "profile/edit")
    assert SENT_TO_STORED["phone_number"] == "primary_phone_number"


# ---------------------------------------------------------------------------
# Names proven wrong
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field", [
    "primary_email", "primary_phone_number", "primary_address",
    "primary_city", "primary_state", "primary_zipcode",
    "primary_address_string",
])
def test_a_proven_wrong_name_is_refused_with_its_evidence(field):
    """Every one is a valid profile/display name and an invalid input.

    profile/edit on 21626 was sent the four address names on 2026-09-30 and
    answered 200, success: true, with 0 of 81 fields changed — modified_ts
    included. It did not touch the record and it did not say so.
    """
    from clients.csuite import KNOWN_INVALID_INPUT_FIELDS

    assert field in KNOWN_INVALID_INPUT_FIELDS
    with pytest.raises(UnconfirmedField) as caught:
        check_input_fields([field], "profile/edit")
    assert "Proven not to work" in str(caught.value)


def test_a_merely_unconfirmed_name_gets_no_false_evidence():
    with pytest.raises(UnconfirmedField) as caught:
        check_input_fields(["some_new_idea"], "profile/edit")
    assert "Proven not to work" not in str(caught.value)


def test_nothing_is_both_proven_wrong_and_allowed():
    from clients.csuite import KNOWN_INVALID_INPUT_FIELDS

    assert not (set(KNOWN_INVALID_INPUT_FIELDS) & CONFIRMED_INPUT_FIELDS)
