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
        "env", "profile_id",
        # 2026-10-01, profile/edit on 21626, all four together
        "address.address", "address.city", "address.state", "address.zipcode"}


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


def test_task_create_is_gated_from_2026_10_01():
    """Gated against CSuite's own task field names, not against the confirmed
    allowlist — no task input name has ever been read back. A gate on
    unmeasured names still stops a caller inventing one and having it silently
    discarded, which is what this method allowed before."""
    client = Client()
    with pytest.raises(UnconfirmedField):
        client.create_task("Do the thing", 1007, anything_at_all="x")
    assert client.sent == [], "nothing may leave the process"

    client.create_task("Do the thing", 1007, task_type_id=3)
    assert client.endpoints == ["task/create"]
    assert client.sent[0][1]["task_type_id"] == 3


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


# ---------------------------------------------------------------------------
# The client's before-snapshot
# ---------------------------------------------------------------------------

def test_an_edit_that_touches_nothing_is_reported_as_nothing_stored():
    stamp = "2026-09-30 16:13:49.24411"
    client = Client({"profile/display": {
        "success": True, "data": display(modified_ts=stamp)}})
    result = client._verify_write(
        "profile/edit", {"profile_id": 21626, "primary_city": "Fairfax"},
        {"success": True, "data": None}, modified_before=stamp)

    assert result["nothing_stored"] is True
    assert result["verified"] is False
    assert "primary_city" in result["fields_dropped"]
    assert "profile_id" not in result["fields_dropped"], "not a sent field"


def test_only_edits_are_snapshotted_before_the_write():
    """A create has no before-state, so reading one would buy nothing."""
    assert "profile/edit" in Client.SNAPSHOT_BEFORE
    assert "profile/create/individual" not in Client.SNAPSHOT_BEFORE


def test_the_snapshot_reads_the_record_by_its_id():
    stamp = "2026-09-30 16:13:49.24411"
    client = Client({"profile/display": {
        "success": True, "data": display(modified_ts=stamp)}})
    assert client._modified_before("profile/edit", {"profile_id": 21626}) == stamp
    assert client.sent == [("profile/display", {"profile_id": 21626})]


def test_no_snapshot_without_an_id_and_none_for_a_create():
    client = Client()
    assert client._modified_before("profile/edit", {}) is None
    assert client._modified_before("profile/create/individual",
                                   {"profile_id": 1}) is None
    assert client.sent == []


# ---------------------------------------------------------------------------
# Allowed-but-unverified is its own category
# ---------------------------------------------------------------------------

def test_unverified_names_are_never_mistaken_for_confirmed_ones():
    """They are CSuite's own task field names, read off task/display — and
    `primary_email` was a valid display name and an invalid input, so being in
    the output vocabulary proves nothing about the input."""
    from clients.csuite import (ENDPOINT_ALLOWED_UNVERIFIED,
                                KNOWN_INVALID_INPUT_FIELDS)

    assert not (set(ENDPOINT_ALLOWED_UNVERIFIED) & set(CONFIRMED_INPUT_FIELDS))
    assert not (set(ENDPOINT_ALLOWED_UNVERIFIED)
                & set(KNOWN_INVALID_INPUT_FIELDS))


@pytest.mark.parametrize("field", ["task_description", "due_ts",
                                   "employee_id", "task_type_id"])
def test_a_task_field_passes_on_task_create_only(field):
    check_input_fields([field], "task/create")
    with pytest.raises(UnconfirmedField):
        check_input_fields([field], "profile/edit")


@pytest.mark.parametrize("field", [
    "primary_email",        # the proven-wrong typo shape
    "task_name",            # plausible and never seen
    "subject",
    "due_date",             # the OUTPUT name; CSuite requires due_ts
])
def test_an_unknown_or_proven_wrong_task_key_raises(field):
    with pytest.raises(UnconfirmedField):
        check_input_fields([field], "task/create")


def test_name_is_deliberately_absent_from_the_task_allowlist():
    """create_task sends it as required; no task read endpoint returns it.
    That is the primary_email shape, and it is unresolved."""
    from clients.csuite import ENDPOINT_ALLOWED_UNVERIFIED, INCONCLUSIVE_PROBES

    assert "name" not in ENDPOINT_ALLOWED_UNVERIFIED
    assert "name (task/create)" in INCONCLUSIVE_PROBES
    assert "UNTESTED" in INCONCLUSIVE_PROBES["name (task/create)"]


def test_profile_id_is_blocked_on_task_create():
    """The gap from 2026-10-01, closed. CONFIRMED_INPUT_FIELDS is
    endpoint-agnostic, so `profile_id` — confirmed for profile/edit — used to be
    accepted here and would have been silently discarded. A task's link is
    `o` + `id`, VERIFIED on task 1034.

    A block beats the global allowlist: a name confirmed elsewhere can still be
    wrong here.
    """
    from clients.csuite import ENDPOINT_BLOCKED_FIELDS

    assert "profile_id" in CONFIRMED_INPUT_FIELDS
    assert ENDPOINT_BLOCKED_FIELDS["profile_id"] == ("task/create",)
    with pytest.raises(UnconfirmedField):
        check_input_fields(["profile_id"], "task/create")
    check_input_fields(["profile_id"], "profile/edit")   # still fine there


# ---------------------------------------------------------------------------
# A read name is not a write name
# ---------------------------------------------------------------------------

def test_the_task_profile_link_is_recorded_as_output_only():
    """Observed on a UI-made task, 2026-10-01: o="profile", id=21661.

    Seven earlier sandbox tasks had all three link fields null, which I read
    as "tasks cannot be linked to a profile". They were unlinked, not
    unlinkable. The names are kept out of both allowlists because a read name
    is not a write name — primary_email was a valid display field and an
    invalid input.
    """
    from clients.csuite import (ENDPOINT_ALLOWED_UNVERIFIED,
                                INCONCLUSIVE_PROBES, OBSERVED_OUTPUT)

    observed = OBSERVED_OUTPUT["task/display"]
    for field in ("o", "id", "task_object"):
        assert field in observed
        assert field not in CONFIRMED_INPUT_FIELDS
        assert field not in ENDPOINT_ALLOWED_UNVERIFIED

    assert "the task -> profile link (task/create)" in INCONCLUSIVE_PROBES


def test_observed_output_never_leaks_into_an_allowlist():
    from clients.csuite import ENDPOINT_ALLOWED_UNVERIFIED, OBSERVED_OUTPUT

    for endpoint, fields in OBSERVED_OUTPUT.items():
        for field in fields:
            assert field not in CONFIRMED_INPUT_FIELDS or \
                field in ("profile_id",), \
                f"{field} is an observed OUTPUT name on {endpoint}"


@pytest.mark.parametrize("field", ["task_object", "object_id", "object_type"])
def test_an_unproven_link_name_is_still_refused_on_task_create(field):
    """`o` and `id` were proven on 2026-10-01. These were not: object_type and
    object_id were never sent, and task_object is a DERIVED output."""
    with pytest.raises(UnconfirmedField):
        check_input_fields([field], "task/create")


@pytest.mark.parametrize("field", ["o", "id"])
def test_the_proven_link_names_pass_on_task_create_and_nowhere_else(field):
    """VERIFIED 2026-10-01 on task 1034: o="profile", id=21661 both stored.

    Scoped hard. `id` must never be a global input name — a task already has a
    `task_id`, and `id` here is the LINKED object's id.
    """
    check_input_fields([field], "task/create")
    assert field not in CONFIRMED_INPUT_FIELDS
    for elsewhere in ("profile/edit", "profile/create/individual",
                      "funit/create"):
        with pytest.raises(UnconfirmedField):
            check_input_fields([field], elsewhere)
