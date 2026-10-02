"""A wrong target_id is worse than a missing one.

2026-10-01: a funit/create was audited with target_id 1069. The fund was
1564. 1069 is the CASH ACCOUNT that was sent in the request —
target_id_from_payload took the alphabetically first *_id and
`cash_account_id` sorts before `fgroup_id`.

The 2026-09-25 decision ("a delete needs an ID a human supplied") exists
because a NULL target_id forced timestamp matching. A wrong id is worse: it
invites acting on the wrong record while looking authoritative.

So the payload may only yield the id of the record its endpoint ACTS ON,
never on a create, and the response wins whenever it carries a created id.

No network, no database.
"""

import pytest

from clients.audit import (resolve_target_id, target_id_from_endpoint,
                           target_id_from_payload, target_id_from_response)

# The sandbox-11 fund create, exactly as it was sent.
FUND_CREATE = {"name": "SENTINEL 4 - SANDBOX ONLY Family Fund",
               "fgroup_id": 1002, "cash_account_id": 1069}


# ---------------------------------------------------------------------------
# The regression
# ---------------------------------------------------------------------------

def test_the_fund_create_records_the_fund_not_the_cash_account():
    assert resolve_target_id("funit/create", FUND_CREATE) is None, \
        "a create's payload holds other records' ids, not its own"
    assert target_id_from_response({"success": 1, "data": {"funit_id": 1564}}) \
        == "1564"


def test_the_cash_account_id_is_never_returned():
    for endpoint in ("funit/create", "funit/edit", "profile/edit"):
        assert target_id_from_payload(FUND_CREATE, endpoint) != "1069"


# ---------------------------------------------------------------------------
# A create's target lives in the response
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("key,value", [
    ("profile_id", 21660), ("funit_id", 1564), ("donation_id", 90),
    ("grant_id", 7), ("task_id", 1002), ("event_date_id", 55),
])
def test_a_csuite_created_id_is_read_from_data(key, value):
    assert target_id_from_response({"success": 1, "data": {key: value}}) \
        == str(value)


def test_a_hubspot_created_id_is_read_from_the_top_level():
    assert target_id_from_response({"id": "701"}) == "701"
    assert target_id_from_response({"objectId": 702}) == "702"


def test_no_id_anywhere_records_null():
    for response in ({"success": 1, "data": None},
                     {"success": 1, "data": {}},
                     {"success": 0, "errors": ["nope"]},
                     None, "not a dict", []):
        assert target_id_from_response(response) is None


# ---------------------------------------------------------------------------
# An edit's target does live in the payload
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("endpoint,payload,expected", [
    ("profile/edit", {"profile_id": 21626, "address.city": "Vienna"}, "21626"),
    ("funit/edit", {"funit_id": 1564, "name": "x"}, "1564"),
    ("task/edit/complete", {"task_id": 1002, "task_guid": "g"}, "1002"),
    ("event/edit/eventdate", {"event_date_id": 55, "event_id": 7}, "55"),
])
def test_an_edit_takes_the_id_its_object_names(endpoint, payload, expected):
    assert resolve_target_id(endpoint, payload) == expected


def test_the_event_date_id_wins_over_the_parent_event_id():
    """event/edit/eventdate acts on the date, not on the event."""
    assert target_id_from_payload({"event_id": 7, "event_date_id": 55},
                                  "event/edit/eventdate") == "55"


# ---------------------------------------------------------------------------
# No guessing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("endpoint,payload", [
    # A create carries ids of OTHER records. None of them is the target.
    ("profile/create/individual", {"household_profile_id": 9}),
    ("funit/create", {"fgroup_id": 1002, "cash_account_id": 1069}),
    ("vendor/create", {"profile_id": 21626}),
    # An endpoint whose object names no key in the payload.
    ("profile/edit", {"cash_account_id": 1069}),
])
def test_an_id_that_is_not_the_target_is_not_recorded(endpoint, payload):
    assert target_id_from_payload(payload, endpoint) is None


def test_a_path_id_still_wins_for_hubspot_style_urls():
    assert target_id_from_endpoint("crm/v3/objects/contacts/701") == "701"
    assert resolve_target_id("crm/v3/objects/contacts/701",
                             {"properties": {}}) == "701"


def test_the_create_path_leaves_null_at_reserve_time_on_purpose():
    """So COALESCE(target_id, response_id) in complete_write can fill it.

    A payload guess at reserve time would win the COALESCE and lock the wrong
    id in, which is exactly what happened to fund 1564.
    """
    assert resolve_target_id("profile/create/individual",
                             {"first_name": "A", "last_name": "B"}) is None
