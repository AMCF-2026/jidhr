"""Unit tests for the probe's masking function.

These cover masking only — nothing here touches the network, and importing
scripts.probe_apis must not either. Masking is the one thing standing between
live donor data and a file committed to the repo, so it gets tested directly.
"""

import pytest

from scripts.probe_apis import (
    REDACTED,
    mask_amount,
    mask_email,
    mask_value,
)


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field", ["email", "primary_email", "Email", "work_email"])
def test_email_fields_keep_only_first_char_and_domain(field):
    assert mask_value(field, "jasmine@amuslimcf.org") == "j*@amuslimcf.org"


def test_email_masking_survives_uppercase_and_plus_addressing():
    assert mask_email("Kods.Ali+daf@example.co.uk") == "K*@example.co.uk"


def test_email_like_value_masked_even_under_an_unrelated_field_name():
    # The field name gives no hint, but the value clearly does.
    assert mask_value("contact_point", "muhi@example.org") == "m*@example.org"


def test_email_field_holding_a_non_email_is_redacted_not_leaked():
    assert mask_value("email", "no address on file") == REDACTED


def test_empty_local_part_still_hides_nothing_it_should_not():
    assert mask_email("@example.org") == "*@example.org"


# ---------------------------------------------------------------------------
# Names, addresses, phones
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field", [
    "name", "first_name", "last_name", "org_name", "household",
    "address1", "street_address", "city", "postal_code",
    "phone", "primary_phone", "mobile", "fax",
])
def test_pii_fields_are_fully_redacted(field):
    assert mask_value(field, "something identifying") == REDACTED


def test_redaction_does_not_echo_any_of_the_original_value():
    original = "Bilqis Abdul-Qaadir"
    masked = mask_value("name", original)
    assert masked == REDACTED
    assert original not in masked


def test_nested_dict_values_are_masked_by_their_own_leaf_key():
    record = {"name": "Someone", "primary_email": "s@x.org", "profile_id": 19879}
    assert mask_value("profile", record) == {
        "name": REDACTED,
        "primary_email": "s*@x.org",
        "profile_id": 19879,
    }


def test_list_values_are_masked_elementwise():
    assert mask_value("phone_numbers", ["555-0100", "555-0101"]) == [REDACTED, REDACTED]


# ---------------------------------------------------------------------------
# Amounts
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    (0, "$0"),
    (7.5, "$0-10"),
    (42, "$10-100"),
    (750.25, "$100-1k"),
    (5000, "$1k-10k"),
    (25_000, "$10k-100k"),
    (250_000, "$100k-1M"),
    (2_500_000, "$1M-10M"),
    (25_000_000, "$10M+"),
])
def test_amount_reduced_to_order_of_magnitude(value, expected):
    assert mask_amount(value) == expected


@pytest.mark.parametrize("field", [
    "donation_amount", "amount", "fund_balance", "total_giving",
    "fee_amount", "admin_fee", "lifetime_value", "market_value",
])
def test_money_field_names_trigger_amount_masking(field):
    assert mask_value(field, 5000) == "$1k-10k"


def test_exact_figure_never_appears_in_the_masked_amount():
    masked = mask_value("donation_amount", 4321.99)
    assert "4321" not in masked
    assert masked == "$1k-10k"


def test_amount_masking_accepts_csuite_string_amounts():
    # CSuite returns donation_amount as a string like "1,500.00".
    assert mask_value("donation_amount", "1,500.00") == "$1k-10k"


def test_negative_amount_keeps_its_sign_but_not_its_size():
    assert mask_value("balance", -5000) == "-$1k-10k"


def test_non_numeric_money_field_is_flagged_not_passed_through():
    assert mask_value("amount", "see attached memo") == "<non-numeric>"


# ---------------------------------------------------------------------------
# IDs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field,value", [
    ("id", 12345),
    ("profile_id", 19879),
    ("funit_id", 1042),
    ("event_date_id", 771),
    ("hs_object_id", "123456789"),
    ("channelGuid", "a1b2c3d4-0000-4444-8888-abcdefabcdef"),
    ("check_num", "10432"),
])
def test_ids_are_kept_verbatim(field, value):
    assert mask_value(field, value) == value


def test_id_rule_beats_the_money_rule_for_fee_type_id():
    # 'fee_type_id' contains 'fee' but is an identifier, not a figure.
    assert mask_value("fee_type_id", 1007) == 1007


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field,value", [
    ("donation_date", "2026-03-14"),
    ("event_date", "2026-09-08"),
    ("createdate", "2026-01-02T00:00:00.000Z"),
    ("lastmodifieddate", "2026-01-02T13:45:01.123Z"),
    ("start_time", "7:30 pm PST"),
    ("updated_at", "2026-05-01 09:00:00"),
])
def test_dates_are_kept_verbatim_because_we_need_the_formats(field, value):
    assert mask_value(field, value) == value


def test_date_rule_beats_the_money_rule_for_total_date_fields():
    # A field named e.g. 'total_date' is a date, not a figure.
    assert mask_value("total_date", "2026-03-14") == "2026-03-14"


# ---------------------------------------------------------------------------
# Everything else
# ---------------------------------------------------------------------------

def test_none_stays_none():
    assert mask_value("name", None) is None
    assert mask_value("donation_amount", None) is None


def test_flags_are_preserved_so_opt_in_state_is_readable():
    assert mask_value("newsletter", 1) == 1
    assert mask_value("newsletter", 0) == 0
    assert mask_value("archived", True) is True


def test_enum_like_strings_pass_through():
    assert mask_value("status", "PUBLISHED") == "PUBLISHED"
    assert mask_value("event_type_code", "fundraiser") == "fundraiser"


def test_state_is_treated_as_part_of_an_address_not_as_an_enum():
    # CSuite profiles carry a mailing 'state'; erring toward redaction.
    assert mask_value("state", "MD") == REDACTED


def test_free_text_is_reduced_to_a_length_so_donor_names_cannot_ride_along():
    assert mask_value("memo", "Check from the Ali family") == "<text len=25>"


def test_long_unclassified_strings_are_reduced_to_a_length():
    long_value = "x" * 100
    assert mask_value("mystery_field", long_value) == "<text len=100>"


def test_dotted_field_paths_are_masked_by_their_leaf_segment():
    assert mask_value("properties.email", "z@x.org") == "z*@x.org"
    assert mask_value("properties.firstname", "Zaid") == REDACTED
    assert mask_value("properties.lifetime_giving", 5000) == "$1k-10k"


# ---------------------------------------------------------------------------
# Nested record shapes introduced by probe extension #2
#
# event/display/eventdate returns registrants as nested arrays (profiles[],
# guests[]). Those rows are flattened to dotted paths before masking, so the
# leaf-segment rules have to hold at depth.
# ---------------------------------------------------------------------------

def test_registrant_row_is_masked_leaf_by_leaf():
    registrant = {
        "profile_id": 19879,
        "name": "Someone Real",
        "email": "someone@example.org",
        "rsvp": 1,
        "attended": 0,
        "guest_count": 2,
        "amount_paid": "150.00",
    }
    assert mask_value("profiles", registrant) == {
        "profile_id": 19879,
        "name": REDACTED,
        "email": "s*@example.org",
        "rsvp": 1,
        "attended": 0,
        "guest_count": 2,
        "amount_paid": "$100-1k",
    }


def test_guest_rows_in_a_list_are_each_masked():
    guests = [
        {"guest_name": "A Person", "guest_email": "a@x.org", "attended": 1},
        {"guest_name": "B Person", "guest_email": None, "attended": 0},
    ]
    assert mask_value("guests", guests) == [
        {"guest_name": REDACTED, "guest_email": "a*@x.org", "attended": 1},
        {"guest_name": REDACTED, "guest_email": None, "attended": 0},
    ]


@pytest.mark.parametrize("path,value,expected", [
    ("guests.guest_name", "A Person", REDACTED),
    ("profiles.rsvp", 1, 1),
    ("profiles.attended", 0, 0),
    ("profiles.registration_date", "2026-03-20", "2026-03-20"),
    ("profiles.profile_id", 19879, 19879),
    ("guests.amount_paid", "150.00", "$100-1k"),
])
def test_nested_registrant_paths_mask_by_leaf(path, value, expected):
    assert mask_value(path, value) == expected


def test_rsvp_and_attended_flags_survive_masking_so_they_can_be_counted():
    # C3 reports the distinct values of these; masking must not flatten them.
    for flag in ("rsvp", "attended", "checked_in", "no_show"):
        for value in (0, 1):
            assert mask_value(flag, value) == value


def test_deeply_nested_dicts_still_mask_their_leaves():
    payload = {"event": {"registrants": {"name": "X", "email": "x@y.org"}}}
    assert mask_value("data", payload) == {
        "event": {"registrants": {"name": REDACTED, "email": "x*@y.org"}}
    }


# ---------------------------------------------------------------------------
# F3: Config must import when an env var is present but blank.
#
# These live here rather than in their own file because this change set is
# scoped to tests/test_probe_masking.py.
# ---------------------------------------------------------------------------

# Every env var config.py casts to int() or float(). Add to this list when a
# new numeric setting is introduced.
NUMERIC_CONFIG_ENV_VARS = ["PORT"]


@pytest.mark.parametrize("var", NUMERIC_CONFIG_ENV_VARS)
def test_config_imports_when_a_numeric_env_var_is_blank(monkeypatch, var):
    """A cleared-but-present env var returns '', and int('') raises."""
    import importlib

    monkeypatch.setenv(var, "")
    import config
    reloaded = importlib.reload(config)
    assert reloaded.Config is not None


@pytest.mark.parametrize("var", NUMERIC_CONFIG_ENV_VARS)
def test_blank_numeric_env_var_falls_back_to_the_documented_default(
        monkeypatch, var):
    import importlib

    monkeypatch.setenv(var, "")
    import config
    reloaded = importlib.reload(config)
    assert getattr(reloaded.Config, var) == 5000


def test_numeric_env_var_is_still_honoured_when_set(monkeypatch):
    import importlib

    monkeypatch.setenv("PORT", "8080")
    import config
    reloaded = importlib.reload(config)
    assert reloaded.Config.PORT == 8080


def test_config_source_has_no_bare_int_cast_of_an_env_var():
    """Guards the fix itself: int(os.environ.get('X', default)) is the bug."""
    import os as _os
    import re as _re

    repo_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
    with open(_os.path.join(repo_root, "config.py"), encoding="utf-8") as handle:
        source = handle.read()

    bad = _re.findall(
        r"(?:int|float)\(\s*os\.environ\.get\(\s*['\"][^'\"]+['\"]\s*,",
        source,
    )
    assert bad == [], (
        "config.py casts an env var with a default argument; a blank value "
        "still reaches int()/float(). Use os.environ.get('X') or 'default'."
    )


# ---------------------------------------------------------------------------
# Label suffixes: a money word in the name does not always mean an amount
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field", [
    "first_gift_fund", "latest_gift_fund", "payment_method_name",
    "donation_type", "grant_status",
])
def test_label_fields_are_not_reported_as_non_numeric_amounts(field):
    # These contain a money word but hold a label. Masking them through
    # mask_amount produced "<non-numeric>", which hid what the field was.
    assert mask_value(field, "AMCF General Fund") != "<non-numeric>"


def test_status_enums_survive_so_their_distinct_values_can_be_reported():
    # C2/C6 report the distinct values of these; they must not be mangled.
    assert mask_value("donation_status", "closed") == "closed"
    assert mask_value("grant_status", "new") == "new"
    assert mask_value("payment_method_id", 1003) == 1003


def test_fund_named_fields_are_redacted_because_fund_names_carry_surnames():
    # "Ali Family Fund" identifies a household as surely as a name field.
    assert mask_value("fund_name", "Ali Family Fund") == REDACTED
    assert mask_value("first_gift_fund", "Ali Family Fund") == REDACTED
    assert mask_value("fund.fund_name", "Ali Family Fund") == REDACTED


def test_real_amounts_are_still_reduced_despite_the_label_guard():
    assert mask_value("donation_amount", "2500.00") == "$1k-10k"
    assert mask_value("grant_amount", "25000.00") == "$10k-100k"
    assert mask_value("current_fundbalance", "312000.00") == "$100k-1M"
    assert mask_value("ticket_price", "0.00") == "$0"


# ---------------------------------------------------------------------------
# Record discovery (F2): the envelope is not a record
# ---------------------------------------------------------------------------

from scripts.probe_apis import find_records  # noqa: E402


def test_hubspot_lists_envelope_is_read_from_the_lists_key():
    payload = {"lists": [{"listId": "126", "name": "Giving Circle"}], "total": 1}
    records, container = find_records(payload)
    assert container == "lists"
    assert len(records) == 1


def test_subscription_definitions_envelope_is_recognised():
    payload = {"subscriptionDefinitions": [{"id": "1265988358"}]}
    records, container = find_records(payload)
    assert container == "subscriptionDefinitions"
    assert len(records) == 1


def test_unknown_envelope_key_falls_back_to_the_first_object_list():
    payload = {"someNewKey": [{"a": 1}, {"a": 2}], "total": 2}
    records, container = find_records(payload)
    assert len(records) == 2
    assert "someNewKey" in container


def test_empty_csuite_search_reports_zero_records_not_one_envelope():
    # A search with no matches used to come back as "1 record" whose only
    # field was `results`, which read as a hit.
    payload = {"success": 1, "data": {"results": [], "count": 0}}
    records, container = find_records(payload)
    assert records == []
    assert "empty" in container


def test_csuite_display_envelope_is_still_a_single_record():
    payload = {"success": 1, "data": {"funit_id": 1299, "fund_name": "X"}}
    records, _ = find_records(payload)
    assert len(records) == 1
    assert records[0]["funit_id"] == 1299


# ---------------------------------------------------------------------------
# A "number" is not an identifier
#
# Regression: CSuite returns `primary_phone_number`, which ends in _number.
# The id rule matched first and wrote a real phone number into the receipt.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field", [
    "primary_phone_number", "phone_number", "mobile_number", "fax_number",
    "work_phone_number",
])
def test_phone_fields_are_redacted_even_when_they_end_in_number(field):
    assert mask_value(field, "415-980-9091") == REDACTED


def test_check_number_is_still_treated_as_an_identifier():
    # The _number suffix still means "id" for the fields it was added for.
    assert mask_value("check_number", "10432") == "10432"
    assert mask_value("check_num", "10432") == "10432"


@pytest.mark.parametrize("field", ["fedid", "ssn", "tax_id", "account_number"])
def test_government_and_bank_identifiers_are_redacted(field):
    assert mask_value(field, "52-1234567") == REDACTED


def test_no_phone_shaped_value_survives_masking_under_any_phone_field():
    for field in ("primary_phone_number", "phone", "mobile", "fax"):
        masked = mask_value(field, "415-980-9091")
        assert "415" not in str(masked)
