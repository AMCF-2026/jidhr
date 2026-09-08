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
