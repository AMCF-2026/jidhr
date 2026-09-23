"""scripts/hubspot_event_enrichment.py — the dry-run plan.

Fixtures only. No network and no database: MirrorDB from
test_mirror_reports answers the mirror reads, and SpyHubSpot answers
HubSpot and records every call it is asked to make — which is what lets
one test assert the thing that matters most here, that a run without
--apply never attempts a write.

Every person, address and event in this file is invented.
"""

import io

import pytest

from clients.hubspot import is_hubspot_write
from scripts import hubspot_event_enrichment as enrich
from tests.test_mirror_reports import MirrorDB, mirror  # noqa: F401


# ---------------------------------------------------------------------------
# A HubSpot that records instead of calling
# ---------------------------------------------------------------------------

PORTAL_PROPERTIES = [
    {"name": "email", "label": "Email", "type": "string",
     "fieldType": "text", "groupName": "contactinformation"},
    {"name": "firstname", "label": "First Name", "type": "string",
     "fieldType": "text", "groupName": "contactinformation"},
    {"name": "hs_eventbrite_lastregisteredevent", "label":
     "Last Registered Event", "type": "string", "fieldType": "text",
     "groupName": "eventbrite", "hubspotDefined": True},
]


class SpyHubSpot:
    """Answers from fixtures; records every call; never touches a socket.

    Deliberately not a HubSpotClient subclass. The point of the spy is to
    prove the script cannot reach a write, so it must not inherit one.
    """

    CONTACT_BATCH_SIZE = 100

    def __init__(self, contacts=None, properties=None):
        self.calls = []
        self.contacts = dict(contacts or {})
        self.properties = (PORTAL_PROPERTIES if properties is None
                           else properties)

    # -- reads --------------------------------------------------------
    def get_contact_properties(self, archived: bool = False) -> dict:
        self.calls.append(("GET", "crm/v3/properties/contacts", None))
        return {"results": self.properties}

    def batch_read_contacts_by_email(self, emails, properties=None) -> dict:
        self.calls.append(("POST", "crm/v3/objects/contacts/batch/read",
                           list(emails)))
        assert len(emails) <= self.CONTACT_BATCH_SIZE, \
            "batch read exceeded HubSpot's cap of 100"
        return {"results": [self.contacts[e] for e in emails
                            if e in self.contacts]}

    def _post(self, endpoint, data=None):
        self.calls.append(("POST", endpoint, data))
        if endpoint.endswith("/search"):
            return {"total": 0, "results": []}
        return {}

    # -- writes, which nothing in a dry run may reach -----------------
    def create_contact_property_group(self, name, label, display_order=-1):
        self.calls.append(("POST", "crm/v3/properties/contacts/groups",
                           {"name": name}))
        return {"name": name}

    def create_contact_property(self, definition):
        self.calls.append(("POST", "crm/v3/properties/contacts", definition))
        return {"name": definition.get("name")}

    def batch_upsert_contacts(self, inputs):
        inputs = list(inputs)
        self.calls.append(("POST", "crm/v3/objects/contacts/batch/upsert",
                           inputs))
        assert len(inputs) <= self.CONTACT_BATCH_SIZE, \
            "batch upsert exceeded HubSpot's cap of 100"
        return {"status": "COMPLETE"}

    @property
    def writes(self):
        return [(method, endpoint) for method, endpoint, _ in self.calls
                if is_hubspot_write(method, endpoint)]


def contact(contact_id, email, **properties):
    props = {"email": email}
    props.update(properties)
    return {"id": str(contact_id), "properties": props}


# ---------------------------------------------------------------------------
# Mirror fixtures — invented
# ---------------------------------------------------------------------------

# Shaped like the live catalogue: event_name is the SERIES name and is
# the same generic string on nearly every date, while event_description
# carries the title a person would recognise.
EVENTS = [
    {"_id": 100, "event_date_id": 100, "event_id": 5,
     "event_name": "Event - Other", "event_description": "Spring Iftar",
     "event_date": "2026-03-14", "archived": 1},
    {"_id": 101, "event_date_id": 101, "event_id": 5,
     "event_name": "Event - Other",
     "event_description": "Summer Picnic\r\nat the lake",
     "event_date": "2026-06-02", "archived": 1},
    {"_id": 102, "event_date_id": 102, "event_id": 6,
     "event_name": "Event - Other", "event_description": "Donor Briefing",
     "event_date": None, "archived": 0},
    {"_id": 103, "event_date_id": 103, "event_id": 6,
     "event_name": "Event - Other",
     "event_description": "Nobody Reachable",
     "event_date": "2026-08-01", "archived": 0},
]


def reg(event_date_id, profile_id, email, name, attended=None, rsvp=1):
    return {"_id": f"{event_date_id}:{profile_id}",
            "event_date_id": str(event_date_id), "event_id": 5,
            "profile_id": profile_id, "event_profile_email": email,
            "event_profile_name": name, "rsvp": rsvp, "attended": attended,
            "guest_count": 0, "pulled_at": "2026-09-23T10:00:00Z"}


REGISTRATIONS = [
    # Amina registered twice and was checked in once.
    reg(100, 7001, "Amina.Yusuf@example.org", "Yusuf, Amina", attended=1),
    reg(101, 7001, "amina.yusuf@example.org ", "Yusuf, Amina"),
    # Bilal is new to HubSpot; three-word name.
    reg(101, 7002, "bilal@example.net", "Fitzgerald, Mary Anne"),
    # Single-token name, undated event.
    reg(102, 7003, "cher@example.net", "Cher"),
    # No email at all — counted, never guessed from the name.
    reg(102, 7004, None, "Someone, Nameless"),
    reg(103, 7005, "", "Address, Also No"),
    # One CSuite profile under two addresses — not merged.
    reg(100, 7006, "dual.one@example.org", "Person, Dual"),
    reg(101, 7006, "dual.two@example.org", "Person, Dual"),
]


@pytest.fixture
def loaded(mirror):
    mirror.load({"event": EVENTS, "event_registration": REGISTRATIONS})
    return mirror


def run(argv, client, tmp_path):
    """main() with HubSpot and the streams swapped out."""
    out, err = io.StringIO(), io.StringIO()
    report = tmp_path / "report.md"
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(enrich, "HubSpotClient", lambda: client)
        patch.setattr("sys.stdout", out)
        patch.setattr("sys.stderr", err)
        code = enrich.main(list(argv) + ["--out", str(report)])
    text = report.read_text(encoding="utf-8") if report.exists() else ""
    return code, out.getvalue(), err.getvalue(), text


# ---------------------------------------------------------------------------
# Email normalisation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    ("Amina.Yusuf@Example.ORG", "amina.yusuf@example.org"),
    ("  spaced@example.org  ", "spaced@example.org"),
    ("\tTabbed@Example.org\n", "tabbed@example.org"),
    ("already@example.org", "already@example.org"),
    (None, None),
    ("", None),
    ("   ", None),
    ("not-an-address", None),
    (12345, None),
])
def test_email_normalisation(raw, expected):
    assert enrich.normalize_email(raw) == expected


def test_normalisation_does_not_fold_dots_or_strip_aliases():
    """Two addresses Google treats as one inbox stay two addresses.

    Folding them would merge two HubSpot contacts on a guess about one
    mail provider's local rules.
    """
    assert enrich.normalize_email("j.smith@gmail.com") != \
        enrich.normalize_email("jsmith@gmail.com")
    assert enrich.normalize_email("a+events@example.org") == \
        "a+events@example.org"


def test_the_same_address_in_two_cases_is_one_contact(loaded):
    groups, _ = enrich.group_by_email(REGISTRATIONS)
    assert len(groups["amina.yusuf@example.org"]) == 2


# ---------------------------------------------------------------------------
# Name splitting
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("full, first, last", [
    # The shape 87% of live registrant names come in: "Last, First".
    ("Yusuf, Amina", "Amina", "Yusuf"),
    ("Aucoin, Alix", "Alix", "Aucoin"),
    ("Fitzgerald, Mary Anne", "Mary Anne", "Fitzgerald"),
    ("  Yusuf ,  Amina  ", "Amina", "Yusuf"),
    # No comma: the last space decides.
    ("Amina Yusuf", "Amina", "Yusuf"),
    ("Mary Anne Fitzgerald", "Mary Anne", "Fitzgerald"),
    ("Cher", "Cher", ""),
    ("  padded  name  ", "padded", "name"),
    ("", "", ""),
    (None, "", ""),
    ("van der Berg", "van der", "Berg"),
    # Degenerate: half a comma-form name is a name, not two.
    ("Yusuf,", "Yusuf", ""),
])
def test_name_split_rules(full, first, last):
    assert enrich.split_name(full) == (first, last)


def test_a_comma_name_is_never_reversed():
    """The bug this rule exists to prevent.

    Splitting "Aucoin, Alix" on the last space gives firstname
    "Aucoin," — the surname, with a comma on it — and lastname "Alix".
    Fifty contacts would have been created that way.
    """
    first, last = enrich.split_name("Aucoin, Alix")
    assert "," not in first and "," not in last
    assert (first, last) == ("Alix", "Aucoin")


@pytest.mark.parametrize("name, organisation", [
    ("Islamic Food Bank of Toledo", True),
    ("Dream of Detroit", True),
    ("Yusuf, Amina", False),
    ("Cher", False),
    ("", False),
])
def test_a_comma_less_multi_word_name_is_flagged_as_an_organisation(
        name, organisation):
    assert enrich.looks_like_an_organisation(name) is organisation


def test_a_new_contact_gets_the_split_name_and_the_five_properties():
    plan = enrich.contact_plan(
        "bilal@example.net",
        [reg(101, 7002, "bilal@example.net", "Fitzgerald, Mary Anne")],
        {str(e["_id"]): e for e in EVENTS})
    props = enrich.properties_for_new(plan)

    assert props["firstname"] == "Mary Anne"
    assert props["lastname"] == "Fitzgerald"
    assert props[enrich.REGISTRATIONS_PROPERTY] == "101"
    assert props[enrich.COUNT_PROPERTY] == "1"
    assert props[enrich.LAST_REGISTERED_PROPERTY] == "2026-06-02"


def test_a_single_token_name_leaves_lastname_unset():
    plan = enrich.contact_plan("cher@example.net",
                               [reg(102, 7003, "cher@example.net", "Cher")],
                               {str(e["_id"]): e for e in EVENTS})
    props = enrich.properties_for_new(plan)
    assert props["firstname"] == "Cher"
    assert "lastname" not in props


# ---------------------------------------------------------------------------
# Never lower a date
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("existing, computed, expected", [
    ("2026-06-02", "2026-03-14", "2026-06-02"),   # computed is older: keep
    ("2026-03-14", "2026-06-02", "2026-06-02"),   # computed is newer: move
    (None, "2026-06-02", "2026-06-02"),
    ("2026-06-02", None, "2026-06-02"),
    (None, None, None),
    ("1780000000000", "2026-01-01", "2026-05-28"),  # HubSpot epoch ms
    ("2026-06-02T00:00:00Z", "2026-03-14", "2026-06-02"),
])
def test_dates_never_move_backwards(existing, computed, expected):
    assert enrich.merge_date(existing, computed) == expected


def test_an_existing_later_date_survives_a_plan_that_would_lower_it():
    plan = enrich.contact_plan(
        "amina.yusuf@example.org",
        [reg(100, 7001, "a@example.org", "Yusuf, Amina", attended=1)],
        {str(e["_id"]): e for e in EVENTS})
    before = {enrich.LAST_REGISTERED_PROPERTY: "2027-01-01",
              enrich.LAST_ATTENDED_PROPERTY: "2027-01-01"}
    after = enrich.properties_for_existing(plan, before)

    assert after[enrich.LAST_REGISTERED_PROPERTY] == "2027-01-01"
    assert after[enrich.LAST_ATTENDED_PROPERTY] == "2027-01-01"


# ---------------------------------------------------------------------------
# Rows with no email
# ---------------------------------------------------------------------------

def test_rows_with_no_email_are_skipped_and_counted():
    groups, stats = enrich.group_by_email(REGISTRATIONS)
    assert stats["rows_no_email"] == 2
    assert all(email for email in groups)


def test_a_nameless_row_is_never_matched_by_name():
    """The two email-less registrants have names; neither becomes a plan."""
    groups, _ = enrich.group_by_email(REGISTRATIONS)
    planned_names = {
        enrich.contact_plan(email, rows, {})["name"]
        for email, rows in groups.items()}
    assert "Someone, Nameless" not in planned_names
    assert "Address, Also No" not in planned_names


def test_a_profile_under_two_addresses_is_reported_not_merged():
    groups, stats = enrich.group_by_email(REGISTRATIONS)
    assert list(stats["ambiguous_profiles"]) == ["7006"]
    assert stats["ambiguous_profiles"]["7006"] == \
        ["dual.one@example.org", "dual.two@example.org"]
    # Two addresses, so two planned contacts — not one merged contact.
    assert "dual.one@example.org" in groups
    assert "dual.two@example.org" in groups


# ---------------------------------------------------------------------------
# The option set
# ---------------------------------------------------------------------------

def test_options_cover_only_event_dates_with_a_reachable_registrant():
    events = {str(e["_id"]): e for e in EVENTS}
    options = enrich.build_options(REGISTRATIONS, events)
    values = {option["value"] for option in options}
    # 103's only registrant has no email, so no workflow could use it.
    assert values == {"100", "101", "102"}


def test_an_undated_event_says_so_in_its_label():
    events = {str(e["_id"]): e for e in EVENTS}
    options = {o["value"]: o["label"]
               for o in enrich.build_options(REGISTRATIONS, events)}
    assert options["101"] == "Summer Picnic at the lake — 2026-06-02"
    assert options["102"] == "Donor Briefing — undated (id 102)"


def test_an_event_missing_from_the_mirror_still_gets_an_option():
    option = enrich.event_option("999", None)
    assert option == {"label": "Event date 999 — undated (id 999)",
                      "value": "999"}


def test_the_label_uses_the_description_not_the_series_name():
    """event_name is the series and reads the same on almost every date.

    Labelling nineteen options "Event - Other" would be a list no
    workflow could be built from.
    """
    row = {"event_name": "Event - Other",
           "event_description": "AMCF Open House", "event_date": "2026-10-20"}
    assert enrich.event_option("1429", row)["label"] == \
        "AMCF Open House — 2026-10-20"
    # With no description, the series name is better than nothing.
    assert enrich.event_option("1429", {"event_name": "Newsletters"})[
        "label"] == "Newsletters — undated (id 1429)"


# --- the documented limits ------------------------------------------
# knowledge.hubspot.com/properties/property-field-types-in-hubspot:
# 3,000 characters per option INCLUDING label, value and description;
# 512,000 bytes or 5,000 options per property, whichever comes first.

def test_the_per_option_budget_is_shared_with_the_value():
    """3,000 is the whole option's budget, not the label's.

    A label capped at a flat 3,000 would push an option with a long
    value over the limit.
    """
    assert enrich.label_budget("1429") == 3000 - 4
    assert enrich.label_budget("1429", "a description") == 3000 - 4 - 13
    assert enrich.label_budget("x" * 4000) == 0


def test_a_very_long_event_name_is_truncated_to_the_documented_budget():
    option = enrich.event_option("1429", {"event_description": "x" * 5000,
                                          "event_date": "2026-01-01"})
    assert enrich.option_size(option) == enrich.HUBSPOT_ENUM_OPTION_CHARS
    assert len(option["label"]) == enrich.HUBSPOT_ENUM_OPTION_CHARS - 4
    assert option["label"].endswith("…")


def test_a_label_that_fits_is_left_alone():
    option = enrich.event_option("1429", {"event_description": "AMCF Open "
                                          "House", "event_date": "2026-10-20"})
    assert option["label"] == "AMCF Open House — 2026-10-20"


def test_truncation_keeps_the_suffix_that_makes_a_label_unique():
    """The id is what distinguishes the label, so the title gives way."""
    fitted = enrich.fit_label("x" * 100, " (id 1429)", 40)
    assert fitted.endswith(" (id 1429)")
    assert len(fitted) == 40


@pytest.mark.parametrize("options, broken", [
    ([], None),
    ([{"label": "a", "value": "1"}], None),
    ([{"label": "a", "value": str(i)} for i in range(5001)],
     "5,001 options needed"),
    ([{"label": "x" * 3000, "value": "1"}],
     "exceed HubSpot's 3,000-character per-option budget"),
])
def test_every_documented_ceiling_is_checked(options, broken):
    found = enrich.over_limit(options)
    if broken is None:
        assert found is None
    else:
        assert found and broken in found


def test_the_byte_ceiling_is_checked_even_when_the_count_is_fine():
    """500 options is well inside 5,000 and still blows 512,000 bytes."""
    options = [{"label": "x" * 2000, "value": str(i)} for i in range(400)]
    assert len(options) < enrich.HUBSPOT_ENUM_OPTION_LIMIT
    assert all(enrich.option_size(o) <= enrich.HUBSPOT_ENUM_OPTION_CHARS
               for o in options)
    assert "512,000" in enrich.over_limit(options)


# --- unique labels ---------------------------------------------------

DUPLICATE_EVENTS = {
    "200": {"event_date_id": 200, "event_description": "Office Hours",
            "event_date": "2026-05-05"},
    "201": {"event_date_id": 201, "event_description": "Office Hours",
            "event_date": "2026-05-05"},
    "202": {"event_date_id": 202, "event_description": "Open House",
            "event_date": "2026-05-06"},
}

DUPLICATE_REGISTRATIONS = [
    reg(200, 1, "a@example.org", "One, A"),
    reg(201, 2, "b@example.org", "Two, B"),
    reg(202, 3, "c@example.org", "Three, C"),
]


def test_two_event_dates_with_the_same_label_both_get_their_id():
    options = {o["value"]: o["label"] for o in enrich.build_options(
        DUPLICATE_REGISTRATIONS, DUPLICATE_EVENTS)}

    # Both, not just the second: a bare label beside a suffixed one
    # reads as the real one, and neither of them is.
    assert options["200"] == "Office Hours — 2026-05-05 (id 200)"
    assert options["201"] == "Office Hours — 2026-05-05 (id 201)"
    # The one that was already distinct is left alone.
    assert options["202"] == "Open House — 2026-05-06"


def test_every_label_in_an_option_set_is_unique():
    labels = [o["label"] for o in enrich.build_options(
        DUPLICATE_REGISTRATIONS, DUPLICATE_EVENTS)]
    assert len(labels) == len(set(labels))

    labels = [o["label"] for o in enrich.build_options(
        REGISTRATIONS, {str(e["_id"]): e for e in EVENTS})]
    assert len(labels) == len(set(labels))


def test_a_duplicate_label_still_fits_the_budget_after_the_id_is_added():
    events = {"200": {"event_description": "x" * 5000,
                      "event_date": "2026-05-05"},
              "201": {"event_description": "x" * 5000,
                      "event_date": "2026-05-05"}}
    options = enrich.build_options(
        [reg(200, 1, "a@example.org", "One, A"),
         reg(201, 2, "b@example.org", "Two, B")], events)

    assert len({o["label"] for o in options}) == 2
    for option in options:
        assert enrich.option_size(option) <= enrich.HUBSPOT_ENUM_OPTION_CHARS
        assert option["label"].endswith(f" (id {option['value']})")


# --- --exclude-emails ------------------------------------------------

def test_exclusion_file_parsing(tmp_path):
    path = tmp_path / "skip.txt"
    path.write_text("\n".join([
        "# people who asked not to be in HubSpot",
        "",
        "  Amina.Yusuf@Example.ORG  ",
        "cher@example.net",
        "not-an-address",
    ]), encoding="utf-8")

    excluded, stats = enrich.read_exclusions(str(path))
    assert excluded == {"amina.yusuf@example.org", "cher@example.net"}
    assert stats["lines"] == 3
    assert stats["unreadable_lines"] == ["not-an-address"]


def test_no_exclusion_file_excludes_nobody():
    excluded, stats = enrich.read_exclusions(None)
    assert excluded == set()
    assert stats["addresses"] == 0


def test_an_excluded_address_produces_no_planned_write():
    excluded = {"amina.yusuf@example.org", "bilal@example.net"}
    events = {str(e["_id"]): e for e in EVENTS}
    contacts = {"amina.yusuf@example.org": contact(
        11, "amina.yusuf@example.org")}

    plan = enrich.build_plan(REGISTRATIONS, events, contacts, excluded)

    planned = {record["plan"]["email"]
               for record in plan["updates"] + plan["creates"]
               + plan["unchanged"]}
    assert planned.isdisjoint(excluded)
    # Amina is in HubSpot and would otherwise have been an update;
    # Bilal would otherwise have been a create. Neither is planned.
    assert plan["updates"] == []
    assert {r["plan"]["email"] for r in plan["creates"]} == {
        "cher@example.net", "dual.one@example.org", "dual.two@example.org"}


def test_excluded_rows_are_counted_not_forgotten():
    _, stats = enrich.group_by_email(
        REGISTRATIONS, {"amina.yusuf@example.org"})
    # Amina appears on two event dates: two rows, one address.
    assert stats["excluded_rows"] == 2
    assert stats["excluded_addresses"] == ["amina.yusuf@example.org"]


def test_an_excluded_address_never_reaches_an_upsert_payload(loaded,
                                                             tmp_path):
    skip = tmp_path / "skip.txt"
    skip.write_text("bilal@example.net\n", encoding="utf-8")
    client = SpyHubSpot()

    code, _, _, report = run(["--apply", "--exclude-emails", str(skip)],
                             client, tmp_path)

    assert code == enrich.EXIT_OK
    sent = [payload for method, endpoint, payload in client.calls
            if endpoint.endswith("/batch/upsert")]
    addresses = {row["id"] for batch in sent for row in batch}
    assert "bilal@example.net" not in addresses
    assert "cher@example.net" in addresses
    # And it is not quietly read from HubSpot either.
    looked_up = [payload for _, endpoint, payload in client.calls
                 if endpoint.endswith("/batch/read")]
    assert all("bilal@example.net" not in batch for batch in looked_up)


def test_the_report_counts_the_exclusions(loaded, tmp_path):
    skip = tmp_path / "skip.txt"
    skip.write_text("amina.yusuf@example.org\n", encoding="utf-8")
    _, out, _, report = run(["--exclude-emails", str(skip)], SpyHubSpot(),
                            tmp_path)

    assert "| rows excluded by operator | 2 |" in report
    assert "| …distinct addresses excluded | 1 |" in report
    assert "## Excluded by operator" in report
    assert "a*@example.org" in report
    assert "amina.yusuf@example.org" not in report
    assert "excluded by operator" in out


def test_an_event_whose_only_reachable_registrant_is_excluded_loses_its_option():
    events = {str(e["_id"]): e for e in EVENTS}
    options = {o["value"] for o in enrich.build_options(
        REGISTRATIONS, events, {"cher@example.net"})}
    # 102's only registrant with an address was Cher.
    assert "102" not in options
    assert {"100", "101"} <= options


def test_both_checkbox_properties_share_one_option_set():
    options = enrich.build_options(REGISTRATIONS,
                                   {str(e["_id"]): e for e in EVENTS})
    definitions = {d["name"]: d for d in enrich.property_definitions(options)}
    assert definitions[enrich.REGISTRATIONS_PROPERTY]["options"] == \
        definitions[enrich.ATTENDED_PROPERTY]["options"]
    assert definitions[enrich.LAST_REGISTERED_PROPERTY]["type"] == "date"
    assert definitions[enrich.COUNT_PROPERTY]["type"] == "number"
    assert all(d["groupName"] == "amcf_events" for d in definitions.values())


def test_too_many_options_stops_rather_than_truncating(loaded, tmp_path,
                                                       monkeypatch):
    monkeypatch.setattr(enrich, "HUBSPOT_ENUM_OPTION_LIMIT", 2)
    code, out, err, _ = run([], SpyHubSpot(), tmp_path)
    assert code == enrich.EXIT_TOO_MANY_OPTIONS
    assert "3 options needed but HubSpot allows 2" in err
    assert "truncat" in err


# ---------------------------------------------------------------------------
# What an existing contact gets, and does not get
# ---------------------------------------------------------------------------

def test_only_the_five_properties_are_written_to_an_existing_contact():
    plan = enrich.contact_plan(
        "amina.yusuf@example.org",
        [r for r in REGISTRATIONS if r["profile_id"] == 7001],
        {str(e["_id"]): e for e in EVENTS})
    after = enrich.properties_for_existing(
        plan, {"firstname": "Ami", "lastname": "Y", "email": "a@b.org"})

    assert set(after) <= set(enrich.PLANNED_PROPERTIES)
    assert "firstname" not in after
    assert "lastname" not in after


def test_existing_ticks_are_unioned_not_replaced():
    plan = enrich.contact_plan(
        "amina.yusuf@example.org",
        [r for r in REGISTRATIONS if r["profile_id"] == 7001],
        {str(e["_id"]): e for e in EVENTS})
    after = enrich.properties_for_existing(
        plan, {enrich.REGISTRATIONS_PROPERTY: "55;100"})

    assert after[enrich.REGISTRATIONS_PROPERTY].split(";") == \
        ["55", "100", "101"]


def test_attended_is_taken_only_from_rows_csuite_marked_attended():
    plan = enrich.contact_plan(
        "amina.yusuf@example.org",
        [r for r in REGISTRATIONS if r["profile_id"] == 7001],
        {str(e["_id"]): e for e in EVENTS})
    # Two registrations, one check-in — never inferred from rsvp, which
    # is set on both.
    assert plan["registered"] == ["100", "101"]
    assert plan["attended"] == ["100"]
    assert plan["last_registered"] == "2026-06-02"
    assert plan["last_attended"] == "2026-03-14"


def test_a_contact_with_nothing_to_change_is_not_planned_for_a_write(
        loaded, tmp_path):
    client = SpyHubSpot(contacts={
        "cher@example.net": contact(
            1, "cher@example.net",
            **{enrich.REGISTRATIONS_PROPERTY: "102",
               enrich.ATTENDED_PROPERTY: "",
               enrich.COUNT_PROPERTY: "1"})})
    registrations = [r for r in REGISTRATIONS
                     if r["event_profile_email"] == "cher@example.net"]
    plan = enrich.build_plan(registrations,
                             {str(e["_id"]): e for e in EVENTS},
                             client.contacts)
    assert plan["updates"] == []
    assert len(plan["unchanged"]) == 1


# ---------------------------------------------------------------------------
# The dry run makes no write
# ---------------------------------------------------------------------------

def test_dry_run_never_calls_a_write_method(loaded, tmp_path):
    client = SpyHubSpot(contacts={
        "amina.yusuf@example.org": contact(11, "amina.yusuf@example.org"),
        "cher@example.net": contact(12, "cher@example.net"),
    })
    code, out, err, report = run([], client, tmp_path)

    assert code == enrich.EXIT_OK
    assert client.writes == [], \
        f"a dry run attempted HubSpot writes: {client.writes}"
    assert client.calls, "the dry run made no HubSpot call at all"
    assert "DRY RUN" in out
    assert report


def test_apply_is_what_reaches_the_writes(loaded, tmp_path):
    """The other half of the previous test: the writes exist and work.

    Without this, "no writes happened" would also pass if the write path
    were broken or absent.
    """
    client = SpyHubSpot()
    code, out, err, _ = run(["--apply"], client, tmp_path)

    assert code == enrich.EXIT_OK
    endpoints = [endpoint for _, endpoint in client.writes]
    assert "crm/v3/properties/contacts/groups" in endpoints
    assert endpoints.count("crm/v3/properties/contacts") == 5
    assert "crm/v3/objects/contacts/batch/upsert" in endpoints


def test_upserts_are_keyed_by_email_and_batched_at_one_hundred():
    records = [{"plan": {"email": f"p{i}@example.org"},
                "properties": {enrich.COUNT_PROPERTY: "1"}}
               for i in range(250)]
    chunks = list(enrich.batches(records))
    assert [len(c) for c in chunks] == [100, 100, 50]

    inputs = enrich.upsert_inputs(records[:2])
    assert inputs[0] == {"id": "p0@example.org",
                         "properties": {enrich.COUNT_PROPERTY: "1"}}


# ---------------------------------------------------------------------------
# The report
# ---------------------------------------------------------------------------

def test_the_report_masks_every_address(loaded, tmp_path):
    client = SpyHubSpot(contacts={
        "amina.yusuf@example.org": contact(11, "amina.yusuf@example.org")})
    _, out, _, report = run([], client, tmp_path)

    for text in (out, report):
        assert "amina.yusuf@example.org" not in text
        assert "bilal@example.net" not in text
    assert "a*@example.org" in report


def test_the_report_names_the_existing_event_properties(loaded, tmp_path):
    _, _, _, report = run([], SpyHubSpot(), tmp_path)
    assert "hs_eventbrite_lastregisteredevent" in report
    assert "amcf_event_registrations" in report
    assert "marketing contact" in report.lower()


def test_counts_land_in_the_summary(loaded, tmp_path):
    client = SpyHubSpot(contacts={
        "amina.yusuf@example.org": contact(11, "amina.yusuf@example.org")})
    _, out, _, report = run([], client, tmp_path)

    # 5 addresses; 1 already in HubSpot; 2 rows carried no address.
    assert "| contacts matched (already in HubSpot) | 1 |" in report
    assert "| contacts to create | **4** |" in report
    assert "| rows skipped, no email | 2 |" in report
    assert "| CSuite profiles under more than one email | 1 |" in report
    assert "| registration rows read | 8 |" in report
    assert "DRY RUN" in out


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------

def test_an_empty_mirror_stops_with_exit_2(mirror, tmp_path):
    mirror.load({})
    client = SpyHubSpot()
    code, out, err, _ = run([], client, tmp_path)
    assert code == enrich.EXIT_NOT_LOADED
    assert "mirror not loaded for event_registration" in err
    assert client.calls == [], "the mirror guard ran after calling HubSpot"


def test_a_failed_property_read_stops_before_planning(loaded, tmp_path):
    client = SpyHubSpot()
    client.get_contact_properties = lambda archived=False: {"error": "401"}
    code, _, err, _ = run([], client, tmp_path)
    assert code == enrich.EXIT_FAILED
    assert "401" in err


def test_the_script_never_touches_csuite():
    import inspect
    source = inspect.getsource(enrich)
    assert "CSuiteClient" not in source
    assert "fetch_all" not in source
