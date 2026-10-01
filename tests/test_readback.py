"""A 200 from CSuite means accepted, not stored.

2026-09-30: profile/create/individual was sent primary_email and returned
HTTP 200 with a profile_id. profile/display showed primary_email: None —
the field exists, nothing was stored in it. CSuite validates the fields
it knows and discards the rest without comment.

No network.
"""

import pytest

from sync.readback import (DERIVED_FIELDS, FieldDropped, compare,
                           normalise_email, normalise_payload, stored_name,
                           verify)
from sync.sandbox_writes import WriteBudget, sandbox_write

SANDBOX_URL = "https://amuslimcf-sandbox.fcsuite.com/api/v2"


def display(**fields):
    base = {"profile_id": 21625, "first_name": None, "last_name": None,
            "primary_email": None, "primary_phone_number": None,
            "website": None, "name": "derived", "label": "derived",
            "ptype": "indiv", "modified_ts": "2026-09-30 15:22:29"}
    base.update(fields)
    return base


def reader(stored):
    def read(endpoint, body):
        return {"success": True, "http_status": 200, "outcome": "ok",
                "data": stored}
    return read


# ---------------------------------------------------------------------------
# compare
# ---------------------------------------------------------------------------

def test_everything_stored_is_no_drop():
    sent = {"first_name": "HUBSYNC", "last_name": "SENTINEL"}
    assert compare(sent, display(first_name="HUBSYNC",
                                 last_name="SENTINEL")) == {}


def test_a_dropped_field_is_reported():
    """The exact failure of 2026-09-30."""
    sent = {"primary_email": "hubsync-sentinel@example.invalid"}
    dropped = compare(sent, display(primary_email=None))
    assert "primary_email" in dropped
    assert dropped["primary_email"][1] is None


def test_a_partial_drop_names_only_what_was_lost():
    sent = {"first_name": "HUBSYNC", "last_name": "SENTINEL",
            "primary_email": "a@b.invalid"}
    dropped = compare(sent, display(first_name="HUBSYNC",
                                    last_name="SENTINEL"))
    assert set(dropped) == {"primary_email"}


def test_derived_fields_are_not_compared():
    """name, label and ptype are assembled by CSuite.

    Comparing them would report a difference on every single write.
    """
    sent = {"name": "whatever", "ptype": "x", "first_name": "HUBSYNC"}
    assert compare(sent, display(first_name="HUBSYNC")) == {}
    for field in ("name", "label", "ptype", "modified_ts", "created_ts"):
        assert field in DERIVED_FIELDS


def test_env_and_epoch_are_not_compared():
    """The client adds both; neither is stored on a profile."""
    assert compare({"env": "sandbox", "epoch": 1738194125}, display()) == {}


def test_a_sent_none_is_not_a_drop():
    assert compare({"website": None}, display(website=None)) == {}


@pytest.mark.parametrize("sent, stored", [
    (1, "1"), ("1005", 1005), (" HUBSYNC ", "HUBSYNC"), (True, "1"),
])
def test_values_are_compared_as_text(sent, stored):
    """CSuite returns 1 for a boolean and "1005" for an integer id often
    enough that strict equality would report differences that are not."""
    assert compare({"x": sent}, {"x": stored}) == {}


def test_the_input_name_maps_to_the_stored_name():
    """CSuite's create inputs are not always its display outputs.

    That mismatch is the whole reason this module exists.
    """
    assert stored_name("email") == "primary_email"
    assert stored_name("first_name") == "first_name"
    assert compare({"email": "a@b.invalid"},
                   display(primary_email="a@b.invalid")) == {}


# ---------------------------------------------------------------------------
# Email normalisation — matching is exact and case-sensitive
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    ("  A@B.Invalid ", "a@b.invalid"),
    ("A@B.INVALID", "a@b.invalid"),
    ("a@b.invalid", "a@b.invalid"),
    ("", None), ("   ", None), (None, None),
])
def test_emails_are_trimmed_and_lowercased(raw, expected):
    assert normalise_email(raw) == expected


def test_a_payload_has_every_email_field_normalised():
    out = normalise_payload({"email": " A@B.Invalid ",
                             "primary_email": "C@D.INVALID",
                             "first_name": "  Keep Me  "})
    assert out["email"] == "a@b.invalid"
    assert out["primary_email"] == "c@d.invalid"
    assert out["first_name"] == "  Keep Me  ", "only emails are touched"


def test_case_differing_addresses_compare_equal():
    """Otherwise a read-back would fail on a write that actually worked."""
    assert compare({"email": "A@B.Invalid"},
                   display(primary_email="a@b.invalid")) == {}


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------

def test_verify_returns_the_record_when_nothing_was_dropped():
    stored = display(first_name="HUBSYNC")
    assert verify(reader(stored), "profile/edit",
                  {"first_name": "HUBSYNC"}, 21625) == stored


def test_verify_raises_and_names_the_dropped_fields():
    with pytest.raises(FieldDropped) as caught:
        verify(reader(display()), "profile/create/individual",
               {"email": "a@b.invalid", "primary_phone_number": "555"}, 21625)
    message = str(caught.value)
    assert "email" in message and "primary_phone_number" in message
    assert "accepted" in message and "did NOT store" in message
    assert caught.value.record_id == 21625


def test_a_failed_read_back_is_a_failure_not_a_pass():
    def broken(endpoint, body):
        return {"success": False, "http_status": 500,
                "outcome": "server_error", "error": "boom"}

    with pytest.raises(FieldDropped):
        verify(broken, "profile/edit", {"website": "x"}, 21625)


def test_an_unreadable_record_is_its_own_kind_of_failure():
    """Still a FieldDropped, so anything that stops keeps stopping — but
    nameable, so a report can say "not checked" rather than "lost"."""
    from sync.readback import ReadBackUnavailable

    def broken(endpoint, body):
        return {"success": False, "error": "boom"}

    with pytest.raises(ReadBackUnavailable):
        verify(broken, "profile/edit", {"website": "x"}, 21625)
    assert issubclass(ReadBackUnavailable, FieldDropped)


def test_a_list_shaped_display_is_unwrapped():
    def read(endpoint, body):
        return {"success": True, "data": [display(first_name="HUBSYNC")]}

    assert verify(read, "profile/edit", {"first_name": "HUBSYNC"}, 21625)


# ---------------------------------------------------------------------------
# Wired into the sandbox write path
# ---------------------------------------------------------------------------

class Client:
    base_url = SANDBOX_URL
    env = "sandbox"
    api_key = "k"
    api_secret = "s"

    def __init__(self, stored=None, created_id=21625):
        self.stored = stored if stored is not None else display()
        self.created_id = created_id
        self.sent = None

    def _request(self, endpoint, data=None):
        self.sent = data
        return {"success": True, "http_status": 200, "outcome": "ok",
                "data": {"profile_id": self.created_id}}


def test_the_sandbox_write_path_verifies_by_default_when_asked():
    client = Client(stored=display(first_name="HUBSYNC"))
    result = sandbox_write(client, "profile/create/individual",
                           {"first_name": "HUBSYNC"}, WriteBudget(1),
                           verify_with=reader(display(first_name="HUBSYNC")))
    assert result["success"] is True


def test_a_dropped_field_raises_out_of_the_sandbox_write_path():
    client = Client()
    with pytest.raises(FieldDropped):
        sandbox_write(client, "profile/create/individual",
                      {"email": "a@b.invalid"}, WriteBudget(1),
                      verify_with=reader(display(primary_email=None)))


def test_the_write_path_normalises_emails_before_sending():
    client = Client(stored=display(primary_email="a@b.invalid"))
    sandbox_write(client, "profile/create/individual",
                  {"email": "  A@B.Invalid "}, WriteBudget(1),
                  verify_with=reader(display(primary_email="a@b.invalid")))
    assert client.sent["email"] == "a@b.invalid"


def test_verification_is_off_unless_a_reader_is_given():
    """The production path keeps its existing behaviour."""
    client = Client()
    result = sandbox_write(client, "profile/edit", {"website": "x"},
                           WriteBudget(1))
    assert result["success"] is True


def test_the_production_flag_is_on_by_default():
    """Flipped 2026-09-30. A dropped field is invisible without it."""
    from clients.csuite import CSuiteClient
    assert CSuiteClient.verify_writes is True


# ---------------------------------------------------------------------------
# Reformatted is not dropped
# ---------------------------------------------------------------------------

def test_a_value_csuite_punctuates_is_stored_not_dropped():
    """2026-09-30: profile/edit on 21626 was sent phone_number
    "7035550100" and profile/display returned primary_phone_number
    "703-555-0100". Text comparison called that a drop, which would have
    put a false error in front of a user on every phone write."""
    from sync.readback import compare_detail

    stored = display(primary_phone_number="703-555-0100")
    dropped, reformatted = compare_detail({"phone_number": "7035550100"},
                                          stored)
    assert dropped == {}
    assert "phone_number" in reformatted


def test_a_reformatted_value_does_not_raise():
    read = reader(display(primary_phone_number="703-555-0100"))
    assert verify(read, "profile/edit", {"phone_number": "7035550100"}, 21626)


@pytest.mark.parametrize("sent,stored", [
    ("7035550100", "703-555-0101"),   # a different number
    ("7035550100", "703-555-010"),    # truncated
    ("7035550100", ""),               # blank
    ("7035550100", "n/a"),            # no digits at all
])
def test_a_value_that_is_not_the_same_value_is_still_dropped(sent, stored):
    """The allowance is for punctuation, not for a different answer."""
    from sync.readback import compare_detail

    dropped, reformatted = compare_detail(
        {"phone_number": sent}, display(primary_phone_number=stored))
    assert "phone_number" in dropped
    assert reformatted == {}


def test_punctuation_alone_is_not_enough_to_match():
    """Two values of pure punctuation share an empty digit string. Matching
    on that would make any two unrecognised values look equal."""
    from sync.readback import compare_detail

    dropped, _ = compare_detail({"website": "---"},
                                display(website="..."))
    assert "website" in dropped


def test_compare_still_returns_only_real_drops():
    """compare() is the older name and several callers use it."""
    stored = display(primary_phone_number="703-555-0100", primary_email=None)
    assert compare({"phone_number": "7035550100"}, stored) == {}
    assert "email" in compare({"email": "a@b.invalid"}, stored)


# ---------------------------------------------------------------------------
# An untouched record stored nothing, whatever the fields say
# ---------------------------------------------------------------------------

def test_an_unchanged_modified_ts_means_nothing_was_stored():
    """2026-09-30: profile/edit on 21626 was sent four unrecognised address
    fields. 200, success: true, 0 of 81 fields changed, modified_ts
    byte-identical. The success flag said nothing; modified_ts said it all."""
    from sync.readback import NothingStored

    stamp = "2026-09-30 16:13:49.24411"
    read = reader(display(modified_ts=stamp))
    with pytest.raises(NothingStored) as caught:
        verify(read, "profile/edit", {"primary_city": "Fairfax"}, 21626,
               modified_before=stamp)
    assert "did NOT touch the record" in str(caught.value)
    assert "modified_ts is unchanged" in str(caught.value)


def test_nothing_stored_beats_a_field_comparison_that_looks_fine():
    """The stronger statement wins.

    A record CSuite did not write to cannot have stored anything, however
    the per-field guess comes out — and the comparison can come out clean,
    because a field already holding the sent value looks stored.
    """
    from sync.readback import NothingStored

    stamp = "2026-09-30 16:13:49.24411"
    read = reader(display(modified_ts=stamp, website="https://kept.invalid"))
    with pytest.raises(NothingStored):
        verify(read, "profile/edit", {"website": "https://kept.invalid"},
               21626, modified_before=stamp)


def test_a_moved_modified_ts_lets_the_field_check_decide():
    read = reader(display(modified_ts="2026-09-30 17:00:00",
                          website="https://example.invalid"))
    assert verify(read, "profile/edit", {"website": "https://example.invalid"},
                  21626, modified_before="2026-09-30 16:13:49.24411")


def test_a_moved_modified_ts_still_catches_a_real_drop():
    read = reader(display(modified_ts="2026-09-30 17:00:00"))
    with pytest.raises(FieldDropped):
        verify(read, "profile/edit", {"website": "https://example.invalid"},
               21626, modified_before="2026-09-30 16:13:49.24411")


def test_no_before_state_means_the_check_is_skipped_not_passed():
    """A create has no before-state. None must not read as "unchanged"."""
    read = reader(display(modified_ts="2026-09-30 17:00:00",
                          primary_email="a@b.invalid"))
    assert verify(read, "profile/create/individual",
                  {"email": "a@b.invalid"}, 21626)


def test_nothing_stored_is_a_field_dropped_so_callers_still_stop():
    from sync.readback import NothingStored

    assert issubclass(NothingStored, FieldDropped)


# ---------------------------------------------------------------------------
# A write budget on the client cannot be delegated past
# ---------------------------------------------------------------------------

class Counting:
    """A CSuiteClient whose transport is a list, with a real budget."""

    def __init__(self, limit):
        from clients.csuite import CSuiteClient
        from sync.sandbox_writes import WriteBudget

        self.client = CSuiteClient.__new__(CSuiteClient)
        self.client.api_key = "k"
        self.client.api_secret = "s"
        self.client.base_url = SANDBOX_URL
        self.client.env = "sandbox"
        self.client.verify_writes = False
        self.client.write_budget = WriteBudget(limit)
        self.posted = []

        class Session:
            def post(_self, url, **kwargs):
                self.posted.append(url)
                raise AssertionError("the budget should have refused this")

        self.client.session = Session()


def test_a_client_budget_refuses_the_write_after_the_limit(monkeypatch):
    import clients.csuite as mod
    from sync.sandbox_writes import WriteBudgetExceeded

    monkeypatch.setattr(mod, "reserve_write",
                        lambda *a, **kw: {"id": 1})
    monkeypatch.setattr(mod, "complete_write", lambda *a, **kw: None)

    harness = Counting(0)
    with pytest.raises(WriteBudgetExceeded):
        harness.client._request("profile/edit", {"profile_id": 1})
    assert harness.posted == [], "nothing may be sent once the cap is spent"


def test_a_client_budget_ignores_reads(monkeypatch):
    """Reads do not consume a write cap, and must not be refused by one."""
    harness = Counting(0)

    class Session:
        def post(_self, url, **kwargs):
            class R:
                status_code = 200
                text = '{"success":1,"data":{}}'

                def json(self):
                    return {"success": 1, "data": {}}
            return R()

    harness.client.session = Session()
    result = harness.client._request("profile/display", {"profile_id": 1})
    assert result["success"] is True
    assert harness.client.write_budget.used == 0


def test_delegating_through_a_proxy_still_hits_the_budget(monkeypatch):
    """The 2026-10-01 failure, as a test.

    A proxy that overrides _request and delegates the rest cannot enforce
    anything: the delegated bound method's `self` is the inner client. With
    the budget on the client, the route the call takes stops mattering.
    """
    import clients.csuite as mod
    from sync.sandbox_writes import WriteBudgetExceeded

    monkeypatch.setattr(mod, "reserve_write", lambda *a, **kw: {"id": 1})
    monkeypatch.setattr(mod, "complete_write", lambda *a, **kw: None)

    harness = Counting(0)

    class Proxy:
        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def _request(self, endpoint, data=None):   # never reached, as before
            raise AssertionError("unreachable via a delegated method")

    proxy = Proxy(harness.client)
    with pytest.raises(WriteBudgetExceeded):
        proxy.edit_profile(21626, website="https://example.invalid")
    assert harness.posted == []
