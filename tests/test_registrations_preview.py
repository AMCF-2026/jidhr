"""Registrations: phase 1 previews and writes nothing.

Measured against production 2026-10-07 before any of this was written:

* `event/display/eventdate` is the only endpoint carrying registrants — one
  call per event date.
* A registrant row is {attended, event_profile_email, event_profile_name,
  guests, profile_id, rsvp}. No ticket, order or payment reference, so no
  money field can be synced from here even by accident.
* **No cancelled state exists.** A cancellation is a row that stops being
  returned, indistinguishable from a short read.
* `attended` is set on NONE of the 113 rows across the eleven mapped events,
  so ATTENDED has nothing to send.
* 113 rows dedupe to 109 (person, event) registrations from 100 distinct
  people: 83 resolve to one HubSpot contact, 17 to none, 0 to more than one.
* `hs_marketable_status` is read-only to the API, so this sync cannot make
  anyone marketing. 7 of the matched contacts are deliberately
  non-marketing and this never touches the field.

The 83 + 20 = 103 that prompted the reconciliation was an arithmetic error
of mine, not duplicate data: "new" had been computed as inputs minus
CANONICAL emails, which counted 3 secondary-address matches as new. Keyed on
canonical plus hs_additional_emails it is 83 + 17 = 100.

No network, no database.
"""

import json
from datetime import datetime, timezone

import pytest

from intents import sync_commands
from sync import registrations as reg


def registrant(email, profile_id=1, rsvp=None, attended=None, guests=None):
    row = {"event_profile_email": email, "event_profile_name": "A Person",
           "profile_id": profile_id, "rsvp": rsvp, "attended": attended}
    if guests is not None:
        row["guests"] = guests
    return row


class CSuite:
    """event/display/eventdate, and nothing else."""

    def __init__(self, by_event=None, fail_on=()):
        self.by_event = by_event or {}
        self.fail_on = set(str(e) for e in fail_on)
        self.asked = []

    def _request(self, endpoint, data=None):
        assert endpoint == "event/display/eventdate", endpoint
        rid = str((data or {}).get("event_date_id"))
        self.asked.append(rid)
        if rid in self.fail_on:
            return {"success": False, "error": "CSuite returned 500"}
        return {"success": True,
                "data": {"event_date_id": rid,
                         "profiles": list(self.by_event.get(rid, []))}}


class HubSpot:
    """batch/read by email, answering from a canonical/alias map."""

    def __init__(self, contacts=None, error=None):
        # {canonical_email: (contact_id, marketing, [aliases])}
        self.contacts = contacts or {}
        self.error = error
        self.calls = 0

    def _post(self, endpoint, data=None):
        assert endpoint == "crm/v3/objects/contacts/batch/read", endpoint
        self.calls += 1
        if self.error:
            return {"status": "error", "message": self.error}
        wanted = {i["id"] for i in data["inputs"]}
        results = []
        for canonical, (cid, marketing, aliases) in self.contacts.items():
            if wanted & ({canonical} | set(aliases)):
                results.append({"id": cid, "properties": {
                    "email": canonical,
                    "hs_additional_emails": ";".join(aliases),
                    "hs_marketable_status": "true" if marketing else "false"}})
        return {"results": results, "numErrors": len(wanted) - len(results)}


@pytest.fixture(autouse=True)
def no_db(monkeypatch):
    """No registration_map, no run_log. Those have their own tests."""
    monkeypatch.setattr(reg, "migration_applied", lambda: False)
    monkeypatch.setattr(reg, "load_map", lambda: {})
    monkeypatch.setattr(reg, "open_run", lambda applied: None)
    monkeypatch.setattr(reg, "close_run", lambda *a, **k: None)


def arm(monkeypatch, enabled):
    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", enabled)


# ---------------------------------------------------------------------------
# The flag, and the fact there is no write path
# ---------------------------------------------------------------------------

def test_the_flag_is_off_by_default():
    import config

    assert config.Config.REGISTRATIONS_SYNC_ENABLED is False


@pytest.mark.parametrize("raw,expected", [
    ("true", True), ("TRUE", True), (" true ", True),
    ("false", False), ("1", False), ("yes", False), ("", False),
])
def test_only_the_exact_word_true_enables_it(monkeypatch, raw, expected):
    monkeypatch.setenv("REGISTRATIONS_SYNC_ENABLED", raw)
    import importlib

    import config
    importlib.reload(config)
    try:
        assert config.Config.REGISTRATIONS_SYNC_ENABLED is expected
    finally:
        monkeypatch.delenv("REGISTRATIONS_SYNC_ENABLED", raising=False)
        importlib.reload(config)


def test_a_live_run_is_refused_while_the_flag_is_off(monkeypatch):
    arm(monkeypatch, False)

    with pytest.raises(reg.RegistrationsSyncDisabled) as caught:
        reg.run(csuite=object(), hubspot=object(), dry_run=False)

    assert "REGISTRATIONS_SYNC_ENABLED" in str(caught.value)
    assert "nothing was read" in str(caught.value)


def test_a_live_run_is_refused_AGAIN_when_the_flag_is_on(monkeypatch):
    """Two refusals, not one. The flag exists for phase 2; phase 1 has no
    write path, and "0 registered" must not be what a live run returns."""
    arm(monkeypatch, True)

    with pytest.raises(reg.PhaseOnePreviewOnly) as caught:
        reg.run(csuite=object(), hubspot=object(), dry_run=False)

    assert "preview-only" in str(caught.value)
    assert "Nothing was read" in str(caught.value)


def test_a_live_run_reads_nothing_before_refusing(monkeypatch):
    arm(monkeypatch, True)
    csuite = CSuite()

    with pytest.raises(reg.PhaseOnePreviewOnly):
        reg.run(csuite=csuite, hubspot=HubSpot(), dry_run=False)

    assert csuite.asked == []


def test_nothing_in_the_module_writes_to_hubspot():
    """Phase 1's whole claim, as a source check: no write verb anywhere."""
    import inspect

    source = inspect.getsource(reg)
    for verb in ("_put(", "_patch(", "_delete(", '"PUT"', '"PATCH"',
                 '"DELETE"', "_send_with_status"):
        assert verb not in source, verb
    # The one POST it makes is a read-shaped batch/read.
    assert source.count("_post(") == 1
    assert "contacts/batch/read" in source


# ---------------------------------------------------------------------------
# Dedupe, guests, and the units
# ---------------------------------------------------------------------------

def test_dedupe_is_by_normalised_email():
    rows = [registrant("A@X.Inv"), registrant("a@x.inv"),
            registrant(" a@x.inv ")]

    deduped, dropped = reg.dedupe(rows)

    assert list(deduped) == ["a@x.inv"]
    assert dropped == 2


def test_a_registrant_with_no_email_is_not_a_registration():
    deduped, dropped = reg.dedupe([registrant(None), registrant("")])

    assert deduped == {}
    assert dropped == 0


def test_guests_are_excluded():
    """A guest is a different person on someone else's row, with a different
    field name, who has not given this address to AMCF."""
    rows = [registrant("host@x.inv", guests=[
        {"contact_email": "guest@x.inv", "contact_name": "G", "rsvp": 1}])]

    deduped, _dropped = reg.dedupe(rows)

    assert list(deduped) == ["host@x.inv"]
    assert "guest@x.inv" not in deduped


def test_the_same_person_at_two_events_is_two_registrations(monkeypatch):
    """The unit is (person, event). Reporting one number for both units is
    how 83 + 20 came to be read against 100."""
    csuite = CSuite({"1": [registrant("a@x.inv")],
                     "2": [registrant("a@x.inv")]})
    hubspot = HubSpot({"a@x.inv": ("701", True, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1", "2"))

    assert out["unique_emails"] == 2, "two registrations, one person"
    assert out["would_register"] == 2
    assert hubspot.calls == 1, "one batch/read for both events"


# ---------------------------------------------------------------------------
# Contact resolution, including the alias case
# ---------------------------------------------------------------------------

def test_a_registrant_with_a_contact_would_be_registered():
    csuite = CSuite({"1": [registrant("a@x.inv", profile_id=99, rsvp=1)]})
    hubspot = HubSpot({"a@x.inv": ("701", True, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",))

    assert out["would_register"] == 1
    record = out["events"][0]["would_register"][0]
    assert record["hubspot_contact_id"] == "701"
    assert record["csuite_profile_id"] == "99"
    assert record["rsvp"] == "1"
    assert record["marketing"] is True


def test_a_secondary_address_still_resolves():
    """3 of the 83 matches are this case. Keyed on canonical only they were
    counted as new, which is where 83 + 20 = 103 came from."""
    csuite = CSuite({"1": [registrant("old@x.inv")]})
    hubspot = HubSpot({"new@x.inv": ("701", True, ["old@x.inv"])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",))

    assert out["would_register"] == 1
    assert out["withheld"] == 0


def test_a_registrant_with_no_contact_is_withheld_not_created():
    csuite = CSuite({"1": [registrant("nobody@x.inv")]})

    out = reg.run(csuite=csuite, hubspot=HubSpot({}), event_ids=("1",))

    assert out["withheld"] == 1
    assert out["would_register"] == 0
    why = out["events"][0]["withheld"][0]["why"]
    assert "no HubSpot contact" in why
    assert "not created" in why


def test_a_non_marketing_contact_is_registered_and_counted(monkeypatch):
    """Registered, and hs_marketable_status never touched — it is read-only
    to the API, so this sync could not change it even if asked."""
    csuite = CSuite({"1": [registrant("a@x.inv")]})
    hubspot = HubSpot({"a@x.inv": ("701", False, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",))

    assert out["would_register"] == 1
    assert out["non_marketing"] == 1
    assert out["events"][0]["would_register"][0]["marketing"] is False


def test_an_unreadable_contact_index_refuses_to_plan():
    """Without it every registrant looks new, and every new registrant looks
    like a contact to create."""
    csuite = CSuite({"1": [registrant("a@x.inv")]})

    out = reg.run(csuite=csuite, hubspot=HubSpot(error="403 forbidden"),
                  event_ids=("1",))

    assert "could not be read" in out["error"]
    assert out["would_register"] == 0
    assert out["events"] == []


# ---------------------------------------------------------------------------
# One bad event does not become an event with no registrants
# ---------------------------------------------------------------------------

def test_a_failed_event_read_is_review_not_zero_registrants():
    csuite = CSuite({"1": [registrant("a@x.inv")], "2": []}, fail_on=["2"])
    hubspot = HubSpot({"a@x.inv": ("701", True, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1", "2"))

    assert out["events_read"] == 1, "only one event was actually read"
    assert out["review"] == 1
    assert "the registrant read failed" in out["review_rows"][0][1]
    assert out["would_register"] == 1, "the good event still planned"


# ---------------------------------------------------------------------------
# The cancellation guard: refuses, never infers
# ---------------------------------------------------------------------------

def test_an_empty_list_against_known_rows_is_refused():
    """Zero registrants and a failed read are the same response from
    CSuite, so nothing is inferred from zero."""
    why = reg.shrink_guard("1", [], known_count=12)

    assert why and "came back EMPTY" in why
    assert "nothing is inferred" in why


def test_an_empty_list_with_no_known_rows_is_fine():
    """An event nobody has registered for is not a failure."""
    assert reg.shrink_guard("1", [], known_count=0) is None


def test_a_sharp_drop_is_refused():
    why = reg.shrink_guard("1", [registrant(f"{i}@x.inv") for i in range(2)],
                           known_count=12)

    assert why and "fell from 12 to 2" in why
    assert "not as 10 cancellations" in why


def test_a_small_drop_is_allowed_through():
    """A drop from 12 to 11 is a cancellation; the guard is for collapses."""
    rows = [registrant(f"{i}@x.inv") for i in range(11)]

    assert reg.shrink_guard("1", rows, known_count=12) is None


def test_growth_is_never_refused():
    rows = [registrant(f"{i}@x.inv") for i in range(20)]

    assert reg.shrink_guard("1", rows, known_count=12) is None


def test_a_guarded_event_plans_nothing_at_all(monkeypatch):
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {
        ("1", f"{i}@x.inv"): {"last_state": "REGISTERED"} for i in range(12)})
    csuite = CSuite({"1": []})

    out = reg.run(csuite=csuite, hubspot=HubSpot({}), event_ids=("1",))

    assert out["review"] == 1
    assert out["would_register"] == 0
    assert out["withheld"] == 0


def test_phase_1_asserts_no_cancellations_at_all():
    """The guard exists from the start, but nothing uses it to cancel: there
    is no CANCELLED anywhere in the module."""
    import inspect

    source = inspect.getsource(reg)
    assert "CANCELLED" in source, "the state is named in the docstring"
    assert source.count("'CANCELLED'") == 0
    assert '"CANCELLED"' not in source.replace(
        "('REGISTERED', 'ATTENDED', 'CANCELLED')", "")


def test_only_REGISTERED_is_ever_planned():
    assert reg.REGISTERED == "REGISTERED"
    import inspect
    source = inspect.getsource(reg.plan_event)
    assert "ATTENDED" not in source


# ---------------------------------------------------------------------------
# A record already registered is not registered again
# ---------------------------------------------------------------------------

def test_a_known_registration_is_not_sent_again(monkeypatch):
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {
        ("1", "a@x.inv"): {"last_state": "REGISTERED"}})
    csuite = CSuite({"1": [registrant("a@x.inv")]})
    hubspot = HubSpot({"a@x.inv": ("701", True, [])})

    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",))

    assert out["already"] == 1
    assert out["would_register"] == 0


# ---------------------------------------------------------------------------
# run_log.outcomes carries the inputs, hashed
# ---------------------------------------------------------------------------

def test_the_outcome_rows_carry_the_per_record_inputs():
    csuite = CSuite({"1": [registrant("a@x.inv", profile_id=99, rsvp=1)],
                     "2": [registrant("nobody@x.inv")]})
    hubspot = HubSpot({"a@x.inv": ("701", True, [])})
    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1", "2"))

    rows = reg._outcome_rows(out)
    assert len(rows) == 2
    kinds = {r["outcome"] for r in rows}
    assert kinds == {"would_register", "withheld"}
    for row in rows:
        assert row["event_date_id"] in ("1", "2")
        assert row["email_sha1"]


def test_no_email_address_reaches_the_run_log():
    """clients/audit.payload_meta draws the same line: an id is the point of
    an audit trail, a value is not."""
    csuite = CSuite({"1": [registrant("secret@donor.invalid")]})
    hubspot = HubSpot({"secret@donor.invalid": ("701", True, [])})
    out = reg.run(csuite=csuite, hubspot=hubspot, event_ids=("1",))

    blob = json.dumps(reg._outcome_rows(out))
    assert "secret@donor.invalid" not in blob
    assert "donor.invalid" not in blob
    assert "secret" not in blob


def test_the_fingerprint_is_stable_and_normalised():
    assert reg.email_fingerprint("A@X.Inv") == reg.email_fingerprint("a@x.inv")
    assert reg.email_fingerprint(" a@x.inv ") == reg.email_fingerprint("a@x.inv")
    assert reg.email_fingerprint("a@x.inv") != reg.email_fingerprint("b@x.inv")
    assert len(reg.email_fingerprint("a@x.inv")) == 12


def test_a_review_event_appears_in_the_outcome_rows(monkeypatch):
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "load_map", lambda: {
        ("1", f"{i}@x.inv"): {"last_state": "REGISTERED"} for i in range(12)})
    out = reg.run(csuite=CSuite({"1": []}), hubspot=HubSpot({}),
                  event_ids=("1",))

    rows = reg._outcome_rows(out)
    assert [r["outcome"] for r in rows] == ["review"]
    assert "EMPTY" in rows[0]["why"]


def test_the_run_log_row_is_closed_even_when_the_run_dies(monkeypatch):
    closed = {}
    monkeypatch.setattr(reg, "open_run", lambda applied: 7)
    monkeypatch.setattr(reg, "close_run",
                        lambda rid, status, counts, outcomes=None,
                        error_summary=None: closed.update(
                            id=rid, status=status))
    monkeypatch.setattr(reg, "read_registrants",
                        lambda c, e: (_ for _ in ()).throw(
                            RuntimeError("nobody predicted this")))

    with pytest.raises(RuntimeError):
        reg.run(csuite=CSuite(), hubspot=HubSpot(), event_ids=("1",))

    assert closed["id"] == 7


# ---------------------------------------------------------------------------
# Scope and the chat surface
# ---------------------------------------------------------------------------

def test_phase_one_is_the_eleven_mapped_events():
    assert reg.PHASE_ONE_EVENT_IDS == (
        "1153", "1155", "1157", "1159", "1168", "1429", "1430", "1462",
        "1463", "1464", "1466")
    assert len(reg.PHASE_ONE_EVENT_IDS) == 11


def test_the_scope_is_not_discovered_at_runtime():
    """A sync that decides for itself which events to touch is one whose
    blast radius changes without anyone editing it."""
    import inspect

    signature = inspect.signature(reg.run)
    assert signature.parameters["event_ids"].default is reg.PHASE_ONE_EVENT_IDS


def test_chat_can_ask_for_the_preview():
    assert sync_commands.can_handle("sync registrations dry run")
    assert sync_commands.can_handle("sync registrations")


def test_sync_all_cannot_reach_the_registrations_sync():
    """Phase 1 writes nothing, but the day it does, three words should not
    be what starts it."""
    for phrase in sync_commands.ALL_SYNC_PHRASES:
        assert "registration" not in phrase
    for phrase in sync_commands.REGISTRATION_SYNC_PHRASES:
        assert phrase not in sync_commands.ALL_SYNC_PHRASES


def test_a_plain_request_is_refused_not_run(monkeypatch):
    """"sync registrations" without "dry run" asks for a live run."""
    monkeypatch.setattr(
        "sync.registrations.run",
        lambda **kw: (_ for _ in ()).throw(
            reg.RegistrationsSyncDisabled("the flag is off")))

    reply = sync_commands.handle("sync registrations", None)

    assert "⏸️" in reply
    assert "turned off" in reply
    assert "sync registrations dry run" in reply


def test_the_preview_reports_in_the_right_units():
    reply = sync_commands._format_registration_results({
        "events_read": 11, "registrant_rows": 113, "unique_emails": 109,
        "duplicates_dropped": 3, "would_register": 92, "withheld": 17,
        "already": 0, "review": 0, "review_rows": [], "non_marketing": 7,
        "csuite_calls": 11, "hubspot_calls": 1, "migration_applied": False,
        "run_logged": True, "run_id": 7, "error": None})

    assert "**113** registrant rows" in reply
    assert "**109** registrations after dedupe by email within each event" in reply
    assert "**92** would be sent as REGISTERED" in reply
    assert "**17** withheld" in reply
    assert "**7** of those contacts are deliberately NON-marketing" in reply
    assert "0 HubSpot writes — phase 1 has no write path" in reply
    assert "005_registration_map.sql" in reply
    assert "hashed addresses, never addresses" in reply


def test_the_preview_reports_a_refusal_as_the_whole_reply():
    reply = sync_commands._format_registration_results(
        {"error": "HubSpot contacts could not be read (403)"})

    assert reply.startswith("❌ **Registrations preview stopped.**")
    assert "registrant rows" not in reply


# ---------------------------------------------------------------------------
# The migration
# ---------------------------------------------------------------------------

def test_the_migration_is_additive_and_not_executed():
    sql = open("migrations/005_registration_map.sql").read()

    assert "CREATE TABLE IF NOT EXISTS hubsync.registration_map" in sql
    assert "UNIQUE (csuite_eventdate_id, contact_email)" in sql
    assert "NOT EXECUTED" in sql
    for forbidden in ("DROP TABLE", "DELETE FROM", "ALTER TABLE public"):
        assert forbidden not in sql


def test_nothing_executes_the_migration():
    import subprocess

    found = subprocess.run(
        ["grep", "-rn", "005_registration_map", "--include=*.py", "."],
        capture_output=True, text=True).stdout
    for line in found.strip().split("\n"):
        if line:
            assert "execute_query" not in line, line
