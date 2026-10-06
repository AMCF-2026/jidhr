"""The donation sync does not overwrite a csuite_profile_id it hasn't checked.

It writes five HubSpot properties per matched contact, one of them
`csuite_profile_id` — the field the DAF duplicate guard reads to decide
whether a donor already has a CSuite profile. Until 2026-10-06 it wrote that
field on every matched contact from an email match, without reading what was
there. The DAF path was changed on 2026-10-02 so it would never overwrite a
stored id; this was the path that still did, and 68 production contacts carry
ids that do not resolve in CSuite.

Decisions taken 2026-10-06:

* The four donation fields are written in every case. The money is right even
  when the link is disputed.
* A stale id is COUNTED, never repointed. Overwriting it destroys the only
  record of what it pointed at; a backfill is its own job with its own flag.
* The dry run resolves existence for the rows it prints and no others, and
  says so, because one profile/display per contact would turn a preview into
  the most expensive read in the app.

No network.
"""

import pytest

from intents import sync_commands
from sync.donations import DonationSync, LinkDecision
from sync.profile_state import (PROFILE_EXISTS, PROFILE_MISSING,
                                PROFILE_UNREADABLE, ProfileStateCache,
                                csuite_profile_state)


class CSuite:
    """profile/display over a set of ids that exist. Counts its reads."""

    def __init__(self, live=(), unreadable=()):
        self.live = {str(i) for i in live}
        self.unreadable = {str(i) for i in unreadable}
        self.reads = []

    def _request(self, endpoint, data=None):
        assert endpoint == "profile/display", endpoint
        key = str((data or {}).get("profile_id"))
        self.reads.append(key)
        if key in self.unreadable:
            return {"success": False, "error": "Internal server error",
                    "http_status": 500}
        if key in self.live:
            return {"success": True, "data": {"profile_id": int(key)}}
        return {"success": False, "errors": ["Profile not found"]}


def contact(contact_id="70123", stored=None):
    props = {"email": "d@example.invalid"}
    if stored is not None:
        props["csuite_profile_id"] = stored
    return {"id": contact_id, "properties": props}


def sync_with(csuite):
    instance = DonationSync.__new__(DonationSync)
    instance.csuite = csuite
    instance.hubspot = None
    return instance


def decide(csuite, contact_row, profile_id, resolve=True):
    instance = sync_with(csuite)
    return instance._link_decision(contact_row, profile_id,
                                   ProfileStateCache(csuite), resolve=resolve)


# ---------------------------------------------------------------------------
# The shared module
# ---------------------------------------------------------------------------

def test_the_three_states_are_distinguished():
    csuite = CSuite(live=[1034], unreadable=[9999])

    assert csuite_profile_state(csuite, 1034)[0] == PROFILE_EXISTS
    assert csuite_profile_state(csuite, 8443)[0] == PROFILE_MISSING
    assert csuite_profile_state(csuite, 9999)[0] == PROFILE_UNREADABLE


def test_the_daf_workflow_still_imports_it_from_where_it_always_did():
    """Moved to sync/profile_state.py; every caller and test has imported it
    from intents.daf_workflow since 2026-10-02."""
    from intents import daf_workflow

    assert daf_workflow.csuite_profile_state is csuite_profile_state
    assert daf_workflow.PROFILE_EXISTS == PROFILE_EXISTS
    assert daf_workflow.PROFILE_MISSING == PROFILE_MISSING
    assert daf_workflow.PROFILE_UNREADABLE == PROFILE_UNREADABLE


def test_the_cache_asks_csuite_once_per_id():
    csuite = CSuite(live=[1034])
    cache = ProfileStateCache(csuite)

    for _ in range(5):
        cache.state(1034)
        cache.state(8443)

    assert csuite.reads == ["1034", "8443"], "one read each, then memoised"
    assert cache.reads == 2


# ---------------------------------------------------------------------------
# The decision
# ---------------------------------------------------------------------------

def test_an_empty_link_is_written():
    decision = decide(CSuite(), contact(stored=None), 1034)

    assert decision.write_link is True
    assert decision.counter == "link_written"
    assert decision.action == "write"


@pytest.mark.parametrize("stored", ["", "   "])
def test_a_blank_link_counts_as_empty(stored):
    assert decide(CSuite(), contact(stored=stored), 1034).write_link is True


def test_a_matching_link_is_not_rewritten():
    """Writing it again is a no-op that still costs an audit row."""
    decision = decide(CSuite(live=[1034]), contact(stored="1034"), 1034)

    assert decision.write_link is False
    assert decision.counter == "link_unchanged"
    assert decision.action == "same"


def test_a_conflicting_live_profile_is_left_alone():
    """Two live profiles for one donor is a merge, not a sync decision."""
    csuite = CSuite(live=[8443])
    decision = decide(csuite, contact(stored="8443"), 1034)

    assert decision.write_link is False
    assert decision.counter == "link_conflict"
    assert decision.state == PROFILE_EXISTS


def test_a_stale_link_is_counted_and_never_repointed():
    """The 68. Overwriting one destroys the only record of what it pointed
    at, so it is reported for a human and left exactly as it is."""
    decision = decide(CSuite(live=[1034]), contact(stored="8443"), 1034)

    assert decision.write_link is False
    assert decision.counter == "link_stale"
    assert decision.action == "stale"


def test_an_unverifiable_link_is_left_alone():
    """A 500 says nothing about whether the profile is there."""
    decision = decide(CSuite(unreadable=[8443]), contact(stored="8443"), 1034)

    assert decision.write_link is False
    assert decision.counter == "link_unverifiable"
    assert decision.state == PROFILE_UNREADABLE


def test_a_dry_run_decision_makes_no_csuite_call():
    csuite = CSuite(live=[8443])
    decision = decide(csuite, contact(stored="8443"), 1034, resolve=False)

    assert csuite.reads == [], "the comparison is free; the verdict is not"
    assert decision.write_link is False
    assert decision.action == "differs"
    assert decision.counter == "link_differs", \
        "calling it a conflict would claim the stored id resolves, which is " \
        "the thing the preview has not checked"


def test_a_withheld_link_is_logged_against_the_contact(caplog):
    import logging

    decision = LinkDecision(False, "link_stale", "stale", stored="8443")
    with caplog.at_level(logging.WARNING, logger="sync.donations"):
        decision.log("70123", 1034)

    message = " ".join(r.getMessage() for r in caplog.records)
    assert "70123" in message and "8443" in message and "1034" in message
    assert "NOT written" in message


# ---------------------------------------------------------------------------
# The contact read
# ---------------------------------------------------------------------------

class HubSpot:
    def __init__(self, found=None, raises=None):
        self.found = found
        self.raises = raises
        self.patched = []

    def search_contact_by_email(self, email, properties=None):
        if self.raises:
            raise self.raises
        return self.found

    def update_contact(self, contact_id, properties):
        self.patched.append((contact_id, properties))
        return {"id": contact_id}

    def update_contact_by_email(self, email, properties):
        raise AssertionError(
            "the sync must read the contact first, not search twice")


def read(found=None, raises=None):
    instance = DonationSync.__new__(DonationSync)
    instance.csuite = CSuite()
    instance.hubspot = HubSpot(found=found, raises=raises)
    # _read_contact paces itself against HubSpot's 5/s search cap, so it needs
    # the counters a real __init__ would have set. sync() resets them itself.
    instance.reset_counters()
    return instance._read_contact("d@example.invalid")


def test_a_missing_contact_and_a_failed_lookup_are_different():
    assert read(found={"results": []}) == (None, None)

    row, error = read(found={"error": "HubSpot returned 500"})
    assert row is None and "500" in error

    row, error = read(raises=RuntimeError("connection reset"))
    assert row is None and "connection reset" in error


def test_a_found_contact_comes_back_whole():
    row, error = read(found={"results": [contact(stored="1034")]})

    assert error is None
    assert row["id"] == "70123"
    assert row["properties"]["csuite_profile_id"] == "1034"


# ---------------------------------------------------------------------------
# End to end through sync()
# ---------------------------------------------------------------------------

def run_sync(monkeypatch, csuite, hubspot, aggregates, emails,
             dry_run=False):
    monkeypatch.setattr("config.Config.CSUITE_DONATION_SYNC_ENABLED", True)
    instance = DonationSync.__new__(DonationSync)
    instance.csuite = csuite
    instance.hubspot = hubspot
    monkeypatch.setattr(instance, "get_profile_emails",
                        lambda limit=None: emails)
    monkeypatch.setattr(instance, "get_donations_with_limit",
                        lambda limit=None: [{"x": 1}])
    monkeypatch.setattr(instance, "aggregate_donations",
                        lambda donations: aggregates)
    return instance, instance.sync(dry_run=dry_run)


AGG = {"total": 250.0, "count": 2, "last_amount": 100.0,
       "last_date": "2026-09-01"}


def test_a_live_run_writes_the_money_but_not_a_conflicting_link(monkeypatch):
    """Decision (a): the four donation fields go in either way."""
    hubspot = HubSpot(found={"results": [contact(stored="8443")]})
    _, results = run_sync(monkeypatch, CSuite(live=[8443]), hubspot,
                          {1034: AGG}, {1034: "d@example.invalid"})

    assert results["link_conflict"] == 1
    assert results["updated"] == 1
    assert len(hubspot.patched) == 1
    _contact_id, properties = hubspot.patched[0]
    assert "csuite_profile_id" not in properties
    assert properties["lifetime_giving"] == "250.0"
    assert properties["donation_count"] == "2"
    assert properties["last_donation_amount"] == "100.0"
    assert properties["last_donation_date"]


def test_a_live_run_writes_the_link_when_the_contact_had_none(monkeypatch):
    hubspot = HubSpot(found={"results": [contact(stored=None)]})
    _, results = run_sync(monkeypatch, CSuite(), hubspot,
                          {1034: AGG}, {1034: "d@example.invalid"})

    assert results["link_written"] == 1
    assert hubspot.patched[0][1]["csuite_profile_id"] == "1034"


def test_a_stale_link_is_not_repointed_end_to_end(monkeypatch):
    hubspot = HubSpot(found={"results": [contact(stored="8443")]})
    _, results = run_sync(monkeypatch, CSuite(live=[1034]), hubspot,
                          {1034: AGG}, {1034: "d@example.invalid"})

    assert results["link_stale"] == 1
    assert "csuite_profile_id" not in hubspot.patched[0][1]


def test_a_failed_contact_read_is_an_error_not_a_missing_contact(monkeypatch):
    hubspot = HubSpot(raises=RuntimeError("connection reset"))
    _, results = run_sync(monkeypatch, CSuite(), hubspot,
                          {1034: AGG}, {1034: "d@example.invalid"})

    assert results["errors"] == 1
    assert results["skipped_not_found"] == 0
    assert hubspot.patched == [], "nothing is written on an unreadable contact"


def test_a_contact_that_does_not_exist_is_skipped_not_an_error(monkeypatch):
    hubspot = HubSpot(found={"results": []})
    _, results = run_sync(monkeypatch, CSuite(), hubspot,
                          {1034: AGG}, {1034: "d@example.invalid"})

    assert results["skipped_not_found"] == 1
    assert results["errors"] == 0


# ---------------------------------------------------------------------------
# The dry-run table
# ---------------------------------------------------------------------------

def test_a_dry_run_writes_nothing_and_collects_a_row(monkeypatch):
    hubspot = HubSpot(found={"results": [contact(stored="8443")]})
    _, results = run_sync(monkeypatch, CSuite(live=[8443]), hubspot,
                          {1034: AGG}, {1034: "d@example.invalid"},
                          dry_run=True)

    assert hubspot.patched == []
    assert results["link_differs"] == 1
    assert results["link_conflict"] == 0, "unverified is not a conflict"
    assert results["link_rows"] == [{"contact_id": "70123",
                                     "current": "8443",
                                     "proposed": "1034",
                                     "action": "differs"}]


def test_a_dry_run_makes_no_profile_reads_while_collecting(monkeypatch):
    csuite = CSuite(live=[8443])
    hubspot = HubSpot(found={"results": [contact(stored="8443")]})
    run_sync(monkeypatch, csuite, hubspot, {1034: AGG},
             {1034: "d@example.invalid"}, dry_run=True)

    assert csuite.reads == []


def test_existence_is_resolved_for_the_shown_rows_only():
    csuite = CSuite(live=[1034])
    instance = sync_with(csuite)
    results = {"link_rows": [
        {"contact_id": str(70000 + i), "current": "", "proposed": "1034",
         "action": "write"} for i in range(30)]}

    shown = instance.resolve_shown_links(results, limit=25)

    assert len(shown) == 25
    assert all(row["proposed_exists"] == "yes" for row in shown)
    assert "proposed_exists" not in results["link_rows"][25]
    assert csuite.reads == ["1034"], "one distinct id, read once"
    assert results["profile_reads"] == 1


def test_resolution_names_a_proposed_id_that_is_not_in_csuite():
    instance = sync_with(CSuite(live=[], unreadable=[9999]))
    results = {"link_rows": [
        {"contact_id": "70123", "current": "8443", "proposed": "1034",
         "action": "differs"},
        {"contact_id": "70456", "current": "", "proposed": "9999",
         "action": "write"},
    ]}

    shown = instance.resolve_shown_links(results, limit=25)

    assert shown[0]["proposed_exists"] == "NO — not in CSuite"
    assert shown[0]["current_exists"] == "NO — stale"
    assert "unknown" in shown[1]["proposed_exists"]
    assert shown[1]["current_exists"] == ""


def test_resolving_none_is_the_default_for_a_plain_call():
    """run_donation_sync only resolves when asked, so an importer that has
    not opted in pays nothing."""
    import inspect

    from sync.donations import run_donation_sync

    assert inspect.signature(
        run_donation_sync).parameters["resolve_shown"].default == 0


# ---------------------------------------------------------------------------
# The rendered report
# ---------------------------------------------------------------------------

def results_with(**overrides):
    base = {"updated": 3, "skipped_no_email": 1, "skipped_not_found": 2,
            "errors": 0, "details": [], "link_written": 1,
            "link_unchanged": 1, "link_conflict": 1, "link_differs": 0,
            "link_stale": 0, "link_unverifiable": 0, "link_rows": [],
            "profile_reads": 0}
    base.update(overrides)
    return base


def test_the_report_counts_every_link_outcome():
    reply = sync_commands._format_donation_sync_results(
        results_with(link_stale=4), dry_run=True)

    assert "🔗 **csuite_profile_id**" in reply
    assert "**1** written (contact had none)" in reply
    assert "**1** already correct — not rewritten" in reply
    assert "points at a different live profile" in reply
    assert "**4**" in reply
    assert "stored id is not in CSuite" in reply
    assert "NOT repointed" in reply


def test_the_table_shows_25_rows_and_says_how_many_it_hid():
    rows = [{"contact_id": str(70000 + i), "current": "", "proposed": "1034",
             "action": "write", "proposed_exists": "yes",
             "current_exists": ""} for i in range(30)]
    reply = sync_commands._format_donation_sync_results(
        results_with(link_rows=rows, profile_reads=1), dry_run=True)

    assert "first 25 of 30" in reply
    assert "(existence checked for the rows shown)" in reply
    assert "and **5** more rows not shown" in reply
    assert "The counts above are the full totals." in reply
    assert reply.count("| `70") == 25
    assert "1 CSuite profile read(s)" in reply


def test_an_unresolved_table_does_not_claim_a_verdict():
    rows = [{"contact_id": "70123", "current": "8443", "proposed": "1034",
             "action": "differs"}]
    reply = sync_commands._format_donation_sync_results(
        results_with(link_rows=rows), dry_run=True)

    assert "not checked" in reply
    assert "existence checked for the rows shown" not in reply


def test_a_live_run_prints_no_table():
    """The table is a preview. A live run already happened."""
    rows = [{"contact_id": "70123", "current": "", "proposed": "1034",
             "action": "write"}]
    reply = sync_commands._format_donation_sync_results(
        results_with(link_rows=rows), dry_run=False)

    assert "| contact |" not in reply
    assert "🔗 **csuite_profile_id**" in reply, "the counts still appear"


def test_an_older_result_dict_renders_without_the_link_section():
    """Nothing crashes if a caller hands over a result from before this."""
    reply = sync_commands._format_donation_sync_results(
        {"updated": 1, "skipped_no_email": 0, "skipped_not_found": 0,
         "errors": 0, "details": []}, dry_run=False)

    assert "Donation Sync Complete" in reply
    assert "🔗" not in reply


def test_an_unchecked_difference_is_not_called_a_conflict():
    """The dry-run label has to match what was actually established."""
    reply = sync_commands._format_donation_sync_results(
        results_with(link_conflict=0, link_differs=2), dry_run=True)

    assert "this preview did not check" in reply
    assert "points at a different live profile" not in reply
