"""A preview of the run that would actually happen.

Until 2026-10-06 every donation preview was a sample: the limits were
`500 if (dry_run or quick)`, so `dry_run` alone forced sampling and there was
no way to see the real figures. The two counts that decide whether this sync
is safe to enable — how many HubSpot contacts are claimed by more than one
CSuite profile, and how many stored csuite_profile_ids are stale — cannot be
read off 500 rows out of ~18,800. A preview you have to extrapolate from is
not a preview.

Sampling is now `quick`'s job alone, and "sync donations dry run full" asks
for the whole thing. It is still ungated: it writes nothing.

No network.
"""

import pytest

from intents import sync_commands
from sync.donations import SAMPLE_SIZE, DonationSync


class Reads(DonationSync):
    """Records the limits it was given; touches nothing."""

    def __init__(self, profiles=None, donations=None):
        self.asked = {}
        self.csuite = None       # never used: a dry run reads no profile
        self.hubspot = None
        self._profiles = profiles or {}
        self._donations = donations or []

    def get_profile_emails(self, limit=None):
        self.asked["profiles"] = limit
        return dict(self._profiles)

    def get_donations_with_limit(self, limit=None):
        self.asked["donations"] = limit
        return list(self._donations)


def arm(monkeypatch, enabled=False):
    monkeypatch.setattr("config.Config.CSUITE_DONATION_SYNC_ENABLED", enabled)


# ---------------------------------------------------------------------------
# Sampling belongs to `quick`
# ---------------------------------------------------------------------------

def test_a_quick_run_samples(monkeypatch):
    arm(monkeypatch)
    sync = Reads()
    result = sync.sync(dry_run=True, quick=True)

    assert sync.asked == {"profiles": SAMPLE_SIZE}
    assert result["sampled"] is True


def test_a_full_dry_run_asks_for_no_limit(monkeypatch):
    arm(monkeypatch)
    sync = Reads()
    result = sync.sync(dry_run=True, quick=False)

    assert sync.asked["profiles"] is None
    assert result["sampled"] is False


def test_a_live_run_is_unsampled_as_before(monkeypatch):
    arm(monkeypatch, True)
    sync = Reads()
    sync.sync(dry_run=False, quick=False)

    assert sync.asked["profiles"] is None


def test_a_full_dry_run_needs_no_flag(monkeypatch):
    """It writes nothing, and it is what the decision to enable rests on."""
    arm(monkeypatch, False)

    result = Reads().sync(dry_run=True, quick=False)

    assert result["sampled"] is False
    assert result["updated"] == 0


def test_the_counts_say_what_was_actually_read(monkeypatch):
    arm(monkeypatch)
    sync = Reads(profiles={1: "a@x.invalid", 2: "b@x.invalid"},
                 donations=[{"d": 1}, {"d": 2}, {"d": 3}])
    sync.aggregate_donations = lambda donations: {}
    result = sync.sync(dry_run=True, quick=False)

    assert result["profiles_read"] == 2
    assert result["donations_read"] == 3


# ---------------------------------------------------------------------------
# The chat phrase
# ---------------------------------------------------------------------------

@pytest.fixture
def chat(monkeypatch):
    asked = {}

    def record(dry_run=False, quick=False, resolve_shown=0):
        asked.update(dry_run=dry_run, quick=quick, resolve_shown=resolve_shown)
        return {"updated": 0, "skipped_no_email": 0, "skipped_not_found": 0,
                "errors": 0, "details": [], "sampled": quick,
                "profiles_read": 18797, "donations_read": 41233}

    monkeypatch.setattr(sync_commands, "run_donation_sync", record)
    return asked


@pytest.mark.parametrize("phrase", [
    "sync donations dry run",
    "sync donations test",
])
def test_a_plain_preview_is_still_sampled(chat, phrase):
    sync_commands.handle(phrase, None)

    assert chat["dry_run"] is True
    assert chat["quick"] is True


@pytest.mark.parametrize("phrase", [
    "sync donations dry run full",
    "sync donations full dry run",
])
def test_full_unwires_the_sampling(chat, phrase):
    sync_commands.handle(phrase, None)

    assert chat["dry_run"] is True
    assert chat["quick"] is False


def test_full_without_dry_run_is_not_a_full_live_run(chat):
    """"full" must not be a way to ask for an unflagged live run — the flag
    decides that, and this phrase only widens a preview."""
    sync_commands.handle("sync donations full", None)

    assert chat["dry_run"] is False
    assert chat["quick"] is False, "a live run was never sampled"


# ---------------------------------------------------------------------------
# The footers
# ---------------------------------------------------------------------------

def results_with(**overrides):
    base = {"updated": 3, "skipped_no_email": 0, "skipped_not_found": 0,
            "errors": 0, "details": [], "sampled": True,
            "profiles_read": 500, "donations_read": 500}
    base.update(overrides)
    return base


def test_a_sampled_preview_says_the_counts_are_not_totals():
    reply = sync_commands._format_donation_sync_results(
        results_with(sampled=True), dry_run=True)

    assert "not the whole database" in reply
    assert "these counts are not totals" in reply


def test_a_sampled_preview_states_the_cost_of_the_full_one():
    """So the person knows what they are asking for before they ask."""
    reply = sync_commands._format_donation_sync_results(
        results_with(sampled=True), dry_run=True)

    assert 'sync donations dry run full' in reply
    assert "18,800" in reply
    assert "takes minutes" in reply
    assert "makes no writes" in reply


def test_a_full_preview_reports_what_it_read_and_claims_totals():
    reply = sync_commands._format_donation_sync_results(
        results_with(sampled=False, profiles_read=18797,
                     donations_read=41233), dry_run=True)

    assert "18,797 profiles and 41,233 donations read" in reply
    assert "these counts are totals" in reply
    assert "not the whole database" not in reply


def test_a_full_preview_does_not_advertise_itself():
    reply = sync_commands._format_donation_sync_results(
        results_with(sampled=False), dry_run=True)

    assert "dry run full" not in reply


def test_both_previews_still_name_the_flag():
    for sampled in (True, False):
        reply = sync_commands._format_donation_sync_results(
            results_with(sampled=sampled), dry_run=True)
        assert "CSUITE_DONATION_SYNC_ENABLED=true" in reply


def test_the_sample_size_is_not_duplicated_in_the_report():
    """The footer's number and the sync's limit have to be the same number."""
    assert sync_commands.DONATION_SAMPLE_SIZE == SAMPLE_SIZE
