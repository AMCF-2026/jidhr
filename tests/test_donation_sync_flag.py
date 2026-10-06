"""A writing donation sync is off by default, and "sync all" cannot slip past.

The donation sync writes five HubSpot properties per matched contact, one of
which is `csuite_profile_id` — the field the DAF duplicate guard reads to
decide whether a donor already has a CSuite profile. It repoints that field on
every matched contact on every run, from an email match, without reading what
was there. The DAF path was changed on 2026-10-02 so it would never overwrite
a stored id; this is the path that still does, and 68 production contacts
carry ids that do not resolve in CSuite.

It is also uncapped: CSUITE_WRITE_BUDGET governs CSuite writes inside
CSuiteClient._request and has nothing to say about a HubSpot PATCH.

Where the gate lives matters more than that it exists. "sync all" calls
run_donation_sync(dry_run=False) directly, so a check in the chat handler
alone would be bypassed by typing three words. It sits in DonationSync.sync,
which every writing path goes through.

No network.
"""

import pytest

from intents import sync_commands
from sync.donations import (DonationSync, DonationSyncDisabled,
                            donation_sync_allowed, run_donation_sync)


class Exploding:
    """Any CSuite or HubSpot call at all is a failure of the gate."""

    def __getattr__(self, name):
        def fail(*args, **kwargs):
            raise AssertionError(
                f"nothing may be called while the sync is off: {name}")
        return fail


@pytest.fixture
def sync(monkeypatch):
    """A DonationSync whose clients would explode if touched."""
    instance = DonationSync.__new__(DonationSync)
    instance.csuite = Exploding()
    instance.hubspot = Exploding()
    return instance


def arm(monkeypatch, enabled):
    # The DOTTED path, not the imported Config class. The parametrized test
    # below reloads config, which rebinds config.Config to a NEW class object
    # and leaves every `from config import Config` in the process pointing at
    # the old one — the stale-class hazard that broke 17 tests on 2026-10-02.
    # A dotted path is resolved at call time, so it always hits the live class.
    monkeypatch.setattr("config.Config.CSUITE_DONATION_SYNC_ENABLED", enabled)


# ---------------------------------------------------------------------------
# The default
# ---------------------------------------------------------------------------

def test_the_flag_is_off_by_default():
    import config

    assert config.Config.CSUITE_DONATION_SYNC_ENABLED is False


@pytest.mark.parametrize("raw,expected", [
    ("true", True),
    ("TRUE", True),
    ("  true  ", True),
    ("false", False),
    ("1", False),            # only the exact word enables it
    ("yes", False),
    ("", False),
    ("truthy", False),
])
def test_only_the_exact_word_true_enables_it(monkeypatch, raw, expected):
    """Fail closed: a typo is off, not on."""
    monkeypatch.setenv("CSUITE_DONATION_SYNC_ENABLED", raw)
    import importlib

    import config as config_module
    importlib.reload(config_module)
    try:
        assert config_module.Config.CSUITE_DONATION_SYNC_ENABLED is expected
    finally:
        monkeypatch.delenv("CSUITE_DONATION_SYNC_ENABLED", raising=False)
        importlib.reload(config_module)


def test_an_unset_variable_is_off(monkeypatch):
    monkeypatch.delenv("CSUITE_DONATION_SYNC_ENABLED", raising=False)
    import importlib

    import config as config_module
    importlib.reload(config_module)
    assert config_module.Config.CSUITE_DONATION_SYNC_ENABLED is False


def test_the_flag_is_read_at_call_time_not_at_import(monkeypatch):
    """A flag captured at import answers for the environment as it was."""
    arm(monkeypatch, False)
    assert donation_sync_allowed() is False
    arm(monkeypatch, True)
    assert donation_sync_allowed() is True


# ---------------------------------------------------------------------------
# A live run is refused
# ---------------------------------------------------------------------------

def test_a_live_run_is_refused_while_the_flag_is_off(monkeypatch, sync):
    arm(monkeypatch, False)

    with pytest.raises(DonationSyncDisabled) as caught:
        sync.sync()

    assert "CSUITE_DONATION_SYNC_ENABLED" in str(caught.value)
    assert "dry run" in str(caught.value)


def test_the_refusal_reads_nothing_before_refusing(monkeypatch, sync):
    """Not just no writes — no CSuite paging either. The Exploding doubles
    fail the test if anything is called at all."""
    arm(monkeypatch, False)

    with pytest.raises(DonationSyncDisabled):
        sync.sync(dry_run=False)


def test_a_quick_live_run_is_still_a_live_run(monkeypatch, sync):
    """quick= only samples. It still PATCHes."""
    arm(monkeypatch, False)

    with pytest.raises(DonationSyncDisabled):
        sync.sync(dry_run=False, quick=True)


def test_the_refusal_raises_rather_than_returning_zero_updates(
        monkeypatch, sync):
    """"0 contacts updated" is also what a successful run over an empty CSuite
    looks like. A refusal that reads as a clean run is the whole failure mode
    this repo has been removing."""
    arm(monkeypatch, False)

    try:
        result = sync.sync()
    except DonationSyncDisabled:
        return
    pytest.fail(f"the refusal came back as a value: {result!r}")


def test_run_donation_sync_refuses_too(monkeypatch):
    """The module-level entry point, which is what callers actually import."""
    arm(monkeypatch, False)
    monkeypatch.setattr("sync.donations.CSuiteClient", lambda: Exploding())
    monkeypatch.setattr("sync.donations.HubSpotClient", lambda: Exploding())

    with pytest.raises(DonationSyncDisabled):
        run_donation_sync()


# ---------------------------------------------------------------------------
# A dry run is NOT gated
# ---------------------------------------------------------------------------

def test_a_dry_run_is_allowed_while_the_flag_is_off(monkeypatch):
    """It writes nothing to HubSpot, and the decision to enable should rest on
    being able to look first."""
    arm(monkeypatch, False)
    reached = {}

    class Readable(DonationSync):
        def __init__(self):
            pass

        def get_profile_emails(self, limit=None):
            reached["limit"] = limit
            return {}

    result = Readable().sync(dry_run=True)

    assert reached["limit"] == 500, "a dry run is sampled"
    assert result["updated"] == 0


def test_a_dry_run_still_runs_when_the_flag_is_on(monkeypatch):
    arm(monkeypatch, True)

    class Readable(DonationSync):
        def __init__(self):
            pass

        def get_profile_emails(self, limit=None):
            return {}

    assert Readable().sync(dry_run=True)["updated"] == 0


# ---------------------------------------------------------------------------
# The two chat entry points
# ---------------------------------------------------------------------------

def test_plain_sync_donations_says_it_is_off_without_saying_it_failed(
        monkeypatch):
    def refuse(dry_run=False, quick=False, resolve_shown=0):
        raise DonationSyncDisabled("CSUITE_DONATION_SYNC_ENABLED is off")

    monkeypatch.setattr(sync_commands, "run_donation_sync", refuse)
    reply = sync_commands.handle("sync donations", None)

    assert "⏸️" in reply
    assert "turned off" in reply
    assert "❌" not in reply, "nothing failed; nothing was attempted"
    assert "sync donations dry run" in reply, "say what is safe to do instead"
    assert "CSUITE_DONATION_SYNC_ENABLED=true" in reply


def test_sync_all_cannot_walk_past_the_gate(monkeypatch):
    """The hole this was built around: _run_all_syncs calls
    run_donation_sync(dry_run=False) directly and never touches
    _sync_donations."""
    asked = {}

    def refuse(dry_run=False, quick=False, resolve_shown=0):
        asked["dry_run"] = dry_run
        raise DonationSyncDisabled("off")

    monkeypatch.setattr(sync_commands, "run_donation_sync", refuse)
    monkeypatch.setattr(sync_commands, "run_event_sync",
                        lambda dry_run=False: {"created": 0})
    monkeypatch.setattr(sync_commands, "run_newsletter_sync",
                        lambda dry_run=False: {"subscribed": 0})

    reply = sync_commands.handle("sync all", None)

    assert asked["dry_run"] is False, "sync all asks for a live run"
    assert "⏸️ Donations: skipped" in reply
    assert "CSUITE_DONATION_SYNC_ENABLED is off" in reply
    assert "✅ Donations" not in reply, "a skipped sync is not a successful one"


def test_a_real_failure_still_reads_as_a_failure(monkeypatch):
    """The ⏸️ line must not have swallowed the ❌ one."""
    def boom(dry_run=False, quick=False, resolve_shown=0):
        raise RuntimeError("CSuite returned 500")

    monkeypatch.setattr(sync_commands, "run_donation_sync", boom)
    reply = sync_commands.handle("sync donations", None)

    assert "❌ Donation sync failed" in reply
    assert "CSuite returned 500" in reply
    assert "⏸️" not in reply


def test_a_dry_run_through_chat_is_not_refused(monkeypatch):
    asked = {}

    def record(dry_run=False, quick=False, resolve_shown=0):
        asked.update(dry_run=dry_run, quick=quick,
                     resolve_shown=resolve_shown)
        return {"updated": 3, "skipped_no_email": 1,
                "skipped_not_found": 2, "errors": 0, "details": []}

    monkeypatch.setattr(sync_commands, "run_donation_sync", record)
    reply = sync_commands.handle("sync donations dry run", None)

    assert asked == {"dry_run": True, "quick": True,
                     "resolve_shown": 25}
    assert "DRY RUN" in reply
    assert "⏸️" not in reply
