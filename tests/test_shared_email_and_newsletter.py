"""Two things that wrote without being asked, and one contact with two donors.

* **The newsletter sync** had no flag, no cap and no dry-run default. It POSTs
  a subscription change per opted-in CSuite profile — a change to a real
  person's communication preferences. Found on 2026-10-06 when a test that
  called "sync all" unpatched spent 70 seconds paging CSuite on its way to
  doing it for real. It wrote nothing only because CSuite returned no opt-ins.

* **One HubSpot contact claimed by two CSuite profiles.** Matching is
  profile.primary_email -> contact.email and CSuite permits duplicates, so
  both resolved to one contact and the second PATCH overwrote the first
  donor's figures. Measured with contact 542578284242, profiles 21333
  ($5,000, 4 gifts) and 21325 ($250, 1 gift): the contact finished showing
  $250 and a count of 1, because 21325 was processed second. Which one won
  depended on the order donations came back from CSuite.

No network.
"""

import pytest

from intents import sync_commands
from sync.donations import DonationSync
from sync.newsletter import (NewsletterSync, NewsletterSyncDisabled,
                             newsletter_sync_allowed)

EMAIL = "shared@example.invalid"
CONTACT = "542578284242"


# ---------------------------------------------------------------------------
# (a) the newsletter flag
# ---------------------------------------------------------------------------

class Exploding:
    def __getattr__(self, name):
        def fail(*args, **kwargs):
            raise AssertionError(
                f"nothing may be called while the sync is off: {name}")
        return fail


def arm_newsletter(monkeypatch, enabled):
    monkeypatch.setattr("config.Config.CSUITE_NEWSLETTER_SYNC_ENABLED",
                        enabled)


@pytest.fixture
def newsletter():
    instance = NewsletterSync.__new__(NewsletterSync)
    instance.csuite = Exploding()
    instance.hubspot = Exploding()
    instance.subscription_id = "123"
    return instance


def test_the_newsletter_flag_is_off_by_default():
    import config

    assert config.Config.CSUITE_NEWSLETTER_SYNC_ENABLED is False


@pytest.mark.parametrize("raw,expected", [
    ("true", True), ("TRUE", True), (" true ", True),
    ("false", False), ("1", False), ("yes", False), ("", False),
])
def test_only_the_exact_word_true_enables_the_newsletter(monkeypatch, raw,
                                                         expected):
    monkeypatch.setenv("CSUITE_NEWSLETTER_SYNC_ENABLED", raw)
    import importlib

    import config
    importlib.reload(config)
    try:
        assert config.Config.CSUITE_NEWSLETTER_SYNC_ENABLED is expected
    finally:
        monkeypatch.delenv("CSUITE_NEWSLETTER_SYNC_ENABLED", raising=False)
        importlib.reload(config)


def test_the_newsletter_flag_is_read_at_call_time(monkeypatch):
    arm_newsletter(monkeypatch, False)
    assert newsletter_sync_allowed() is False
    arm_newsletter(monkeypatch, True)
    assert newsletter_sync_allowed() is True


def test_a_live_newsletter_run_is_refused(monkeypatch, newsletter):
    arm_newsletter(monkeypatch, False)

    with pytest.raises(NewsletterSyncDisabled) as caught:
        newsletter.sync()

    assert "CSUITE_NEWSLETTER_SYNC_ENABLED" in str(caught.value)


def test_the_newsletter_refusal_reads_nothing_first(monkeypatch, newsletter):
    """Not just no writes — no CSuite paging either. That paging is what took
    70 seconds and what would have led to the writes."""
    arm_newsletter(monkeypatch, False)

    with pytest.raises(NewsletterSyncDisabled):
        newsletter.sync(dry_run=False, quick=True)


def test_a_newsletter_dry_run_is_not_gated(monkeypatch):
    arm_newsletter(monkeypatch, False)
    reached = {}

    class Readable(NewsletterSync):
        def __init__(self):
            self.subscription_id = "123"

        def get_opted_in_profiles(self, limit=None):
            reached["limit"] = limit
            return []

    result = Readable().sync(dry_run=True)

    assert reached["limit"] == 500
    assert result["subscribed"] == 0


def test_sync_all_cannot_walk_past_the_newsletter_gate(monkeypatch):
    """The path that bypasses a gate placed in the chat handler."""
    asked = {}

    def refuse(dry_run=False, quick=False):
        asked["dry_run"] = dry_run
        raise NewsletterSyncDisabled("off")

    monkeypatch.setattr(sync_commands, "run_newsletter_sync", refuse)
    monkeypatch.setattr(sync_commands, "run_donation_sync",
                        lambda **kw: {"updated": 0})
    monkeypatch.setattr(sync_commands.event_apply, "run",
                        lambda **kw: {"created": 0, "updated": 0,
                                      "unchanged": 0, "review": 0})

    reply = sync_commands.handle("sync all", None)

    assert asked["dry_run"] is False
    assert "⏸️ Newsletter: skipped" in reply
    assert "✅ Newsletter" not in reply


def test_plain_sync_newsletter_says_it_is_off(monkeypatch):
    def refuse(dry_run=False, quick=False):
        raise NewsletterSyncDisabled("off")

    monkeypatch.setattr(sync_commands, "run_newsletter_sync", refuse)
    reply = sync_commands.handle("sync newsletter", None)

    assert "⏸️" in reply and "turned off" in reply
    assert "❌" not in reply
    assert "communication preferences" in reply, "say what it would change"
    assert "CSUITE_NEWSLETTER_SYNC_ENABLED=true" in reply


def test_a_real_newsletter_failure_still_reads_as_a_failure(monkeypatch):
    monkeypatch.setattr(
        sync_commands, "run_newsletter_sync",
        lambda **kw: (_ for _ in ()).throw(RuntimeError("CSuite 500")))
    reply = sync_commands.handle("sync newsletter", None)

    assert "❌ Newsletter sync failed" in reply
    assert "⏸️" not in reply


# ---------------------------------------------------------------------------
# (b) one contact, two CSuite profiles
# ---------------------------------------------------------------------------

class CSuite:
    def __init__(self):
        self.reads = []

    def _request(self, endpoint, data=None):
        self.reads.append(str((data or {}).get("profile_id")))
        return {"success": True,
                "data": {"profile_id": int((data or {})["profile_id"])}}


class HubSpot:
    def __init__(self, stored=None, contact_id=CONTACT, found=True):
        self.props = {"email": EMAIL}
        if stored:
            self.props["csuite_profile_id"] = stored
        self.contact_id = contact_id
        self.found = found
        self.patched = []
        self.searches = 0

    def search_contact_by_email(self, email, properties=None):
        self.searches += 1
        if not self.found:
            return {"results": []}
        return {"results": [{"id": self.contact_id,
                             "properties": dict(self.props)}]}

    def update_contact(self, contact_id, properties):
        self.patched.append((contact_id, dict(properties)))
        self.props.update(properties)
        return {"id": contact_id}


BIG = {"total": 5000.0, "count": 4, "last_amount": 1000.0,
       "last_date": "2026-03-01"}
SMALL = {"total": 250.0, "count": 1, "last_amount": 250.0,
         "last_date": "2026-09-01"}


def run(monkeypatch, emails, aggregates, hubspot=None, csuite=None,
        dry_run=False):
    monkeypatch.setattr("config.Config.CSUITE_DONATION_SYNC_ENABLED", True)
    instance = DonationSync.__new__(DonationSync)
    instance.csuite = csuite or CSuite()
    instance.hubspot = hubspot or HubSpot()
    instance.get_profile_emails = lambda limit=None: emails
    instance.get_donations_with_limit = lambda limit=None: [{"x": 1}]
    instance.aggregate_donations = lambda donations: aggregates
    return instance, instance.sync(dry_run=dry_run)


def test_a_shared_contact_is_written_nothing_at_all(monkeypatch):
    hubspot = HubSpot()
    _, results = run(monkeypatch, {21333: EMAIL, 21325: EMAIL},
                     {21333: BIG, 21325: SMALL}, hubspot=hubspot)

    assert hubspot.patched == [], "the $250 used to overwrite the $5,000"
    assert results["shared_email"] == 1
    assert results["updated"] == 0
    assert results["link_written"] == 0


def test_the_shared_contact_is_counted_once_not_once_per_profile(monkeypatch):
    _, results = run(monkeypatch,
                     {1: EMAIL, 2: EMAIL, 3: EMAIL},
                     {1: BIG, 2: SMALL, 3: SMALL})

    assert results["shared_email"] == 1, "one contact needs a human, not three"
    assert results["shared_email_rows"][0]["profiles"] == ["1", "2", "3"]


def test_the_row_names_the_contact_and_every_profile(monkeypatch):
    _, results = run(monkeypatch, {21333: EMAIL, 21325: EMAIL},
                     {21333: BIG, 21325: SMALL},
                     hubspot=HubSpot(stored="21333"))

    row = results["shared_email_rows"][0]
    assert row["contact_id"] == CONTACT
    assert sorted(row["profiles"]) == ["21325", "21333"]
    assert row["current"] == "21333"


def test_nothing_is_summed(monkeypatch):
    """Two profiles on one address may be one person entered twice or two
    people in a household. Summing would invent a donor."""
    hubspot = HubSpot()
    run(monkeypatch, {21333: EMAIL, 21325: EMAIL},
        {21333: BIG, 21325: SMALL}, hubspot=hubspot)

    assert "lifetime_giving" not in hubspot.props
    assert "5250" not in str(hubspot.props)


def test_the_case_of_the_address_does_not_hide_the_clash(monkeypatch):
    """CSuite stores primary_email as it was typed."""
    hubspot = HubSpot()
    _, results = run(monkeypatch,
                     {21333: "Shared@Example.Invalid", 21325: EMAIL},
                     {21333: BIG, 21325: SMALL}, hubspot=hubspot)

    assert results["shared_email"] == 1
    assert hubspot.patched == []


def test_an_unshared_contact_is_still_written(monkeypatch):
    """The guard must not stop the ordinary case."""
    hubspot = HubSpot()
    _, results = run(monkeypatch, {21333: EMAIL}, {21333: BIG},
                     hubspot=hubspot)

    assert results["shared_email"] == 0
    assert len(hubspot.patched) == 1
    assert hubspot.patched[0][1]["lifetime_giving"] == "5000.0"


def test_two_profiles_with_DIFFERENT_emails_are_both_written(monkeypatch):
    hubspot = HubSpot()
    _, results = run(monkeypatch,
                     {21333: EMAIL, 21325: "other@example.invalid"},
                     {21333: BIG, 21325: SMALL}, hubspot=hubspot)

    assert results["shared_email"] == 0
    assert len(hubspot.patched) == 2


def test_a_profile_with_no_donations_does_not_create_a_clash(monkeypatch):
    """Only profiles this run would WRITE can collide. One with no donations
    is never written, so it cannot pick a winner."""
    hubspot = HubSpot()
    _, results = run(monkeypatch, {21333: EMAIL, 21325: EMAIL},
                     {21333: BIG}, hubspot=hubspot)

    assert results["shared_email"] == 0
    assert len(hubspot.patched) == 1


def test_a_shared_contact_that_does_not_exist_is_just_not_found(monkeypatch):
    _, results = run(monkeypatch, {21333: EMAIL, 21325: EMAIL},
                     {21333: BIG, 21325: SMALL},
                     hubspot=HubSpot(found=False))

    assert results["skipped_not_found"] == 1
    assert results["shared_email"] == 0


def test_a_shared_contact_read_failure_is_an_error(monkeypatch):
    class Failing(HubSpot):
        def search_contact_by_email(self, email, properties=None):
            return {"error": "HubSpot returned 500"}

    _, results = run(monkeypatch, {21333: EMAIL, 21325: EMAIL},
                     {21333: BIG, 21325: SMALL}, hubspot=Failing())

    assert results["errors"] == 1
    assert results["shared_email"] == 0


def test_a_dry_run_reports_the_clash_without_reading_csuite(monkeypatch):
    csuite = CSuite()
    _, results = run(monkeypatch, {21333: EMAIL, 21325: EMAIL},
                     {21333: BIG, 21325: SMALL}, csuite=csuite, dry_run=True)

    assert results["shared_email"] == 1
    assert csuite.reads == [], "no profile/display is needed to spot a clash"


def test_the_clash_is_logged_against_the_contact(monkeypatch, caplog):
    import logging

    with caplog.at_level(logging.WARNING, logger="sync.donations"):
        run(monkeypatch, {21333: EMAIL, 21325: EMAIL},
            {21333: BIG, 21325: SMALL})

    message = " ".join(r.getMessage() for r in caplog.records)
    assert CONTACT in message
    assert "21333" in message and "21325" in message
    assert "NOTHING was written" in message
    assert "not summed" in message


# ---------------------------------------------------------------------------
# The preview lists it, and the footer names the flag
# ---------------------------------------------------------------------------

def results_with(**overrides):
    base = {"updated": 3, "skipped_no_email": 0, "skipped_not_found": 0,
            "errors": 0, "details": [], "link_written": 3,
            "link_unchanged": 0, "link_conflict": 0, "link_differs": 0,
            "link_stale": 0, "link_unverifiable": 0, "shared_email": 0,
            "shared_email_rows": [], "link_rows": [], "profile_reads": 0}
    base.update(overrides)
    return base


def test_the_preview_lists_every_shared_contact():
    reply = sync_commands._format_donation_sync_results(
        results_with(shared_email=1, shared_email_rows=[
            {"contact_id": CONTACT, "profiles": ["21333", "21325"],
             "current": "21333"}]), dry_run=True)

    assert "1 contact(s) claimed by more than one CSuite profile" in reply
    assert "nothing was written to them" in reply
    assert f"`{CONTACT}`" in reply
    assert "21333, 21325" in reply
    assert "stored link: 21333" in reply
    assert "not summed" in reply


def test_a_long_shared_list_is_truncated_but_the_count_is_not():
    rows = [{"contact_id": str(70000 + i), "profiles": ["1", "2"],
             "current": ""} for i in range(25)]
    reply = sync_commands._format_donation_sync_results(
        results_with(shared_email=25, shared_email_rows=rows), dry_run=True)

    assert "25 contact(s) claimed" in reply
    assert "and **15** more" in reply
    assert "the count above is the total" in reply


def test_no_clash_means_no_section():
    reply = sync_commands._format_donation_sync_results(
        results_with(), dry_run=True)

    assert "claimed by more than one" not in reply


def test_the_dry_run_footer_names_the_flag():
    reply = sync_commands._format_donation_sync_results(
        results_with(), dry_run=True)

    assert "CSUITE_DONATION_SYNC_ENABLED=true" in reply
    assert "is refused and nothing is written" in reply
    assert "without 'dry run'" not in reply, \
        "that instruction no longer works — a plain run is refused"
    assert "Sampled: 500 profiles" in reply


def test_a_live_run_has_no_footer_about_flags():
    reply = sync_commands._format_donation_sync_results(
        results_with(), dry_run=False)

    assert "CSUITE_DONATION_SYNC_ENABLED" not in reply
    assert "Sampled" not in reply
