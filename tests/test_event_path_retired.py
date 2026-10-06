"""One event sync, one externalAccountId, and a dry run that says what it did.

Four things were wrong at once, all measured against production 2026-10-06.

* **Two implementations.** sync/events.py (chat) and scripts/event_sync.py
  (CLI) wrote to the same marketing events with different payloads, different
  dedup rules and a different externalAccountId. The chat one had no notion of
  an update at all: once an event existed it was skipped forever, so a changed
  CSuite date never reached HubSpot.
* **Two account ids.** sync/event_hubspot declared "amuslimcf-csuite" while
  clients/hubspot.create_marketing_event quietly injected "jidhr-amcf" from
  inside the client. HubSpot keys an event on
  (externalAccountId, externalEventId) — reading csuite-1464 back under
  jidhr-amcf finds it and under anything else 404s — so an --apply run under
  the declared value would have found none of the 11 existing events and, per
  HubSpot's documented upsert behaviour, created 11 duplicates.
* **A dedup check that always said no.** event_exists called
  GET marketing-events/external/{id}, which 404s for every id on this portal,
  and read the error as "does not exist". So every run re-PUT all 11 events
  and skipped nothing.
* **A dry run that claimed to write nothing** on the line after it wrote a
  hubsync.run_log row.

No network, no database.
"""

import pytest

from intents import sync_commands
from sync import event_apply as ea
from sync import event_hubspot as eh


# ---------------------------------------------------------------------------
# One constant
# ---------------------------------------------------------------------------

def test_the_account_id_is_the_one_the_portal_actually_uses():
    assert eh.EXTERNAL_ACCOUNT_ID == "jidhr-amcf"


def test_the_client_injects_no_account_id_of_its_own():
    """The bug that let the two values diverge: the one that was really sent
    lived inside the client, out of sight of the module that declared it."""
    from clients.hubspot import HubSpotClient

    client = HubSpotClient.__new__(HubSpotClient)
    sent = {}
    client._put = lambda endpoint, data=None: sent.update(
        endpoint=endpoint, data=data) or {"objectId": "hs-1"}

    result = client.create_marketing_event({"eventName": "X",
                                            "externalEventId": "csuite-1",
                                            "externalAccountId": "jidhr-amcf"})

    assert result == {"objectId": "hs-1"}
    assert sent["data"]["externalAccountId"] == "jidhr-amcf", \
        "passed through, not replaced"


def test_the_client_passes_through_whatever_account_id_it_is_given():
    """Proves it does not substitute its own, without grepping the source —
    a comment naming the old value would pass a grep test and mean nothing."""
    from clients.hubspot import HubSpotClient

    client = HubSpotClient.__new__(HubSpotClient)
    sent = {}
    client._put = lambda endpoint, data=None: sent.update(data=data) or {}

    client.create_marketing_event({"eventName": "X",
                                   "externalEventId": "csuite-1",
                                   "externalAccountId": "some-other-value"})

    assert sent["data"]["externalAccountId"] == "some-other-value"


def test_the_client_refuses_a_payload_with_no_account_id():
    """Silently supplying one is what hid the divergence. Refusing names it."""
    from clients.hubspot import HubSpotClient

    client = HubSpotClient.__new__(HubSpotClient)
    client._put = lambda *a, **k: pytest.fail("nothing may be sent")

    result = client.create_marketing_event({"eventName": "X",
                                            "externalEventId": "csuite-1"})

    assert "externalAccountId is required" in result["error"]
    assert "EXTERNAL_ACCOUNT_ID" in result["error"]


def test_the_404ing_lookup_is_gone():
    from clients.hubspot import HubSpotClient

    assert not hasattr(HubSpotClient, "search_marketing_event_by_external_id")


def test_the_retired_module_is_gone():
    with pytest.raises(ImportError):
        import sync.events          # noqa: F401

    import sync

    assert not hasattr(sync, "run_event_sync")
    assert not hasattr(sync, "EventSync")


# ---------------------------------------------------------------------------
# The seed tool
# ---------------------------------------------------------------------------

PORTAL = {
    "csuite-1464": {"externalEventId": "csuite-1464",
                    "objectId": "863913950938", "appInfo": {"name": "Irritable-Needle"}},
    "csuite-1153": {"externalEventId": "csuite-1153",
                    "objectId": "749259629267", "appInfo": {"name": "Irritable-Needle"}},
    "1611602760239": {"externalEventId": "1611602760239",
                      "objectId": "616843853508", "appInfo": {"name": "Eventbrite"}},
    "1587977957819": {"externalEventId": "1587977957819",
                      "objectId": "616932842223", "appInfo": {"name": "Eventbrite"}},
}


@pytest.fixture
def seed(monkeypatch):
    import scripts.event_map_seed as mod

    monkeypatch.setattr(eh, "hubspot_index",
                        lambda hubspot: (dict(PORTAL), 1, None))
    return mod


def test_the_seed_takes_only_csuite_events(seed):
    rows, skipped, error = seed.csuite_events(object())

    assert error is None
    assert [external for external, _ in rows] == ["csuite-1153", "csuite-1464"]
    assert sorted(external for external, _ in skipped) == \
        ["1587977957819", "1611602760239"]


def test_the_seed_never_emits_an_eventbrite_event(seed):
    sql = seed.render(*seed.csuite_events(object())[:2])

    assert "INSERT" in sql
    assert sql.count("INSERT") == 2
    for foreign in ("616843853508", "616932842223"):
        assert f"'{foreign}'" not in sql, "an Eventbrite objectId was emitted"
    assert "NOT seeded, deliberately" in sql
    assert "Eventbrite" in sql, "say what was left out and why"


def test_the_seeded_row_carries_the_hubspot_id_and_the_external_id(seed):
    sql = seed.render(*seed.csuite_events(object())[:2])

    assert "'1464', '863913950938', 'csuite-1464'" in sql
    assert "ON CONFLICT (csuite_eventdate_id) DO NOTHING" in sql


def test_the_seeded_hash_is_not_a_hash(seed):
    """It must be unable to equal a computed content_hash, and must not look
    like a measurement."""
    import hashlib
    import re

    assert not re.fullmatch(r"[0-9a-f]{64}", seed.SEEDED_HASH)
    assert len(seed.SEEDED_HASH) != 64
    assert seed.SEEDED_HASH != hashlib.sha256(b"").hexdigest()
    assert "seeded" in seed.SEEDED_HASH


def test_the_seeded_status_is_one_the_schema_allows(seed):
    assert seed.SEEDED_STATUS in {"synced", "pending", "unknown", "review",
                                  "error"}
    assert seed.SEEDED_STATUS != "unknown", \
        "'unknown' triggers the resolve path and mislabels the reason"
    assert seed.SEEDED_STATUS != "synced", \
        "nothing has been synced by this job yet"


def test_a_quoted_value_cannot_break_out_of_the_literal(seed):
    assert seed.quote("O'Brien") == "'O''Brien'"
    assert seed.quote(None) == "NULL"


# ---------------------------------------------------------------------------
# The seeded map makes the first run an UPDATE
# ---------------------------------------------------------------------------

def event_row(event_date_id, description="AMCF Open House"):
    return {"event_date_id": event_date_id, "event_description": description,
            "event_date": "2026-11-03", "start_time": "6:00 PM ET",
            "location": "Reston", "event_id": 900, "archived": 0}


def seeded_map(seed, ids):
    return {str(i): {"csuite_eventdate_id": str(i),
                     "hubspot_event_id": f"hs-{i}",
                     "external_event_id": f"csuite-{i}",
                     "content_hash": seed.SEEDED_HASH,
                     "status": seed.SEEDED_STATUS} for i in ids}


def portal_index(ids):
    return {f"csuite-{i}": {"externalEventId": f"csuite-{i}",
                            "objectId": f"hs-{i}"} for i in ids}


def test_a_seeded_map_plans_zero_creates(seed):
    """The whole point of the sentinel hash."""
    ids = [1153, 1155, 1157, 1159, 1168, 1429, 1430, 1462, 1463, 1464, 1466]
    rows = [event_row(i) for i in ids]

    result = ea.plan(rows, seeded_map(seed, ids), portal_index(ids), "AMCF")

    assert len(result["creates"]) == 0, "creating one would duplicate it"
    assert len(result["updates"]) == len(ids)
    assert len(result["unchanged"]) == 0, \
        "unchanged would silently keep the wrong start times"


def test_a_real_matching_hash_would_have_read_as_unchanged(seed):
    """Shows what the sentinel is avoiding: seed the true hash and the run
    decides there is nothing to do."""
    row = event_row(1464)
    truthful = dict(seeded_map(seed, [1464]))
    truthful["1464"]["content_hash"] = eh.content_hash(row)

    result = ea.plan([row], truthful, portal_index([1464]), "AMCF")

    assert len(result["unchanged"]) == 1
    assert len(result["updates"]) == 0


def test_an_empty_map_also_plans_an_update_not_a_create(seed):
    """plan() already adopts an event that is in HubSpot but not in the map,
    so the seed is bookkeeping rather than a rescue."""
    result = ea.plan([event_row(1464)], {}, portal_index([1464]), "AMCF")

    assert len(result["creates"]) == 0
    assert result["updates"][0][2] == ("already in HubSpot, not in "
                                       "event_map — adopting it")


def test_a_seeded_row_whose_event_left_the_portal_needs_a_human(seed):
    """What the seed buys: a disappearance reads as review rather than as a
    brand-new event."""
    result = ea.plan([event_row(1464)], seeded_map(seed, [1464]), {}, "AMCF")

    assert len(result["creates"]) == 0
    assert any("needs a person" in reason for _m, reason in result["review"])


# ---------------------------------------------------------------------------
# The chat command: dry run by default, brakes on a live one
# ---------------------------------------------------------------------------

@pytest.fixture
def chat(monkeypatch):
    calls = []

    def fake_run(hubspot=None, dry_run=True, limit=None, organizer=None,
                 pace_ms=None, **options):
        calls.append({"dry_run": dry_run, "limit": limit, **options})
        return {"dry_run": dry_run, "limit": limit, "withheld": 0,
                "withheld_rows": [], "created": 0,
                "updated": 0, "unchanged": 0, "deferred": 0, "unknown": 0,
                "failed": 0, "skipped": 0, "review": 0, "review_rows": [],
                "csuite_calls": 2, "hubspot_calls": 1,
                "event_dates_read": 179, "run_logged": True,
                "migration_applied": True, "error": None, "stopped": None}

    monkeypatch.setattr(ea, "run", fake_run)
    return calls


@pytest.mark.parametrize("phrase", ["sync events", "update events",
                                    "sync events dry run"])
def test_a_plain_request_previews(chat, phrase):
    reply = sync_commands.handle(phrase, None)

    assert chat == [{"dry_run": True, "limit": None,
                     "updates_only": False, "include_ids": []}]
    assert "DRY RUN" in reply
    assert "Nothing was written to HubSpot" in reply


def test_a_live_run_needs_the_word_apply(chat):
    reply = sync_commands.handle("sync events apply", None)

    assert chat == [{"dry_run": False, "limit": ea.CHAT_DEFAULT_LIMIT,
                     "updates_only": False, "include_ids": []}]
    assert "APPLIED" in reply


def test_the_preview_says_how_to_make_it_write(chat):
    reply = sync_commands.handle("sync events", None)

    assert 'Say *"sync events apply"*' in reply
    assert f"capped at {ea.CHAT_DEFAULT_LIMIT} records" in reply


@pytest.mark.parametrize("phrase,expected", [
    ("sync events apply", 5),
    ("sync events apply limit 20", 20),
    ("sync events apply limit 1", 1),
    ("sync events apply limit 0", 0),
    ("sync events apply no limit", None),
    ("sync events apply unlimited", None),
])
def test_the_limit_can_be_set_in_the_message(chat, phrase, expected):
    sync_commands.handle(phrase, None)

    assert chat[0]["limit"] == expected


def test_a_preview_is_never_capped(chat):
    """A cap on a preview would hide rows from the person deciding."""
    sync_commands.handle("sync events limit 2", None)

    assert chat[0]["dry_run"] is True
    assert chat[0]["limit"] is None


def test_sync_all_caps_the_event_run(chat, monkeypatch):
    # The other two syncs are stubbed, not merely flagged off. Calling
    # _run_all_syncs unpatched runs the real NEWSLETTER sync live — it pages
    # CSuite for every opt-in and then POSTs subscribe_contact per contact,
    # with no flag and no cap. Found by this test taking 70 seconds.
    monkeypatch.setattr(sync_commands, "run_donation_sync",
                        lambda **kw: {"updated": 0})
    monkeypatch.setattr(sync_commands, "run_newsletter_sync",
                        lambda **kw: {"subscribed": 0})

    sync_commands.handle("sync all", None)

    assert any(c["dry_run"] is False
               and c["limit"] == ea.CHAT_DEFAULT_LIMIT for c in chat)


# ---------------------------------------------------------------------------
# The formatter: six outcomes, six lines
# ---------------------------------------------------------------------------

def result(**overrides):
    base = {"dry_run": False, "created": 1, "updated": 2, "unchanged": 3,
            "deferred": 4, "unknown": 5, "failed": 6, "skipped": 7,
            "review": 8, "review_rows": [], "csuite_calls": 2,
            "hubspot_calls": 1, "event_dates_read": 179, "run_logged": True,
            "migration_applied": True, "error": None, "stopped": None,
            "limit": None}
    base.update(overrides)
    return base


def test_every_outcome_gets_its_own_line():
    reply = sync_commands._format_event_sync_results(result())

    for count, word in [("1", "created"), ("2", "updated"),
                        ("3", "unchanged"), ("4", "deferred"),
                        ("5", "unknown"), ("6", "failed")]:
        assert f"**{count}** {word}" in reply, word
    assert "**8** need a human" in reply
    assert "**7** not syncable" in reply


def test_review_reads_as_needing_a_person():
    reply = sync_commands._format_event_sync_results(
        result(review=1, review_rows=[("1429", "AMCF Open House",
                                       "no timezone — assumed ET")]))

    assert "need a human" in reply
    assert "also counted above" in reply, \
        "review overlaps the buckets above and must say so"
    assert "All of these are withheld." in reply
    assert "AMCF Open House" in reply
    assert "no timezone — assumed ET" in reply


def test_unknown_says_it_is_never_retried():
    reply = sync_commands._format_event_sync_results(result())

    assert "may or may not have landed" in reply
    assert "Never retried" in reply


def test_deferred_is_not_reported_as_failed():
    reply = sync_commands._format_event_sync_results(
        result(deferred=3, failed=0))

    assert "**3** deferred" in reply
    assert "`limit` was reached" in reply
    assert "**0** failed" in reply


def test_a_dry_run_omits_the_outcomes_only_a_write_can_have():
    reply = sync_commands._format_event_sync_results(result(dry_run=True))

    assert "would be created" in reply
    assert "deferred" not in reply
    assert "unknown" not in reply


def test_a_long_review_list_is_truncated_but_the_count_is_not():
    rows = [(str(i), f"Event {i}", "no time at all") for i in range(40)]
    reply = sync_commands._format_event_sync_results(
        result(review=40, review_rows=rows))

    assert "**40** need a human" in reply
    assert "and **30** more" in reply
    assert "the count above is the total" in reply


def test_a_read_failure_is_the_whole_reply():
    reply = sync_commands._format_event_sync_results(
        result(error="HubSpot marketing events could not be listed (500)"))

    assert reply.startswith("❌ **Event sync stopped.**")
    assert "could not be listed" in reply
    assert "created" not in reply, "no counts over a run that did not happen"


def test_a_stop_is_reported_with_what_to_do():
    reply = sync_commands._format_event_sync_results(
        result(stopped="create for csuite-1 returned no id"))

    assert "🛑 **Stopped:**" in reply
    assert "Nothing further was created" in reply


def test_a_missing_migration_is_said_out_loud():
    reply = sync_commands._format_event_sync_results(
        result(migration_applied=False))

    assert "no duplicate guard" in reply
    assert "001_hubsync_event_map.sql" in reply


# ---------------------------------------------------------------------------
# The dry-run message tells the truth about hubsync
# ---------------------------------------------------------------------------

def test_a_dry_run_admits_the_run_log_row():
    reply = sync_commands._format_event_sync_results(
        result(dry_run=True, run_logged=True))

    assert "Nothing was written to HubSpot." in reply
    assert "One `hubsync.run_log` row was written" in reply


def test_a_dry_run_with_no_hubsync_says_nothing_was_written_anywhere():
    reply = sync_commands._format_event_sync_results(
        result(dry_run=True, run_logged=False, migration_applied=False))

    assert "Nothing was written anywhere" in reply


def test_the_cli_no_longer_claims_hubsync_was_untouched():
    source = open("scripts/event_sync.py").read()

    assert "nothing was written to HubSpot or to hubsync" not in source
    assert "hubsync.run_log row was written" in source


# ---------------------------------------------------------------------------
# run() refuses rather than guessing
# ---------------------------------------------------------------------------

def test_a_live_run_without_the_migration_refuses():
    out = ea.run.__wrapped__ if hasattr(ea.run, "__wrapped__") else ea.run
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ea, "migration_applied", lambda: False)
        result = out(hubspot=object(), dry_run=False)

    assert "no duplicate guard" in result["error"]
    assert "001_hubsync_event_map.sql" in result["error"]
    assert result["created"] == 0


def test_a_failed_hubspot_listing_refuses_to_plan():
    """Without the index every event looks absent, and every absent event
    looks like a create."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ea, "migration_applied", lambda: True)
        patch.setattr(ea, "load_map", lambda: {})
        patch.setattr(ea.eh, "fetch_event_dates",
                      lambda client, pace_ms=None: eh.Fetched(
                          rows=[event_row(1464)], calls=1, total_429s=0,
                          error=None, complete=True))
        patch.setattr(ea.eh, "hubspot_index",
                      lambda hubspot: ({}, 1, "HubSpot returned 500"))
        patch.setattr("clients.csuite.CSuiteClient", lambda: object())
        result = ea.run(hubspot=object(), dry_run=True)

    assert "could not be listed" in result["error"]
    assert result["plan"] is None
