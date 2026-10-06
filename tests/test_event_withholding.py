"""Anything the report calls "needs a human" is never written.

Measured against production on 2026-10-06, an unrestricted apply would have:

* created **73** HubSpot marketing events for CSuite dates the same report
  called "needs a human" — all archived in CSuite, the earliest dated 2021;
* created **74** events in total for dates already in the past, out of 77;
* PATCHed event csuite-1153 from its real 10:00 start to midnight, because
  CSuite holds no start_time for it and the mapper places those at midnight;
* and moved csuite-1157 from 2026-09-03 to 2026-07-21, because the old sync
  had parsed a date out of the free-text start_time 'September 3rd'.

The report said "79 need a human — nothing was written to these" while apply
would have written most of them. The report was wrong about apply, not the
other way round.

So: review is withheld everywhere, updates go before creates, past creates
are withheld unless their id is named, and the preview predicts all of it
with apply's own rules.

No network.
"""

from datetime import date

import pytest

from intents import sync_commands
from sync import event_apply as ea
from sync import event_hubspot as eh

PINNED_TODAY = date(2026, 10, 6)


def row(event_date_id, event_date="2026-12-01", start_time="3 pm ET",
        archived=0, description="An event"):
    return {"event_date_id": event_date_id, "event_id": 900,
            "event_name": "Event - Other", "event_description": description,
            "event_date": event_date, "start_time": start_time,
            "location": "Virtual", "archived": archived,
            "goal_amount": None, "available_seats": 10}


def mapped_of(**kwargs):
    return eh.map_event_date(row(**kwargs), "AMCF")


def hs(object_id, external):
    return {"objectId": object_id, "externalEventId": external,
            "startDateTime": "2026-12-01T10:00:00Z"}


def plan_with(creates=(), updates=()):
    """A plan built by hand, with review populated the way plan() does."""
    result = {"creates": [], "updates": [], "unchanged": [], "skipped": [],
              "review": []}
    for mapped in creates:
        result["creates"].append((mapped, "new"))
    for mapped in updates:
        result["updates"].append(
            (mapped, hs(f"hs-{mapped.csuite_eventdate_id}",
                        mapped.external_event_id), "content hash changed"))
    for mapped in list(creates) + list(updates):
        if mapped.review_reason:
            result["review"].append((mapped, mapped.review_reason))
    return result


class FakeHubSpot:
    def __init__(self):
        self.creates = []
        self.updates = []

    def _post(self, endpoint, payload=None):
        self.creates.append(payload)
        return {"objectId": f"hs-new-{len(self.creates)}"}

    def _patch(self, endpoint, payload=None):
        self.updates.append((endpoint, payload))
        return {"objectId": "hs-1"}


@pytest.fixture(autouse=True)
def no_db(monkeypatch):
    monkeypatch.setattr(ea, "_save_map", lambda *a, **k: None)


def apply(plan, **kwargs):
    kwargs.setdefault("today", PINNED_TODAY)
    hubspot = FakeHubSpot()
    outcomes = ea.apply_plan(hubspot, plan, **kwargs)
    return hubspot, outcomes


def outcome_of(outcomes, record_id):
    for o in outcomes:
        if str(o["id"]) == str(record_id):
            return o
    return None


# ---------------------------------------------------------------------------
# (a) review is never written, in any path
# ---------------------------------------------------------------------------

def test_an_archived_create_and_a_flagged_update_are_both_withheld():
    """The brief's case, under no limit at all."""
    archived_create = mapped_of(event_date_id=1043, event_date="2026-12-10",
                                archived=1)
    flagged_update = mapped_of(event_date_id=1153, event_date="2026-12-31",
                               start_time=None)
    plan = plan_with(creates=[archived_create], updates=[flagged_update])
    assert archived_create.review_reason and flagged_update.review_reason

    hubspot, outcomes = apply(plan, limit=None)

    assert hubspot.creates == [], "an archived event was created"
    assert hubspot.updates == [], "a flagged event was PATCHed"
    assert outcome_of(outcomes, 1043)["outcome"] == "withheld"
    assert outcome_of(outcomes, 1153)["outcome"] == "withheld"
    assert "needs a human" in outcome_of(outcomes, 1043)["why"]
    assert "archived in CSuite" in outcome_of(outcomes, 1043)["why"]


def test_a_withheld_record_keeps_its_reason():
    flagged = mapped_of(event_date_id=1157, start_time="September 3rd")
    _hubspot, outcomes = apply(plan_with(creates=[flagged]))

    why = outcome_of(outcomes, 1157)["why"]
    assert why.startswith("needs a human: ")
    assert "no usable time" in why


def test_an_unflagged_future_create_is_still_written():
    """Withholding has to mean something, so it cannot be everything."""
    clean = mapped_of(event_date_id=1528, event_date="2026-12-15")
    assert clean.review_reason is None

    hubspot, outcomes = apply(plan_with(creates=[clean]))

    assert len(hubspot.creates) == 1
    assert outcome_of(outcomes, 1528)["outcome"] == "created"


def test_an_unflagged_update_is_still_written():
    clean = mapped_of(event_date_id=1462, event_date="2026-12-01")
    hubspot, outcomes = apply(plan_with(updates=[clean]))

    assert len(hubspot.updates) == 1
    assert outcome_of(outcomes, 1462)["outcome"] == "updated"


def test_a_limit_cannot_buy_a_withheld_record():
    """Withheld is not deferred: a bigger limit never writes it."""
    flagged = mapped_of(event_date_id=1043, archived=1)
    for limit in (None, 1, 99):
        hubspot, outcomes = apply(plan_with(creates=[flagged]), limit=limit)
        assert hubspot.creates == []
        assert outcome_of(outcomes, 1043)["outcome"] == "withheld"


# ---------------------------------------------------------------------------
# (b) updates before creates
# ---------------------------------------------------------------------------

def test_updates_are_written_before_creates():
    create = mapped_of(event_date_id=1528, event_date="2026-12-15")
    update = mapped_of(event_date_id=1462, event_date="2026-12-01")
    plan = plan_with(creates=[create], updates=[update])

    _hubspot, outcomes = apply(plan)
    written = [o["outcome"] for o in outcomes
               if o["outcome"] in ("created", "updated")]

    assert written == ["updated", "created"]


def test_a_limit_of_one_buys_the_update_not_the_create():
    """The reversible half. A create adds a row somebody deletes by hand."""
    create = mapped_of(event_date_id=1528, event_date="2026-12-15")
    update = mapped_of(event_date_id=1462, event_date="2026-12-01")

    hubspot, outcomes = apply(plan_with(creates=[create], updates=[update]),
                              limit=1)

    assert len(hubspot.updates) == 1
    assert hubspot.creates == []
    assert outcome_of(outcomes, 1528)["outcome"] == "deferred"


# ---------------------------------------------------------------------------
# (c) updates only
# ---------------------------------------------------------------------------

def test_updates_only_withholds_every_create():
    create = mapped_of(event_date_id=1528, event_date="2026-12-15")
    update = mapped_of(event_date_id=1462, event_date="2026-12-01")

    hubspot, outcomes = apply(plan_with(creates=[create], updates=[update]),
                              updates_only=True)

    assert hubspot.creates == []
    assert len(hubspot.updates) == 1
    assert outcome_of(outcomes, 1528)["outcome"] == "withheld"
    assert "updates only" in outcome_of(outcomes, 1528)["why"]


def test_updates_only_is_off_by_default():
    create = mapped_of(event_date_id=1528, event_date="2026-12-15")
    hubspot, _ = apply(plan_with(creates=[create]))

    assert len(hubspot.creates) == 1


@pytest.mark.parametrize("phrase,expected", [
    ("sync events apply updates only", True),
    ("sync events apply", False),
    ("sync events", False),
])
def test_chat_can_ask_for_updates_only(phrase, expected):
    assert sync_commands._event_options(phrase)["updates_only"] is expected


# ---------------------------------------------------------------------------
# (d) past creates are withheld unless named
# ---------------------------------------------------------------------------

def test_a_past_create_is_withheld_by_default():
    past = mapped_of(event_date_id=1043, event_date="2021-07-29")

    hubspot, outcomes = apply(plan_with(creates=[past]))

    assert hubspot.creates == []
    why = outcome_of(outcomes, 1043)["why"]
    assert "starts in the past" in why
    assert "2021-07-29" in why
    assert "include it with its id" in why


def test_a_future_create_is_not_withheld():
    future = mapped_of(event_date_id=1528, event_date="2026-12-15")
    hubspot, _ = apply(plan_with(creates=[future]))

    assert len(hubspot.creates) == 1


def test_an_event_starting_today_is_not_past():
    today = mapped_of(event_date_id=1495, event_date="2026-10-06")
    hubspot, _ = apply(plan_with(creates=[today]))

    assert len(hubspot.creates) == 1


def test_a_named_id_is_created_even_though_it_is_past():
    """The brief's "include 1495" case."""
    past = mapped_of(event_date_id=1495, event_date="2026-09-30")

    hubspot, outcomes = apply(plan_with(creates=[past]),
                              include_ids=["1495"])

    assert len(hubspot.creates) == 1
    assert outcome_of(outcomes, 1495)["outcome"] == "created"


def test_naming_one_id_does_not_release_the_others():
    wanted = mapped_of(event_date_id=1495, event_date="2026-09-30")
    other = mapped_of(event_date_id=1043, event_date="2021-07-29")

    hubspot, outcomes = apply(plan_with(creates=[wanted, other]),
                              include_ids=["1495"])

    assert len(hubspot.creates) == 1
    assert outcome_of(outcomes, 1043)["outcome"] == "withheld"


def test_naming_an_id_does_NOT_override_review():
    """Two different brakes. "I want this one" is not "I have looked at it"."""
    archived_past = mapped_of(event_date_id=1043, event_date="2021-07-29",
                              archived=1)

    hubspot, outcomes = apply(plan_with(creates=[archived_past]),
                              include_ids=["1043"])

    assert hubspot.creates == []
    assert "needs a human" in outcome_of(outcomes, 1043)["why"]


def test_chat_reads_include_ids_from_the_message():
    options = sync_commands._event_options("sync events apply include 1495")
    assert options["include_ids"] == ["1495"]

    options = sync_commands._event_options(
        "sync events apply include 1495 include 1231")
    assert options["include_ids"] == ["1495", "1231"]


def test_chat_cannot_turn_future_only_off():
    """A one-line message must not be able to say "create 74 events for
    things that already happened". The CLI has --past-creates."""
    assert "future_only" not in sync_commands._event_options(
        "sync events apply past creates no limit all of them")


def test_the_cli_exposes_the_two_brakes_and_the_blunt_one():
    source = open("scripts/event_sync.py").read()

    assert "--updates-only" in source
    assert "--include-id" in source
    assert "--past-creates" in source
    assert "future_only=not args.past_creates" in source


# ---------------------------------------------------------------------------
# (e) run() refuses an incomplete or possibly-truncated read
# ---------------------------------------------------------------------------

def fetched(rows, complete=True, error=None):
    return eh.Fetched(rows=rows, calls=1, complete=complete, error=error,
                      total_429s=0)


def run_with(monkeypatch, rows, complete=True):
    monkeypatch.setattr(ea, "migration_applied", lambda: True)
    monkeypatch.setattr(ea, "load_map", lambda: {})
    monkeypatch.setattr(ea.eh, "fetch_event_dates",
                        lambda client, pace_ms=None: fetched(rows, complete))
    monkeypatch.setattr(ea.eh, "hubspot_index",
                        lambda hubspot: ({}, 1, None))
    monkeypatch.setattr("clients.csuite.CSuiteClient", lambda: object())
    return ea.run(hubspot=object(), dry_run=True, today=PINNED_TODAY)


def test_an_incomplete_read_refuses_to_plan(monkeypatch):
    result = run_with(monkeypatch, [row(1)], complete=False)

    assert "did not complete" in result["error"]
    assert "cannot be told from what does not exist" in result["error"]
    assert result["plan"] is None


def test_a_read_at_the_request_limit_refuses_to_plan(monkeypatch):
    """event/list/dates does not paginate, so a response AT view_limit is
    indistinguishable from one cut off at it — and there is no page 2."""
    ceiling = ea._date_read_ceiling()
    assert ceiling == 1000

    result = run_with(monkeypatch, [row(i) for i in range(ceiling)])

    assert "may be truncated" in result["error"]
    assert str(ceiling) in result["error"]
    assert "does not paginate" in result["error"]
    assert result["plan"] is None


def test_a_read_below_the_limit_plans_normally(monkeypatch):
    result = run_with(monkeypatch, [row(1)])

    assert result["error"] is None
    assert result["plan"] is not None


def test_the_ceiling_comes_from_the_endpoint_contract():
    from clients.csuite_fetch import ENDPOINT_CONTRACTS

    contract = ENDPOINT_CONTRACTS[eh.EVENT_DATES_ENDPOINT]
    assert contract.paginate is False, "if it ever pages, drop the ceiling"
    assert ea._date_read_ceiling() == contract.view_limit


# ---------------------------------------------------------------------------
# The preview predicts what apply would do
# ---------------------------------------------------------------------------

def test_the_preview_counts_what_apply_would_write(monkeypatch):
    rows = [row(1043, event_date="2021-07-29", archived=1),
            row(1528, event_date="2026-12-15"),
            row(1495, event_date="2026-09-30")]
    monkeypatch.setattr(ea, "migration_applied", lambda: True)
    monkeypatch.setattr(ea, "load_map", lambda: {})
    monkeypatch.setattr(ea.eh, "fetch_event_dates",
                        lambda client, pace_ms=None: fetched(rows))
    monkeypatch.setattr(ea.eh, "hubspot_index", lambda hubspot: ({}, 1, None))
    monkeypatch.setattr("clients.csuite.CSuiteClient", lambda: object())
    monkeypatch.setattr(ea, "record_run", lambda *a, **k: None)

    result = ea.run(hubspot=object(), dry_run=True, today=PINNED_TODAY)

    assert result["created"] == 1, "only the future, unflagged one"
    assert result["withheld"] == 2
    reasons = dict(result["withheld_rows"])
    assert "needs a human" in reasons["1043"]
    assert "starts in the past" in reasons["1495"]


# ---------------------------------------------------------------------------
# (f) the report says review overlaps
# ---------------------------------------------------------------------------

def report(**overrides):
    base = {"dry_run": True, "created": 1, "updated": 2, "unchanged": 0,
            "deferred": 0, "unknown": 0, "failed": 0, "skipped": 98,
            "review": 79, "review_rows": [], "withheld": 79,
            "withheld_rows": [("1043", "needs a human: archived in CSuite")],
            "csuite_calls": 1, "hubspot_calls": 1, "event_dates_read": 186,
            "run_logged": True, "migration_applied": True, "error": None,
            "stopped": None, "limit": None, "updates_only": False,
            "future_only": True, "include_ids": []}
    base.update(overrides)
    return base


def test_the_review_line_says_it_is_counted_above():
    reply = sync_commands._format_event_sync_results(report())

    assert "**79** need a human" in reply
    assert "*also counted above*" in reply
    assert "not a separate group" in reply
    assert "All of these are withheld." in reply


def test_the_report_has_a_withheld_line():
    reply = sync_commands._format_event_sync_results(report())

    assert "**79** withheld" in reply
    assert "deliberately NOT written" in reply


def test_the_withheld_section_groups_by_kind():
    reply = sync_commands._format_event_sync_results(report(
        withheld=3,
        withheld_rows=[("1", "needs a human: archived in CSuite"),
                       ("2", "starts in the past (2021-07-29) — include it"),
                       ("3", "updates only — no event was created")]))

    assert "**1** needs a human" in reply
    assert "**1** starts in the past" in reply
    assert "**1** updates only" in reply


def test_a_long_withheld_list_is_truncated_but_the_count_is_not():
    rows = [(str(i), "starts in the past (2021-01-01)") for i in range(40)]
    reply = sync_commands._format_event_sync_results(
        report(withheld=40, withheld_rows=rows))

    assert "**40** withheld" in reply
    assert "and **30** more" in reply
    assert "the count above is the total" in reply


def test_updates_only_is_stated_in_the_report():
    reply = sync_commands._format_event_sync_results(
        report(updates_only=True))

    assert "Updates only" in reply
    assert "every create was withheld" in reply


def test_turning_future_only_off_is_stated_as_a_warning():
    reply = sync_commands._format_event_sync_results(
        report(future_only=False))

    assert "future_only is off" in reply
    assert "past events can be created" in reply


def test_named_ids_are_listed_in_the_report():
    reply = sync_commands._format_event_sync_results(
        report(include_ids=["1495"]))

    assert "Past creates allowed by id" in reply
    assert "`1495`" in reply
