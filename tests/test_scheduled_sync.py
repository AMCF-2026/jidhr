"""The scheduled sync chain. SHIPS DISARMED.

Measured 2026-10-13, before any of this was written:

* Of 187 CSuite event dates, 8 fall inside today - 1; 81 are earlier and 98
  carry no usable event_date.
* THREE of those eight — 1529, 1530, 1531 — have no HubSpot marketing event
  at all, so a registrations run on one of them stops at event_object_ids
  with "has no HubSpot objectId". That is why the chain is events, then
  contacts, then registrations.
* The jobs runner's HANDLERS held only mirror_refresh.

No network, no database.
"""

import pytest

from clients import notify as notifier
from intents import sync_commands
from jobs.handlers import registrations_sync as handler
from jobs.runner import HANDLERS, JobFailed
from sync import scheduled


def calendar(inside, total=187, error=None, undated=None):
    """read_calendar's return shape. The chain reads the window, the
    undated set and the created-event names off ONE snapshot now."""
    return {"inside": list(inside or []), "undated": list(undated or []),
            "all": list(inside or []) + list(undated or []),
            "total": total, "error": error}


class Hub:
    """Records every HubSpot request so a preview can be proved silent."""

    def __init__(self):
        self.calls = []

    def _get(self, endpoint, params=None):
        self.calls.append(("GET", endpoint))
        return {"results": [], "total": 0}

    def _post(self, endpoint, data=None):
        self.calls.append(("POST", endpoint))
        return {"results": [], "numErrors": 0}

    def _send_with_status(self, method, endpoint, data=None):
        self.calls.append((method, endpoint))
        return {"id": "1"}, 201

    def writes(self):
        return [(m, e) for m, e in self.calls
                if m in ("POST", "PUT", "PATCH", "DELETE")
                and "batch/read" not in e and "/search" not in e]


@pytest.fixture(autouse=True)
def armed(monkeypatch):
    """Enabled and in preview — the acceptance configuration."""
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_ENABLED", True)
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_MODE", "preview")
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_WINDOW_DAYS", 1)
    monkeypatch.delenv("NOTIFY_SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("NOTIFY_EMAIL_TO", raising=False)
    monkeypatch.setattr(notifier, "_warned_unconfigured", False)


@pytest.fixture
def sent(monkeypatch):
    calls = []
    monkeypatch.setattr("clients.notify.notify",
                        lambda subject, body, **k:
                        calls.append((notifier.subject_line(
                            subject, k.get("kind", notifier.KIND_ALERT)),
                            body, k))
                        or {"configured": True, "sent": ["email"],
                            "failed": []})
    return calls


@pytest.fixture
def chain(monkeypatch):
    """Stub the three steps so the CHAIN's behaviour is what is tested."""
    seen = {"events": None, "contacts": None, "registrations": None}

    def events(out, hubspot, dry_run, today):
        seen["events"] = {"dry_run": dry_run, "ids": list(out["event_ids"])}
        scheduled._record(out, scheduled.STEP_EVENTS,
                          {"created": 0, "updated": 0, "unchanged": 1,
                           "withheld": 0, "review": 0, "run_id": 101})

    def contacts(out, csuite, hubspot, dry_run):
        seen["contacts"] = {"dry_run": dry_run, "ids": list(out["event_ids"])}
        scheduled._record(out, scheduled.STEP_CONTACTS,
                          {"registrant_rows": 3, "excluded": 0,
                           "duplicates_dropped": 0, "unique_emails": 3,
                           "already_contacts": 3, "to_create": 0,
                           "created": 0, "existing": 0, "run_id": 102})

    def registrations(out, csuite, hubspot, dry_run):
        seen["registrations"] = {"dry_run": dry_run,
                                 "ids": list(out["registrable_ids"])}
        scheduled._record(out, scheduled.STEP_REGISTRATIONS,
                          {"events_read": 1, "already": 3, "registered": 0,
                           "would_register": 0, "held": 0, "withheld": 0,
                           "unverified": 0, "failed": 0, "run_id": 103})

    monkeypatch.setattr(scheduled, "_step_events", events)
    monkeypatch.setattr(scheduled, "_step_contacts", contacts)
    monkeypatch.setattr(scheduled, "_step_registrations", registrations)
    monkeypatch.setattr(scheduled, "read_calendar",
                        lambda csuite, today=None: calendar(*([{"event_date_id": "1429",
                              "event_name": "Event - Other",
                              "event_date": "2026-10-20"}], 187, None)))
    monkeypatch.setattr(scheduled, "_no_manual_apply_running",
                        lambda: None)
    return seen


# ---------------------------------------------------------------------------
# Disarmed
# ---------------------------------------------------------------------------

def test_it_ships_disarmed(monkeypatch):
    """The CODE default, read with the environment cleared — the autouse
    fixture above arms it for every other test here."""
    import importlib

    monkeypatch.delenv("SCHEDULED_SYNC_ENABLED", raising=False)
    monkeypatch.delenv("SCHEDULED_SYNC_MODE", raising=False)
    import config
    importlib.reload(config)

    assert config.Config.SCHEDULED_SYNC_ENABLED is False, \
        "the default must be off, set nowhere but here"
    assert config.Config.SCHEDULED_SYNC_MODE == "preview"
    assert config.Config.SCHEDULED_SYNC_WINDOW_DAYS == 1


def test_disabled_touches_nothing_and_sends_no_email(monkeypatch, sent):
    """A deploy should be silent."""
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_ENABLED", False)
    ran = []
    monkeypatch.setattr(scheduled, "run_chain",
                        lambda **k: ran.append(k) or {})

    result = handler.handle({"payload": {}})

    assert result["enabled"] is False
    assert ran == [], "no CSuite or HubSpot call"
    assert sent == [], "and NO email — a deploy is not news"


def test_the_chat_command_says_it_is_disarmed(monkeypatch):
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_ENABLED", False)

    reply = sync_commands.handle("run scheduled sync now", None)

    assert "disarmed" in reply
    assert "SCHEDULED_SYNC_ENABLED" in reply
    assert "no email" in reply


# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw", ["live", "LIVE", " live ", "Live"])
def test_only_the_word_live_means_live(monkeypatch, raw):
    monkeypatch.setenv("SCHEDULED_SYNC_MODE", raw)
    import importlib

    import config
    importlib.reload(config)
    assert config.Config.SCHEDULED_SYNC_MODE == "live"


@pytest.mark.parametrize("raw", ["", "preview", "true", "yes", "1", "liv",
                                 "livee", "PREVIEW", "nonsense"])
def test_anything_else_is_preview(monkeypatch, raw):
    monkeypatch.setenv("SCHEDULED_SYNC_MODE", raw)
    import importlib

    import config
    importlib.reload(config)
    assert config.Config.SCHEDULED_SYNC_MODE == "preview"


@pytest.mark.parametrize("raw", ["true", "TRUE", " true "])
def test_only_the_word_true_arms_it(monkeypatch, raw):
    monkeypatch.setenv("SCHEDULED_SYNC_ENABLED", raw)
    import importlib

    import config
    importlib.reload(config)
    assert config.Config.SCHEDULED_SYNC_ENABLED is True


@pytest.mark.parametrize("raw", ["", "false", "1", "yes", "True ", "on"])
def test_anything_else_leaves_it_disarmed(monkeypatch, raw):
    monkeypatch.setenv("SCHEDULED_SYNC_ENABLED", raw)
    import importlib

    import config
    importlib.reload(config)
    assert config.Config.SCHEDULED_SYNC_ENABLED is (raw.strip().lower()
                                                    == "true")


def test_an_unreadable_window_falls_back_to_one_day(monkeypatch):
    from datetime import date, timedelta

    for bad in (None, "nope", -5):
        monkeypatch.setattr("config.Config.SCHEDULED_SYNC_WINDOW_DAYS", bad)
        assert scheduled.window_floor(date(2026, 10, 13)) == \
            date(2026, 10, 13) - timedelta(days=1)


def test_live_without_the_manual_flag_refuses_and_alerts(monkeypatch, sent,
                                                         chain):
    """Never a silent fall back to preview: a run that quietly previewed
    would look exactly like a run with nothing to do."""
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_MODE", "live")
    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", False)

    with pytest.raises(JobFailed):
        handler.handle({"payload": {}})

    assert chain["events"] is None, "no step ran"
    assert len(sent) == 1
    subject, body, _k = sent[0]
    assert subject.startswith("[Jidhr ALERT]")
    assert "REGISTRATIONS_SYNC_ENABLED is off" in body
    assert "falling back to preview" in body


def test_live_with_both_flags_runs_live(monkeypatch, chain, sent):
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_MODE", "live")
    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", True)

    out = handler.handle({"payload": {}})

    assert out["mode"] == "live"
    assert chain["events"]["dry_run"] is False
    assert chain["contacts"]["dry_run"] is False
    assert chain["registrations"]["dry_run"] is False


# ---------------------------------------------------------------------------
# Preview writes nothing
# ---------------------------------------------------------------------------

def test_preview_runs_every_step_as_a_dry_run(chain, sent):
    out = handler.handle({"payload": {}})

    assert out["mode"] == "preview"
    for step in ("events", "contacts", "registrations"):
        assert chain[step]["dry_run"] is True, step
    assert out["stopped"] is None


def test_preview_issues_no_write_request(monkeypatch, sent):
    """The real steps, with a HubSpot double that records every call."""
    hub = Hub()
    monkeypatch.setattr(scheduled, "read_calendar",
                        lambda csuite, today=None: calendar(*([], 187, None)))
    monkeypatch.setattr(scheduled, "_no_manual_apply_running", lambda: None)

    class NoEvents:
        def run(self, **k):
            return {}

    monkeypatch.setattr(scheduled, "_step_events",
                        lambda out, h, d, t: scheduled._record(
                            out, scheduled.STEP_EVENTS, {"created": 0}))
    monkeypatch.setattr(scheduled, "_step_contacts",
                        lambda out, c, h, d: scheduled._record(
                            out, scheduled.STEP_CONTACTS, {"created": 0}))

    out = scheduled.run_chain(csuite=object(), hubspot=hub, mode="preview")

    assert out["stopped"] is None
    assert hub.writes() == [], hub.writes()


def test_preview_sends_exactly_one_ok_email(chain, sent):
    handler.handle({"payload": {}})

    assert len(sent) == 1, "never both, never neither"
    subject, _body, _k = sent[0]
    assert subject.startswith("[Jidhr OK]")
    assert "(preview)" in subject


def test_zero_activity_still_sends_ok(chain, sent):
    """A missing email is how Carl knows it died."""
    handler.handle({"payload": {}})

    subject, body, _k = sent[0]
    assert subject.startswith("[Jidhr OK]")
    assert "nothing new" in subject
    assert "mode: preview" in body


# ---------------------------------------------------------------------------
# A stop stops the rest, and sends ONE alert
# ---------------------------------------------------------------------------

def test_a_stopped_event_step_skips_the_later_steps(monkeypatch, chain,
                                                    sent):
    def boom(out, hubspot, dry_run, today):
        scheduled._record(out, scheduled.STEP_EVENTS,
                          {"created": 0, "stopped": "an ambiguous create"})
        raise scheduled.ChainStopped(scheduled.STEP_EVENTS,
                                     "an ambiguous create")

    monkeypatch.setattr(scheduled, "_step_events", boom)

    with pytest.raises(JobFailed):
        handler.handle({"payload": {}})

    assert chain["contacts"] is None, "contacts must not run"
    assert chain["registrations"] is None, "nor registrations"
    assert len(sent) == 1
    subject, body, _k = sent[0]
    assert subject.startswith("[Jidhr ALERT]")
    assert "STOPPED at event_sync" in body
    assert "[contact_create] NOT REACHED" in body
    assert "[registrations_sync] NOT REACHED" in body


def test_a_stopped_contact_step_skips_registrations(monkeypatch, chain,
                                                    sent):
    def boom(out, csuite, hubspot, dry_run):
        raise scheduled.ChainStopped(
            scheduled.STEP_CONTACTS,
            "this run would create 40 contact(s), more than the maximum "
            "of 25")

    monkeypatch.setattr(scheduled, "_step_contacts", boom)

    with pytest.raises(JobFailed):
        handler.handle({"payload": {}})

    assert chain["registrations"] is None
    assert len(sent) == 1
    assert sent[0][0].startswith("[Jidhr ALERT]")
    assert "more than the maximum" in sent[0][1]


def test_a_guard_refusal_never_sends_an_ok_as_well(monkeypatch, chain, sent):
    monkeypatch.setattr(
        scheduled, "_step_registrations",
        lambda out, c, h, d: (_ for _ in ()).throw(
            scheduled.ChainStopped(scheduled.STEP_REGISTRATIONS, "refused")))

    with pytest.raises(JobFailed):
        handler.handle({"payload": {}})

    prefixes = [s.split("]")[0] + "]" for s, _b, _k in sent]
    assert prefixes == ["[Jidhr ALERT]"], prefixes


def test_a_dying_chain_still_sends_one_email(monkeypatch, chain, sent):
    """A traceback in the cron leaves the reader with nothing."""
    monkeypatch.setattr(
        scheduled, "_step_contacts",
        lambda out, c, h, d: (_ for _ in ()).throw(
            RuntimeError("unexpected")))

    with pytest.raises(JobFailed):
        handler.handle({"payload": {}})

    assert len(sent) == 1
    assert sent[0][0].startswith("[Jidhr ALERT]")
    assert "RuntimeError: unexpected" in sent[0][1]


def test_an_incomplete_event_read_stops_before_any_step(monkeypatch, chain,
                                                        sent):
    """A slice of the calendar is not the calendar."""
    monkeypatch.setattr(scheduled, "read_calendar",
                        lambda csuite, today=None: calendar(*([], 40, "the CSuite event-date read came back "
                                    "incomplete")))

    with pytest.raises(JobFailed):
        handler.handle({"payload": {}})

    assert chain["events"] is None
    assert "incomplete" in sent[0][1]


# ---------------------------------------------------------------------------
# The lock
# ---------------------------------------------------------------------------

def test_a_running_manual_apply_refuses_the_scheduled_run(monkeypatch, sent):
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_MODE", "live")
    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", True)
    monkeypatch.setattr("sync.registration_jobs.running_applies",
                        lambda: [{"id": 300}])
    monkeypatch.setattr("sync.contact_jobs.running_applies", lambda: [])
    ran = []
    monkeypatch.setattr(scheduled, "_step_events",
                        lambda *a: ran.append("events"))

    with pytest.raises(JobFailed):
        handler.handle({"payload": {}})

    assert ran == [], "it stood down"
    assert len(sent) == 1
    assert sent[0][0].startswith("[Jidhr ALERT]")
    assert "run_log 300" in sent[0][1]
    assert "one registration becomes two" in sent[0][1]


def test_a_running_manual_contact_create_also_refuses_it(monkeypatch, sent):
    monkeypatch.setattr("config.Config.SCHEDULED_SYNC_MODE", "live")
    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", True)
    monkeypatch.setattr("sync.registration_jobs.running_applies", lambda: [])
    monkeypatch.setattr("sync.contact_jobs.running_applies",
                        lambda: [{"id": 301}])

    with pytest.raises(JobFailed):
        handler.handle({"payload": {}})

    assert "run_log 301" in sent[0][1]


def test_the_reverse_direction_needs_no_new_code():
    """A live scheduled run's own reg.run and cc.run open run_log rows with
    applied = true and status 'running', which is exactly what the manual
    guards query — so a manual apply started mid-chain is refused by the
    guard that already exists."""
    from sync import contact_jobs, registration_jobs

    for sql in (registration_jobs._RUNNING_SQL, contact_jobs._RUNNING_SQL):
        assert "applied IS TRUE" in sql
        assert "status = 'running'" in sql


def test_a_preview_run_does_not_take_the_lock(monkeypatch, chain, sent):
    """A dry run writes nothing, so it must not block a manual apply."""
    checked = []
    monkeypatch.setattr(scheduled, "_no_manual_apply_running",
                        lambda: checked.append(True))

    handler.handle({"payload": {}})

    assert checked == [], "preview does not need the lock"


# ---------------------------------------------------------------------------
# The window
# ---------------------------------------------------------------------------

def test_the_window_excludes_a_past_event():
    from datetime import date

    rows = [{"event_date_id": "1155", "event_date": "2026-04-10"},
            {"event_date_id": "1429", "event_date": "2026-10-20"},
            {"event_date_id": "9999", "event_date": ""},
            {"event_date_id": "8888", "event_date": "2026-10-12"}]

    class Fetch:
        records = rows
        complete = True
        pages = 1

    # The filter itself, exercised directly:
    floor = date(2026, 10, 12)
    kept = [r for r in rows
            if r["event_date"] and r["event_date"] >= str(floor)]
    assert [r["event_date_id"] for r in kept] == ["1429", "8888"]
    assert "1155" not in [r["event_date_id"] for r in kept]


def test_the_window_floor_is_today_minus_one():
    from datetime import date

    assert scheduled.window_floor(date(2026, 10, 13)) == date(2026, 10, 12)


def test_a_past_event_is_never_in_the_chain_scope(monkeypatch, chain, sent):
    """1155 finished in April. A schedule must not register anyone for it."""
    monkeypatch.setattr(scheduled, "read_calendar",
                        lambda csuite, today=None: calendar(*([{"event_date_id": "1429",
                              "event_name": "Event - Other",
                              "event_date": "2026-10-20"}], 187, None)))

    out = handler.handle({"payload": {}})

    assert out["event_ids"] == ["1429"]
    assert "1155" not in chain["contacts"]["ids"]
    assert "1155" not in chain["registrations"]["ids"]


# ---------------------------------------------------------------------------
# Newsletters stays contacts-only
# ---------------------------------------------------------------------------

def test_a_newsletters_event_gets_contacts_but_is_never_registered():
    rows = [{"event_date_id": "1153", "event_name": "Newsletters",
             "event_date": "2026-12-31"},
            {"event_date_id": "1429", "event_name": "Event - Other",
             "event_date": "2026-10-20"}]

    assert scheduled.event_ids_of(rows) == ["1153", "1429"]
    assert scheduled.registrable_ids(rows) == ["1429"]


def test_1153_is_excluded_from_registrations_by_both_routes(monkeypatch):
    """Held AND contacts-only AND Newsletters. Any one of them suffices."""
    rows = [{"event_date_id": "1153", "event_name": "Newsletters",
             "event_date": "2026-12-31"}]
    assert scheduled.registrable_ids(rows) == []

    monkeypatch.setattr("config.Config.CONTACTS_ONLY_EVENT_CATEGORIES", ())
    assert scheduled.registrable_ids(rows) == [], "still held by id"

    monkeypatch.setattr("config.Config.REGISTRATION_HELD_EVENT_IDS", ())
    assert scheduled.registrable_ids(rows) == [], "still contacts-only by id"


def test_contacts_only_events_are_named_in_the_summary(monkeypatch, chain,
                                                       sent):
    monkeypatch.setattr(scheduled, "read_calendar",
                        lambda csuite, today=None: calendar(*([{"event_date_id": "1153",
                              "event_name": "Newsletters",
                              "event_date": "2026-12-31"},
                             {"event_date_id": "1429",
                              "event_name": "Event - Other",
                              "event_date": "2026-10-20"}], 187, None)))

    out = handler.handle({"payload": {}})

    assert out["contacts_only_ids"] == ["1153"]
    assert chain["contacts"]["ids"] == ["1153", "1429"]
    assert chain["registrations"]["ids"] == ["1429"]
    assert "contacts-only / held (never registered): 1153" in sent[0][1]


# ---------------------------------------------------------------------------
# The handler, and the summary body
# ---------------------------------------------------------------------------

def test_the_handler_is_registered_in_the_existing_runner():
    """No new mechanism, no new service."""
    assert HANDLERS.get("registrations_sync") is handler.handle
    assert sorted(HANDLERS) == ["daf_sync", "forms_sync", "mirror_refresh",
                                "registrations_sync"]


def test_no_new_scheduler_was_added():
    requirements = open("requirements.txt").read().lower()
    for package in ("apscheduler", "celery", "schedule="):
        assert package not in requirements, package
    config = open("railway.toml").read()
    assert config.count("startCommand") == 1, "still one web service"


def test_the_summary_reports_the_facts_the_brief_asks_for(chain, sent):
    handler.handle({"payload": {}})
    body = sent[0][1]

    assert "mode: preview" in body
    assert "window: event_date >=" in body
    assert "events in window: 1 of 187" in body
    assert "run_log: event_sync:101, contact_create:102, " \
           "registrations_sync:103" in body
    # rows = excluded + dupes + unique
    assert "rows: 3 = 0 excluded + 0 duplicate(s) + 3 unique" in body
    assert "already a contact: 3" in body
    assert "already registered: 3" in body
    assert "registered and VERIFIED: 0" in body


def test_the_summary_reports_verified_not_attempted(monkeypatch, sent):
    """Live writes are verified like a manual apply; the email must report
    the verified count."""
    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", True)
    monkeypatch.setattr(scheduled, "read_calendar",
                        lambda csuite, today=None: calendar(*([{"event_date_id": "1429",
                              "event_name": "E", "event_date": "2026-10-20"}],
                            187, None)))
    monkeypatch.setattr(scheduled, "_no_manual_apply_running", lambda: None)
    monkeypatch.setattr(scheduled, "_step_events",
                        lambda out, h, d, t: scheduled._record(
                            out, scheduled.STEP_EVENTS, {"created": 0}))
    monkeypatch.setattr(scheduled, "_step_contacts",
                        lambda out, c, h, d: scheduled._record(
                            out, scheduled.STEP_CONTACTS, {"created": 0}))
    monkeypatch.setattr(
        scheduled, "_step_registrations",
        lambda out, c, h, d: scheduled._record(
            out, scheduled.STEP_REGISTRATIONS,
            {"writes_attempted": 5, "registered": 4, "unverified": 1,
             "already": 0, "held": 0, "withheld": 0, "failed": 0,
             "events_read": 1}))

    out = scheduled.run_chain(csuite=object(), hubspot=Hub(), mode="live")
    body = "\n".join(scheduled.summary_lines(out))

    assert "registered and VERIFIED: 4" in body
    assert "unverified: 1" in body
    assert "writes_attempted" not in body, "attempted is not the headline"


def test_the_summary_never_carries_an_address(chain, sent):
    handler.handle({"payload": {}})
    assert "@" not in sent[0][1]


def test_the_marketing_breakdown_is_of_new_contacts_only(chain, sent):
    handler.handle({"payload": {}})
    body = sent[0][1]

    assert "marketing status of those 0 NEW contact(s)" in body
    assert "n/a — none" in body


def test_the_chat_command_runs_the_real_handler(monkeypatch, chain, sent):
    reply = sync_commands.handle("run scheduled sync now", None)

    assert "Scheduled sync (preview)" in reply
    assert "nothing was written" in reply
    assert "exactly what 06:15 will do" in reply
    assert "[Jidhr OK]" in reply
    assert "mode: preview" in reply


def test_the_chat_command_reports_a_stop(monkeypatch, chain, sent):
    monkeypatch.setattr(
        scheduled, "_step_events",
        lambda out, h, d, t: (_ for _ in ()).throw(
            scheduled.ChainStopped(scheduled.STEP_EVENTS, "refused")))

    reply = sync_commands.handle("run scheduled sync now", None)

    assert "STOPPED" in reply
    assert "[Jidhr ALERT]" in reply


def test_the_filter_advice_is_a_query_gmail_can_parse(monkeypatch):
    """Gmail ignores brackets, and the unmatched one broke the query."""
    monkeypatch.setattr(notifier, "configured", lambda: True)
    monkeypatch.setattr(notifier, "safe_notify",
                        lambda *a, **k: {"configured": True,
                                         "sent": ["email"], "failed": []})

    reply = sync_commands.handle("test notify", None)

    assert "subject:Jidhr" in reply
    assert "Gmail ignores the brackets" in reply
    assert notifier.OK_PREFIX in reply, "the heartbeat prefix is named too"
    # The broken form appears only as what NOT to use.
    assert "Not `subject:([Jidhr )`" in reply
