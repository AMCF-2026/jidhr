"""Guardrails for unattended operation.

The go-live readiness report found four gaps that only matter once nobody
is watching the reply:

  * check_reconciles was computed and then IGNORED — the report warned and
    the write went ahead anyway (sync/contact_create, V5.73).
  * No batch-size ceiling. shrink_guard only fires for an event with
    history; an event with known_count == 0 had nothing to compare against,
    and contact creation had no cap at all beyond the hand-typed limit.
  * No notifier of any kind existed anywhere in the repo, so a stopped
    background run was discoverable only by typing "contacts status".
  * maxconn=2 against --threads 4, so three concurrent DB-touching
    requests in one worker raised PoolError.

No network, no database.
"""

import pytest

from clients import notify as notifier
from intents import sync_commands
from sync import contact_create as cc
from sync import registrations as reg
from tests.test_contact_create import (CSuite, HubSpot, EMAIL, profile,
                                       registrant)


@pytest.fixture(autouse=True)
def quiet(monkeypatch):
    """No notifier configured, and the once-per-process warning reset."""
    monkeypatch.delenv("NOTIFY_SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("NOTIFY_EMAIL_TO", raising=False)
    monkeypatch.setattr(notifier, "_warned_unconfigured", False)
    monkeypatch.setattr(cc, "open_run", lambda applied: None)
    monkeypatch.setattr(cc, "close_run", lambda *a, **k: None)
    monkeypatch.setattr(reg, "load_map", lambda: {})
    monkeypatch.setattr(reg, "_latest_audit_id", lambda endpoint: 200)


@pytest.fixture
def sent(monkeypatch):
    """Capture every notification instead of sending it."""
    calls = []
    monkeypatch.setattr("clients.notify.notify",
                        lambda subject, body, **ctx:
                        calls.append((subject, body, ctx))
                        or {"configured": True, "sent": ["slack"],
                            "failed": []})
    return calls


def arm(monkeypatch, on=True):
    monkeypatch.setattr("config.Config.REGISTRATIONS_SYNC_ENABLED", on)


# ---------------------------------------------------------------------------
# CHANGE 1: the notifier
# ---------------------------------------------------------------------------

def test_unset_is_a_no_op():
    assert notifier.configured() is False
    result = notifier.safe_notify("subject", "body", run_log=1)
    assert result == {"configured": False, "sent": [], "failed": []}


def test_unset_warns_once_per_process(monkeypatch, caplog):
    import logging

    with caplog.at_level(logging.WARNING, logger="clients.notify"):
        notifier.safe_notify("a", "b")
        notifier.safe_notify("c", "d")
        notifier.safe_notify("e", "f")

    warnings = [r for r in caplog.records if "notifications are off" in
                r.getMessage()]
    assert len(warnings) == 1, "a per-guard warning would bury the log"


def test_a_slack_webhook_is_posted(monkeypatch):
    monkeypatch.setenv("NOTIFY_SLACK_WEBHOOK_URL", "https://example.invalid/h")
    posted = {}

    class Response:
        status_code = 200

    class Requests:
        @staticmethod
        def post(url, json=None, timeout=None):
            posted.update(url=url, json=json, timeout=timeout)
            return Response()

    monkeypatch.setitem(__import__("sys").modules, "requests", Requests)

    result = notifier.safe_notify("Jidhr: stopped", "because X", run_log=9,
                                  to_create=3)

    assert result["sent"] == ["slack"]
    assert posted["url"] == "https://example.invalid/h"
    assert posted["timeout"] == notifier.TIMEOUT_SECONDS
    text = posted["json"]["text"]
    assert "Jidhr: stopped" in text and "because X" in text
    assert "run_log: 9" in text and "to_create: 3" in text


def test_a_notifier_failure_can_never_fail_a_run(monkeypatch):
    monkeypatch.setenv("NOTIFY_SLACK_WEBHOOK_URL", "https://example.invalid/h")

    class Requests:
        @staticmethod
        def post(url, json=None, timeout=None):
            raise RuntimeError("network is down")

    monkeypatch.setitem(__import__("sys").modules, "requests", Requests)

    result = notifier.safe_notify("s", "b")

    assert result["failed"] == ["slack"]
    assert result["sent"] == []


def test_even_a_broken_notifier_module_cannot_raise(monkeypatch):
    monkeypatch.setattr(notifier, "notify",
                        lambda *a, **k: (_ for _ in ()).throw(
                            TypeError("bad signature")))

    result = notifier.safe_notify("s", "b")

    assert result["failed"] == ["notify"]


def test_a_notification_never_carries_an_address(monkeypatch):
    monkeypatch.setenv("NOTIFY_SLACK_WEBHOOK_URL", "https://example.invalid/h")
    posted = {}

    class Response:
        status_code = 200

    class Requests:
        @staticmethod
        def post(url, json=None, timeout=None):
            posted.update(json=json)
            return Response()

    monkeypatch.setitem(__import__("sys").modules, "requests", Requests)

    notifier.safe_notify("s", "donor@private.invalid could not be created",
                         masked="d***@private.invalid")

    text = posted["json"]["text"]
    assert "donor@private.invalid" not in text
    assert "<address redacted>" in text
    assert "d***@private.invalid" in text, "the masked form survives"


def test_email_without_an_smtp_host_is_reported_not_dropped(monkeypatch):
    monkeypatch.setenv("NOTIFY_EMAIL_TO", "ops@example.invalid")
    monkeypatch.delenv("NOTIFY_SMTP_HOST", raising=False)

    result = notifier.safe_notify("s", "b")

    assert result["configured"] is True
    assert result["failed"] == ["email"]


def test_the_test_notify_command_says_when_it_is_off():
    reply = sync_commands.handle("test notify", None)

    assert "Notifications are off" in reply
    assert "NOTIFY_SLACK_WEBHOOK_URL" in reply
    assert "Guards still refuse" in reply


def test_the_test_notify_command_reports_what_was_sent(monkeypatch):
    monkeypatch.setattr(notifier, "configured", lambda: True)
    monkeypatch.setattr(notifier, "safe_notify",
                        lambda *a, **k: {"configured": True,
                                         "sent": ["slack"], "failed": []})

    reply = sync_commands.handle("test notify", None)

    assert "Test notification sent via: slack" in reply


def test_test_notify_is_staff_only_like_the_rest_of_the_module():
    assert sync_commands.ALLOWED_ROLES == frozenset({"admin", "staff"})
    assert sync_commands.can_handle("test notify")


def test_sync_all_cannot_reach_test_notify():
    for phrase in sync_commands.ALL_SYNC_PHRASES:
        assert not any(p in phrase
                       for p in sync_commands.NOTIFY_TEST_PHRASES)


# ---------------------------------------------------------------------------
# CHANGE 2: check_reconciles gates the write
# ---------------------------------------------------------------------------

def test_an_unreconciled_plan_writes_nothing(monkeypatch, sent):
    """It used to compute the check and then call _apply anyway."""
    arm(monkeypatch)
    csuite = CSuite({"1430": [registrant(EMAIL, 21676)]},
                    profiles={21676: profile()})
    hubspot = HubSpot()
    monkeypatch.setattr(cc, "check_reconciles",
                        lambda out: out.update(
                            reconciles=False,
                            reconcile_problems=["3 rows do not account for "
                                                "1 + 1 + 1 = 3"]) or False)

    out = cc.run(csuite=csuite, hubspot=hubspot, dry_run=False, limit=5,
                 event_ids=("1430",))

    assert hubspot.posted == [], "ZERO requests"
    assert out["created"] == 0
    assert out["writes_attempted"] == 0
    assert out["stopped"] and "do not reconcile" in out["stopped"]
    assert "not a reason to apply anyway" in out["stopped"]

    assert len(sent) == 1
    subject, body, context = sent[0]
    assert "contact creation stopped" in subject
    assert "reconcile" in body
    assert "path" in context and context["path"] == "create contacts"


def test_a_reconciled_plan_still_writes(monkeypatch):
    arm(monkeypatch)
    csuite = CSuite({"1430": [registrant(EMAIL, 21676)]},
                    profiles={21676: profile()})
    hubspot = HubSpot()

    out = cc.run(csuite=csuite, hubspot=hubspot, dry_run=False, limit=5,
                 event_ids=("1430",))

    assert out["reconciles"] is True
    assert out["created"] == 1
    assert len(hubspot.posted) == 1


def test_a_dry_run_is_unaffected_by_the_gate(monkeypatch):
    """The gate is on the WRITE. A preview still reports the problem."""
    monkeypatch.setattr(cc, "check_reconciles",
                        lambda out: out.update(
                            reconciles=False,
                            reconcile_problems=["bad"]) or False)
    csuite = CSuite({"1430": [registrant(EMAIL, 21676)]},
                    profiles={21676: profile()})

    out = cc.run(csuite=csuite, hubspot=HubSpot(), event_ids=("1430",))

    assert out["stopped"] is None, "a preview does not stop; it reports"
    assert out["reconciles"] is False
    assert "do not add up" in sync_commands._format_contact_results(out)


# ---------------------------------------------------------------------------
# CHANGE 3: batch-size guards
# ---------------------------------------------------------------------------

def test_too_large_a_create_batch_is_refused(monkeypatch, sent):
    arm(monkeypatch)
    monkeypatch.setattr(cc, "MAX_CREATE_BATCH", 2)
    rows = [registrant(f"p{i}@example.invalid", 21000 + i) for i in range(5)]
    csuite = CSuite({"1430": rows},
                    profiles={21000 + i: profile() for i in range(5)})
    hubspot = HubSpot()

    out = cc.run(csuite=csuite, hubspot=hubspot, dry_run=False, limit=5,
                 event_ids=("1430",))

    assert hubspot.posted == [], "ZERO requests"
    assert out["stopped"] and "would create 5" in out["stopped"]
    assert "maximum of 2" in out["stopped"]
    assert sent and "batch too large" in sent[0][1]


def test_the_typed_limit_still_caps_below_the_maximum(monkeypatch):
    """Both guards, and the tighter one wins."""
    arm(monkeypatch)
    monkeypatch.setattr(cc, "MAX_CREATE_BATCH", 25)
    rows = [registrant(f"p{i}@example.invalid", 21000 + i) for i in range(5)]
    csuite = CSuite({"1430": rows},
                    profiles={21000 + i: profile() for i in range(5)})
    hubspot = HubSpot()

    out = cc.run(csuite=csuite, hubspot=hubspot, dry_run=False, limit=2,
                 event_ids=("1430",))

    assert out["created"] == 2
    assert out["deferred"] == 3
    assert out["stopped"] is None


def test_the_create_cap_defaults_to_25_and_survives_a_bad_env(monkeypatch):
    assert cc.MAX_CREATE_BATCH == 25
    monkeypatch.setenv("MAX_CREATE_BATCH", "0")
    assert cc._int_from_env("MAX_CREATE_BATCH", 25) == 25
    monkeypatch.setenv("MAX_CREATE_BATCH", "not a number")
    assert cc._int_from_env("MAX_CREATE_BATCH", 25) == 25
    monkeypatch.setenv("MAX_CREATE_BATCH", "40")
    assert cc._int_from_env("MAX_CREATE_BATCH", 25) == 40


def test_a_first_run_event_over_the_ceiling_plans_nothing():
    """shrink_guard cannot cover known_count == 0 — there is no previous
    size to compare against."""
    rows = [registrant(f"p{i}@x.invalid", i) for i in range(60)]

    refusal = reg.first_run_guard("9999", rows, known_count=0)

    assert refusal
    assert "60 registrant rows" in refusal
    assert "never been synced" in refusal
    assert "ceiling for a first run is 50" in refusal


def test_a_first_run_event_under_the_ceiling_is_fine():
    rows = [registrant(f"p{i}@x.invalid", i) for i in range(41)]
    assert reg.first_run_guard("1157", rows, known_count=0) == ""


def test_an_event_with_history_is_left_to_shrink_guard():
    """known_count > 0 means shrink_guard owns it; the ceiling does not
    apply, or a legitimately large recurring event would be blocked."""
    rows = [registrant(f"p{i}@x.invalid", i) for i in range(500)]
    assert reg.first_run_guard("1157", rows, known_count=400) == ""


def test_the_ceiling_stops_the_event_being_planned():
    rows = [registrant(f"p{i}@x.invalid", i) for i in range(60)]

    plan = reg.plan_event("9999", rows, {}, {})

    assert plan["would_register"] == []
    assert plan["review"] and "never been synced" in plan["review"]


def test_both_registration_guards_coexist():
    """A sharp drop is still a sharp drop."""
    rows = [registrant("a@x.invalid", 1)]
    plan = reg.plan_event("1157", rows, {}, {
        reg.map_key("1157", f"p{i}@x.invalid"): {"status": "synced"}
        for i in range(40)})

    assert plan["review"], "shrink_guard still fires"
    assert "short read" in plan["review"]


def test_a_registrations_stop_notifies(monkeypatch, sent):
    from tests.test_registrations_preview import (CSuite as RegCSuite,
                                                  HubSpot as RegHubSpot,
                                                  registrant as reg_row)

    arm(monkeypatch)
    monkeypatch.setattr(reg, "migration_applied", lambda: True)
    monkeypatch.setattr(reg, "open_run", lambda applied: 77)
    monkeypatch.setattr(reg, "close_run", lambda *a, **k: None)
    monkeypatch.setattr(reg, "event_object_ids",
                        lambda h, ids: ({}, 1, None))

    out = reg.run(csuite=RegCSuite({"1430": [reg_row("a@x.inv")]},
                                   dates={"1430": "2025-01-01"}),
                  hubspot=RegHubSpot({"a@x.inv": ("701", True, [])}),
                  dry_run=False, limit=1, event_ids=("1430",),
                  now_ms=1760000000000)

    assert out["stopped"]
    assert sent, "a stopped registrations run must notify"
    subject, _body, context = sent[0]
    assert "registrations sync stopped" in subject
    assert context["run_log"] == 77


# ---------------------------------------------------------------------------
# CHANGE 4: the DB pool
# ---------------------------------------------------------------------------

def test_the_pool_is_at_least_threads_plus_one(monkeypatch):
    from clients import database

    monkeypatch.delenv("DB_MAX_CONNECTIONS", raising=False)
    monkeypatch.delenv("GUNICORN_THREADS", raising=False)
    assert database._max_connections() == 5, "4 threads + 1"

    monkeypatch.setenv("GUNICORN_THREADS", "8")
    assert database._max_connections() == 9

    monkeypatch.setenv("DB_MAX_CONNECTIONS", "12")
    assert database._max_connections() == 12, "an explicit value wins"


def test_a_bad_pool_setting_falls_back_rather_than_disabling(monkeypatch):
    from clients import database

    monkeypatch.delenv("DB_MAX_CONNECTIONS", raising=False)
    for bad in ("", "nope", "0", "-3"):
        monkeypatch.setenv("GUNICORN_THREADS", bad)
        assert database._max_connections() == 5, bad


def test_the_pool_matches_the_threads_the_deploy_actually_uses():
    """railway.toml and the pool must not disagree — maxconn 2 against
    --threads 4 is what made three concurrent requests fail."""
    import re

    from clients import database

    config = open("railway.toml").read()
    threads = int(re.search(r"--threads (\d+)", config).group(1))
    assert database.DEFAULT_GUNICORN_THREADS == threads
    assert database._max_connections() >= threads + 1


# ---------------------------------------------------------------------------
# CHANGE 5: category-based contacts-only
# ---------------------------------------------------------------------------

def test_the_newsletters_category_is_the_default():
    import config

    assert config.Config.CONTACTS_ONLY_EVENT_CATEGORIES == ("newsletters",)
    assert cc.contacts_only_categories() == ("newsletters",)


@pytest.mark.parametrize("category, expected", [
    ("Newsletters", True),
    ("newsletters", True),
    ("  NEWSLETTERS  ", True),
    ("Event - Other", False),
    ("Unassigned", False),
    ("", False),
    (None, False),
])
def test_the_category_decides(category, expected):
    assert cc.is_contacts_only_row({"event_name": category}) is expected


def test_the_category_reads_event_name_not_the_title():
    """event_description is the individual event's title; event_name is the
    parent series, which takes three values across all 179 dates."""
    assert cc.is_contacts_only_row(
        {"event_name": "Event - Other",
         "event_description": "Newsletters are great"}) is False
    assert cc.is_contacts_only_row(
        {"event_name": "Newsletters",
         "event_description": "AMCF Open House"}) is True


def test_the_event_sync_withholds_a_sign_up_list():
    """This is how csuite-1153's marketing event came to exist: the event
    planner discovers its own scope and had no category filter."""
    from sync import event_apply as ea

    reason = ea.contacts_only_reason({"event_name": "Newsletters",
                                      "event_date_id": 9001})

    assert reason
    assert "SIGN-UP LIST" in reason
    assert "No marketing event is created" in reason
    assert ea.contacts_only_reason({"event_name": "Event - Other"}) == ""


def test_the_event_sync_skips_it_in_the_plan():
    from sync import event_apply as ea

    rows = [{"event_date_id": 9001, "event_name": "Newsletters",
             "event_description": "A Campaign", "event_date": "2026-12-31"},
            {"event_date_id": 9002, "event_name": "Event - Other",
             "event_description": "A Real Event", "event_date": "2026-11-01"}]

    plan = ea.plan(rows, {}, {}, "AMCF")

    skipped_ids = [m.csuite_eventdate_id for m, _why in plan["skipped"]]
    assert "9001" in skipped_ids, "the sign-up list is withheld"
    assert "9002" not in skipped_ids, "the real event is not"
    reason = [why for m, why in plan["skipped"]
              if m.csuite_eventdate_id == "9001"][0]
    assert "SIGN-UP LIST" in reason


def test_a_new_sign_up_list_needs_no_list_editing():
    """The point of the category rule: an id nobody has added is still
    treated as contacts-only."""
    csuite = CSuite({"9001": [registrant(EMAIL, 21676)]},
                    profiles={21676: profile()})
    csuite.by_event["9001"] = [registrant(EMAIL, 21676)]

    out = {"csuite_calls": 0, "hubspot_calls": 0, "events_read": 0,
           "held_events": [], "contacts_only_events": [],
           "contacts_only_by_category": [], "excluded": 0,
           "excluded_reasons": {}, "registrant_rows": 0, "unique_emails": 0,
           "duplicates_dropped": 0, "already_contacts": 0, "marketing": 0,
           "non_marketing": 0, "marketing_unknown": 0,
           "non_marketing_rows": []}

    class Rows(CSuite):
        def _request(self, endpoint, data=None):
            response = super()._request(endpoint, data)
            if endpoint == "event/display/eventdate":
                response["data"]["event_name"] = "Newsletters"
            return response

    rows = Rows({"9001": [registrant(EMAIL, 21676)]},
                profiles={21676: profile()})
    cc.plan(rows, HubSpot(), ("9001",), out)

    assert out["contacts_only_by_category"] == ["9001"]
    assert out["contacts_only_events"] == ["9001"]


def test_the_explicit_id_list_still_overrides(monkeypatch):
    """1153 is on the id list AND matches the category. It must appear once,
    and the id list is consulted first."""
    monkeypatch.setattr("config.Config.CONTACTS_ONLY_EVENT_CATEGORIES", ())
    assert cc.is_contacts_only_row({"event_name": "Newsletters"}) is False
    assert "1153" in cc.contacts_only_event_ids()
    assert cc.resolve_requested_event("1153") == "1153"


def test_1153_behaviour_is_unchanged():
    """Still held for registrations, still allowed for contacts, still
    listed once."""
    with pytest.raises(reg.EventRefused):
        reg.resolve_requested_event("1153")
    assert cc.resolve_requested_event("1153") == "1153"
    assert "1153" in reg.held_event_ids()


def test_a_contacts_only_event_is_not_double_counted(monkeypatch):
    """On the id list and matching the category: one entry, not two."""
    class Rows(CSuite):
        def _request(self, endpoint, data=None):
            response = super()._request(endpoint, data)
            if endpoint == "event/display/eventdate":
                response["data"]["event_name"] = "Newsletters"
            return response

    csuite = Rows({"1153": [registrant(EMAIL, 21676)]},
                  profiles={21676: profile()})

    out = cc.run(csuite=csuite, hubspot=HubSpot(), event_ids=("1153",))

    assert out["contacts_only_events"] == ["1153"]
    assert out["contacts_only_by_category"] == []


# ---------------------------------------------------------------------------
# CHANGE 6, and the write path
# ---------------------------------------------------------------------------

def test_the_task_type_comment_matches_production():
    """Production task 1038 reads back task_type_id 1065."""
    config_text = open("config.py").read()

    assert "it IS confirmed in production" in config_text
    assert "task 1038" in config_text
    assert "NOT confirmed in production — there is no task-type list" \
        not in config_text


def test_the_create_body_is_byte_identical_to_v573(monkeypatch):
    """Nothing in this hotfix may change what is sent."""
    arm(monkeypatch)
    csuite = CSuite({"1430": [registrant(EMAIL, 21676)]},
                    profiles={21676: profile()})
    hubspot = HubSpot()

    cc.run(csuite=csuite, hubspot=hubspot, dry_run=False, limit=1,
           event_ids=("1430",))

    assert hubspot.posted == [(
        "POST", "crm/v3/objects/contacts",
        {"properties": {"email": EMAIL, "firstname": "New",
                        "lastname": "Person"}})]


def test_no_scheduler_was_added():
    """hotfix-59's job, explicitly out of scope here."""
    import subprocess

    requirements = open("requirements.txt").read().lower()
    for package in ("apscheduler", "celery", "rq=", "schedule="):
        assert package not in requirements, package

    found = subprocess.run(
        ["grep", "-rIl", "--include=*.py", "-e", "BackgroundScheduler",
         "-e", "add_job", "sync/", "jobs/", "clients/", "intents/"],
        capture_output=True, text=True)
    assert found.stdout.strip() == "", found.stdout


# ---------------------------------------------------------------------------
# The email channel: subjects that can be filtered, bodies that read plain
# ---------------------------------------------------------------------------
#
# Carl's choice: email only for now, SMTP via a Workspace app password.
# Slack stays supported and unset. The sender itself is stdlib smtplib and
# shipped in V5.74; what this covers is the subject prefixes and the plain
# text body.
#
# The prefix is applied in notify(), not at the call sites: a prefix a
# caller has to remember is one some caller will forget, and the one it
# forgets is the alert nobody's filter catches.


def test_every_failure_subject_is_prefixed_as_an_alert():
    assert notifier.subject_line("contact creation stopped") == \
        "[Jidhr ALERT] contact creation stopped"
    assert notifier.ALERT_PREFIX == "[Jidhr ALERT]"


def test_the_test_command_subject_is_prefixed_as_a_test():
    assert notifier.subject_line("test notification", notifier.KIND_TEST) == \
        "[Jidhr TEST] test notification"
    assert notifier.TEST_PREFIX == "[Jidhr TEST]"


def test_both_prefixes_share_a_searchable_stem():
    """One Gmail filter, subject:([Jidhr ), has to catch both."""
    for prefix in (notifier.ALERT_PREFIX, notifier.TEST_PREFIX):
        assert prefix.startswith("[Jidhr ")


def test_prefixing_is_idempotent():
    """A subject passed through twice must not double up."""
    once = notifier.subject_line("stopped")
    assert notifier.subject_line(once) == once
    assert notifier.subject_line(once, notifier.KIND_TEST) == \
        "[Jidhr TEST] stopped"


def test_an_empty_subject_is_still_prefixed():
    assert notifier.subject_line("") == "[Jidhr ALERT]"
    assert notifier.subject_line(None) == "[Jidhr ALERT]"


def _capture(monkeypatch):
    """Capture the SMTP message a notification would send."""
    monkeypatch.setenv("NOTIFY_EMAIL_TO", "carl@amuslimcf.org")
    monkeypatch.setenv("NOTIFY_SMTP_HOST", "smtp.gmail.com")
    monkeypatch.setenv("NOTIFY_SMTP_USER", "jidhr@amuslimcf.org")
    monkeypatch.setenv("NOTIFY_SMTP_PASSWORD", "app-password")
    monkeypatch.setenv("NOTIFY_EMAIL_FROM", "jidhr@amuslimcf.org")
    sent = {}

    class SMTP:
        def __init__(self, host, port, timeout=None):
            sent.update(host=host, port=port, timeout=timeout)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def starttls(self):
            sent["starttls"] = True

        def login(self, user, password):
            sent["login"] = user

        def send_message(self, message):
            sent["message"] = message

    monkeypatch.setattr("smtplib.SMTP", SMTP)
    return sent


def test_the_email_carries_the_prefixed_subject(monkeypatch):
    sent = _capture(monkeypatch)

    result = notifier.safe_notify("contact creation stopped",
                                  "counts did not reconcile",
                                  run_log=62, path="create contacts",
                                  events="1430", to_create=5)

    assert result["sent"] == ["email"]
    assert sent["message"]["Subject"] == \
        "[Jidhr ALERT] contact creation stopped"
    assert sent["message"]["To"] == "carl@amuslimcf.org"
    assert sent["message"]["From"] == "jidhr@amuslimcf.org"
    assert sent["host"] == "smtp.gmail.com"
    assert sent["port"] == 587, "STARTTLS submission, not 25"
    assert sent["starttls"] is True
    assert sent["timeout"] == notifier.TIMEOUT_SECONDS


def test_the_email_body_is_plain_text_with_the_facts(monkeypatch):
    sent = _capture(monkeypatch)

    notifier.safe_notify("registrations sync stopped",
                         "the write for event 1430 failed (HTTP 400)",
                         run_log=77, path="sync registrations",
                         events="1430", registered=3, failed=1,
                         write_audit="201, 202")

    message = sent["message"]
    assert message.get_content_type() == "text/plain"
    body = message.get_content()

    # The reason, then the facts, one per line.
    assert "the write for event 1430 failed (HTTP 400)" in body
    for line in ("run_log: 77", "path: sync registrations", "events: 1430",
                 "registered: 3", "failed: 1", "write_audit: 201, 202"):
        assert line in body, line

    # No markup: the Slack asterisks used to arrive as literal characters.
    assert "*" not in body
    assert "•" not in body


def test_the_email_body_never_carries_an_address(monkeypatch):
    sent = _capture(monkeypatch)

    notifier.safe_notify("contact creation stopped",
                         "donor@private.invalid could not be created",
                         masked="d***@private.invalid", run_log=1)

    body = sent["message"].get_content()
    assert "donor@private.invalid" not in body
    assert "<address redacted>" in body
    assert "d***@private.invalid" in body, "the masked form survives"
    assert sent["message"]["To"] == "carl@amuslimcf.org", \
        "the recipient is not redacted out of the envelope"


def test_slack_stays_supported_but_is_not_required(monkeypatch):
    """Email only for now. Slack must not become mandatory."""
    sent = _capture(monkeypatch)
    monkeypatch.delenv("NOTIFY_SLACK_WEBHOOK_URL", raising=False)

    result = notifier.safe_notify("stopped", "why", run_log=1)

    assert result["sent"] == ["email"]
    assert result["failed"] == []
    assert "message" in sent


def test_an_smtp_failure_still_cannot_fail_a_run(monkeypatch):
    monkeypatch.setenv("NOTIFY_EMAIL_TO", "carl@amuslimcf.org")
    monkeypatch.setenv("NOTIFY_SMTP_HOST", "smtp.gmail.com")

    class SMTP:
        def __init__(self, *a, **k):
            raise OSError("connection refused")

    monkeypatch.setattr("smtplib.SMTP", SMTP)

    result = notifier.safe_notify("stopped", "why")

    assert result["failed"] == ["email"]
    assert result["sent"] == []


def test_a_guard_sends_an_alert_subject_end_to_end(monkeypatch):
    """The whole path: a refused batch produces a filterable email."""
    sent = _capture(monkeypatch)
    arm(monkeypatch)
    monkeypatch.setattr(cc, "MAX_CREATE_BATCH", 1)
    rows = [registrant(f"p{i}@example.invalid", 21000 + i) for i in range(3)]
    csuite = CSuite({"1430": rows},
                    profiles={21000 + i: profile() for i in range(3)})
    hubspot = HubSpot()

    out = cc.run(csuite=csuite, hubspot=hubspot, dry_run=False, limit=3,
                 event_ids=("1430",))

    assert hubspot.posted == []
    assert out["stopped"]
    subject = sent["message"]["Subject"]
    assert subject.startswith("[Jidhr ALERT]")
    body = sent["message"].get_content()
    # The notification carries the guard's own reason; the chat report
    # carries the longer "would create N" wording.
    assert "would write 3, max 1" in body
    assert "would create 3" in out["stopped"]
    assert "path: create contacts" in body
    assert "to_create: 3" in body


def test_the_test_command_names_the_subject_to_look_for(monkeypatch):
    monkeypatch.setattr(notifier, "configured", lambda: True)
    monkeypatch.setattr(notifier, "safe_notify",
                        lambda *a, **k: {"configured": True,
                                         "sent": ["email"], "failed": []})

    reply = sync_commands.handle("test notify", None)

    assert "[Jidhr TEST] test notification" in reply
    assert "[Jidhr ALERT]" in reply
    assert "subject:([Jidhr )" in reply
    assert "check spam" in reply


def test_the_test_command_asks_for_the_test_kind(monkeypatch):
    """It must not send an ALERT-prefixed test — that would train the eye
    to ignore the prefix that matters."""
    seen = {}
    monkeypatch.setattr(notifier, "configured", lambda: True)
    monkeypatch.setattr(notifier, "safe_notify",
                        lambda subject, body, **k: seen.update(
                            subject=subject, kind=k.get("kind"))
                        or {"configured": True, "sent": ["email"],
                            "failed": []})

    sync_commands.handle("test notify", None)

    assert seen["kind"] == notifier.KIND_TEST


def test_no_call_site_prefixes_its_own_subject():
    """The prefix belongs to notify(). A call site that adds its own would
    survive the idempotency guard but duplicate the intent."""
    import pathlib

    for path in ("sync/contact_create.py", "sync/registrations.py",
                 "sync/registration_jobs.py", "sync/contact_jobs.py",
                 "intents/sync_commands.py"):
        source = pathlib.Path(path).read_text()
        for literal in ('"[Jidhr ALERT]', "'[Jidhr ALERT]"):
            assert literal not in source, f"{path} hardcodes a prefix"


# ---------------------------------------------------------------------------
# The SMTP port, and settings a human can read
# ---------------------------------------------------------------------------
#
# Syntax asked whether NOTIFY_SMTP_HOST and NOTIFY_SMTP_PORT were the real
# names. They are — clients/notify.py reads them at lines 181 and 193 — and
# these tests pin them so the answer cannot drift.
#
# Reading that code turned up a trap: smtplib.SMTP + starttls() is correct
# for 587 and WRONG for 465, which needs SMTP_SSL. The pairing failure does
# not raise anything readable, and this module swallows its own errors by
# design, so the symptom would have been an email that never arrived with
# no clue why.


def test_the_env_var_names_are_what_was_documented(monkeypatch):
    """Pinned, because they were handed to someone to type into Railway."""
    import pathlib

    source = pathlib.Path("clients/notify.py").read_text()
    for name in ("NOTIFY_EMAIL_TO", "NOTIFY_SMTP_HOST", "NOTIFY_SMTP_PORT",
                 "NOTIFY_SMTP_USER", "NOTIFY_SMTP_PASSWORD",
                 "NOTIFY_EMAIL_FROM", "NOTIFY_SLACK_WEBHOOK_URL"):
        assert f'"{name}"' in source, name

    reported = [name for name, _set, _shown in notifier.settings()]
    assert reported == ["NOTIFY_EMAIL_TO", "NOTIFY_SMTP_HOST",
                        "NOTIFY_SMTP_PORT", "NOTIFY_SMTP_USER",
                        "NOTIFY_SMTP_PASSWORD", "NOTIFY_EMAIL_FROM",
                        "NOTIFY_SLACK_WEBHOOK_URL"]


def test_the_port_defaults_to_587(monkeypatch):
    monkeypatch.delenv("NOTIFY_SMTP_PORT", raising=False)
    assert notifier.smtp_port() == 587
    assert notifier.DEFAULT_SMTP_PORT == 587


@pytest.mark.parametrize("raw", ["", "   ", "nope", "0", "-1"])
def test_an_unusable_port_falls_back_rather_than_raising(monkeypatch, raw):
    """The caller is an error path. Failing to report a failure is worse
    than reporting it over the default port."""
    monkeypatch.setenv("NOTIFY_SMTP_PORT", raw)
    assert notifier.smtp_port() == 587


def test_587_uses_starttls(monkeypatch):
    monkeypatch.setenv("NOTIFY_EMAIL_TO", "carl@amuslimcf.org")
    monkeypatch.setenv("NOTIFY_SMTP_HOST", "smtp.gmail.com")
    monkeypatch.setenv("NOTIFY_SMTP_PORT", "587")
    calls = []

    class SMTP:
        def __init__(self, host, port, timeout=None):
            calls.append(("SMTP", host, port))

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def starttls(self):
            calls.append(("starttls",))

        def login(self, u, p):
            pass

        def send_message(self, m):
            calls.append(("sent",))

    monkeypatch.setattr("smtplib.SMTP", SMTP)

    assert notifier.safe_notify("s", "b")["sent"] == ["email"]
    assert calls[0] == ("SMTP", "smtp.gmail.com", 587)
    assert ("starttls",) in calls


def test_465_uses_implicit_tls_and_never_calls_starttls(monkeypatch):
    """The trap: SMTP + starttls on 465 hangs, and the swallowed error
    would have made it undiagnosable."""
    monkeypatch.setenv("NOTIFY_EMAIL_TO", "carl@amuslimcf.org")
    monkeypatch.setenv("NOTIFY_SMTP_HOST", "smtp.gmail.com")
    monkeypatch.setenv("NOTIFY_SMTP_PORT", "465")
    calls = []

    class SMTPSSL:
        def __init__(self, host, port, timeout=None):
            calls.append(("SMTP_SSL", host, port))

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def starttls(self):
            raise AssertionError("starttls must not be called on 465")

        def login(self, u, p):
            pass

        def send_message(self, m):
            calls.append(("sent",))

    class SMTP:
        def __init__(self, *a, **k):
            raise AssertionError("465 must not use plain SMTP")

    monkeypatch.setattr("smtplib.SMTP_SSL", SMTPSSL)
    monkeypatch.setattr("smtplib.SMTP", SMTP)

    assert notifier.safe_notify("s", "b")["sent"] == ["email"]
    assert calls[0] == ("SMTP_SSL", "smtp.gmail.com", 465)
    assert ("sent",) in calls


def test_465_is_the_only_implicit_tls_port():
    assert notifier.IMPLICIT_TLS_PORTS == (465,)
    assert notifier.DEFAULT_SMTP_PORT not in notifier.IMPLICIT_TLS_PORTS


def test_the_settings_report_never_shows_the_password(monkeypatch):
    monkeypatch.setenv("NOTIFY_SMTP_PASSWORD", "abcd efgh ijkl mnop")
    monkeypatch.setenv("NOTIFY_SLACK_WEBHOOK_URL", "https://hooks/secret")

    report = dict((name, shown) for name, _set, shown
                  in notifier.settings())

    assert report["NOTIFY_SMTP_PASSWORD"] == "set (not shown)"
    assert "abcd" not in str(report)
    assert "secret" not in str(report)


def test_the_settings_report_shows_non_secret_values(monkeypatch):
    monkeypatch.setenv("NOTIFY_EMAIL_TO", "carl@amuslimcf.org")
    monkeypatch.setenv("NOTIFY_SMTP_HOST", "smtp.gmail.com")

    report = dict((name, shown) for name, _set, shown
                  in notifier.settings())

    assert report["NOTIFY_EMAIL_TO"] == "carl@amuslimcf.org"
    assert report["NOTIFY_SMTP_HOST"] == "smtp.gmail.com"


def test_test_notify_prints_the_settings_either_way(monkeypatch):
    off = sync_commands.handle("test notify", None)
    assert "NOTIFY_SMTP_HOST" in off
    assert "defaults to 587" in off
    assert "STARTTLS" in off

    monkeypatch.setattr(notifier, "configured", lambda: True)
    monkeypatch.setattr(notifier, "safe_notify",
                        lambda *a, **k: {"configured": True,
                                         "sent": ["email"], "failed": []})
    on = sync_commands.handle("test notify", None)
    assert "NOTIFY_SMTP_HOST" in on
    assert "check spam first" in on


def test_the_settings_table_names_the_tls_mode_for_the_configured_port(
        monkeypatch):
    monkeypatch.setenv("NOTIFY_SMTP_PORT", "465")
    reply = sync_commands.handle("test notify", None)
    assert "Port **465** uses implicit TLS" in reply

    monkeypatch.setenv("NOTIFY_SMTP_PORT", "587")
    reply = sync_commands.handle("test notify", None)
    assert "Port **587** uses STARTTLS" in reply
