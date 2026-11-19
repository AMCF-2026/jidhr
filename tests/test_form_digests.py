"""V5.96: daily digests for the D forms. Every submission here is a TEST
DOUBLE with invented values; sentinel strings stand in for sensitive data.
No network, no database."""

from datetime import date, datetime, timedelta, timezone

import pytest

from config import Config
from sync import form_digests as fd
from sync import form_registry as registry
from sync import forms_sync

NOW = datetime(2026, 10, 28, 10, 0, tzinfo=timezone.utc)
START = NOW - timedelta(hours=24)


def ms(when):
    return int(when.timestamp() * 1000)


def sub(conv, when, **values):
    base = {"firstname": "Aisha", "lastname": "Khan",
            "email": "aisha@example.invalid"}
    base.update(values)
    return {"conversionId": conv, "submittedAt": ms(when),
            "values": [{"name": k, "value": v} for k, v in base.items()]}


def on(mode="preview", since=date(2026, 10, 1)):
    return {"on": True, "mode": mode, "since": since}


SECRET = {
    "investment": {"how_much_are_you_putting_in_to_this_investment_cycle_":
                   "SENTINEL-AMOUNT-777"},
    "endowment_distribution": {
        "endowment_distribution_amount2": "SENTINEL-AMOUNT-888",
        "endowment_distribution_reason": "SENTINEL-REASON",
        "address": "SENTINEL-STREET", "zip": "SENTINEL-ZIP"},
    "asset_donation": {"number_of_shares": "SENTINEL-SHARES",
                       "what_stocksmutual_funds_are_you_donating":
                           "SENTINEL-TICKER",
                       "would_you_like_us_to_set_a_limit_order_or_strike_"
                       "price_if_yes_at_what_amount": "SENTINEL-LIMIT"},
    "nonprofit_directory": {"organization_ein": "SENTINEL-EIN-12",
                            "fiscal_sponsor_ein": "SENTINEL-EIN-34",
                            "organization_mission_statement":
                                "SENTINEL-MISSION"},
}


# --- the whitelist ----------------------------------------------------------------

def test_no_sensitive_out_field_is_ever_on_a_digest_list():
    for form in registry.FORMS:
        shown = {f for f, _l, _t in form["digest"]}
        out = {f for f, how in form["sensitive"].items()
               if how == registry.OUT}
        assert not shown & out, form["key"]
        for field, _label, how in form["digest"]:
            if field in form["sensitive"]:
                assert how == form["sensitive"][field], (form["key"], field)


def test_only_d_forms_have_digests_and_ach_has_none():
    assert {f["key"] for f in registry.FORMS if f["digest"]} == {
        "asset_donation", "endowment_distribution", "investment",
        "nonprofit_directory"}
    assert registry.BY_KEY["ach_request"]["digest"] == ()


@pytest.mark.parametrize("key", sorted(SECRET))
def test_planted_sensitive_values_never_reach_the_email(key):
    form = registry.BY_KEY[key]
    s = sub("abcd1234-x", NOW - timedelta(hours=2), **SECRET[key],
            daf_endowment_account_number="4400123456")
    digest = fd.build(form, [s], on(), START, NOW)
    text = digest["text"] + digest["subject"]
    for value in SECRET[key].values():
        assert value not in text, (key, value)
    assert "aisha@example.invalid" not in text, "the email is masked"
    assert "a***@example.invalid" in text
    if key in ("investment", "endowment_distribution"):
        assert "4400123456" not in text and "••••3456" in text
        assert "Amount: given" in text


def test_last4_masks_numbers_and_shows_names():
    assert fd.last4("4400 1234 56") == "••••3456"
    assert fd.last4("DAF-778899") == "••••8899"
    assert fd.last4("Azhar Zakat Fund") == "Azhar Zakat Fund"


# --- what goes in, and when ---------------------------------------------------------

def test_only_submissions_in_the_window_and_after_the_since_date():
    form = registry.BY_KEY["nonprofit_directory"]
    subs = [sub("new00001-x", NOW - timedelta(hours=3), company="New Org"),
            sub("old00001-x", NOW - timedelta(days=3), company="Old Org")]
    digest = fd.build(form, subs, on(), START, NOW)
    assert digest["count"] == 1 and "New Org" in digest["text"]
    assert "Old Org" not in digest["text"]
    late = fd.build(form, subs, on(since=date(2026, 10, 29)), START, NOW)
    assert late is None, "nothing before the since-date"


def test_the_window_runs_from_the_last_success_capped_at_seven_days():
    assert fd.window_start(NOW, None) == NOW - timedelta(hours=24)
    two = NOW - timedelta(days=2)
    assert fd.window_start(NOW, two) == two
    assert fd.window_start(NOW, NOW - timedelta(days=30)) == \
        NOW - timedelta(days=7)


def test_investment_is_grouped_by_cycle():
    form = registry.BY_KEY["investment"]
    subs = [sub("a0000001-x", NOW - timedelta(hours=5),
                investment_cycle_selection="January 2027"),
            sub("a0000002-x", NOW - timedelta(hours=4),
                investment_cycle_selection="April 2027"),
            sub("a0000003-x", NOW - timedelta(hours=3),
                investment_cycle_selection="January 2027")]
    text = fd.build(form, subs, on(), START, NOW)["text"]
    assert "January 2027 (2)" in text and "April 2027 (1)" in text


def test_no_email_when_nothing_is_new():
    form = registry.BY_KEY["investment"]
    assert fd.build(form, [], on(), START, NOW) is None


def test_build_all_skips_forms_that_are_off_or_undated_or_ach():
    raw = {k: [sub("n0000001-x", NOW - timedelta(hours=1))]
           for k in ("investment", "asset_donation", "ach_request",
                     "nonprofit_directory")}
    switches = {f["key"]: {"on": False, "mode": "preview", "since": None}
                for f in registry.FORMS}
    switches["investment"] = on()
    switches["asset_donation"] = {"on": True, "mode": "preview",
                                  "since": None}            # no since-date
    switches["ach_request"] = on()                          # locked anyway
    assert [d["key"] for d in fd.build_all(raw, switches, NOW, START)] == \
        ["investment"]


# --- who gets it ---------------------------------------------------------------------

def test_preview_goes_to_the_report_recipients_only(monkeypatch):
    monkeypatch.setattr(Config, "JIDHR_FORMS_REPORT_TO",
                        ("carl@example.invalid",), raising=False)
    form = registry.BY_KEY["investment"]
    to, problem = fd.recipients(form, on(), {"JIDHR_OWNER_EMAIL_KODS": "k@x"})
    assert to == ("carl@example.invalid",) and problem is None
    digest = fd.build(form, [sub("a0000001-x", NOW)], on(), START, NOW)
    assert digest["subject"].startswith("[Preview] Investment Form — 1 new")
    assert "PREVIEW, sent to Carl only" in digest["text"]


def test_live_goes_to_every_owner():
    form = registry.BY_KEY["investment"]
    env = {"JIDHR_OWNER_EMAIL_KODS": "kods@example.invalid",
           "JIDHR_OWNER_EMAIL_NORA": "nora@example.invalid"}
    to, problem = fd.recipients(form, on("live"), env)
    assert to == ("kods@example.invalid", "nora@example.invalid")
    assert problem is None


def test_a_live_form_with_a_missing_owner_address_refuses():
    env = {"JIDHR_FORM_INVESTMENT_ON": "true",
           "JIDHR_FORM_INVESTMENT_MODE": "live",
           "JIDHR_FORM_INVESTMENT_SINCE": "2026-11-01",
           "JIDHR_OWNER_EMAIL_KODS": "kods@example.invalid"}
    with pytest.raises(forms_sync.BadSetting) as caught:
        forms_sync.read_switches(env)
    assert "JIDHR_OWNER_EMAIL_NORA" in str(caught.value)


def test_send_logs_counts_only(monkeypatch, caplog):
    import logging

    import clients.notify as notify
    sent = []
    monkeypatch.setattr(Config, "JIDHR_FORMS_REPORT_TO",
                        ("carl@example.invalid",), raising=False)
    monkeypatch.setattr(notify, "send_report",
                        lambda to, subj, body: sent.append((to, subj)) or
                        {"sent": list(to), "failed": []})
    form = registry.BY_KEY["nonprofit_directory"]
    d = fd.build(form, [sub("a0000001-x", NOW, company="Private Org")],
                 on(), START, NOW)
    with caplog.at_level(logging.INFO, logger="sync.form_digests"):
        result = fd.send([d], {"nonprofit_directory": on()}, {})
    assert result == [("nonprofit_directory", ["carl@example.invalid"], [])]
    logged = " ".join(r.getMessage() for r in caplog.records)
    assert "Private Org" not in logged and "Aisha" not in logged


def test_the_raw_submissions_never_reach_the_job_row(monkeypatch):
    from jobs.handlers import forms_sync as handler

    for holder in {Config, __import__("config").Config}:
        monkeypatch.setattr(holder, "SCHEDULED_SYNC_ENABLED", True,
                            raising=False)
        monkeypatch.setattr(holder, "JIDHR_FORMS_SYNC_ENABLED", True,
                            raising=False)
        monkeypatch.setattr(holder, "JIDHR_FORMS_REPORT_TO", (),
                            raising=False)
    out = {"mode": "observe", "run_at": NOW.isoformat(), "forms": [],
           "unknown": [], "not_listed": [], "read_errors": [],
           "_raw": {"investment": [sub("a0000001-x", NOW,
                                       firstname="Privatename")]}}
    monkeypatch.setattr(forms_sync, "run", lambda: out)
    # No earlier runs. Patched at the database, not at last_success, whose
    # signature changed when the window became per form.
    import clients.database as database
    monkeypatch.setattr(database, "execute_query",
                        lambda sql, params=None, fetch=True: [])
    result = handler.handle({"id": 9})
    assert "Privatename" not in repr(result)
    assert "_raw" not in result and "digests" in result


# --- JIDHR_FORMS_LIVE_CC ---------------------------------------------------------
#
# Pinned against main (1c2616d): with the variable unset, send() calls
# send_report(to, subject, text) with exactly those three arguments, and
# send_report sends one message per address with no Cc line. Set, live
# digests gain a Cc; preview never does.

OWNERS_ENV = {"JIDHR_OWNER_EMAIL_KODS": "kods@example.invalid",
              "JIDHR_OWNER_EMAIL_NORA": "nora@example.invalid",
              "JIDHR_OWNER_EMAIL_MUNIRA": "munira@example.invalid"}


def _record_send_report(monkeypatch):
    import clients.notify as notify
    calls = []

    def fake(*args, **kwargs):
        calls.append((args, kwargs))
        to = list(args[0]) + list(kwargs.get("cc") or ())
        return {"sent": to, "failed": []}

    monkeypatch.setattr(notify, "send_report", fake)
    return calls


def _digest(key, mode):
    form = registry.BY_KEY[key]
    return fd.build(form, [sub("a0000001-x", NOW)], on(mode), START, NOW)


@pytest.mark.parametrize("env_cc", [None, "", " , ,"])
def test_cc_unset_sends_exactly_as_main(monkeypatch, env_cc):
    monkeypatch.setattr(Config, "JIDHR_FORMS_REPORT_TO",
                        ("carl@example.invalid",), raising=False)
    calls = _record_send_report(monkeypatch)
    env = dict(OWNERS_ENV)
    if env_cc is not None:
        env["JIDHR_FORMS_LIVE_CC"] = env_cc
    live = _digest("investment", "live")
    preview = _digest("nonprofit_directory", "preview")
    result = fd.send([live, preview], {"investment": on("live"),
                                       "nonprofit_directory": on()}, env)
    assert calls == [
        ((("kods@example.invalid", "nora@example.invalid"),
          live["subject"], live["text"]), {}),
        ((("carl@example.invalid",), preview["subject"], preview["text"]),
         {}),
    ]
    assert result == [
        ("investment", ["kods@example.invalid", "nora@example.invalid"], []),
        ("nonprofit_directory", ["carl@example.invalid"], []),
    ]


def test_cc_set_is_added_to_live_digests_only(monkeypatch):
    monkeypatch.setattr(Config, "JIDHR_FORMS_REPORT_TO",
                        ("carl@example.invalid",), raising=False)
    calls = _record_send_report(monkeypatch)
    env = dict(OWNERS_ENV, JIDHR_FORMS_LIVE_CC=(
        " carl@example.invalid, ops@example.invalid ,,CARL@example.invalid"))
    live = _digest("nonprofit_directory", "live")
    preview = _digest("asset_donation", "preview")
    fd.send([live, preview], {"nonprofit_directory": on("live"),
                              "asset_donation": on()}, env)
    assert calls == [
        ((("munira@example.invalid",), live["subject"], live["text"]),
         {"cc": ("carl@example.invalid", "ops@example.invalid")}),
        ((("carl@example.invalid",), preview["subject"], preview["text"]),
         {}),
    ]
    # The To line is the owners, whatever the CC says.
    to, problem = fd.recipients(registry.BY_KEY["nonprofit_directory"],
                                on("live"), env)
    assert to == ("munira@example.invalid",) and problem is None
    # The subject and text do not change with the CC.
    assert live == _digest("nonprofit_directory", "live")


class _FakeSMTP:
    sent = []

    def __init__(self, host, port, timeout=None):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def starttls(self):
        pass

    def login(self, user, password):
        pass

    def send_message(self, message):
        _FakeSMTP.sent.append(message)


@pytest.fixture
def smtp(monkeypatch):
    import smtplib
    _FakeSMTP.sent = []
    monkeypatch.setattr(smtplib, "SMTP", _FakeSMTP)
    monkeypatch.setenv("NOTIFY_SMTP_HOST", "smtp.example.invalid")
    for name in ("NOTIFY_SMTP_PORT", "NOTIFY_SMTP_USER",
                 "NOTIFY_SMTP_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("NOTIFY_EMAIL_FROM", "jidhr@example.invalid")
    return _FakeSMTP.sent


def test_send_report_without_cc_is_one_message_per_address(smtp):
    from clients.notify import send_report

    out = send_report(["kods@example.invalid", "nora@example.invalid"],
                      "Subject", "Body")
    assert out == {"sent": ["kods@example.invalid", "nora@example.invalid"],
                   "failed": []}
    assert [m["To"] for m in smtp] == ["kods@example.invalid",
                                       "nora@example.invalid"]
    assert all(m["Cc"] is None for m in smtp)
    assert all(m["From"] == "jidhr@example.invalid" for m in smtp)


def test_send_report_with_cc_is_one_message_with_a_cc_line(smtp):
    from clients.notify import send_report

    out = send_report(["kods@example.invalid", "nora@example.invalid"],
                      "Subject", "Body",
                      cc=("carl@example.invalid", "NORA@example.invalid"))
    assert len(smtp) == 1
    message = smtp[0]
    assert message["To"] == "kods@example.invalid, nora@example.invalid"
    assert message["Cc"] == "carl@example.invalid"
    assert message["Subject"] == "Subject"
    assert out == {"sent": ["kods@example.invalid", "nora@example.invalid",
                            "carl@example.invalid"], "failed": []}


# --- a failed digest raises one alert per form -------------------------------------
#
# Pinned against 69aa62a: a clean send calls nothing new and returns the same
# results. A failure (any recipient, live or preview, or no recipients at
# all) adds exactly one safe_notify per form, carrying counts only.

def _record_alerts(monkeypatch):
    import clients.notify as notify
    alerts = []
    monkeypatch.setattr(notify, "safe_notify",
                        lambda subject, body, kind=None, **context:
                        alerts.append((subject, body, kind, context)))
    return alerts


def _send_report_failing(monkeypatch, failing):
    import clients.notify as notify

    def fake(to, subject, text, cc=()):
        everyone = list(to) + list(cc)
        return {"sent": [a for a in everyone if a not in failing],
                "failed": [a for a in everyone if a in failing]}

    monkeypatch.setattr(notify, "send_report", fake)


def _private_sub(conv):
    return sub(conv, NOW, firstname="Zainab", lastname="Qureshi",
               email="zainab.private@example.invalid", company="Private Org")


def test_a_clean_send_raises_no_alert(monkeypatch):
    monkeypatch.setattr(Config, "JIDHR_FORMS_REPORT_TO",
                        ("carl@example.invalid",), raising=False)
    alerts = _record_alerts(monkeypatch)
    _send_report_failing(monkeypatch, failing=())
    live = _digest("investment", "live")
    preview = _digest("nonprofit_directory", "preview")
    result = fd.send([live, preview], {"investment": on("live"),
                                       "nonprofit_directory": on()},
                     dict(OWNERS_ENV, JIDHR_FORMS_LIVE_CC="ops@example.invalid"))
    assert alerts == []
    assert result == [
        ("investment", ["kods@example.invalid", "nora@example.invalid",
                        "ops@example.invalid"], []),
        ("nonprofit_directory", ["carl@example.invalid"], []),
    ]


def test_a_failed_send_raises_exactly_one_alert_per_form(monkeypatch):
    from clients.notify import KIND_ALERT

    monkeypatch.setattr(Config, "JIDHR_FORMS_REPORT_TO",
                        ("carl@example.invalid",), raising=False)
    alerts = _record_alerts(monkeypatch)
    # Investment live: one of two owners fails. Asset Donation preview: its
    # only recipient fails. Nonprofit Directory live: clean.
    _send_report_failing(monkeypatch, failing=("nora@example.invalid",
                                               "carl@example.invalid"))
    form = registry.BY_KEY
    inv = fd.build(form["investment"], [_private_sub("a1"), _private_sub("a2")],
                   on("live"), START, NOW)
    asset = fd.build(form["asset_donation"], [_private_sub("b1")], on(),
                     START, NOW)
    npd = fd.build(form["nonprofit_directory"], [_private_sub("c1")],
                   on("live"), START, NOW)
    fd.send([inv, asset, npd], {"investment": on("live"),
                                "asset_donation": on(),
                                "nonprofit_directory": on("live")},
            OWNERS_ENV)

    assert len(alerts) == 2
    (s1, b1, k1, c1), (s2, b2, k2, c2) = alerts
    assert k1 == k2 == KIND_ALERT
    assert "Investment Form" in s1 and "Investment Form" in b1
    assert c1 == {"form": "investment", "mode": "live", "submissions": 2,
                  "failed_recipients": 1}
    assert "2 submissions" in b1 and "1 of 2 recipients" in b1
    assert "AMCF Asset Donation Transfer Form" in s2
    assert c2 == {"form": "asset_donation", "mode": "preview",
                  "submissions": 1, "failed_recipients": 1}
    for alert in alerts:
        text = repr(alert)
        for private in ("Zainab", "Qureshi", "zainab", "Private Org",
                        "@example.invalid"):
            assert private not in text, private


def test_a_digest_with_no_recipients_raises_one_alert(monkeypatch):
    monkeypatch.setattr(Config, "JIDHR_FORMS_REPORT_TO", (), raising=False)
    alerts = _record_alerts(monkeypatch)
    _send_report_failing(monkeypatch, failing=())
    d = _digest("nonprofit_directory", "preview")
    result = fd.send([d], {"nonprofit_directory": on()}, {})
    assert result == [("nonprofit_directory", [], ["no recipients"])]
    assert len(alerts) == 1
    assert "no recipients" in alerts[0][1]
    assert alerts[0][3]["failed_recipients"] == 0


def test_the_alert_is_a_real_jidhr_alert(monkeypatch):
    """Through the real safe_notify: the subject carries the prefix."""
    import clients.notify as notify
    seen = []
    monkeypatch.setattr(notify, "_targets",
                        lambda: ("https://hooks.example.invalid/x", None))
    monkeypatch.setattr(notify, "_send_slack",
                        lambda url, text: seen.append(text) or True)
    monkeypatch.setattr(Config, "JIDHR_FORMS_REPORT_TO",
                        ("carl@example.invalid",), raising=False)
    _send_report_failing(monkeypatch, failing=("carl@example.invalid",))
    d = _digest("nonprofit_directory", "preview")
    fd.send([d], {"nonprofit_directory": on()}, {})
    assert len(seen) == 1
    assert seen[0].startswith("[Jidhr ALERT] Nonprofit Directory Application")
    assert "carl@example.invalid" not in seen[0]
