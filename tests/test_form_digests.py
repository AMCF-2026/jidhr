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
    monkeypatch.setattr(fd, "last_success", lambda job_id: None)
    result = handler.handle({"id": 9})
    assert "Privatename" not in repr(result)
    assert "_raw" not in result and "digests" in result
