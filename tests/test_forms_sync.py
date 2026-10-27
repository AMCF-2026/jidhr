"""V5.94: the form registry and the nightly forms report in observe mode.

Every HubSpot object here is a TEST DOUBLE with invented values. No
network, no database.
"""

import ast
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from config import Config
from sync import form_registry as registry
from sync import forms_sync

NOW = datetime(2026, 10, 28, 6, 0, tzinfo=timezone.utc)
REPO = Path(__file__).resolve().parent.parent


def ms(when):
    return int(when.timestamp() * 1000)


# --- the registry ---------------------------------------------------------------

def test_every_form_hubspot_listed_on_2026_10_27_is_registered():
    """The Step 0 discovery found 28 (27 HubSpot + 1 collected)."""
    assert len(registry.FORMS) == 28
    assert len(registry.BY_ID) == 28 and len(registry.BY_KEY) == 28


def test_categories_are_a_to_e_and_keys_make_env_names():
    for form in registry.FORMS:
        assert form["category"] in registry.CATEGORIES, form["key"]
        assert re.fullmatch(r"[a-z0-9_]+", form["key"]), form["key"]
        assert re.fullmatch(r"[0-9a-f-]{36}", form["form_id"]), form["key"]


def test_daf_and_endowment_match_the_config_they_already_run_on():
    assert registry.BY_KEY["daf"]["form_id"] == Config.DAF_INQUIRY_FORM_ID
    assert registry.BY_KEY["endowment"]["form_id"] == \
        Config.ENDOWMENT_INQUIRY_FORM_ID


def test_owners_are_exactly_the_ones_carl_confirmed():
    owners = {f["key"]: f["owners"] for f in registry.FORMS if f["owners"]}
    assert owners == {
        "daf": ("munira",), "nonprofit_directory": ("munira",),
        "endowment": ("ola",), "endowment_distribution": ("ola",),
        "ach_request": ("kods",), "investment": ("kods", "nora"),
        "asset_donation": ("kods",),
    }
    assert registry.task_assignee(registry.BY_KEY["asset_donation"]) == 1007


def test_owner_ids_are_the_ones_production_runs():
    """DAF tasks 1033-1038 and 1099 carry 1012; endowment 1067/1069 1002."""
    assert registry.task_assignee(registry.BY_KEY["daf"]) == 1012
    assert registry.task_assignee(registry.BY_KEY["endowment"]) == 1002
    assert registry.OWNERS["kods"]["employee_id"] == 1007
    assert registry.OWNERS["nora"]["employee_id"] == 1004


@pytest.mark.parametrize("key", ["investment", "general_signup"])
def test_no_single_owner_means_no_task(key):
    with pytest.raises(registry.NoOwner):
        registry.task_assignee(registry.BY_KEY[key])


def test_report_recipients_are_separate_from_owners():
    daf = registry.BY_KEY["daf"]
    assert daf["report_to_env"] == "JIDHR_DAF_REPORT_TO"
    assert daf["owners"] == ("munira",)
    assert all(f["report_to_env"] == "JIDHR_FORMS_REPORT_TO"
               for f in registry.FORMS if f["key"] != "daf")


def test_the_money_forms_carry_their_sensitive_fields():
    ach = registry.BY_KEY["ach_request"]["sensitive"]
    for field in ("routing_number", "account_number",
                  "confirm_account_number", "ein", "bank_name"):
        assert ach[field] == registry.OUT, field
    assert ach["vender_id"] == registry.LAST4
    for key in ("asset_donation", "endowment_distribution", "investment",
                "nonprofit_directory"):
        assert registry.BY_KEY[key]["sensitive"], key
    assert {f["category"] for f in registry.FORMS
            if f["key"] in ("ach_request", "endowment_distribution",
                            "investment")} == {"D"}


def test_the_registry_names_no_one_but_the_owners():
    source = (REPO / "sync" / "form_registry.py").read_text()
    names = set(re.findall(r'"name": "([^"]+)"', source))
    assert names == {o["name"] for o in registry.OWNERS.values()}
    assert "@" not in source, "no email address in the registry"


# --- the switches -------------------------------------------------------------------

def test_switches_default_off_preview_and_no_date():
    assert registry.switches(registry.BY_KEY["ach_request"], {}) == \
        {"on": False, "mode": "preview", "since": None}


def test_only_the_exact_word_true_turns_a_form_on():
    form = registry.BY_KEY["general_signup"]
    for value, on in (("true", True), ("True", False), ("yes", False),
                      ("1", False)):
        env = {"JIDHR_FORM_GENERAL_SIGNUP_ON": value}
        assert registry.switches(form, env)["on"] is on, value


@pytest.mark.parametrize("name,value", [
    ("JIDHR_FORM_ACH_REQUEST_MODE", "lve"),
    ("JIDHR_FORM_ACH_REQUEST_SINCE", "2026-13-01"),
    ("JIDHR_FORM_ACH_REQUEST_SINCE", "yesterday"),
])
def test_a_wrong_switch_refuses_and_names_itself(name, value):
    with pytest.raises(forms_sync.BadSetting) as caught:
        forms_sync.read_switches({name: value})
    assert name in str(caught.value)


def test_every_wrong_switch_is_named_in_one_refusal():
    with pytest.raises(forms_sync.BadSetting) as caught:
        forms_sync.read_switches({"JIDHR_FORM_DAF_MODE": "x",
                                  "JIDHR_FORM_INVESTMENT_SINCE": "y"})
    assert "JIDHR_FORM_DAF_MODE" in str(caught.value)
    assert "JIDHR_FORM_INVESTMENT_SINCE" in str(caught.value)


# --- observe: read-only ---------------------------------------------------------------

class Hub:
    """HubSpot double: GETs only. `subs` form_id -> submittedAt list."""

    def __init__(self, subs=None, extra_forms=(), drop=(), broken=(),
                 list_error=None):
        self.subs = subs or {}
        self.calls = []
        self.list_error = list_error
        self.broken = set(broken)
        self.listed = [{"id": f["form_id"], "name": f["name"]}
                       for f in registry.FORMS if f["form_id"] not in drop]
        self.listed += [{"id": i, "name": n} for i, n in extra_forms]

    def _get(self, path, params=None):
        self.calls.append(("GET", path, dict(params or {})))
        assert path == "marketing/v3/forms"
        if self.list_error:
            return {"status": "error", "category": "RATE_LIMITS",
                    "message": self.list_error}
        return {"results": self.listed}

    def get_form_submissions(self, form_id, limit=50, after=None):
        self.calls.append(("GET", "submissions", form_id))
        if form_id in self.broken:
            return {"error": "HTTP 503"}
        return {"results": [{"submittedAt": t, "values": [
            {"name": "email", "value": "someone@example.invalid"},
            {"name": "firstname", "value": "Privatename"}]}
            for t in self.subs.get(form_id, [])]}

    def __getattr__(self, name):
        raise AssertionError(f"observe mode called HubSpot {name}()")


GENERAL = registry.BY_KEY["general_signup"]["form_id"]
ACH = registry.BY_KEY["ach_request"]["form_id"]


def observe(hub, environ=None):
    return forms_sync.run(hub, now=NOW, environ=environ or {})


def test_observe_counts_each_window():
    hub = Hub({GENERAL: [ms(NOW - timedelta(hours=2)),
                         ms(NOW - timedelta(days=3)),
                         ms(NOW - timedelta(days=200))]})
    out = observe(hub)
    row = next(f for f in out["forms"] if f["key"] == "general_signup")
    assert (row["last_24h"], row["last_7d"], row["total"]) == (1, 2, 3)
    assert row["newest"] == "2026-10-28"
    assert forms_sync.new_today(out) == 1


def test_observe_only_ever_gets():
    hub = Hub({GENERAL: [ms(NOW)]})
    observe(hub)
    assert {c[0] for c in hub.calls} == {"GET"}
    assert ("GET", "marketing/v3/forms",
            {"limit": 100, "formTypes": "all"}) in hub.calls


def test_observe_never_touches_csuite_or_writes():
    tree = ast.parse((REPO / "sync" / "forms_sync.py").read_text())
    imported = {getattr(n, "module", None) or "" for n in ast.walk(tree)
                if isinstance(n, (ast.Import, ast.ImportFrom))}
    imported |= {a.name for n in ast.walk(tree) if isinstance(n, ast.Import)
                 for a in n.names}
    assert not any("csuite" in m for m in imported)
    source = (REPO / "sync" / "forms_sync.py").read_text()
    for writer in ("record_write", "reserve_write", "_post(", "_patch(",
                   "_put(", "execute_query"):
        assert writer not in source, writer


def test_an_unknown_form_is_reported():
    hub = Hub(extra_forms=[("11111111-2222-3333-4444-555555555555",
                            "Spring Gala RSVP")])
    out = observe(hub)
    assert out["unknown"] == [{"form_id":
                               "11111111-2222-3333-4444-555555555555",
                               "name": "Spring Gala RSVP"}]
    assert "1 unknown form" in forms_sync.subject(out)
    assert "Spring Gala RSVP" in forms_sync.email_text(out)


def test_a_form_hubspot_no_longer_lists_is_named():
    out = observe(Hub(drop={registry.BY_KEY["blank_form"]["form_id"]}))
    assert out["not_listed"] == ["blank_form"]
    assert "archived or deleted" in forms_sync.email_text(out)


def test_one_unreadable_form_does_not_stop_the_rest():
    out = observe(Hub({GENERAL: [ms(NOW)]}, broken={ACH}))
    assert out["read_errors"] == ["ach_request"]
    assert forms_sync.new_today(out) == 1
    assert "1 unreadable" in forms_sync.subject(out)


def test_an_unreadable_form_list_refuses():
    with pytest.raises(forms_sync.Refused):
        observe(Hub(list_error="too many requests"))


def test_a_bad_switch_refuses_before_any_read():
    hub = Hub()
    with pytest.raises(forms_sync.BadSetting):
        observe(hub, {"JIDHR_FORM_ACH_REQUEST_MODE": "on"})
    assert hub.calls == []


# --- the report ---------------------------------------------------------------------------

def test_the_report_carries_no_submitter_data():
    out = observe(Hub({GENERAL: [ms(NOW)], ACH: [ms(NOW)]}))
    text = forms_sync.email_text(out) + forms_sync.subject(out)
    assert "Privatename" not in text and "@" not in text


def test_the_report_groups_by_category_and_says_observe():
    out = observe(Hub({GENERAL: [ms(NOW)]}))
    text = forms_sync.email_text(out)
    assert "OBSERVE mode" in text
    assert "Nothing was written to CSuite or HubSpot." in text
    assert text.index("A: CSuite record") < text.index("C: HubSpot only") < \
        text.index("E: ignored")
    assert "• General Email Signup Form: 1 · 1 · 1 · 2026-10-28" in text
    assert forms_sync.subject(out) == \
        "[Observe] Website forms — Wed Oct 28 (1 new)"


def test_quiet_c_and_d_forms_are_listed_but_e_forms_are_not():
    out = observe(Hub())
    quiet = {f["key"] for f in forms_sync.quiet(out)}
    assert "ach_request" in quiet and "general_signup" in quiet
    assert not quiet & {f["key"] for f in registry.FORMS
                        if f["category"] in ("A", "E")}


def test_switches_and_owners_show_for_b_and_d_forms():
    out = observe(Hub(), {"JIDHR_FORM_INVESTMENT_ON": "true",
                          "JIDHR_FORM_INVESTMENT_SINCE": "2026-11-01"})
    text = forms_sync.email_text(out)
    assert ("• Investment Form: on, preview, 2026-11-01 — owner: "
            "Zouita, Kods, Moorefield, Nora") in text
    assert "• AMCF Asset Donation Transfer Form: off, preview, no " \
           "since-date — owner: Zouita, Kods" in text


def test_no_recipients_no_email(monkeypatch):
    monkeypatch.setattr(Config, "JIDHR_FORMS_REPORT_TO", (), raising=False)
    out = observe(Hub())
    assert forms_sync.send_email(out)["skipped"] == "no recipients"


def test_the_report_goes_to_the_forms_recipients(monkeypatch):
    sent = []
    monkeypatch.setattr(Config, "JIDHR_FORMS_REPORT_TO",
                        ("carl@example.invalid",), raising=False)
    import clients.notify as notify
    monkeypatch.setattr(notify, "send_report",
                        lambda to, subj, body: sent.append((to, subj))
                        or {"sent": list(to), "failed": []})
    forms_sync.send_email(observe(Hub()))
    assert sent == [(("carl@example.invalid",),
                     "[Observe] Website forms — Wed Oct 28 (0 new)")]


# --- the job --------------------------------------------------------------------------------

def _setting(monkeypatch, name, value):
    import config as config_module
    for holder in {Config, config_module.Config}:
        monkeypatch.setattr(holder, name, value, raising=False)


def test_the_job_is_off_unless_both_switches_say_true(monkeypatch):
    from jobs.handlers import forms_sync as handler
    monkeypatch.setattr(forms_sync, "run",
                        lambda **k: pytest.fail("ran while disabled"))
    for scheduled_on, forms_on in ((False, True), (True, False)):
        _setting(monkeypatch, "SCHEDULED_SYNC_ENABLED", scheduled_on)
        _setting(monkeypatch, "JIDHR_FORMS_SYNC_ENABLED", forms_on)
        assert handler.handle({}) == {"enabled": False}


def test_a_bad_setting_alerts_and_the_job_completes(monkeypatch):
    from jobs.handlers import forms_sync as handler
    alerts = []
    _setting(monkeypatch, "SCHEDULED_SYNC_ENABLED", True)
    _setting(monkeypatch, "JIDHR_FORMS_SYNC_ENABLED", True)
    monkeypatch.setattr("clients.notify.safe_notify",
                        lambda subj, body, **k: alerts.append(subj) or {})
    monkeypatch.setenv("JIDHR_FORM_DAF_MODE", "maybe")
    monkeypatch.setattr("clients.hubspot.HubSpotClient",
                        lambda: pytest.fail("read after a bad setting"))
    result = handler.handle({})
    assert result["stood_down"] is True
    assert alerts == ["nightly forms report: a setting needs fixing"]


def test_the_job_row_keeps_counts_only(monkeypatch):
    from jobs.handlers import forms_sync as handler
    _setting(monkeypatch, "SCHEDULED_SYNC_ENABLED", True)
    _setting(monkeypatch, "JIDHR_FORMS_SYNC_ENABLED", True)
    _setting(monkeypatch, "JIDHR_FORMS_REPORT_TO", ())
    out = observe(Hub({GENERAL: [ms(NOW)]}))
    monkeypatch.setattr(forms_sync, "run", lambda: out)
    result = handler.handle({})
    assert result["new_24h"] == 1 and result["forms"] == 28
    assert "Privatename" not in repr(result)


def test_the_seed_runs_after_daf_and_before_the_mirror():
    import scripts.jobs_run as jr
    assert jr.DAF_SEED["priority"] < jr.FORMS_SEED["priority"] < \
        jr.MIRROR_SEED["priority"]
    assert jr.FORMS_SEED["max_attempts"] == 1
    from jobs import runner
    assert "forms_sync" in runner.HANDLERS
