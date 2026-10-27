"""V5.96: "mark handled <form> before <date>". Every HubSpot object here is a
TEST DOUBLE with invented ids; no network, no database."""

from datetime import datetime, timezone

import pytest

from clients import audit
from config import Config
from intents import daf_workflow, mark_handled_bulk as bulk

CUT = "2026-10-19"


def ms(day, hour=12):
    return int(datetime.fromisoformat(f"{day}T{hour:02d}:00:00+00:00")
               .timestamp() * 1000)


def sub(sid, day, hour=12):
    return {"conversionId": sid, "submittedAt": ms(day, hour),
            "values": [{"name": "firstname", "value": "Private"},
                       {"name": "email", "value": "p@example.invalid"}]}


HOLD = bulk.HOLD_BACK["daf"]
SUBS = [sub("aaaa0001-x", "2026-01-05"), sub("aaaa0002-x", "2026-10-18", 23),
        sub("aaaa0003-x", "2026-10-19", 0),        # ON the cutoff: never
        sub("aaaa0004-x", "2026-10-21"),           # after: never
        sub("done0001-x", "2026-03-01"),           # already handled
        sub("held0001-x", "2026-03-02")]           # held
SUBS += [sub(f"{h}-hold", "2026-02-01") for h in HOLD]


class Hub:
    def __init__(self, subs=SUBS):
        self.subs = subs

    def get_form_submissions(self, form_id, limit=50, after=None):
        return {"results": list(self.subs)}


@pytest.fixture
def world(monkeypatch):
    rows = []
    monkeypatch.setattr(Config, "CSUITE_DAF_CREATE_ENABLED", True)
    monkeypatch.setattr(audit, "processed_submissions",
                        lambda form_id, **k: {"done0001-x": "1"})
    monkeypatch.setattr(audit, "held_submissions",
                        lambda form_id: {"held0001-x": "missing_fields"})
    monkeypatch.setattr(audit, "record_write",
                        lambda system, method, endpoint, **k:
                        rows.append((system, endpoint, k["payload"])) or True)
    monkeypatch.setattr("sync.daf_reconcile.link_rows_allowed", lambda: True)
    return rows


def say(text, actor=7):
    return bulk.handle(text, Hub(), actor)


APPLY = (f"mark handled daf before {CUT} exclude {', '.join(sorted(HOLD))} "
         "apply")


def test_the_preview_lists_what_it_would_mark_and_names_the_held_back(world):
    reply = say(f"mark handled daf before {CUT}")
    assert world == [], "a preview writes nothing"
    assert "**Would record as handled: 2**" in reply
    assert "`aaaa0001` — 2026-01-05" in reply
    assert "`aaaa0002` — 2026-10-18" in reply
    assert "**Held back, never marked: 11**" in reply
    for h in HOLD:
        assert f"`{h}`" in reply
    assert "aaaa0003" not in reply and "aaaa0004" not in reply
    assert "On or after 2026-10-19, never touched: 2" in reply
    assert "Already handled or held: 2" in reply
    assert APPLY in reply, "the preview spells out the exact apply command"


def test_apply_with_the_exact_exclude_list_records_one_row_each(world):
    reply = say(APPLY)
    assert [p["hubspot_submission_id"] for _s, _e, p in world] == \
        ["aaaa0001-x", "aaaa0002-x"]
    assert {e for _s, e, _p in world} == \
        {"submission/link/bulk_marked_handled"}
    assert {s for s, _e, _p in world} == {"jidhr"}
    assert all(p["actor_user_id"] == 7 for _s, _e, p in world)
    assert "2 DAF submissions before 2026-10-19 recorded as handled" in reply


@pytest.mark.parametrize("exclude", [
    ", ".join(HOLD[:-1]),                                  # one missing
    ", ".join(HOLD + ("deadbeef",)),                       # one extra
])
def test_apply_refuses_unless_the_exclude_list_matches(world, exclude):
    reply = say(f"mark handled daf before {CUT} exclude {exclude} apply")
    assert world == []
    assert "Not recorded" in reply and "Nothing was written" in reply


def test_apply_without_an_exclude_list_refuses(world):
    reply = say(f"mark handled daf before {CUT} apply")
    assert world == [] and "Missing:" in reply


def test_never_touches_the_cutoff_day_or_later(world):
    say(APPLY)
    marked = {p["hubspot_submission_id"] for _s, _e, p in world}
    assert not marked & {"aaaa0003-x", "aaaa0004-x"}
    assert not marked & {f"{h}-hold" for h in HOLD}
    assert not marked & {"done0001-x", "held0001-x"}


def test_a_failed_row_stops_and_is_not_retried(world, monkeypatch):
    calls = []

    def fail_second(system, method, endpoint, **k):
        calls.append(k["payload"]["hubspot_submission_id"])
        if len(calls) == 2:
            raise audit.AuditUnavailable("down")
        return True

    monkeypatch.setattr(audit, "record_write", fail_second)
    reply = say(APPLY)
    assert calls == ["aaaa0001-x", "aaaa0002-x"]
    assert "1 DAF submission before" in reply and "Stopped" in reply


def test_unreadable_history_writes_nothing(world, monkeypatch):
    def broken(form_id, **k):
        raise audit.ProcessedHistoryUnavailable("down")

    monkeypatch.setattr(audit, "processed_submissions", broken)
    reply = say(APPLY)
    assert world == [] and "could not be read" in reply


def test_a_future_date_and_no_actor_refuse(world):
    assert "in the future" in say("mark handled daf before 2099-01-01")
    assert "could not be told" in say(APPLY, actor=None)
    assert world == []


def test_the_reply_and_rows_carry_no_submitter_data(world):
    reply = say(f"mark handled daf before {CUT}") + say(APPLY)
    assert "Private" not in reply and "p@example" not in reply
    assert "Private" not in repr(world)


def test_the_chat_routes_it_and_the_single_mark_still_works():
    assert daf_workflow.can_handle(f"mark handled daf before {CUT}")
    assert daf_workflow.can_handle("mark inquiry handled daf 6fff2fa0")
    assert not bulk.is_bulk("mark inquiry handled daf 6fff2fa0")


def test_the_hold_back_list_is_the_eleven():
    assert len(HOLD) == 11 and len(set(HOLD)) == 11
    assert {"2fcd52ac", "1a3fbc12", "14602884"} <= set(HOLD)
