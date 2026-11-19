"""The digest window is per form: a failed digest is repeated the next night,
a clean night moves the window, a stood-down night moves nothing.

Driven end to end through jobs/handlers/forms_sync.handle, night after
night, against an in-memory `jobs` table. The fake answers both the query
a0a86f3 used (max(finished_at)) and the per-form one, so every test here
also runs against a0a86f3 to pin what changed. Every submission is a TEST
DOUBLE; no network, no database, no SMTP server.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

import jobs.handlers.forms_sync  # noqa: F401  imported before the clock is
import sync.form_digests  # noqa: F401  faked, so their `datetime` is real
from config import Config
from sync import forms_sync

DAY1 = datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc)
DAY2 = DAY1 + timedelta(days=1)
DAY3 = DAY1 + timedelta(days=2)


def sub(conv, when):
    """conv's first 8 characters are what a digest prints."""
    return {"conversionId": conv, "submittedAt": int(when.timestamp() * 1000),
            "values": [{"name": "firstname", "value": "Test"},
                       {"name": "lastname", "value": "Double"},
                       {"name": "email", "value": "t@example.invalid"}]}


A = sub("subA0001-x", DAY1 - timedelta(hours=2))
B = sub("subB0002-x", DAY2 - timedelta(hours=2))
C = sub("subC0003-x", DAY3 - timedelta(hours=2))


class FakeJobs:
    """The jobs table, as much of it as the digest window reads."""

    def __init__(self):
        self.rows = []

    def add(self, job_id, finished_at, result):
        self.rows.append({"id": job_id, "status": "complete",
                          "finished_at": finished_at, "result": result})

    def execute_query(self, sql, params=None, fetch=True):
        text = " ".join(sql.split())
        assert "FROM jobs" in text and "job_type = 'forms_sync'" in text
        assert "status = 'complete'" in text
        others = [r for r in self.rows if r["id"] != params[0]]
        if "max(finished_at)" in text:
            last = max((r["finished_at"] for r in others), default=None)
            return [{"finished_at": last}]
        if "SELECT finished_at, result" in text:
            since = params[1]
            found = [r for r in others if r["finished_at"] >= since]
            return [{"finished_at": r["finished_at"], "result": r["result"]}
                    for r in sorted(found, key=lambda r: r["finished_at"],
                                    reverse=True)]
        raise AssertionError(f"unexpected query: {text}")


class Mail:
    """smtplib.SMTP stand-in. `refuse` is the set of addresses whose
    presence on a message makes the server reject that whole message."""

    def __init__(self):
        self.delivered = []          # (night, recipient, body)
        self.refuse = set()
        self.partial = set()     # refused one by one; the rest delivered
        self.night = None

    def factory(self):
        mail = self

        class SMTP:
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
                to = [a.strip() for a in str(message["To"]).split(",")]
                cc = [a.strip() for a in str(message["Cc"] or "").split(",")
                      if a.strip()]
                if mail.refuse & set(to + cc):
                    raise OSError("550 mailbox unavailable")
                body = message.get_content()
                refused = {}
                for address in to + cc:
                    if address in mail.partial:
                        refused[address] = (550, b"mailbox unavailable")
                    else:
                        mail.delivered.append((mail.night, address, body))
                return refused

        return SMTP

    def got(self, address, conv):
        """How many times `address` received submission `conv`."""
        return sum(1 for _n, a, body in self.delivered
                   if a == address and f"(submission {conv[:8]})" in body)


@pytest.fixture
def world(monkeypatch):
    import smtplib

    import clients.database as database
    import clients.notify as notify

    jobs, mail, alerts = FakeJobs(), Mail(), []
    monkeypatch.setattr(database, "execute_query", jobs.execute_query)
    monkeypatch.setattr(smtplib, "SMTP", mail.factory())
    monkeypatch.setattr(notify, "safe_notify",
                        lambda subject, body, kind=None, **ctx:
                        alerts.append((subject, ctx)))
    monkeypatch.setattr(forms_sync, "send_email", lambda out: {})
    for holder in {Config, __import__("config").Config}:
        monkeypatch.setattr(holder, "SCHEDULED_SYNC_ENABLED", True,
                            raising=False)
        monkeypatch.setattr(holder, "JIDHR_FORMS_SYNC_ENABLED", True,
                            raising=False)
        monkeypatch.setattr(holder, "JIDHR_FORMS_REPORT_TO",
                            ("carl@example.invalid",), raising=False)
    monkeypatch.setenv("NOTIFY_SMTP_HOST", "smtp.example.invalid")
    monkeypatch.setenv("NOTIFY_EMAIL_FROM", "jidhr@example.invalid")
    for name in ("NOTIFY_SMTP_PORT", "NOTIFY_SMTP_USER",
                 "NOTIFY_SMTP_PASSWORD", "NOTIFY_EMAIL_TO",
                 "NOTIFY_SLACK_WEBHOOK_URL", "JIDHR_FORMS_LIVE_CC"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("JIDHR_OWNER_EMAIL_MUNIRA", "munira@example.invalid")
    monkeypatch.setenv("JIDHR_OWNER_EMAIL_KODS", "kods@example.invalid")
    monkeypatch.setenv("JIDHR_OWNER_EMAIL_NORA", "nora@example.invalid")
    return {"jobs": jobs, "mail": mail, "alerts": alerts,
            "monkeypatch": monkeypatch}


def night(world, job_id, now, raw, key, mode="live", stood_down=False):
    """One nightly run at `now`; its result is stored like the runner does."""
    from datetime import datetime as real_datetime

    import jobs.handlers.forms_sync as handler

    mp = world["monkeypatch"]

    class Clock(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    mp.setattr("datetime.datetime", Clock)
    world["mail"].night = job_id
    if stood_down:
        def run():
            raise forms_sync.BadSetting("JIDHR_FORM_X_MODE is 'lve'")
    else:
        switches = {"on": True, "mode": mode, "since": date(2026, 10, 1)}
        out = {"mode": "observe", "run_at": now.isoformat(),
               "forms": [{"key": key, "category": "D", "switches": switches,
                          "last_24h": len(raw)}],
               "unknown": [], "not_listed": [], "read_errors": [],
               "_raw": {key: list(raw)}}

        def run():
            return out
    mp.setattr(forms_sync, "run", run)
    try:
        result = handler.handle({"id": job_id})
    finally:
        mp.setattr("datetime.datetime", real_datetime)
    world["jobs"].add(job_id, now + timedelta(minutes=5), result)
    return result


# --- the four behaviours -----------------------------------------------------------

def test_a_failed_digest_is_repeated_the_next_night(world):
    mail = world["mail"]
    mail.refuse = {"munira@example.invalid"}
    first = night(world, 1, DAY1, [A], "nonprofit_directory")
    assert first["digests"] == {"nonprofit_directory":
                                {"sent": 0, "failed": 1}}
    assert len(world["alerts"]) == 1

    mail.refuse = set()
    night(world, 2, DAY2, [A, B], "nonprofit_directory")
    assert mail.got("munira@example.invalid", A["conversionId"]) == 1
    assert mail.got("munira@example.invalid", B["conversionId"]) == 1


def test_a_clean_night_moves_the_window_forward(world):
    mail = world["mail"]
    night(world, 1, DAY1, [A], "nonprofit_directory")
    night(world, 2, DAY2, [A, B], "nonprofit_directory")
    assert mail.got("munira@example.invalid", A["conversionId"]) == 1
    assert mail.got("munira@example.invalid", B["conversionId"]) == 1
    assert [n for n, _a, body in mail.delivered
            if "(submission subA0001)" in body] == [1]
    assert world["alerts"] == []


def test_a_stood_down_night_moves_no_window(world):
    mail = world["mail"]
    night(world, 1, DAY1, [A], "nonprofit_directory")
    stood = night(world, 2, DAY2, [], "nonprofit_directory", stood_down=True)
    assert stood.get("stood_down") and "digests" not in stood
    night(world, 3, DAY3, [A, B, C], "nonprofit_directory")
    for s in (A, B, C):
        assert mail.got("munira@example.invalid", s["conversionId"]) == 1


def test_with_cc_a_failure_leaves_no_partial_duplicates(world):
    """Investment has two owners. With a CC the digest is one message, so
    Nora's mailbox refusing it means nobody got it, and the next night
    everyone gets each submission exactly once."""
    world["monkeypatch"].setenv("JIDHR_FORMS_LIVE_CC", "ops@example.invalid")
    mail = world["mail"]
    mail.refuse = {"nora@example.invalid"}
    first = night(world, 1, DAY1, [A], "investment")
    assert first["digests"] == {"investment": {"sent": 0, "failed": 3}}

    mail.refuse = set()
    night(world, 2, DAY2, [A, B], "investment")
    for address in ("kods@example.invalid", "nora@example.invalid",
                    "ops@example.invalid"):
        for s in (A, B):
            assert mail.got(address, s["conversionId"]) == 1, (address, s)


# --- the per-form choice, directly ------------------------------------------------

def test_covered_reads_the_stored_digests_map():
    from sync.form_digests import covered

    assert covered({"digests": {}}, "investment") is True       # nothing new
    assert covered({"digests": {"investment": {"sent": 2, "failed": 0}}},
                   "investment") is True
    assert covered({"digests": {"investment": {"sent": 1, "failed": 1}}},
                   "investment") is False
    assert covered({"digests": {"investment": {"sent": 1, "failed": 1}}},
                   "nonprofit_directory") is True               # other form
    assert covered({"stood_down": True}, "investment") is False
    assert covered({"enabled": False}, "investment") is False
    assert covered({}, "investment") is False


def test_no_run_ever_is_24_hours_and_none_covering_is_the_7_day_cap():
    from sync.form_digests import (DEFAULT_LOOKBACK, MAX_LOOKBACK,
                                   last_success, window_start)

    assert last_success("investment", None, DAY2) is None
    assert last_success("investment", {"ever": False, "runs": []},
                        DAY2) is None
    assert window_start(DAY2, None) == DAY2 - DEFAULT_LOOKBACK
    old_only = {"ever": True, "runs": []}
    assert last_success("investment", old_only, DAY2) == DAY2 - MAX_LOOKBACK
    failing = {"ever": True, "runs": [
        (DAY1, {"digests": {"investment": {"sent": 0, "failed": 2}}})]}
    assert last_success("investment", failing, DAY2) == DAY2 - MAX_LOOKBACK


def test_a_database_error_falls_back_to_24_hours(monkeypatch):
    import clients.database as database
    from sync.form_digests import recent_runs

    def broken(sql, params=None, fetch=True):
        raise RuntimeError("no database")
    monkeypatch.setattr(database, "execute_query", broken)
    assert recent_runs(1, DAY2) is None


def test_a_result_stored_as_json_text_is_read(monkeypatch):
    import json

    import clients.database as database
    from sync.form_digests import last_success, recent_runs

    stored = json.dumps({"digests": {"investment": {"sent": 2,
                                                     "failed": 0}}})
    rows = {"max": [{"finished_at": DAY1}],
            "recent": [{"finished_at": DAY1, "result": stored}]}
    monkeypatch.setattr(database, "execute_query",
                        lambda sql, params=None, fetch=True:
                        rows["max" if "max(" in sql else "recent"])
    assert last_success("investment", recent_runs(2, DAY2), DAY2) == DAY1


# --- a server that refuses SOME recipients ----------------------------------------
#
# smtplib does not raise when a server accepts a message for some recipients
# and refuses others; it returns the refused ones. Pinned against 2b4a8f4,
# where that counted as delivered: no alert, no repeat.

def test_a_partial_refusal_with_cc_alerts_once_and_repeats(world):
    world["monkeypatch"].setenv("JIDHR_FORMS_LIVE_CC", "ops@example.invalid")
    mail = world["mail"]
    mail.partial = {"nora@example.invalid"}
    first = night(world, 1, DAY1, [A], "investment")
    assert first["digests"] == {"investment": {"sent": 0, "failed": 3}}
    assert len(world["alerts"]) == 1
    assert world["alerts"][0][1]["form"] == "investment"

    mail.partial = set()
    night(world, 2, DAY2, [A, B], "investment")
    assert len(world["alerts"]) == 1
    # Nora gets A once, on the repeat. Kods and ops had it on night 1 and
    # get it again: the known cost of repeating a partial delivery.
    assert mail.got("nora@example.invalid", A["conversionId"]) == 1
    assert [n for n, a, body in mail.delivered
            if a == "nora@example.invalid"
            and "(submission subA0001)" in body] == [2]
    for address in ("kods@example.invalid", "ops@example.invalid"):
        assert mail.got(address, A["conversionId"]) == 2
    for address in ("kods@example.invalid", "nora@example.invalid",
                    "ops@example.invalid"):
        assert mail.got(address, B["conversionId"]) == 1


def test_full_acceptance_with_cc_neither_alerts_nor_repeats(world):
    world["monkeypatch"].setenv("JIDHR_FORMS_LIVE_CC", "ops@example.invalid")
    mail = world["mail"]
    first = night(world, 1, DAY1, [A], "investment")
    assert first["digests"] == {"investment": {"sent": 3, "failed": 0}}
    night(world, 2, DAY2, [A, B], "investment")
    assert world["alerts"] == []
    for address in ("kods@example.invalid", "nora@example.invalid",
                    "ops@example.invalid"):
        assert mail.got(address, A["conversionId"]) == 1
        assert mail.got(address, B["conversionId"]) == 1


def test_send_email_is_false_when_any_recipient_is_refused(monkeypatch):
    import smtplib

    import clients.notify as notify

    class SMTP:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def starttls(self):
            pass

        def send_message(self, message):
            return {"b@example.invalid": (550, b"no")}

    monkeypatch.setattr(smtplib, "SMTP", SMTP)
    monkeypatch.setenv("NOTIFY_SMTP_HOST", "smtp.example.invalid")
    monkeypatch.delenv("NOTIFY_SMTP_PORT", raising=False)
    assert notify._send_email(["a@example.invalid", "b@example.invalid"],
                              "S", "T") is False
