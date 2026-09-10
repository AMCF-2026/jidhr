"""Step 2a: a failed operation must never look like a successful one.

One test class per item in the brief. Every failure path asserts two things:
the user is told, and nothing was written or appended.
"""

import logging
from datetime import datetime, timezone

import pytest

from intents.context import Actor, RequestContext, Services, new_draft_state

ACTOR = Actor(user_id=1, email="staff@amuslimcf.org", role="staff")


def make_ctx(hubspot=None, csuite=None, claude=None, draft_state=None,
             workflow_state=None):
    return RequestContext(
        actor=ACTOR,
        services=Services(hubspot=hubspot, csuite=csuite, claude=claude),
        draft_state=draft_state if draft_state is not None else new_draft_state(),
        workflow_state=workflow_state if workflow_state is not None else {},
        conversation_history=[],
    )


# ===========================================================================
# 1. OpenRouter failures raise, and cost the conversation nothing
# ===========================================================================

class TestOpenRouter:
    def _client(self, monkeypatch, response=None, exc=None):
        from clients.openrouter import OpenRouterClient

        client = OpenRouterClient()
        client.api_key = "test-key"
        calls = []

        def fake_post(url, **kwargs):
            calls.append(url)
            if exc is not None:
                raise exc
            return response

        monkeypatch.setattr("clients.openrouter.requests.post", fake_post)
        return client, calls

    def test_http_500_raises_with_the_status(self, monkeypatch):
        from clients.openrouter import OpenRouterError

        class Resp:
            status_code = 500
            text = "upstream exploded"

        client, _ = self._client(monkeypatch, response=Resp())

        with pytest.raises(OpenRouterError) as excinfo:
            client.chat([{"role": "user", "content": "hi"}])

        assert excinfo.value.status == 500

    def test_a_timeout_raises_rather_than_returning_prose(self, monkeypatch):
        import requests
        from clients.openrouter import OpenRouterError

        client, _ = self._client(
            monkeypatch, exc=requests.exceptions.Timeout("slow"))

        with pytest.raises(OpenRouterError) as excinfo:
            client.chat([{"role": "user", "content": "hi"}])

        assert excinfo.value.status == "timeout"

    def test_a_missing_api_key_raises(self, monkeypatch):
        from clients.openrouter import OpenRouterClient, OpenRouterError

        client = OpenRouterClient()
        client.api_key = ""

        with pytest.raises(OpenRouterError):
            client.chat([{"role": "user", "content": "hi"}])

    def test_429_is_retried_once_then_gives_up(self, monkeypatch):
        from clients.openrouter import OpenRouterError

        class Resp:
            status_code = 429
            text = "slow down"

        slept = []
        monkeypatch.setattr("clients.openrouter.time.sleep", slept.append)
        client, calls = self._client(monkeypatch, response=Resp())

        with pytest.raises(OpenRouterError):
            client.chat([{"role": "user", "content": "hi"}])

        assert len(calls) == 2, "one retry, no more"
        assert slept == [2]

    def test_a_429_that_then_succeeds_returns_the_answer(self, monkeypatch):
        class Fail:
            status_code = 429
            text = "slow down"

        class Ok:
            status_code = 200

            def json(self):
                return {"choices": [{"message": {"content": "the answer"}}]}

        responses = [Fail(), Ok()]
        monkeypatch.setattr("clients.openrouter.time.sleep", lambda s: None)
        monkeypatch.setattr("clients.openrouter.requests.post",
                            lambda url, **kw: responses.pop(0))

        from clients.openrouter import OpenRouterClient

        client = OpenRouterClient()
        client.api_key = "k"
        assert client.chat([{"role": "user", "content": "hi"}]) == "the answer"

    def test_a_500_is_not_retried(self, monkeypatch):
        from clients.openrouter import OpenRouterError

        class Resp:
            status_code = 500
            text = "boom"

        monkeypatch.setattr("clients.openrouter.time.sleep", lambda s: None)
        client, calls = self._client(monkeypatch, response=Resp())

        with pytest.raises(OpenRouterError):
            client.chat([{"role": "user", "content": "hi"}])

        assert len(calls) == 1


class TestAssistantOpenRouterFailure:
    def _assistant(self, claude):
        from assistant import JidhrAssistant

        a = JidhrAssistant.__new__(JidhrAssistant)
        a.claude, a.hubspot, a.csuite = claude, None, None
        a.conversation_history = []
        a.services = Services(hubspot=None, csuite=None, claude=claude)
        a.draft_state, a.workflow_state = new_draft_state(), {}
        return a

    def test_the_user_is_told_and_history_is_untouched(self):
        from clients.openrouter import OpenRouterError

        class Broken:
            def chat(self, **kwargs):
                raise OpenRouterError(503, "service unavailable")

        a = self._assistant(Broken())
        response = a.process_query("what is our fee structure", ACTOR)

        assert "didn't respond" in response
        assert "503" in response
        assert "wasn't lost" in response
        assert a.conversation_history == [], (
            "a failed turn must leave no trace in history")

    def test_a_second_attempt_starts_clean(self):
        from clients.openrouter import OpenRouterError

        class Flaky:
            def __init__(self):
                self.calls = 0

            def chat(self, messages=None, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    raise OpenRouterError(503, "down")
                return "here you go"

        claude = Flaky()
        a = self._assistant(claude)
        a.process_query("question", ACTOR)
        second = a.process_query("question", ACTOR)

        assert second == "here you go"
        # One user turn and one assistant turn — the failed attempt is absent.
        assert [m["role"] for m in a.conversation_history] == ["user", "assistant"]

    def test_a_successful_turn_is_unchanged(self):
        class Fine:
            def chat(self, **kwargs):
                return "the answer"

        a = self._assistant(Fine())
        assert a.process_query("hello", ACTOR) == "the answer"
        assert len(a.conversation_history) == 2


# ===========================================================================
# 2. Contact multi-match never writes to results[0]
# ===========================================================================

def _contact(cid, first, last, email, codes=None):
    props = {"firstname": first, "lastname": last, "email": email}
    if codes is not None:
        props["constituent_codes"] = codes
    return {"id": cid, "properties": props}


class RecordingHubSpot:
    def __init__(self, contacts):
        self._contacts = contacts
        self.writes = []

    def search_contacts(self, query, limit=10):
        return {"results": self._contacts}

    def create_call_note(self, body, contact_id):
        self.writes.append(("call", contact_id, body))
        return {"id": "note-1"}

    def create_meeting_note(self, title, body, contact_id):
        self.writes.append(("meeting", contact_id, body))
        return {"id": "note-1"}

    def create_note(self, body, contact_id):
        self.writes.append(("note", contact_id, body))
        return {"id": "note-1"}

    def update_giving_circle_status(self, contact_id, status):
        self.writes.append(("gc", contact_id, status))
        return {"id": contact_id}

    @staticmethod
    def get_contact_url(contact_id):
        return f"https://hubspot.example/contact/{contact_id}"


class TestContactPick:
    TWO = [_contact("1", "Ahmed", "Khan", "ahmed.k@example.org"),
           _contact("2", "Ahmed", "Siddiqui", "ahmed.s@example.org")]

    def test_two_matches_write_nothing_and_are_listed(self):
        from intents.notes import handle

        hubspot = RecordingHubSpot(self.TWO)
        state = {}
        ctx = make_ctx(hubspot=hubspot, workflow_state=state)

        out = handle("log call with Ahmed - discussed the DAF timeline", ctx)

        assert hubspot.writes == [], "must not write to results[0]"
        assert "1." in out and "2." in out
        assert "ahmed.k@example.org" in out and "ahmed.s@example.org" in out
        assert "haven't written anything" in out
        assert state["pending_contact_pick"]["action"] == "note"

    def test_a_digit_picks_the_right_contact_and_writes_once(self):
        from intents.notes import handle

        hubspot = RecordingHubSpot(self.TWO)
        state = {}
        ctx = make_ctx(hubspot=hubspot, workflow_state=state)

        handle("log call with Ahmed - discussed the DAF timeline", ctx)
        out = handle("2", ctx)

        assert len(hubspot.writes) == 1
        kind, contact_id, body = hubspot.writes[0]
        assert (kind, contact_id) == ("call", "2")
        assert "DAF timeline" in body
        assert "✅" in out
        assert "pending_contact_pick" not in state

    def test_one_match_still_writes_straight_through(self):
        from intents.notes import handle

        hubspot = RecordingHubSpot([self.TWO[0]])
        ctx = make_ctx(hubspot=hubspot, workflow_state={})

        out = handle("log call with Ahmed - discussed timeline", ctx)

        assert len(hubspot.writes) == 1
        assert "✅" in out

    def test_zero_matches_says_so_and_writes_nothing(self):
        from intents.notes import handle

        hubspot = RecordingHubSpot([])
        ctx = make_ctx(hubspot=hubspot, workflow_state={})

        out = handle("log call with Nobody - said hello", ctx)

        assert hubspot.writes == []
        assert "couldn't find" in out.lower()

    def test_a_failed_search_is_reported_not_swallowed(self):
        from intents.notes import handle

        class Broken(RecordingHubSpot):
            def search_contacts(self, query, limit=10):
                raise RuntimeError("hubspot down")

        hubspot = Broken([])
        ctx = make_ctx(hubspot=hubspot, workflow_state={})

        out = handle("log call with Ahmed - hello", ctx)

        assert hubspot.writes == []
        assert "❌" in out and "hubspot down" in out

    def test_a_failed_write_is_not_reported_as_success(self):
        from intents.notes import handle

        class FailingWrite(RecordingHubSpot):
            def create_call_note(self, body, contact_id):
                return {"error": "insufficient scopes"}

        ctx = make_ctx(hubspot=FailingWrite([self.TWO[0]]), workflow_state={})
        out = handle("log call with Ahmed - hello", ctx)

        assert "✅" not in out
        assert "insufficient scopes" in out

    def test_a_non_digit_message_clears_the_pending_pick(self):
        from intents.notes import take_pending_contact_pick

        state = {"pending_contact_pick": {"action": "note", "contacts": []}}
        contact, pending = take_pending_contact_pick("never mind", state)

        assert (contact, pending) == (None, None)
        assert "pending_contact_pick" not in state

    def test_can_handle_claims_a_digit_only_while_pending(self):
        from intents import notes

        state = {"pending_contact_pick": {"action": "note", "contacts": []}}
        assert notes.can_handle("2", workflow_state=state)
        assert not notes.can_handle("sync donations", workflow_state=state)
        assert not notes.can_handle("2", workflow_state={})

    def test_gc_upgrade_also_refuses_to_guess(self):
        from intents.notes import handle

        contacts = [_contact("1", "Sara", "Ali", "sara.a@example.org", ""),
                    _contact("2", "Sara", "Noor", "sara.n@example.org", "")]
        hubspot = RecordingHubSpot(contacts)
        state = {}
        ctx = make_ctx(hubspot=hubspot, workflow_state=state)

        out = handle("set gc status for Sara to voting member", ctx)

        assert hubspot.writes == []
        assert "1." in out and "2." in out
        assert state["pending_contact_pick"]["action"] == "gc_status"

        out2 = handle("1", ctx)
        assert hubspot.writes == [("gc", "1", "GC Voting Member")]
        assert "Sara Ali" in out2


class TestGCNameExtractor:
    @pytest.mark.parametrize("query,expected", [
        ("upgrade sara to voting member", "sara"),
        ("set gc status for ahmed to member", "ahmed"),
        ("make lisa a voting member", "lisa"),
        ("upgrade giving circle for aaliyah", "aaliyah"),
        ("upgrade sara ahmad to voting member", "sara ahmad"),
    ])
    def test_names_survive_the_strip(self, query, expected):
        from intents.notes import _extract_gc_name

        assert _extract_gc_name(query) == expected

    def test_the_bare_letter_a_no_longer_eats_the_name(self):
        """str.replace('a', '') turned "Sara" into "Sr" and "Aaliyah" into "liyh"."""
        from intents.notes import _extract_gc_name

        for name in ("sara", "aaliyah", "amara", "hana"):
            assert _extract_gc_name(f"upgrade {name} to voting member") == name


# ===========================================================================
# 3. A failed ticket close is never reported as closed
# ===========================================================================

class TestTicketClose:
    def _run(self, close_result):
        from intents.daf_workflow import _format_confirmation

        results = {
            "profile_created": True, "fund_created": True,
            "hubspot_updated": True, "hubspot_created": False,
            "ticket_closed": False, "ticket_close_failed": None,
            "errors": [],
        }
        state = {"profile_id": 1, "funit_id": 2, "ticket_id": "T-99",
                 "hubspot_contact_id": "C-1"}

        if close_result is True:
            results["ticket_closed"] = True
        else:
            results["ticket_close_failed"] = close_result

        return _format_confirmation(
            {"first_name": "A", "last_name": "B", "email": "a@b.org"},
            state, results, "DAF")

    def test_a_successful_close_reads_as_closed(self):
        out = self._run(True)
        assert "📋 Ticket closed" in out
        assert "✅" in out

    def test_a_failed_close_says_so_with_the_ticket_id(self):
        out = self._run("insufficient scopes")
        assert "NOT closed" in out
        assert "T-99" in out
        assert "insufficient scopes" in out

    def test_a_failed_close_downgrades_the_header(self):
        out = self._run("insufficient scopes")
        assert "✅ **DAF Created!**" not in out
        assert "with warnings" in out


# ===========================================================================
# 4. _fetch_event_detail returns None, and callers say so
# ===========================================================================

class TestEventDetail:
    def test_a_failed_fetch_returns_none(self):
        from intents.events import _fetch_event_detail

        class Broken:
            def get_event_date(self, eid):
                raise RuntimeError("csuite down")

        assert _fetch_event_detail(1, Broken()) is None

    def test_an_unsuccessful_response_returns_none(self):
        from intents.events import _fetch_event_detail

        class Sad:
            def get_event_date(self, eid):
                return {"success": False, "error": "nope"}

        assert _fetch_event_detail(1, Sad()) is None

    def test_a_good_response_returns_the_dict(self):
        from intents.events import _fetch_event_detail

        class Fine:
            def get_event_date(self, eid):
                return {"success": True, "data": {"event_name": "Gala"}}

        assert _fetch_event_detail(1, Fine()) == {"event_name": "Gala"}

    def test_show_attendees_reports_the_failure(self):
        from intents.events import _show_attendees

        class Csuite:
            def get_event_dates(self, limit=200):
                return {"success": True, "data": {"results": [
                    {"event_date_id": 7, "event_name": "Spring Gala",
                     "event_description": "Spring Gala",
                     "event_date": "2026-04-11", "archived": 0}]}}

            def get_event_date(self, eid):
                return {"success": False, "error": "boom"}

        out = _show_attendees("who's registered for Spring Gala",
                              "who's registered for spring gala", Csuite(), {})

        assert "Couldn't load details" in out
        assert "Spring Gala" in out

    def test_no_isinstance_str_check_remains_on_detail_results(self):
        import inspect

        from intents import events

        source = inspect.getsource(events)
        assert "isinstance(event_detail, str)" not in source
        assert "isinstance(current_detail, str)" not in source
        assert "isinstance(prior_detail, str)" not in source


# ===========================================================================
# 5. One failing gatherer does not silence the others
# ===========================================================================

class TestGatherContext:
    def test_a_failing_gatherer_produces_a_visible_line(self, caplog):
        from intents.queries import _run_gatherer

        def boom():
            raise RuntimeError("csuite timeout")

        with caplog.at_level(logging.WARNING, logger="intents.queries"):
            out = _run_gatherer("fund", boom)

        assert out == ["[fund lookup failed — do not guess about fund]"]
        assert any("fund" in r.getMessage() for r in caplog.records)

    def test_a_working_gatherer_is_passed_through(self):
        from intents.queries import _run_gatherer

        assert _run_gatherer("fund", lambda: ["a", "b"]) == ["a", "b"]

    def test_one_failure_does_not_stop_the_rest(self):
        from intents.queries import gather_context

        class Csuite:
            def search_funds(self, term):
                raise RuntimeError("csuite down")

            def get_funds(self, **kwargs):
                raise RuntimeError("csuite down")

            def get_donations(self, **kwargs):
                return {"success": True, "data": {"results": [
                    {"donation_amount": "50.00", "fund_name": "F",
                     "donation_date": "2026-01-01"}]}}

            def get_donations_by_profile(self, pid, limit=10):
                return {"success": True, "data": {"results": []}}

        context = gather_context("fund balance and recent donations",
                                 None, Csuite())

        assert "lookup failed" in context
        assert "Donation" in context or "donation" in context


# ===========================================================================
# 6. A cadence check that could not run says so
# ===========================================================================

class TestCadenceGate:
    def test_check_result_distinguishes_clean_from_broken(self):
        from content.queue_check import CheckResult

        clean = CheckResult(ok=True, conflicts=[])
        broken = CheckResult(ok=False, error="HubSpot down")

        assert bool(clean) is True
        assert bool(broken) is False
        assert broken.error == "HubSpot down"

    def test_a_failed_fetch_yields_not_ok(self, monkeypatch):
        from content import queue_check

        def boom(hubspot=None):
            raise queue_check.QueueUnavailable("HubSpot down")

        monkeypatch.setattr(queue_check, "fetch_queue", boom)
        result = queue_check.check_schedule(
            "body", None, "facebook", datetime.now(timezone.utc))

        assert result.ok is False
        assert "HubSpot down" in result.error

    def test_an_empty_queue_is_a_clean_pass_not_an_error(self, monkeypatch):
        from content import queue_check

        monkeypatch.setattr(queue_check, "fetch_queue", lambda hubspot=None: [])
        result = queue_check.check_schedule(
            "body", None, "facebook", datetime.now(timezone.utc))

        assert result.ok is True and result.conflicts == []

    def test_get_queue_still_swallows_for_its_other_callers(self, monkeypatch):
        from content import queue_check

        def boom(hubspot=None):
            raise queue_check.QueueUnavailable("down")

        monkeypatch.setattr(queue_check, "fetch_queue", boom)
        assert queue_check.get_queue() == []

    def test_content_tells_the_user_the_check_could_not_run(self, monkeypatch):
        from intents import content
        from content.queue_check import CheckResult

        monkeypatch.setattr(
            content, "check_schedule",
            lambda **kwargs: CheckResult(ok=False, error="HubSpot down"))

        draft = new_draft_state()
        draft.update({"active": True, "type": "social", "body": "A post",
                      "platform": "facebook"})
        ctx = make_ctx(draft_state=draft)

        out = content._save_social_post("schedule for tomorrow at 5pm", ctx)

        assert "Cadence check unavailable" in out
        assert "HubSpot down" in out
        assert "your call" in out
        # Held, not posted: the override path is available.
        assert ctx.draft_state.get("pending_schedule")


# ===========================================================================
# 7. Capped fetches are labelled
# ===========================================================================

class TestPartialData:
    def test_partial_note_is_silent_when_complete(self):
        from intents.reports import partial_note

        assert partial_note(100, True) is None

    def test_partial_note_warns_when_incomplete(self):
        from intents.reports import partial_note

        note = partial_note(1000, False, "grants")
        assert "Partial data" in note
        assert "1000 grants" in note
        assert "NOT complete" in note

    # The capped page-walkers these used to cover (_fetch_all_grants,
    # _fetch_all_donations) are gone as of Step 3c. Their whole purpose was
    # to report "complete=False" so a report could print a banner over a
    # lower-bound total. The reports now read a complete mirror instead, so
    # there is no lower bound to warn about — see tests/test_mirror_reports.py
    # for what replaced them. partial_note itself stays, because the
    # HubSpot-sourced reports are still genuinely capped.

    def test_the_capped_hubspot_reports_still_carry_the_banner(self):
        from intents.reports import _report_tasks

        class HubSpot:
            def get_tasks(self, limit=50):
                return {"results": [
                    {"properties": {"hs_task_subject": f"Task {i}",
                                    "hs_task_status": "NOT_STARTED",
                                    "hs_task_priority": "HIGH"}}
                    for i in range(limit)]}

            def get_task_url(self):
                return "https://example.invalid/tasks"

        out = _report_tasks(HubSpot())

        assert "Partial data" in out
        assert "NOT complete" in out

    def test_the_mirror_backed_reports_no_longer_carry_it(self, monkeypatch):
        """A banner saying "NOT complete" over complete data is its own
        kind of lie, and it trains people to ignore the banner."""
        import intents.reports as reports_module

        rows = {
            "grant": [{"csuite_id": "1", "fund_group_id": None,
                       "data": {"grant_id": 1, "grant_amount": "10.00",
                                "grant_date": datetime.now().strftime(
                                    "%Y-%m-%d"),
                                "fund_name": "F"},
                       "synced_at": datetime(2026, 9, 10, 12, 0)}],
        }

        def fake_query(sql, params=None, fetch=True):
            collapsed = " ".join(str(sql).split())
            record_type = (params or ("",))[0]
            found = rows.get(record_type, [])
            if collapsed.startswith("SELECT COUNT(*)"):
                return [{"n": len(found)}]
            if collapsed.startswith("SELECT MAX(synced_at)"):
                return [{"synced_at": found[0]["synced_at"] if found else None}]
            return found

        monkeypatch.setattr("clients.database.execute_query", fake_query)

        out = reports_module._report_grants("grant report this quarter")

        assert "Partial data" not in out
        assert "NOT complete" not in out
        assert "(CSuite mirror)" in out


# ===========================================================================
# 8. A crashing handler becomes a plain failure line
# ===========================================================================

class TestHandlerGuard:
    def _chain(self, monkeypatch, handler):
        class Module:
            ALLOWED_ROLES = frozenset({"admin", "staff"})

            def can_handle(self, query, **kwargs):
                return True

            handle = staticmethod(handler)

        monkeypatch.setattr("intents.HANDLER_CHAIN", [("boomer", Module())])

    def test_a_raising_handler_returns_a_failure_line(self, monkeypatch, caplog):
        from intents import route_intent

        def handler(query, ctx):
            raise RuntimeError("hubspot exploded")

        self._chain(monkeypatch, handler)
        name, wrapped = route_intent("anything", make_ctx())

        with caplog.at_level(logging.WARNING, logger="intents"):
            out = wrapped("anything", make_ctx())

        assert "boomer hit an error" in out
        assert "hubspot exploded" in out
        assert "may not have completed" in out
        assert any(r.levelno == logging.WARNING for r in caplog.records)

    def test_a_working_handler_is_untouched(self, monkeypatch):
        from intents import route_intent

        self._chain(monkeypatch, lambda query, ctx: "all good")
        _, wrapped = route_intent("anything", make_ctx())

        assert wrapped("anything", make_ctx()) == "all good"

    def test_the_original_handler_is_still_reachable(self, monkeypatch):
        from intents import route_intent

        def handler(query, ctx):
            return "ok"

        self._chain(monkeypatch, handler)
        _, wrapped = route_intent("anything", make_ctx())

        assert wrapped.__wrapped__ is handler

    def test_the_assistant_sees_the_failure_line_not_an_exception(self,
                                                                  monkeypatch):
        from assistant import JidhrAssistant

        def handler(query, ctx):
            raise ValueError("bad input")

        self._chain(monkeypatch, handler)

        a = JidhrAssistant.__new__(JidhrAssistant)
        a.claude = a.hubspot = a.csuite = None
        a.conversation_history = []
        a.services = Services(None, None, None)
        a.draft_state, a.workflow_state = new_draft_state(), {}

        out = a.process_query("anything", ACTOR)

        assert "boomer hit an error" in out
        # assistant.py's own backstop still says "Nothing was changed"; it is
        # only reached for a handler route_intent did not wrap.
        assert "hit an error" in out
