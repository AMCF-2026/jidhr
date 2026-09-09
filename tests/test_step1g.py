"""Step 1g: the fund pick reaches production, one draft shape, real links,
and chat traffic logged in full.

No network — clients are stubs.
"""

import ast
import pathlib

import pytest

from intents.context import DEFAULT_DRAFT_STATE, Actor, new_draft_state
from intents.queries import fund_url

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

RAW = "Ramadan Relief Fund-(DAF0101)"
RAW_2 = "Ramadan Iftar Fund-(DAF0102)"


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class FundCSuite:
    """Two similarly-named funds, so a bare name is always ambiguous."""

    def __init__(self):
        self.display_calls = []

    def search_funds(self, term):
        return {"success": True, "data": {"results": [
            {"id": 111, "name": RAW},
            {"id": 222, "name": RAW_2},
        ]}}

    def get_fund(self, fund_id):
        self.display_calls.append(fund_id)
        balances = {111: "1200.00", 222: "7500.00"}
        names = {111: RAW, 222: RAW_2}
        return {"success": True, "data": {
            "funit_id": fund_id,
            "fund_name": names.get(fund_id, "Unknown"),
            "current_fundbalance": balances.get(fund_id, "0.00"),
        }}

    def get_funds(self, limit=20, offset=0):
        return {"success": True, "data": {"results": []}}

    def get_grants_by_fund(self, fund_id, limit=10):
        return {"success": True, "data": {"results": []}}


class SilentClaude:
    def __init__(self):
        self.prompts = []

    def chat(self, messages=None, system_prompt=None, **kwargs):
        self.prompts.append(messages)
        return "ack"


def make_assistant(csuite):
    """A JidhrAssistant with its three clients replaced by stubs."""
    from assistant import JidhrAssistant
    from intents.context import Services

    assistant = JidhrAssistant.__new__(JidhrAssistant)
    assistant.claude = SilentClaude()
    assistant.hubspot = None
    assistant.csuite = csuite
    assistant.conversation_history = []
    assistant.services = Services(hubspot=None, csuite=csuite,
                                  claude=assistant.claude)
    assistant.draft_state = new_draft_state()
    assistant.workflow_state = {}
    return assistant


ACTOR = Actor(user_id=1, email="staff@amuslimcf.org", role="staff")


# ---------------------------------------------------------------------------
# 1. The pick flow works through process_query, not just the gatherer
# ---------------------------------------------------------------------------

def test_assistant_threads_workflow_state_into_the_fallback_gatherer():
    csuite = FundCSuite()
    assistant = make_assistant(csuite)

    assistant.process_query("balance for Ramadan", ACTOR)

    assert "pending_fund_pick" in assistant.workflow_state, (
        "gather_context must receive workflow_state or the pick is inert")


def test_a_bare_digit_resolves_the_fund_through_process_query():
    csuite = FundCSuite()
    assistant = make_assistant(csuite)

    assistant.process_query("balance for Ramadan", ACTOR)
    assert csuite.display_calls == []

    assistant.process_query("2", ACTOR)

    assert csuite.display_calls == [222]
    assert "pending_fund_pick" not in assistant.workflow_state


def test_the_picked_fund_balance_reaches_the_model_prompt():
    csuite = FundCSuite()
    assistant = make_assistant(csuite)

    assistant.process_query("balance for Ramadan", ACTOR)
    assistant.process_query("1", ACTOR)

    # conversation_history is mutated in place, so [-1] is the assistant's
    # reply by now. The enhanced message is the last user turn.
    history = assistant.claude.prompts[-1]
    last_prompt = [m for m in history if m["role"] == "user"][-1]["content"]
    assert "$1,200.00" in last_prompt
    assert "Ramadan Relief Fund" in last_prompt


def test_an_unrelated_message_clears_the_pending_pick_end_to_end():
    csuite = FundCSuite()
    assistant = make_assistant(csuite)

    assistant.process_query("balance for Ramadan", ACTOR)
    assistant.process_query("who is Ahmed", ACTOR)

    assert "pending_fund_pick" not in assistant.workflow_state


def test_gather_context_still_works_without_workflow_state():
    """The parameter is optional; older callers must not break."""
    from intents.queries import gather_context

    context = gather_context("balance for Ramadan", None, FundCSuite())
    assert "Ramadan Relief Fund" in context


# ---------------------------------------------------------------------------
# 2. One draft-shape definition
# ---------------------------------------------------------------------------

DRAFT_KEYS = frozenset(DEFAULT_DRAFT_STATE)


def _dict_literals_with_draft_keys(path):
    """Dict literals in `path` whose keys are a draft shape."""
    tree = ast.parse(path.read_text(), filename=str(path))
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        keys = {k.value for k in node.keys
                if isinstance(k, ast.Constant) and isinstance(k.value, str)}
        # A literal that covers most of the shape is a second definition.
        if len(keys & DRAFT_KEYS) >= len(DRAFT_KEYS) - 1 and keys:
            found.append(node.lineno)
    return found


@pytest.mark.parametrize("filename", ["assistant.py", "intents/content.py"])
def test_no_second_draft_shape_literal(filename):
    path = REPO_ROOT / filename
    offenders = _dict_literals_with_draft_keys(path)

    assert not offenders, (
        f"{filename} redefines the draft shape at line(s) {offenders}; "
        "import DEFAULT_DRAFT_STATE from intents.context instead")


def test_the_shape_lives_in_context_and_includes_created_at():
    assert "created_at" in DEFAULT_DRAFT_STATE
    assert DEFAULT_DRAFT_STATE["active"] is False


def test_new_draft_state_returns_a_copy_not_the_shared_dict():
    first, second = new_draft_state(), new_draft_state()
    first["body"] = "mutated"

    assert second["body"] is None
    assert DEFAULT_DRAFT_STATE["body"] is None, "the default must stay pristine"


@pytest.mark.parametrize("name", ["_DEFAULT_DRAFT", "_EMPTY_DRAFT"])
def test_the_old_private_shapes_are_gone(name):
    import assistant
    import intents.content as content

    assert not hasattr(assistant.JidhrAssistant, name)
    assert not hasattr(content, name)


def test_a_cleared_draft_matches_the_shared_shape_exactly():
    from intents.content import _clear_draft_state
    from intents.context import RequestContext, Services

    draft = {"active": True, "body": "x", "pending_schedule": {"when": "now"},
             "stray": 1}
    ctx = RequestContext(actor=ACTOR,
                         services=Services(None, None, None),
                         draft_state=draft, workflow_state={},
                         conversation_history=[])

    _clear_draft_state(ctx)

    assert set(draft) == set(DEFAULT_DRAFT_STATE)
    assert draft == DEFAULT_DRAFT_STATE


# ---------------------------------------------------------------------------
# 3. The CSuite fund link is built in code
# ---------------------------------------------------------------------------

def test_fund_url_uses_the_shared_config_constant():
    from config import Config

    assert fund_url(1046) == Config.CSUITE_FUND_URL.format(funit_id=1046)
    assert fund_url(1046).endswith("funit_id=1046")
    assert "/erp/funit/display" in fund_url(1046)


@pytest.mark.parametrize("value", [None, ""])
def test_fund_url_declines_a_missing_id(value):
    assert fund_url(value) is None


def test_the_detail_context_carries_a_finished_link():
    from intents.queries import _gather_fund_context

    csuite = FundCSuite()
    joined = "\n".join(_gather_fund_context("fund 222", "fund 222", csuite, {}))

    assert "CSuite link: " in joined
    assert fund_url(222) in joined


def test_candidate_lines_carry_links_too():
    from intents.queries import _gather_fund_context

    csuite = FundCSuite()
    joined = "\n".join(
        _gather_fund_context("balance for Ramadan", "balance for ramadan",
                             csuite, {}))

    assert fund_url(111) in joined
    assert fund_url(222) in joined


def test_queries_does_not_hand_roll_the_csuite_host():
    """The URL must come from config, not a string literal in the gatherer."""
    source = (REPO_ROOT / "intents" / "queries.py").read_text()

    assert "CSUITE_FUND_URL" in source
    assert "fcsuite.com" not in source, "hard-coded host in queries.py"


# ---------------------------------------------------------------------------
# 4. Chat logging is full-length and single-line
# ---------------------------------------------------------------------------

@pytest.fixture
def app_module(monkeypatch):
    import importlib
    import sys

    monkeypatch.setenv("SECRET_KEY", "test-secret-key")
    monkeypatch.setenv("DATABASE_URL", "")
    for name in ("app", "auth", "assistant", "config"):
        sys.modules.pop(name, None)
    return importlib.import_module("app")


def test_newlines_become_pipes_so_one_message_is_one_log_record(app_module):
    flattened = app_module.flatten_for_log("line one\nline two\n\n  line three  ")

    assert flattened == "line one | line two | line three"
    assert "\n" not in flattened


def test_a_long_response_is_capped_and_says_so(app_module):
    flattened = app_module.flatten_for_log("x" * 5000)

    assert len(flattened) < 5000
    assert "truncated" in flattened
    assert "5000 chars total" in flattened


def test_a_response_under_the_cap_is_logged_whole(app_module):
    body = "y" * 3000
    assert app_module.flatten_for_log(body) == body


def test_the_cap_is_four_thousand(app_module):
    assert app_module.CHAT_LOG_MAX_CHARS == 4000


def test_none_and_empty_are_safe(app_module):
    assert app_module.flatten_for_log(None) == ""
    assert app_module.flatten_for_log("") == ""


def test_chat_no_longer_truncates_at_one_hundred_characters():
    source = (REPO_ROOT / "app.py").read_text()

    assert "message[:100]" not in source
    assert "response[:100]" not in source
    assert 'log_user_action("Chat request", flatten_for_log(message))' in source
    assert 'log_user_action("Chat response", flatten_for_log(response))' in source


def test_the_request_log_line_carries_the_actor_email(app_module, caplog):
    import logging

    class FakeUser:
        is_authenticated = True
        email = "staff@amuslimcf.org"

    monkey = pytest.MonkeyPatch()
    monkey.setattr(app_module, "current_user", FakeUser())
    try:
        with caplog.at_level(logging.INFO, logger="app"):
            app_module.log_user_action("Chat request", "balance for Ramadan")
    finally:
        monkey.undo()

    message = caplog.records[-1].getMessage()
    assert "staff@amuslimcf.org" in message
    assert "Chat request" in message
    assert "balance for Ramadan" in message
