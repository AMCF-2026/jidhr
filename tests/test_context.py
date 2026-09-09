"""Step 1c: RequestContext, role gating, and the end of the service locator.

No database and no network. Services are stub objects — the point of the
refactor is that a handler's dependencies are visible in its signature, so a
test can supply them without constructing an assistant.
"""

import ast
import logging
import pathlib

import pytest

from intents import HANDLER_CHAIN, route_intent
from intents.context import Actor, RequestContext, Services, VALID_ROLES

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
INTENTS_DIR = REPO_ROOT / "intents"


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class _StubClient:
    """Records nothing, does nothing. Presence is the point."""

    def __init__(self, name):
        self.name = name


def make_ctx(role="staff", draft_state=None, workflow_state=None,
             conversation_history=None):
    return RequestContext(
        actor=Actor(user_id=7, email="staff@amuslimcf.org", role=role,
                    csuite_profile_id="19879"),
        services=Services(
            hubspot=_StubClient("hubspot"),
            csuite=_StubClient("csuite"),
            claude=_StubClient("claude"),
        ),
        draft_state=draft_state if draft_state is not None else {},
        workflow_state=workflow_state if workflow_state is not None else {},
        conversation_history=(
            conversation_history if conversation_history is not None else []),
    )


class _FakeHandler:
    """Stands in for a handler module."""

    def __init__(self, allowed=frozenset({"admin", "staff"}), matches=True,
                 raises=None):
        self.ALLOWED_ROLES = allowed
        self._matches = matches
        self._raises = raises
        self.can_handle_calls = 0

    def can_handle(self, query, **kwargs):
        self.can_handle_calls += 1
        if self._raises is not None:
            raise self._raises
        return self._matches

    def handle(self, query, ctx):
        return "handled"


# ---------------------------------------------------------------------------
# a. RequestContext constructs with stub services
# ---------------------------------------------------------------------------

def test_request_context_constructs_with_stub_services():
    ctx = make_ctx()

    assert ctx.actor.user_id == 7
    assert ctx.actor.email == "staff@amuslimcf.org"
    assert ctx.actor.role == "staff"
    assert ctx.actor.csuite_profile_id == "19879"
    assert ctx.services.hubspot.name == "hubspot"
    assert ctx.services.csuite.name == "csuite"
    assert ctx.services.claude.name == "claude"
    assert ctx.draft_state == {}
    assert ctx.workflow_state == {}
    assert ctx.conversation_history == []


def test_actor_is_frozen_so_a_handler_cannot_change_who_is_asking():
    ctx = make_ctx()
    with pytest.raises(Exception):
        ctx.actor.role = "admin"


def test_state_is_shared_by_reference_not_copied():
    """Handlers mutate in place; the assistant saves the same objects back."""
    draft = {"active": False}
    history = []
    ctx = make_ctx(draft_state=draft, conversation_history=history)

    ctx.draft_state["active"] = True
    ctx.conversation_history.append({"role": "user", "content": "hi"})

    assert draft["active"] is True
    assert len(history) == 1


def test_is_allowed_reads_the_actor_role():
    assert make_ctx(role="staff").is_allowed(frozenset({"admin", "staff"}))
    assert make_ctx(role="admin").is_allowed(frozenset({"admin", "staff"}))
    assert not make_ctx(role="donor").is_allowed(frozenset({"admin", "staff"}))


# ---------------------------------------------------------------------------
# b. The router gates on role before consulting can_handle
# ---------------------------------------------------------------------------

def test_router_skips_a_handler_the_actor_role_excludes(monkeypatch):
    staff_only = _FakeHandler(allowed=frozenset({"admin", "staff"}))
    monkeypatch.setattr("intents.HANDLER_CHAIN", [("staff_only", staff_only)])

    assert route_intent("anything", make_ctx(role="donor")) is None
    # Not merely unmatched — never even asked.
    assert staff_only.can_handle_calls == 0


def test_router_calls_a_handler_the_actor_role_includes(monkeypatch):
    staff_only = _FakeHandler(allowed=frozenset({"admin", "staff"}))
    monkeypatch.setattr("intents.HANDLER_CHAIN", [("staff_only", staff_only)])

    match = route_intent("anything", make_ctx(role="staff"))

    assert match is not None
    name, handler = match
    assert name == "staff_only"
    assert handler("anything", make_ctx()) == "handled"
    assert staff_only.can_handle_calls == 1


def test_router_keeps_priority_order_among_permitted_handlers(monkeypatch):
    first = _FakeHandler(matches=False)
    second = _FakeHandler(matches=True)
    third = _FakeHandler(matches=True)
    monkeypatch.setattr("intents.HANDLER_CHAIN",
                        [("first", first), ("second", second), ("third", third)])

    name, _ = route_intent("q", make_ctx())

    assert name == "second"
    assert third.can_handle_calls == 0, "routing must stop at the first match"


def test_router_passes_draft_and_workflow_state_to_can_handle(monkeypatch):
    seen = {}

    class _Recorder(_FakeHandler):
        def can_handle(self, query, **kwargs):
            seen.update(kwargs)
            return False

    monkeypatch.setattr("intents.HANDLER_CHAIN", [("rec", _Recorder())])
    ctx = make_ctx(draft_state={"active": True}, workflow_state={"step": "x"})
    route_intent("q", ctx)

    assert seen["draft_state"] == {"active": True}
    assert seen["workflow_state"] == {"step": "x"}


def test_handler_with_no_allowed_roles_attribute_is_skipped(monkeypatch):
    """Fail closed: a module that forgot to declare roles is not reachable."""
    class _Undeclared:
        def can_handle(self, query, **kwargs):
            return True

        def handle(self, query, ctx):
            return "should not run"

    monkeypatch.setattr("intents.HANDLER_CHAIN", [("undeclared", _Undeclared())])
    assert route_intent("q", make_ctx(role="admin")) is None


# ---------------------------------------------------------------------------
# c. A raising can_handle is skipped, and says so
# ---------------------------------------------------------------------------

def test_raising_can_handle_is_skipped_and_logged_at_warning(monkeypatch,
                                                             caplog):
    broken = _FakeHandler(raises=ValueError("boom"))
    good = _FakeHandler(matches=True)
    monkeypatch.setattr("intents.HANDLER_CHAIN",
                        [("broken", broken), ("good", good)])

    with caplog.at_level(logging.WARNING, logger="intents"):
        match = route_intent("q", make_ctx())

    # Routing survives and reaches the next handler.
    assert match is not None and match[0] == "good"

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, "a handler that raises must not fail silently"
    joined = " ".join(r.getMessage() for r in warnings)
    assert "broken" in joined
    assert "boom" in joined


def test_a_raising_handler_does_not_stop_the_chain(monkeypatch, caplog):
    broken = _FakeHandler(raises=RuntimeError("nope"))
    monkeypatch.setattr("intents.HANDLER_CHAIN", [("broken", broken)])

    with caplog.at_level(logging.WARNING, logger="intents"):
        assert route_intent("q", make_ctx()) is None


# ---------------------------------------------------------------------------
# d. Every routed module declares valid roles
# ---------------------------------------------------------------------------

def _routed_modules():
    from intents import queries
    return list(HANDLER_CHAIN) + [("queries", queries)]


@pytest.mark.parametrize("name,module", _routed_modules())
def test_every_handler_declares_allowed_roles(name, module):
    assert hasattr(module, "ALLOWED_ROLES"), f"{name} has no ALLOWED_ROLES"


@pytest.mark.parametrize("name,module", _routed_modules())
def test_allowed_roles_is_a_frozenset_of_valid_roles(name, module):
    roles = module.ALLOWED_ROLES
    assert isinstance(roles, frozenset), f"{name}.ALLOWED_ROLES is {type(roles)}"
    assert roles, f"{name}.ALLOWED_ROLES is empty — nobody could reach it"
    unknown = roles - VALID_ROLES
    assert not unknown, f"{name} allows unknown role(s): {sorted(unknown)}"


def test_nothing_is_donor_facing_yet():
    for name, module in _routed_modules():
        assert "donor" not in module.ALLOWED_ROLES, (
            f"{name} is donor-facing; step 1c expects staff-and-above only")


# ---------------------------------------------------------------------------
# e. No intents/ module reaches into an object named `assistant`
# ---------------------------------------------------------------------------

class _AssistantAttributeVisitor(ast.NodeVisitor):
    """Finds `assistant.<anything>` attribute access."""

    def __init__(self):
        self.hits = []

    def visit_Attribute(self, node):
        value = node.value
        if isinstance(value, ast.Name) and value.id == "assistant":
            self.hits.append((node.lineno, f"assistant.{node.attr}"))
        self.generic_visit(node)


def _intents_modules():
    return sorted(p for p in INTENTS_DIR.glob("*.py"))


@pytest.mark.parametrize("path", _intents_modules(), ids=lambda p: p.name)
def test_no_intents_module_reaches_into_an_assistant(path):
    """The service locator is gone: handlers get ctx, not the assistant.

    Comments and docstrings are invisible to ast, which is deliberate — a
    docstring mentioning assistant.py is prose, not coupling.
    """
    visitor = _AssistantAttributeVisitor()
    visitor.visit(ast.parse(path.read_text(), filename=str(path)))

    assert not visitor.hits, (
        f"{path.name} still reaches into `assistant`: "
        + ", ".join(f"line {line}: {expr}" for line, expr in visitor.hits))


@pytest.mark.parametrize("path", _intents_modules(), ids=lambda p: p.name)
def test_no_intents_module_takes_a_parameter_named_assistant(path):
    """The rename is complete only if the parameter is gone too."""
    tree = ast.parse(path.read_text(), filename=str(path))
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = node.args
            names = [a.arg for a in
                     args.posonlyargs + args.args + args.kwonlyargs]
            if "assistant" in names:
                offenders.append(f"line {node.lineno}: {node.name}()")

    assert not offenders, f"{path.name}: {', '.join(offenders)}"


def test_content_package_does_not_import_from_intents():
    """content/ is a layer below intents/ and must not depend upward."""
    offenders = []
    for path in sorted((REPO_ROOT / "content").glob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split(".")[0] == "intents":
                        offenders.append(f"{path.name}:{node.lineno}")
            elif isinstance(node, ast.ImportFrom):
                if (node.module or "").split(".")[0] == "intents":
                    offenders.append(f"{path.name}:{node.lineno}")

    assert not offenders, f"content/ imports intents/: {offenders}"


# ---------------------------------------------------------------------------
# The assistant builds a context rather than handing itself out
# ---------------------------------------------------------------------------

def test_assistant_build_context_accepts_a_users_row():
    from assistant import JidhrAssistant

    assistant = JidhrAssistant()
    ctx = assistant.build_context({
        "id": 42, "email": "carl@amuslimcf.org", "role": "admin",
        "csuite_profile_id": 19879,
    })

    assert isinstance(ctx, RequestContext)
    assert ctx.actor.user_id == 42
    assert ctx.actor.role == "admin"
    assert ctx.actor.csuite_profile_id == "19879"
    # The live state objects, not copies.
    assert ctx.draft_state is assistant.draft_state
    assert ctx.workflow_state is assistant.workflow_state
    assert ctx.conversation_history is assistant.conversation_history
    assert ctx.services is assistant.services


def test_assistant_build_context_accepts_an_actor_directly():
    from assistant import JidhrAssistant

    actor = Actor(user_id=1, email="a@b.org", role="staff")
    ctx = JidhrAssistant().build_context(actor)

    assert ctx.actor is actor


def test_assistant_reuses_one_services_instance():
    from assistant import JidhrAssistant

    assistant = JidhrAssistant()
    first = assistant.build_context(Actor(1, "a@b.org", "staff"))
    second = assistant.build_context(Actor(1, "a@b.org", "staff"))

    assert first.services is second.services
    assert first.services.hubspot is assistant.hubspot
