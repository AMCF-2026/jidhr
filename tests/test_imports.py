"""Import-time foundation tests (Step 1a).

Every import is performed inside a test body rather than at module
scope. Module-level imports would run during pytest's collection phase,
before the autouse DATABASE_URL fixture in conftest.py has a chance to
clear the environment — which would make these tests pass or fail based
on the developer's shell rather than on the code.
"""

import ast
import importlib
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


def test_import_intents_succeeds_without_database_url():
    """a. `import intents` must not need a database."""
    assert importlib.import_module("intents") is not None


def test_import_content_queue_check_succeeds_without_database_url():
    """b. `import content.queue_check` must not need a database."""
    assert importlib.import_module("content.queue_check") is not None


def test_import_content_content_memory_succeeds_without_database_url():
    """c. content_memory lives in content/ now and imports cleanly."""
    assert importlib.import_module("content.content_memory") is not None


def test_intents_content_memory_no_longer_exists():
    """d. The old intents/ path must not resolve after the move."""
    with pytest.raises(ImportError):
        importlib.import_module("intents.content_memory")


def test_is_configured_false_without_database_url():
    """e. is_configured() reports missing config without connecting."""
    database = importlib.import_module("clients.database")
    assert database.is_configured() is False


def test_execute_query_raises_runtime_error_without_database_url():
    """f. The pool is lazy: the failure surfaces at query time, not import."""
    database = importlib.import_module("clients.database")
    with pytest.raises(RuntimeError) as exc_info:
        database.execute_query("SELECT 1")
    assert "DATABASE_URL" in str(exc_info.value)


def test_content_package_has_no_import_edge_into_intents():
    """g. content/ is a leaf relative to intents/ — no edges back."""
    offenders = []
    for path in sorted((REPO_ROOT / "content").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "intents" or alias.name.startswith("intents."):
                        offenders.append(
                            f"{path.name}:{node.lineno} imports {alias.name}"
                        )
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module == "intents" or module.startswith("intents."):
                    offenders.append(
                        f"{path.name}:{node.lineno} imports from {module}"
                    )
    assert offenders == [], (
        "content/ must not import from intents/: " + "; ".join(offenders)
    )
