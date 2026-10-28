"""V5.97.2: config.py loads .env for the app, never for the test suite.

With a real .env in the folder its flags leaked into the suite and 152
tests failed (2026-10-28). Neither test here reads a .env: load_dotenv is
replaced by a recorder, so what is checked is whether it is CALLED.
"""

import importlib
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_config_does_not_load_dotenv_under_pytest(monkeypatch):
    import dotenv

    calls = []
    monkeypatch.setattr(dotenv, "load_dotenv",
                        lambda *a, **k: calls.append(1) or True)
    saved = sys.modules.pop("config", None)
    try:
        importlib.import_module("config")
    finally:
        # Put the original module back so nothing else sees a new Config.
        if saved is not None:
            sys.modules["config"] = saved
        else:
            sys.modules.pop("config", None)

    assert calls == []


def test_the_app_still_loads_dotenv_when_pytest_is_absent():
    # A fresh interpreter that never imports pytest, as the app runs.
    code = (
        "import sys, dotenv\n"
        "calls = []\n"
        "dotenv.load_dotenv = lambda *a, **k: calls.append(1) or True\n"
        "import config\n"
        "assert 'pytest' not in sys.modules\n"
        "print('calls', len(calls))\n")
    env = {k: v for k, v in os.environ.items()
           if k in ("PATH", "HOME", "SYSTEMROOT")}
    out = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT,
                         env=env, capture_output=True, text=True,
                         timeout=60)

    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "calls 1"
