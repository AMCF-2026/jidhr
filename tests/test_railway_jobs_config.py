"""The cron service (jidhr-jobs) is configured in the Railway dashboard.

V5.80 gave it its own config file, /railway.jobs.toml. That cannot work: on
2026-10-14 the new service refused the config path ("services that have never
used Config as Code cannot opt in") and still applied the ROOT railway.toml —
8 gunicorn workers on a Nixpacks image — so it booted the web app and died on
the missing SECRET_KEY. V5.82 deletes the file. The cron service's settings
live in its dashboard, and app.py refuses to start the web app on a service
marked JIDHR_ROLE=jobs.

The guard is tested by importing app.py in a SUBPROCESS with SECRET_KEY and
DATABASE_URL set empty: every case stops at import, before anything connects.
"""

import os
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "railway.toml"


def web_config():
    return tomllib.loads(WEB.read_text())


def test_there_is_no_jobs_config_file():
    """New services cannot use one; a file that is silently ignored only
    misleads the next reader."""
    assert not (ROOT / "railway.jobs.toml").exists()


def test_the_runner_the_cron_service_starts_exists_and_takes_once():
    script = ROOT / "scripts" / "jobs_run.py"
    source = script.read_text()

    assert script.exists()
    assert '"--once"' in source
    assert "sys.exit(main())" in source, "the exit code reaches Railway"


def test_the_web_config_is_byte_identical_to_v579():
    """V5.82 leaves railway.toml alone. Moving web's settings into its
    dashboard and deleting the file is a separate, later commit."""
    diff = subprocess.run(["git", "diff", "--quiet", "e5fb901", "--",
                           "railway.toml"], cwd=ROOT)
    assert diff.returncode == 0, "railway.toml changed"


def test_the_web_config_still_serves_the_web_app():
    deploy = web_config()["deploy"]

    assert deploy["startCommand"].startswith("gunicorn app:app")
    assert deploy["healthcheckPath"] == "/health"
    assert "cronSchedule" not in deploy, \
        "a cron schedule on the WEB service would stop it serving"


# --- JIDHR_ROLE=jobs: the web app refuses to start ----------------------------

def import_app(role):
    env = dict(os.environ, SECRET_KEY="", DATABASE_URL="",
               DATABASE_PUBLIC_URL="")
    env.pop("JIDHR_ROLE", None)
    if role is not None:
        env["JIDHR_ROLE"] = role
    return subprocess.run([sys.executable, "-c", "import app"], cwd=ROOT,
                          env=env, capture_output=True, text=True,
                          timeout=120)


@pytest.mark.parametrize("role", ["jobs", "JOBS", " jobs "])
def test_the_web_app_refuses_to_start_on_the_jobs_service(role):
    found = import_app(role)

    assert found.returncode != 0
    assert "RuntimeError: JIDHR_ROLE=jobs" in found.stderr
    assert "SECRET_KEY is not set" not in found.stderr, \
        "the guard comes first, so the reason is the right one"
    refusals = [line for line in found.stdout.splitlines()
                if "Refusing to start the web app" in line]
    assert len(refusals) == 1, "exactly one clear log line"
    assert "CRITICAL" in refusals[0]


@pytest.mark.parametrize("role", [None, "", "web"])
def test_the_guard_does_not_fire_without_jobs(role):
    """Web does not set it. Unset, empty or anything else passes the guard —
    here, to the SECRET_KEY check, which stops the import in this test."""
    found = import_app(role)

    assert "JIDHR_ROLE=jobs" not in found.stderr + found.stdout
    assert "SECRET_KEY is not set" in found.stderr
