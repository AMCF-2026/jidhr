"""The cron service (jidhr-jobs) is configured in the Railway dashboard.

V5.80 gave it its own config file, /railway.jobs.toml. That cannot work: on
2026-10-14 the new service refused the config path ("services that have never
used Config as Code cannot opt in") and still applied the ROOT railway.toml —
8 gunicorn workers on a Nixpacks image — so it booted the web app and died on
the missing SECRET_KEY. V5.82 deletes the file. The cron service's settings
live in its dashboard, and app.py refuses to start the web app on a service
marked JIDHR_ROLE=jobs.

V5.84: railway.toml's start command picks the role itself — JIDHR_ROLE=jobs
runs the runner, anything else runs the unchanged gunicorn line — and the file
no longer carries a healthcheck, which would fail every cron deploy.

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


V579_GUNICORN = ("gunicorn app:app --bind 0.0.0.0:$PORT --workers 8 "
                 "--threads 4 --timeout 900 --graceful-timeout 120")


def v579_config():
    text = subprocess.run(["git", "show", "e5fb901:railway.toml"], cwd=ROOT,
                          capture_output=True, text=True, check=True).stdout
    return tomllib.loads(text)


def test_the_web_command_inside_the_switch_is_the_v579_command_exactly():
    """The web branch runs the same gunicorn line, byte for byte."""
    assert v579_config()["deploy"]["startCommand"] == V579_GUNICORN
    start = web_config()["deploy"]["startCommand"]
    assert f"else exec {V579_GUNICORN}; fi" in start
    assert start.count("gunicorn") == 1


def test_only_the_start_command_and_the_healthcheck_changed():
    then, now = v579_config(), web_config()
    assert then["build"] == now["build"]
    for key in ("restartPolicyType", "restartPolicyMaxRetries"):
        assert then["deploy"][key] == now["deploy"][key]
    assert set(then["deploy"]) - set(now["deploy"]) == {
        "healthcheckPath", "healthcheckTimeout"}
    assert set(now["deploy"]) - set(then["deploy"]) == set()
    assert "cronSchedule" not in now["deploy"], \
        "a cron schedule here would apply to WEB too and stop it serving"


def test_there_is_no_healthcheck_in_the_file():
    """It applies to every service, and the runner serves no HTTP: Railway
    marks a deploy failed when the path never answers 2xx. Web's
    healthcheck lives in web's dashboard."""
    deploy = web_config()["deploy"]
    assert "healthcheckPath" not in deploy
    assert "healthcheckTimeout" not in deploy


# --- the start command, executed ------------------------------------------------
#
# `python` and `gunicorn` are stub scripts on PATH that record their argv, so
# the REAL command string runs and nothing real starts. Run both ways Railway
# might: split into argv (exec form) and handed to a shell (shell form).

STUB = """#!/bin/sh
printf '%s' "$(basename "$0")" > "$STUB_OUT"
for arg in "$@"; do printf ' %s' "$arg" >> "$STUB_OUT"; done
"""


def run_start(tmp_path, role, form):
    import shlex

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    for name in ("python", "gunicorn"):
        stub = bin_dir / name
        stub.write_text(STUB)
        stub.chmod(0o755)
    out = tmp_path / "ran.txt"
    env = {"PATH": f"{bin_dir}:/usr/bin:/bin", "PORT": "8080",
           "STUB_OUT": str(out)}
    if role is not None:
        env["JIDHR_ROLE"] = role
    start = web_config()["deploy"]["startCommand"]
    argv = (shlex.split(start) if form == "exec"
            else ["/bin/sh", "-c", start])
    subprocess.run(argv, env=env, check=True, timeout=30)
    return out.read_text()


@pytest.mark.parametrize("form", ["exec", "shell"])
def test_jobs_runs_the_runner_once(tmp_path, form):
    assert run_start(tmp_path, "jobs", form) == \
        "python scripts/jobs_run.py --once"


@pytest.mark.parametrize("form", ["exec", "shell"])
@pytest.mark.parametrize("role", [None, "", "web", "JOBS", "jobs2"])
def test_anything_else_runs_the_v579_gunicorn(tmp_path, role, form):
    """Only the exact value "jobs" picks the runner. A near miss starts
    gunicorn — and app.py's own JIDHR_ROLE guard (V5.82) then refuses a
    case-variant of "jobs", so it still cannot serve web there."""
    assert run_start(tmp_path, role, form) == \
        V579_GUNICORN.replace("$PORT", "8080")


def test_the_web_config_still_serves_the_web_app():
    deploy = web_config()["deploy"]

    assert "exec gunicorn app:app" in deploy["startCommand"]
    assert "cronSchedule" not in deploy


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
