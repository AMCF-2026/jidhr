"""The jidhr-jobs service's own config file.

Without /railway.jobs.toml set as its config path, jidhr-jobs would read the
web service's /railway.toml — and Railway's config-as-code OVERRIDES the
dashboard, so the web's gunicorn startCommand would win over any start
command typed in. A second web app never exits, a cron service whose run
stays Active has every later run SKIPPED, and nothing would notice. These
tests pin the file that prevents that, and pin the web's file as untouched.
"""

import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JOBS = ROOT / "railway.jobs.toml"
WEB = ROOT / "railway.toml"


def jobs_config():
    return tomllib.loads(JOBS.read_text())


def web_config():
    return tomllib.loads(WEB.read_text())


def test_the_jobs_service_runs_the_runner_not_the_web_app():
    start = jobs_config()["deploy"]["startCommand"]

    assert start == "python scripts/jobs_run.py --once"
    assert "gunicorn" not in start, "that is the web app"
    assert "--once" in start, "a cron run must claim, run and EXIT"


def test_it_runs_at_0615_utc_daily():
    cron = jobs_config()["deploy"]["cronSchedule"]

    assert cron == "15 6 * * *"
    assert len(cron.split()) == 5, "a standard 5-field cron expression"


def test_it_never_restarts():
    """A restart would only rerun --once; a failure is the ALERT email's
    job, not a retry's."""
    assert jobs_config()["deploy"]["restartPolicyType"] == "NEVER"


def test_it_has_no_healthcheck():
    """The runner serves no HTTP, so any healthcheck would fail the deploy."""
    deploy = jobs_config()["deploy"]
    assert "healthcheckPath" not in deploy
    assert "healthcheckTimeout" not in deploy


def test_it_builds_the_same_image_as_the_web_service():
    assert jobs_config()["build"] == web_config()["build"]


def test_the_runner_it_starts_exists_and_takes_once():
    script = ROOT / "scripts" / "jobs_run.py"
    source = script.read_text()

    assert script.exists()
    assert '"--once"' in source
    assert "sys.exit(main())" in source, "the exit code reaches Railway"


def test_the_web_config_is_byte_identical_to_v579():
    """The web service's file must not change: the fix is a SECOND file, not
    an edit to the one serving production."""
    diff = subprocess.run(["git", "diff", "--quiet", "e5fb901", "--",
                           "railway.toml"], cwd=ROOT)
    assert diff.returncode == 0, "railway.toml changed"


def test_the_web_config_still_serves_the_web_app():
    """Belt and braces: the web's own settings are what they were."""
    deploy = web_config()["deploy"]

    assert deploy["startCommand"].startswith("gunicorn app:app")
    assert deploy["healthcheckPath"] == "/health"
    assert "cronSchedule" not in deploy, \
        "a cron schedule on the WEB service would stop it serving"


def test_the_jobs_file_says_where_to_point_the_service():
    """The path is set in the dashboard and nowhere else, so the file has to
    say what it is."""
    text = JOBS.read_text()

    assert "/railway.jobs.toml" in text
    assert "Config-as-code" in text
