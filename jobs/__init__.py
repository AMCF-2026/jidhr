"""
Jobs
====
A small Postgres-backed job queue for work that should not run inside a
web request: the nightly CSuite mirror refresh, to start with.

    jobs.runner            claim / run / finish, retries, daily recurrence
    jobs.handlers.*        one module per job_type, registered with the runner
    scripts/jobs_run.py    the CLI Railway's cron service invokes

Importing `jobs.handlers` is what registers every handler; the runner
itself knows nothing about specific job types.
"""
