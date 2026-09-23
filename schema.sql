-- Jidhr database schema.
-- This is documentation only — NOT auto-executed at deploy time.
-- To apply: paste into Railway's Postgres console.
-- Source of truth for the production schema.

CREATE TABLE IF NOT EXISTS content_history (
    id              SERIAL PRIMARY KEY,
    content_type    TEXT NOT NULL,
    channel         TEXT,
    external_id     TEXT,
    title           TEXT,
    topics          JSONB,
    summary         TEXT,
    cta             TEXT,
    full_body       TEXT,
    sent_at         TIMESTAMP NOT NULL,
    logged_by       TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    source_url      TEXT
);

CREATE INDEX IF NOT EXISTS idx_content_lookup
    ON content_history (content_type, channel, sent_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_content_external
    ON content_history (content_type, external_id);

-- ===========================================================================
-- Reconciling with the live schema
-- ===========================================================================
-- Only `content_history` and `users` were ever transcribed from DDL. The
-- other v2 tables this code writes to — csuite_mirror, sync_runs,
-- sync_staging, write_audit — are still known only by the column lists
-- their briefs carried (see clients/audit.py, sync/mirror.py). They are
-- not reproduced here because a guessed CREATE TABLE in a file that calls
-- itself the source of truth is worse than an absent one.
--
-- To finish, paste the output of:
--     pg_dump --schema-only --no-owner --no-privileges "$DATABASE_URL"
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- users — who may sign in, and at what level.
--
-- Transcribed from clients/users.py's module docstring, which is what that
-- module's queries assume. NOT verified against the live database: if the two
-- ever disagree, the live database wins and this block is the thing to fix.
--
-- The UNIQUE constraint on email is load-bearing: get_or_create_user()'s
-- ON CONFLICT (email) DO UPDATE has nothing to conflict on without it, and
-- concurrent logins would insert duplicate rows.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS users (
    id                  BIGSERIAL PRIMARY KEY,
    email               TEXT UNIQUE NOT NULL,
    display_name        TEXT,
    role                TEXT NOT NULL DEFAULT 'staff'
                        CHECK (role IN ('admin', 'staff', 'donor')),
    csuite_profile_id   BIGINT,
    hubspot_contact_id  TEXT,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    last_login_at       TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ---------------------------------------------------------------------------
-- jobs — the queue behind scripts/jobs_run.py and jobs/runner.py.
--
-- RECONSTRUCTED FROM information_schema (Step 3b brief, 2026-09-15): the
-- column names and defaults are exact; the types are best-effort where the
-- brief gave only a family ("timestamptz", "int"). NOT verified against a
-- pg_dump. If the two ever disagree, the live database wins and this block
-- is the thing to fix.
--
-- There is deliberately NO schedule column. Recurrence is in payload:
--     {"recurring": "daily", "at": "06:00"}      (UTC)
-- and jobs/runner.py queues the next occurrence after each finish.
--
-- Status values the runner uses: 'queued', 'running', 'complete', 'failed'.
-- If the live table carries a CHECK constraint with a different vocabulary,
-- jobs/runner.py's STATUS_* constants are what to change.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS jobs (
    id              BIGSERIAL PRIMARY KEY,
    job_type        TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT NOT NULL DEFAULT 'queued',
    priority        INTEGER NOT NULL DEFAULT 100,
    run_after       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    requested_by    BIGINT REFERENCES users(id),
    sync_run_id     BIGINT,
    claimed_by      TEXT,
    claimed_at      TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    result          JSONB,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The claim query's access path: status + run_after, ordered by priority.
-- Not confirmed to exist live; harmless if it already does.
CREATE INDEX IF NOT EXISTS idx_jobs_claimable
    ON jobs (status, run_after, priority);


-- ---------------------------------------------------------------------------
-- csuite_donations — PROPOSED, NOT APPLIED.
--
-- Since 2026-09-17 individual gifts are mirrored as jsonb rows in
-- csuite_mirror (record_type = 'donation', see sync/mirror.py
-- DONATION_FIELDS). That needs no DDL and is the live shape. This typed
-- table is the upgrade path ONLY if date-range or lapsed-donor queries over
-- ~27k jsonb rows prove too slow. Do not create it on speculation; if it is
-- created, sync/mirror.py's donation gatherer is what changes to fill it.
--
-- No donor name, email, address, card or bank column: none exists on the
-- CSuite record, and the name lives on the profile row already.
-- ---------------------------------------------------------------------------
--
-- CREATE TABLE csuite_donations (
--     donation_id        BIGINT PRIMARY KEY,
--     donation_guid      UUID,
--     profile_id         BIGINT NOT NULL,          -- joins csuite_mirror 'profile'
--     funit_id           BIGINT NOT NULL,          -- joins csuite_mirror 'fund'
--     donation_date      DATE NOT NULL,
--     donation_amount    NUMERIC(14,2) NOT NULL,
--     donation_status    TEXT,
--     anonymous_donation BOOLEAN NOT NULL DEFAULT FALSE,
--     payment_method_id  INTEGER,
--     synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
--     run_id             BIGINT REFERENCES sync_runs(id),
--     expires_at         TIMESTAMPTZ                -- NOW() + 96h, donor-derived
-- );
-- CREATE INDEX idx_csuite_donations_date    ON csuite_donations (donation_date);
-- CREATE INDEX idx_csuite_donations_profile ON csuite_donations (profile_id);
-- CREATE INDEX idx_csuite_donations_fund    ON csuite_donations (funit_id, donation_date);
