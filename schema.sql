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
-- TODO: reconcile with live schema
-- ===========================================================================
-- Step 1c asked for "the nine v2 tables exactly as they exist in the live
-- database". Only two are reachable from this repository:
--
--     content_history   -- defined above
--     users             -- defined below, from clients/users.py's docstring
--
-- Those are the only table names any code here references (every call site
-- goes through clients.database.execute_query; see clients/users.py,
-- content/content_memory.py, content/social_capture.py,
-- content/content_analysis.py — all of which touch only those two).
--
-- The remaining seven table names are NOT in this repository, and nothing was
-- run against the database to discover them. They are deliberately left blank
-- rather than guessed: an invented column list here would be worse than an
-- absent one, because this file claims to be the source of truth.
--
-- To finish this section, paste the output of:
--     pg_dump --schema-only --no-owner --no-privileges "$DATABASE_URL"
-- and drop in the CREATE TABLE blocks for the seven missing tables.
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
