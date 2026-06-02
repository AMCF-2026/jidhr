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
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_content_lookup
    ON content_history (content_type, channel, sent_at DESC);
