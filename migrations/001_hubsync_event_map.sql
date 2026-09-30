-- ---------------------------------------------------------------------------
-- 001_hubsync_event_map.sql
-- CSuite event dates -> HubSpot marketing events: mapping and run log.
-- ---------------------------------------------------------------------------
-- NOT EXECUTED by anything in this repository. Run it yourself in
-- TablePlus. scripts/event_sync.py refuses to run until both tables
-- exist and reports the fact rather than creating them.
--
-- Written 2026-09-30. See reports/event_sync_discovery.md for why each
-- column is here; the short version:
--
--   * CSuite has NO last-modified field on an event date, so change
--     detection is a content hash over the fields HubSpot actually
--     receives. A change to goal_amount, which HubSpot never sees, must
--     not cause an update.
--   * HubSpot's lookup-by-external-id endpoint 404s on this portal, so a
--     create cannot be made idempotent by asking HubSpot first. This
--     table IS the duplicate guard, and the status column is how an
--     ambiguous create is resolved on the next run instead of retried.
--   * Nothing here is ever deleted by the sync. An event date that
--     vanishes or is archived in CSuite is recorded for review.
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS hubsync;


-- ---------------------------------------------------------------------------
-- event_map — one row per CSuite event date the sync has ever touched
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS hubsync.event_map (
    id                     BIGSERIAL PRIMARY KEY,

    -- The CSuite side. UNIQUE is the duplicate guard: with no working
    -- external-id lookup in HubSpot, this constraint is what stops a
    -- second create.
    csuite_eventdate_id    TEXT NOT NULL UNIQUE,

    -- The HubSpot side. NULL while a create is unresolved — see 'unknown'
    -- below. Not UNIQUE: two CSuite dates pointing at one HubSpot event
    -- would be a bug, but blocking it here would block the repair too.
    hubspot_event_id       TEXT,

    -- The externalEventId actually sent, stored verbatim rather than
    -- recomputed, so a change to the naming convention does not silently
    -- orphan existing rows. Convention as of 2026-09-30: 'csuite-<id>'.
    external_event_id      TEXT,

    -- sha256 over the canonical JSON of the mapped fields only. An
    -- update fires when and only when this changes.
    content_hash           TEXT,

    last_synced_at         TIMESTAMPTZ,

    --   synced   HubSpot matches the hash we last sent
    --   pending  mapped and queued; no HubSpot write has succeeded yet
    --   unknown  a create failed ambiguously (timeout, 5xx, no id in the
    --            body). It may or may not have landed. NEVER retried:
    --            the next run looks the external id up in the listing and
    --            resolves it either way.
    --   review   a human needs to look. Used for: a CSuite date that has
    --            vanished or been archived, a start time with no timezone,
    --            and a date with no time at all.
    --   error    the last write failed for a reason that is safe to retry
    status                 TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('synced', 'pending', 'unknown', 'review', 'error')),

    last_error             TEXT,

    -- Why a row is in review, so the reason survives the next run.
    review_reason          TEXT,

    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The two questions every run asks.
CREATE INDEX IF NOT EXISTS event_map_status_idx
    ON hubsync.event_map (status);
CREATE INDEX IF NOT EXISTS event_map_external_idx
    ON hubsync.event_map (external_event_id);


-- ---------------------------------------------------------------------------
-- run_log — one row per sync run
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS hubsync.run_log (
    id                BIGSERIAL PRIMARY KEY,
    job               TEXT NOT NULL DEFAULT 'event_sync',

    -- FALSE for a dry run. A dry run still writes a row: "we looked and
    -- would have done nothing" is worth being able to prove.
    applied           BOOLEAN NOT NULL DEFAULT FALSE,

    started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at       TIMESTAMPTZ,

    csuite_calls      INTEGER NOT NULL DEFAULT 0,
    hubspot_calls     INTEGER NOT NULL DEFAULT 0,

    event_dates_read  INTEGER NOT NULL DEFAULT 0,
    created_count     INTEGER NOT NULL DEFAULT 0,
    updated_count     INTEGER NOT NULL DEFAULT 0,
    unchanged_count   INTEGER NOT NULL DEFAULT 0,
    skipped_count     INTEGER NOT NULL DEFAULT 0,
    review_count      INTEGER NOT NULL DEFAULT 0,
    failed_count      INTEGER NOT NULL DEFAULT 0,

    status            TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'complete', 'failed')),
    error_summary     TEXT,

    -- Per-record outcomes as JSON, so a run is diagnosable without a
    -- second table. Field VALUES are not stored — only the CSuite id, the
    -- outcome and the reason.
    outcomes          JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS run_log_started_idx
    ON hubsync.run_log (started_at DESC);


-- ---------------------------------------------------------------------------
-- Four rows that already exist in HubSpot, for reference, NOT inserted.
-- ---------------------------------------------------------------------------
-- A private app named "Irritable-Needle" has already created these, with
-- the same 'csuite-' convention (verified 2026-09-30):
--
--   csuite-1153 -> 749259629267    csuite-1157 -> 749288924917
--   csuite-1155 -> 749247088350    csuite-1159 -> 749248889559
--
-- They are deliberately NOT seeded here. The sync discovers them by
-- listing HubSpot's marketing events and adopts them into event_map on
-- its first --apply run, which keeps one code path instead of two and
-- means this file cannot go stale. Three of the four have an incorrect
-- start time and one has the wrong date entirely — see the discovery
-- report — so they will surface as content-hash updates, not as silent
-- matches.
