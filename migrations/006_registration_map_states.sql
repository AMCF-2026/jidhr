-- 006: widen registration_map's status and last_state CHECK constraints.
--
-- DO NOT RUN THIS AUTOMATICALLY. Carl runs migrations in TablePlus.
--
-- WHY
-- ---
-- Two CHECK constraints from 005 reject values the code now writes. Both
-- were measured against production on 2026-10-09 by attempting the real
-- INSERT inside a transaction that was rolled back:
--
--   status = 'unverified'
--     -> CheckViolation: new row for relation "registration_map" violates
--        check constraint "registration_map_status_check"
--
--   last_state = 'NO_SHOW'
--     -> CheckViolation: new row for relation "registration_map" violates
--        check constraint "registration_map_last_state_check"
--
-- The first is why run_log 34 reported "registration_map could NOT be
-- updated" and left ZERO rows for event 1155. hotfix-50 introduced the
-- 'unverified' status without widening the constraint, and every test for
-- it faked record_registration, so nothing caught it.
--
-- The second would have broken this hotfix: HubSpot returned
-- attendanceState NO_SHOW for the 1155 registration, and that state is now
-- what gets stored.
--
-- 'NO_SHOW' is one of exactly four documented attendanceState values. The
-- API enumerates them itself when given a bad one: "State value should be
-- one of REGISTERED, CANCELLED, ATTENDED, NO_SHOW".
--
-- Safe to run twice: each constraint is dropped IF EXISTS before being
-- recreated, and widening a CHECK cannot reject a row already stored.

BEGIN;

ALTER TABLE hubsync.registration_map
    DROP CONSTRAINT IF EXISTS registration_map_status_check;

ALTER TABLE hubsync.registration_map
    ADD CONSTRAINT registration_map_status_check
    CHECK (status IN (
        'synced',       -- written to HubSpot and confirmed on read-back
        'pending',      -- the column default; never written by the sync
        'withheld',     -- deliberately not sent
        'unknown',      -- ambiguous write: no HTTP status came back
        'unverified',   -- 2xx, but the read-back could not confirm it
        'review',       -- confirmed as something unexpected (e.g. CANCELLED)
        'error'         -- kept for rows written before hotfix-46
    ));

ALTER TABLE hubsync.registration_map
    DROP CONSTRAINT IF EXISTS registration_map_last_state_check;

-- The four documented HubSpot attendanceState values, and nothing else: a
-- state this sync has never seen should fail loudly rather than be stored.
ALTER TABLE hubsync.registration_map
    ADD CONSTRAINT registration_map_last_state_check
    CHECK (last_state IN ('REGISTERED', 'ATTENDED', 'CANCELLED', 'NO_SHOW'));

COMMIT;
