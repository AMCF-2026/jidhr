-- 007: let write_audit record LOCAL bookkeeping as target_system 'jidhr'.
--
-- DO NOT RUN THIS AUTOMATICALLY. Carl runs migrations in TablePlus.
-- Run it BEFORE "reconcile daf tasks" (the live run). The dry run needs
-- nothing; the live run checks for this constraint first and refuses,
-- saying so, rather than discovering it as a CheckViolation mid-run.
--
-- WHY
-- ---
-- write_audit_target_system_check allows only 'hubspot' and 'csuite'.
-- V5.79's DAF reconcile writes "submission/link/<reason>" rows that tie a
-- HubSpot submission to work done before tasks carried a submission id.
-- Those rows are NOT CSuite writes — no request goes anywhere — so tagging
-- them 'csuite' would be false, and it would also be wrong in effect:
-- sync/sandbox_writes reconciles a write budget by COUNTING write_audit
-- rows per target_system, and a link row counted there would read as "more
-- writes were audited than counted — the cap did not hold".
--
-- This is the same class of failure as migration 006: code that writes a
-- value the CHECK forbids. 006 was found by a production CheckViolation;
-- this one is written first.
--
-- Safe to run twice: the constraint is dropped IF EXISTS and recreated, and
-- widening a CHECK cannot reject a row already stored.

BEGIN;

ALTER TABLE write_audit
    DROP CONSTRAINT IF EXISTS write_audit_target_system_check;

ALTER TABLE write_audit
    ADD CONSTRAINT write_audit_target_system_check
    CHECK (target_system IN (
        'hubspot',   -- a request to HubSpot
        'csuite',    -- a request to CSuite
        'jidhr'      -- local bookkeeping; NO request left the process
    ));

COMMIT;
