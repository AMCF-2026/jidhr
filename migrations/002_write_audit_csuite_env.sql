-- 002 — record which CSuite environment a write ran under.
--
-- WHY
-- ---
-- On 2026-10-01 a HubSpot PATCH wrote sandbox CSuite profile 21663 onto a live
-- HubSpot contact. There is one HubSpot portal and two CSuite environments, and
-- nothing recorded which one a write had been made from — so answering "did any
-- HubSpot write ever run outside production?" meant reconstructing it from
-- payload_meta id values and from memory of who ran what. It happened to be
-- answerable. It should not have had to be.
--
-- SAFETY
-- ------
-- Additive and nullable: existing rows keep NULL, which is honest — the
-- environment of a write made before this column existed is genuinely unknown
-- and must not be back-filled with a guess. Reversible with a single DROP.
--
-- ORDER OF OPERATIONS
-- -------------------
-- Run this BEFORE deploying the code that writes the column. The code does not
-- require it — clients/audit.py checks once per process whether the column is
-- there and omits it if not, because reserve_write REFUSES the write when it
-- cannot record a row, so a schema/code ordering mismatch would otherwise stop
-- every write to CSuite and HubSpot. Running it first simply means the first
-- write after the deploy is already labelled.
--
-- Not executed by Claude. Run it in TablePlus.

ALTER TABLE write_audit
    ADD COLUMN IF NOT EXISTS csuite_env text;

COMMENT ON COLUMN write_audit.csuite_env IS
    'CSUITE_ENV at the moment of the write: ''live'' or ''sandbox''. NULL for '
    'rows written before 2026-10-01, whose environment is unknown and must not '
    'be inferred.';

-- Reading back: which writes ran outside production?
--   SELECT csuite_env, target_system, count(*)
--   FROM write_audit GROUP BY 1, 2 ORDER BY 1, 2;

-- To reverse:
--   ALTER TABLE write_audit DROP COLUMN csuite_env;
