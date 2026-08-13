-- SPDX-License-Identifier: Apache-2.0
--
-- Task-contract version split (design 05 G5, issue #54).
--
-- The single rdq_schema_version.version counter is bumped by EVERY migration,
-- including server-only ones (0002 config, 0003 audit) that never touch the
-- task tables a worker binds to (rdq_task / rdq_dlq_task / rdq_attempt).
-- An exact-match worker gate was therefore locked out by server-feature
-- migrations until every language binding bumped in lockstep.
--
-- This adds a SECOND, independent counter, task_contract_version, bumped ONLY
-- when a migration changes the task-table contract. Workers gate on it; the
-- server (and `rdq migrate`) still track the overall `version`. Server-only
-- migrations bump `version` and leave task_contract_version untouched, so
-- workers keep running across them while still refusing a genuinely-changed
-- task schema.
--
-- The task-table contract is unchanged since 0001, so its version is 1. The
-- column is added nullable, backfilled, then made NOT NULL so it mirrors
-- `version` (NOT NULL, no lingering default) rather than carrying a default a
-- future task-contract bump would have to remember to override.

ALTER TABLE rdq_schema_version ADD COLUMN task_contract_version integer;
UPDATE rdq_schema_version
    SET task_contract_version = 1, version = 4, applied_at = now()
    WHERE singleton;
ALTER TABLE rdq_schema_version ALTER COLUMN task_contract_version SET NOT NULL;
