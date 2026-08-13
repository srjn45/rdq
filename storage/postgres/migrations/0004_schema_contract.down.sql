-- SPDX-License-Identifier: Apache-2.0
--
-- Reverse the task-contract version split (0004 up): drop the second counter
-- and restore the overall version to 3.

ALTER TABLE rdq_schema_version DROP COLUMN task_contract_version;
UPDATE rdq_schema_version SET version = 3, applied_at = now() WHERE singleton;
