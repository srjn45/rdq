-- SPDX-License-Identifier: Apache-2.0
--
-- Rollback for 0003_audit.up.sql: remove the audit log table and revert
-- the schema-version gate to v2.

DROP TABLE IF EXISTS rdq_audit;

UPDATE rdq_schema_version SET version = 2, applied_at = now() WHERE singleton;
