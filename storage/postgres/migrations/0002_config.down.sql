-- SPDX-License-Identifier: Apache-2.0
--
-- Rollback for 0002_config.up.sql: drops the ConfigStore tables and steps the
-- schema-version gate back to 1 (matching 0001_init).

DROP TABLE IF EXISTS rdq_queue_config;

UPDATE rdq_schema_version SET version = 1, applied_at = now() WHERE singleton;
