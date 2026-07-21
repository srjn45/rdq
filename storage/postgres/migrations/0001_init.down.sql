-- SPDX-License-Identifier: Apache-2.0
--
-- Reverse of 0001_init.up.sql. Drops the version-1 rdq schema in dependency
-- order so `Down` returns the database to a clean state (design 06 T2.1
-- acceptance: migrations apply cleanly up AND down).

DROP TABLE IF EXISTS rdq_schema_version;
DROP TABLE IF EXISTS rdq_attempt;
DROP TABLE IF EXISTS rdq_dlq_task;
DROP TABLE IF EXISTS rdq_task;
