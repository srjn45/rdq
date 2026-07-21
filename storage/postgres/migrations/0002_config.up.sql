-- SPDX-License-Identifier: Apache-2.0
--
-- ConfigStore tables (T5.4, design 04 §3). These tables are owned by the
-- rdq-server admin plane, NOT by the task Storage SPI — they live in the same
-- database but are completely separate from rdq_task / rdq_dlq_task.
--
-- api_written: set when a row was last written via the admin API; prevents
-- a YAML boot-seed from overwriting it on the next restart (G16, design 03 §1).
-- paused: pause state persists here so it survives restarts (design 04 §2).
-- config_json: the QueueConfig as JSON (design 03 schema), stored as JSONB for
-- future query support; the application layer owns all schema knowledge.

CREATE TABLE rdq_queue_config (
    queue       text        PRIMARY KEY,              -- queue name (envelope §2 charset)
    config_json jsonb       NOT NULL DEFAULT '{}'::jsonb,
    paused      boolean     NOT NULL DEFAULT false,
    api_written boolean     NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Advance the schema-version gate so engines running against this database know
-- the config tables are present (design 05 G5). The Java binding (T7.4) applies
-- the same migrations; rdq_queue_config rows are server-only but the version
-- bump is shared so the gate stays consistent across all bindings.
UPDATE rdq_schema_version SET version = 2, applied_at = now() WHERE singleton;
