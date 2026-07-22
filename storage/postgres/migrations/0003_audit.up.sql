-- SPDX-License-Identifier: Apache-2.0
--
-- Audit log table (T6.3, design 06). Records every DLQ mutation (redrive,
-- purge, pause, resume) and API config change (config_write), carrying the
-- authenticated principal, the target queue and selector, the affected count,
-- and the operation outcome (FR-18, Config OI-2).
--
-- This table is owned by the rdq-server audit plane, separate from the task
-- storage SPI tables (rdq_task / rdq_dlq_task) and the config store
-- (rdq_queue_config). It lives in the same database for operational simplicity.

CREATE TABLE rdq_audit (
    id            bigserial    PRIMARY KEY,
    -- when the operation completed (authoritative time from the server)
    timestamp     timestamptz  NOT NULL,
    -- authenticated caller name ("anonymous" in dev/embedded mode)
    principal     text         NOT NULL,
    -- stable action name: redrive|purge|pause|resume|config_write
    action        text         NOT NULL,
    -- target queue; empty string for cross-queue operations
    queue         text         NOT NULL DEFAULT '',
    -- human-readable selector description (e.g. "ids:[x,y]", "filter:{...}", "all")
    selector      text         NOT NULL DEFAULT '',
    -- tasks affected; -1 for ops that do not produce a count (pause/config_write)
    count         integer      NOT NULL DEFAULT -1,
    -- "success" or "failure"
    outcome       text         NOT NULL,
    -- error string when outcome=failure; empty string on success
    error_message text         NOT NULL DEFAULT '',
    created_at    timestamptz  NOT NULL DEFAULT now()
);

-- Queue-scoped audit history (most-recent-first within a queue).
CREATE INDEX rdq_audit_queue_ts_idx ON rdq_audit (queue, timestamp DESC);
-- Principal audit trail (most-recent-first for a given caller).
CREATE INDEX rdq_audit_principal_ts_idx ON rdq_audit (principal, timestamp DESC);

-- Advance the schema-version gate to v3 (design 05 G5).
UPDATE rdq_schema_version SET version = 3, applied_at = now() WHERE singleton;
