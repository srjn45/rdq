-- SPDX-License-Identifier: Apache-2.0
--
-- rdq PostgreSQL reference schema, version 1 (design 02 §4).
--
-- This schema is a CROSS-LANGUAGE CONTRACT (design 05 G5): the Java Postgres
-- binding (T7.4) implements these same tables and claim semantics, never its
-- own schema. The shared schema is what makes the cross-language redrive loop
-- (T8.2) work. Column layout mirrors the wire envelope (design 01 §2) so the
-- envelope decomposes losslessly into rows (T2.2); unknown top-level and
-- per-attempt JSON fields are preserved in the `residual` JSONB columns.
--
-- Two hot tables keep the scheduler's index small: live tasks in `rdq_task`
-- (PENDING/IN_FLIGHT/SUCCEEDED) and dead-lettered tasks in `rdq_dlq_task` (a
-- DLQ can grow large without polluting the claim index). `rdq_attempt` holds the
-- ordered failure history for tasks in either table.

-- Live tasks. A task is claimed with FOR UPDATE SKIP LOCKED (design 02 §4);
-- claim_token fences every post-claim mutation.
CREATE TABLE rdq_task (
    id                   text        PRIMARY KEY,           -- ULID (design 01 §1)
    queue                text        NOT NULL,
    envelope_version     integer     NOT NULL,
    handler_ref          text        NOT NULL,
    handler_version      text,
    payload              bytea       NOT NULL,
    payload_content_type text        NOT NULL,
    payload_ref          text,                              -- reserved, unused in v1 (design 01 §2)
    headers              jsonb       NOT NULL DEFAULT '{}'::jsonb,
    status               text        NOT NULL
                             CHECK (status IN ('PENDING', 'IN_FLIGHT', 'SUCCEEDED')),
    attempt_count        integer     NOT NULL DEFAULT 0,
    redrive_count        integer     NOT NULL DEFAULT 0,
    next_attempt_at      timestamptz,                       -- null once terminal
    lease_expires_at     timestamptz,                       -- set while IN_FLIGHT
    claim_token          uuid,                              -- fencing token (design 02 §4)
    created_at           timestamptz NOT NULL,
    residual             jsonb       NOT NULL DEFAULT '{}'::jsonb  -- unknown envelope fields (design 01 §5)
);

-- The claim index: partial composite over due candidates only (design 02 §4).
-- SUCCEEDED rows are excluded so the scheduler never scans retained successes.
CREATE INDEX rdq_task_due_idx
    ON rdq_task (queue, next_attempt_at)
    WHERE status IN ('PENDING', 'IN_FLIGHT');

-- Dead-lettered tasks. Same envelope columns as rdq_task; error_type and
-- dead_lettered_at are denormalized for DLQFilter pushdown and stable
-- cursor pagination (design 02 §2, §3 invariant 8).
CREATE TABLE rdq_dlq_task (
    id                   text        PRIMARY KEY,
    queue                text        NOT NULL,
    envelope_version     integer     NOT NULL,
    handler_ref          text        NOT NULL,
    handler_version      text,
    payload              bytea       NOT NULL,
    payload_content_type text        NOT NULL,
    payload_ref          text,
    headers              jsonb       NOT NULL DEFAULT '{}'::jsonb,
    status               text        NOT NULL CHECK (status = 'DEAD'),
    attempt_count        integer     NOT NULL DEFAULT 0,
    redrive_count        integer     NOT NULL DEFAULT 0,
    next_attempt_at      timestamptz,
    lease_expires_at     timestamptz,
    claim_token          uuid,
    created_at           timestamptz NOT NULL,
    residual             jsonb       NOT NULL DEFAULT '{}'::jsonb,
    dead_lettered_at     timestamptz NOT NULL,              -- death time; DLQ ordering + time-range filter
    error_type           text                               -- terminal error type; DLQFilter pushdown
);

-- DLQ pagination + filter-pushdown indexes (design 02 §2 DLQFilter).
CREATE INDEX rdq_dlq_task_page_idx ON rdq_dlq_task (queue, dead_lettered_at, id);
CREATE INDEX rdq_dlq_task_error_type_idx ON rdq_dlq_task (queue, error_type);
CREATE INDEX rdq_dlq_task_handler_ref_idx ON rdq_dlq_task (queue, handler_ref);

-- Attempt history, referenced by BOTH task tables (design 02 §4). task_id is
-- not a foreign key because a task lives in exactly one of rdq_task /
-- rdq_dlq_task at a time and moves between them; the (task_id, attempt_no)
-- uniqueness plus the lookup index carry the relationship.
CREATE TABLE rdq_attempt (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id       text        NOT NULL,
    attempt_no    integer     NOT NULL,
    started_at    timestamptz NOT NULL,
    finished_at   timestamptz,                              -- null while in flight
    outcome       text        NOT NULL
                      CHECK (outcome IN ('SUCCESS', 'RETRYABLE_FAILURE',
                                         'PERMANENT_FAILURE', 'LEASE_EXPIRED')),
    error_type    text,
    error_message text,
    error_detail  jsonb,
    error_stack   text,
    residual      jsonb       NOT NULL DEFAULT '{}'::jsonb, -- unknown per-attempt fields (design 01 §5)
    UNIQUE (task_id, attempt_no)
);

CREATE INDEX rdq_attempt_task_idx ON rdq_attempt (task_id, attempt_no);

-- Schema-version gate (design 05 G5). A single row records the schema-contract
-- version. An engine reads it at startup and refuses to run against an unknown
-- (newer) version rather than corrupting rows it does not understand. The
-- singleton primary key keeps exactly one row.
CREATE TABLE rdq_schema_version (
    singleton  boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
    version    integer     NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO rdq_schema_version (singleton, version) VALUES (true, 1);
