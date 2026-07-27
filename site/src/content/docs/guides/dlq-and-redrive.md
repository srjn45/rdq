---
title: DLQ analysis & redrive
description: Inspect the dead-letter queue with full failure history, then redrive or purge tasks safely — single or bulk, always audit-logged.
---

rdq treats the dead-letter queue as a **product, not a graveyard**. A task that
exhausts its attempts — or hits a non-retryable error — lands in the DLQ carrying
its complete failure history. From there an on-call engineer browses failures,
reads the exceptions, ships a fix, and redrives the affected tasks. This guide
covers what lands in the DLQ, how to browse and filter it, and how to redrive or
purge safely.

## What lands in the DLQ

A task is dead-lettered when it exhausts `max_attempts`, when a handler returns a
permanent/non-retryable failure, or when it is unroutable (no registered handler
for its `handler_ref`, parked with a distinct error class). Every DLQ entry
carries the **full task plus its complete attempt history** — for each attempt:
`attempt_no`, `started_at`, `finished_at`, `outcome`, `error_type`,
`error_message`, and `error_stack`. That history is what makes a DLQ entry
explain itself. See [the task lifecycle](/rdq/concepts/task-lifecycle/).

## Browse and filter

List the DLQ for a queue, filtering by error type, handler, and time range. Via
the CLI:

```bash
rdq --server http://rdq-server:8080 --token $TOKEN \
  dlq list payments.charge \
  --error-type java.net.SocketTimeoutException \
  --from 2026-07-27T14:00:00Z --limit 50
```

Via the server API (cursor-paginated):

```
GET /v1/queues/{queue}/dlq?error_type=&handler_ref=&from=&to=&limit=&cursor=
    → 200 { tasks: [envelope], next_cursor }
```

Inspect one task's full envelope and attempt history as JSON:

```bash
rdq --server http://rdq-server:8080 --token $TOKEN dlq inspect 01J2ZK7Q...
```

```
GET /v1/tasks/{id}   → 200 envelope (any status, including DEAD)
```

## Redrive

Redrive re-enqueues DLQ tasks back to the retry queue with a **reset policy** —
`attempt_count` is reset and `redrive_count` incremented, so a fixed handler gets
its full retry budget again. Redrive a single task, or bulk-redrive everything
matching a filter (the classic "all `payments.charge` tasks that failed with
`TimeoutException` after 14:00" case).

Single or explicit ids:

```bash
rdq --server URL --token $TOKEN dlq redrive payments.charge --id 01J2ZK7Q... --id 01J2ZM4B...
```

Bulk by filter:

```bash
rdq --server URL --token $TOKEN dlq redrive payments.charge \
  --error-type com.example.TransientError --from 2026-07-27T14:00:00Z
```

Server API — supply either `ids` or a `filter`, and the returned `count` is
authoritative:

```
POST /v1/queues/{queue}/dlq:redrive   → 200 { count }
  body: { ids: [...] } | { filter: { error_type?, handler_ref?, from?, to? } }
```

Redrive is **synchronous** in v1, bounded by selector size (an async job API for
million-entry DLQs is post-v1). Redriving a large batch into a fragile downstream
is a thundering-herd risk — the queue's per-instance `worker.rate_limit` is the
safety valve; see [queue configuration](/rdq/guides/queue-configuration/).

### Optional payload edit on single redrive

When a task failed because of a malformed payload, you can edit the payload as
part of a **single-task** redrive. The edit is recorded in the audit log so the
change is traceable. Bulk redrive never edits payloads.

## Purge

Purge permanently removes matching DLQ tasks — same selector shape as redrive
(ids or filter):

```bash
rdq --server URL --token $TOKEN dlq purge payments.charge --handler-ref legacy.handler
```

```
POST /v1/queues/{queue}/dlq:purge   → 200 { count }
  body: { ids: [...] } | { filter: { ... } }
```

## Every mutation is audited

Every DLQ mutation — redrive, purge, and single-redrive payload edit — writes an
audit record capturing **who** (the principal), **when**, and **what** (the
selector or ids, and the affected `count`). Pause/resume and API-sourced config
writes flow through the same audit sink. Redrive and purge require the `operator`
role (or `admin`); `submitter` cannot mutate the DLQ. See
[running rdq-server](/rdq/guides/rdq-server/) for the role model.

## See also

- [The rdq CLI](/rdq/guides/cli/)
- [Server API reference](/rdq/reference/server-api/)
- [Tasks, attempts & the lifecycle](/rdq/concepts/task-lifecycle/)
- [Observability & metrics](/rdq/guides/observability/)
