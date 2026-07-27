---
title: The rdq CLI
description: The rdq ops CLI — queue stats, DLQ browse and inspect, redrive, purge, and schema migrations over API or direct storage.
---

`rdq` is the ops CLI: a single Go binary for queue stats, DLQ
browse/inspect/redrive/purge, and schema migrations. It is the on-call
engineer's primary tool until the web UI ships.

## Two transports

Every command runs in one of two modes.

**API mode (`--server URL`)** — an ordinary client of the public rdq-server REST
API. All commands go through `/v1`; no server internals are imported. Add
`--token` for authenticated servers.

```bash
rdq --server http://rdq-server:8080 stats my-queue
rdq --server http://rdq-server:8080 --token $TOKEN dlq list my-queue
```

**Direct-storage mode (`--dsn DSN`)** — talks straight to Postgres via the
storage plugin. No rdq-server needed; ideal for embedded deployments and one-off
maintenance.

```bash
rdq --dsn "postgres://rdq:rdq@localhost:5432/rdq?sslmode=disable" stats my-queue
```

## `rdq stats <queue>`

Print a per-queue operational snapshot — the numbers you page on:

```bash
rdq --server http://rdq-server:8080 --token $TOKEN stats my-queue
```

```
Queue:               my-queue
Pending:             42
In-flight:           3
DLQ depth:           7
Oldest pending age:  4m12s
```

`DLQ depth` and `Oldest pending age` are the two flagship signals — see
[observability & metrics](/rdq/guides/observability/).

## `rdq dlq list <queue> [flags]`

Page the dead-letter queue.

| Flag | Description |
|---|---|
| `--limit N` | Tasks per page (default 20) |
| `--cursor C` | Pagination cursor from a prior listing |
| `--error-type E` | Filter by final-attempt error type |
| `--handler-ref H` | Filter by handler ref |
| `--from RFC3339` | Dead-lettered at or after this time (inclusive) |
| `--to RFC3339` | Dead-lettered before this time (exclusive) |

```bash
rdq --server URL --token $TOKEN dlq list payments.charge \
  --error-type java.net.SocketTimeoutException \
  --from 2026-07-27T14:00:00Z --limit 50
```

## `rdq dlq inspect <id>`

Print the full envelope — all fields plus the complete attempt history — for one
task as JSON:

```bash
rdq --server URL --token $TOKEN dlq inspect 01J2ZK7Q...
```

```json
{
  "id": "01J2ZK7Q...",
  "queue": "payments.charge",
  "handler_ref": "charge-payment",
  "status": "DEAD",
  "attempt_count": 3,
  "attempts": [
    { "attempt_no": 1, "outcome": "RETRYABLE_FAILURE",
      "error_type": "java.net.SocketTimeoutException", "error_message": "connect timed out" },
    { "attempt_no": 2, "outcome": "RETRYABLE_FAILURE",
      "error_type": "java.net.SocketTimeoutException", "error_message": "connect timed out" },
    { "attempt_no": 3, "outcome": "RETRYABLE_FAILURE",
      "error_type": "java.net.SocketTimeoutException", "error_message": "connect timed out" }
  ]
}
```

## `rdq dlq redrive <queue> [flags]`

Move matching DLQ tasks back to `PENDING` — `attempt_count` reset,
`redrive_count` incremented. Supply `--id` flags **or** filter flags, not both.

```bash
# by id
rdq --dsn DSN dlq redrive my-queue --id abc123 --id def456

# by filter
rdq --dsn DSN dlq redrive my-queue --error-type com.example.TransientError
```

```
redriven 128 task(s) from my-queue
```

## `rdq dlq purge <queue> [flags]`

Permanently remove matching DLQ tasks — same selector shape as redrive:

```bash
rdq --dsn DSN dlq purge my-queue --handler-ref legacy.handler
rdq --dsn DSN dlq purge my-queue --id abc123
```

Redrive and purge are audit-logged (principal, selector, count). Against a
server they require the `operator` role or higher. See
[DLQ analysis & redrive](/rdq/guides/dlq-and-redrive/).

## `rdq migrate`

Apply the Postgres schema migrations (direct-storage mode only). Idempotent —
safe to call on every startup:

```bash
rdq --dsn "postgres://rdq:rdq@db:5432/rdq?sslmode=disable" migrate
```

## Build

```bash
cd cli
go build -o rdq .
```

## See also

- [DLQ analysis & redrive](/rdq/guides/dlq-and-redrive/)
- [Observability & metrics](/rdq/guides/observability/)
- [Running rdq-server](/rdq/guides/rdq-server/)
- [Server API reference](/rdq/reference/server-api/)
