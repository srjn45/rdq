# rdq CLI

`rdq` is the ops CLI for rdq: queue stats, DLQ browse/inspect/redrive/purge, and schema migrations.

## Two transports (G2)

### API mode (`--server URL`)

An ordinary client of the public rdq-server REST API. All commands go through the
`/v1` endpoints — no server internals imported.

```sh
rdq --server http://rdq-server:8080 stats my-queue
rdq --server http://rdq-server:8080 --token $TOKEN dlq list my-queue
```

### Direct-storage mode (`--dsn DSN`)

Talks directly to Postgres via the storage plugin. No rdq-server needed — suitable for
embedded deployments or one-off maintenance runs.

```sh
rdq --dsn "postgres://rdq:rdq@localhost:5432/rdq?sslmode=disable" stats my-queue
rdq --dsn "postgres://..." migrate
```

## Commands

### `rdq stats <queue>`

Print a per-queue operational snapshot.

```
Queue:               my-queue
Pending:             42
In-flight:           3
DLQ depth:           7
Oldest pending age:  4m12s
```

### `rdq dlq list <queue> [flags]`

Page the dead-letter queue. Flags:

| Flag | Description |
|------|-------------|
| `--limit N` | Tasks per page (default 20) |
| `--cursor C` | Pagination cursor from a prior listing |
| `--error-type E` | Filter by final-attempt error type |
| `--handler-ref H` | Filter by handler ref |
| `--from RFC3339` | Dead-lettered at or after this time (inclusive) |
| `--to RFC3339` | Dead-lettered before this time (exclusive) |

### `rdq dlq inspect <id>`

Print the full envelope (all fields + attempt history) for one task as JSON.

### `rdq dlq redrive <queue> [flags]`

Move matching DLQ tasks back to PENDING (attempt_count reset, redrive_count incremented).
Supply `--id` flags OR filter flags, not both.

```sh
# by id
rdq --dsn DSN dlq redrive my-queue --id abc123 --id def456

# by filter
rdq --dsn DSN dlq redrive my-queue --error-type com.example.TransientError
```

### `rdq dlq purge <queue> [flags]`

Permanently remove matching DLQ tasks.

```sh
rdq --dsn DSN dlq purge my-queue --handler-ref legacy.handler
rdq --dsn DSN dlq purge my-queue --id abc123
```

### `rdq migrate`

Apply the T2.1 Postgres schema migrations (direct-storage mode only).
Idempotent — safe to call on every startup.

```sh
rdq --dsn "postgres://rdq:rdq@db:5432/rdq?sslmode=disable" migrate
```

## Build

```sh
cd cli
go build -o rdq .
```
