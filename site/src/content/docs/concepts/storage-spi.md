---
title: Storage SPI & compliance kit
description: The storage service-provider interface every backend implements, its atomic-claim correctness rule, and the public compliance test-kit.
---

The **storage SPI** is the behavioral contract every storage plugin implements.
It embodies rdq's central split: the engine (an embedded SDK worker or an
`rdq-server` node) contains all *decisions* — policy, backoff, outcome
classification — while a plugin contains all *durability and atomicity*. A plugin
never interprets a task; it stores it, schedules it, and hands out claims (PRD
§8.4, FR-8..10).

## Design principles

- **Dumb storage, smart engine.** Plugins never see the retry policy (it lives in
  queue config, resolved at claim time — see [Wire envelope](/rdq/concepts/wire-envelope/)).
  Every SPI mutation is a single atomic storage operation; the engine never needs
  a cross-call transaction.
- **Poll-based floor, push-based option.** The required interface is polling
  (`claimDue`), implementable on any backend. Backends with change notification
  (Postgres `LISTEN/NOTIFY`, Redis keyspace events) may expose it as an optional
  capability to cut idle latency.
- **Fencing everywhere.** Every claim carries a token; every post-claim mutation
  requires it. A zombie worker — paused GC, network partition, expired lease —
  can never corrupt state.
- **Storage owns the clock.** The backend's clock is the time authority for both
  due-ness and lease expiry. The engine computes `next_attempt_at` but never
  decides *when now is* — it tolerates skew and defers to storage, keeping
  due-ness consistent across many engine instances with drifting clocks.

## The operations

The Go shape below is the contract (Java uses the same contract with idiomatic
naming):

```go
type Storage interface {
    // --- lifecycle ---
    Enqueue(ctx, task Envelope) error
    ClaimDue(ctx, queue string, limit int, lease time.Duration) ([]Claimed, error)
    ExtendLease(ctx, id TaskID, token ClaimToken, lease time.Duration) error

    // --- outcome resolution (all require a valid token; ErrStaleClaim otherwise) ---
    Reschedule(ctx, id TaskID, token ClaimToken, attempt Attempt, nextAt time.Time) error
    Complete(ctx, id TaskID, token ClaimToken, attempt Attempt) error
    DeadLetter(ctx, id TaskID, token ClaimToken, attempt Attempt) error

    // --- DLQ ---
    DLQList(ctx, queue string, f DLQFilter, page Page) ([]Envelope, Cursor, error)
    Get(ctx, id TaskID) (Envelope, error)
    Redrive(ctx, queue string, sel Selector) (int, error)
    Purge(ctx, queue string, sel Selector) (int, error)

    // --- ops ---
    Stats(ctx, queue string) (Stats, error)
    PurgeSucceeded(ctx, queue string, olderThan time.Time) (int, error)
    Capabilities() Capabilities
}
```

| Operation | Role |
|---|---|
| `Enqueue` | Admit a task. Idempotent by `id` within a queue (safe submit retries); the same id in a *different* queue returns `ErrIDConflict` (HTTP 409). |
| `ClaimDue` | Atomically claim up to `limit` due tasks, leasing each and minting a fencing token. |
| `ExtendLease` | Heartbeat a long-running handler's lease; `ErrStaleClaim` if the lease was lost. |
| `Reschedule` | Failure path: append the attempt, set `PENDING` with the engine-computed `next_attempt_at`. |
| `Complete` | Success path: append the attempt, mark `SUCCEEDED`. |
| `DeadLetter` | Exhaustion / permanent failure: append the attempt, move to the DLQ. |
| `DLQList` | Page the DLQ with a stable cursor; attempt bodies omitted unless `IncludeAttempts`. |
| `Get` | Fetch one task in *any* status with full history; `ErrNotFound` if absent. Backs `GET /v1/tasks/{id}`. |
| `Redrive` | Return selected DLQ tasks to `PENDING`, `attempt_count=0`, `redrive_count+1`, history preserved. |
| `Purge` | Permanently remove selected DLQ tasks. |
| `Stats` | Per-queue snapshot: `pending`, `in_flight`, `dlq_depth`, `oldest_pending_age`. Powers Prometheus metrics. |
| `PurgeSucceeded` | Delete `SUCCEEDED` tasks older than a cutoff (`task_ttl` enforcement). |
| `Capabilities` | Advertise optional accelerations. |

`Redrive`/`Purge` take a `Selector` — an explicit id set **or** a `DLQFilter`
(`error_type`, `handler_ref`, dead-lettered time range), never both. Sentinel
errors are `ErrStaleClaim`, `ErrNotFound`, `ErrStaleCursor`, and `ErrIDConflict`.

### Optional capabilities

The engine always works against the polling floor; capabilities only remove
latency or transfer cost:

```go
type Capabilities struct {
    Notify         bool // WaitDue(ctx, queue) — block until a task may be due
    FilterPushdown bool // DLQFilter evaluated natively; else core filters client-side
    BatchEnqueue   bool
}
```

## The atomic-claim requirement

`ClaimDue` claims a task that is **due** — either `PENDING` with
`next_attempt_at <= now`, or `IN_FLIGHT` with `lease_expires_at <= now` (built-in
crash recovery). Claiming makes it `IN_FLIGHT` with `lease_expires_at = now +
lease`, and reclaiming an expired lease atomically appends a `LEASE_EXPIRED`
attempt record.

The single correctness rule every backend must guarantee:

> **No two workers may ever claim the same task.**

Each backend owns its own atomic-claim mechanics:

- **PostgreSQL** — `FOR UPDATE SKIP LOCKED` (the reference plugin):

  ```sql
  UPDATE rdq_task SET status='IN_FLIGHT',
                      lease_expires_at = now() + $lease,
                      claim_token = gen_random_uuid()
  WHERE id IN (
    SELECT id FROM rdq_task
    WHERE queue = $1
      AND ( (status='PENDING'   AND next_attempt_at  <= now())
         OR (status='IN_FLIGHT' AND lease_expires_at <= now()) )
    ORDER BY next_attempt_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED )
  RETURNING *;
  ```

  Atomicity and mutual exclusion come from the row locks; matching `claim_token`
  in the `WHERE` clause of every subsequent mutation is the fencing implementation.

- **Redis** — a per-queue sorted set scored by `next_attempt_at`, claimed by a Lua
  script (`ZRANGEBYSCORE` + move + token set), atomic by Redis's single-threaded
  execution.

- **MongoDB** — a `findAndModify` loop, one document per call, batched by the
  engine.

Claims are **fenced** end-to-end: `Reschedule`, `Complete`, `DeadLetter`, and
`ExtendLease` bearing a stale token must fail with `ErrStaleClaim` and change
nothing. That is what lets a crashed worker's task be safely reclaimed while the
zombie worker can no longer corrupt it.

## The compliance test-kit

The SPI ships as a **public contract with a compliance test-kit** so third
parties can build and verify their own plugins. The kit is a single exported
entry point that a plugin runs against its implementation from an ordinary test:

```go
func TestCompliance(t *testing.T) {
    compliance.Run(t, func() spi.Storage { return mystore.New(...) })
}
```

It uses testcontainers to exercise real backends and verifies eight correctness
invariants, each as a named subtest so a failure points at the exact contract
clause:

1. **No double claim** — under N concurrent claimants, at most one valid token
   per task at any moment.
2. **Fencing** — a stale token on any mutation fails with `ErrStaleClaim` and
   changes nothing.
3. **Lease recovery counts** — reclaiming an expired lease appends
   `LEASE_EXPIRED` atomically with the reclaim.
4. **Atomic transitions** — every mutation is all-or-nothing; a crash between
   calls leaves a valid state (at-least-once, never lost).
5. **Idempotent enqueue** by task id.
6. **Lossless round-trip** of the envelope, including unknown-field preservation.
7. **Redrive resets, history persists** — `PENDING`, `attempt_count=0`,
   `redrive_count` incremented, prior attempts readable.
8. **Stable pagination** for `DLQList` — cursor-based, no skips or dupes across
   pages while entries are added.

The kit also ships a contention benchmark; the reference Postgres plugin targets
**≥1k claims/sec on a single modest node** with zero double-claims under a
kill-9 chaos test. The in-memory reference store, the Postgres binding, and the
Java Postgres binding all run this same kit — which is what freezes the storage
contract as a cross-backend guarantee rather than a per-plugin hope.

PostgreSQL is the production-hardened reference plugin in v1; Redis and MongoDB
are fast-follow. The Postgres schema is itself a cross-language contract: the Java
worker's Postgres binding implements the *same* tables and claim semantics as the
Go plugin, which is what makes the cross-language redrive loop work.

## What is NOT in the SPI

Backoff computation, outcome classification, queue-config resolution, handler and
callback invocation and timeouts, metrics emission, and audit records for
redrive/purge are all **engine responsibilities** — deliberately outside the SPI.
Plugins store and schedule; the engine decides.

## See also

- [The wire envelope](/rdq/concepts/wire-envelope/)
- [Architecture — one core, two hosts](/rdq/concepts/architecture/)
- [Tasks, attempts & the lifecycle](/rdq/concepts/task-lifecycle/)
- [Storage backends & sizing](/rdq/reference/storage-backends/)
- [DLQ analysis & redrive](/rdq/guides/dlq-and-redrive/)
