---
title: Storage backends & sizing
description: PostgreSQL is the v1 reference storage plugin; Redis and MongoDB are fast-follow via the SPI and compliance kit. Includes Postgres throughput and sizing guidance.
---

rdq introduces no new stateful infrastructure — retry queues and DLQs live in a
datastore you already run, reached through the [Storage SPI](/rdq/concepts/storage-spi/).
Each plugin owns its own atomic-claim mechanics behind one correctness bar:

> **No two workers may ever claim the same task.**

Availability is inherited: rdq adds no stateful component of its own, so its HA
story is exactly the backend's — Postgres replicas, Redis Sentinel, Mongo replica
sets. Stateless rdq workers coordinate only through the backend's atomic claims and
leases, so scaling is "add another instance" and node death is a non-event.

## Backend matrix

| Backend | Status | Atomic-claim mechanism |
|---|---|---|
| **PostgreSQL** | v1 — shipped, production-hardened reference plugin | `SELECT … FOR UPDATE SKIP LOCKED` |
| **Redis** | Fast-follow (post-v1) | Atomic sorted-set pops (Lua) |
| **MongoDB** | Fast-follow (post-v1) | `findAndModify` |

The SPI ships as a public contract with a **compliance test-kit** so third parties
can build and verify their own plugins — including a chaos test that `kill -9`s a
worker mid-processing and asserts the task is reclaimed after lease expiry with zero
double-claims. Redis and MongoDB are the first validation targets for that
pluggability.

## PostgreSQL (v1 reference)

The Postgres plugin claims due tasks with a single `FOR UPDATE SKIP LOCKED` query.
Workers that find all candidate rows locked skip them and return immediately — they
**never block waiting for a peer's lock**, so aggregate claim throughput scales
roughly linearly with worker count until Postgres itself (CPU or connections)
becomes the bottleneck, not the claim algorithm.

```sql
SELECT id FROM rdq_task
WHERE queue = $1
  AND ( (status='PENDING'   AND next_attempt_at  <= now())
     OR (status='IN_FLIGHT' AND lease_expires_at <= now()) )
ORDER BY next_attempt_at
LIMIT $2
FOR UPDATE SKIP LOCKED
```

### Throughput baselines

Measured on GitHub Actions `ubuntu-latest` (4 vCPU / 16 GB, Postgres 16-alpine in
testcontainers over a loopback bridge). Reproduce with
`go test -run=^$ -bench=. -benchtime=10s -count=3 ./storage/postgres/...`.

| Operation | Goroutines | Measured (4 vCPU) |
|---|---|---|
| `Enqueue` (pure admit) | 4 (GOMAXPROCS) | ≈ 2 500 enqueues/sec |
| `ClaimDue` (serial, 1 worker) | 1 | ≈ 600 claims/sec |
| `ClaimDue` (1×GOMAXPROCS) | 4 | ≈ 1 800 claims/sec |
| `ClaimDue` (2×GOMAXPROCS) | 8 | ≈ 2 800 claims/sec |
| `ClaimDue` (4×GOMAXPROCS) | 16 | ≈ 3 400 claims/sec |

These are sustained steady-state numbers with a resident pool of 256 due tasks per
queue. One claim round-trip is roughly 3–5 SQL statements in a single transaction:
the `FOR UPDATE SKIP LOCKED` scan, the lease flip, and (on resolution) the attempt
record and state transition.

**Scaling shape:** 1 worker → 600/s; 4 → 1 800/s (3.0×, near-linear); 8 → 2 800/s
(4.7×); 16 → 3 400/s (5.7×, Postgres CPU saturating). Add workers to raise
throughput; the plateau starts when Postgres CPU hits 100% or connections are
exhausted. A loopback Docker bridge adds ≈ 0.2–0.5 ms round-trip; production
Postgres on the same host or a fast LAN typically shows higher numbers.

### Connection-pool sizing

Every `ClaimDue` opens a short transaction (one connection). Rule of thumb:

```
db_connections_needed ≈ N_workers + N_producers + 5   (headroom/ops)
```

| Scale | Workers | Producers | Recommended `max_connections` |
|---|---|---|---|
| Small (dev / staging) | 4 | 4 | 20 |
| Medium | 16 | 8 | 40 |
| Large | 64 | 16 | 100 |
| Very large | 128+ | 32+ | Use a pooler (PgBouncer) |

At `N_workers ≥ ~80`, put PgBouncer in **transaction mode** in front of Postgres —
rdq's claim transaction is milliseconds long, an ideal fit. For **long-running
handlers** (with `ExtendLease` heartbeats), the DB connection is returned after
`ClaimDue` returns and only re-acquired for the resolution call; `N_workers` does
**not** equal connections held, so you can safely run more workers than
connections.

### When contention dominates

1. **Many workers, few due tasks** — `SKIP LOCKED` handles the race gracefully but
   most `ClaimDue` calls return empty; raise the resident pool (enqueue ahead) or
   cut worker count to match queue depth.
2. **Very short handlers (< 5 ms)** — claim overhead dominates; increase the
   `ClaimDue` `limit` (batch size) so each claim hands out a small batch.
3. **Single queue, many workers** — the scan touches the same index rows every
   time; at 16+ workers on one queue with a small pool you see diminishing returns.
   Distribute load across multiple queues.

### Worked sizing examples

**10 000 tasks/hour (≈ 2.8/sec).** A single goroutine (≈ 600 claims/sec) has huge
headroom — start with 2 workers for redundancy and 1 producer (2 500 enqueues/sec
capacity). `max_connections = 2 + 1 + 5 = 8`; the Postgres default of 100 is fine,
no pooler.

**1 000 000 tasks/hour (≈ 278/sec).** 4 workers on one 4-vCPU node (1 800
claims/sec) carries this with 6× headroom; 4 producers cover 278 enqueues/sec.
`max_connections ≈ 4 + 4 + 5 = 13` (set 25 for margin). A 4-vCPU / 8 GB Postgres
handles the load; size `shared_buffers` to hold the hot
`(queue, next_attempt_at) WHERE status IN ('PENDING','IN_FLIGHT')` index — typically
a few hundred MB for < 1M in-flight tasks, which stays small when turnover is fast.

See the full [sizing guide](https://github.com/srjn45/rdq/blob/main/docs/operations/sizing.md)
and `BENCHMARKS.md` for hardware context and how to calibrate for your own
environment. Benchmark output includes a `claims/sec` / `enqueues/sec` metric read
straight off the `-bench` output.

## Fast-follow plugins (post-v1)

Redis and MongoDB plugins are on the [roadmap](/rdq/reference/roadmap/). Each will
implement the same SPI operations and pass the compliance kit — the same
double-claim and lease-recovery guarantees the Postgres plugin meets — so
application and server code is unchanged when swapping backends. Their atomic-claim
mechanics (Lua sorted-set pops for Redis, `findAndModify` for MongoDB) differ, but
the correctness bar and lease semantics are identical.

## See also

- [Storage SPI](/rdq/concepts/storage-spi/)
- [Configuration](/rdq/reference/configuration/)
- [Roadmap](/rdq/reference/roadmap/)
- [Architecture](/rdq/concepts/architecture/)
