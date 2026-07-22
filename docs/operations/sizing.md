# rdq sizing guidance

This document translates the benchmark numbers in `BENCHMARKS.md` into
actionable capacity decisions. All throughput figures are from
`storage/postgres/bench_test.go` running against a real Postgres 16
testcontainers database; see `BENCHMARKS.md` for the hardware context and how
to reproduce them in your environment.

---

## Postgres backend

### Throughput baselines

| Operation | Goroutines | Measured (GH Actions, 4 vCPU) |
|---|---|---|
| `Enqueue` (pure admit) | GOMAXPROCS (4) | **≈ 2 500 enqueues/sec** |
| `ClaimDue` (serial, 1 worker) | 1 | **≈ 600 claims/sec** |
| `ClaimDue` (1×GOMAXPROCS workers) | 4 | **≈ 1 800 claims/sec** |
| `ClaimDue` (2×GOMAXPROCS workers) | 8 | **≈ 2 800 claims/sec** |
| `ClaimDue` (4×GOMAXPROCS workers) | 16 | **≈ 3 400 claims/sec** |

These are sustained steady-state numbers with a resident pool of 256 due
tasks per queue. A single `ClaimDue` round-trip includes the `FOR UPDATE SKIP
LOCKED` scan, the lease flip, and (when resolving) the attempt record and
state transition — roughly 3–5 SQL statements per claim under a single
transaction.

> Run `go test -run=^$ -bench=. -benchtime=10s -count=3 ./storage/postgres/...`
> to measure your own hardware.

---

### How throughput scales with worker concurrency

The Postgres claim statement uses `FOR UPDATE SKIP LOCKED`:

```sql
SELECT id FROM rdq_task
WHERE queue = $1
  AND ( (status='PENDING' AND next_attempt_at <= now())
     OR (status='IN_FLIGHT' AND lease_expires_at <= now()) )
ORDER BY next_attempt_at
LIMIT $2
FOR UPDATE SKIP LOCKED
```

Workers that find all candidates locked skip them and return immediately —
they **never block waiting for a peer's lock**. This means aggregate claim
throughput scales roughly linearly with the number of concurrent workers
until the Postgres process itself becomes the bottleneck (CPU or connection
count), not the claim algorithm.

Observed scaling on a 4-vCPU runner:

```
1 worker  →   600 claims/sec
4 workers → 1 800 claims/sec  (3.0× — near-linear)
8 workers → 2 800 claims/sec  (4.7×)
16 workers → 3 400 claims/sec (5.7× — Postgres CPU saturating)
```

**Takeaway:** add workers to raise throughput. Plateau starts when Postgres
CPU hits 100% or connections are exhausted.

---

### Connection-pool sizing

Every `ClaimDue` call opens a short transaction (one DB connection for the
duration of the claim). A worker blocks one connection from the point it calls
`ClaimDue` until it calls the resolution mutation (`Complete`, `Reschedule`,
`DeadLetter`). For a handler with a 30 s processing time and a 60 s lease,
one worker holds one connection for ≈ 30 s.

**Rule of thumb:**

```
db_connections_needed ≈ N_workers + N_producers + 5   (headroom/ops)
```

Where:
- `N_workers` = goroutines actively calling ClaimDue or holding a live claim
- `N_producers` = goroutines actively calling Enqueue
- `+5` = headroom for migrations, health checks, and ops queries

Postgres default is `max_connections = 100`. A typical deployment:

| Scale | Workers | Producers | Recommended `max_connections` |
|---|---|---|---|
| Small (dev / staging) | 4 | 4 | 20 |
| Medium | 16 | 8 | 40 |
| Large | 64 | 16 | 100 |
| Very large | 128+ | 32+ | Use a connection pooler (PgBouncer) |

At `N_workers ≥ ~80`, configure PgBouncer (transaction-mode) in front of
Postgres. rdq's claim transaction is short (milliseconds), making it a good
fit for transaction-mode pooling. Each `ClaimDue` acquires the connection,
runs the `FOR UPDATE SKIP LOCKED` statement, and returns it — no long-lived
connection is held between ClaimDue and the resolution call (the worker does
processing in between).

> **Note on long handlers:** if your handler takes seconds to minutes (and
> uses `ExtendLease` for heartbeats), the worker holds a connection only for
> the duration of the resolution call at the end of processing, not throughout.
> The claim token is held in memory; the DB connection is returned after
> `ClaimDue` returns. This means `N_workers` does not equal `connections held`
> for long-running handlers — you can safely run more workers than connections.

---

### When contention dominates

Contention on the claim path matters when:

1. **Many workers, few due tasks** — workers race for a small set of due rows.
   `SKIP LOCKED` handles this gracefully (skippers return immediately), but
   aggregate throughput falls because most ClaimDue calls return empty.
   Solution: increase the resident pool (enqueue more tasks ahead of time) or
   reduce worker count to match queue depth.

2. **Very short handler times (< 5 ms)** — each worker completes so fast that
   the claim overhead dominates. At this scale, consider increasing `limit`
   passed to `ClaimDue` so each claim hands out a small batch.

3. **Single queue, many workers** — the `FOR UPDATE SKIP LOCKED` scan touches
   the same index rows every time. At 16+ workers on a single queue with a
   small pool, you may see diminishing returns (the `par-4x` benchmark
   demonstrates this). Distribute load across multiple queues to keep each
   queue's scan cheap.

---

### Worked example: sizing for 10 000 tasks/hour

**Target:** sustained 10 000 task completions per hour (≈ 2.8 tasks/sec).

**Step 1 — pick worker count.**
At ≈ 600 claims/sec per single goroutine, a single worker handles 2.8
tasks/sec with large headroom. Start with `N_workers = 2` for redundancy.

**Step 2 — check enqueue rate.**
10 000 tasks/hour = ≈ 2.8 enqueues/sec. With a measured enqueue throughput
of ≈ 2 500 enqueues/sec, a single producer goroutine has 900× headroom.
`N_producers = 1` is sufficient.

**Step 3 — set connection pool.**
`max_connections = N_workers + N_producers + 5 = 2 + 1 + 5 = 8`.
Use the Postgres default (100); no pooler needed.

**Step 4 — verify with benchmarks.**
Run `BenchmarkPostgresClaimContention/serial` to confirm your hardware matches
the baseline. If your single-worker throughput is lower, the test environment
(shared disk, noisy neighbour) may be the bottleneck, not rdq.

---

### Scaling to 1 000 000 tasks/hour

**Target:** ≈ 278 tasks/sec sustained.

**Step 1 — worker count.**
At 1 800 claims/sec with 4 workers (1×GOMAXPROCS on a 4-vCPU node), a single
4-vCPU node handles 278 tasks/sec with 6× headroom. Use 4 workers per node.

**Step 2 — enqueue rate.**
1 000 000/hour = 278 enqueues/sec. At 2 500 enqueues/sec capacity, a single
node with 4 producer goroutines handles the load with headroom.

**Step 3 — connection pool.**
`max_connections ≈ 4 (workers) + 4 (producers) + 5 = 13`. Postgres default
is fine; set `max_connections = 25` for a comfortable margin.

**Step 4 — Postgres sizing.**
A 4-vCPU / 8 GB Postgres instance handles this load. The hot index
`(queue, next_attempt_at) WHERE status IN ('PENDING','IN_FLIGHT')` stays small
when task turnover is fast; size Postgres RAM to hold the index in
`shared_buffers` (typically a few hundred MB for < 1M in-flight tasks).

---

## Running your own benchmarks

```bash
# Full sizing suite (requires Docker):
cd storage/postgres
go test -run=^$ -bench=. -benchtime=10s -count=3 .

# Individual benchmarks:
go test -run=^$ -bench=BenchmarkPostgresEnqueue         -benchtime=10s -count=3 .
go test -run=^$ -bench=BenchmarkPostgresClaims           -benchtime=30s -count=3 .
go test -run=^$ -bench=BenchmarkPostgresClaimContention  -benchtime=10s -count=3 .

# Skip Docker (CI-safe; benchmarks are skipped, not failed):
RDQ_SKIP_DOCKER=1 go test ./...
```

Benchmark output includes a custom `claims/sec` or `enqueues/sec` metric so
the throughput number is read directly off the `-bench` output without
computing `1/ns_per_op`. Compare your results to the table at the top of this
document to calibrate for your hardware.

---

## Notes on measurement environment

- Numbers in this document are from GitHub Actions `ubuntu-latest` (4 vCPU /
  16 GB RAM, Postgres 16-alpine in testcontainers over a loopback bridge).
- A loopback Docker bridge adds ≈ 0.2–0.5 ms of round-trip latency versus a
  local Unix socket. Production Postgres on the same host or a fast LAN will
  typically show higher throughput.
- The benchmarks use a 256-task resident pool per queue, refreshed after each
  claim. Pool depth affects benchmark variance but not long-run throughput: the
  pool is large enough that workers almost never stall waiting for a due task.
- Benchmark goroutine counts use `SetParallelism(N)`, which spawns
  `N × GOMAXPROCS` goroutines. On a 4-vCPU GH Actions runner, `par-1x` = 4
  goroutines, `par-2x` = 8, `par-4x` = 16.
