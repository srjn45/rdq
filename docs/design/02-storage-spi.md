# rdq design 02 — Storage SPI

Status: Draft v1 · Companion: [01 — Wire envelope](01-wire-envelope.md) · PRD: [§8.4 FR-8..10](../PRD.md)

The SPI is the behavioral contract every storage plugin implements. The engine (embedded
SDK worker or `rdq-server`) contains all *decisions* — policy, backoff, outcome
classification. Plugins contain all *durability and atomicity*. A plugin never interprets
a task; it stores, schedules, and hands out claims.

## 1. Design principles

- **Dumb storage, smart engine.** Plugins never see retry policy (envelope §3). Every SPI
  mutation is a single atomic storage operation — no cross-call transactions required of the
  engine.
- **Poll-based floor, push-based option.** The required interface is polling (`claimDue`),
  implementable on any backend. Backends with change notification (Postgres LISTEN/NOTIFY,
  Redis keyspace events) can expose it via an optional capability to cut idle latency.
- **Fencing everywhere.** Every claim carries a token; every post-claim mutation requires it.
  A zombie worker (paused GC, network partition, expired lease) can never corrupt state.
- **Storage owns the clock.** The storage backend's clock is the time authority for both
  due-ness and lease expiry (Postgres `now()`, Redis server time). The engine computes
  `next_attempt_at` but never decides *when now is* — it tolerates skew and defers to storage
  (G9, resolved v1 — see design 05 §0.1). This keeps due-ness consistent across many engine
  instances with drifting clocks.

**Mixed claiming is discouraged.** The claim semantics allow embedded workers *and*
`rdq-server` callback dispatch to claim the same queue concurrently, but running both against
one queue means two execution paths and confusing ops. v1 does not enforce this — it is
documented as discouraged (G14, resolved v1 — see design 05 §0.1).

## 2. Interface

Go shape (Java is the same contract with idiomatic naming):

```go
type Storage interface {
    // --- lifecycle ---
    // Idempotent by task.ID within a queue: re-enqueue of an existing ID in the SAME queue is
    // a no-op (safe submit retries). The same ID already present in a DIFFERENT queue is NOT a
    // no-op — it returns ErrIDConflict (a silent cross-queue no-op would return a confusing
    // foreign envelope). Maps to 409 at the API (G8, resolved v1 — see design 05 §0.1).
    Enqueue(ctx, task Envelope) error

    // Atomically claim up to `limit` due tasks for `queue`:
    //   due = (status=PENDING AND next_attempt_at <= now)
    //      OR (status=IN_FLIGHT AND lease_expires_at <= now)   // built-in crash recovery
    // Claimed tasks become IN_FLIGHT with lease_expires_at = now + lease.
    // Reclaim of an expired lease atomically appends a LEASE_EXPIRED attempt record.
    // Best-effort ordering by next_attempt_at ascending. NEVER returns a task another
    // live claim holds. Returns a fencing ClaimToken per task.
    ClaimDue(ctx, queue string, limit int, lease time.Duration) ([]Claimed, error)

    // Heartbeat for long-running handlers. Fails with ErrStaleClaim if the lease was
    // lost (task reclaimed elsewhere) — the handler must then abandon its work.
    ExtendLease(ctx, id TaskID, token ClaimToken, lease time.Duration) error

    // --- outcome resolution (all require a valid token; ErrStaleClaim otherwise) ---
    // Failure path: append attempt, set PENDING + next_attempt_at (engine computed backoff).
    Reschedule(ctx, id TaskID, token ClaimToken, attempt Attempt, nextAt time.Time) error
    // Success path: append attempt, mark SUCCEEDED (retained until task_ttl purge).
    Complete(ctx, id TaskID, token ClaimToken, attempt Attempt) error
    // Exhaustion / permanent failure: append attempt, move task to the DLQ.
    DeadLetter(ctx, id TaskID, token ClaimToken, attempt Attempt) error

    // --- DLQ ---
    // Page the DLQ. Envelopes are returned WITHOUT attempt bodies by default (histories make
    // pages heavy); set DLQFilter.IncludeAttempts to include full histories (G13). Get returns
    // everything regardless.
    DLQList(ctx, queue string, f DLQFilter, page Page) ([]Envelope, Cursor, error)
    // Fetch one task by id in ANY status (PENDING/IN_FLIGHT/SUCCEEDED/DEAD) with full attempt
    // history; ErrNotFound if absent. Backs GET /v1/tasks/{id}. Replaces the DEAD-only DLQGet
    // (G4, resolved v1 — see design 05 §0.1).
    Get(ctx, id TaskID) (Envelope, error)
    // Back to PENDING, attempt_count=0, redrive_count+1, history preserved (envelope §2).
    Redrive(ctx, queue string, sel Selector) (int, error)   // Selector: ids or DLQFilter
    Purge(ctx, queue string, sel Selector) (int, error)

    // --- ops ---
    Stats(ctx, queue string) (Stats, error)  // pending, in_flight, dlq_depth, oldest_pending_age
    PurgeSucceeded(ctx, queue string, olderThan time.Time) (int, error)  // task_ttl enforcement
    Capabilities() Capabilities
}
```

`DLQFilter`: `error_type`, `handler_ref`, dead-lettered time range, plus `include_attempts`
(default false — G13, resolved v1). `Stats` powers the Prometheus metrics (PRD FR-22): SDKs
expose a Prometheus `Collector`/registry hook the embedding app mounts, and `rdq-server`
serves `/metrics` itself (G18, resolved v1 — see design 05 §0.1). Sentinel errors:
`ErrStaleClaim`, `ErrNotFound`, `ErrStaleCursor`, `ErrIDConflict`.

### Optional capabilities

```go
type Capabilities struct {
    Notify        bool // WaitDue(ctx, queue) — block until a task may be due; claim still via ClaimDue
    FilterPushdown bool // DLQFilter evaluated natively; else core paginates + filters client-side
    BatchEnqueue  bool
}
```

The engine always works against the floor; capabilities only remove latency or transfer cost.

## 3. Correctness invariants (the compliance kit tests exactly these)

1. **No double claim.** Under N concurrent claimants, every claim of a task is fenced: at
   most one *valid* token per task at any moment. (Chaos test: kill -9 a claimant, task is
   reclaimable after lease expiry, old token is dead.)
2. **Fencing.** `Reschedule`/`Complete`/`DeadLetter`/`ExtendLease` with a stale token must
   fail with `ErrStaleClaim` and change nothing.
3. **Lease recovery counts.** Reclaiming an expired lease appends `LEASE_EXPIRED` to the
   attempt history atomically with the reclaim (poison-pill protection, envelope §2).
4. **Atomic transitions.** Each mutation is all-or-nothing; a crash between any two SPI
   calls leaves the task in a valid state (worst case: retried after lease expiry —
   at-least-once, never lost).
5. **Idempotent enqueue** by task ID.
6. **Lossless round-trip** of the envelope, including unknown-field preservation.
7. **Redrive resets, history persists.** After redrive: `PENDING`, `attempt_count=0`,
   `redrive_count` incremented, all prior attempts readable.
8. **Stable pagination** for `DLQList` (cursor-based; no skips/dupes across pages while
   entries are added).

The kit ships as a test suite any plugin runs against its implementation
(testcontainers for real backends), plus a contention benchmark (target for the reference
plugin: ≥1k claims/sec single node, PRD §11).

## 4. Reference mapping — PostgreSQL (v1)

Two tables keep the hot path small: `rdq_task` (PENDING/IN_FLIGHT/SUCCEEDED) and
`rdq_dlq_task` (DEAD moves here — DLQs can grow large and slow without polluting the
scheduler's index). `rdq_attempt` (task_id, attempt_no, outcome, error fields) referenced
by both. `claim_token` is a UUID column on `rdq_task`; matching the token in the `WHERE`
clause of every mutation is the fencing implementation.

Claim (single statement — atomicity and mutual exclusion come from the row locks):

```sql
UPDATE rdq_task SET status='IN_FLIGHT',
                    lease_expires_at = now() + $lease,
                    claim_token = gen_random_uuid()
WHERE id IN (
  SELECT id FROM rdq_task
  WHERE queue = $1
    AND ( (status='PENDING'   AND next_attempt_at   <= now())
       OR (status='IN_FLIGHT' AND lease_expires_at <= now()) )
  ORDER BY next_attempt_at
  LIMIT $2
  FOR UPDATE SKIP LOCKED )
RETURNING *;
```

Index: partial composite `(queue, next_attempt_at) WHERE status IN ('PENDING','IN_FLIGHT')`.
`Notify` capability via LISTEN/NOTIFY on enqueue/reschedule. Schema managed by versioned
migrations shipped with the plugin.

**The Postgres schema is itself a cross-language contract** (G5, resolved v1 — see design 05
§0.1). The Java worker's Postgres binding implements the *same* tables and claim semantics as
the Go plugin — that shared schema is what makes the cross-language redrive loop work; the Java
binding never defines its own schema. A `rdq_schema_version` row records the schema version and
gates startup: an engine refuses to run against an unknown (newer) schema version rather than
corrupting rows it does not understand.

## 5. Sketch mappings — Redis, MongoDB (fast-follow, sanity check that the SPI fits)

- **Redis:** per-queue ZSET scored by `next_attempt_at` (single ZSET; an expired lease is
  re-inserted with score = lease expiry), task envelope in a hash, claim as a Lua script
  (ZRANGEBYSCORE + move + token set — atomic by Redis's single-threaded execution). DLQ is
  a separate ZSET scored by death time. Filters: client-side (no `FilterPushdown`).
- **MongoDB:** one collection per role, claim via `findAndModify` loop (one document per
  call, batched by the engine), partial index mirroring the Postgres one.

Both sketches implement every invariant in §3 with no SPI changes — the contract holds.

## 6. Engine responsibilities (for contrast — NOT in the SPI)

Backoff computation (initial × multiplier^n, capped, jittered) · outcome classification
(FR-26..29) · queue-config resolution at claim time · handler/callback invocation and
timeouts (handler timeout ≤ lease; `ExtendLease` heartbeat for long handlers) · metrics
emission from `Stats` · audit records for redrive/purge (audit sink is engine-side, not a
storage-plugin concern).

## 7. Open items (resolved for v1)

- **OI-1 — resolved v1 (see design 05 §0):** claim fairness across a worker's queues is
  **round-robin** — one `ClaimDue` per queue per tick, batch-capped. Weighted fairness is
  post-v1; the seam is noted (revisit when a queue demonstrably starves others).
- **OI-2 — resolved v1 (see design 05 §0):** `Selector`-by-filter redrive on plugins without
  `FilterPushdown` — core streams `DLQList` and redrives by ids; entries arriving mid-stream
  are excluded and the response `count` is authoritative. Documented as-is (design 04 §2).
- **OI-3 — resolved v1 (see design 05 §0):** `PurgeSucceeded` does **not** archive. `task_ttl`
  owns retention; `PurgeSucceeded`/`Purge` delete. No JSONL-before-delete step.
