# rdq design 05 — v1 implementation plan

Status: Draft v1 · Companions: [PRD](../PRD.md), [01 — Wire envelope](01-wire-envelope.md), [02 — Storage SPI](02-storage-spi.md), [03 — Queue config](03-queue-config.md), [04 — Server API](04-server-api.md)

This is the build order for v1. It sequences the work so that **contracts freeze first**
and everything downstream is a thin, independently-testable host over them (PRD §12 risk
mitigation). The critical path is the vertical slice `core → postgres → engine`: once a task
can be submitted, claimed atomically, retried on a backoff, and dead-lettered with history,
every remaining surface (SDKs, server, CLI) is a client of that loop.

## 0. Scope decisions folded into this plan

Resolutions of the open questions, so the milestones below are unambiguous. Design docs
01–04 get patched to match as part of **M0**.

| Ref | Question | Decision for v1 |
|---|---|---|
| PRD §12 cut-line | gRPC intake + gRPC callbacks | **Deferred to fast-follow.** v1 server = REST intake + HTTP callbacks only. Parity is already speced; gRPC is purely additive and needs no contract change. |
| Config OI-1 | Per-queue rate limiting | **In v1.** `worker.rate_limit` (token bucket per queue), engine-enforced. Bulk redrive after an outage is exactly the thundering-herd case; the redrive story (FR-17) is a flagship feature and must ship with its safety valve. |
| OQ-1 | Submit-only vs embedded worker | **Split artifacts.** Java: `io.github.srjn45:rdq-java-client` (submit) + `io.github.srjn45:rdq-java-worker` (engine, depends on client). Go: `submit` sub-package usable without importing the worker. Enables "submit here, execute there" hybrid; the module seam is drawn now, not retrofitted. |
| OQ-2 | Large-payload story | **Defer claim-check.** v1 enforces the per-queue `max_payload_size` (default 1 MiB) and rejects larger with `PAYLOAD_TOO_LARGE`. Object-storage pointer is post-v1; reserve `payload_ref` as a future envelope field so adding it stays additive. |
| OQ-3 | Succeeded-task retention default | **`ttl_succeeded: 24h`** (already the config default). `PurgeSucceeded` enforces it on a sweep. |
| OQ-4 | Name/domain claims | **GitHub-based identity everywhere, no purchases.** Go modules `github.com/srjn45/rdq/*` (G1); Maven group `io.github.srjn45` (free Sonatype GitHub-verified namespace — no domain proof needed). A domain (`retrydlq.dev` evaluated as the affordable candidate; `rdq.dev` is premium-priced) and/or `rdq` GitHub org are **deferred** until a docs site is wanted; both are additive later (a new Maven group can be introduced alongside; org rename keeps redirects). |
| SPI OI-1 | Multi-queue claim fairness | **Round-robin across a worker's queues in v1**, one `ClaimDue` per queue per tick, batch-capped. Weighted fairness is post-v1; note the seam. |
| SPI OI-2 | Filter-redrive consistency | **Documented as-is:** on plugins without `FilterPushdown`, core streams `DLQList` and acts by ids; mid-stream arrivals are excluded; response `count` is authoritative. |
| SPI OI-3 | Purge vs archive | **No archive.** `task_ttl` owns retention; `PurgeSucceeded`/`Purge` delete. |
| Config OI-2 | Config-change audit | **In v1** for API-sourced config writes (same audit sink as redrive/purge). |
| Config OI-3 | `defaults` merge semantics | **Per-key deep-merge**, documented with examples. Block-replace causes surprising resets. |
| API OI-1 | Task-lookup authz | **Resolve queue from the task row, enforce the queue grant.** No global read role. |
| API OI-2 | Batch-submit atomicity | **Per-item results** (`207`-style array). Idempotent ids make per-item retry safe. |

### 0.1 Gap-review resolutions (second pass, 2026-07-20)

A pre-handoff review of PRD + designs 01–04 surfaced these; all are folded into T0.1's doc
patch and the affected backlog tasks.

| # | Gap | Resolution for v1 |
|---|---|---|
| G1 | Canonical Go module path (freezes at first push) | **Keep `github.com/srjn45/rdq/*`.** Zero external dependencies; PRD §10's `rdq.dev/sdk-go` is corrected. A vanity/org path later is a known breaking migration, accepted. |
| G2 | Embedded adopters have no DLQ ops tooling (FR-16 promises CLI, but CLI was API-only) | **CLI gains a direct-storage mode** (`--dsn postgres://…`) alongside server-API mode, reusing the storage plugin. Zero-extra-infra ops for the SDK persona. |
| G3 | FR-18 audit records had no defined durable home | **Pluggable `AuditSink` interface.** Default: structured JSON log (embedded). rdq-server additionally ships a Postgres-table sink (`rdq_audit`, same DB as ConfigStore) so "who redrove what" is queryable. SPI stays audit-free. |
| G4 | **SPI has no `Get(ctx, id)`** — `GET /v1/tasks/{id}` (any status) was unimplementable; `DLQGet` only covers DEAD | **Add `Get(ctx, id) (Envelope, error)` to the SPI** (and retire `DLQGet` as redundant). Caught before freeze — contract change is free now. |
| G5 | **The Postgres schema is itself a cross-language contract** — the Java worker binds to the *same* tables as the Go plugin (that's what makes the cross-language loop T8.2 work), but nothing said so | Schema + claim semantics are **owned by `storage/postgres` migrations**; the Java binding implements the same schema version, never its own. A `rdq_schema_version` row gates startup: engines refuse to run against an unknown schema version. |
| G6 | Go has no canonical `error.type` string (Java has class names); classification globs and fixtures match on it | Convention: wrapper/classifier-supplied name wins; otherwise `fmt.Sprintf("%T", err)` of the innermost unwrapped error. Documented in design 01; frozen in the M1 fixtures. |
| G7 | `LEASE_EXPIRED` attempt's `error` object shape undefined | `error.type = "rdq.LeaseExpired"`, message states the lease deadline; no stack. Fixed in the M1 fixtures. |
| G8 | Idempotent `Enqueue` with a client-supplied id that already exists **in a different queue** | Not a no-op: reject (`409`/`ErrIDConflict`). Silent cross-queue no-op would return a confusing foreign envelope. |
| G9 | Whose clock defines "due"? (engine computes `next_attempt_at`, storage evaluates it) | **The storage backend's clock is the time authority** for due-ness and lease expiry (Postgres `now()`). Engines tolerate skew; documented in design 02. |
| G10 | No graceful shutdown story (k8s rollouts would look like worker crashes) | Drain on SIGTERM: stop claiming, finish in-flight handlers within the lease, then exit. Required behavior of the M3 worker runtime and rdq-server. |
| G11 | No health/readiness endpoints (Helm chart needs them) | `GET /healthz` (liveness) + `GET /readyz` (storage reachable) on rdq-server, outside `/v1` auth. |
| G12 | `worker.rate_limit` semantics with N instances | **Per-instance** token bucket (coordination-free, matches FR-19 statelessness); effective global rate = N × limit. Documented loudly — a global limiter would require storage coordination and is post-v1 at most. |
| G13 | DLQ grows unbounded; `DLQList` returns full envelopes incl. full attempt histories (heavy pages) | v1: `DLQList` returns envelopes **without attempt bodies** by default (`include_attempts` opt-in); `Get` returns everything. DLQ retention stays manual (purge + depth metric alerting); optional `ttl_dead` is post-v1. |
| G14 | Mixed claiming — embedded workers *and* rdq-server callback dispatch on the same queue | Allowed by the claim semantics but **documented as discouraged** (two execution paths, confusing ops). Not enforced in v1. |
| G15 | Maven group requires a verifiable namespace (a domain-based group needs proof of domain ownership) | **Moot under OQ-4's resolution:** `io.github.srjn45` is GitHub-verified and free — no domain required. If a domain-based group is adopted later, its claim must precede the Sonatype namespace registration. |
| G16 | ConfigStore backend unspecified | v1 ships a Postgres `ConfigStore` (same DB, own tables) + YAML file at boot; API-written config wins per design 03 §1. |
| G17 | Who runs migrations, when | `rdq migrate` CLI subcommand (explicit, CI/CD-friendly) + opt-in `--auto-migrate` flag on rdq-server. Never silently on by default. |
| G18 | Embedded metrics exposure | SDKs expose a Prometheus `Collector`/registry hook the app mounts; rdq-server serves `/metrics` itself. |
| G19 | `PurgeSucceeded` sweep scheduling | Jittered background ticker in the worker runtime; concurrent sweeps are harmless (idempotent delete-older-than). |
| G20 | Two invariant suites (Go kit + Java port) can drift | Accepted for v1, mitigated: both consume the **same frozen JSON fixtures**, and G5's shared-schema rule means the Java binding is additionally checked by the Go kit's behavior on the same tables. A language-neutral scenario-fixture kit is a post-v1 idea. |

## 1. Milestone map

```
M0  Contracts frozen + names claimed + first push        (gate: nothing merges before this)
     │
M1  core/ engine slice — envelope, SPI, compliance kit    ◄── the starting point
     │        (deliverable: a plugin author has everything to build & self-verify a plugin)
     ├──────────────┐
M2  storage/postgres│  M3  core/ retry engine (claim→invoke→classify→outcome, backoff,
     (passes M1 kit)│       leases, heartbeat, rate limit, registry, outcome mappers)
     └──────┬───────┘        │
            └────────┬───────┘
M4  sdk-go  ─────────┤  embedded worker + submit-only, over M3+M2
     │               │
M5  server/ ─────────┤  REST intake, HTTP callbacks, DLQ/ops/admin, auth, ConfigStore
     │               │
M6  cli/ + observability  (Prometheus metrics, structured logs, audit log, rdq CLI)
     │
M7  sdk-java  reimplements the engine FROM THE SPEC, kept honest by M1 contracts + kit
     │
M8  Hardening + v1 release  (chaos/double-claim proof, cross-language redrive loop, docs,
                             packaging, sizing benchmarks)
```

Dependency notes: **M2 and M3 parallelize** once M1 lands (M2 builds against the compliance
kit; M3 codes against the SPI interface and tests against an in-memory reference plugin). M4
needs both. M7 (Java) can begin any time after M1 froze the contracts — it is deliberately
insulated from the Go code and validated only through the wire format + compliance kit, which
is what keeps the two engines honest.

---

## M0 — Contracts frozen, names claimed, first push

**Goal:** lock the four design docs against the §0 decisions and establish the shared remote
so the two working copies stop being able to silently diverge.

- Patch docs 01–04 to reflect §0 (gRPC deferred, rate-limit in v1, SDK split, deep-merge,
  authz/batch resolutions, `payload_ref` reserved).
- **Register the `io.github.srjn45` Sonatype namespace** (free GitHub verification, OQ-4).
  Domain / GitHub-org claims deferred — nothing blocks on them.
- Confirm `~/dev/rdq` is canonical; delete/park `~/dev/kafka-retry-dlq`. Push `main` to
  `git@github.com:srjn45/rdq.git` — CI badge and Go module paths go live here.
- Turn CI's `continue-on-error` Java quality gates into hard gates once the code is real
  (leave lenient while modules are placeholders).

**Exit:** design docs match §0; `main` pushed; CI green on the remote; one canonical repo.

---

## M1 — core/ engine slice (the starting point)

**Goal:** the contract layer, in Go, complete enough that a third party could build a storage
plugin against it and self-verify — *before* rdq's own engine or Postgres plugin exist. This
is the requested first slice and the highest-leverage work in the project: everything else is
a host over it.

Package layout under `core/` (module `github.com/srjn45/rdq/core`):

```
core/
  envelope/      # the wire model (design 01)
  spi/           # the Storage interface + value types (design 02)
  compliance/    # the plugin test kit (design 02 §3)
  memstore/      # in-memory reference Storage — the kit's own first subject
```

### M1.1 — Envelope types (`core/envelope`)

- Go structs for `Envelope`, `Attempt`, `Error`, enums `Status`
  (`PENDING|IN_FLIGHT|SUCCEEDED|DEAD`) and `Outcome`
  (`SUCCESS|RETRYABLE_FAILURE|PERMANENT_FAILURE|LEASE_EXPIRED`).
- **Canonical JSON**: `snake_case`, RFC-3339-millis timestamps, integer-ms durations, ULID
  ids, base64 payload + `payload_content_type`. Custom (un)marshal where Go defaults don't
  match (time precision, duration-as-ms).
- **Unknown-field preservation**: capture-and-re-emit residual fields (envelope §5.1) so a
  plugin's round-trip and a forward-version reader don't drop data. This is a contract
  requirement, not a nicety — the compliance kit tests it.
- Validation helpers: queue/handler charset (`[a-z0-9._-]`, ≤240), `envelope_version`
  handling (read ≤ own, write own), field-length truncation markers (message 4 KiB, stack
  64 KiB) with the `…[truncated]` sentinel.
- ULID generation + parsing (a small dependency or vendored implementation).
- Reserve `payload_ref` as a documented-but-unused optional field (OQ-2 seam).

*Tests:* golden JSON fixtures (the same fixtures the compliance kit reuses), round-trip
identity incl. unknown fields, truncation, version-skew read.

### M1.2 — Storage SPI (`core/spi`)

- The `Storage` interface exactly as design 02 §2: `Enqueue`, `ClaimDue`, `ExtendLease`,
  `Reschedule`, `Complete`, `DeadLetter`, `DLQList`, `DLQGet`, `Redrive`, `Purge`, `Stats`,
  `PurgeSucceeded`, `Capabilities`.
- Value types: `Claimed{Envelope, ClaimToken}`, `ClaimToken`, `TaskID`, `Attempt`,
  `DLQFilter`, `Selector` (ids | filter), `Page`/`Cursor`, `Stats`, `Capabilities`.
- Sentinel errors: `ErrStaleClaim`, `ErrNotFound`, `ErrStaleCursor` — the fencing and
  pagination contract lives in these.
- Doc comments restate the atomicity/fencing/due-definition guarantees so an implementer
  reads the contract at the call site.

### M1.3 — In-memory reference store (`core/memstore`)

- A correct, mutex-guarded `Storage` implementation. It is (a) the compliance kit's first
  subject — the kit must pass against it before Postgres exists — and (b) the substrate M3's
  engine unit-tests run on, so the retry loop is testable without a database.
- Implements fencing (token per claim), lease expiry + `LEASE_EXPIRED` append, idempotent
  enqueue, redrive reset, cursor pagination. `Capabilities{}` all false (pure floor).

### M1.4 — Compliance kit (`core/compliance`)

- A table of invariant tests (design 02 §3, invariants 1–8) exposed as an exported
  `Run(t, factory func() spi.Storage)` so any plugin imports it and runs it against its own
  implementation.
- **Concurrency/chaos harness** for invariant 1 (no double-claim): N goroutines hammering
  `ClaimDue`, assert each task claimed by exactly one valid token; simulate a dropped worker
  (stop heartbeating) and assert reclaim-after-lease + dead old token.
- **Fencing** (invariant 2): stale-token `Reschedule/Complete/DeadLetter/ExtendLease` →
  `ErrStaleClaim`, no state change.
- Round-trip (6), redrive reset (7), stable pagination (8), idempotent enqueue (5),
  lease-recovery-counts (3), atomic-transition (4) checks.
- **Contention benchmark** skeleton (`BenchmarkClaims`) targeting ≥1k claims/sec, wired but
  backend-agnostic (real number comes from Postgres in M2).
- `testcontainers` plumbing is provided by the *plugin* (M2 wires Postgres); the kit itself
  stays backend-neutral and runs against `memstore` in `core`'s own CI.

**Exit:** `go test ./...` in `core/` green; the compliance kit passes against `memstore`;
JSON fixtures are frozen and shared. A plugin author now has: envelope spec + Go types, the
SPI interface, and a runnable kit that tells them when they're done. **This is the freeze
point for the storage contract.**

---

## M2 — storage/postgres (reference plugin)

**Goal:** the production plugin, passing M1's kit against real Postgres.

- Versioned migrations: `rdq_task`, `rdq_dlq_task`, `rdq_attempt`, partial composite index
  `(queue, next_attempt_at) WHERE status IN ('PENDING','IN_FLIGHT')`, `claim_token` UUID.
- The claim statement (design 02 §4) — single `UPDATE … FOR UPDATE SKIP LOCKED … RETURNING`.
- Fencing = `claim_token` match in every mutation's `WHERE`.
- Envelope decomposition into columns + attempts table; residual unknown fields stored as a
  JSONB column and re-emitted (round-trip invariant).
- `Notify` capability via `LISTEN/NOTIFY` on enqueue/reschedule; `FilterPushdown` (SQL
  `WHERE` on `DLQFilter`); cursor pagination on a stable key (id).
- Run the M1 compliance kit with `testcontainers-go`; **hit the ≥1k claims/sec single-node
  benchmark** and record it (PRD §11).

**Exit:** compliance kit + benchmark green against Postgres in CI; chaos test (kill -9 mid
processing → reclaim) passes.

---

## M3 — core/ retry engine (parallel with M2)

**Goal:** the decision layer — turn a claimed task into an outcome and the next state. Codes
against the `spi.Storage` interface; unit-tests on `memstore`.

- **Claim loop / worker runtime:** poll `ClaimDue` (respect `Notify` when present, else
  `poll_interval` floor), per-queue round-robin (SPI OI-1), `concurrency` fan-out.
- **Outcome classification (FR-26–28):** the precedence ladder — `OutcomeMapper` >
  per-call `Permanent/Retryable` wrappers > code classifiers (`errors.Is/As`) > config globs
  > default(retryable). Language-neutral glob matcher on `error.type`.
- **Backoff:** `min(initial × mult^(n−1), max) × (1 ± jitter·rand)`; write `next_attempt_at`;
  `Reschedule`. Exhaustion or `PERMANENT_FAILURE` → `DeadLetter`. Success → `Complete`.
- **Leases + heartbeat:** handler timeout ≤ lease; `ExtendLease` heartbeat loop when
  `heartbeat: true`; on `ErrStaleClaim` the handler abandons.
- **Rate limiting (§0):** per-queue token bucket gating handler invocations (`worker.rate_limit`).
- **Registry (FR-11–13):** name→handler map; `handler_version` mismatch policy
  (`run-latest|dead-letter`); unroutable `handler_ref` → DLQ with a distinct error class,
  never hot-looped.
- **Queue-config resolution at claim time** (design 03) with per-key deep-merge of `defaults`
  (Config OI-3); strict validation of the config schema at load.
- Backoff/classification are deterministic pure functions with heavy unit coverage.

**Exit:** engine drives a full submit→retry→succeed and submit→exhaust→DLQ loop over
`memstore` in tests; all classification/backoff/lease edge cases covered.

---

## M4 — sdk-go (embedded)

**Goal:** the Go embedded SDK, split submit-only vs worker (§0).

- `submit` sub-package: `Submit(queue, handler_ref, payload, headers?)`, idempotent id reuse —
  importable without pulling in the worker.
- Worker package: wires M3's runtime + M2 (or any `spi.Storage`); `rdq.Register(name, fn)`
  with `func(ctx, task) error` signature, `rdq.Permanent/Retryable` wrappers, per-queue
  `OutcomeMapper`, sync-retry wrapper (in-process attempts before durable enqueue, design 03).
- Code-builder queue config (`rdq.Queue("…").MaxAttempts(…)`) and optional YAML loader.

**Exit:** an example Go consumer wraps a failing call, retries with backoff, dead-letters
with history; submit-only build compiles without the worker.

## M5 — server/ (rdq-server)

**Goal:** the standalone host — REST + HTTP callbacks (gRPC deferred, §0).

- REST data plane (`POST tasks`, `:batch` per-item 207, `GET tasks/{id}`), DLQ/ops
  (list/redrive/purge/stats/pause/resume), admin (queue config CRUD) — design 04.
- **HTTP callback dispatch:** deliver raw payload + `X-RDQ-*` headers, HMAC signing,
  `response_mapping` classification, response-body → `error.detail`.
- `ConfigStore` (CRUD + watch) separate from the task `Storage`; pause state; **callback
  allowlist** (SSRF) as server config; strict config validation.
- AuthN/Z: static token file → principal + per-queue×role grants; task-lookup resolves queue
  from the row and enforces the grant (API OI-1).
- OpenAPI spec is the normative artifact and ships in the repo.

**Exit:** cross-process submit→callback→retry→DLQ→redrive loop works over HTTP against Postgres.

## M6 — cli/ + observability

- Prometheus metrics from `Stats` (retry rate, success-after-retry, DLQ arrivals, **DLQ
  depth**, **oldest-pending age**, claim latency, handler duration — labeled by queue).
- Structured logs with task id + queue on every transition; trace-context propagation.
- **Audit log** for every DLQ mutation + API config change (who/when/what filter, FR-18,
  Config OI-2), engine-side sink.
- `rdq` CLI: queue stats, DLQ browse/filter/inspect, single + bulk redrive, purge — a plain
  API client (same surface a future web UI will use).

## M7 — sdk-java

**Goal:** reimplement the embedded engine **from the spec**, not by porting Go — kept honest
by the M1 contracts + a Java run of the compliance kit against the wire fixtures.

- Split `io.github.srjn45:rdq-java-client` (submit) + `io.github.srjn45:rdq-java-worker` (engine + Postgres
  binding), §0.
- Envelope (Jackson, canonical JSON — **no Java-native serialization**, FR-14), Storage SPI
  (idiomatic Java names, same contract), engine (classification via exception-class lists,
  hierarchy-aware; backoff; leases; heartbeat; rate limit; registry).
- The existing prototype's `RetrySync`/`RetryConfig` inform the sync-retry layer; the
  serialized-lambda `FunctionRegistry`/`InMemoryRetryQueue` are **deleted** (superseded by
  named handlers + the SPI) at the start of this milestone.
- Java replays the frozen JSON fixtures and runs the invariant suite against its Postgres
  binding — proving the two engines agree at the wire + storage level.

## M8 — Hardening + v1 release

- Chaos/double-claim proof at scale (multi-worker, kill -9, lease reclaim) — the §11 success
  metric.
- **Cross-language loop**: task submitted via Go SDK, dead-lettered, redriven, executed by a
  Java worker (and via the server callback path) — the flagship cross-language success metric.
- Sizing benchmarks + documented guidance per backend; time-to-first-integration validated
  <30 min with a fresh user.
- Packaging: server Dockerfile + Helm chart, CLI single binary, Maven publish `io.github.srjn45:*`,
  Go module tags. README/quickstarts. Apache source headers added to all real source files
  (not before — placeholders stay bare).

---

## 2. Sequencing rationale

1. **Freeze contracts before hosts (M0–M1).** The named risk (two SDKs + server in one
   release, PRD §12) is only survivable if envelope + SPI are frozen and everything else is
   thin. M1 ships the kit *with* the contract so "frozen" is enforceable, not aspirational.
2. **Vertical slice first (M1→M2/M3→M4).** One real end-to-end retry+DLQ path over Postgres
   is worth more than breadth; it de-risks the SPI and the engine simultaneously and is the
   substrate every later surface reuses.
3. **Parallelize where the interface allows.** M2 and M3 both depend only on M1's interface,
   not on each other — Postgres codes to the kit, the engine codes to `memstore`. M7 (Java)
   can start as soon as M1 freezes, insulated by the wire format.
4. **Server before CLI** so the CLI is validated as an ordinary API client (no private
   endpoints), which is also the web-UI contract for post-v1.
5. **Java last among hosts** because it is the highest-cost reimplementation and benefits
   most from a fully-exercised, fixture-backed contract — it inherits a hardened spec.

## 3. Open items this plan does not close

- **Weighted multi-queue fairness** (SPI OI-1) — v1 ships round-robin; revisit when a queue
  demonstrably starves others.
- **Async bulk-redrive jobs** (API §6) — v1 redrive is synchronous, bounded by selector size;
  million-entry DLQs get the `202 + job id` API post-v1.
- **Claim-check large payloads** (OQ-2) — reserved via `payload_ref`; design when a real
  >1 MiB workload appears.
