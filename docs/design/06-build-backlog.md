# rdq design 06 — v1 build backlog (autopilot execution plan)

Status: Draft v1 · Parent: [05 — Implementation plan](05-implementation-plan.md) · Contracts: [01](01-wire-envelope.md) · [02](02-storage-spi.md) · [03](03-queue-config.md) · [04](04-server-api.md)

This is the granular, dependency-ordered task list that **warden autopilot** executes. Design
05 is the milestone map and rationale; this doc is the backlog. Every task is scoped to a
single agent / single PR-sized change and states its **deliverable, files, acceptance
criteria, and dependencies** so completion is machine-checkable (a test/command passes) rather
than a judgment call.

## How to read a task

```
### T<milestone>.<n> — <title>              deps: T… , T…
Deliverable:  one sentence — the artifact this produces.
Files:        paths created/edited.
Steps:        the concrete work.
Acceptance:   the command(s)/assertion(s) that must pass. Autopilot lands only when green.
```

**Global definition of done** (applies to every code task): the module's `go vet` / build
passes, new code has tests, `go test ./...` (or `./gradlew test`) is green, no `continue-on-error`
gate regresses, and — for files containing real logic — an Apache-2.0 source header is present
(placeholders excluded). Autopilot must run the module's checks before landing each task.

**Autopilot guardrails** (set before handing off):
- One task = one branch/worktree = one landed change; tasks within a milestone land in
  dependency order.
- **Contract-freezing tasks (T1.*) require human approval to land** — they are the frozen
  spec everything else assumes. Configure these as approval-gated in autopilot.
- Anything touching secrets, `git push` to origin, external name registration, or Maven
  publish is **approval-gated**, never autonomous.
- If acceptance can't be made green, autopilot stops and reports — it does not weaken the test.

---

## M0 — Contracts frozen, names claimed, first push

> Gate: nothing in M1+ merges until M0 completes. Several M0 tasks are human-only (external
> registrations, push authorization) — autopilot prepares the diffs and **requests approval**.

### T0.1 — Fold scope decisions into design docs 01–04     deps: —
Deliverable: docs 01–04 patched to match design 05 §0 (single source of truth for the build).
Files: `docs/design/01-wire-envelope.md`, `02-storage-spi.md`, `03-queue-config.md`, `04-server-api.md`.
Steps: mark gRPC intake/callbacks as post-v1 in 04; add `worker.rate_limit` to 03 schema
(v1); note SDK client/worker artifact split in PRD §10 / 03; add reserved `payload_ref` to
01; document per-key deep-merge (03 OI-3 → resolved), task-lookup authz (04 OI-1 → resolved),
batch per-item (04 OI-2 → resolved), round-robin fairness (02 OI-1 → resolved v1).
Additionally fold in **design 05 §0.1 (G1–G20)**: correct PRD §10 module paths to
`github.com/srjn45/rdq/*` and the Maven group to `io.github.srjn45` (G1/OQ-4 — GitHub-based
identity, domain deferred; update PRD §14 OQ-4 to resolved); SPI `Get` replaces `DLQGet` (G4); shared-Postgres-schema
contract + `rdq_schema_version` (G5); Go `error.type` convention (G6) and `LEASE_EXPIRED`
error shape (G7) in design 01; cross-queue id conflict → `ErrIDConflict`/409 (G8); storage
clock is time authority (G9); drain-on-SIGTERM (G10) and `/healthz`+`/readyz` (G11) in 04;
per-instance rate-limit semantics (G12); `DLQList` without attempt bodies by default +
`include_attempts` (G13); mixed-claiming discouraged note (G14); ConfigStore = Postgres +
boot YAML (G16); `rdq migrate` + `--auto-migrate` (G17); metrics exposure (G18).
Acceptance: each resolved open item in 01–04 is either removed or annotated "resolved v1 — see
design 05 §0"; cross-doc links still valid.

### T0.2 — Maven namespace registration (HUMAN)              deps: —
Deliverable: `io.github.srjn45` Sonatype Central namespace registered (free, GitHub-account
verification — no domain needed). **Human action; autopilot cannot perform this — it only
tracks the checklist and blocks T8.4 publishing until confirmed.**
Note (OQ-4 resolution): domain purchase (`retrydlq.dev` is the evaluated affordable
candidate) and `rdq` GitHub-org claim are **deferred** until a docs site is wanted; neither
blocks anything in v1. G1: Go module path stays `github.com/srjn45/rdq/*`, no vanity-import
hosting needed.
Acceptance: owner confirms the Sonatype namespace is verified.

### T0.3 — Canonicalize working copy                         deps: —
Deliverable: `~/dev/rdq` confirmed canonical; `~/dev/kafka-retry-dlq` archived/removed to stop
silent divergence.
Acceptance: only one active working copy; both were identical at `9f5826e` before removal.

### T0.4 — First push + CI on remote (HUMAN-APPROVED)        deps: T0.1, T0.3
Deliverable: `main` pushed to `git@github.com:srjn45/rdq.git`; CI green on the remote; badge live.
Steps: push; verify the `go` and `java` CI jobs pass on GitHub; once modules carry real code
(post-M1), flip the Java quality gates off `continue-on-error`.
Acceptance: remote `main` == local `main`; Actions run green; module paths resolve.

---

## M1 — core/ engine slice  (CONTRACT FREEZE — land with approval)

> Module `github.com/srjn45/rdq/core`. Delivers envelope + SPI + compliance kit + in-memory
> store. When M1 lands, the storage contract is frozen. Fixtures created here are reused by
> Postgres (M2) and Java (M7).

### T1.1 — Envelope types + enums                            deps: T0.*
Deliverable: `core/envelope` package with `Envelope`, `Attempt`, `Error` structs and `Status`
/ `Outcome` enums.
Files: `core/envelope/envelope.go`, `status.go`, `outcome.go`, `envelope_test.go`.
Steps: model every field in design 01 §2 with `snake_case` JSON tags; enums as string types
with `Valid()` + exhaustive parse; reserve `payload_ref *string`.
Acceptance: `go test ./envelope/...`; structs marshal to the design-01 example shape.

### T1.2 — Canonical JSON codec                              deps: T1.1
Deliverable: marshal/unmarshal matching the canonical encoding (design 01 §1) exactly.
Files: `core/envelope/codec.go`, `codec_test.go`, `core/envelope/testdata/*.json` (golden fixtures).
Steps: RFC-3339-millis timestamps (custom time type), integer-ms durations, base64 payload,
ULID id type (vendor/small dep) with generate+parse. Freeze a set of golden fixtures — these
become the cross-language contract fixtures. Fixtures must include: a `LEASE_EXPIRED` attempt
with `error.type = "rdq.LeaseExpired"` (G7) and Go-produced `error.type` values per the G6
convention (classifier-supplied name, else `%T` of the innermost unwrapped error).
Acceptance: golden round-trip `read(write(x)) == x` byte-stable for canonical fields; fixtures
committed under `testdata/`.

### T1.3 — Unknown-field preservation                        deps: T1.2
Deliverable: residual unknown JSON fields captured on read and re-emitted on write (envelope §5.1).
Files: `core/envelope/codec.go` (extend), `codec_test.go`.
Acceptance: a fixture with extra top-level + extra attempt fields round-trips losslessly;
test asserts the residual survives.

### T1.4 — Validation + truncation helpers                   deps: T1.1
Deliverable: charset/length validation and field truncation with the `…[truncated]` sentinel.
Files: `core/envelope/validate.go`, `validate_test.go`.
Steps: queue/handler `[a-z0-9._-]` ≤240; message truncate 4 KiB, stack 64 KiB;
`envelope_version` read-≤-own / write-own rule.
Acceptance: table tests for valid/invalid names, boundary truncation, version-skew read.

### T1.5 — Storage SPI interface + value types              deps: T1.1
Deliverable: `core/spi` package — the `Storage` interface and all value/error types (design 02 §2).
Files: `core/spi/storage.go`, `types.go`, `errors.go`, `capabilities.go`.
Steps: interface per design 02 §2 **as amended by G4/G8/G9**: `Get(ctx, id)` (any status)
replaces `DLQGet`; `Enqueue` returns `ErrIDConflict` when the id exists in a different queue;
doc comments state that the storage backend's clock is the time authority for due-ness and
lease expiry. Value types: `Claimed`, `ClaimToken`, `TaskID`, `Attempt`, `DLQFilter`
(+ `include_attempts` on list, G13), `Selector`, `Page`, `Cursor`, `Stats`, `Capabilities`;
sentinels `ErrStaleClaim`, `ErrNotFound`, `ErrStaleCursor`, `ErrIDConflict`; doc comments
restate atomicity/fencing/due-definition.
Acceptance: `go build ./spi/...`; interface signature reviewed against design 02 §2 line-by-line.

### T1.6 — In-memory reference store                         deps: T1.5, T1.3
Deliverable: `core/memstore` — a correct mutex-guarded `Storage` (the kit's first subject +
the engine's test substrate).
Files: `core/memstore/memstore.go`, `memstore_test.go`.
Steps: fencing token per claim; due = PENDING-due OR IN_FLIGHT-lease-expired; lease reclaim
appends `LEASE_EXPIRED`; idempotent enqueue by id; redrive reset (attempt_count=0,
redrive_count+1, history kept); cursor pagination; `Capabilities{}` all false.
Acceptance: `go test ./memstore/...` for the direct unit behaviors.

### T1.7 — Compliance kit: invariants 1–8                    deps: T1.6
Deliverable: `core/compliance` — exported `Run(t, factory func() spi.Storage)` covering
design 02 §3 invariants 1–8.
Files: `core/compliance/kit.go`, `claims_test.go`, `fencing_test.go`, `dlq_test.go`, etc.
Steps: no-double-claim concurrency harness (N goroutines, exactly-one-valid-token, drop-worker
→ reclaim-after-lease, dead old token); stale-token fencing → `ErrStaleClaim`+no-change;
lease-recovery-counts; atomic-transition; idempotent-enqueue; lossless round-trip (reuse T1.2
fixtures); redrive-reset; stable cursor pagination.
Acceptance: `Run` passes against `memstore` in `core` CI; each invariant has a named subtest.

### T1.8 — Compliance kit: contention benchmark skeleton     deps: T1.7
Deliverable: backend-neutral `BenchmarkClaims` wired into the kit (real number lands in M2).
Files: `core/compliance/bench_test.go`.
Acceptance: `go test -bench=Claims ./compliance/...` runs against `memstore`; documents the
≥1k-claims/sec target it will be held to on Postgres.

### T1.9 — core/ doc + freeze note                           deps: T1.1–T1.8
Deliverable: `core/doc.go` updated; a short `core/README.md` telling a plugin author "implement
`spi.Storage`, run `compliance.Run`, you're done."
Acceptance: `go test ./...` green in `core/`; **human approval to freeze** the contract.

---

## M2 — storage/postgres  (parallel with M3 after M1)

> Module `github.com/srjn45/rdq/storage/postgres`. Must pass the M1 kit against real Postgres.

### T2.1 — Schema + migrations                               deps: T1.9
Deliverable: versioned migrations for `rdq_task`, `rdq_dlq_task`, `rdq_attempt` + indexes.
Files: `storage/postgres/migrations/000x_*.sql`, `schema.go`.
Steps: partial composite index `(queue, next_attempt_at) WHERE status IN ('PENDING','IN_FLIGHT')`;
`claim_token uuid`; `rdq_attempt` referenced by both task tables; JSONB residual column;
`rdq_schema_version` row (G5) — engines refuse to start against an unknown schema version.
**This schema is a cross-language contract**: the Java Postgres binding (T7.4) implements the
same tables and claim semantics, never its own schema — that is what makes the cross-language
loop (T8.2) work.
Acceptance: migrations apply cleanly up/down in a testcontainers Postgres; schema-version gate
tested.

### T2.2 — Envelope ↔ rows mapping                           deps: T2.1
Deliverable: lossless decomposition of the envelope into columns + attempts, residual in JSONB.
Files: `storage/postgres/mapping.go`, `mapping_test.go`.
Acceptance: round-trip test incl. unknown fields (uses T1.2 fixtures).

### T2.3 — Claim + fencing implementation                    deps: T2.2
Deliverable: `ClaimDue` (the design-02 §4 `FOR UPDATE SKIP LOCKED` statement) + fenced
`Reschedule/Complete/DeadLetter/ExtendLease` (token in every `WHERE`).
Files: `storage/postgres/claim.go`, `mutations.go`.
Acceptance: unit tests for each mutation; stale token → `ErrStaleClaim`.

### T2.4 — DLQ / stats / purge / redrive                     deps: T2.2
Deliverable: `DLQList` (SQL filter pushdown + cursor), `DLQGet`, `Redrive`, `Purge`, `Stats`,
`PurgeSucceeded`.
Files: `storage/postgres/dlq.go`, `stats.go`.
Acceptance: unit tests for each; pagination stable under concurrent inserts.

### T2.5 — Capabilities: Notify + FilterPushdown             deps: T2.3, T2.4
Deliverable: `LISTEN/NOTIFY` on enqueue/reschedule; `Capabilities{Notify:true, FilterPushdown:true}`.
Files: `storage/postgres/notify.go`, `capabilities.go`.
Acceptance: a `WaitDue` unblocks on enqueue in a test.

### T2.6 — Run compliance kit + benchmark on Postgres        deps: T2.3, T2.4, T2.5
Deliverable: `compliance.Run` green against Postgres via testcontainers; benchmark ≥1k
claims/sec on a modest node, recorded.
Files: `storage/postgres/compliance_test.go`, `BENCHMARKS.md`.
Acceptance: kit green in CI; benchmark number committed; chaos (kill -9 → reclaim) passes.

---

## M3 — core/ retry engine  (parallel with M2 after M1)

> Under `core/` (e.g. `core/engine`, `core/policy`, `core/registry`). Codes to `spi.Storage`,
> unit-tested on `memstore`.

### T3.1 — Queue-config schema + strict loader + deep-merge  deps: T1.9
Deliverable: config types (design 03 §2), strict validation (unknown keys rejected), per-key
deep-merge of `defaults`.
Files: `core/config/config.go`, `validate.go`, `merge.go`, `*_test.go`, `testdata/*.yaml`.
Acceptance: valid configs load; typo → error at load; deep-merge examples asserted; duration/
size human-unit parsing tested.

### T3.2 — Backoff computation                               deps: T3.1
Deliverable: pure `delay(n)` = `min(initial×mult^(n−1), max) × (1 ± jitter·rand)`.
Files: `core/policy/backoff.go`, `backoff_test.go`.
Acceptance: deterministic tests with injected RNG; bounds and cap verified.

### T3.3 — Outcome classification ladder                     deps: T3.1
Deliverable: precedence resolver (design 03 §4): OutcomeMapper > per-call wrappers > code
classifiers (`errors.Is/As`) > config globs > default(retryable); glob matcher on `error.type`.
Files: `core/policy/classify.go`, `glob.go`, `classify_test.go`.
Acceptance: table tests across all five layers incl. wrapper overrides and glob precedence.

### T3.4 — Handler registry + version/unroutable policy      deps: T1.5
Deliverable: name→handler registry; `handler_version` mismatch (`run-latest|dead-letter`);
unknown `handler_ref` → DLQ with distinct error class, never hot-loop.
Files: `core/registry/registry.go`, `registry_test.go`.
Acceptance: mismatch + unroutable paths tested against `memstore`.

### T3.5 — Per-queue rate limiter                            deps: T3.1
Deliverable: token-bucket gate per queue from `worker.rate_limit` (§0 decision).
Files: `core/engine/ratelimit.go`, `ratelimit_test.go`.
Acceptance: throughput capped to configured rate in a timing test (injected clock).

### T3.6 — Worker runtime: claim loop + leases + heartbeat   deps: T3.2, T3.3, T3.4, T3.5, T1.6
Deliverable: the loop — poll/Notify claim, per-queue round-robin, concurrency fan-out, invoke,
classify, `Reschedule|Complete|DeadLetter`; handler timeout ≤ lease; heartbeat `ExtendLease`;
`ErrStaleClaim` → abandon.
Files: `core/engine/worker.go`, `worker_test.go`, `core/engine/clock.go` (injectable).
Also: **graceful drain** (G10) — on stop/SIGTERM, cease claiming, finish in-flight handlers
within the lease, then return; jittered `PurgeSucceeded` sweeper ticker (G19).
Acceptance: full submit→retry→succeed and submit→exhaust→DLQ loops over `memstore`; lease
overrun → reclaim + `LEASE_EXPIRED`; drain test (no new claims after stop, in-flight
completes); deterministic via injected clock/RNG.

---

## M4 — sdk-go  (embedded; after M3 + M2)

### T4.1 — submit-only sub-package                           deps: T1.9, T3.1
Deliverable: `sdk-go/submit` — `Submit(queue, handler_ref, payload, headers?)` with idempotent
id reuse; importable without the worker.
Files: `sdk-go/submit/submit.go`, `submit_test.go`.
Acceptance: builds and tests without importing `core/engine`.

### T4.2 — worker binding + registration API                 deps: T3.6, T2.6, T4.1
Deliverable: `rdq.Register(name, func(ctx,task) error)`, `rdq.Permanent/Retryable` wrappers,
per-queue `OutcomeMapper`, wires M3 runtime to any `spi.Storage`.
Files: `sdk-go/rdq.go`, `worker.go`, `outcome.go`, `*_test.go`.
Acceptance: registration + wrapper + mapper covered against `memstore` and Postgres.

### T4.3 — sync-retry wrapper + config builder               deps: T4.2, T3.1
Deliverable: in-process bounded retries before durable enqueue (design 03 `sync_retry`); code
builder `rdq.Queue("…").MaxAttempts(…)` + optional YAML loader.
Files: `sdk-go/syncretry.go`, `queuebuilder.go`, `*_test.go`.
Acceptance: sync attempts exhaust → durable enqueue; builder == YAML equivalence test.

### T4.4 — Go example + quickstart                           deps: T4.2, T4.3
Deliverable: runnable example consumer (failing call → retry → DLQ with history) + README.
Files: `sdk-go/examples/consumer/main.go`, `sdk-go/README.md`.
Acceptance: example runs against a testcontainers Postgres end to end.

---

## M5 — server/  (REST + HTTP callbacks; gRPC deferred)

### T5.1 — HTTP scaffolding + errors + OpenAPI               deps: T3.6
Deliverable: `/v1` router, RFC-9457 `problem+json` with stable `code`s, `429 + Retry-After`,
`GET /healthz` + `GET /readyz` outside `/v1` auth (G11), OpenAPI spec committed.
Files: `server/http/*.go`, `server/openapi.yaml`.
Acceptance: error contract + spec lint pass; health endpoints respond (readyz reflects
storage reachability); spec is the normative artifact.

### T5.2 — Data plane                                        deps: T5.1, T4.1
Deliverable: `POST tasks` (202), `:batch` per-item 207 (API OI-2), `GET tasks/{id}`; idempotent
id; 404/413/422 rejections.
Files: `server/http/tasks.go`, `tasks_test.go`.
Acceptance: submit idempotency + batch per-item results tested; rejections asserted.

### T5.3 — DLQ / ops plane                                   deps: T5.1, T2.4
Deliverable: DLQ list/redrive/purge/stats + pause/resume; filter-redrive streaming note
(SPI OI-2), authoritative `count`.
Files: `server/http/dlq.go`, `ops.go`, `*_test.go`.
Acceptance: redrive/purge by ids and by filter; pause stops claiming, submits still accepted.

### T5.4 — ConfigStore + admin plane                         deps: T5.1, T3.1
Deliverable: `ConfigStore` (CRUD + watch) separate from task Storage — v1 backend: Postgres
(same DB, own tables) + YAML file at boot, API wins per design 03 §1 (G16); admin queue-config
CRUD; strict validation; effect at next claim; pause state persisted.
Files: `server/config/store.go`, `server/http/admin.go`, `*_test.go`.
Acceptance: PUT config → next-claim behavior change; delete-non-empty → 409.

### T5.5 — HTTP callback dispatch                            deps: T5.4, T3.3
Deliverable: deliver raw payload + `X-RDQ-*` headers; HMAC signing; `response_mapping`
classification (FR-29 defaults); response body (4 KiB) → `error.detail`; timeout → retryable
`TIMEOUT`.
Files: `server/callback/http.go`, `sign.go`, `*_test.go`.
Acceptance: 2xx/4xx/5xx/timeout classified correctly against a stub receiver; HMAC verifiable.

### T5.6 — AuthN/Z + callback allowlist (SSRF)               deps: T5.2, T5.3, T5.4, T5.5
Deliverable: static token file → principal + per-queue×role grants (submitter/operator/admin);
task-lookup resolves queue from row + enforces grant (API OI-1); global callback allowlist as
server config; strict config validation of secret_ref `env:` indirection.
Files: `server/auth/*.go`, `server/config/server.go`, `*_test.go`.
Acceptance: role matrix enforced; callback URL off-allowlist rejected at config load.

### T5.7 — Cross-process integration test                    deps: T5.5, T5.6, T2.6
Deliverable: submit → HTTP callback → retry → DLQ → redrive loop over HTTP against Postgres.
Files: `server/integration_test.go`.
Acceptance: full loop green in CI (testcontainers).

---

## M6 — cli/ + observability

### T6.1 — Prometheus metrics                                deps: T3.6, T2.4
Deliverable: metrics from `Stats` (retry rate, success-after-retry, DLQ arrivals, DLQ depth,
oldest-pending age, claim latency, handler duration) labeled by queue; `/metrics` on server.
Files: `core/metrics/*.go`, `server/http/metrics.go`, `*_test.go`.
Acceptance: metrics registered + emitted; DLQ-depth and oldest-age present (flagship alerts).

### T6.2 — Structured logs + trace propagation               deps: T3.6
Deliverable: structured log on every state transition (task id + queue); `traceparent`
propagated submit→retry→handler; payloads never logged in full (FR-25).
Files: `core/log/*.go`, wiring in engine/server.
Acceptance: transition-log test; payload-redaction test.

### T6.3 — Audit log                                          deps: T5.3, T5.4
Deliverable: pluggable `AuditSink` interface (G3) recording every DLQ mutation + API config
change (principal, selector, count — FR-18, Config OI-2). Default sink: structured JSON log
(embedded mode). rdq-server additionally ships a Postgres-table sink (`rdq_audit`, same DB as
ConfigStore) so audit history is queryable. The task-storage SPI stays audit-free.
Files: `core/audit/*.go`, `server/audit/pgsink.go`, wiring in server ops/admin.
Acceptance: redrive/purge/pause/config-write each write an audit record through the sink;
both sinks tested.

### T6.4 — rdq CLI                                            deps: T5.2, T5.3, T6.1
Deliverable: `rdq` single-binary CLI — queue stats, DLQ browse/filter/inspect, single + bulk
redrive, purge, `rdq migrate` (G17) — with **two transports** (G2): server-API mode
(`--server URL --token …`) and direct-storage mode (`--dsn postgres://…`, reusing the storage
plugin) so embedded adopters get full ops tooling with zero extra infrastructure. The API
mode keeps the CLI an ordinary client of the public API (the future web-UI contract).
Files: `cli/cmd/*.go`, `cli/main.go`, `cli/README.md`.
Acceptance: each command passes an integration test in **both** transports; `rdq migrate`
applies the T2.1 migrations; help/usage complete.

---

## M7 — sdk-java  (reimplement from spec; validated by contracts, not by porting Go)

### T7.1 — Delete superseded prototype code                  deps: T1.9
Deliverable: remove serialized-lambda `FunctionRegistry`, `SerializedConsumer`,
`InMemoryRetryQueue`, `RetryAsync*` (superseded by named handlers + SPI). Keep `RetrySync`/
`RetryConfig` concepts for the sync-retry layer.
Files: delete under `sdk-java/src/main/java/code/srjn/retry/async/**`; adjust tests.
Acceptance: `./gradlew test` green after removal; no references to deleted classes.

### T7.2 — Split client / worker artifacts                   deps: T7.1
Deliverable: Gradle restructured into `io.github.srjn45:rdq-java-client` (submit) + `io.github.srjn45:rdq-java-worker`
(engine + Postgres binding, depends on client).
Files: `sdk-java/settings.gradle.kts`, module build files.
Acceptance: both artifacts build; client has no worker/engine dependency.

### T7.3 — Envelope (Jackson canonical JSON)                 deps: T7.2
Deliverable: Java envelope model with canonical JSON — **no Java-native serialization** (FR-14);
replays the frozen T1.2 fixtures.
Files: `sdk-java/.../envelope/*.java`, tests loading `core/envelope/testdata/*.json`.
Acceptance: Java reads/writes the shared fixtures byte-compatibly (canonical fields); unknown
fields preserved.

### T7.4 — Storage SPI (Java) + Postgres binding             deps: T7.3
Deliverable: idiomatic-Java `Storage` interface (same contract) + Postgres implementation
binding to the **shared T2.1 schema** (G5) — same tables, same claim semantics, gated by
`rdq_schema_version`; never its own schema.
Files: `sdk-java/.../spi/*.java`, `.../postgres/*.java`.
Acceptance: a Java port of the invariant suite passes against testcontainers Postgres running
the T2.1 migrations; schema-version gate honored.

### T7.5 — Engine: classification/backoff/lease/registry     deps: T7.4
Deliverable: worker engine — exception-class-list classification (hierarchy-aware), backoff,
leases, heartbeat, rate limit, registry, sync-retry (informed by `RetrySync`/`RetryConfig`).
Files: `sdk-java/.../engine/*.java`, tests.
Acceptance: submit→retry→succeed and submit→exhaust→DLQ over Postgres; jacoco verification passes.

### T7.6 — Java example + quickstart                         deps: T7.5
Deliverable: runnable Java consumer example + README.
Acceptance: example runs end to end against testcontainers Postgres.

---

## M8 — Hardening + v1 release

### T8.1 — Chaos / double-claim proof at scale               deps: T2.6, T3.6
Deliverable: multi-worker chaos harness — kill -9 mid-processing, assert zero double-claims,
reclaim after lease (PRD §11 metric).
Files: `storage/postgres/chaos_test.go` (or a dedicated harness).
Acceptance: zero double-claims across the run; recorded.

### T8.2 — Cross-language redrive loop                       deps: T5.7, T7.6, T6.4
Deliverable: task submitted via Go SDK → dead-lettered → redriven → executed by a Java worker,
and via the server HTTP callback path — the flagship cross-language success metric.
Files: `integration/crosslang_test.*` (or a scripted e2e).
Acceptance: the full cross-language loop passes.

### T8.3 — Sizing benchmarks + guidance                      deps: T2.6
Deliverable: throughput/contention benchmarks + documented per-backend sizing guidance.
Files: `docs/operations/sizing.md`, updated `BENCHMARKS.md`.
Acceptance: numbers reproduced in CI; guidance published.

### T8.4 — Packaging + release (HUMAN-APPROVED publishes)    deps: T4.4, T5.7, T6.4, T7.6
Deliverable: server Dockerfile + Helm chart, CLI single-binary release, Maven publish
`io.github.srjn45:*`, Go module tags, quickstart READMEs. Apache source headers added to all real
source files.
Acceptance: images/binaries build in CI; Maven/Go publishing steps are **approval-gated**;
source headers present on non-placeholder files.

### T8.5 — <30-min integration validation                    deps: T4.4, T7.6, T6.4
Deliverable: fresh-user time-to-first-integration validated under 30 minutes (SDK wrap on an
existing consumer), per PRD §11.
Acceptance: a fresh user completes the quickstart in <30 min; friction logged and fixed.

---

## Milestone dependency summary (for the autopilot DAG)

```
M0 ─▶ M1 ─┬─▶ M2 ─┐
          └─▶ M3 ─┴─▶ M4 ─▶ M5 ─▶ M6 ─▶ M8
              M1 ─────────────────▶ M7 ─▶ M8
```

- **Approval-gated (never fully autonomous):** T0.2, T0.4, T1.9 (contract freeze), T8.4 (publishes).
- **Parallel branches autopilot can run concurrently:** M2 ∥ M3 (after M1); M7 ∥ {M4,M5,M6}
  (after M1); within M1, T1.1→T1.4 (envelope) ∥ T1.5 (SPI) until they converge at T1.6.
- **Handoff to autopilot:** feed this backlog as the goal; configure the guardrails above; the
  DAG edges are the `deps:` fields on each task.
