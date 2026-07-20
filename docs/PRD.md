# rdq — Product Requirements Document

**rdq — Retry & Dead-letter Queues for any broker, any storage, any language.**

| | |
|---|---|
| Status | Draft v1 |
| Date | 2026-07-20 |
| Owner | srjn45 |

---

## 1. Problem statement

Every event-driven system eventually faces the same question: *a message handler failed — now what?* Today teams answer it by hand-rolling retry topics per Kafka consumer, wiring broker-specific DLQs (SQS redrive, RabbitMQ dead-letter exchanges), or adopting heavyweight durable-execution platforms. The result is duplicated, broker-locked, language-locked plumbing — and when messages do land in a DLQ, engineers get a payload with no failure context and no safe way to replay it.

The general shape of the problem is broader than messaging: **a function was called with some arguments and it failed.** What teams need is a mechanism that durably remembers the function reference and its arguments, retries the call per a configured policy, and — if it never succeeds — parks it somewhere inspectable and replayable.

## 2. Product overview

rdq is a durable retry engine with first-class dead-letter queues. A client submits a failed unit of work as `(handler reference, payload, retry policy)`. rdq persists it in a datastore **the company already operates** (PostgreSQL in v1; Redis, MongoDB, and others via a storage plugin interface), re-invokes the handler on the configured backoff schedule, and guarantees one of two terminal outcomes:

1. the call eventually **succeeds**, or
2. the task lands in the **DLQ with its full failure history** — every attempt's error, stack trace, and timestamp — where it can be browsed, analyzed, and redriven after a fix ships.

rdq maintains exactly **one retry queue and one DLQ per logical queue**.

### One core, two hosts

The engine (task model, retry policies, storage SPI, DLQ semantics, wire format) is a single core shipped in two form factors — both in v1:

- **Embedded SDK** (Java and Go in v1): hosts the core inside the application process. Handlers are in-process functions. Zero additional infrastructure — the only dependency is the storage backend the team already runs.
- **Standalone service** (`rdq-server`, written in Go): hosts the same core behind REST/gRPC intake APIs. Handlers are remote **callbacks** (HTTP or gRPC) registered per queue. Acts as a central retry hub for an organization; any language can integrate via the API without an SDK.

A callback that times out or returns an error is simply another failed attempt — it flows through the identical retry/DLQ path as an in-process handler failure.

## 3. Goals

- **G1 — Broker-agnostic.** Works with Kafka, SQS, Redpanda, AutoMQ, RabbitMQ, or no broker at all. rdq never talks to the broker; it accepts failures from *any* source.
- **G2 — Bring-your-own-storage.** Retry queues and DLQs live in the adopter's existing datastore via a documented storage SPI. rdq introduces no new stateful infrastructure.
- **G3 — Polyglot by design.** A language-neutral wire format from day one. v1: Java SDK, Go SDK, and REST/gRPC API for everything else.
- **G4 — DLQ as a product, not a graveyard.** Full failure history per task, browse/search, and safe single/bulk redrive.
- **G5 — Horizontally scalable and fault tolerant.** Stateless workers; all coordination delegated to the storage backend via atomic claims and leases. Add nodes to scale; node death is a non-event.
- **G6 — Simple adoption path.** Wrapping an existing consumer with the SDK, or posting a failure to the API, is a < 30 minute integration.

## 4. Non-goals

- **Not a message broker.** rdq does not replace Kafka/SQS; it does not sit on the hot path of successful messages.
- **Not a workflow/durable-execution engine.** No multi-step orchestration, signals, or sagas (Temporal's territory). One task = one function call.
- **Not exactly-once.** rdq promises at-least-once execution and documents that handlers must be idempotent.
- **No ordering guarantee by default.** Retrying out-of-band inherently breaks partition ordering; per-key ordering is a post-v1 opt-in (see §13).
- **Not a scheduler/cron system.** Delayed first execution is incidental capability, not a product surface (post-v1 at most).

## 5. Landscape and differentiation

| Alternative | Gap rdq fills |
|---|---|
| Temporal / Cadence | Heavyweight adoption: new programming model + dedicated cluster. rdq is a bolt-on for one narrow job. |
| spring-kafka retry topics / Uber DLQ pattern | Kafka-only, JVM-only, retry state lives in extra topics with poor inspection/redrive UX. |
| SQS / RabbitMQ native DLQs | Broker-locked, no retry orchestration with backoff policies, no failure context attached, weak redrive tooling. |
| Sidekiq / Hangfire / Celery / RQ | Language-locked and storage-locked job queues; retry is a feature, DLQ analysis is an afterthought. |

**Wedge:** bring-your-own-storage + broker-agnostic + polyglot + first-class DLQ analysis and redrive. No established tool occupies this intersection.

## 6. Personas and primary use cases

- **Backend engineer (SDK user):** wraps a Kafka/SQS consumer in the rdq SDK; failed messages retry with backoff and dead-letter with context instead of being lost or blocking the partition.
- **Platform engineer (server operator):** runs `rdq-server` as a central retry hub; teams in any language integrate via API; scales it horizontally behind a load balancer.
- **On-call engineer (DLQ analyst):** paged on DLQ depth; browses failed tasks, reads the exception history, identifies the bug, and bulk-redrives affected tasks after the fix deploys.

## 7. Core concepts and data model

- **Queue (logical):** a named unit of configuration (e.g. `payments.charge`). Owns one retry policy, one retry queue, one DLQ.
- **Task:** one unit of failed work. Fields: `id` (ULID), `queue`, `handler_ref`, `payload` (bytes), `payload_content_type` (e.g. `application/json`), `headers` (string map, for trace context / source metadata such as original topic-partition-offset), `attempt_count`, `next_attempt_at`, `status` (`PENDING | IN_FLIGHT | SUCCEEDED | DEAD`), `lease_expires_at`, `created_at`.
- **Attempt:** one execution record: `attempt_no`, `started_at`, `finished_at`, `outcome`, `error_type`, `error_message`, `error_stack`. The ordered list of attempts is the task's failure history and travels with it into the DLQ.
- **Retry policy (per queue):** `max_attempts`, `initial_backoff`, `backoff_multiplier` (1 = linear), `max_backoff`, `jitter`, `retryable_errors`, `non_retryable_errors` (skip straight to DLQ), `task_ttl`.
- **Handler reference:** a stable string name — the contract between a stored task and the code that executes it (§9).

## 8. Functional requirements

### 8.1 Intake

- **FR-1 (SDK wrap):** SDKs provide a consumer/function wrapper: on failure of the wrapped call, optionally run **synchronous in-process retries** first (bounded attempts/backoff), then submit the task to durable storage. Submission failure surfaces to the caller — rdq never silently drops work.
- **FR-2 (Direct submit):** SDKs and the server expose `submit(queue, handler_ref, payload, headers?)` for callers that already know the work failed.
- **FR-3 (Server API):** REST and gRPC intake on `rdq-server`, including callback registration per queue (URL/endpoint, protocol, timeout, auth header config).

### 8.2 Outcome contract and result mapping

Not every language or API signals failure by throwing — Go returns `err`, some functions return booleans or status objects, some HTTP APIs return 200-with-error-payload. The engine therefore defines one canonical attempt outcome, and every host maps its local idiom onto it:

- **FR-26 (Canonical outcome):** every handler invocation resolves to exactly one of `SUCCESS`, `RETRYABLE_FAILURE(error)`, or `PERMANENT_FAILURE(error)`, where `error` carries `error_type`, `error_message`, and optional detail/stack for the attempt history. `PERMANENT_FAILURE` bypasses remaining attempts and dead-letters immediately (generalizing "non-retryable errors").
- **FR-27 (Default classification):** absent a custom mapper, the universal rule is: **an error/exception is a failure; any return value (including void/nil) is a success.** Java — normal return = success; thrown exception = failure, classified retryable/permanent by the queue's error lists (retryable by default). Go — handlers are `func(ctx, task) error`; `nil` = success, non-nil = failure, classified via `errors.Is/As` against configured error types, with `rdq.Permanent(err)` / `rdq.Retryable(err)` wrappers for per-call overrides.
- **FR-28 (Custom result mappers):** for functions that signal failure through return values (booleans, status enums, response objects), SDKs accept a per-queue `OutcomeMapper`: `(returnValue, error) → Outcome`. When provided, the mapper is authoritative — it fully replaces the FR-27 default and sees both the return value and any error/exception (so it can still classify thrown errors, e.g. treat a specific exception as success for idempotent replays). The mapper also supplies the error description recorded in the attempt history, so a `false` or `{"status":"FAILED"}` return still yields a meaningful DLQ entry.
- **FR-29 (Callback response mapping, server mode):** defaults — HTTP 2xx → success; 408/429/5xx → retryable; other 4xx → permanent. gRPC `UNAVAILABLE`/`DEADLINE_EXCEEDED`/`RESOURCE_EXHAUSTED`/`ABORTED` → retryable; other non-OK → permanent. Overridable per queue, including an optional response-body mapper (e.g. inspect a JSON `status` field) for APIs that signal failure inside a 200 response.

### 8.3 Retry engine

- **FR-4:** Workers claim due tasks (`next_attempt_at <= now`, status `PENDING`) **atomically** via the storage SPI, invoke the handler, and record the attempt.
- **FR-5:** On failure: increment attempt count, compute next backoff (with jitter), reschedule. On exhaustion of `max_attempts` — or on a non-retryable error — move to DLQ with full attempt history. On success: mark succeeded (retained per `task_ttl`, then purged).
- **FR-6 (Leases):** every claim carries a visibility-timeout lease; expired leases make the task reclaimable by any worker. Handlers exceeding the lease are abandoned-and-retried, never lost.
- **FR-7 (At-least-once):** documented contract; SDK docs prominently state the idempotent-handler requirement.

### 8.4 Storage SPI

- **FR-8:** a minimal, documented plugin interface — approximately: `enqueue(task)`, `claimDue(queue, batch, lease)`, `recordAttempt(...)`, `reschedule(...)`, `complete(...)`, `deadLetter(...)`, `dlqList/dlqGet/dlqSearch(...)`, `redrive(...)`, `purge(...)`, `stats(queue)`.
- **FR-9:** each plugin owns its atomic-claim mechanics: PostgreSQL uses `FOR UPDATE SKIP LOCKED`; Redis uses atomic sorted-set pops (Lua); MongoDB uses `findAndModify`. Correctness requirement: **no two workers may ever claim the same task**.
- **FR-10 (v1):** PostgreSQL is the reference plugin, shipped and production-hardened. The SPI ships as a public contract with a **compliance test-kit** so third parties can build and verify plugins. Redis and MongoDB are fast-follow (§13).

### 8.5 Handler identity and registry

- **FR-11:** handlers register under explicit stable names: `rdq.register("charge-payment", fn)` (or annotation/struct-tag equivalents). No serialized closures — names survive deploys, restarts, and language boundaries.
- **FR-12:** optional handler `version` tag; a task carries the version it was submitted under, and mismatch behavior (`run-latest` | `dead-letter`) is configurable per queue.
- **FR-13:** a task whose `handler_ref` has no registered handler (or callback) is parked as unroutable in the DLQ with a distinct error class — never dropped, never hot-looped.

### 8.6 Wire format

- **FR-14:** one language-neutral task envelope (JSON; payload as opaque bytes + content-type) shared by both SDKs, the server API, and all storage plugins. Versioned with an explicit `envelope_version` field from day one. Java-native serialization and other language-specific encodings are prohibited in the core.

### 8.7 DLQ analysis and redrive

- **FR-15:** DLQ entries carry the complete task plus full attempt history (every error type/message/stack, timestamps).
- **FR-16:** browse and filter DLQ by queue, error type, handler, and time range — via SDK, server API, and `rdq` CLI.
- **FR-17 (Redrive):** re-enqueue to the retry queue with a reset policy: single task, bulk by filter (e.g. "all `payments.charge` tasks that failed with `TimeoutException` after 14:00"). Optional payload edit on single-task redrive (audit-logged). Purge with the same granularity.
- **FR-18:** every DLQ mutation (redrive, purge, edit) writes an audit record: who, when, what filter.

### 8.8 Scaling and fault tolerance

- **FR-19:** all engine processes (embedded workers and `rdq-server` nodes) are stateless; the storage backend is the only coordination point. No leader election, no cluster membership.
- **FR-20:** N app instances running the embedded SDK, or N `rdq-server` nodes, safely share one storage backend through atomic claims — horizontal scaling is "add another instance."
- **FR-21:** rdq's availability story inherits the storage backend's HA (Postgres replicas, Redis Sentinel, Mongo replica sets). rdq adds no stateful component of its own.

### 8.9 Observability

- **FR-22:** metrics (Prometheus format): retry rate, success-after-retry rate, DLQ arrivals, **DLQ depth**, **age of oldest pending task**, claim latency, handler duration — labeled by queue. DLQ depth and oldest-task age are the flagship alerting signals.
- **FR-23:** `rdq` CLI for queue stats, DLQ browse/inspect, and redrive. (Web UI is post-v1, §13.)
- **FR-24:** structured logs with task id + queue on every state transition; trace-context headers propagate through submit → retry → handler invocation.

### 8.10 Security (server mode)

- **FR-25:** API authentication (static token minimum; pluggable). Per-queue authorization: which principals may submit vs. redrive/purge. TLS for intake and callbacks; configurable auth headers on outbound callbacks. Payloads treated as sensitive: never logged in full.

## 9. v1 scope summary

**In v1:** core engine · one wire format · storage SPI + compliance kit · PostgreSQL plugin · Java SDK (embedded worker + sync-retry wrapper) · Go SDK (same) · `rdq-server` in Go (REST + gRPC intake, HTTP + gRPC callbacks) · `rdq` CLI · Prometheus metrics · DLQ browse + redrive + audit log.

**Explicitly out (see §13):** web UI, Redis/Mongo plugins, Python/TS SDKs, per-key ordering, payload encryption at rest, scheduled/delayed first execution, multi-tenancy beyond per-queue auth.

## 10. Repository and packaging

Monorepo, renamed **`rdq`** (GitHub redirect from `kafka-retry-dlq` preserves history):

Go modules are `github.com/srjn45/rdq/*` and the Maven group is `io.github.srjn45` — GitHub-based
identity everywhere, no domain purchase (OQ-4 resolved, see §14 and design 05 §0/§0.1/G1).

```
rdq/
  core/          # Go module github.com/srjn45/rdq/core — engine semantics, envelope, SPI + compliance kit
  sdk-java/      # Maven: io.github.srjn45:rdq-java-client (submit) + io.github.srjn45:rdq-java-worker (engine)
  sdk-go/        # Go module github.com/srjn45/rdq/sdk-go — worker + a submit-only sub-package
  server/        # github.com/srjn45/rdq/server — rdq-server (Go): API, callback dispatch, Dockerfile + Helm chart
  storage/postgres/  # github.com/srjn45/rdq/storage/postgres
  cli/           # github.com/srjn45/rdq/cli — rdq CLI (Go, single binary)
  docs/
```

**SDK client/worker artifact split (OQ-1 resolved, design 05 §0).** The engine ships as a
submit-only artifact plus a worker artifact so apps can "submit here, execute there": Java
`io.github.srjn45:rdq-java-client` (submit) + `io.github.srjn45:rdq-java-worker` (engine,
depends on client); Go a `submit` sub-package importable without the worker.

The existing prototype (`RetrySync`, `RetryConfig` policy concepts) informs the Java SDK's sync-retry layer; the serialized-lambda `FunctionRegistry` and `InMemoryRetryQueue` are superseded by §8.5 and the storage SPI.

## 11. Success metrics

- Time-to-first-integration (SDK wrap on an existing consumer) under 30 minutes, validated with fresh users.
- A task submitted in Go is redriven and successfully executed after a handler fix — the full loop works cross-language via the server.
- Storage SPI compliance kit passes for Postgres plugin at ≥1k task claims/sec on a single modest node, with zero double-claims under a multi-worker chaos test (kill -9 during processing → task reclaimed after lease expiry).
- External signal post-launch: a third-party storage plugin or SDK appears (proof the SPI/wire-format contracts are usable).

## 12. Risks

- **Scope: two SDKs + server in one release.** Mitigation: the shared envelope + SPI contracts are frozen first; SDKs and server are thin hosts over them. Cut line if needed: server ships with HTTP callbacks only (gRPC callbacks fast-follow).
- **Storage-backend performance variance.** Mitigation: compliance kit includes throughput/contention benchmarks; documented sizing guidance per backend.
- **"rdq" vs Python's "rq" confusion.** Accepted; distinct enough, and the expansion tagline disambiguates.
- **Callback security surface (SSRF via attacker-controlled callback URLs).** Mitigation: callback allowlists + per-queue registration auth in FR-25.

## 13. Post-v1 roadmap

1. **Web UI** for DLQ analysis and redrive (the on-call engineer's console).
2. **Redis and MongoDB storage plugins** (validating SPI pluggability with the compliance kit).
3. **Python and TypeScript SDKs.**
4. **Per-key ordered retry** (opt-in: hold processing of key K while an earlier task for K is pending).
5. **Broker-native intake adapters** (e.g. a Kafka consumer-group adapter that dead-letters on behalf of legacy apps with zero code change).
6. Payload encryption at rest; delayed/scheduled first execution; org-level multi-tenancy.

## 14. Open questions

- **OQ-1:** Should the Java SDK's embedded worker be opt-in (separate artifact) so apps can run submit-only and delegate execution to `rdq-server`? (Leaning yes — "submit here, execute there" is a likely hybrid deployment.)
- **OQ-2:** Envelope payload limit and large-payload story (inline bytes vs. claim-check pointer to object storage)?
- **OQ-3:** Success-record retention default (`task_ttl`) — keep succeeded tasks for observability, and for how long?
- **OQ-4 — resolved v1 (see design 05 §0/§0.1):** GitHub-based identity everywhere, no purchases. Go modules `github.com/srjn45/rdq/*` (G1); Maven group `io.github.srjn45` (free, Sonatype GitHub-verified — no domain proof needed, G15). A domain (`retrydlq.dev` is the evaluated affordable candidate; `rdq.dev` is premium-priced) and/or an `rdq` GitHub org are **deferred** until a docs site is wanted — both are additive later (a new Maven group can be introduced alongside; an org rename keeps redirects) and neither blocks v1.
