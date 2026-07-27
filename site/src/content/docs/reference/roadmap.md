---
title: Roadmap
description: What ships in rdq v1 versus the post-v1 roadmap — web UI, Redis/Mongo plugins, more SDKs, ordered retry, broker adapters, encryption, scheduling, and multi-tenancy.
---

This page tracks what is in the v1 release versus what is planned for after it,
faithful to the [PRD](https://github.com/srjn45/rdq/blob/main/docs/PRD.md) (§9 scope,
§13 roadmap). "Later" items are directional, not committed dates.

## v1 scope

Everything below is in the first release — one engine shipped in two form factors
(embedded SDK and `rdq-server`) over shared, frozen contracts.

| Area | In v1 |
|---|---|
| Core engine | Task model, retry policies, leases, outcome classification |
| Wire format | One language-neutral, versioned task envelope |
| Storage | Storage SPI + compliance test-kit; **PostgreSQL** reference plugin (`FOR UPDATE SKIP LOCKED`), production-hardened |
| SDKs | **Java** SDK (embedded worker + sync-retry wrapper) and **Go** SDK (same) |
| Server | `rdq-server` in Go — **REST intake + HTTP callbacks** |
| CLI | `rdq` CLI — queue stats, DLQ browse/inspect, redrive/purge |
| DLQ | Full attempt history, browse + filter, single/bulk redrive, purge, audit log |
| Observability | Prometheus metrics (DLQ depth, oldest-pending age, retry/success-after-retry rates, claim latency, handler duration) |
| Security | Static bearer token auth, per-queue×role grants, callback allowlist, TLS-terminating deployment |

### Deferred within v1 (specified, additive later)

These are speced for parity but land as a fast-follow — each is additive over the
v1 contract, so no breaking change is needed to add them:

- **gRPC intake** and **gRPC callbacks** (v1 ships REST + HTTP callbacks). If scope
  pressure hits, gRPC callbacks are the documented cut line.
- **Async bulk-redrive jobs** (`202` + job id) for million-entry DLQs — v1 redrive
  is synchronous, bounded by selector size.
- **DLQ watch/streaming (SSE)** and the **response-body outcome mapper**.

## Post-v1 roadmap

In roughly the PRD's stated order:

1. **Web UI** — the on-call engineer's console for DLQ analysis and redrive. It
   consumes exactly the same [server API](/rdq/reference/server-api/) as the CLI —
   no private endpoints.
2. **Redis and MongoDB storage plugins** — validating SPI pluggability against the
   compliance kit (Redis: atomic Lua sorted-set pops; MongoDB: `findAndModify`).
   See [storage backends](/rdq/reference/storage-backends/).
3. **Python and TypeScript SDKs** — extending the polyglot story beyond Java/Go and
   the REST API.
4. **Per-key ordered retry** — opt-in: hold processing of key *K* while an earlier
   task for *K* is still pending. (v1 makes no ordering guarantee by default —
   retrying out of band inherently breaks partition ordering.)
5. **Broker-native intake adapters** — e.g. a Kafka consumer-group adapter that
   dead-letters on behalf of legacy apps with zero code change.
6. **Payload encryption at rest**, **delayed/scheduled first execution**, and
   **org-level multi-tenancy** above per-queue grants.

## What rdq will not become

These are explicit non-goals, not roadmap gaps:

- **Not a message broker** — rdq never sits on the hot path of successful messages
  and does not replace Kafka/SQS.
- **Not a workflow/durable-execution engine** — one task is one function call; no
  multi-step orchestration, signals, or sagas.
- **Not exactly-once** — the contract is at-least-once; handlers must be
  idempotent.
- **Not a scheduler/cron system** — delayed first execution is at most an incidental
  post-v1 capability, not a product surface.

## See also

- [What is rdq?](/rdq/start/what-is-rdq/)
- [Storage backends & sizing](/rdq/reference/storage-backends/)
- [Server API](/rdq/reference/server-api/)
- [Architecture](/rdq/concepts/architecture/)
