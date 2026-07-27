---
title: Architecture — one core, two hosts
description: How rdq ships one shared retry engine as an embedded SDK and as rdq-server, coordinated entirely through your storage backend.
---

rdq is one engine with two ways to run it. The engine — the task model, retry
policies, storage SPI, DLQ semantics, and wire format — is a single, shared core.
Everything else is a thin **host** that embeds that core and feeds it work.

## The core

The core owns every *decision* in the system:

- the **task model** and its status machine ([Tasks & the lifecycle](/rdq/concepts/task-lifecycle/)),
- **retry policies** — backoff, jitter, `max_attempts`, error classification,
- the **outcome contract** that turns a handler result into `SUCCESS`,
  `RETRYABLE_FAILURE`, or `PERMANENT_FAILURE` ([Outcome contract](/rdq/concepts/outcome-contract/)),
- **DLQ semantics** — full failure history, browse/search, redrive,
- the language-neutral **wire envelope** ([Wire envelope](/rdq/concepts/wire-envelope/)),
- the **storage SPI** every backend implements ([Storage SPI](/rdq/concepts/storage-spi/)).

The core is written in Go (`core/`) and mirrored as a frozen contract in the Java
worker. It never talks to a broker and never touches durability directly — it
delegates all persistence, atomicity, and scheduling to a storage plugin.

## Two hosts

Both host form factors ship in v1 and run the *same* core semantics.

**Embedded SDK (Go and Java).** The core runs inside your application process.
Handlers are ordinary in-process functions registered under stable names. The
only dependency is the storage backend you already operate — there is no rdq
service to deploy. This is the fastest path: wrap a failing consumer, register a
handler, run a worker. See the [Go SDK](/rdq/guides/go-sdk/) and
[Java SDK](/rdq/guides/java-sdk/) guides.

**Standalone service (`rdq-server`, Go).** The same core runs behind REST and
gRPC intake APIs. Handlers are remote **callbacks** — an HTTP or gRPC endpoint
registered per queue — so any language integrates without an SDK. `rdq-server`
is a central retry hub for an organization. See [Running rdq-server](/rdq/guides/rdq-server/).

A callback that times out or returns an error is simply another failed attempt.
It flows through the identical retry and DLQ path as an in-process handler
failure — there is one engine, not two.

## Storage as the coordinator

rdq has **no control plane of its own**. Every engine process — an embedded
worker or an `rdq-server` node — is stateless. All coordination is delegated to
the storage backend through two primitives:

- **Atomic claims.** A worker claims due tasks with a single atomic storage
  operation. The backend guarantees that *no two workers ever claim the same
  task* (Postgres `FOR UPDATE SKIP LOCKED`, a Redis Lua pop, Mongo
  `findAndModify`). See [Storage SPI](/rdq/concepts/storage-spi/).
- **Leases.** Every claim carries a visibility-timeout lease and a fencing token.
  If a worker dies mid-handler, its lease expires and the task becomes reclaimable
  by any other worker — the old token is dead, so the crashed worker can no longer
  mutate the task.

Consequences of this design:

- **No leader election, no cluster membership, no gossip.** Nodes do not know
  about each other. The storage backend is the single point of truth and the
  single point of coordination.
- **Horizontal scaling is "add a node."** N app instances or N `rdq-server`
  nodes safely share one backend. Throughput scales with claim capacity.
- **Node death is a non-event.** A killed worker's in-flight tasks are reclaimed
  after lease expiry. Nothing is lost; at worst a task is retried
  (at-least-once).
- **rdq inherits its HA from storage.** Postgres replicas, Redis Sentinel, Mongo
  replica sets — rdq adds no stateful component of its own.

The storage backend also **owns the clock**: it decides when a task is due and
when a lease has expired, so many engine instances with drifting clocks stay
consistent.

## rdq is not on the happy path

rdq is **not a broker** and **not a workflow engine**. It never replaces Kafka or
SQS, and it never sits on the hot path of *successful* messages. You call rdq only
when a unit of work has already failed (or when you want it retried durably). One
task equals one function call — there is no orchestration, no sagas, no signals.

This is what makes adoption cheap: rdq is a bolt-on for one narrow job, not a new
runtime you migrate onto.

## The flow

A single task travels a fixed path from submission to a terminal outcome:

```
  producer
     │  submit(queue, handler_ref, payload, headers?)
     ▼
  ┌─────────────────────────────────────────────┐
  │                 STORAGE                       │
  │  (Postgres / Redis / Mongo — you operate it)  │
  │   retry queue  ·  DLQ  ·  attempt history     │
  └─────────────────────────────────────────────┘
     ▲            │  claimDue (atomic, leased, fenced)
     │            ▼
     │        worker / rdq-server node   (stateless, N of them)
     │            │  invoke handler (in-process) or callback (HTTP/gRPC)
     │            ▼
     │        outcome: SUCCESS | RETRYABLE_FAILURE | PERMANENT_FAILURE
     │            │
     │   ┌────────┼─────────────────────────┐
     │   │        │                          │
     │  SUCCESS  RETRYABLE                 PERMANENT / exhausted
     │   │     (reschedule w/ backoff)       │
     │  mark        │                     move to DLQ (+ full history)
     │ SUCCEEDED    └──► back to storage      │
     │  (ttl)                                 ▼
     └───────────────────────── redrive ── DLQ analyst
                              (single or bulk, after a fix ships)
```

The producer submits; storage durably holds the task; a stateless worker
atomically claims it under a lease; the handler or callback runs; the outcome is
recorded as an attempt. A retryable failure is rescheduled with backoff; success
marks the task `SUCCEEDED` (retained per `task_ttl`, then purged); a permanent
failure or exhausted attempt budget moves the task to the DLQ with its complete
failure history, where an operator can browse it and **redrive** it back onto the
retry queue after shipping a fix.

## See also

- [Tasks, attempts & the lifecycle](/rdq/concepts/task-lifecycle/)
- [The outcome contract](/rdq/concepts/outcome-contract/)
- [The wire envelope](/rdq/concepts/wire-envelope/)
- [Storage SPI & compliance kit](/rdq/concepts/storage-spi/)
- [What is rdq?](/rdq/start/what-is-rdq/)
