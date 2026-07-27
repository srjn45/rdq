---
title: What is rdq?
description: rdq is a durable retry engine with first-class dead-letter queues — for any broker, any storage, any language. Bring your own datastore; rdq adds no new infra.
---

**rdq — Retry & Dead-letter Queues for any broker, any storage, any language.**

A function call failed — a Kafka handler, an SQS consumer, any `func(args)`. Hand rdq the
handler's name and its payload, and rdq guarantees one of exactly two outcomes:

1. the call **eventually succeeds** — retried on your configured backoff schedule, or
2. it lands in a **dead-letter queue with its full failure history** — every attempt's error,
   stack trace, and timestamp — where it can be inspected, fixed, and redriven.

That's the whole job. rdq is not a broker, not a workflow engine, and not on the hot path of
successful messages. It durably remembers a failed unit of work and sees it through to a
terminal outcome.

## The problem it solves

Every event-driven system eventually hits the same question: *a message handler failed — now
what?* Today teams answer it by hand-rolling retry topics per Kafka consumer, wiring
broker-specific DLQs (SQS redrive, RabbitMQ dead-letter exchanges), or adopting a heavyweight
durable-execution platform. The result is duplicated, broker-locked, language-locked plumbing —
and when a message *does* land in a DLQ, the on-call engineer gets a bare payload with no
failure context and no safe way to replay it.

The general shape of the problem is broader than messaging: **a function was called with some
arguments and it failed.** What you need is something that durably remembers the function
reference and its arguments, retries the call on a policy, and — if it never succeeds — parks it
somewhere inspectable and replayable. That is exactly what rdq is.

## What makes rdq different

- **🗄️ Bring your own storage.** Retry queues and DLQs live in the datastore you already
  operate — PostgreSQL in v1; Redis, MongoDB, and others via a documented
  [storage SPI](/rdq/concepts/storage-spi/). rdq introduces no new stateful infrastructure.
- **🔌 Broker-agnostic.** Kafka, SQS, Redpanda, AutoMQ, RabbitMQ — or no broker at all. rdq
  never talks to your broker; it accepts failures from *any* source and never sits on the happy
  path of successful messages.
- **🌐 Polyglot by design.** A [language-neutral wire envelope](/rdq/concepts/wire-envelope/)
  from day one. v1 ships a Go SDK, a Java SDK, and a REST/gRPC API for everything else. A task
  submitted from Go can be redriven and executed from any host.
- **⚰️ DLQ as a product, not a graveyard.** The complete failure history travels with each task:
  browse and filter by queue, error type, handler, and time — then single- or bulk-redrive after
  a fix ships. Every mutation is audit-logged.
- **📈 Horizontally scalable & fault tolerant.** Stateless workers coordinate entirely through
  the storage backend using [atomic claims and leases](/rdq/concepts/task-lifecycle/). Add a node
  to scale; a `kill -9` mid-task is a non-event — the lease expires and another worker reclaims it.

## One core, two hosts

The engine — task model, retry policies, storage SPI, DLQ semantics, and wire format — is a
single core, shipped in **two form factors**, both in v1:

- **Embedded SDK** (Go and Java): hosts the core inside your application process. Handlers are
  in-process functions. Zero additional infrastructure — the only dependency is the storage
  backend you already run.
- **Standalone service** (`rdq-server`, written in Go): hosts the same core behind REST/gRPC
  intake APIs. Handlers are remote **callbacks** (HTTP or gRPC) registered per queue. It acts as
  a central retry hub for an organization — any language integrates over the API, no SDK required.

A callback that times out or errors is simply another failed attempt; it flows through the
identical retry/DLQ path as an in-process handler failure. See
[Architecture](/rdq/concepts/architecture/) for the full picture.

## What rdq is *not*

- **Not a message broker.** It does not replace Kafka or SQS and never sits on the hot path of
  successful messages.
- **Not a workflow / durable-execution engine.** No multi-step orchestration, signals, or sagas.
  One task = one function call. (That's Temporal's territory.)
- **Not exactly-once.** rdq promises **at-least-once** execution — your handlers must be
  idempotent. See [the task lifecycle](/rdq/concepts/task-lifecycle/).
- **Not a scheduler / cron.** Delayed first execution is incidental, not a product surface.

## Where rdq fits in the landscape

| Alternative | Gap rdq fills |
|---|---|
| Temporal / Cadence | Heavyweight: a new programming model plus a dedicated cluster. rdq is a bolt-on for one narrow job. |
| spring-kafka retry topics / Uber DLQ pattern | Kafka-only, JVM-only; retry state lives in extra topics with poor inspection and redrive UX. |
| SQS / RabbitMQ native DLQs | Broker-locked, no backoff-policy orchestration, no failure context attached, weak redrive tooling. |
| Sidekiq / Hangfire / Celery / RQ | Language- and storage-locked job queues; retry is a feature, DLQ analysis is an afterthought. |

The wedge is the intersection no established tool occupies: **bring-your-own-storage +
broker-agnostic + polyglot + first-class DLQ analysis and redrive.**

## Next steps

- [Install](/rdq/start/install/) — SDKs, the server image, and the CLI.
- [Quickstart](/rdq/start/quickstart/) — submit a task and run a worker in under 30 minutes.
- [Architecture](/rdq/concepts/architecture/) — one core, two hosts, storage as the coordinator.

> 🚧 rdq (formerly `kafka-retry-dlq`) is being rebuilt from the ground up. These docs describe
> the v1 target design; APIs are not yet stable.
