---
title: "Introducing rdq: retry & dead-letter queues for any broker, any storage, any language"
description: rdq is a durable retry engine with first-class dead-letter queues that lives in the datastore you already run. Broker-agnostic, polyglot, and horizontally scalable — embed the SDK or run it as a central hub. Here's why we built it and how it works.
date: 2026-07-27
authors: srjn45
excerpt: Every event-driven system eventually asks the same question — a handler failed, now what? Today you answer it with broker-locked, language-locked, hand-rolled plumbing, and the DLQ you land in is a graveyard with no failure context and no safe replay. rdq is the missing bolt-on for that one narrow job.
tags:
  - announcement
  - dead-letter-queue
  - reliability
  - kafka
---

Every event-driven system eventually runs into the same question: **a message handler failed —
now what?**

Today, teams answer it the hard way. You hand-roll a retry topic per Kafka consumer. You wire up
broker-specific dead-letter queues — SQS redrive policies, RabbitMQ dead-letter exchanges — each
with its own quirks. Or you reach for a heavyweight durable-execution platform and take on a new
programming model and a dedicated cluster to solve one narrow problem. The result is duplicated,
broker-locked, language-locked plumbing. And when a message finally *does* land in a DLQ, the
on-call engineer opens it to find a bare payload: no error, no stack trace, no history, and no
safe way to replay it once the bug is fixed.

**rdq** is the tool that should have existed for that job. It's a durable retry engine with
first-class dead-letter queues, and its entire promise is small enough to say in one breath: hand
it a failed unit of work, and it guarantees one of exactly two outcomes.

1. The call **eventually succeeds**, retried on your configured backoff schedule, or
2. it lands in a **dead-letter queue with its full failure history** — every attempt's error,
   stack trace, and timestamp — where it can be inspected, fixed, and redriven.

That's it. rdq is not a broker, not a workflow engine, and it never sits on the hot path of your
successful messages.

## The problem is more general than messaging

The framing that unlocked the design: *a function was called with some arguments, and it failed.*
A Kafka handler, an SQS consumer, a plain `func(args)` — they're all the same shape. What you
actually need is something that durably remembers **the function reference and its arguments**,
re-invokes the call on a policy, and — if it never succeeds — parks it somewhere inspectable and
replayable.

Once you frame it that way, the broker stops mattering. rdq never talks to Kafka or SQS. It
accepts failures from *any* source and returns one of two terminal outcomes. That's what lets one
tool serve Kafka, SQS, Redpanda, RabbitMQ, AutoMQ — or no broker at all.

## Four decisions that define rdq

### 1. Bring your own storage

rdq adds **no new stateful infrastructure**. Retry queues and DLQs live in a datastore you
already operate. PostgreSQL is the v1 reference backend; Redis, MongoDB, and others plug in
through a documented [storage SPI](/rdq/concepts/storage-spi/) that ships with a public compliance
test-kit, so third parties can build and verify their own plugins.

This is the difference between "adopt a new database to get retries" and "point rdq at the
Postgres you're already running." The second one is a Tuesday afternoon.

### 2. Broker-agnostic, off the hot path

rdq never sits between your producer and consumer. It receives work only *after* something has
already failed. Your throughput on the happy path is untouched; rdq scales with your *failure*
rate, which is (hopefully) a much smaller number.

### 3. Polyglot from day one

There is one [language-neutral wire envelope](/rdq/concepts/wire-envelope/) — JSON, with the
payload as opaque bytes plus a content type — shared by both SDKs, the server API, and every
storage plugin. v1 ships a **Go SDK**, a **Java SDK**, and a **REST/gRPC API** for everything
else. A task submitted from Go can be redriven and executed through the server by a totally
different service. No language-specific serialization is allowed in the core.

### 4. The DLQ is a product, not a graveyard

This is the part most tools get wrong. In rdq, the complete failure history travels *with* the
task into the DLQ. You can browse and filter by queue, error type, handler, and time range. You
can redrive a single task or bulk-redrive by filter — *"every `payments.charge` task that failed
with `TimeoutException` after 14:00"* — with the retry policy reset. Single-task redrive can even
edit the payload first. And **every mutation is audit-logged**: who, when, what filter. The
[DLQ analysis & redrive guide](/rdq/guides/dlq-and-redrive/) walks through it.

## One core, two hosts

The engine — task model, retry policies, storage SPI, DLQ semantics, wire format — is a single
core shipped in two form factors, both in v1:

- **Embedded SDK** (Go and Java): the core runs inside your application process; handlers are
  in-process functions. Zero extra infrastructure beyond your storage.
- **Standalone service** (`rdq-server`, in Go): the same core behind REST/gRPC intake; handlers
  are remote **callbacks** (HTTP or gRPC) registered per queue. A central retry hub any language
  can use over the wire.

And they compose. Because the client and worker artifacts are split, you can **submit here and
execute there** — enqueue from a lightweight producer, let a fleet of workers (or the server)
drain the queue. A callback that times out is just another failed attempt; it flows through the
identical retry/DLQ path as an in-process error. The full picture is in
[Architecture](/rdq/concepts/architecture/).

## Correctness without a coordinator

rdq has no leader election and no cluster membership. Every process — embedded workers and
`rdq-server` nodes alike — is **stateless**, and the storage backend is the only coordination
point. Workers claim due tasks **atomically** (Postgres uses `FOR UPDATE SKIP LOCKED`), and every
claim carries a **lease**. If a worker dies mid-task — `kill -9`, a crash, a network partition —
the lease expires and any other worker reclaims the task. Nothing is lost, and no two workers ever
run the same task concurrently.

That's the whole scaling story: add a node to go faster; node death is a non-event. Your
availability inherits your storage backend's HA — Postgres replicas, Redis Sentinel, Mongo
replica sets — because rdq adds no stateful component of its own. The
[task lifecycle](/rdq/concepts/task-lifecycle/) has the details.

## The honest limits

rdq is deliberately narrow, and the non-goals are part of the design:

- **At-least-once, not exactly-once.** Your handlers must be idempotent. We say this loudly and
  often because it's the one contract you can't ignore.
- **Not a workflow engine.** One task equals one function call. No sagas, no signals, no
  multi-step orchestration.
- **No ordering guarantee by default.** Retrying out-of-band inherently breaks partition
  ordering; per-key ordered retry is a post-v1 opt-in.

If you need durable multi-step orchestration, you want Temporal. If you need retries and a DLQ you
can actually operate, you want rdq.

## Where it's going

v1 is the core engine, the Go and Java SDKs, `rdq-server`, the PostgreSQL plugin, the `rdq` CLI,
Prometheus metrics, and DLQ browse/redrive/audit. On the [roadmap](/rdq/reference/roadmap/): a web
UI for DLQ analysis, Redis and MongoDB plugins, Python and TypeScript SDKs, per-key ordered retry,
and broker-native intake adapters that dead-letter on behalf of legacy apps with zero code change.

## Try it

The fastest path is the [Quickstart](/rdq/start/quickstart/): submit a task, run a worker, watch
it retry and then dead-letter, then redrive it after a fix. If you just want the mental model
first, start with [What is rdq?](/rdq/start/what-is-rdq/).

rdq is open source under Apache-2.0. The code — and the design docs behind every decision above —
live on [GitHub](https://github.com/srjn45/rdq).

---

*rdq — Retry & Dead-letter Queues for any broker, any storage, any language.*
