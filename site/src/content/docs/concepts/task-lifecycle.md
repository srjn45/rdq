---
title: Tasks, attempts & the lifecycle
description: The Task data model, its status machine, attempt history, leases and reclaim, the handler registry, versioning, and the at-least-once contract.
---

A **task** is one unit of failed work: a handler reference, a payload, and the
metadata rdq needs to retry it durably. This page covers the task's fields, the
status machine it moves through, the attempt records that form its failure
history, and the leasing that makes execution fault-tolerant.

## The Task

A task is submitted as `(handler_ref, payload, headers?)` against a **queue** — a
named unit of configuration (`payments.charge`) that owns one retry policy, one
retry queue, and one DLQ. The stored task carries:

| Field | Meaning |
|---|---|
| `id` | ULID, assigned at submit. Stable across retries **and redrive** — one identity for the task's whole life. |
| `queue` | Logical queue name; resolves the retry policy at claim time. |
| `handler_ref` | Stable handler name — the contract between the stored task and the code that runs it. |
| `handler_version` | Optional version pin (see below). |
| `payload` / `payload_content_type` | Opaque bytes plus MIME type. rdq never parses the payload. |
| `headers` | Flat string map: trace context (`traceparent`), source metadata (`rdq.source`, e.g. `kafka://topic/partition/offset`). |
| `status` | `PENDING · IN_FLIGHT · SUCCEEDED · DEAD`. |
| `attempt_count` | Attempts consumed so far. Reset to 0 on redrive. |
| `redrive_count` | Increments each redrive; prior attempts are preserved. |
| `next_attempt_at` | When the task becomes due. Null once terminal. |
| `lease_expires_at` | Set while `IN_FLIGHT`; null otherwise. |
| `created_at` | Submit time. |

Note that the **retry policy is not stored on the task**. `max_attempts`,
backoff, lease duration, and error classification live in
[queue configuration](/rdq/guides/queue-configuration/) and are resolved by the
engine at claim time — so an operator can retune a queue mid-incident and
already-queued tasks obey immediately.

## The status machine

```
                submit
                  │
                  ▼
   ┌──────────► PENDING ◄───────────────┐
   │              │  claimDue            │ reschedule
   │              ▼  (lease + token)     │ (backoff)
   │          IN_FLIGHT ─────────────────┘
   │            │  │  │
   │            │  │  └── lease expires ──► reclaimable (LEASE_EXPIRED attempt)
   │            │  │
   │      success│  │permanent / attempts exhausted
   │            ▼  ▼
   │        SUCCEEDED   DEAD ──► DLQ (full history)
   │        (task_ttl     │
   │         then purge)  └── redrive ──┐
   └─────────────────────────────────────┘
```

- **PENDING** — durably stored, due at `next_attempt_at`, waiting to be claimed.
- **IN_FLIGHT** — claimed by a worker under a lease; a handler is running.
- **SUCCEEDED** — the handler succeeded. Retained for observability until
  `task_ttl` elapses, then purged.
- **DEAD** — attempts exhausted or a permanent failure occurred. The task moves
  to the DLQ carrying its complete attempt history, where it can be inspected and
  redriven.

## Attempts — the failure history

Every execution appends an **attempt record** to the task:

```json
{
  "attempt_no": 1,
  "started_at": "2026-07-20T14:03:22.200Z",
  "finished_at": "2026-07-20T14:03:22.950Z",
  "outcome": "RETRYABLE_FAILURE",
  "error": {
    "type": "java.net.SocketTimeoutException",
    "message": "connect timed out after 500ms",
    "stack": "java.net.SocketTimeoutException: connect timed out\n\tat ..."
  }
}
```

An attempt's `outcome` is one of `SUCCESS`, `RETRYABLE_FAILURE`,
`PERMANENT_FAILURE`, or `LEASE_EXPIRED`. The ordered list of attempts **is** the
task's failure history: every error type, message, stack, and timestamp. It
travels with the task into the DLQ so an on-call engineer sees exactly why each
attempt failed. See the [outcome contract](/rdq/concepts/outcome-contract/) for
how a handler result becomes an outcome, and the
[wire envelope](/rdq/concepts/wire-envelope/) for the record's exact shape.

## Leases, visibility timeout & reclaim

When a worker claims a task it becomes `IN_FLIGHT` with a **lease** —
`lease_expires_at = now + lease` — and a **fencing token**. The lease is a
visibility timeout: while it holds, no other worker sees the task as claimable.

- The handler must finish (or heartbeat) before the lease expires. Long-running
  handlers extend the lease via a heartbeat; a heartbeat that finds the lease
  lost tells the handler to abandon its work.
- If a worker crashes or overruns, the lease lapses. The task becomes reclaimable
  by *any* worker, and the reclaim atomically appends a `LEASE_EXPIRED` attempt
  record.
- **`LEASE_EXPIRED` counts against `max_attempts`.** Otherwise a handler that
  reliably crashes its worker would retry forever and never reach the DLQ — this
  is rdq's poison-pill protection.

The fencing token means a resurrected zombie worker can never corrupt state: its
token is stale, and every post-claim mutation with a stale token is rejected.
This is what makes node death a non-event ([Architecture](/rdq/concepts/architecture/)).

## The handler registry & stable references

Handlers register under **explicit, stable string names** — never serialized
closures, so names survive deploys, restarts, and language boundaries:

```go
reg.Register("charge-payment", chargePaymentHandler)
```

```java
registry.register("charge-payment", new ChargePaymentHandler());
```

The `handler_ref` on a stored task is the contract between the durable task and
the code that executes it. Registration is **one-shot per name**: registering the
same ref twice is an error, so the effective handler never depends on init
ordering.

## Handler versioning

A handler may declare a `version` (e.g. `"v3"`), and a task carries the version
it was submitted under. When a claimed task's pinned `handler_version` differs
from the registered handler's version, the queue's `version_mismatch` policy
decides:

- **`run-latest`** (default) — ignore the pin and run the registered handler.
- **`dead-letter`** — refuse to run possibly-wrong code and route the task to the
  DLQ with error type `rdq.HandlerVersionMismatch`.

An unpinned task, or one whose pin matches, always runs. Version comparison is
equality, not ordering — the value is opaque.

## Unroutable tasks are parked, never hot-looped

If a claimed task's `handler_ref` has **no registered handler** on the worker
fleet, no amount of retrying will make one appear. Rescheduling would make the
task due again immediately and spin a tight re-claim/re-fail loop. So rdq
**dead-letters it immediately** with the distinct error type `rdq.Unroutable` —
never rescheduled, never dropped, never hot-looped. The distinct error class
(separate from `rdq.HandlerVersionMismatch`) lets an operator tell "we deployed a
fleet missing this handler" apart from "this task is version-pinned to code we
don't run" at 3am.

## At-least-once & the idempotency requirement

rdq guarantees **at-least-once** execution, not exactly-once. A crash after a
handler's side effect but before the outcome is recorded, or a lease that expires
while a handler is still running, both result in the task being retried. That is
the correct, safe default — nothing is ever lost — but it means **your handlers
must be idempotent**. Processing the same task twice must be harmless: dedupe on
the task `id`, use conditional writes, or make the side effect naturally
idempotent. This is a first-class contract, not a footnote.

## See also

- [The outcome contract](/rdq/concepts/outcome-contract/)
- [The wire envelope](/rdq/concepts/wire-envelope/)
- [Architecture — one core, two hosts](/rdq/concepts/architecture/)
- [Queue configuration & retry policies](/rdq/guides/queue-configuration/)
- [DLQ analysis & redrive](/rdq/guides/dlq-and-redrive/)
