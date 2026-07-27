---
title: The wire envelope
description: The single language-neutral JSON representation of a task, shared by both SDKs, the server API, the CLI, and every storage plugin.
---

The **envelope** is the one language-neutral representation of a task. It is the
contract shared by the Java SDK, the Go SDK, `rdq-server`'s API, the CLI, and
every storage plugin. If two components ever disagree about a task, the envelope
is the arbiter. There is exactly one wire format — Java-native serialization and
other language-specific encodings are prohibited in the core (PRD §8.6, FR-14).

## Canonical encoding

- **JSON**, UTF-8, is the canonical form and the compliance-kit fixture format.
  gRPC surfaces carry a proto mapping of the same model, but JSON is the arbiter.
- Field names are **`snake_case`**.
- Timestamps are **RFC 3339 UTC, millisecond precision**: `2026-07-20T14:03:22.117Z`.
- Durations are integer **milliseconds**.
- IDs are **ULIDs** — lexicographically sortable, generated with no coordination.
- The user **payload is opaque bytes**: base64-encoded in JSON and tagged with
  `payload_content_type`. rdq never parses the payload.

## A concrete envelope

```json
{
  "envelope_version": 1,
  "id": "01J2ZK7Q8XW5H3N9G4T6B8RDQ0",
  "queue": "payments.charge",
  "handler_ref": "charge-payment",
  "handler_version": "v3",
  "payload": "eyJvcmRlcl9pZCI6IDQyfQ==",
  "payload_content_type": "application/json",
  "headers": {
    "traceparent": "00-4bf9...-01",
    "rdq.source": "kafka://payments/3/42351",
    "rdq.submitted_by": "checkout-service"
  },
  "status": "PENDING",
  "attempt_count": 2,
  "redrive_count": 0,
  "next_attempt_at": "2026-07-20T14:05:22.117Z",
  "lease_expires_at": null,
  "created_at": "2026-07-20T14:03:22.117Z",
  "attempts": [
    {
      "attempt_no": 1,
      "started_at": "2026-07-20T14:03:22.200Z",
      "finished_at": "2026-07-20T14:03:22.950Z",
      "outcome": "RETRYABLE_FAILURE",
      "error": {
        "type": "java.net.SocketTimeoutException",
        "message": "connect timed out after 500ms",
        "detail": null,
        "stack": "java.net.SocketTimeoutException: connect timed out\n\tat ..."
      }
    }
  ]
}
```

### Field notes

| Field | Notes |
|---|---|
| `envelope_version` | Integer. Bumped only for breaking changes (see Evolution). |
| `id` | ULID, assigned at submit. Stable across retries **and redrive** — one identity for the task's whole life (tracing, audit, idempotent enqueue). |
| `queue` | Logical queue name. Charset `[a-z0-9._-]`, max 240 chars. |
| `handler_ref` | Stable handler name; same charset rule. |
| `handler_version` | Optional; mismatch behavior is queue config. |
| `payload` / `payload_content_type` | Opaque base64 bytes plus MIME type. Default max envelope size **1 MiB** (configurable per queue); larger payloads are rejected with `PAYLOAD_TOO_LARGE`. |
| `headers` | Flat string→string map. The **`rdq.` prefix is reserved** for system metadata (user headers must not use it). Well-known keys: `rdq.source` (origin locator, e.g. `kafka://topic/partition/offset`, `sqs://queue-url/msg-id`), `rdq.submitted_by`. Trace context (`traceparent`) rides here. |
| `status` | `PENDING` · `IN_FLIGHT` · `SUCCEEDED` · `DEAD`. |
| `attempt_count` | Attempts consumed so far. **Reset to 0 on redrive**; `redrive_count` increments instead, and prior attempts stay in `attempts`. |
| `redrive_count` | Number of times the task has been redriven. |
| `next_attempt_at` | When the task becomes due. Null once terminal. |
| `lease_expires_at` | Set while `IN_FLIGHT`; null otherwise. |
| `created_at` | Submit time. |
| `attempts` | Full failure history, ordered. Travels with the task into the DLQ. |

A `payload_ref` field is **reserved but unused in v1** — a future pointer to an
externally-stored payload (object-storage claim-check). It is reserved now so
adding it later stays additive; do not populate it.

## The attempt record

Each entry in `attempts` records one execution:

- `attempt_no`, `started_at`, `finished_at` (null while in flight),
- `outcome` ∈ `SUCCESS` | `RETRYABLE_FAILURE` | `PERMANENT_FAILURE` | `LEASE_EXPIRED`,
- an optional `error` object.

`LEASE_EXPIRED` is recorded when a claim's lease lapses without a reported outcome
(worker crash, handler overrun). It **counts against `max_attempts`** — poison-pill
protection so a handler that reliably kills its worker still reaches the DLQ. Its
`error` has `type = "rdq.LeaseExpired"`, a message stating the lease deadline, and
no `stack`.

The `error` object:

| Field | Notes |
|---|---|
| `type` | Language-native class/type string. Java uses the exception class name. Go, which has no class names, uses the classifier/wrapper-supplied name when present, otherwise `%T` of the innermost unwrapped error. Config classification globs match on this string. |
| `message` | Truncated at 4 KiB. |
| `detail` | Optional structured JSON supplied by an `OutcomeMapper`. |
| `stack` | Optional, truncated at 64 KiB. |

Truncation is marked with a trailing `…[truncated]`.

## What is deliberately NOT in the envelope

The **retry policy** is not in the envelope. A task references its `queue`; the
policy — `max_attempts`, backoff, error classification, lease duration — lives in
[queue configuration](/rdq/guides/queue-configuration/) and is resolved by the
engine **at claim time**, not frozen into the task at submit.

This makes policy live-tunable: an operator can raise `max_attempts` or stretch
backoff during an incident and *already-queued* tasks obey immediately. The
tradeoff — a task submitted under one policy may finish under another — is
accepted and documented. A direct consequence is that the
[storage SPI](/rdq/concepts/storage-spi/) never sees policy: plugins store and
schedule; the engine decides.

## Where the envelope appears

- **SDK ↔ storage plugin.** Plugins must round-trip the envelope losslessly. They
  may decompose it into native structures (Postgres columns, Redis hashes), but
  the compliance kit verifies `read(write(env)) == env`, including unknown-field
  preservation.
- **`rdq-server` API.** Submit accepts a client subset (`queue`, `handler_ref`,
  `payload`, `payload_content_type`, `headers`); the server assigns `id`,
  `status`, and timestamps. Reads return the full envelope.
- **CLI / exports.** `rdq dlq show <id>` prints the envelope; DLQ exports are
  JSONL of envelopes.

## Evolution rules

1. Within a version, changes are **additive only**. Consumers ignore unknown
   fields, and storage plugins must **preserve** unknown fields — store the
   residual, return it on read.
2. **Breaking changes** (rename, retype, semantic change) bump `envelope_version`.
   Engines read all versions ≤ their own and write their own.
3. New `outcome` or `status` values are breaking — readers switch on them
   exhaustively.

## See also

- [Tasks, attempts & the lifecycle](/rdq/concepts/task-lifecycle/)
- [The outcome contract](/rdq/concepts/outcome-contract/)
- [Storage SPI & compliance kit](/rdq/concepts/storage-spi/)
- [Queue configuration & retry policies](/rdq/guides/queue-configuration/)
- [Server API (REST & gRPC)](/rdq/reference/server-api/)
