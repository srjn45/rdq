# rdq design 01 — Wire envelope

Status: Draft v1 · Companion: [02 — Storage SPI](02-storage-spi.md) · PRD: [§8.6 FR-14](../PRD.md)

The envelope is the single language-neutral representation of a task. It is the contract
shared by the Java SDK, the Go SDK, `rdq-server`'s API, the CLI, and every storage plugin.
If two components disagree about a task, the envelope is the arbiter.

## 1. Canonical encoding

- **JSON**, UTF-8. gRPC surfaces carry a proto mapping of the same model; JSON remains the
  canonical form and the compliance-kit fixture format.
- Field names: `snake_case`.
- Timestamps: RFC 3339 UTC with millisecond precision (`2026-07-20T14:03:22.117Z`).
- Durations: integer milliseconds.
- IDs: **ULID** (lexicographically sortable, no coordination needed to generate).
- The user payload is **opaque bytes**: base64 in JSON, tagged with `payload_content_type`.
  rdq never parses payloads.

## 2. Task envelope

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
| `envelope_version` | Integer. Bumped only for breaking changes (see §5). |
| `id` | ULID, assigned at submit. Stable across retries **and across redrive** — the task keeps one identity for its whole life (tracing, audit, idempotent enqueue). |
| `queue` | Logical queue name. `[a-z0-9._-]`, max 240 chars. |
| `handler_ref` | Stable handler name (PRD §8.5). Same charset rule. |
| `handler_version` | Optional; mismatch behavior is queue config. |
| `payload` / `payload_content_type` | Opaque bytes + MIME type. Default max envelope size **1 MiB** (configurable per queue); larger payloads are rejected with `PAYLOAD_TOO_LARGE`. The object-storage claim-check is deferred post-v1 (OQ-2 resolved v1 — see design 05 §0). |
| `payload_ref` | **Reserved, unused in v1.** Optional future pointer to an externally-stored payload (object-storage claim-check). Reserved now so that adding it later stays additive (OQ-2 resolved v1 — deferred to post-v1, see design 05 §0). |
| `headers` | Flat string→string map. The **`rdq.` prefix is reserved** for system metadata; user headers must not use it. Well-known: `rdq.source` (origin locator, e.g. `kafka://topic/partition/offset`, `sqs://queue-url/msg-id`), `rdq.submitted_by`. Trace context (`traceparent`) rides here. |
| `status` | `PENDING` \| `IN_FLIGHT` \| `SUCCEEDED` \| `DEAD`. |
| `attempt_count` | Attempts consumed so far. **Reset to 0 on redrive**; `redrive_count` increments instead, and prior attempts stay in `attempts`. |
| `next_attempt_at` | When the task becomes due. Null once terminal. |
| `lease_expires_at` | Set while `IN_FLIGHT`; null otherwise. |
| `attempts` | Full failure history, ordered. Travels with the task into the DLQ (PRD FR-15). |

### Attempt record

`outcome` ∈ `SUCCESS` | `RETRYABLE_FAILURE` | `PERMANENT_FAILURE` | `LEASE_EXPIRED`.

`LEASE_EXPIRED` is recorded when a claim's lease lapses without a reported outcome (worker
crash, handler overrun). It **counts against `max_attempts`** — otherwise a handler that
reliably crashes its worker would retry forever and never reach the DLQ (poison-pill
protection). Its `error` object has `type = "rdq.LeaseExpired"`, a `message` stating the lease
deadline, and no `stack` (G7, resolved v1 — see design 05 §0.1); this shape is frozen in the
M1 compliance fixtures.

`error` object: `type` (language-native class/type string), `message` (truncated at 4 KiB),
`detail` (optional structured JSON from the `OutcomeMapper`), `stack` (optional, truncated at
64 KiB). Truncation is marked with a trailing `…[truncated]`.

**`error.type` across languages.** Java (and other class-based runtimes) uses the native
exception class name. Go, which has no class names, follows this convention (G6, resolved v1 —
see design 05 §0.1): the wrapper/classifier-supplied name wins when present, otherwise
`fmt.Sprintf("%T", err)` of the innermost unwrapped error. Config classification globs (design
03 §4) and the M1 fixtures match on this string, so the convention is frozen with the fixtures.

## 3. What is deliberately NOT in the envelope

**Retry policy.** The task references its `queue`; policy (`max_attempts`, backoff, error
classification, lease duration) lives in **queue configuration** and is resolved by the
engine **at claim time**, not frozen into the task at submit.

Rationale: this makes policy live-tunable — an operator can raise `max_attempts` or stretch
backoff during an incident and *already-queued* tasks obey immediately. The tradeoff
(a task submitted under one policy may finish under another) is acceptable and documented;
freezing policy per task would turn every incident response into a redrive exercise.

Consequence: the storage SPI never sees policy. Plugins store and schedule; the engine decides.

## 4. Where the envelope appears

- **SDK ↔ storage plugin** boundary: plugins must round-trip the envelope losslessly. They
  MAY decompose it into native structures (Postgres columns, Redis hashes) — the compliance
  kit verifies `read(write(env)) == env`, including unknown-field preservation (§5).
- **`rdq-server` API**: submit accepts a client subset (`queue`, `handler_ref`, `payload`,
  `payload_content_type`, `headers`); server assigns `id`, `status`, timestamps. Reads return
  the full envelope.
- **CLI / exports**: `rdq dlq show <id>` prints the envelope; DLQ exports are JSONL of envelopes.

## 5. Evolution rules

1. Within a version, changes are **additive only**; consumers ignore unknown fields, and
   storage plugins must **preserve** unknown fields (store the residual, return it on read).
2. Breaking changes (rename/retype/semantic change) bump `envelope_version`. Engines read
   all versions ≤ their own and write their own.
3. New `outcome`/`status` values are breaking (readers switch on them exhaustively).
