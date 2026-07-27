---
title: Queue configuration & retry policies
description: Tune per-queue retry policies — attempts, backoff, jitter, error classification, TTL, and handler version handling.
---

A queue is a named unit of configuration: it owns one retry policy, one retry
queue, and one DLQ. The task envelope deliberately excludes retry behavior —
**everything about how a task is retried, leased, and classified lives in the
queue config.** rdq resolves config **at claim time**, so every field below is
live-tunable during an incident: no restarts, no redeploys; updates take effect
at the next claim.

## Where config comes from

| Mode | Source |
|---|---|
| Embedded SDK | A code builder (`rdq.Queue("payments.charge").MaxAttempts(5)…`) **or** an optional YAML file — one source per process, not merged. |
| `rdq-server` | A boot YAML file, plus the admin API (`PUT /v1/admin/queues/{queue}/config`). API-written config wins for a queue it defines. |

A top-level `defaults` block applies to every queue. Per-queue values override it
via **per-key deep-merge**: a queue that sets one field of `retry` inherits the
rest of `defaults.retry` rather than replacing the block. That per-key merge is
the only merging rdq does.

## The retry policy fields

```yaml
config_version: 1

defaults:
  retry:
    max_attempts: 5
    initial_backoff: 1s
    backoff_multiplier: 2.0   # 1.0 = linear
    max_backoff: 10m
    jitter: 0.2               # fraction of computed backoff, 0..1
  limits:
    ttl_succeeded: 24h        # retention of SUCCEEDED tasks (task_ttl)
```

| Field | Meaning |
|---|---|
| `max_attempts` | Total attempts before dead-lettering (≥ 1). |
| `initial_backoff` | Delay before the second attempt (`delay(1)`). |
| `backoff_multiplier` | Growth factor per attempt (≥ 1.0). `1.0` is linear; `2.0` doubles each time. |
| `max_backoff` | Ceiling on any single delay. |
| `jitter` | Random spread as a fraction of the computed delay, in `[0, 1]`. |
| `retryable_errors` | Error-type globs that force retry (`classification.retryable`). |
| `non_retryable_errors` | Error-type globs that skip straight to the DLQ (`classification.permanent`). |
| `task_ttl` | How long SUCCEEDED tasks are retained before purge (`limits.ttl_succeeded`). |

The engine computes each delay as:

```
delay(n) = min(initial_backoff × multiplier^(n−1), max_backoff) × (1 ± jitter·rand)
```

## Exponential vs linear

Exponential backoff (`multiplier > 1`) is the default and the right choice for a
downstream that may be overloaded — each retry backs off harder, relieving
pressure:

```yaml
queues:
  payments.charge:
    retry:
      max_attempts: 8
      initial_backoff: 2s
      backoff_multiplier: 2.0   # 2s → 4s → 8s → 16s → … capped at max_backoff
      max_backoff: 10m
```

Linear backoff (`multiplier: 1.0`) retries on a fixed cadence — useful when a
dependency recovers on a predictable schedule and you don't want delays to grow:

```yaml
queues:
  inventory.sync:
    retry:
      max_attempts: 6
      initial_backoff: 30s
      backoff_multiplier: 1.0   # 30s → 30s → 30s → …
      max_backoff: 30s
```

## Why jitter

Without jitter, a wave of tasks that fail together retry at the *same* instants
and hammer the recovering downstream in synchronized bursts — a thundering herd.
Jitter spreads retries across a window. `jitter: 0.2` scatters each delay by
±20%, so identical schedules desynchronize. Keep it non-zero (0.1–0.3 is
typical) on any queue with meaningful volume.

## Error classification

Config-level classification uses language-neutral globs on the reported
`error.type`. Anything in `retryable` is retried; anything in `permanent` skips
remaining attempts and dead-letters immediately:

```yaml
queues:
  payments.charge:
    classification:
      retryable: ["java.net.*", "TIMEOUT"]      # glob on error.type
      permanent: ["*.ValidationException"]
```

Globs are only one layer. Full precedence, most-specific first:

1. `OutcomeMapper` (authoritative when present)
2. Per-call wrappers: `rdq.Permanent(err)` / `rdq.Retryable(err)` (Go),
   `PermanentException` / `RetryableException` (Java)
3. Code classifiers: Java exception-class lists (hierarchy-aware), Go `errors.Is/As`
4. Config globs (`retryable` / `permanent`) — the only layer expressible in YAML
5. Default: a failure is retryable

Layers 1–3 exist only in the SDKs; the config globs (layer 4) are the primary
tool in server mode. See [the outcome contract](/rdq/concepts/outcome-contract/).

## Handler version mismatch

An optional handler `version` tag travels with each submitted task. When a task's
version no longer matches the currently registered handler, the queue decides
what happens:

```yaml
queues:
  payments.charge:
    handler:
      version_mismatch: dead-letter   # run-latest | dead-letter
```

- `run-latest` — execute with the current handler regardless of the task's
  submitted version. Safe when handler changes are backward-compatible.
- `dead-letter` — park the task in the DLQ instead of running a mismatched
  handler. Choose this when a version bump signals an incompatible payload shape.

A task whose `handler_ref` has no registered handler at all is always parked as
unroutable in the DLQ with a distinct error class — never dropped, never
hot-looped.

## Related tuning

Other per-queue blocks pair with the retry policy — `execution` (`lease`,
`handler_timeout`, `heartbeat`), `worker` (`batch_size`, `concurrency`,
`poll_interval`, per-instance `rate_limit`), `sync_retry` (embedded in-process
retries before durable enqueue), and `callback` (server mode). Config is
validated **strictly**: unknown keys are rejected at load/API time so typos fail
fast, not at 3am. The full schema and every field is in the
[configuration reference](/rdq/reference/configuration/).

## See also

- [Configuration reference](/rdq/reference/configuration/)
- [The outcome contract](/rdq/concepts/outcome-contract/)
- [Go SDK](/rdq/guides/go-sdk/) · [Java SDK](/rdq/guides/java-sdk/)
- [DLQ analysis & redrive](/rdq/guides/dlq-and-redrive/)
