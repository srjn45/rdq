# rdq design 03 — Queue configuration schema

Status: Draft v1 · Companions: [01 — Wire envelope](01-wire-envelope.md), [02 — Storage SPI](02-storage-spi.md)

Queue config is the third contract: **everything the envelope deliberately excludes lives
here.** A task names its queue; the queue's config decides how it is retried, leased,
classified, and (in server mode) called back. Config is resolved by the engine **at claim
time** (envelope §3), which makes every field below live-tunable during an incident.

## 1. Sources

| Mode | Primary source | Notes |
|---|---|---|
| Embedded SDK | **Code builder** (`rdq.Queue("payments.charge").MaxAttempts(5)...`) | Config lives with the handler code that owns it. |
| Embedded SDK | Optional **YAML file** | For teams that prefer config-as-files; same schema. One source per process — code or file, not merged. |
| `rdq-server` | **YAML file** at boot + **admin API** (create/update per queue) | API-written configs persist via a small `ConfigStore` interface (CRUD + watch), separate from the task `Storage` SPI. File and API configs share the schema; API wins for a queue it defines. |

A top-level `defaults` block applies to every queue; per-queue values override it. That is
the only merging rdq does.

Updates take effect at the **next claim** — no restarts, no redeploys (this is the payoff
of claim-time resolution). Embedded file-watch reload is optional; the admin API path is
always hot.

## 2. Schema

YAML for humans; the identical structure as JSON on the admin API. Durations accept human
units (`500ms`, `1s`, `10m`); the API/wire form is integer milliseconds. Sizes accept
`KiB`/`MiB`.

```yaml
config_version: 1

defaults:                    # applies to all queues; any per-queue key overrides
  retry:
    max_attempts: 5
    initial_backoff: 1s
    backoff_multiplier: 2.0  # 1.0 = linear
    max_backoff: 10m
    jitter: 0.2              # fraction of computed backoff, 0..1
  execution:
    lease: 60s               # visibility timeout (SPI claim lease)
    handler_timeout: 45s     # must be <= lease; engine aborts/abandons past this
    heartbeat: false         # true => engine extends lease while handler runs (long jobs)
  limits:
    max_payload_size: 1MiB
    ttl_succeeded: 24h       # retention of SUCCEEDED tasks (task_ttl)
  worker:                    # embedded/server worker tuning, per queue
    batch_size: 32           # ClaimDue limit
    poll_interval: 500ms     # floor when Notify capability is absent
    concurrency: 8           # parallel handler invocations per instance

queues:
  payments.charge:
    retry:
      max_attempts: 8
      initial_backoff: 2s
    classification:          # matched against attempt error.type (language-neutral)
      retryable: ["java.net.*", "TIMEOUT"]        # glob on error.type
      permanent: ["*.ValidationException"]
      # precedence: code-level classifiers (Java class lists, Go errors.Is/As,
      # OutcomeMapper) > these globs > default(retryable). See §4.
    handler:
      version_mismatch: dead-letter   # run-latest | dead-letter (PRD FR-12)
    sync_retry:              # embedded SDK only; in-process retries BEFORE durable enqueue
      attempts: 2
      backoff: 100ms
    callback:                # server mode only; ignored by embedded SDK
      protocol: http         # http | grpc
      url: https://payments.internal/rdq/charge
      timeout: 30s           # must be <= handler_timeout
      auth:
        type: bearer          # none | bearer | header
        secret_ref: env:PAYMENTS_CB_TOKEN   # indirection only — raw secrets never in config
      response_mapping:       # overrides FR-29 defaults per status/code
        retryable_status: [408, 429, "5xx"]
        permanent_status: ["4xx"]
        # body_mapper: post-v1 (inspect JSON field for 200-with-error APIs)
```

## 3. Field rules and validation

Config is validated **strictly**: unknown keys are rejected (unlike the envelope, which
preserves unknown fields — config typos must fail fast, at load/API time, not at 3am).

- `max_attempts` ≥ 1 · `backoff_multiplier` ≥ 1.0 · `jitter` ∈ [0, 1]
- `handler_timeout` ≤ `lease`; `callback.timeout` ≤ `handler_timeout`
- Queue names: `[a-z0-9._-]`, ≤ 240 chars (envelope §2); a task for an unconfigured queue
  is rejected at submit (embedded) / 404 (server) — never silently defaulted
- `secret_ref` schemes in v1: `env:` (process env var). Vault/SM integrations post-v1.
- Callback URLs are checked against the server's global **callback allowlist**
  (server config, not queue config — a queue author must not be able to widen it; SSRF
  mitigation, PRD FR-25/§12)
- `config_version` bumps on breaking schema changes; loaders reject newer-than-known

Backoff formula (engine-side, for reference):
`delay(n) = min(initial_backoff × multiplier^(n−1), max_backoff) × (1 ± jitter·rand)`.

## 4. Classification precedence

Language-neutral globs in config cannot express `errors.Is` chains or class hierarchies,
so classification is layered — most-specific wins:

1. `OutcomeMapper` (authoritative when present, FR-28)
2. Per-call wrappers: `rdq.Permanent(err)` / `rdq.Retryable(err)`
3. Code-level classifiers: Java exception-class lists (hierarchy-aware), Go `errors.Is/As` targets
4. Config globs (`classification.retryable` / `.permanent`) matched against reported `error.type`
5. Default: failure is retryable

Layers 1–3 are code and only exist in SDKs; layer 4 is the only one expressible in YAML
and the primary tool in server mode (alongside `response_mapping`).

## 5. What is global (server config), NOT per queue

`rdq-server` has its own config file — storage backend DSN, API listen/auth, callback
allowlist, metrics endpoint, global defaults block. Rule of thumb: **per-queue config is
what a queue's owning team may set; server config is what the platform operator sets.**
The admin API enforces that boundary (per-queue authz, PRD FR-25).

## 6. Open items

- **OI-1:** per-queue rate limiting of handler/callback invocations (protect a struggling
  downstream during mass redrive). Leaning: yes, `worker.rate_limit` post-v1 — bulk redrive
  after an outage is exactly when you'd thundering-herd the service you just fixed.
- **OI-2:** config change audit — server mode should record who changed a queue's config
  (same audit sink as redrive/purge, FR-18). Leaning: yes in v1 for API-sourced changes.
- **OI-3:** does `defaults` merging go one level deep (block replace) or per-key deep-merge?
  Leaning: per-key deep-merge, documented with examples — block replace causes surprising
  resets when a queue overrides one field of `retry`.
