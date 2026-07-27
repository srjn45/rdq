---
title: Configuration
description: Environment variables and server config for rdq-server, plus the per-queue policy schema — retry, execution, limits, worker, classification, and callbacks.
---

rdq configuration comes in three layers: **process environment** (how `rdq-server`
boots and reaches storage), **server config** (platform-operator settings a queue
owner can't touch — the auth token file and callback allowlist), and **per-queue
policy** (how each queue is retried, leased, classified, and called back). The
per-queue schema is shared by both SDKs and the server, seeded by a boot YAML file
and live-tunable through the [admin API](/rdq/reference/server-api/#admin--callback-registration).

## Process environment (`rdq-server`)

These are read directly by the `rdq-server` binary at startup.

| Name | Type | Default | Description |
|---|---|---|---|
| `RDQ_DSN` | string | *(required)* | PostgreSQL DSN, e.g. `postgres://user:pass@host/db`. The server refuses to start without it. |
| `RDQ_ADDR` | string | `:8080` | TCP listen address for the REST API and health probes. |

On start, `rdq-server` applies schema migrations and refuses to run against a
schema version it does not understand. Secrets referenced by callback auth are
resolved from the environment via the `env:` scheme — e.g. a queue's
`secret_ref: env:PAYMENTS_CB_TOKEN` dereferences `$PAYMENTS_CB_TOKEN` at config
load (a missing/empty variable fails fast, so a callback is never dispatched with a
blank credential).

### Design-level defaults (not yet wired)

The following are documented design surfaces; the values are the intended defaults,
presented here as design — not all are exposed as flags/vars in the current build.

| Name | Type | Default | Description |
|---|---|---|---|
| gRPC listen address | string | `:9090` *(design)* | gRPC intake port. gRPC intake is post-v1; v1 ships REST only. |
| TLS cert / key | path | *(none)* | TLS for intake. In v1, terminate TLS at a proxy/load balancer; native TLS is a documented design surface. |
| Graceful-drain timeout | duration | `15s` | On `SIGTERM` the server stops claiming, finishes in-flight work within its lease, then exits. |

## Server config (operator-owned)

Loaded from the server's config source (design 03 §5), these settings are outside
any queue owner's reach — the SSRF and authn boundary.

| Key | Type | Default | Description |
|---|---|---|---|
| `tokens_path` | string | *(empty)* | Path to the static bearer-token file mapping token → principal + per-queue×role grants. Empty leaves the `/v1` auth boundary open (dev/embedded mode only). |
| `callback_allowlist` | list of strings | *(empty = deny-all)* | Permitted callback base URLs (`scheme://host[:port][/prefix]` or bare `host[:port]`). A queue's callback URL is delivered only if it matches. Empty means **no** callback URL is permitted until an operator opts a target in. Host match is exact, case-insensitive, with no subdomain wildcard. |

## Per-queue policy schema

The queue-config document is strict: unknown keys are rejected at load or on an
admin write — a typo fails fast at boot, never at 3am. A top-level `defaults` block
applies to every queue; each queue overrides it by per-key deep-merge. In YAML,
durations carry units (`500ms`, `1s`, `10m`, `24h`), sizes carry binary units
(`KiB`, `MiB`, `GiB`), and rates read as `count/period` (`100/s`). The admin API
speaks the same schema as JSON, where durations are integer milliseconds and sizes
integer bytes.

> The values in the tables below are the illustrative defaults from the shipped
> example `defaults` block. rdq validates bounds (shown as rules) but does not
> hard-code these numbers — the effective value is whatever your `defaults`/queue
> config sets.

### `retry` — the backoff ladder

`delay(n) = min(initial_backoff × multiplier^(n-1), max_backoff) × (1 ± jitter)`.

| Name | Type | Example | Description / rule |
|---|---|---|---|
| `max_attempts` | int | `5` | Attempts before dead-lettering. Must be ≥ 1. |
| `initial_backoff` | duration | `1s` | First retry delay. Must be > 0. |
| `backoff_multiplier` | float | `2.0` | Growth factor (1.0 = linear). Must be ≥ 1.0. |
| `max_backoff` | duration | `10m` | Delay ceiling. Must be > 0. |
| `jitter` | float | `0.2` | Randomization fraction in `[0, 1]`. |

### `execution` — leasing & timeouts

| Name | Type | Example | Description / rule |
|---|---|---|---|
| `lease` | duration | `60s` | Visibility timeout on a claim; an expired lease makes the task reclaimable. Must be > 0. |
| `handler_timeout` | duration | `45s` | Max handler runtime. Must be > 0 and **≤ `lease`**. |
| `heartbeat` | bool | `false` | Extend the lease for long handlers rather than letting it expire. |

### `limits` — payload & retention

| Name | Type | Example | Description / rule |
|---|---|---|---|
| `max_payload_size` | size | `1MiB` | Per-queue payload cap; oversized submits get `413 PAYLOAD_TOO_LARGE`. Must be > 0. |
| `ttl_succeeded` | duration | `24h` | How long succeeded tasks are retained before purge. Must be ≥ 0. |

### `worker` — claim-loop tuning

| Name | Type | Example | Description / rule |
|---|---|---|---|
| `batch_size` | int | `32` | Tasks claimed per `ClaimDue` round-trip. Must be ≥ 1. |
| `poll_interval` | duration | `500ms` | Delay between claim polls when idle. Must be > 0. |
| `concurrency` | int | `8` | Handlers run concurrently per instance. Must be ≥ 1. |
| `rate_limit` | rate | `100/s` | Per-instance token bucket; global rate across N instances is `N × rate_limit`. Omit for unlimited. |

### `classification` — error → outcome globs

| Name | Type | Example | Description |
|---|---|---|---|
| `retryable` | list of globs | `["java.net.*", "TIMEOUT"]` | Error types treated as retryable. |
| `permanent` | list of globs | `["*.ValidationException"]` | Error types that skip straight to the DLQ. |

Code classifiers and `OutcomeMapper`s take precedence; this glob layer is the only
one expressible in YAML. See the [outcome contract](/rdq/concepts/outcome-contract/).

### `handler`, `sync_retry`

| Name | Type | Example | Description |
|---|---|---|---|
| `handler.version_mismatch` | enum | `dead-letter` | `run-latest` or `dead-letter` when a task's handler version differs from the registered one. |
| `sync_retry.attempts` | int | `2` | In-process retries before durable enqueue (embedded SDK submit path only). Must be ≥ 0. |
| `sync_retry.backoff` | duration | `100ms` | Delay between sync-retry attempts. Must be ≥ 0. |

### `callback` — server-mode HTTP delivery

Ignored by the embedded SDK. Registering a callback = writing this block via the
admin API; the `url` must match the server `callback_allowlist`.

| Name | Type | Example | Description / rule |
|---|---|---|---|
| `protocol` | enum | `http` | `http` (v1) or `grpc` (accepted by the schema; transport is post-v1). |
| `url` | string | `https://payments.internal/rdq/charge` | Absolute callback URL. Required when a callback is configured. |
| `timeout` | duration | `30s` | Per-call timeout; a timeout is a retryable `TIMEOUT` failure. Must be > 0 and ≤ `handler_timeout`. |
| `auth.type` | enum | `bearer` | `none`, `bearer`, or `header`. |
| `auth.secret_ref` | string | `env:PAYMENTS_CB_TOKEN` | Indirection to a secret; v1 supports the `env:` scheme only. Required for `bearer`/`header`. |
| `response_mapping.retryable_status` | list | `[408, 429, "5xx"]` | Status codes/classes classified retryable, overriding the defaults. |
| `response_mapping.permanent_status` | list | `["4xx"]` | Status codes/classes classified permanent. |

### Example config

```yaml
config_version: 1

defaults:
  retry:
    max_attempts: 5
    initial_backoff: 1s
    backoff_multiplier: 2.0
    max_backoff: 10m
    jitter: 0.2
  execution:
    lease: 60s
    handler_timeout: 45s
  limits:
    max_payload_size: 1MiB
    ttl_succeeded: 24h
  worker:
    batch_size: 32
    poll_interval: 500ms
    concurrency: 8
    rate_limit: 100/s

queues:
  payments.charge:
    retry:
      max_attempts: 8
      initial_backoff: 2s
    classification:
      retryable: ["java.net.*", "TIMEOUT"]
      permanent: ["*.ValidationException"]
    callback:
      protocol: http
      url: https://payments.internal/rdq/charge
      timeout: 30s
      auth:
        type: bearer
        secret_ref: env:PAYMENTS_CB_TOKEN
      response_mapping:
        retryable_status: [408, 429, "5xx"]
        permanent_status: ["4xx"]
```

## See also

- [Queue configuration guide](/rdq/guides/queue-configuration/)
- [rdq-server guide](/rdq/guides/rdq-server/)
- [Server API](/rdq/reference/server-api/)
- [Storage backends & sizing](/rdq/reference/storage-backends/)
