---
title: Running rdq-server
description: Run rdq-server as a central retry hub — REST intake, per-queue HTTP callbacks, horizontal scaling, TLS, and auth.
---

`rdq-server` hosts the same engine as the SDKs, but behind an HTTP API. Any
language integrates without an SDK: submit failed work over REST, and rdq
delivers each retry attempt to a **callback** endpoint you register per queue. A
callback that times out or errors is just another failed attempt — it flows
through the identical retry/DLQ path as an in-process handler. Use the server as
a central retry hub for an organization.

> v1 intake is REST (JSON) with HTTP callbacks. gRPC intake and gRPC callbacks
> are a fast-follow and are purely additive over this contract.

## Run with Docker

```bash
docker run -e RDQ_DSN=postgres://user:pass@host/db \
           -p 8080:8080 ghcr.io/srjn45/rdq-server:2.1.0
```

`RDQ_DSN` points at the PostgreSQL database that holds task and config state. The
schema must be migrated first — run `rdq --dsn DSN migrate`, or start the server
with the opt-in `--auto-migrate` flag. A server started against an unmigrated or
unknown schema version refuses to run; migrations are never applied silently by
default.

Liveness and readiness probes sit outside `/v1` and need no auth: `GET /healthz`
(process is up) and `GET /readyz` (storage is reachable). Wire your orchestrator
probes to them so a rollout waits for a warm instance.

## Intake

Submit and look up tasks on the data plane. Bodies use the wire envelope model
(payload is base64 with a content type):

```
POST /v1/queues/{queue}/tasks          → 202, full envelope
POST /v1/queues/{queue}/tasks:batch    → 207, per-item results
GET  /v1/tasks/{id}                     → 200, envelope (any status, incl. DEAD)
GET  /v1/queues/{queue}/stats           → 200, { pending, in_flight, dlq_depth, oldest_pending_age_ms }
```

```bash
curl -X POST http://rdq-server:8080/v1/queues/payments.charge/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "id": "01J2ZK7Q...",
        "handler_ref": "charge-payment",
        "payload": "eyJvcmRlcl9pZCI6IjQyIn0=",
        "payload_content_type": "application/json"
      }'
```

Submit is **idempotent**: supply your own ULID `id` and resubmitting the same id
to the same queue is a no-op that returns the existing envelope, so a client
retrying a timed-out submit simply reuses its id. Batch submit is per-item (207
with a result array), not all-or-nothing. A task for an unconfigured queue is
rejected `404 QUEUE_NOT_FOUND` — never silently defaulted. Under storage
degradation the server returns `429` + `Retry-After`; treat submit as retryable.
The full endpoint catalog is in the [server API reference](/rdq/reference/server-api/).

## Register a callback per queue

In server mode, handlers are remote callbacks configured in the queue's config
(via the boot YAML file or `PUT /v1/admin/queues/{queue}/config`). rdq delivers
the task to `callback.url` so the receiver sees **its own payload natively** —
metadata rides in headers, not a wrapper:

```yaml
queues:
  payments.charge:
    callback:
      protocol: http                 # http (gRPC is post-v1)
      url: https://payments.internal/rdq/charge
      timeout: 30s                   # must be <= handler_timeout
      auth:
        type: bearer                 # none | bearer | header | hmac
        secret_ref: env:PAYMENTS_CB_TOKEN   # indirection only — raw secrets never in config
      response_mapping:
        retryable_status: [408, 429, "5xx"]
        permanent_status: ["4xx"]
```

Each delivery is an HTTP `POST` to your URL carrying the raw payload bytes and
rdq metadata as headers:

```
POST {callback.url}
Content-Type: {payload_content_type}
X-RDQ-Task-Id: 01J2ZK7Q...     X-RDQ-Queue: payments.charge
X-RDQ-Handler-Ref: charge-payment   X-RDQ-Attempt: 3
traceparent: ...               # propagated from the task headers
Authorization: ...             # per callback.auth
```

## Callback response mapping

The response classifies the attempt. Defaults (overridable per queue via
`response_mapping`):

| Response | Outcome |
|---|---|
| HTTP 2xx | `SUCCESS` |
| 408, 429, 5xx | `RETRYABLE_FAILURE` |
| Other 4xx | `PERMANENT_FAILURE` (dead-letter) |
| Timeout (`callback.timeout`) | `RETRYABLE_FAILURE` (`TIMEOUT`) |

The response body (first 4 KiB) is recorded as the attempt's error detail on
failure, so a callback that returns `500 {"reason": "..."}` produces a DLQ entry
that explains itself. See [the outcome contract](/rdq/concepts/outcome-contract/).

## Scale horizontally

Every `rdq-server` node is **stateless** — the storage backend is the only
coordination point. There is no leader election and no cluster membership. Put N
nodes behind an ordinary load balancer and point them all at one database;
atomic claims and leases guarantee no task runs twice, and node death is a
non-event (the lease expires and another node reclaims the task).

On `SIGTERM` the server drains gracefully: it stops claiming, finishes in-flight
callbacks within their lease, then exits — so a Kubernetes rollout is clean
rather than a burst of `LEASE_EXPIRED` attempts.

## TLS and auth basics

- **API auth:** `Authorization: Bearer <token>`. The v1 token source is a static
  token file mapping token → principal + grants (pluggable for OIDC later).
- **Authorization is per queue × role:** `submitter` (submit, get task, stats),
  `operator` (+ DLQ list/redrive/purge, pause/resume), `admin` (+ config
  read/write, queue delete). Task lookup is queue-scoped — there is no global
  read role.
- **TLS** protects both intake and outbound callbacks. Configure auth headers
  (bearer/header) or HMAC signing on callbacks so receivers can verify the caller.
- **Callback allowlist:** callback URLs are checked against a server-level
  allowlist that queue owners cannot widen — the SSRF mitigation. The allowlist
  and token file are platform-operator config, outside any queue owner's reach.
- Payloads are treated as sensitive and never logged in full.

## Admin, pause, and config

Manage queue config and pause claiming during an incident:

```
GET/PUT /v1/admin/queues/{queue}/config   # design-03 schema, strict validation
POST    /v1/admin/queues/{queue}:pause    # stop claiming; submits still accepted
POST    /v1/admin/queues/{queue}:resume
```

Pause is the ops brake: work keeps accumulating durably while a broken downstream
is fixed, then `:resume` + redrive drains it. Every redrive/purge/pause and every
API config write is audit-logged. See [DLQ analysis & redrive](/rdq/guides/dlq-and-redrive/).

## See also

- [Server API reference](/rdq/reference/server-api/)
- [Queue configuration & retry policies](/rdq/guides/queue-configuration/)
- [DLQ analysis & redrive](/rdq/guides/dlq-and-redrive/)
- [Observability & metrics](/rdq/guides/observability/)
- [Architecture — one core, two hosts](/rdq/concepts/architecture/)
