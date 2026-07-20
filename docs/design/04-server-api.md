# rdq design 04 — rdq-server API

Status: Draft v1 · Companions: [01 — Wire envelope](01-wire-envelope.md), [02 — Storage SPI](02-storage-spi.md), [03 — Queue config](03-queue-config.md)

`rdq-server` exposes four surfaces: **data plane** (submit/lookup), **DLQ & ops**
(browse/redrive/purge/stats/pause), **admin** (queue config), and **outbound callback
delivery** (the contract rdq honors when invoking *your* endpoint). REST (JSON) and gRPC
have full parity; the OpenAPI spec and `.proto` files ship in the repo and are the
normative artifacts — this doc states the shape and the semantics.

Conventions: base path `/v1` · request/response bodies use the envelope model (design 01)
· cursor pagination · errors are RFC 9457 `application/problem+json` with a stable machine
`code` (`QUEUE_NOT_FOUND`, `PAYLOAD_TOO_LARGE`, `INVALID_TASK`, `FORBIDDEN`,
`STALE_CURSOR`, `STORAGE_UNAVAILABLE`, …) · `429` + `Retry-After` under storage
degradation — clients treat submit as retryable (it is idempotent, see below).

## 1. Data plane

```
POST /v1/queues/{queue}/tasks          → 202 full envelope
  body: { id?, handler_ref, payload(b64), payload_content_type, headers? }
POST /v1/queues/{queue}/tasks:batch    → 202 [envelope]     (BatchEnqueue capability)
GET  /v1/tasks/{id}                    → 200 envelope       (any status incl. DEAD)
```

- **Idempotent submit:** the client MAY supply `id` (a ULID it generated). Resubmitting the
  same `id` is a no-op returning the existing envelope (maps directly onto the SPI's
  idempotent `Enqueue`). Clients retrying a timed-out submit MUST reuse their `id`.
  Server-generated `id` otherwise.
- Rejections: `404 QUEUE_NOT_FOUND` (unconfigured queue — never silently defaulted, design
  03 §3), `413 PAYLOAD_TOO_LARGE` (per-queue limit), `422 INVALID_TASK`.

## 2. DLQ & ops

```
GET  /v1/queues/{queue}/dlq            → 200 { tasks: [envelope], next_cursor }
       ?error_type=&handler_ref=&from=&to=&limit=&cursor=
POST /v1/queues/{queue}/dlq:redrive    → 200 { count }
       body: { ids: [...] } | { filter: {error_type?, handler_ref?, from?, to?} }
POST /v1/queues/{queue}/dlq:purge      → 200 { count }      (same selector shape)
GET  /v1/queues/{queue}/stats          → 200 { pending, in_flight, dlq_depth, oldest_pending_age_ms }
POST /v1/admin/queues/{queue}:pause    → 204   (stop claiming; submits still accepted)
POST /v1/admin/queues/{queue}:resume   → 204
```

- Filter-based redrive/purge on plugins without `FilterPushdown` streams `DLQList` and acts
  by ids; entries dead-lettered mid-stream are not included (SPI OI-2 — documented, and the
  response `count` is authoritative).
- Every redrive/purge/pause writes an audit record (principal, selector, count — FR-18).
- **Pause** is the ops brake: taskflow keeps accumulating durably while a broken downstream
  is fixed, then `:resume` + redrive drains it. Pause state lives in queue config storage.
- v1 keeps redrive **synchronous** (bounded by selector size); an async job API for
  million-entry DLQs is post-v1 (see §6).

## 3. Admin

```
GET    /v1/admin/queues                       → 200 [queue names + summary]
GET    /v1/admin/queues/{queue}/config        → 200 queue config (design 03 schema, JSON)
PUT    /v1/admin/queues/{queue}/config        → 200 validated config   (upsert; strict validation)
DELETE /v1/admin/queues/{queue}               → 409 unless empty (no pending/in-flight/DLQ tasks)
```

Config writes persist via `ConfigStore`, take effect at next claim (design 03 §1), and are
audit-logged (design 03 OI-2 → resolved: **yes, in v1**).

## 4. Outbound callback contract (HTTP)

rdq delivers the task to the registered `callback.url` so the receiver sees **its own
payload natively** — metadata rides in headers, not in a wrapper the receiver must unpack:

```
POST {callback.url}
Content-Type: {payload_content_type}
X-RDQ-Task-Id: 01J2ZK7Q...          X-RDQ-Queue: payments.charge
X-RDQ-Handler-Ref: charge-payment    X-RDQ-Attempt: 3
X-RDQ-Signature: t=1721484202,v1=hex(hmac-sha256(secret, t + "." + body))   # if auth.type: hmac
traceparent: ...                     # propagated from task headers
Authorization: ...                   # per callback.auth (bearer/header)

<raw payload bytes>
```

- Response is classified per queue `response_mapping` (defaults FR-29). The response body
  (first 4 KiB) is recorded as the attempt's `error.detail` on failure — a callback that
  returns `500 {"reason": "..."}`` produces a DLQ entry that explains itself.
- Timeout: `callback.timeout`; a timeout is a `RETRYABLE_FAILURE` (`TIMEOUT` error type).
- **HMAC signing** (`auth.type: hmac`, secret via `secret_ref`) lets receivers verify the
  caller — standard webhook hygiene; timestamp in the signature bounds replay.
- gRPC callbacks: the client implements `rdq.v1.Handler/Execute(TaskContext, payload) →
  google.rpc.Status`-style response; status codes map per FR-29.

## 5. AuthN/Z

- `Authorization: Bearer <token>`. v1 token source: static token file mapping token →
  principal + grants (pluggable interface for OIDC/etc. later).
- Grants are per queue (glob supported: `payments.*`) × role:

| Role | Allows |
|---|---|
| `submitter` | submit, get task, stats |
| `operator` | submitter + DLQ list/redrive/purge, pause/resume |
| `admin` | operator + config read/write, queue delete |

- The callback allowlist and token file are server config — outside any queue owner's reach
  (design 03 §5).

## 6. Non-goals / post-v1

Async bulk-redrive jobs (`202 + job id` for huge DLQs) · DLQ watch/streaming (SSE) ·
response-body outcome mapper (design 03) · multi-tenancy above per-queue grants · web UI
(consumes exactly this API — no private endpoints, the CLI and future UI are ordinary
clients).

## 7. Open items

- **OI-1:** should `GET /v1/tasks/{id}` require queue-scoped grant knowledge (task→queue
  lookup cost) or a global read role? Leaning: resolve queue from the task row, enforce
  queue grant.
- **OI-2:** batch submit atomicity — all-or-nothing vs per-item results. Leaning: per-item
  results (`207`-style array), since idempotent ids make client retries of failures safe.
