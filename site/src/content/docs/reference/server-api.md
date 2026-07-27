---
title: Server API (REST & gRPC)
description: The rdq-server v1 REST contract — submit, look up, DLQ browse/redrive/purge, stats, admin config, and health — plus the planned gRPC parity.
---

`rdq-server` exposes four surfaces over HTTP: the **data plane** (submit and look
up tasks), **DLQ & ops** (browse, redrive, purge, stats, pause/resume), **admin**
(queue-configuration CRUD, including callback registration), and **health**
(liveness/readiness). The [`server/openapi.yaml`](https://github.com/srjn45/rdq/blob/main/server/openapi.yaml)
document is the normative contract; this page summarizes it.

Conventions:

- Base path is `/v1`. Health probes sit **outside** `/v1` and its auth.
- Request and response bodies use the [wire envelope](/rdq/concepts/wire-envelope/) model (JSON; payload as base64 bytes + content type).
- Errors are RFC 9457 `application/problem+json` with a stable machine `code`
  (`QUEUE_NOT_FOUND`, `ID_CONFLICT`, `PAYLOAD_TOO_LARGE`, `INVALID_TASK`,
  `STALE_CURSOR`, `FORBIDDEN`, `RATE_LIMITED`, `STORAGE_UNAVAILABLE`, …).
- Auth is `Authorization: Bearer <token>` on every `/v1` route (see [Auth](#authentication--authorization)).
- **v1 ships REST intake + HTTP callbacks.** gRPC intake and gRPC callbacks are a
  fast-follow; parity is specified and purely additive (see [gRPC parity](#grpc-parity)).

## Endpoint catalog

| Method | Path | Surface | Min role | Purpose |
|---|---|---|---|---|
| `POST` | `/v1/queues/{queue}/tasks` | data-plane | submitter | Submit a task (idempotent) |
| `POST` | `/v1/queues/{queue}/tasks:batch` | data-plane | submitter | Submit many tasks (per-item `207`) |
| `GET` | `/v1/tasks/{id}` | data-plane | submitter¹ | Look up a task in any status |
| `GET` | `/v1/queues/{queue}/dlq` | dlq-ops | operator | Browse/filter the DLQ (cursor-paged) |
| `POST` | `/v1/queues/{queue}/dlq:redrive` | dlq-ops | operator | Redrive by ids or filter |
| `POST` | `/v1/queues/{queue}/dlq:purge` | dlq-ops | operator | Purge by ids or filter |
| `GET` | `/v1/queues/{queue}/stats` | dlq-ops | submitter | Per-queue depth/age snapshot |
| `POST` | `/v1/admin/queues/{queue}:pause` | dlq-ops | operator | Stop claiming; submits still accepted |
| `POST` | `/v1/admin/queues/{queue}:resume` | dlq-ops | operator | Resume claiming |
| `GET` | `/v1/admin/queues` | admin | admin | List queue names + summary |
| `GET` | `/v1/admin/queues/{queue}/config` | admin | admin | Read queue config |
| `PUT` | `/v1/admin/queues/{queue}/config` | admin | admin | Upsert queue config (callback registration) |
| `DELETE` | `/v1/admin/queues/{queue}` | admin | admin | Delete an empty queue |
| `GET` | `/healthz` | health | none | Liveness |
| `GET` | `/readyz` | health | none | Readiness (storage reachable) |

¹ `GET /v1/tasks/{id}` resolves the task's queue from its stored row and enforces
your grant on **that** queue — there is no global read role.

> **README shorthand.** The README's `POST /v1/tasks` / `GET /v1/dlq` are
> abbreviations; the real, queue-scoped paths above are what the OpenAPI spec and
> the server implement.

## Data plane

### Submit a task

`POST /v1/queues/{queue}/tasks` → `202` with the full envelope. The client may
supply an `id` (a ULID it generated) to make the submit idempotent: resubmitting
the same `id` to the same queue is a no-op returning the existing envelope. The
same `id` in a **different** queue is rejected `409 ID_CONFLICT`. Clients retrying
a timed-out submit MUST reuse their `id`.

```json
// Request body — SubmitTaskRequest
{
  "id": "01J2ZK7Q8V9M3F5T7B2C4D6E8G",
  "handler_ref": "charge-payment",
  "payload": "eyJvcmRlcl9pZCI6IjQyIn0=",
  "payload_content_type": "application/json",
  "headers": { "traceparent": "00-...-01", "x-source-topic": "payments" }
}
```

```json
// 202 response — Envelope (projection; full schema frozen in core/envelope)
{
  "id": "01J2ZK7Q8V9M3F5T7B2C4D6E8G",
  "queue": "payments.charge",
  "handler_ref": "charge-payment",
  "status": "PENDING"
}
```

Rejections: `404 QUEUE_NOT_FOUND` (queue not configured — never silently
defaulted), `409 ID_CONFLICT`, `413 PAYLOAD_TOO_LARGE` (per-queue limit),
`422 INVALID_TASK`, `429 RATE_LIMITED`, `503 STORAGE_UNAVAILABLE` (safe to retry —
submit is idempotent).

### Batch submit

`POST /v1/queues/{queue}/tasks:batch` takes a JSON array of `SubmitTaskRequest`
and returns `207 Multi-Status` with one result per item — a failure on one item
never blocks the others. Idempotent ids make per-item retry of the failed entries
safe.

```json
// 207 response — array of BatchItemResult
[
  { "index": 0, "status": 202, "envelope": { "id": "01J2...", "status": "PENDING" } },
  { "index": 1, "status": 409, "error": { "code": "ID_CONFLICT", "status": 409, "title": "ID Conflict" } }
]
```

### Look up a task

`GET /v1/tasks/{id}` → `200` envelope in any status
(`PENDING | IN_FLIGHT | SUCCEEDED | DEAD`). `403 FORBIDDEN` if you lack a grant on
the task's queue; `404` if no task has that id.

## DLQ & ops

### Browse

`GET /v1/queues/{queue}/dlq` returns cursor-paginated DLQ entries carrying the
full task plus attempt history. Filter with query params:

```
GET /v1/queues/payments.charge/dlq?error_type=TIMEOUT&handler_ref=charge-payment&from=2026-07-27T14:00:00Z&to=...&limit=50&cursor=...
```

```json
// 200 response
{ "tasks": [ { "id": "01J2...", "queue": "payments.charge", "status": "DEAD" } ], "next_cursor": "..." }
```

### Redrive and purge

Both take the same selector shape — a list of ids **or** a filter — and return the
number of affected tasks. Redrive re-enqueues to the retry queue with a reset
policy; purge deletes. Every mutation writes an audit record (principal, selector,
count).

```json
// POST /v1/queues/{queue}/dlq:redrive  (or :purge)
{ "filter": { "error_type": "TimeoutException", "from": "2026-07-27T14:00:00Z" } }
// — or —
{ "ids": ["01J2ZK7Q...", "01J2ZK8R..."] }
```

```json
// 200 response
{ "count": 128 }
```

On plugins without filter pushdown, filter-based redrive/purge streams the DLQ and
acts by id; entries dead-lettered mid-stream are excluded, and the returned
`count` is authoritative. v1 redrive/purge is **synchronous** (bounded by selector
size); an async job API for million-entry DLQs is post-v1.

### Stats

`GET /v1/queues/{queue}/stats` → `200`:

```json
{ "pending": 42, "in_flight": 8, "dlq_depth": 3, "oldest_pending_age_ms": 1500 }
```

### Pause / resume

`POST /v1/admin/queues/{queue}:pause` and `:resume` return `204`. Pause is the ops
brake: the server stops **claiming** while submits are still accepted, so taskflow
accumulates durably while a broken downstream is fixed, then `:resume` + redrive
drains it. Pause state lives in queue-config storage.

## Admin & callback registration

```
GET    /v1/admin/queues                 → 200 [queue names + summary]
GET    /v1/admin/queues/{queue}/config  → 200 queue config (JSON)
PUT    /v1/admin/queues/{queue}/config  → 200 validated config (upsert, strict)
DELETE /v1/admin/queues/{queue}         → 409 unless empty (no pending/in-flight/DLQ tasks)
```

**Callback registration is config, not a separate endpoint.** A queue's callback
target (URL, protocol, timeout, auth) is a `callback` block in its queue config,
written via `PUT .../config`. The URL must match the server's callback allowlist
or the config write is rejected. See [Configuration](/rdq/reference/configuration/)
for the callback schema and [rdq-server guide](/rdq/guides/rdq-server/) for the
outbound callback contract.

## Health

- `GET /healthz` — liveness. Reports only that the process is up; never touches
  dependencies. No auth.
- `GET /readyz` — readiness. `200 {"status":"ready","checks":{"storage":"ok"}}`
  when every dependency probe passes; `503 STORAGE_UNAVAILABLE` (with `Retry-After`)
  otherwise, so a rollout waits for a warm instance. No auth.

## Authentication & authorization

Every `/v1` route requires `Authorization: Bearer <token>`. The v1 token source is
a static token file mapping a token to a principal holding per-`(queue × role)`
grants; queue grants support globs (`payments.*`). Roles are ordered —
`submitter ⊂ operator ⊂ admin`:

| Role | Allows |
|---|---|
| `submitter` | submit, get task, stats |
| `operator` | submitter + DLQ list/redrive/purge, pause/resume |
| `admin` | operator + config read/write, queue delete |

Missing/unknown token → `401 UNAUTHENTICATED`; authenticated but under-privileged →
`403 FORBIDDEN`. The token file and callback allowlist are server config, outside
any queue owner's reach.

## gRPC parity

gRPC intake and gRPC callbacks are **post-v1**, a fast-follow that is purely
additive over the REST/HTTP contract — no v1 contract change. The fast-follow
callback shape has the client implement
`rdq.v1.Handler/Execute(TaskContext, payload)` returning a `google.rpc.Status`-style
response, with status codes classified per the same rules as HTTP callbacks.

## See also

- [rdq-server guide](/rdq/guides/rdq-server/)
- [Configuration](/rdq/reference/configuration/)
- [DLQ & redrive](/rdq/guides/dlq-and-redrive/)
- [Wire envelope](/rdq/concepts/wire-envelope/)
