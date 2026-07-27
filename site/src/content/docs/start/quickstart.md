---
title: Quickstart
description: Submit a task and run a worker in under 30 minutes — with the Go SDK, the Java SDK, or rdq-server. Watch a failure retry, then dead-letter, then redrive.
---

The goal: get one failed unit of work retrying — and, when it exhausts its policy, landing in a
dead-letter queue you can inspect and redrive. Pick your host below. All three share the same
[storage](/rdq/reference/storage-backends/), [envelope](/rdq/concepts/wire-envelope/), and
[outcome contract](/rdq/concepts/outcome-contract/).

## Prerequisites

- A reachable **PostgreSQL 14+** instance (the v1 reference backend).
- One of: Go 1.22+, Java 17+, or Docker.

## Path A — Go SDK

```go
import (
    "context"
    "time"

    "github.com/srjn45/rdq/sdk-go"
    "github.com/srjn45/rdq/sdk-go/submit"
    "github.com/srjn45/rdq/storage/postgres"
    "github.com/srjn45/rdq/core/envelope"
)

// 1. Open storage and apply migrations once.
db, _ := postgres.Open("postgres://user:pass@localhost/mydb")
_ = postgres.Migrate(ctx, db)
store := postgres.New(db)

// 2. Register your handler under a stable name.
rdq.Register("charge-payment", func(ctx context.Context, t envelope.Envelope) error {
    // decode t.Payload(), do real work …
    // return nil → SUCCEEDED; return err → retried per policy
    return nil
})

// 3. Submit a task from the producer side.
env, _ := submit.Submit("payments.charge", "charge-payment", []byte(`{"order_id":"42"}`))
_ = store.Enqueue(ctx, env)

// 4. Run the worker (blocks until ctx is cancelled).
worker, _ := rdq.NewWorker(store, rdq.WithQueue("payments.charge",
    rdq.MaxAttempts(5),
    rdq.BackoffExponential(time.Second, 2.0, 5*time.Minute),
))
worker.Run(ctx)
```

Full detail — outcome classification, the submit-only sub-package, graceful shutdown — is in the
[Go SDK guide](/rdq/guides/go-sdk/).

## Path B — Java SDK

```java
Storage store = PostgresStorage.open(dataSource);

HandlerRegistry registry = new HandlerRegistry();
registry.register("charge-payment", new Handler() {
    @Override public String version() { return "v1"; }
    @Override public void handle(Envelope task) throws Exception {
        // decode task.payload(), do real work …
    }
});

QueueSpec spec = QueueSpec.builder("payments.charge")
    .maxAttempts(5)
    .backoff(Backoff.exponential(Duration.ofSeconds(1), 2.0, Duration.ofMinutes(5)))
    .build();

Worker.builder(store, registry).addQueue(spec).build().run();
```

A normal return is a success; a thrown exception is a failure, classified retryable or permanent
by the queue's error lists. More in the [Java SDK guide](/rdq/guides/java-sdk/).

## Path C — rdq-server (any language)

Start the hub against your storage:

```bash
docker run -e RDQ_DSN=postgres://user:pass@host/db \
           -p 8080:8080 ghcr.io/srjn45/rdq-server:latest
```

Submit a task over REST (payload is base64-encoded opaque bytes):

```bash
curl -X POST localhost:8080/v1/queues/payments.charge/tasks \
  -H 'content-type: application/json' -d '{
  "handler_ref": "charge-payment",
  "payload": "eyJvcmRlcl9pZCI6IjQyIn0="
}'
```

Register a per-queue callback (`rdq-server` invokes it to execute the task), and failures flow
through the same retry/DLQ path. See the [rdq-server guide](/rdq/guides/rdq-server/) and the
[Server API reference](/rdq/reference/server-api/).

## Inspect and redrive

However you submitted it, when a task exhausts its policy it lands in the DLQ with its full
[attempt history](/rdq/concepts/task-lifecycle/). Inspect and replay with the CLI:

```bash
# what's dead, and why
rdq dlq list --queue payments.charge

# read one task's complete failure history
rdq dlq get 01J8Z8M4Q0P9K3X2C7B1A6D5E4

# fix shipped — redrive everything that timed out after 14:00
rdq dlq redrive --queue payments.charge --error TimeoutException --since 14:00
```

Bulk redrive resets the retry policy and re-enqueues the matching tasks; every mutation is
[audit-logged](/rdq/guides/dlq-and-redrive/).

## What just happened

1. You submitted `(queue, handler_ref, payload)` to durable storage.
2. A stateless worker **atomically claimed** the due task under a lease, invoked the handler, and
   recorded the attempt.
3. On failure it backed off and rescheduled; on exhaustion it dead-lettered with full history.
4. You browsed the DLQ and redrove after the fix — cross-host, cross-language.

## Next steps

- [Architecture](/rdq/concepts/architecture/) — how the pieces coordinate through storage.
- [Queue configuration & retry policies](/rdq/guides/queue-configuration/) — tune backoff, jitter, TTL.
- [The outcome contract](/rdq/concepts/outcome-contract/) — success vs retryable vs permanent.
- [Observability & metrics](/rdq/guides/observability/) — DLQ depth and oldest-task-age alerts.
