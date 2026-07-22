# rdq ![Build Status](https://github.com/srjn45/rdq/actions/workflows/ci.yml/badge.svg)

**Retry & Dead-letter Queues — for any broker, any storage, any language.**

> 🚧 **Redesign in progress.** rdq (formerly `kafka-retry-dlq`) is being rebuilt from the
> ground up. The [PRD](docs/PRD.md) and [design docs](docs/design/) describe the target;
> implementation is underway and nothing here is API-stable yet.

## What is rdq?

A function call failed — a Kafka handler, an SQS consumer, any `func(args)`. Hand rdq the
handler's name and its arguments, and rdq guarantees one of two outcomes:

1. the call **eventually succeeds** (retried on your configured backoff schedule), or
2. it lands in a **dead-letter queue with its full failure history** — every attempt's
   error, stack trace, and timestamp — where it can be inspected, fixed, and redriven.

What makes rdq different:

- **Bring your own storage** — retry queues and DLQs live in the datastore you already run
  (PostgreSQL first; Redis, MongoDB, and more via a documented storage SPI). rdq adds no
  new stateful infrastructure.
- **Broker-agnostic** — Kafka, SQS, Redpanda, AutoMQ, RabbitMQ, or no broker at all. rdq
  accepts failures from any source; it never sits on the hot path of successful messages.
- **Two form factors, one engine** — embed the SDK in your app (zero extra infra), or run
  `rdq-server` as a central retry hub with REST/gRPC intake and HTTP/gRPC callbacks.
- **At-least-once, horizontally scalable** — stateless workers, atomic claims, and leases;
  fault tolerance is inherited from your storage backend's own HA.

## Repository layout

| Path | What it is |
|---|---|
| [`core/`](core/) | The engine (Go): envelope model, retry policies, outcome classification, storage SPI + compliance kit |
| [`storage/postgres/`](storage/postgres/) | Reference storage plugin (`FOR UPDATE SKIP LOCKED` claims) |
| [`server/`](server/) | `rdq-server`: REST/gRPC intake, DLQ & admin APIs, callback delivery |
| [`cli/`](cli/) | `rdq` CLI: stats, DLQ browse, redrive/purge |
| [`sdk-go/`](sdk-go/) | Embedded Go SDK |
| [`sdk-java/`](sdk-java/) | Java SDK (embedded engine per spec; carries the original sync retrier) |
| [`docs/`](docs/) | [PRD](docs/PRD.md) and design docs |

## Quickstart

**Requirements**: Go 1.22+, PostgreSQL 14+, Docker (for `rdq-server`).

### Go SDK — submit a task and run a worker

```go
import (
    "context"
    "github.com/srjn45/rdq/sdk-go"
    "github.com/srjn45/rdq/sdk-go/submit"
    "github.com/srjn45/rdq/storage/postgres"
)

// 1. Open storage (apply migrations once: postgres.Migrate(ctx, db)).
db, _ := postgres.Open("postgres://user:pass@localhost/mydb")
store := postgres.New(db)

// 2. Register your handler.
rdq.Register("payments.charge", func(ctx context.Context, task envelope.Envelope) error {
    // decode task.Payload(), do real work …
    // return nil  → SUCCEEDED
    // return err  → retried per policy
    return nil
})

// 3. Submit a task from the producer side.
env, _ := submit.Submit("payments.charge", []byte(`{"order_id":"42"}`))
store.Enqueue(ctx, env)

// 4. Run the worker (blocks until stopped).
worker, _ := rdq.NewWorker(store, rdq.WithQueue("payments.charge",
    rdq.MaxAttempts(5),
    rdq.BackoffExponential(time.Second, 2.0, 5*time.Minute),
))
worker.Run(ctx)
```

See [sdk-go/README.md](sdk-go/README.md) for the full Go SDK guide.

### Java SDK — minimal worker snippet

```java
Storage store = PostgresStorage.open(dataSource);

HandlerRegistry registry = new HandlerRegistry();
registry.register("payments.charge", new Handler() {
    @Override public String version() { return "v1"; }
    @Override public void handle(Envelope task) throws Exception {
        // decode task.payload(), do real work …
    }
});

QueueSpec spec = QueueSpec.builder("payments")
    .maxAttempts(5)
    .backoff(Backoff.exponential(Duration.ofSeconds(1), 2.0, Duration.ofMinutes(5)))
    .build();

Worker.builder(store, registry).addQueue(spec).build().run();
```

See [sdk-java/README.md](sdk-java/README.md) for the full Java SDK guide.

### rdq-server (Docker)

```bash
docker run -e RDQ_DSN=postgres://user:pass@host/db \
           -p 8080:8080 ghcr.io/srjn45/rdq-server:2.1.0
```

Submit tasks via `POST /v1/tasks`, inspect the DLQ at `GET /v1/dlq`, redrive
with `POST /v1/dlq/{id}/redrive`. Full API: [docs/design/04-server-api.md](docs/design/04-server-api.md).

---

## Design

- [PRD](docs/PRD.md)
- [01 — Wire envelope](docs/design/01-wire-envelope.md)
- [02 — Storage SPI](docs/design/02-storage-spi.md)
- [03 — Queue configuration](docs/design/03-queue-config.md)
- [04 — rdq-server API](docs/design/04-server-api.md)

## License

[Apache-2.0](LICENSE)
