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

## Design

- [PRD](docs/PRD.md)
- [01 — Wire envelope](docs/design/01-wire-envelope.md)
- [02 — Storage SPI](docs/design/02-storage-spi.md)
- [03 — Queue configuration](docs/design/03-queue-config.md)
- [04 — rdq-server API](docs/design/04-server-api.md)

## License

[Apache-2.0](LICENSE)
