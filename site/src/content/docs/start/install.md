---
title: Install
description: Add the Go or Java SDK, run rdq-server from the container image, or grab the rdq CLI. The only external dependency is storage you already run.
---

rdq ships in a few pieces — pick the ones you need. Everything shares one
[wire envelope](/rdq/concepts/wire-envelope/) and one [storage backend](/rdq/reference/storage-backends/),
so you can mix and match (submit from the SDK, execute via the server, inspect from the CLI).

## Requirements

- **Go 1.22+** — for the Go SDK and building from source.
- **PostgreSQL 14+** — the v1 reference [storage backend](/rdq/reference/storage-backends/).
- **Docker** — for running `rdq-server` from the published image.
- **Java 17+ / Maven** — for the Java SDK.

## Go SDK

```bash
go get github.com/srjn45/rdq/sdk-go
```

The worker package is `github.com/srjn45/rdq/sdk-go`; a submit-only sub-package
(`github.com/srjn45/rdq/sdk-go/submit`) lets producer-side apps enqueue work without pulling in
the worker. You'll also want a storage plugin:

```bash
go get github.com/srjn45/rdq/storage/postgres
```

Apply the migrations once at startup:

```go
db, _ := postgres.Open("postgres://user:pass@localhost/mydb")
if err := postgres.Migrate(ctx, db); err != nil { /* ... */ }
store := postgres.New(db)
```

Continue in the [Go SDK guide](/rdq/guides/go-sdk/).

## Java SDK

The engine is split into a submit-only client and the worker (so apps can "submit here, execute
there"):

```xml
<!-- Producer side: submit tasks only -->
<dependency>
  <groupId>io.github.srjn45</groupId>
  <artifactId>rdq-java-client</artifactId>
  <version>2.1.0</version>
</dependency>

<!-- Worker side: the embedded engine (depends on the client) -->
<dependency>
  <groupId>io.github.srjn45</groupId>
  <artifactId>rdq-java-worker</artifactId>
  <version>2.1.0</version>
</dependency>
```

Continue in the [Java SDK guide](/rdq/guides/java-sdk/).

## rdq-server (Docker)

Run the central retry hub against storage you already operate:

```bash
docker run -e RDQ_DSN=postgres://user:pass@host/db \
           -p 8080:8080 ghcr.io/srjn45/rdq-server:latest
```

It exposes REST and gRPC intake plus DLQ and admin APIs. Because it's stateless, run as many
replicas as you like behind a load balancer — coordination happens entirely in storage. See the
[rdq-server guide](/rdq/guides/rdq-server/) and the [Server API reference](/rdq/reference/server-api/).

A Helm chart ships alongside the image for Kubernetes deployments; see the
[configuration reference](/rdq/reference/configuration/) for the full environment surface.

## rdq CLI

The `rdq` CLI is a single Go binary for queue stats, DLQ browse/inspect, and redrive:

```bash
go install github.com/srjn45/rdq/cli/cmd/rdq@latest
```

Point it at your storage (or at an `rdq-server`) and start inspecting:

```bash
rdq dlq list --queue payments.charge
```

Continue in the [CLI guide](/rdq/guides/cli/).

## Verify

Once storage is reachable, the fastest end-to-end check is the [Quickstart](/rdq/start/quickstart/):
submit a task, run a worker, watch it succeed after a retry — or land in the DLQ and get redriven.

## See also

- [Quickstart](/rdq/start/quickstart/)
- [Running rdq-server](/rdq/guides/rdq-server/)
- [Storage backends & sizing](/rdq/reference/storage-backends/)
- [Configuration](/rdq/reference/configuration/)
