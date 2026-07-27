---
title: Java SDK
description: Embed rdq in a JVM service — add the dependency, register handlers, and run a durable retry worker over your existing Postgres.
---

The Java SDK embeds the rdq engine inside your JVM application. Handlers are
in-process `Handler` implementations, and retry state lives in the PostgreSQL
schema you already run. This guide covers install, a minimal worker, exception
classification, and the optional sync-retry wrapper.

## Add the dependency

The engine ships as two Maven artifacts under group `io.github.srjn45`:

| Artifact | What it contains |
|---|---|
| `rdq-java-client` | Envelope model + codec — the submit-side API, no Postgres dependency. |
| `rdq-java-worker` | Worker engine + Postgres binding. Depends on `rdq-java-client`. |

Depend on `rdq-java-client` alone for a submit-only producer; depend on
`rdq-java-worker` to run a worker (it pulls the client in transitively). This is
the "submit here, execute there" split.

**Maven**

```xml
<dependency>
  <groupId>io.github.srjn45</groupId>
  <artifactId>rdq-java-worker</artifactId>
  <version>2.1.0</version>
</dependency>
```

**Gradle (Kotlin DSL)**

```kotlin
implementation("io.github.srjn45:rdq-java-worker:2.1.0")
```

Supply any pooled JDBC `DataSource` (HikariCP, c3p0, …) pointing at your
database.

## Apply the schema migration

The Java worker binds to the **same** shared PostgreSQL schema as the Go worker.
Apply the migration exactly once before starting any worker:

```
storage/postgres/migrations/0001_init.up.sql
```

`PostgresStorage.open` throws `SchemaNotInitializedException` if the migration
has not been applied. You can also run it with the CLI (`rdq --dsn DSN migrate`),
see [the rdq CLI](/rdq/guides/cli/).

## Open storage and register a handler

A `Handler` binds to a stable `handler_ref` and carries a `version()` tag plus a
`handle()` method. The registry is a thread-safe `handler_ref → Handler` map.

```java
import io.github.srjn45.rdq.client.envelope.Envelope;
import io.github.srjn45.rdq.worker.engine.*;
import io.github.srjn45.rdq.worker.postgres.PostgresStorage;
import io.github.srjn45.rdq.worker.spi.Storage;

Storage store = PostgresStorage.open(dataSource); // verifies schema version

HandlerRegistry registry = new HandlerRegistry();
registry.register("my-queue.order-processor", new Handler() {
    @Override public String version() { return "v1"; }

    @Override
    public void handle(Envelope task) throws Exception {
        // decode task.payload(), do real work …
        // return normally         → SUCCEEDED
        // throw RetryableException → reschedule with backoff
        // throw PermanentException → dead-letter immediately (no retry)
        // throw anything else      → retried by default (Classifier.empty())
    }
});
```

Handlers must be **idempotent** — rdq is at-least-once, so a task can be
delivered more than once. The `version()` tag travels with each submitted task;
when a task's version no longer matches the registered handler, the queue's
`handler.version_mismatch` policy (`run-latest` | `dead-letter`) decides what
happens. See [queue configuration](/rdq/guides/queue-configuration/).

## Configure the queue and run the worker

Build a `QueueSpec` with the retry budget and backoff schedule, add it to a
`Worker`, and call `run()`:

```java
import java.time.Duration;

QueueSpec spec = QueueSpec.builder("my-queue")
    .maxAttempts(5)
    .backoff(Backoff.builder()
        .initial(Duration.ofSeconds(1))
        .multiplier(2.0)          // 2.0 = exponential; 1.0 = linear
        .max(Duration.ofMinutes(5))
        .build())
    .classifier(Classifier.empty()) // default: all exceptions → retryable
    .lease(Duration.ofSeconds(30))
    .pollInterval(Duration.ofMillis(500))
    .build();

Worker worker = Worker.builder(store, registry)
    .addQueue(spec)
    .build();

worker.run(); // blocks until worker.stop() is called or the thread is interrupted
```

For a compact exponential schedule you can also use the shorthand
`Backoff.exponential(Duration.ofSeconds(1), 2.0, Duration.ofMinutes(5))`. Add
more queues with additional `addQueue(...)` calls before `build()`. Run N
instances against one database to scale horizontally — atomic claims and leases
guarantee no double execution.

## Classifying failures

Every invocation resolves to one canonical outcome. The default rule: **a normal
return is success; a thrown exception is a failure**, retryable unless the
queue's error lists say otherwise. Force a decision by throwing the wrappers:

```java
// Retryable — reschedule with backoff.
throw new RetryableException("rate limited", cause);

// Permanent — dead-letter on first failure, skip remaining attempts.
throw new PermanentException("card declined: do not retry", cause);
```

`Classifier` layers five levels, most specific first: `OutcomeMapper` →
`Retryable`/`Permanent` wrappers → exception-class rules (hierarchy-aware) →
config globs → default (retryable). `Classifier.empty()` uses only the default.
Full model in [the outcome contract](/rdq/concepts/outcome-contract/).

## Graceful shutdown

`worker.run()` blocks; call `worker.stop()` from another thread — typically a
JVM shutdown hook — to drain cleanly. The worker stops claiming new tasks and
lets in-flight handlers finish within their lease before `run()` returns:

```java
Runtime.getRuntime().addShutdownHook(new Thread(worker::stop));
```

Without this, a rolling restart looks like a burst of worker crashes
(`LEASE_EXPIRED` attempts); with it, rollouts are clean.

## Optional: sync-retry wrapper

The Java SDK carries a synchronous in-process retrier (informed by the original
`RetrySync`/`RetryConfig` prototype). Use it to attempt a bounded number of
inline retries on the happy path *before* durably enqueuing the task — the same
pattern as the Go SDK's `SyncRetrier`. Configure it per queue via `sync_retry`
(attempts + backoff); it runs in submit-only hosts too, since it executes before
durable enqueue.

## See also

- [The outcome contract](/rdq/concepts/outcome-contract/)
- [Tasks, attempts & the lifecycle](/rdq/concepts/task-lifecycle/)
- [Queue configuration & retry policies](/rdq/guides/queue-configuration/)
- [Configuration reference](/rdq/reference/configuration/)
- [Storage backends & sizing](/rdq/reference/storage-backends/)
