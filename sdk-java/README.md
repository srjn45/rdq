# rdq Java SDK

Reliable Distributed Queue — Java SDK. Two published artifacts (OQ-1):

| Artifact | What it contains |
|---|---|
| `rdq-java-client` | Envelope model + codec (submit-side API, no Postgres dependency) |
| `rdq-java-worker` | Worker engine + Postgres binding (depends on `rdq-java-client`) |

---

## Quickstart

### 1. Add the dependency

**Gradle (Kotlin DSL)**
```kotlin
implementation("io.github.srjn45:rdq-java-worker:2.1.0")
```

**Maven**
```xml
<dependency>
  <groupId>io.github.srjn45</groupId>
  <artifactId>rdq-java-worker</artifactId>
  <version>2.1.0</version>
</dependency>
```

`rdq-java-client` is pulled in transitively. Supply any JDBC connection pool
(`HikariCP`, `c3p0`, …) as your `DataSource`.

### 2. Apply the schema migration

The Java worker binds to the **same** shared PostgreSQL schema as the Go worker
(design 05, G5). Apply the T2.1 DDL migration exactly once before starting
any worker:

```
storage/postgres/migrations/0001_init.up.sql
```

The storage binding will refuse to open (`SchemaNotInitializedException`) if
the migration has not been applied.

### 3. Minimal consume snippet

```java
import io.github.srjn45.rdq.client.envelope.Envelope;
import io.github.srjn45.rdq.worker.engine.*;
import io.github.srjn45.rdq.worker.postgres.PostgresStorage;
import io.github.srjn45.rdq.worker.spi.Storage;

import javax.sql.DataSource;
import java.time.Duration;

// dataSource — any pooled JDBC DataSource pointing at the migrated database
Storage store = PostgresStorage.open(dataSource);

HandlerRegistry registry = new HandlerRegistry();
registry.register("my-queue.order-processor", new Handler() {
    @Override public String version() { return "v1"; }

    @Override
    public void handle(Envelope task) throws Exception {
        // decode task.payload(), do real work …
        // return normally  → SUCCEEDED
        // throw RetryableException  → reschedule with backoff
        // throw PermanentException  → dead-letter immediately (no retry)
        // throw anything else       → retried by default (Classifier.empty())
    }
});

QueueSpec spec = QueueSpec.builder("my-queue")
    .maxAttempts(5)
    .backoff(Backoff.builder()
        .initial(Duration.ofSeconds(1))
        .multiplier(2.0)
        .max(Duration.ofMinutes(5))
        .build())
    .classifier(Classifier.empty())   // default: all exceptions → retryable
    .lease(Duration.ofSeconds(30))
    .pollInterval(Duration.ofMillis(500))
    .build();

Worker worker = Worker.builder(store, registry)
    .addQueue(spec)
    .build();

// blocks until worker.stop() is called or the thread is interrupted
worker.run();
```

---

## Run the end-to-end example

`example/` is a self-contained Gradle subproject that demonstrates the full
`submit → retry (backoff) → exhaust max attempts → DLQ` lifecycle against a
real Postgres spun up by [Testcontainers](https://testcontainers.com/).

**Requirements**: Docker (or compatible container runtime).

```bash
cd sdk-java
./gradlew :example:test
```

The test in `example/src/test/java/…/RetryExampleTest.java` shows exactly
what happens step-by-step:

1. Open `PostgresStorage` against the migrated schema.
2. Enqueue a task (`order-42`) on queue `example.orders`.
3. Register a handler (`example.order-processor`) that throws
   `RetryableException` on every call to simulate a persistently unavailable
   upstream service.
4. Configure the queue with `maxAttempts=3` and exponential backoff
   (50 ms → 100 ms → 200 ms).
5. Start the worker and wait until the task transitions to `DEAD`.
6. Assert: 3 attempt records (all `RETRYABLE_FAILURE`) and 1 DLQ entry.

The test is skipped automatically when Docker is unavailable, matching the
pattern used by `WorkerIntegrationTest` and `StorageComplianceTest`.

---

## Build the SDK locally

```bash
cd sdk-java
./gradlew build          # compile, test, JaCoCo, SpotBugs, Spotless
./gradlew :example:test  # run only the quickstart example
```

---

## Key API surface

| Class / interface | Description |
|---|---|
| `PostgresStorage.open(DataSource)` | Opens the Postgres binding; verifies schema version |
| `Storage` | SPI: enqueue, claimDue, complete, reschedule, deadLetter, dlqList, … |
| `Handler` | Implement to process tasks: `version()` + `handle(Envelope)` |
| `HandlerRegistry` | Thread-safe `handler_ref → Handler` map |
| `QueueSpec` | Per-queue policy: maxAttempts, backoff, lease, concurrency, … |
| `Worker` | Poll loop + fan-out; call `run()` to block, `stop()` to drain |
| `Backoff` | Exponential (+ jitter) retry delay schedule |
| `Classifier` | Five-layer failure classification (OutcomeMapper → wrapper → class rules → globs → default) |
| `RetryableException` | Wrap to force retryable classification |
| `PermanentException` | Wrap to dead-letter on first failure |
