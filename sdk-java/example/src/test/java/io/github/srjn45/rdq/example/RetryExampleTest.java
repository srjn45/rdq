/*
 * Copyright 2025-2026 Srajan Pathak
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.srjn45.rdq.example;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.srjn45.rdq.client.envelope.Attempt;
import io.github.srjn45.rdq.client.envelope.Envelope;
import io.github.srjn45.rdq.client.envelope.Outcome;
import io.github.srjn45.rdq.client.envelope.Status;
import io.github.srjn45.rdq.worker.engine.Backoff;
import io.github.srjn45.rdq.worker.engine.Classifier;
import io.github.srjn45.rdq.worker.engine.Handler;
import io.github.srjn45.rdq.worker.engine.HandlerRegistry;
import io.github.srjn45.rdq.worker.engine.QueueSpec;
import io.github.srjn45.rdq.worker.engine.RetryableException;
import io.github.srjn45.rdq.worker.engine.Worker;
import io.github.srjn45.rdq.worker.postgres.PostgresStorage;
import io.github.srjn45.rdq.worker.spi.DlqFilter;
import io.github.srjn45.rdq.worker.spi.DlqPage;
import io.github.srjn45.rdq.worker.spi.Page;
import io.github.srjn45.rdq.worker.spi.Storage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end quickstart: submit &rarr; handler failure &rarr; retry with
 * exponential backoff &rarr; exhaust max attempts &rarr; DLQ with history.
 *
 * <p>This test is the canonical Java SDK usage example, exercising every layer of
 * the public API against a real PostgreSQL database. It is skipped automatically
 * when Docker is unavailable (matches the pattern in {@code WorkerIntegrationTest}
 * and {@code StorageComplianceTest}).
 *
 * <p><b>To run:</b>
 * <pre>
 *   cd sdk-java
 *   ./gradlew :example:test
 * </pre>
 */
@Testcontainers(disabledWithoutDocker = true)
class RetryExampleTest {

    private static final DockerImageName IMAGE = DockerImageName.parse("postgres:16-alpine");

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(IMAGE);

    private static DataSource dataSource;

    @BeforeAll
    static void setUp() {
        dataSource = buildDataSource(POSTGRES);
        applyMigrations(dataSource);
    }

    /**
     * Full lifecycle: a task is submitted, its handler throws
     * {@link RetryableException} on every invocation, and after {@code MAX_ATTEMPTS}
     * the engine dead-letters it. The stored attempt history and DLQ entry are then
     * verified.
     *
     * <p>This is the primary quickstart scenario new users should run first.
     */
    @Test
    void submit_retryWithBackoff_exhaustAndDlq() throws InterruptedException {
        final String queue = "example.orders";
        final String handlerRef = "example.order-processor";
        final int maxAttempts = 3;
        final String taskId = "order-42";
        final AtomicInteger callCount = new AtomicInteger();

        // ── 1. Open storage (schema must already be at version 2.1) ────────────
        Storage store = PostgresStorage.open(dataSource);

        // ── 2. Enqueue a task ──────────────────────────────────────────────────
        store.enqueue(
            Envelope.builder()
                .envelopeVersion(1)
                .id(taskId)
                .queue(queue)
                .handlerRef(handlerRef)
                .payload("{\"order_id\":42}".getBytes(StandardCharsets.UTF_8))
                .payloadContentType("application/json")
                .status(Status.PENDING)
                .nextAttemptAt(Instant.now())
                .createdAt(Instant.now())
                .build());

        // ── 3. Register a handler that always fails ────────────────────────────
        //
        // Wrapping the exception in RetryableException (layer-2 wrapper) forces
        // RETRYABLE classification regardless of the queue's classifier config.
        // A plain RuntimeException would also be retried by default (layer-5 default),
        // but the explicit wrapper is shown here to illustrate the API.
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(
            handlerRef,
            new Handler() {
                @Override
                public String version() {
                    return "v1";
                }

                @Override
                public void handle(Envelope task) {
                    int n = callCount.incrementAndGet();
                    System.out.printf(
                        "[rdq-example] handler called (attempt %d/%d) — throwing RetryableException%n",
                        n, maxAttempts);
                    throw new RetryableException(
                        "simulated transient failure on attempt " + n,
                        new RuntimeException("upstream service unavailable"));
                }
            });

        // ── 4. Configure queue spec ────────────────────────────────────────────
        //
        // backoff: 50 ms → 100 ms → 200 ms (multiplier=2) so the test completes quickly.
        // In production use longer initial delays (e.g. Duration.ofSeconds(1)).
        QueueSpec spec =
            QueueSpec.builder(queue)
                .maxAttempts(maxAttempts)
                .backoff(
                    Backoff.builder()
                        .initial(Duration.ofMillis(50))
                        .multiplier(2.0)
                        .max(Duration.ofSeconds(5))
                        .build())
                .classifier(Classifier.empty()) // default: all unclassified exceptions → retryable
                .lease(Duration.ofSeconds(30))
                .handlerTimeout(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(20))
                .build();

        // ── 5. Build and start the worker ──────────────────────────────────────
        Worker worker =
            Worker.builder(store, registry)
                .addQueue(spec)
                .sweepInterval(Duration.ZERO) // disable succeeded-purge sweeper in this demo
                .build();

        Thread workerThread =
            new Thread(
                () -> {
                    try {
                        worker.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                "rdq-example-worker");
        workerThread.setDaemon(true);
        workerThread.start();

        // ── 6. Wait until the task reaches DEAD status ─────────────────────────
        Instant deadline = Instant.now().plusSeconds(30);
        Envelope result = store.get(taskId);
        while (Instant.now().isBefore(deadline) && result.status() != Status.DEAD) {
            Thread.sleep(50);
            result = store.get(taskId);
        }

        worker.stop();
        workerThread.join(5_000);

        // ── 7. Assert: exhausted after maxAttempts, all attempts recorded ───────
        System.out.printf("%n[rdq-example] Final status: %s%n", result.status());
        assertThat(result.status())
            .as("task should be dead-lettered after exhausting all attempts")
            .isEqualTo(Status.DEAD);

        assertThat(callCount.get())
            .as("handler must have been called exactly maxAttempts times")
            .isEqualTo(maxAttempts);

        List<Attempt> attempts = result.attempts();
        assertThat(attempts)
            .as("full history must be attached to the dead-lettered task")
            .hasSize(maxAttempts);

        for (Attempt a : attempts) {
            System.out.printf(
                "[rdq-example]   attempt #%d  outcome=%-22s  error=%s%n",
                a.attemptNo(),
                a.outcome(),
                a.error() != null ? a.error().message() : "—");
            assertThat(a.outcome())
                .as("every attempt should record RETRYABLE_FAILURE")
                .isEqualTo(Outcome.RETRYABLE_FAILURE);
            assertThat(a.error()).as("attempt error info must be present").isNotNull();
        }

        // ── 8. DLQ is visible ──────────────────────────────────────────────────
        DlqPage dlq = store.dlqList(queue, DlqFilter.none(), Page.first());
        System.out.printf("[rdq-example] DLQ depth for queue '%s': %d%n", queue, dlq.tasks().size());
        assertThat(dlq.tasks()).as("dead-lettered task must appear in the DLQ").hasSize(1);
        assertThat(dlq.tasks().get(0).id()).isEqualTo(taskId);
    }

    // ── test harness helpers ────────────────────────────────────────────────────

    private static DataSource buildDataSource(PostgreSQLContainer<?> container) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(container.getJdbcUrl());
        ds.setUser(container.getUsername());
        ds.setPassword(container.getPassword());
        return ds;
    }

    /**
     * Applies the frozen T2.1 migration to {@code ds}. Locates the SQL file by
     * walking up from {@code user.dir}, the same strategy used by
     * {@code TestPostgres} and {@code EngineTestHelper}.
     */
    @SuppressFBWarnings(
        value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
        justification = "frozen T2.1 migration SQL read from the repo — not user input")
    private static void applyMigrations(DataSource ds) {
        Path file = findRepoFile("storage/postgres/migrations/0001_init.up.sql");
        String sql;
        try {
            sql = Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new RuntimeException("reading migration " + file, ex);
        }
        try (Connection conn = ds.getConnection();
            Statement st = conn.createStatement()) {
            st.execute(sql);
        } catch (SQLException ex) {
            throw new RuntimeException("applying T2.1 migration", ex);
        }
    }

    private static Path findRepoFile(String relative) {
        Path dir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve(relative);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException(
            "could not locate " + relative + " from " + System.getProperty("user.dir"));
    }
}
