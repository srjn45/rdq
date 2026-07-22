// SPDX-License-Identifier: Apache-2.0
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

package io.github.srjn45.rdq.worker.engine;

import io.github.srjn45.rdq.client.envelope.Envelope;
import io.github.srjn45.rdq.client.envelope.Outcome;
import io.github.srjn45.rdq.client.envelope.Status;
import io.github.srjn45.rdq.worker.postgres.PostgresStorage;
import io.github.srjn45.rdq.worker.spi.Storage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration acceptance tests for the Java worker engine over testcontainers Postgres
 * (design 05, T7.5 acceptance criterion). Tests two canonical flows:
 *
 * <ol>
 *   <li><b>submit&rarr;retry&rarr;succeed</b>: handler fails on attempt 1, succeeds on
 *       attempt 2 &mdash; task ends SUCCEEDED with two attempt records.</li>
 *   <li><b>submit&rarr;exhaust&rarr;DLQ</b>: handler always fails &mdash; after
 *       {@code maxAttempts} the task is dead-lettered (DEAD).</li>
 * </ol>
 */
@Testcontainers(disabledWithoutDocker = true)
class WorkerIntegrationTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(EngineTestHelper.IMAGE);

    private static DataSource dataSource;

    @BeforeAll
    static void migrate() {
        dataSource = EngineTestHelper.dataSource(POSTGRES);
        EngineTestHelper.applyMigrations(dataSource);
    }

    @BeforeEach
    void reset() {
        EngineTestHelper.truncate(dataSource);
    }

    // ---- submit → retry → succeed ------------------------------------------

    @Test
    void submitRetrySucceed_taskEndsSucceeded() throws Exception {
        final String queue = "engine.retry";
        final String handlerRef = "test.retry";

        AtomicInteger callCount = new AtomicInteger(0);
        CountDownLatch done = new CountDownLatch(1);

        HandlerRegistry registry = new HandlerRegistry();
        registry.register(handlerRef, new Handler() {
            @Override
            public String version() {
                return "v1";
            }

            @Override
            public void handle(Envelope task) throws Exception {
                int call = callCount.incrementAndGet();
                if (call == 1) {
                    throw new RuntimeException("first attempt fails");
                }
                // second call: success
                done.countDown();
            }
        });

        Storage store = PostgresStorage.open(dataSource);
        store.enqueue(pendingTask("retry-task", queue, handlerRef));

        QueueSpec spec = QueueSpec.builder(queue)
            .maxAttempts(3)
            .backoff(Backoff.builder()
                .initial(Duration.ofMillis(10))
                .multiplier(1.0)
                .build())
            .classifier(Classifier.empty())
            .lease(Duration.ofSeconds(10))
            .handlerTimeout(Duration.ofSeconds(5))
            .pollInterval(Duration.ofMillis(30))
            .build();

        Worker worker = Worker.builder(store, registry)
            .addQueue(spec)
            .sweepInterval(Duration.ZERO) // disable sweeper in test
            .build();

        Thread workerThread = new Thread(() -> {
            try {
                worker.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "rdq-worker-test");
        workerThread.setDaemon(true);
        workerThread.start();

        // Wait for success, then stop
        boolean succeeded = done.await(15, TimeUnit.SECONDS);
        worker.stop();
        workerThread.join(5_000);

        assertThat(succeeded).as("task succeeded within timeout").isTrue();
        assertThat(callCount.get()).isGreaterThanOrEqualTo(2);

        Envelope task = store.get("retry-task");
        assertThat(task.status()).isEqualTo(Status.SUCCEEDED);
        assertThat(task.attempts()).isNotNull();
        assertThat(task.attempts().size()).isGreaterThanOrEqualTo(2);
        assertThat(task.attempts().get(0).outcome()).isEqualTo(Outcome.RETRYABLE_FAILURE);
        assertThat(task.attempts().get(task.attempts().size() - 1).outcome()).isEqualTo(Outcome.SUCCESS);
    }

    // ---- submit → exhaust → DLQ --------------------------------------------

    @Test
    void submitExhaustDlq_taskEndsDeadLettered() throws Exception {
        final String queue = "engine.dlq";
        final String handlerRef = "test.dlq";
        final int maxAttempts = 2;

        HandlerRegistry registry = new HandlerRegistry();
        registry.register(handlerRef, new Handler() {
            @Override
            public String version() {
                return "v1";
            }

            @Override
            public void handle(Envelope task) throws Exception {
                throw new RuntimeException("always fails");
            }
        });

        Storage store = PostgresStorage.open(dataSource);
        store.enqueue(pendingTask("dlq-task", queue, handlerRef));

        // Watch for the task to be dead-lettered via abandon hook
        Worker.Builder wb = Worker.builder(store, registry)
            .addQueue(QueueSpec.builder(queue)
                .maxAttempts(maxAttempts)
                .backoff(Backoff.builder()
                    .initial(Duration.ofMillis(10))
                    .multiplier(1.0)
                    .build())
                .classifier(Classifier.empty())
                .lease(Duration.ofSeconds(10))
                .handlerTimeout(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(30))
                .build())
            .sweepInterval(Duration.ZERO);

        Worker worker = wb.build();

        Thread workerThread = new Thread(() -> {
            try {
                worker.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "rdq-worker-dlq-test");
        workerThread.setDaemon(true);
        workerThread.start();

        // Poll until the task is DEAD and full attempt history is visible (same
        // visibility race as RetryExampleTest — DEAD commit can precede history row)
        Instant deadline = Instant.now().plusSeconds(15);
        Envelope task = null;
        while (Instant.now().isBefore(deadline)) {
            task = store.get("dlq-task");
            if (task.status() == Status.DEAD && task.attempts().size() >= maxAttempts) break;
            Thread.sleep(50);
        }

        worker.stop();
        workerThread.join(5_000);

        assertThat(task).isNotNull();
        assertThat(task.status()).as("task should be dead-lettered").isEqualTo(Status.DEAD);
        assertThat(task.attempts()).isNotNull();
        assertThat(task.attempts().size()).isGreaterThanOrEqualTo(maxAttempts);
    }

    // ---- unroutable → DLQ --------------------------------------------------

    @Test
    void unroutableHandler_deadLettersImmediately() throws Exception {
        final String queue = "engine.unroutable";

        HandlerRegistry registry = new HandlerRegistry(); // empty — no handlers registered

        Storage store = PostgresStorage.open(dataSource);
        store.enqueue(pendingTask("unroutable-task", queue, "no.such.handler"));

        Worker worker = Worker.builder(store, registry)
            .addQueue(QueueSpec.builder(queue)
                .maxAttempts(3)
                .backoff(Backoff.builder().initial(Duration.ofMillis(10)).build())
                .classifier(Classifier.empty())
                .lease(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(30))
                .build())
            .sweepInterval(Duration.ZERO)
            .build();

        Thread workerThread = new Thread(() -> {
            try {
                worker.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "rdq-worker-unroutable-test");
        workerThread.setDaemon(true);
        workerThread.start();

        // Wait for DEAD and attempt history visible (same visibility race — DEAD
        // commit can precede the attempt row, causing attempts.get(0) to throw)
        Instant deadline = Instant.now().plusSeconds(10);
        Envelope task = null;
        while (Instant.now().isBefore(deadline)) {
            task = store.get("unroutable-task");
            if (task.status() == Status.DEAD && task.attempts().size() >= 1) break;
            Thread.sleep(50);
        }

        worker.stop();
        workerThread.join(5_000);

        assertThat(task).isNotNull();
        assertThat(task.status()).isEqualTo(Status.DEAD);
        assertThat(task.attempts().get(0).error().type())
            .isEqualTo(HandlerRegistry.ERROR_TYPE_UNROUTABLE);
    }

    // ---- permanent exception → DLQ in one shot ----------------------------

    @Test
    void permanentException_deadLettersOnFirstAttempt() throws Exception {
        final String queue = "engine.permanent";
        final String handlerRef = "test.permanent";
        final CountDownLatch called = new CountDownLatch(1);

        HandlerRegistry registry = new HandlerRegistry();
        registry.register(handlerRef, new Handler() {
            @Override
            public String version() {
                return "v1";
            }

            @Override
            public void handle(Envelope task) throws Exception {
                called.countDown();
                throw new PermanentException(new IllegalArgumentException("bad input"));
            }
        });

        Storage store = PostgresStorage.open(dataSource);
        store.enqueue(pendingTask("perm-task", queue, handlerRef));

        Worker worker = Worker.builder(store, registry)
            .addQueue(QueueSpec.builder(queue)
                .maxAttempts(10) // high max — permanent wrapper must short-circuit
                .backoff(Backoff.builder().initial(Duration.ofMillis(10)).build())
                .classifier(Classifier.empty())
                .lease(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(30))
                .build())
            .sweepInterval(Duration.ZERO)
            .build();

        Thread workerThread = new Thread(() -> {
            try {
                worker.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "rdq-worker-perm-test");
        workerThread.setDaemon(true);
        workerThread.start();

        boolean handlerCalled = called.await(10, TimeUnit.SECONDS);
        // Give a moment for the DLQ write to complete
        Thread.sleep(200);

        worker.stop();
        workerThread.join(5_000);

        assertThat(handlerCalled).isTrue();
        Envelope task = store.get("perm-task");
        assertThat(task.status()).isEqualTo(Status.DEAD);
        assertThat(task.attemptCount()).isEqualTo(1); // dead-lettered on first attempt
    }

    // ---- helpers -----------------------------------------------------------

    private static Envelope pendingTask(String id, String queue, String handlerRef) {
        return Envelope.builder()
            .envelopeVersion(1)
            .id(id)
            .queue(queue)
            .handlerRef(handlerRef)
            .payload(("payload-" + id).getBytes(StandardCharsets.UTF_8))
            .payloadContentType("application/octet-stream")
            .status(Status.PENDING)
            .nextAttemptAt(Instant.now().minusMillis(1))
            .createdAt(Instant.now())
            .build();
    }
}
