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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link Worker} using the in-memory {@link MemStorage} — no
 * Docker required. Exercises the same canonical flows as the integration test.
 */
class WorkerUnitTest {

    private static final String QUEUE = "test.q";
    private static final String REF = "test.handler";

    private static QueueSpec defaultSpec(int maxAttempts) {
        return QueueSpec.builder(QUEUE)
            .maxAttempts(maxAttempts)
            .backoff(Backoff.builder().initial(Duration.ofMillis(5)).multiplier(1.0).build())
            .classifier(Classifier.empty())
            .lease(Duration.ofSeconds(10))
            .handlerTimeout(Duration.ofSeconds(5))
            .pollInterval(Duration.ofMillis(20))
            .build();
    }

    private static Worker workerWith(MemStorage store, HandlerRegistry registry, int maxAttempts) {
        return Worker.builder(store, registry)
            .addQueue(defaultSpec(maxAttempts))
            .sweepInterval(Duration.ZERO)
            .build();
    }

    private static Thread runAsync(Worker worker) {
        Thread t = new Thread(() -> {
            try {
                worker.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "rdq-worker-unit");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static Envelope pendingWithPin(String id, String handlerVersion) {
        return Envelope.builder()
            .envelopeVersion(1)
            .id(id)
            .queue(QUEUE)
            .handlerRef(REF)
            .handlerVersion(handlerVersion)
            .payload(new byte[]{})
            .payloadContentType("text/plain")
            .status(Status.PENDING)
            .nextAttemptAt(Instant.now().minusMillis(1))
            .createdAt(Instant.now())
            .build();
    }

    // ---- submit → retry → succeed ------------------------------------------

    @Test
    void submitRetrySucceed_memStore() throws Exception {
        MemStorage store = new MemStorage();
        AtomicInteger calls = new AtomicInteger(0);
        CountDownLatch done = new CountDownLatch(1);

        HandlerRegistry registry = new HandlerRegistry();
        registry.register(REF, new Handler() {
            @Override
            public String version() { return "v1"; }

            @Override
            public void handle(Envelope task) throws Exception {
                if (calls.incrementAndGet() == 1) throw new RuntimeException("first failure");
                done.countDown();
            }
        });

        store.enqueue(MemStorage.pendingTask("t1", QUEUE, REF));
        Worker worker = workerWith(store, registry, 5);
        Thread t = runAsync(worker);

        boolean ok = done.await(10, TimeUnit.SECONDS);
        worker.stop();
        t.join(3_000);

        assertThat(ok).as("handler succeeded").isTrue();
        Envelope task = store.get("t1");
        assertThat(task.status()).isEqualTo(Status.SUCCEEDED);
        assertThat(task.attempts()).isNotNull();
        assertThat(task.attempts().get(0).outcome()).isEqualTo(Outcome.RETRYABLE_FAILURE);
        assertThat(task.attempts().get(task.attempts().size() - 1).outcome()).isEqualTo(Outcome.SUCCESS);
    }

    // ---- submit → exhaust → DLQ --------------------------------------------

    @Test
    void submitExhaustDlq_memStore() throws Exception {
        MemStorage store = new MemStorage();
        store.enqueue(MemStorage.pendingTask("t2", QUEUE, REF));

        HandlerRegistry registry = new HandlerRegistry();
        registry.register(REF, new Handler() {
            @Override
            public String version() { return "v1"; }

            @Override
            public void handle(Envelope task) throws Exception {
                throw new RuntimeException("always fails");
            }
        });

        Worker worker = workerWith(store, registry, 2);
        Thread t = runAsync(worker);

        Instant deadline = Instant.now().plusSeconds(10);
        while (Instant.now().isBefore(deadline)) {
            if (store.get("t2").status() == Status.DEAD) break;
            Thread.sleep(25);
        }

        worker.stop();
        t.join(3_000);

        assertThat(store.get("t2").status()).isEqualTo(Status.DEAD);
    }

    // ---- unroutable → DLQ -------------------------------------------------

    @Test
    void unroutableHandler_immediatelyDlq() throws Exception {
        MemStorage store = new MemStorage();
        store.enqueue(MemStorage.pendingTask("t3", QUEUE, "no.handler"));

        HandlerRegistry registry = new HandlerRegistry(); // empty
        Worker worker = workerWith(store, registry, 3);
        Thread t = runAsync(worker);

        Instant deadline = Instant.now().plusSeconds(5);
        while (Instant.now().isBefore(deadline)) {
            if (store.get("t3").status() == Status.DEAD) break;
            Thread.sleep(25);
        }

        worker.stop();
        t.join(3_000);

        Envelope task = store.get("t3");
        assertThat(task.status()).isEqualTo(Status.DEAD);
        assertThat(task.attempts().get(0).error().type())
            .isEqualTo(HandlerRegistry.ERROR_TYPE_UNROUTABLE);
    }

    // ---- PermanentException → DLQ in one attempt -------------------------

    @Test
    void permanentException_singleAttemptDlq() throws Exception {
        MemStorage store = new MemStorage();
        store.enqueue(MemStorage.pendingTask("t4", QUEUE, REF));
        CountDownLatch called = new CountDownLatch(1);

        HandlerRegistry registry = new HandlerRegistry();
        registry.register(REF, new Handler() {
            @Override
            public String version() { return "v1"; }

            @Override
            public void handle(Envelope task) throws Exception {
                called.countDown();
                throw new PermanentException(new IllegalArgumentException("bad"));
            }
        });

        Worker worker = workerWith(store, registry, 10);
        Thread t = runAsync(worker);

        assertThat(called.await(5, TimeUnit.SECONDS)).as("handler was invoked").isTrue();
        Thread.sleep(200);

        worker.stop();
        t.join(3_000);

        Envelope task = store.get("t4");
        assertThat(task.status()).isEqualTo(Status.DEAD);
        assertThat(task.attemptCount()).isEqualTo(1);
    }

    // ---- version mismatch dead-letter -------------------------------------

    @Test
    void versionMismatch_deadLetterPolicy_dlq() throws Exception {
        MemStorage store = new MemStorage();
        store.enqueue(pendingWithPin("t5", "v1"));

        HandlerRegistry registry = new HandlerRegistry();
        registry.register(REF, new Handler() {
            @Override
            public String version() { return "v2"; } // mismatch

            @Override
            public void handle(Envelope tsk) {}
        });

        QueueSpec spec = QueueSpec.builder(QUEUE)
            .maxAttempts(3)
            .backoff(Backoff.builder().initial(Duration.ofMillis(5)).build())
            .classifier(Classifier.empty())
            .lease(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(20))
            .versionPolicy(VersionPolicy.DEAD_LETTER)
            .build();
        Worker worker = Worker.builder(store, registry)
            .addQueue(spec)
            .sweepInterval(Duration.ZERO)
            .build();

        Thread t = runAsync(worker);
        Instant deadline = Instant.now().plusSeconds(5);
        while (Instant.now().isBefore(deadline)) {
            if (store.get("t5").status() == Status.DEAD) break;
            Thread.sleep(25);
        }
        worker.stop();
        t.join(3_000);

        Envelope task = store.get("t5");
        assertThat(task.status()).isEqualTo(Status.DEAD);
        assertThat(task.attempts().get(0).error().type())
            .isEqualTo(HandlerRegistry.ERROR_TYPE_VERSION_MISMATCH);
    }

    // ---- poison-pill (max_attempts already reached) -----------------------

    @Test
    void poisonPill_maxAttemptsAlreadyReached_dlqWithoutHandlerCall() throws Exception {
        MemStorage store = new MemStorage();

        Envelope task = Envelope.builder()
            .envelopeVersion(1)
            .id("t6")
            .queue(QUEUE)
            .handlerRef(REF)
            .payload(new byte[]{})
            .payloadContentType("text/plain")
            .status(Status.PENDING)
            .attemptCount(3) // already at limit
            .nextAttemptAt(Instant.now().minusMillis(1))
            .createdAt(Instant.now())
            .build();
        store.enqueue(task);

        AtomicInteger handlerCalls = new AtomicInteger(0);
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(REF, new Handler() {
            @Override
            public String version() { return "v1"; }

            @Override
            public void handle(Envelope t) { handlerCalls.incrementAndGet(); }
        });

        Worker worker = workerWith(store, registry, 3);
        Thread t = runAsync(worker);

        Instant deadline = Instant.now().plusSeconds(5);
        while (Instant.now().isBefore(deadline)) {
            if (store.get("t6").status() == Status.DEAD) break;
            Thread.sleep(25);
        }
        worker.stop();
        t.join(3_000);

        assertThat(store.get("t6").status()).isEqualTo(Status.DEAD);
        assertThat(handlerCalls.get()).isZero();
    }

    // ---- class rule: subclass matches superclass rule ---------------------

    @Test
    void classRule_subclassMatchesSuperclass_permanent() throws Exception {
        MemStorage store = new MemStorage();
        store.enqueue(MemStorage.pendingTask("t7", QUEUE, REF));

        HandlerRegistry registry = new HandlerRegistry();
        registry.register(REF, new Handler() {
            @Override
            public String version() { return "v1"; }

            @Override
            public void handle(Envelope task) throws Exception {
                // Throw a subclass of IllegalArgumentException
                throw new NumberFormatException("bad number");
            }
        });

        // Register IllegalArgumentException (superclass) as PERMANENT
        Classifier classifier = Classifier.builder()
            .classRule(ClassRule.of(IllegalArgumentException.class, Decision.PERMANENT))
            .build();

        QueueSpec spec = QueueSpec.builder(QUEUE)
            .maxAttempts(10)
            .backoff(Backoff.builder().initial(Duration.ofMillis(5)).build())
            .classifier(classifier)
            .lease(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(20))
            .build();

        Worker worker = Worker.builder(store, registry)
            .addQueue(spec)
            .sweepInterval(Duration.ZERO)
            .build();

        Thread t = runAsync(worker);
        Instant deadline = Instant.now().plusSeconds(5);
        while (Instant.now().isBefore(deadline)) {
            if (store.get("t7").status() == Status.DEAD) break;
            Thread.sleep(25);
        }
        worker.stop();
        t.join(3_000);

        assertThat(store.get("t7").status()).isEqualTo(Status.DEAD);
        assertThat(store.get("t7").attemptCount()).isEqualTo(1);
    }

    // ---- config glob classification ---------------------------------------

    @Test
    void configGlob_permanent_deadLettersOnFirstAttempt() throws Exception {
        MemStorage store = new MemStorage();
        store.enqueue(MemStorage.pendingTask("t8", QUEUE, REF));

        HandlerRegistry registry = new HandlerRegistry();
        registry.register(REF, new Handler() {
            @Override
            public String version() { return "v1"; }

            @Override
            public void handle(Envelope task) throws Exception {
                throw new IllegalStateException("validation failed");
            }
        });

        // errType = java.lang.IllegalStateException → matches java.lang.*
        Classifier classifier = Classifier.builder()
            .permanentPattern("java.lang.IllegalStateException")
            .build();

        QueueSpec spec = QueueSpec.builder(QUEUE)
            .maxAttempts(10)
            .backoff(Backoff.builder().initial(Duration.ofMillis(5)).build())
            .classifier(classifier)
            .lease(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(20))
            .build();

        Worker worker = Worker.builder(store, registry)
            .addQueue(spec)
            .sweepInterval(Duration.ZERO)
            .build();

        Thread t = runAsync(worker);
        Instant deadline = Instant.now().plusSeconds(5);
        while (Instant.now().isBefore(deadline)) {
            if (store.get("t8").status() == Status.DEAD) break;
            Thread.sleep(25);
        }
        worker.stop();
        t.join(3_000);

        assertThat(store.get("t8").status()).isEqualTo(Status.DEAD);
        assertThat(store.get("t8").attemptCount()).isEqualTo(1);
    }

    // ---- builder validation -----------------------------------------------

    @Test
    void worker_noQueues_throws() {
        MemStorage store = new MemStorage();
        HandlerRegistry registry = new HandlerRegistry();
        assertThatThrownBy(() -> Worker.builder(store, registry).build())
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void worker_nullStore_throws() {
        HandlerRegistry registry = new HandlerRegistry();
        assertThatThrownBy(() -> Worker.builder(null, registry))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void queueSpec_emptyQueue_throws() {
        assertThatThrownBy(() -> QueueSpec.builder(""))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void queueSpec_zeroLease_throws() {
        assertThatThrownBy(() -> QueueSpec.builder(QUEUE).lease(Duration.ZERO).build())
            .isInstanceOf(IllegalArgumentException.class);
    }
}
