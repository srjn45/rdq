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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.srjn45.rdq.client.envelope.Attempt;
import io.github.srjn45.rdq.client.envelope.Envelope;
import io.github.srjn45.rdq.client.envelope.ErrorInfo;
import io.github.srjn45.rdq.client.envelope.Outcome;
import io.github.srjn45.rdq.worker.spi.Claimed;
import io.github.srjn45.rdq.worker.spi.ClaimToken;
import io.github.srjn45.rdq.worker.spi.Storage;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * The worker claim loop: polls the store for due tasks across its queues, fans
 * them out to registered handlers under per-queue concurrency and rate-limit caps,
 * and resolves each outcome ({@code complete} / {@code reschedule} /
 * {@code deadLetter}). Also runs the jittered PurgeSucceeded sweeper (G19).
 * Mirrors Go {@code core/engine.Worker}.
 *
 * <p>Construct with {@link #builder(Storage, HandlerRegistry)}, then call
 * {@link #run()} to block until {@link #stop()} is called or the calling thread is
 * interrupted.
 */
public final class Worker {

    private static final String ERROR_TYPE_MAX_ATTEMPTS = "rdq.MaxAttemptsExceeded";
    private static final long DEFAULT_SWEEP_INTERVAL_MS = 30_000;
    private static final double DEFAULT_SWEEP_JITTER = 0.2;

    private final Storage store;
    private final HandlerRegistry registry;
    private final List<QueueState> queues;
    private final long sweepIntervalMs;
    private final double sweepJitter;
    private final BiConsumer<Envelope, Exception> abandonHook;
    private final Random rng = new Random();

    private final CountDownLatch stopLatch = new CountDownLatch(1);

    // Handler execution pool: sum of per-queue concurrencies.
    private final ExecutorService handlerPool;
    // Scheduler for heartbeat tasks, timeouts, and sweeper.
    private final ScheduledExecutorService scheduler;

    private Worker(Builder b) {
        this.store = b.store;
        this.registry = b.registry;
        this.sweepIntervalMs = b.sweepIntervalMs;
        this.sweepJitter = b.sweepJitter;
        this.abandonHook = b.abandonHook;

        int totalConcurrency = 0;
        this.queues = new ArrayList<>(b.specs.size());
        for (QueueSpec spec : b.specs) {
            totalConcurrency += spec.concurrency();
            Limiter limiter = spec.ratePerSecond() > 0
                ? Limiter.withRate(spec.ratePerSecond(), spec.rateBurst(), System::nanoTime)
                : Limiter.unlimited();
            queues.add(new QueueState(spec, new Semaphore(spec.concurrency()), limiter));
        }
        this.handlerPool = Executors.newFixedThreadPool(Math.max(1, totalConcurrency));
        // 2 threads per queue for heartbeat/timeout + sweeper
        this.scheduler = Executors.newScheduledThreadPool(Math.max(2, queues.size() * 2 + 1));
    }

    /**
     * Drives the worker until {@link #stop()} is called or the calling thread is
     * interrupted, then drains: stops claim loops, waits for in-flight handlers up
     * to the longest lease (G10).
     */
    public void run() throws InterruptedException {
        List<Thread> pollThreads = new ArrayList<>(queues.size());
        for (QueueState q : queues) {
            Thread t = new Thread(() -> claimLoop(q), "rdq-poll-" + q.spec.queue());
            t.setDaemon(true);
            t.start();
            pollThreads.add(t);
        }
        if (sweepIntervalMs > 0) {
            scheduleNextSweep();
        }

        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Stop claim loops
        for (Thread t : pollThreads) {
            t.interrupt();
        }
        for (Thread t : pollThreads) {
            try {
                t.join(2_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        // Drain in-flight handlers bounded by the longest lease (G10)
        handlerPool.shutdown();
        long drainMs = queues.stream()
            .mapToLong(q -> q.spec.lease().toMillis())
            .max()
            .orElse(DEFAULT_SWEEP_INTERVAL_MS);
        try {
            handlerPool.awaitTermination(drainMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        scheduler.shutdownNow();
    }

    /** Signals the worker to stop; {@link #run()} will return after draining. */
    public void stop() {
        stopLatch.countDown();
    }

    // ---- claim loop --------------------------------------------------------

    private void claimLoop(QueueState q) {
        while (!Thread.currentThread().isInterrupted()) {
            pollOnce(q);
            try {
                Thread.sleep(q.spec.pollInterval().toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @SuppressFBWarnings(
        value = {"DE_MIGHT_IGNORE", "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE"},
        justification = "transient store errors are skipped; Future ignored — semaphore tracks slot lifecycle")
    private void pollOnce(QueueState q) {
        if (Thread.currentThread().isInterrupted()) return;
        int free = q.semaphore.availablePermits();
        if (free <= 0) return;
        int limit = Math.min(free, q.spec.batchSize());
        limit = grantTokens(q, limit);
        if (limit <= 0) return;

        List<Claimed> claimed;
        try {
            claimed = store.claimDue(q.spec.queue(), limit, q.spec.lease());
        } catch (Exception e) {
            return;
        }

        for (Claimed c : claimed) {
            try {
                q.semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            boolean submitted = false;
            try {
                handlerPool.submit(() -> {
                    try {
                        processOne(q, c);
                    } finally {
                        q.semaphore.release();
                    }
                });
                submitted = true;
            } finally {
                if (!submitted) q.semaphore.release();
            }
        }
    }

    private int grantTokens(QueueState q, int want) {
        int granted = 0;
        while (granted < want && q.limiter.allow()) {
            granted++;
        }
        return granted;
    }

    // ---- handler execution -------------------------------------------------

    private void processOne(QueueState q, Claimed c) {
        Envelope task = c.task();
        ClaimToken token = c.token();

        // budgetNo tracks the attempt budget counter (resets after redrive);
        // historyNo is the next position in the monotonic rdq_attempt history
        // sequence — computed from attempts().size() to avoid a
        // UNIQUE(task_id,attempt_no) collision when attempt_count was reset by
        // a redrive while history rows are preserved (T5.7-class fix).
        int budgetNo  = task.attemptCount() + 1;
        int historyNo = (task.attempts() != null ? task.attempts().size() : 0) + 1;

        // Poison-pill guard: a task at max_attempts is dead-lettered without a run.
        if (task.attemptCount() >= q.spec.maxAttempts()) {
            Instant now = Instant.now();
            Attempt att = attempt(historyNo, now, now,
                Outcome.PERMANENT_FAILURE, ERROR_TYPE_MAX_ATTEMPTS,
                "max_attempts (" + q.spec.maxAttempts() + ") reached without a successful attempt");
            safeDeadLetter(task, token, att);
            return;
        }

        // Route
        HandlerRegistry.Resolution res = registry.resolve(task, q.spec.versionPolicy());
        if (res.action() == HandlerRegistry.Action.DEAD_LETTER) {
            Instant now = Instant.now();
            Attempt att = attempt(historyNo, now, now,
                Outcome.PERMANENT_FAILURE, res.errorType(), res.errorMessage());
            safeDeadLetter(task, token, att);
            return;
        }

        Instant started = Instant.now();
        Exception handlerError = invokeHandler(q, task, token, res.handler());
        Instant finished = Instant.now();

        if (handlerError == null) {
            Attempt att = attempt(historyNo, started, finished, Outcome.SUCCESS, null, null);
            try {
                store.complete(task.id(), token, att);
            } catch (Exception e) {
                abandon(task, e);
            }
            return;
        }

        // Failure path: classify, then reschedule or dead-letter
        String errType = Classifier.errorType(handlerError);
        Verdict verdict = q.spec.classifier().classify(handlerError, errType);
        Attempt att = attempt(historyNo, started, finished,
            verdict.decision().toOutcome(), errType, handlerError.getMessage());

        boolean exhausted = budgetNo >= q.spec.maxAttempts();
        if (verdict.decision() == Decision.PERMANENT || exhausted) {
            safeDeadLetter(task, token, att);
        } else {
            Instant nextAt = finished.plus(q.spec.backoff().delay(budgetNo, rng::nextDouble));
            try {
                store.reschedule(task.id(), token, att, nextAt);
            } catch (Exception e) {
                abandon(task, e);
            }
        }
    }

    /**
     * Runs the handler under the handler timeout and, when enabled, the heartbeat.
     * Returns {@code null} on success, or the exception on failure.
     */
    private Exception invokeHandler(QueueState q, Envelope task, ClaimToken token, Handler handler) {
        Thread handlerThread = Thread.currentThread();

        // Heartbeat: extends the lease periodically; interrupts the handler if the
        // lease is lost so the outcome write will get StaleClaimException and be abandoned.
        ScheduledFuture<?> heartbeat = null;
        if (q.spec.heartbeat()) {
            long hbMs = Math.max(1, q.spec.lease().toMillis() / 3);
            heartbeat = scheduler.scheduleWithFixedDelay(() -> {
                try {
                    store.extendLease(task.id(), token, q.spec.lease());
                } catch (Exception e) {
                    handlerThread.interrupt(); // lease lost or other error
                }
            }, hbMs, hbMs, TimeUnit.MILLISECONDS);
        }

        // Timeout: interrupt the handler thread after handlerTimeout.
        ScheduledFuture<?> timeoutTask = null;
        long timeoutMs = q.spec.handlerTimeout().toMillis();
        if (timeoutMs > 0) {
            timeoutTask = scheduler.schedule(
                handlerThread::interrupt, timeoutMs, TimeUnit.MILLISECONDS);
        }

        try {
            handler.handle(task);
            return null; // success
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return e;
        } catch (Exception e) {
            return e;
        } catch (Error e) {
            return new RuntimeException("handler threw Error: " + e.getMessage(), e);
        } finally {
            // Clear the interrupt flag so this pooled thread is reusable.
            Thread.interrupted();
            if (heartbeat != null) heartbeat.cancel(false);
            if (timeoutTask != null) timeoutTask.cancel(false);
        }
    }

    // ---- sweeper -----------------------------------------------------------

    private void scheduleNextSweep() {
        long delayMs = jitteredSweepMs();
        scheduler.schedule(this::sweepOnce, delayMs, TimeUnit.MILLISECONDS);
    }

    @SuppressFBWarnings(
        value = "DE_MIGHT_IGNORE",
        justification = "purge errors must not disrupt the claim loop")
    private void sweepOnce() {
        if (stopLatch.getCount() == 0) return;
        Instant now = Instant.now();
        for (QueueState q : queues) {
            Duration ttl = q.spec.ttlSucceeded();
            if (ttl == null || ttl.toMillis() <= 0) continue;
            try {
                store.purgeSucceeded(q.spec.queue(), now.minus(ttl));
            } catch (Exception ignored) {}
        }
        if (stopLatch.getCount() > 0) {
            scheduleNextSweep();
        }
    }

    private long jitteredSweepMs() {
        if (sweepJitter <= 0) return sweepIntervalMs;
        double factor = 1.0 + sweepJitter * (2.0 * rng.nextDouble() - 1.0);
        long d = (long) (sweepIntervalMs * factor);
        return d > 0 ? d : sweepIntervalMs;
    }

    // ---- helpers -----------------------------------------------------------

    private void safeDeadLetter(Envelope task, ClaimToken token, Attempt att) {
        try {
            store.deadLetter(task.id(), token, att);
        } catch (Exception e) {
            abandon(task, e);
        }
    }

    private void abandon(Envelope task, Exception e) {
        if (abandonHook != null) abandonHook.accept(task, e);
    }

    private static Attempt attempt(int no, Instant started, Instant finished,
                                   Outcome outcome, String errType, String errMsg) {
        Attempt.Builder b = Attempt.builder()
            .attemptNo(no)
            .startedAt(started)
            .finishedAt(finished)
            .outcome(outcome);
        if (errType != null) {
            b.error(ErrorInfo.builder().type(errType).message(errMsg).build());
        }
        return b.build();
    }

    // ---- builder -----------------------------------------------------------

    /**
     * Returns a builder. Both {@code store} and {@code registry} are required;
     * at least one {@link QueueSpec} must be added before calling {@link Builder#build()}.
     */
    public static Builder builder(Storage store, HandlerRegistry registry) {
        return new Builder(store, registry);
    }

    /** Fluent builder for {@link Worker}. */
    public static final class Builder {

        private final Storage store;
        private final HandlerRegistry registry;
        private final List<QueueSpec> specs = new ArrayList<>();
        private long sweepIntervalMs = DEFAULT_SWEEP_INTERVAL_MS;
        private double sweepJitter = DEFAULT_SWEEP_JITTER;
        private BiConsumer<Envelope, Exception> abandonHook;

        private Builder(Storage store, HandlerRegistry registry) {
            this.store = Objects.requireNonNull(store, "store");
            this.registry = Objects.requireNonNull(registry, "registry");
        }

        public Builder addQueue(QueueSpec spec) {
            specs.add(Objects.requireNonNull(spec, "spec"));
            return this;
        }

        /**
         * Overrides the sweeper base interval (default 30&nbsp;s). A non-positive
         * value disables the sweeper.
         */
        public Builder sweepInterval(Duration interval) {
            this.sweepIntervalMs = interval == null ? 0 : interval.toMillis();
            return this;
        }

        /** Overrides the sweeper jitter fraction in [0, 1) (default 0.2). */
        public Builder sweepJitter(double jitter) {
            this.sweepJitter = jitter;
            return this;
        }

        /**
         * Installs a callback invoked when an outcome write is rejected (typically
         * {@link io.github.srjn45.rdq.worker.spi.StaleClaimException} after a lost
         * lease). For observability; the task is abandoned regardless.
         */
        public Builder abandonHook(BiConsumer<Envelope, Exception> hook) {
            this.abandonHook = hook;
            return this;
        }

        public Worker build() {
            if (specs.isEmpty()) {
                throw new IllegalStateException("worker: no queues configured");
            }
            return new Worker(this);
        }
    }

    // ---- internal state --------------------------------------------------------

    private static final class QueueState {
        final QueueSpec spec;
        final Semaphore semaphore;
        final Limiter limiter;

        QueueState(QueueSpec spec, Semaphore semaphore, Limiter limiter) {
            this.spec = spec;
            this.semaphore = semaphore;
            this.limiter = limiter;
        }
    }
}
