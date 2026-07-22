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

import java.time.Duration;
import java.util.Objects;

/**
 * The resolved per-queue execution contract the worker runs against. Mirrors Go
 * {@code engine.QueueSpec}: concrete values (already merged and validated) turned
 * into engine inputs.
 *
 * <p>Build via {@link #builder(String)}. Immutable; safe to share.
 */
public final class QueueSpec {

    private final String queue;
    private final int maxAttempts;
    private final Backoff backoff;
    private final Classifier classifier;
    private final Duration lease;
    private final Duration handlerTimeout;
    private final boolean heartbeat;
    private final int batchSize;
    private final int concurrency;
    private final Duration pollInterval;
    private final double ratePerSecond;
    private final long rateBurst;
    private final VersionPolicy versionPolicy;
    private final Duration ttlSucceeded;

    private QueueSpec(Builder b) {
        this.queue = b.queue;
        this.maxAttempts = b.maxAttempts;
        this.backoff = b.backoff;
        this.classifier = b.classifier;
        this.lease = b.lease;
        this.handlerTimeout = b.handlerTimeout != null ? b.handlerTimeout : b.lease;
        this.heartbeat = b.heartbeat;
        this.batchSize = Math.max(1, b.batchSize);
        this.concurrency = Math.max(1, b.concurrency);
        this.pollInterval = b.pollInterval.toMillis() > 0 ? b.pollInterval : Duration.ofMillis(500);
        this.ratePerSecond = b.ratePerSecond;
        this.rateBurst = b.rateBurst;
        this.versionPolicy = b.versionPolicy;
        this.ttlSucceeded = b.ttlSucceeded;
    }

    public String queue() {
        return queue;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public Backoff backoff() {
        return backoff;
    }

    public Classifier classifier() {
        return classifier;
    }

    public Duration lease() {
        return lease;
    }

    public Duration handlerTimeout() {
        return handlerTimeout;
    }

    public boolean heartbeat() {
        return heartbeat;
    }

    public int batchSize() {
        return batchSize;
    }

    public int concurrency() {
        return concurrency;
    }

    public Duration pollInterval() {
        return pollInterval;
    }

    /** Per-second invocation rate cap. Zero means unlimited. */
    public double ratePerSecond() {
        return ratePerSecond;
    }

    /** Burst capacity for the token bucket. Meaningful only when {@link #ratePerSecond()} &gt; 0. */
    public long rateBurst() {
        return rateBurst;
    }

    public VersionPolicy versionPolicy() {
        return versionPolicy;
    }

    /** Retention window for SUCCEEDED tasks; null or non-positive disables sweeping. */
    public Duration ttlSucceeded() {
        return ttlSucceeded;
    }

    /** Returns a builder seeded with the given {@code queue} name and sensible defaults. */
    public static Builder builder(String queue) {
        return new Builder(queue);
    }

    /** Fluent builder for {@link QueueSpec}. */
    public static final class Builder {

        private final String queue;
        private int maxAttempts = 3;
        private Backoff backoff = Backoff.builder().initial(Duration.ofSeconds(1)).build();
        private Classifier classifier = Classifier.empty();
        private Duration lease = Duration.ofSeconds(30);
        private Duration handlerTimeout = null; // defaults to lease
        private boolean heartbeat = false;
        private int batchSize = 1;
        private int concurrency = 1;
        private Duration pollInterval = Duration.ofMillis(500);
        private double ratePerSecond = 0; // unlimited
        private long rateBurst = 1;
        private VersionPolicy versionPolicy = VersionPolicy.RUN_LATEST;
        private Duration ttlSucceeded = null; // no sweeping

        private Builder(String queue) {
            this.queue = Objects.requireNonNull(queue, "queue");
            if (queue.isEmpty()) throw new IllegalArgumentException("queue must not be empty");
        }

        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = Math.max(1, maxAttempts);
            return this;
        }

        public Builder backoff(Backoff backoff) {
            this.backoff = Objects.requireNonNull(backoff, "backoff");
            return this;
        }

        public Builder classifier(Classifier classifier) {
            this.classifier = Objects.requireNonNull(classifier, "classifier");
            return this;
        }

        public Builder lease(Duration lease) {
            this.lease = Objects.requireNonNull(lease, "lease");
            return this;
        }

        public Builder handlerTimeout(Duration handlerTimeout) {
            this.handlerTimeout = handlerTimeout;
            return this;
        }

        public Builder heartbeat(boolean heartbeat) {
            this.heartbeat = heartbeat;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder concurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        public Builder pollInterval(Duration pollInterval) {
            this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
            return this;
        }

        /** Sets a per-second rate cap. Zero (default) disables rate limiting. */
        public Builder rateLimit(double ratePerSecond, long burst) {
            this.ratePerSecond = ratePerSecond;
            this.rateBurst = burst;
            return this;
        }

        public Builder versionPolicy(VersionPolicy versionPolicy) {
            this.versionPolicy = Objects.requireNonNull(versionPolicy, "versionPolicy");
            return this;
        }

        public Builder ttlSucceeded(Duration ttlSucceeded) {
            this.ttlSucceeded = ttlSucceeded;
            return this;
        }

        public QueueSpec build() {
            Objects.requireNonNull(backoff, "backoff");
            Objects.requireNonNull(classifier, "classifier");
            if (lease == null || lease.toMillis() <= 0) {
                throw new IllegalArgumentException("queue \"" + queue + "\" has a non-positive lease");
            }
            return new QueueSpec(this);
        }
    }
}
