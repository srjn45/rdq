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
import java.util.function.DoubleSupplier;

/**
 * The resolved retry-backoff ladder for a queue (design 03 &sect;2). Mirrors Go
 * {@code core/policy.Backoff}: concrete values, decoupled from config pointers, so
 * {@link #delay} is a pure numeric function.
 *
 * <p>Formula: {@code delay(n) = min(initial × multiplier^(n−1), max) × (1 ± jitter·rand)}
 * where the cap is applied to the exponential term <em>before</em> jitter, exactly
 * as the Go reference implementation.
 *
 * <p>Thread-safe (immutable).
 */
public final class Backoff {

    private final long initialNanos;
    private final double multiplier;
    private final long maxNanos;
    private final double jitter;

    private Backoff(Builder b) {
        this.initialNanos = b.initial.toNanos();
        this.multiplier = b.multiplier;
        this.maxNanos = b.max.toNanos();
        this.jitter = b.jitter;
    }

    /**
     * Computes the wait before retry attempt {@code n} (1-based). When
     * {@code jitter == 0}, {@code rng} is never consulted and may be {@code null}.
     */
    public Duration delay(int n, DoubleSupplier rng) {
        long base = baseDelayNanos(n);
        if (jitter <= 0) {
            return Duration.ofNanos(base);
        }
        double factor = 1.0 + jitter * (2.0 * rng.getAsDouble() - 1.0);
        long d = (long) ((double) base * factor);
        if (d < 0) return Duration.ZERO;
        return Duration.ofNanos(d);
    }

    private long baseDelayNanos(int n) {
        if (n < 1) n = 1;
        double scaled = (double) initialNanos * Math.pow(multiplier, n - 1);
        if (maxNanos > 0 && scaled >= (double) maxNanos) {
            return maxNanos;
        }
        if (scaled >= (double) Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return (long) scaled;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder for {@link Backoff}. */
    public static final class Builder {

        private Duration initial = Duration.ofSeconds(1);
        private double multiplier = 1.0;
        private Duration max = Duration.ZERO; // 0 = uncapped
        private double jitter = 0.0;

        private Builder() {}

        /** Base delay before the first retry (n=1). Default: 1&nbsp;s. */
        public Builder initial(Duration initial) {
            this.initial = Objects.requireNonNull(initial, "initial");
            return this;
        }

        /**
         * Multiplier applied per attempt; 1.0 is linear (constant). Default: 1.0.
         * Config validation requires &ge;&nbsp;1.0.
         */
        public Builder multiplier(double multiplier) {
            this.multiplier = multiplier;
            return this;
        }

        /**
         * Cap on the pre-jitter delay. Zero (default) means uncapped.
         */
        public Builder max(Duration max) {
            this.max = Objects.requireNonNull(max, "max");
            return this;
        }

        /**
         * Symmetric spread as a fraction of the delay in [0, 1]: the result lands
         * in {@code base·[1−jitter, 1+jitter)}. Zero (default) disables jitter.
         */
        public Builder jitter(double jitter) {
            this.jitter = jitter;
            return this;
        }

        public Backoff build() {
            return new Backoff(this);
        }
    }
}
