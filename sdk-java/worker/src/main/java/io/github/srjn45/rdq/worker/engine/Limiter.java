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

import java.util.function.LongSupplier;

/**
 * Per-queue token-bucket rate limiter (design 03 &sect;2, G12). Mirrors Go
 * {@code engine.Limiter}.
 *
 * <p>The bucket refills continuously at the configured rate and holds at most the
 * burst capacity. A limiter with no rate (zero) is unlimited: every
 * {@link #allow()} succeeds. Thread-safe.
 */
final class Limiter {

    private final double ratePerSec;
    private final double burst;
    private final LongSupplier nanosClock;

    private double tokens;
    private long lastNanos;

    /** Creates an unlimited limiter (every {@link #allow()} returns true). */
    static Limiter unlimited() {
        return new Limiter(0, 0, System::nanoTime);
    }

    /**
     * Creates a rate-limited limiter. {@code ratePerSec} &le;&nbsp;0 is treated as
     * unlimited. The bucket starts full.
     *
     * @param ratePerSec tokens per second sustained rate
     * @param burstCount maximum tokens that can accrue while idle (&ge;&nbsp;1)
     * @param nanosClock injectable time source in nanoseconds (use
     *                   {@code System::nanoTime} in production)
     */
    static Limiter withRate(double ratePerSec, long burstCount, LongSupplier nanosClock) {
        return new Limiter(ratePerSec, burstCount, nanosClock);
    }

    private Limiter(double ratePerSec, long burstCount, LongSupplier nanosClock) {
        this.nanosClock = nanosClock;
        this.lastNanos = nanosClock.getAsLong();
        if (ratePerSec <= 0) {
            this.ratePerSec = 0;
            this.burst = 0;
            this.tokens = 0;
        } else {
            this.ratePerSec = ratePerSec;
            this.burst = Math.max(1, (double) burstCount);
            this.tokens = this.burst; // start full
        }
    }

    boolean isUnlimited() {
        return ratePerSec == 0;
    }

    /** Reports whether one invocation may proceed, consuming a token if so. */
    boolean allow() {
        return allowN(1);
    }

    /**
     * Reports whether {@code n} invocations may proceed, consuming {@code n} tokens
     * if so (all-or-nothing). {@code n <= 0} always returns true.
     */
    synchronized boolean allowN(int n) {
        if (n <= 0) return true;
        if (ratePerSec == 0) return true;
        refill();
        double need = n;
        if (tokens < need) return false;
        tokens -= need;
        return true;
    }

    private void refill() {
        long now = nanosClock.getAsLong();
        long elapsed = now - lastNanos;
        if (elapsed <= 0) return;
        lastNanos = now;
        tokens = Math.min(burst, tokens + (elapsed / 1_000_000_000.0) * ratePerSec);
    }
}
