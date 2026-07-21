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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the token-bucket {@link Limiter}. */
class LimiterTest {

    @Test
    void unlimited_alwaysAllows() {
        Limiter lim = Limiter.unlimited();
        assertThat(lim.isUnlimited()).isTrue();
        for (int i = 0; i < 1000; i++) {
            assertThat(lim.allow()).isTrue();
        }
    }

    @Test
    void unlimited_allowN_alwaysAllows() {
        Limiter lim = Limiter.unlimited();
        assertThat(lim.allowN(100)).isTrue();
    }

    @Test
    void unlimited_allowN_zero_alwaysAllows() {
        Limiter lim = Limiter.unlimited();
        assertThat(lim.allowN(0)).isTrue();
    }

    @Test
    void rateLimited_startsFull_bursts() {
        // 1 token/second, burst=5; starts full → first 5 calls succeed
        AtomicLong fakeNanos = new AtomicLong(0);
        Limiter lim = Limiter.withRate(1.0, 5, fakeNanos::get);

        for (int i = 0; i < 5; i++) {
            assertThat(lim.allow()).as("call %d", i).isTrue();
        }
        // Bucket empty
        assertThat(lim.allow()).isFalse();
    }

    @Test
    void rateLimited_refillsOverTime() {
        AtomicLong fakeNanos = new AtomicLong(0);
        Limiter lim = Limiter.withRate(2.0, 2, fakeNanos::get); // 2/s, burst=2

        // Drain the bucket
        assertThat(lim.allow()).isTrue();
        assertThat(lim.allow()).isTrue();
        assertThat(lim.allow()).isFalse();

        // Advance 1 second: 2 new tokens credited
        fakeNanos.set(1_000_000_000L);
        assertThat(lim.allow()).isTrue();
        assertThat(lim.allow()).isTrue();
        assertThat(lim.allow()).isFalse();
    }

    @Test
    void rateLimited_allowN_allOrNothing() {
        AtomicLong fakeNanos = new AtomicLong(0);
        Limiter lim = Limiter.withRate(1.0, 3, fakeNanos::get); // burst=3, starts full

        // Need 2 — succeeds (3 available)
        assertThat(lim.allowN(2)).isTrue();
        // Need 2 — fails (only 1 available)
        assertThat(lim.allowN(2)).isFalse();
        // Need 1 — succeeds
        assertThat(lim.allowN(1)).isTrue();
    }

    @Test
    void rateLimited_allowN_zero_alwaysTrue() {
        AtomicLong fakeNanos = new AtomicLong(0);
        Limiter lim = Limiter.withRate(0.001, 1, fakeNanos::get); // nearly empty rate
        // Drain
        assertThat(lim.allow()).isTrue();
        // allowN(0) must succeed even when empty
        assertThat(lim.allowN(0)).isTrue();
    }

    @Test
    void rateLimited_nonMonotonicClock_doesNotCredit() {
        AtomicLong fakeNanos = new AtomicLong(1_000_000_000L); // start at 1s
        Limiter lim = Limiter.withRate(1.0, 1, fakeNanos::get);

        assertThat(lim.allow()).isTrue(); // drain
        assertThat(lim.allow()).isFalse();

        // Rewind clock: no tokens credited
        fakeNanos.set(0L);
        assertThat(lim.allow()).isFalse();
    }
}
