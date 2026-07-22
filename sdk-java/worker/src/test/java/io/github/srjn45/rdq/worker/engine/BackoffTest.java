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

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link Backoff} — mirrors Go {@code policy_test.go} semantics. */
class BackoffTest {

    @Test
    void linear_noMultiplier_noJitter() {
        Backoff b = Backoff.builder()
            .initial(Duration.ofSeconds(1))
            .multiplier(1.0)
            .build();

        // delay(n) = initial for all n when multiplier=1
        assertThat(b.delay(1, null)).isEqualTo(Duration.ofSeconds(1));
        assertThat(b.delay(2, null)).isEqualTo(Duration.ofSeconds(1));
        assertThat(b.delay(5, null)).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void exponential_doublesEachAttempt() {
        Backoff b = Backoff.builder()
            .initial(Duration.ofSeconds(1))
            .multiplier(2.0)
            .build();

        assertThat(b.delay(1, null)).isEqualTo(Duration.ofSeconds(1));
        assertThat(b.delay(2, null)).isEqualTo(Duration.ofSeconds(2));
        assertThat(b.delay(3, null)).isEqualTo(Duration.ofSeconds(4));
        assertThat(b.delay(4, null)).isEqualTo(Duration.ofSeconds(8));
    }

    @Test
    void max_capsPreJitterDelay() {
        Backoff b = Backoff.builder()
            .initial(Duration.ofSeconds(1))
            .multiplier(2.0)
            .max(Duration.ofSeconds(5))
            .build();

        assertThat(b.delay(1, null)).isEqualTo(Duration.ofSeconds(1));
        assertThat(b.delay(2, null)).isEqualTo(Duration.ofSeconds(2));
        assertThat(b.delay(3, null)).isEqualTo(Duration.ofSeconds(4));
        assertThat(b.delay(4, null)).isEqualTo(Duration.ofSeconds(5)); // capped
        assertThat(b.delay(10, null)).isEqualTo(Duration.ofSeconds(5)); // still capped
    }

    @Test
    void jitter_variesAroundBase() {
        Backoff b = Backoff.builder()
            .initial(Duration.ofSeconds(10))
            .multiplier(1.0)
            .jitter(0.5)
            .build();

        // rng=0 → factor = 1 + 0.5*(0-1) = 0.5 → delay = 5s
        Duration low = b.delay(1, () -> 0.0);
        assertThat(low).isEqualTo(Duration.ofSeconds(5));

        // rng≈1 → factor ≈ 1.5 → delay ≈ 15s
        Duration high = b.delay(1, () -> 0.999999);
        assertThat(high.toSeconds()).isGreaterThanOrEqualTo(14);
    }

    @Test
    void nBelow1_clampsTo1() {
        Backoff b = Backoff.builder().initial(Duration.ofSeconds(3)).build();
        assertThat(b.delay(0, null)).isEqualTo(b.delay(1, null));
        assertThat(b.delay(-5, null)).isEqualTo(b.delay(1, null));
    }

    @Test
    void jitter0_rngNeverCalled() {
        Backoff b = Backoff.builder()
            .initial(Duration.ofSeconds(2))
            .jitter(0.0)
            .build();
        // passing null rng — if jitter=0 the rng must never be called
        Duration d = b.delay(1, null);
        assertThat(d).isEqualTo(Duration.ofSeconds(2));
    }

    @Test
    void errorType_unwrapsToRootCause() {
        RuntimeException outer = new RuntimeException("outer",
            new IllegalStateException("inner"));
        assertThat(Classifier.errorType(outer)).isEqualTo("java.lang.IllegalStateException");
    }

    @Test
    void errorType_nullReturnsEmpty() {
        assertThat(Classifier.errorType(null)).isEmpty();
    }

    @Test
    void errorType_noWrapping_returnsClassName() {
        assertThat(Classifier.errorType(new IllegalArgumentException("x")))
            .isEqualTo("java.lang.IllegalArgumentException");
    }
}
