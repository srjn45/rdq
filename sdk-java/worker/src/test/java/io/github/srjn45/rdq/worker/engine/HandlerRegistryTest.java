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
import io.github.srjn45.rdq.client.envelope.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/** Unit tests for {@link HandlerRegistry} routing logic. */
class HandlerRegistryTest {

    private HandlerRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new HandlerRegistry();
    }

    // ---- registration ------------------------------------------------------

    @Test
    void register_lookup_roundTrip() {
        registry.register("test.handler", noopHandler("v1"));
        assertThat(registry.lookup("test.handler")).isNotNull();
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void register_emptyRef_throws() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> registry.register("", noopHandler("v1")));
    }

    @Test
    void register_nullHandler_throws() {
        assertThatNullPointerException()
            .isThrownBy(() -> registry.register("ref", null));
    }

    @Test
    void register_duplicate_throws() {
        registry.register("ref", noopHandler("v1"));
        assertThatIllegalArgumentException()
            .isThrownBy(() -> registry.register("ref", noopHandler("v2")));
    }

    @Test
    void lookup_unknown_returnsNull() {
        assertThat(registry.lookup("unknown")).isNull();
    }

    // ---- resolve: unroutable -----------------------------------------------

    @Test
    void resolve_unroutable_deadLetters() {
        Envelope task = taskWithRef("no.such.handler", "");
        HandlerRegistry.Resolution res = registry.resolve(task, VersionPolicy.RUN_LATEST);
        assertThat(res.action()).isEqualTo(HandlerRegistry.Action.DEAD_LETTER);
        assertThat(res.errorType()).isEqualTo(HandlerRegistry.ERROR_TYPE_UNROUTABLE);
        assertThat(res.handler()).isNull();
    }

    // ---- resolve: version matching -----------------------------------------

    @Test
    void resolve_noPinnedVersion_alwaysRuns() {
        registry.register("h", noopHandler("v1"));
        Envelope task = taskWithRef("h", ""); // no pin
        HandlerRegistry.Resolution res = registry.resolve(task, VersionPolicy.DEAD_LETTER);
        assertThat(res.action()).isEqualTo(HandlerRegistry.Action.RUN);
        assertThat(res.handler()).isNotNull();
    }

    @Test
    void resolve_pinMatchesRegistered_alwaysRuns() {
        registry.register("h", noopHandler("v2"));
        HandlerRegistry.Resolution res = registry.resolve(taskWithRef("h", "v2"), VersionPolicy.DEAD_LETTER);
        assertThat(res.action()).isEqualTo(HandlerRegistry.Action.RUN);
    }

    @Test
    void resolve_pinMismatch_runLatest_runsAnyway() {
        registry.register("h", noopHandler("v2"));
        HandlerRegistry.Resolution res = registry.resolve(taskWithRef("h", "v1"), VersionPolicy.RUN_LATEST);
        assertThat(res.action()).isEqualTo(HandlerRegistry.Action.RUN);
    }

    @Test
    void resolve_pinMismatch_deadLetterPolicy_deadLetters() {
        registry.register("h", noopHandler("v2"));
        HandlerRegistry.Resolution res = registry.resolve(taskWithRef("h", "v1"), VersionPolicy.DEAD_LETTER);
        assertThat(res.action()).isEqualTo(HandlerRegistry.Action.DEAD_LETTER);
        assertThat(res.errorType()).isEqualTo(HandlerRegistry.ERROR_TYPE_VERSION_MISMATCH);
    }

    // ---- VersionPolicy.from ------------------------------------------------

    @Test
    void versionPolicyFrom_deadLetterString() {
        assertThat(VersionPolicy.from("dead-letter")).isEqualTo(VersionPolicy.DEAD_LETTER);
    }

    @Test
    void versionPolicyFrom_unknownDefaultsToRunLatest() {
        assertThat(VersionPolicy.from("run-latest")).isEqualTo(VersionPolicy.RUN_LATEST);
        assertThat(VersionPolicy.from("")).isEqualTo(VersionPolicy.RUN_LATEST);
        assertThat(VersionPolicy.from(null)).isEqualTo(VersionPolicy.RUN_LATEST);
    }

    // ---- helpers -----------------------------------------------------------

    private static Handler noopHandler(String version) {
        return new Handler() {
            @Override
            public String version() {
                return version;
            }

            @Override
            public void handle(Envelope task) {}
        };
    }

    private static Envelope taskWithRef(String handlerRef, String handlerVersion) {
        return Envelope.builder()
            .envelopeVersion(1)
            .id("t1")
            .queue("q")
            .handlerRef(handlerRef)
            .handlerVersion(handlerVersion)
            .status(Status.IN_FLIGHT)
            .createdAt(Instant.now())
            .build();
    }
}
