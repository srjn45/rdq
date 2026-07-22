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

package io.github.srjn45.rdq.worker.postgres;

import io.github.srjn45.rdq.worker.spi.StaleCursorException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Unit tests for the opaque DLQ pagination cursor codec (design 02 &sect;3
 * invariant 8). No database needed &mdash; the codec is pure, so a foreign or
 * corrupt cursor must be rejected with {@link StaleCursorException} rather than
 * silently mis-paging.
 */
class DlqCursorTest {

    @Test
    void roundTripsPositionExactlyIncludingSubSecond() {
        Instant at = Instant.parse("2026-07-20T14:03:22.117384Z"); // microsecond precision
        String encoded = DlqCursor.encode(at, "01J2ZK7Q8XW5H3N9G4T6B8RDQ0");

        DlqCursor decoded = DlqCursor.decode(encoded);
        assertThat(decoded).isNotNull();
        assertThat(decoded.deadLetteredAt()).isEqualTo(at);
        assertThat(decoded.id()).isEqualTo("01J2ZK7Q8XW5H3N9G4T6B8RDQ0");
    }

    @Test
    void emptyCursorStartsFromBeginning() {
        assertThat(DlqCursor.decode("")).isNull();
        assertThat(DlqCursor.decode(null)).isNull();
    }

    @Test
    void rejectsNonBase64() {
        assertThatExceptionOfType(StaleCursorException.class)
            .isThrownBy(() -> DlqCursor.decode("not base64 !!!"));
    }

    @Test
    void rejectsForeignPrefix() {
        String foreign = Base64.getUrlEncoder().withoutPadding()
            .encodeToString("other1:123:id".getBytes(StandardCharsets.UTF_8));
        assertThatExceptionOfType(StaleCursorException.class)
            .isThrownBy(() -> DlqCursor.decode(foreign));
    }

    @Test
    void rejectsMissingSeparator() {
        String malformed = Base64.getUrlEncoder().withoutPadding()
            .encodeToString("pgdlq1:1234567890".getBytes(StandardCharsets.UTF_8));
        assertThatExceptionOfType(StaleCursorException.class)
            .isThrownBy(() -> DlqCursor.decode(malformed));
    }

    @Test
    void rejectsNonNumericTimestamp() {
        String malformed = Base64.getUrlEncoder().withoutPadding()
            .encodeToString("pgdlq1:notanumber:id".getBytes(StandardCharsets.UTF_8));
        assertThatExceptionOfType(StaleCursorException.class)
            .isThrownBy(() -> DlqCursor.decode(malformed));
    }
}
