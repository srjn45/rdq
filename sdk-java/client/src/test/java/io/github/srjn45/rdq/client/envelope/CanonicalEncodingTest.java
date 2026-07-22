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

package io.github.srjn45.rdq.client.envelope;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit coverage for the canonical encoding rules (design 01 §1). */
class CanonicalEncodingTest {

    @Test
    void timestampsAlwaysRenderThreeFractionalDigitsInUtc() {
        String json = EnvelopeCodec.encodeToString(minimal()
            .createdAt(Instant.parse("2026-07-20T14:03:22.117Z"))
            .nextAttemptAt(Instant.parse("2026-07-20T14:03:22.200Z")) // trailing zeros kept
            .leaseExpiresAt(Instant.parse("2026-07-20T14:03:22.000Z")) // always three digits
            .build());

        assertThat(json).contains("\"created_at\":\"2026-07-20T14:03:22.117Z\"");
        assertThat(json).contains("\"next_attempt_at\":\"2026-07-20T14:03:22.200Z\"");
        assertThat(json).contains("\"lease_expires_at\":\"2026-07-20T14:03:22.000Z\"");
    }

    @Test
    void nonUtcTimestampsAreNormalizedToUtc() {
        // 16:03 at +02:00 is 14:03 UTC.
        Instant plusTwo = OffsetDateTime.of(2026, 7, 20, 16, 3, 22, 117_000_000,
            ZoneOffset.ofHours(2)).toInstant();
        String json = EnvelopeCodec.encodeToString(minimal().createdAt(plusTwo).build());
        assertThat(json).contains("\"created_at\":\"2026-07-20T14:03:22.117Z\"");
    }

    @Test
    void nullableTimestampsAreEmittedAsNullNotOmitted() {
        // next_attempt_at and lease_expires_at are always present, even unset.
        String json = EnvelopeCodec.encodeToString(minimal().build());
        assertThat(json).contains("\"next_attempt_at\":null");
        assertThat(json).contains("\"lease_expires_at\":null");
    }

    @Test
    void nullTimestampsDecodeBackToNull() {
        Envelope e = EnvelopeCodec.decode(EnvelopeCodec.encode(minimal().build()));
        assertThat(e.nextAttemptAt()).isNull();
        assertThat(e.leaseExpiresAt()).isNull();
        assertThat(e.createdAt()).isEqualTo(Instant.parse("2026-07-20T14:00:00.000Z"));
    }

    @Test
    void emptyOptionalFieldsAreOmitted() {
        // No handler_version, headers, attempts, payload_ref → all omitted.
        String json = EnvelopeCodec.encodeToString(minimal().build());
        assertThat(json).doesNotContain("handler_version");
        assertThat(json).doesNotContain("headers");
        assertThat(json).doesNotContain("attempts");
        assertThat(json).doesNotContain("payload_ref");
    }

    @Test
    void statusRoundTripsThroughItsWireToken() {
        for (Status s : Status.values()) {
            assertThat(Status.fromWire(s.wire())).isEqualTo(s);
        }
        assertThat(Status.PENDING.wire()).isEqualTo("PENDING");
        assertThatThrownBy(() -> Status.fromWire("NOPE"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("invalid status");
    }

    @Test
    void outcomeRoundTripsThroughItsWireToken() {
        for (Outcome o : Outcome.values()) {
            assertThat(Outcome.fromWire(o.wire())).isEqualTo(o);
        }
        assertThat(Outcome.LEASE_EXPIRED.wire()).isEqualTo("LEASE_EXPIRED");
        assertThatThrownBy(() -> Outcome.fromWire("NOPE"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("invalid outcome");
    }

    @Test
    void decodeRejectsMalformedJson() {
        assertThatThrownBy(() -> EnvelopeCodec.decode("{not json"))
            .isInstanceOf(RuntimeException.class);
    }

    @Test
    void payloadIsBase64AndDefensivelyCopied() {
        byte[] original = {1, 2, 3};
        Envelope e = minimal().payload(original).build();
        original[0] = 9; // mutating the caller's array must not affect the envelope

        String json = EnvelopeCodec.encodeToString(e);
        assertThat(json).contains("\"payload\":\"AQID\""); // base64 of {1,2,3}

        byte[] got = e.payload();
        got[0] = 7; // mutating the returned array must not affect the envelope
        assertThat(e.payload()[0]).isEqualTo((byte) 1);
    }

    @Test
    void builderCopiesHeadersAndAttempts() {
        Envelope e = minimal()
            .headers(new java.util.HashMap<>(java.util.Map.of("a", "1")))
            .attempts(List.of(Attempt.builder().attemptNo(1).outcome(Outcome.SUCCESS).build()))
            .build();
        assertThat(e.headers()).containsEntry("a", "1");
        assertThat(e.attempts()).hasSize(1);
        assertThat(e.attempts().get(0).attemptNo()).isEqualTo(1);
    }

    private static Envelope.Builder minimal() {
        return Envelope.builder()
            .envelopeVersion(1)
            .id("01J2ZK7Q8XW5H3N9G4T6B8RDQ0")
            .queue("q")
            .handlerRef("h")
            .payload(new byte[0])
            .payloadContentType("application/json")
            .status(Status.PENDING)
            .attemptCount(0)
            .redriveCount(0)
            .createdAt(Instant.parse("2026-07-20T14:00:00.000Z"));
    }
}
