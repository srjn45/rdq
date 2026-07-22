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

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cross-language contract test: the Java codec must read AND write the
 * FROZEN T1.2 fixtures under {@code core/envelope/testdata/} byte-for-byte on
 * the canonical fields, and preserve unknown fields across a round-trip
 * (design 01 §5). These fixtures are frozen — the tests only read them.
 */
class EnvelopeFixturesTest {

    /**
     * Every fixture, built from scratch, must encode to exactly the frozen
     * bytes — this pins the write path independently of the read path (the Go
     * {@code TestGoldenFixtures} equivalent).
     */
    @TestFactory
    Stream<DynamicTest> encodesFromScratchToFrozenBytes() {
        return goldenFixtures().entrySet().stream().map(entry ->
            DynamicTest.dynamicTest(entry.getKey(), () -> {
                byte[] want = readFixture(entry.getKey());
                byte[] got = EnvelopeCodec.encode(entry.getValue());
                assertThat(new String(got, StandardCharsets.UTF_8))
                    .isEqualTo(new String(want, StandardCharsets.UTF_8));
            }));
    }

    /**
     * read(write(x)) == x, byte-stable: decode each frozen fixture and re-encode
     * to identical bytes.
     */
    @TestFactory
    Stream<DynamicTest> roundTripsFixturesByteStably() {
        return goldenFixtures().keySet().stream().map(name ->
            DynamicTest.dynamicTest(name, () -> {
                byte[] want = readFixture(name);
                Envelope decoded = EnvelopeCodec.decode(want);
                byte[] round = EnvelopeCodec.encode(decoded);
                assertThat(new String(round, StandardCharsets.UTF_8))
                    .isEqualTo(new String(want, StandardCharsets.UTF_8));
            }));
    }

    @Test
    void decodesKnownFieldsOfTheReferenceEnvelope() {
        Envelope e = EnvelopeCodec.decode(readFixture("envelope_full.json"));

        assertThat(e.envelopeVersion()).isEqualTo(1);
        assertThat(e.id()).isEqualTo("01J2ZK7Q8XW5H3N9G4T6B8RDQ0");
        assertThat(e.queue()).isEqualTo("payments.charge");
        assertThat(e.handlerRef()).isEqualTo("charge-payment");
        assertThat(e.handlerVersion()).isEqualTo("v3");
        assertThat(new String(e.payload(), StandardCharsets.UTF_8)).isEqualTo("{\"order_id\": 42}");
        assertThat(e.payloadContentType()).isEqualTo("application/json");
        assertThat(e.payloadRef()).isNull();
        assertThat(e.headers()).containsEntry("traceparent", "00-4bf9...-01")
            .containsEntry("rdq.source", "kafka://payments/3/42351")
            .containsEntry("rdq.submitted_by", "checkout-service");
        assertThat(e.status()).isEqualTo(Status.PENDING);
        assertThat(e.attemptCount()).isEqualTo(2);
        assertThat(e.redriveCount()).isZero();
        assertThat(e.nextAttemptAt()).isEqualTo(Instant.parse("2026-07-20T14:05:22.117Z"));
        assertThat(e.leaseExpiresAt()).isNull();
        assertThat(e.createdAt()).isEqualTo(Instant.parse("2026-07-20T14:03:22.117Z"));
        assertThat(e.unknownFields()).isEmpty();

        assertThat(e.attempts()).hasSize(1);
        Attempt a = e.attempts().get(0);
        assertThat(a.attemptNo()).isEqualTo(1);
        assertThat(a.startedAt()).isEqualTo(Instant.parse("2026-07-20T14:03:22.200Z"));
        assertThat(a.finishedAt()).isEqualTo(Instant.parse("2026-07-20T14:03:22.950Z"));
        assertThat(a.outcome()).isEqualTo(Outcome.RETRYABLE_FAILURE);
        assertThat(a.unknownFields()).isEmpty();

        ErrorInfo err = a.error();
        assertThat(err.type()).isEqualTo("java.net.SocketTimeoutException");
        assertThat(err.message()).isEqualTo("connect timed out after 500ms");
        assertThat(err.detail()).isNull();
        assertThat(err.stack()).startsWith("java.net.SocketTimeoutException: connect timed out");
    }

    @Test
    void preservesUnknownFieldsTopLevelAndPerAttempt() {
        Envelope e = EnvelopeCodec.decode(readFixture("unknown_fields.json"));

        // Top-level unknown fields captured, values verbatim; known keys never leak.
        assertThat(e.unknownFields()).containsOnlyKeys("future_priority", "x_experimental");
        assertThat(e.unknownFields().get("future_priority").asInt()).isEqualTo(7);
        assertThat(e.unknownFields().get("x_experimental").get("canary").asBoolean()).isTrue();
        assertThat(e.unknownFields()).doesNotContainKey("attempts");

        // Per-attempt unknown fields captured on the right attempt.
        assertThat(e.attempts()).hasSize(1);
        Map<String, JsonNode> attemptUnknown = e.attempts().get(0).unknownFields();
        assertThat(attemptUnknown).containsOnlyKeys("future_latency_ms", "trace_flags");
        assertThat(attemptUnknown.get("future_latency_ms").asInt()).isEqualTo(142);
        assertThat(attemptUnknown.get("trace_flags").asText()).isEqualTo("01");
    }

    @Test
    void decodesStructuredErrorDetail() {
        Envelope e = EnvelopeCodec.decode(readFixture("error_type_go.json"));

        ErrorInfo first = e.attempts().get(0).error();
        assertThat(first.type()).isEqualTo("billing.CardDeclined");
        assertThat(first.detail().get("decline_code").asText()).isEqualTo("insufficient_funds");
        assertThat(e.status()).isEqualTo(Status.DEAD);
        assertThat(e.redriveCount()).isEqualTo(1);
        assertThat(e.nextAttemptAt()).isNull();
    }

    @Test
    void decodesLeaseExpiredAttemptWithNoFinishedAt() {
        Envelope e = EnvelopeCodec.decode(readFixture("lease_expired.json"));

        Attempt lease = e.attempts().get(1);
        assertThat(lease.outcome()).isEqualTo(Outcome.LEASE_EXPIRED);
        assertThat(lease.finishedAt()).isNull();
        assertThat(lease.error().type()).isEqualTo("rdq.LeaseExpired");
        assertThat(lease.error().stack()).isNull();
    }

    // --- the frozen fixtures, reconstructed in Java (mirrors Go goldenFixtures) ---

    private static Map<String, Envelope> goldenFixtures() {
        return Map.of(
            "envelope_full.json", Envelope.builder()
                .envelopeVersion(1)
                .id("01J2ZK7Q8XW5H3N9G4T6B8RDQ0")
                .queue("payments.charge")
                .handlerRef("charge-payment")
                .handlerVersion("v3")
                .payload("{\"order_id\": 42}".getBytes(StandardCharsets.UTF_8))
                .payloadContentType("application/json")
                .headers(Map.of(
                    "traceparent", "00-4bf9...-01",
                    "rdq.source", "kafka://payments/3/42351",
                    "rdq.submitted_by", "checkout-service"))
                .status(Status.PENDING)
                .attemptCount(2)
                .redriveCount(0)
                .nextAttemptAt(Instant.parse("2026-07-20T14:05:22.117Z"))
                .createdAt(Instant.parse("2026-07-20T14:03:22.117Z"))
                .attempts(List.of(Attempt.builder()
                    .attemptNo(1)
                    .startedAt(Instant.parse("2026-07-20T14:03:22.200Z"))
                    .finishedAt(Instant.parse("2026-07-20T14:03:22.950Z"))
                    .outcome(Outcome.RETRYABLE_FAILURE)
                    .error(ErrorInfo.builder()
                        .type("java.net.SocketTimeoutException")
                        .message("connect timed out after 500ms")
                        .stack("java.net.SocketTimeoutException: connect timed out\n\tat ...")
                        .build())
                    .build()))
                .build(),

            "lease_expired.json", Envelope.builder()
                .envelopeVersion(1)
                .id("01J2ZKQP0T4S6V8X0Z2B4D6F8H")
                .queue("notifications.send")
                .handlerRef("send-email")
                .payload("payload-bytes".getBytes(StandardCharsets.UTF_8))
                .payloadContentType("application/octet-stream")
                .headers(Map.of("rdq.submitted_by", "notify-service"))
                .status(Status.PENDING)
                .attemptCount(2)
                .redriveCount(0)
                .nextAttemptAt(Instant.parse("2026-07-20T14:12:22.117Z"))
                .createdAt(Instant.parse("2026-07-20T14:03:22.117Z"))
                .attempts(List.of(
                    Attempt.builder()
                        .attemptNo(1)
                        .startedAt(Instant.parse("2026-07-20T14:03:22.200Z"))
                        .finishedAt(Instant.parse("2026-07-20T14:03:22.950Z"))
                        .outcome(Outcome.RETRYABLE_FAILURE)
                        .error(ErrorInfo.builder()
                            .type("*net.OpError")
                            .message("dial tcp 10.0.0.5:25: i/o timeout")
                            .build())
                        .build(),
                    Attempt.builder()
                        .attemptNo(2)
                        .startedAt(Instant.parse("2026-07-20T14:05:22.200Z"))
                        .outcome(Outcome.LEASE_EXPIRED)
                        .error(ErrorInfo.builder()
                            .type("rdq.LeaseExpired")
                            .message("lease expired at 2026-07-20T14:07:22.117Z")
                            .build())
                        .build()))
                .build(),

            "error_type_go.json", Envelope.builder()
                .envelopeVersion(1)
                .id("01J2ZM0000000000000000000A")
                .queue("billing.invoice")
                .handlerRef("issue-invoice")
                .payload("{}".getBytes(StandardCharsets.UTF_8))
                .payloadContentType("application/json")
                .status(Status.DEAD)
                .attemptCount(2)
                .redriveCount(1)
                .createdAt(Instant.parse("2026-07-20T14:00:00.000Z"))
                .attempts(List.of(
                    Attempt.builder()
                        .attemptNo(1)
                        .startedAt(Instant.parse("2026-07-20T14:00:01.000Z"))
                        .finishedAt(Instant.parse("2026-07-20T14:00:01.500Z"))
                        .outcome(Outcome.RETRYABLE_FAILURE)
                        .error(ErrorInfo.builder()
                            .type("billing.CardDeclined")
                            .message("card declined")
                            .detail(node("{\"decline_code\":\"insufficient_funds\"}"))
                            .build())
                        .build(),
                    Attempt.builder()
                        .attemptNo(2)
                        .startedAt(Instant.parse("2026-07-20T14:01:00.000Z"))
                        .finishedAt(Instant.parse("2026-07-20T14:01:00.250Z"))
                        .outcome(Outcome.PERMANENT_FAILURE)
                        .error(ErrorInfo.builder()
                            .type("*errors.errorString")
                            .message("invoice already finalized")
                            .build())
                        .build()))
                .build(),

            "unknown_fields.json", Envelope.builder()
                .envelopeVersion(1)
                .id("01J2ZN0000000000000000000B")
                .queue("orders.reserve")
                .handlerRef("reserve-stock")
                .payload("{}".getBytes(StandardCharsets.UTF_8))
                .payloadContentType("application/json")
                .status(Status.PENDING)
                .attemptCount(1)
                .redriveCount(0)
                .nextAttemptAt(Instant.parse("2026-07-20T15:00:00.000Z"))
                .createdAt(Instant.parse("2026-07-20T14:30:00.000Z"))
                .unknownField("future_priority", node("7"))
                .unknownField("x_experimental", node("{\"canary\":true}"))
                .attempts(List.of(Attempt.builder()
                    .attemptNo(1)
                    .startedAt(Instant.parse("2026-07-20T14:30:01.000Z"))
                    .finishedAt(Instant.parse("2026-07-20T14:30:01.250Z"))
                    .outcome(Outcome.RETRYABLE_FAILURE)
                    .error(ErrorInfo.builder()
                        .type("*errors.errorString")
                        .message("insufficient stock")
                        .build())
                    .unknownField("future_latency_ms", node("142"))
                    .unknownField("trace_flags", node("\"01\""))
                    .build()))
                .build());
    }

    private static JsonNode node(String json) {
        try {
            return EnvelopeCodec.mapper().readTree(json);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static byte[] readFixture(String name) {
        try {
            return Files.readAllBytes(fixturesDir().resolve(name));
        } catch (IOException ex) {
            throw new UncheckedIOException("reading fixture " + name, ex);
        }
    }

    /** Walks up from the test working directory to the frozen fixtures directory. */
    private static Path fixturesDir() {
        Path dir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("core/envelope/testdata");
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException(
            "could not locate core/envelope/testdata from " + System.getProperty("user.dir"));
    }
}
