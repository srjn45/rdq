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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.srjn45.rdq.client.envelope.Attempt;
import io.github.srjn45.rdq.client.envelope.Envelope;
import io.github.srjn45.rdq.client.envelope.ErrorInfo;
import io.github.srjn45.rdq.client.envelope.Outcome;
import io.github.srjn45.rdq.client.envelope.Status;
import io.github.srjn45.rdq.worker.spi.StorageException;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The envelope&nbsp;&harr;&nbsp;rows mapping (design 02 &sect;4, mirrors the Go
 * T2.2 {@code mapping.go}). The wire {@link Envelope} decomposes losslessly into
 * the columns of {@code rdq_task} / {@code rdq_dlq_task} plus a row per attempt
 * in {@code rdq_attempt}; unknown top-level and per-attempt JSON fields ride
 * along in the {@code residual} JSONB columns so a task written by a newer
 * {@code envelope_version} round-trips through an older engine (design 01
 * &sect;5).
 *
 * <p>Pure column projection: no SQL, no clock, no fencing. Timestamps bind and
 * read as {@link OffsetDateTime} at UTC so {@code timestamptz} values keep their
 * instant exactly; JSONB text is parsed with a plain Jackson mapper (whitespace
 * and key order from the database are insignificant &mdash; the canonical
 * {@code EnvelopeCodec} re-sorts and re-compacts on the wire).
 */
final class RowMapper {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> STRING_MAP =
        new TypeReference<>() {
        };
    private static final TypeReference<Map<String, JsonNode>> NODE_MAP =
        new TypeReference<>() {
        };

    /** The shared envelope-derived column list of {@code rdq_task}/{@code rdq_dlq_task}. */
    static final String TASK_COLUMNS =
        "id, queue, envelope_version, handler_ref, handler_version, payload, "
            + "payload_content_type, payload_ref, headers, status, attempt_count, "
            + "redrive_count, next_attempt_at, lease_expires_at, created_at, residual";

    private RowMapper() {
    }

    // --- timestamps ---

    /** Binds an {@link Instant} as a UTC {@link OffsetDateTime}, or null. */
    static OffsetDateTime toOffset(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }

    /** Reads a nullable {@code timestamptz} column as a UTC {@link Instant}. */
    static Instant readInstant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime odt = rs.getObject(column, OffsetDateTime.class);
        return odt == null ? null : odt.toInstant();
    }

    // --- JSONB ---

    /**
     * Marshals a string map to a JSONB object payload, returning {@code "{}"} for
     * an empty/absent map so the NOT NULL {@code headers} column is satisfiable.
     */
    static String encodeStringMap(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return "{}";
        }
        return write(map);
    }

    /** Parses a JSONB object payload into a string map, or null when empty. */
    static Map<String, String> decodeStringMap(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            Map<String, String> m = MAPPER.readValue(json, STRING_MAP);
            return m.isEmpty() ? null : m;
        } catch (IOException ex) {
            throw new StorageException("rdq/postgres: decoding headers", ex);
        }
    }

    /**
     * Marshals an unknown-fields map to a JSONB object payload, returning
     * {@code "{}"} for an empty/absent map so the NOT NULL {@code residual} column
     * is satisfiable.
     */
    static String encodeResidual(Map<String, JsonNode> residual) {
        if (residual == null || residual.isEmpty()) {
            return "{}";
        }
        return write(residual);
    }

    /** Parses a JSONB residual payload into an unknown-fields map (never null). */
    static Map<String, JsonNode> decodeResidual(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return MAPPER.readValue(json, NODE_MAP);
        } catch (IOException ex) {
            throw new StorageException("rdq/postgres: decoding residual", ex);
        }
    }

    /** Parses a JSONB payload into a single node, or null when absent. */
    static JsonNode parseNode(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return MAPPER.readTree(json);
        } catch (IOException ex) {
            throw new StorageException("rdq/postgres: decoding error detail", ex);
        }
    }

    /** Serializes a node to compact JSON for a JSONB column, or null when absent. */
    static String writeNode(JsonNode node) {
        return node == null ? null : write(node);
    }

    private static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (IOException ex) {
            throw new StorageException("rdq/postgres: encoding JSON column", ex);
        }
    }

    // --- envelope reassembly ---

    /**
     * Reassembles an {@link Envelope} from the {@link #TASK_COLUMNS} projection of
     * a result row and its ordered {@code attempts}. Inverse of the enqueue
     * decomposition: for any valid envelope, reading back the row it wrote
     * re-encodes to the same canonical bytes (design 01 &sect;1).
     */
    static Envelope readEnvelope(ResultSet rs, Iterable<Attempt> attempts) throws SQLException {
        Envelope.Builder b = Envelope.builder()
            .envelopeVersion(rs.getInt("envelope_version"))
            .id(rs.getString("id"))
            .queue(rs.getString("queue"))
            .handlerRef(rs.getString("handler_ref"))
            .handlerVersion(rs.getString("handler_version"))
            .payload(rs.getBytes("payload"))
            .payloadContentType(rs.getString("payload_content_type"))
            .status(Status.fromWire(rs.getString("status")))
            .attemptCount(rs.getInt("attempt_count"))
            .redriveCount(rs.getInt("redrive_count"))
            .nextAttemptAt(readInstant(rs, "next_attempt_at"))
            .leaseExpiresAt(readInstant(rs, "lease_expires_at"))
            .createdAt(readInstant(rs, "created_at"))
            .headers(decodeStringMap(rs.getString("headers")));

        String payloadRef = rs.getString("payload_ref");
        // The wire model has no payload_ref setter (reserved, unused in v1); it is
        // never populated in v1 rows, so a non-null value would be a schema drift.
        if (payloadRef != null) {
            throw new StorageException("rdq/postgres: unexpected payload_ref on row " + rs.getString("id"));
        }

        for (Map.Entry<String, JsonNode> e : decodeResidual(rs.getString("residual")).entrySet()) {
            b.unknownField(e.getKey(), e.getValue());
        }

        boolean any = false;
        java.util.List<Attempt> list = new java.util.ArrayList<>();
        for (Attempt a : attempts) {
            list.add(a);
            any = true;
        }
        if (any) {
            b.attempts(list);
        }
        return b.build();
    }

    /**
     * Reassembles one {@link Attempt} from an {@code rdq_attempt} row. The
     * {@code error} sub-object is rebuilt only when {@code error_type} is present
     * (every rdq error carries a non-empty type, G6), so its presence is the
     * reliable signal that the attempt carried an error.
     */
    static Attempt readAttempt(ResultSet rs) throws SQLException {
        Attempt.Builder b = Attempt.builder()
            .attemptNo(rs.getInt("attempt_no"))
            .startedAt(readInstant(rs, "started_at"))
            .finishedAt(readInstant(rs, "finished_at"))
            .outcome(Outcome.fromWire(rs.getString("outcome")));

        String errorType = rs.getString("error_type");
        if (errorType != null) {
            b.error(ErrorInfo.builder()
                .type(errorType)
                .message(rs.getString("error_message"))
                .detail(parseNode(rs.getString("error_detail")))
                .stack(rs.getString("error_stack"))
                .build());
        }

        for (Map.Entry<String, JsonNode> e : decodeResidual(rs.getString("residual")).entrySet()) {
            b.unknownField(e.getKey(), e.getValue());
        }
        return b.build();
    }

    /** An ordered mutable copy of {@code e}'s headers, or an empty map. */
    static Map<String, String> headersOrEmpty(Envelope e) {
        Map<String, String> h = e.headers();
        return h == null ? new LinkedHashMap<>() : h;
    }
}
