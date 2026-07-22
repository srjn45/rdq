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

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Canonical JSON codec for the wire {@link Envelope} (design 01 &sect;1).
 *
 * <p>The canonical form is compact UTF-8 JSON with {@code snake_case} keys,
 * RFC-3339 UTC millisecond timestamps, base64 payloads and map keys emitted in
 * sorted order. HTML escaping is left off so the bytes are identical across
 * languages &mdash; the Go, Postgres and Java implementations all replay the
 * same frozen fixtures. Unknown fields are captured on decode and re-emitted
 * verbatim (sorted, after known fields) so a task written by a newer
 * {@code envelope_version} round-trips losslessly through an older reader
 * (design 01 &sect;5).
 *
 * <p>There is deliberately no Java-native serialization path (PRD FR-14).
 */
public final class EnvelopeCodec {

    // RFC-3339 UTC with exactly three fractional digits (design 01 §1:
    // "2026-07-20T14:03:22.117Z"). Trailing zeros are never trimmed.
    private static final DateTimeFormatter MILLIS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private static final ObjectMapper MAPPER = buildMapper();

    private EnvelopeCodec() {
    }

    /** Encodes {@code envelope} in the canonical wire form. */
    public static byte[] encode(Envelope envelope) {
        try {
            return MAPPER.writeValueAsBytes(envelope);
        } catch (IOException ex) {
            throw new UncheckedIOException("envelope: encode failed", ex);
        }
    }

    /** Encodes {@code envelope} in the canonical wire form as a UTF-8 string. */
    public static String encodeToString(Envelope envelope) {
        try {
            return MAPPER.writeValueAsString(envelope);
        } catch (IOException ex) {
            throw new UncheckedIOException("envelope: encode failed", ex);
        }
    }

    /** Decodes canonical wire JSON into a new {@link Envelope}. */
    public static Envelope decode(byte[] data) {
        try {
            return MAPPER.readValue(data, Envelope.class);
        } catch (IOException ex) {
            throw new UncheckedIOException("envelope: decode failed", ex);
        }
    }

    /** Decodes canonical wire JSON into a new {@link Envelope}. */
    public static Envelope decode(String data) {
        try {
            return MAPPER.readValue(data, Envelope.class);
        } catch (IOException ex) {
            throw new UncheckedIOException("envelope: decode failed", ex);
        }
    }

    /**
     * The configured mapper. Package-private: it is the codec's canonical
     * instance, exposed for building {@code detail}/unknown-field
     * {@link com.fasterxml.jackson.databind.JsonNode} values in tests.
     */
    static ObjectMapper mapper() {
        return MAPPER;
    }

    private static ObjectMapper buildMapper() {
        ObjectMapper m = new ObjectMapper();

        // Drive serialization/deserialization from fields only, so the
        // snake_case @JsonProperty on each field is the single source of truth
        // and public accessors never register as duplicate properties. Explicit
        // @JsonValue/@JsonCreator/@JsonAnyGetter/@JsonAnySetter still apply.
        m.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
        m.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

        // Unknown fields are captured via @JsonAnySetter, not rejected.
        m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // Map keys (headers, unknown fields) emit in sorted order.
        m.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        // Default: omit nulls; fields that must always appear (even as null) or
        // must be omitted when empty carry their own @JsonInclude.
        m.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        SimpleModule module = new SimpleModule("rdq-canonical-time");
        module.addSerializer(Instant.class, new CanonicalInstantSerializer());
        module.addDeserializer(Instant.class, new CanonicalInstantDeserializer());
        m.registerModule(module);

        return m;
    }

    /** Renders an {@link Instant} as canonical millisecond RFC-3339 UTC. */
    static final class CanonicalInstantSerializer extends JsonSerializer<Instant> {
        @Override
        public void serialize(Instant value, JsonGenerator gen, SerializerProvider provider)
            throws IOException {
            gen.writeString(MILLIS.format(value));
        }
    }

    /** Parses an RFC-3339 timestamp (any precision/offset) into a UTC {@link Instant}. */
    static final class CanonicalInstantDeserializer extends JsonDeserializer<Instant> {
        @Override
        public Instant deserialize(JsonParser parser, DeserializationContext ctxt)
            throws IOException {
            String raw = parser.getValueAsString();
            return OffsetDateTime.parse(raw).toInstant();
        }
    }
}
