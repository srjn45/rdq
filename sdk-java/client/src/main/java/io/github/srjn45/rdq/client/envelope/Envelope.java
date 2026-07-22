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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The language-neutral wire model of a task (design 01 &sect;2): the single
 * contract shared by the SDKs, {@code rdq-server}, the CLI, and every storage
 * plugin.
 *
 * <p>Serialize and deserialize through {@link EnvelopeCodec}, which produces
 * the canonical JSON form &mdash; compact UTF-8, {@code snake_case} keys,
 * RFC-3339 millisecond-precision UTC timestamps, base64 payloads and sorted
 * map keys &mdash; that is byte-compatible across languages. There is
 * deliberately <em>no</em> Java-native serialization (PRD FR-14).
 *
 * <p>{@code nextAttemptAt} and {@code leaseExpiresAt} are always emitted, as
 * {@code null} when unset. Unknown top-level fields written by a newer
 * {@code envelope_version} are preserved verbatim across a round-trip
 * (design 01 &sect;5).
 */
@JsonPropertyOrder({
    "envelope_version",
    "id",
    "queue",
    "handler_ref",
    "handler_version",
    "payload",
    "payload_content_type",
    "payload_ref",
    "headers",
    "status",
    "attempt_count",
    "redrive_count",
    "next_attempt_at",
    "lease_expires_at",
    "created_at",
    "attempts",
})
public final class Envelope {

    @JsonProperty("envelope_version")
    private int envelopeVersion;

    @JsonProperty("id")
    private String id;

    @JsonProperty("queue")
    private String queue;

    @JsonProperty("handler_ref")
    private String handlerRef;

    @JsonProperty("handler_version")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private String handlerVersion;

    @JsonProperty("payload")
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private byte[] payload;

    @JsonProperty("payload_content_type")
    private String payloadContentType;

    // Reserved for the post-v1 claim-check seam (design 05, OQ-2); unused in v1.
    @JsonProperty("payload_ref")
    private String payloadRef;

    @JsonProperty("headers")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, String> headers;

    @JsonProperty("status")
    private Status status;

    @JsonProperty("attempt_count")
    private int attemptCount;

    @JsonProperty("redrive_count")
    private int redriveCount;

    @JsonProperty("next_attempt_at")
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Instant nextAttemptAt;

    @JsonProperty("lease_expires_at")
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Instant leaseExpiresAt;

    @JsonProperty("created_at")
    private Instant createdAt;

    @JsonProperty("attempts")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<Attempt> attempts;

    // Unknown top-level fields, preserved on the round-trip. A TreeMap so the
    // re-emitted keys are sorted, keeping the canonical bytes deterministic.
    @JsonIgnore
    private final Map<String, JsonNode> unknown = new TreeMap<>();

    private Envelope() {
    }

    // Copy constructor: build() returns a fresh, independent instance so the
    // builder can never mutate a handed-out envelope (defensive by design).
    private Envelope(Envelope s) {
        envelopeVersion = s.envelopeVersion;
        id = s.id;
        queue = s.queue;
        handlerRef = s.handlerRef;
        handlerVersion = s.handlerVersion;
        payload = s.payload == null ? null : s.payload.clone();
        payloadContentType = s.payloadContentType;
        payloadRef = s.payloadRef;
        headers = s.headers == null ? null : new LinkedHashMap<>(s.headers);
        status = s.status;
        attemptCount = s.attemptCount;
        redriveCount = s.redriveCount;
        nextAttemptAt = s.nextAttemptAt;
        leaseExpiresAt = s.leaseExpiresAt;
        createdAt = s.createdAt;
        attempts = s.attempts == null ? null : new ArrayList<>(s.attempts);
        unknown.putAll(s.unknown);
    }

    public int envelopeVersion() {
        return envelopeVersion;
    }

    public String id() {
        return id;
    }

    public String queue() {
        return queue;
    }

    public String handlerRef() {
        return handlerRef;
    }

    public String handlerVersion() {
        return handlerVersion;
    }

    /** The opaque payload bytes (a defensive copy), or null. */
    public byte[] payload() {
        return payload == null ? null : payload.clone();
    }

    public String payloadContentType() {
        return payloadContentType;
    }

    public String payloadRef() {
        return payloadRef;
    }

    /** The header map (a defensive copy), or null. */
    public Map<String, String> headers() {
        return headers == null ? null : new LinkedHashMap<>(headers);
    }

    public Status status() {
        return status;
    }

    public int attemptCount() {
        return attemptCount;
    }

    public int redriveCount() {
        return redriveCount;
    }

    /** When the task becomes due; null once terminal. */
    public Instant nextAttemptAt() {
        return nextAttemptAt;
    }

    /** Set while {@link Status#IN_FLIGHT}; null otherwise. */
    public Instant leaseExpiresAt() {
        return leaseExpiresAt;
    }

    public Instant createdAt() {
        return createdAt;
    }

    /** The attempt history (a defensive copy), or null. */
    public List<Attempt> attempts() {
        return attempts == null ? null : new ArrayList<>(attempts);
    }

    /** The preserved unknown top-level fields, keyed by name (may be empty). */
    @JsonAnyGetter
    public Map<String, JsonNode> unknownFields() {
        return new TreeMap<>(unknown);
    }

    @JsonAnySetter
    void putUnknownField(String key, JsonNode value) {
        unknown.put(key, value);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder for an {@link Envelope}. */
    public static final class Builder {
        private final Envelope e = new Envelope();

        public Builder envelopeVersion(int envelopeVersion) {
            e.envelopeVersion = envelopeVersion;
            return this;
        }

        public Builder id(String id) {
            e.id = id;
            return this;
        }

        public Builder queue(String queue) {
            e.queue = queue;
            return this;
        }

        public Builder handlerRef(String handlerRef) {
            e.handlerRef = handlerRef;
            return this;
        }

        public Builder handlerVersion(String handlerVersion) {
            e.handlerVersion = handlerVersion;
            return this;
        }

        public Builder payload(byte[] payload) {
            e.payload = payload == null ? null : payload.clone();
            return this;
        }

        public Builder payloadContentType(String payloadContentType) {
            e.payloadContentType = payloadContentType;
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            e.headers = headers == null ? null : new LinkedHashMap<>(headers);
            return this;
        }

        public Builder status(Status status) {
            e.status = status;
            return this;
        }

        public Builder attemptCount(int attemptCount) {
            e.attemptCount = attemptCount;
            return this;
        }

        public Builder redriveCount(int redriveCount) {
            e.redriveCount = redriveCount;
            return this;
        }

        public Builder nextAttemptAt(Instant nextAttemptAt) {
            e.nextAttemptAt = nextAttemptAt;
            return this;
        }

        public Builder leaseExpiresAt(Instant leaseExpiresAt) {
            e.leaseExpiresAt = leaseExpiresAt;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            e.createdAt = createdAt;
            return this;
        }

        public Builder attempts(List<Attempt> attempts) {
            e.attempts = attempts == null ? null : new ArrayList<>(attempts);
            return this;
        }

        public Builder unknownField(String key, JsonNode value) {
            e.unknown.put(key, value);
            return this;
        }

        public Envelope build() {
            return new Envelope(e);
        }
    }
}
