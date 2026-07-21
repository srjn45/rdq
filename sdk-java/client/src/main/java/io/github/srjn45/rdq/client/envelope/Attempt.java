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
import java.util.Map;
import java.util.TreeMap;

/**
 * One execution record in an {@link Envelope}'s history (design 01 &sect;2).
 *
 * <p>{@code finishedAt} is null while the attempt is still in flight (it is
 * always emitted, as {@code null} in that case). Unknown per-attempt fields
 * written by a newer {@code envelope_version} are captured on decode and
 * re-emitted verbatim, sorted, after all known fields (design 01 &sect;5).
 */
@JsonPropertyOrder({"attempt_no", "started_at", "finished_at", "outcome", "error"})
public final class Attempt {

    @JsonProperty("attempt_no")
    private int attemptNo;

    @JsonProperty("started_at")
    private Instant startedAt;

    @JsonProperty("finished_at")
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Instant finishedAt;

    @JsonProperty("outcome")
    private Outcome outcome;

    @JsonProperty("error")
    private ErrorInfo error;

    // Unknown per-attempt fields, preserved on the round-trip. A TreeMap so the
    // re-emitted keys are sorted, keeping the canonical bytes deterministic.
    @JsonIgnore
    private final Map<String, JsonNode> unknown = new TreeMap<>();

    private Attempt() {
    }

    // Copy constructor: build() returns a fresh, independent instance.
    private Attempt(Attempt s) {
        attemptNo = s.attemptNo;
        startedAt = s.startedAt;
        finishedAt = s.finishedAt;
        outcome = s.outcome;
        error = s.error;
        unknown.putAll(s.unknown);
    }

    public int attemptNo() {
        return attemptNo;
    }

    public Instant startedAt() {
        return startedAt;
    }

    /** When the attempt finished, or null if the worker never reported an outcome. */
    public Instant finishedAt() {
        return finishedAt;
    }

    public Outcome outcome() {
        return outcome;
    }

    public ErrorInfo error() {
        return error;
    }

    /** The preserved unknown per-attempt fields, keyed by name (may be empty). */
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

    /** Fluent builder for an {@link Attempt}. */
    public static final class Builder {
        private final Attempt a = new Attempt();

        public Builder attemptNo(int attemptNo) {
            a.attemptNo = attemptNo;
            return this;
        }

        public Builder startedAt(Instant startedAt) {
            a.startedAt = startedAt;
            return this;
        }

        public Builder finishedAt(Instant finishedAt) {
            a.finishedAt = finishedAt;
            return this;
        }

        public Builder outcome(Outcome outcome) {
            a.outcome = outcome;
            return this;
        }

        public Builder error(ErrorInfo error) {
            a.error = error;
            return this;
        }

        public Builder unknownField(String key, JsonNode value) {
            a.unknown.put(key, value);
            return this;
        }

        public Attempt build() {
            return new Attempt(a);
        }
    }
}
