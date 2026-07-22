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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * The failure detail attached to a non-successful attempt (design 01 &sect;2).
 *
 * <p>{@code type} is the language-native class/type string; for a
 * {@link Outcome#LEASE_EXPIRED} attempt it is {@code "rdq.LeaseExpired"}
 * (design 05, G6/G7). {@code detail} is optional structured JSON supplied by an
 * {@code OutcomeMapper}; {@code stack} is optional and truncated at 64&nbsp;KiB
 * by the writer.
 */
@JsonPropertyOrder({"type", "message", "detail", "stack"})
public final class ErrorInfo {

    @JsonProperty("type")
    private String type;

    @JsonProperty("message")
    private String message;

    @JsonProperty("detail")
    private JsonNode detail;

    @JsonProperty("stack")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private String stack;

    private ErrorInfo() {
    }

    public String type() {
        return type;
    }

    public String message() {
        return message;
    }

    /** The optional structured {@code detail} node, preserved verbatim, or null. */
    public JsonNode detail() {
        return detail;
    }

    public String stack() {
        return stack;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder for an {@link ErrorInfo}. */
    public static final class Builder {
        private final ErrorInfo e = new ErrorInfo();

        public Builder type(String type) {
            e.type = type;
            return this;
        }

        public Builder message(String message) {
            e.message = message;
            return this;
        }

        public Builder detail(JsonNode detail) {
            e.detail = detail;
            return this;
        }

        public Builder stack(String stack) {
            e.stack = stack;
            return this;
        }

        public ErrorInfo build() {
            return e;
        }
    }
}
