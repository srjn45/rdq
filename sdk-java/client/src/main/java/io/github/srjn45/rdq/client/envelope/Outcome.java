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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * The result recorded for a single attempt (design 01 &sect;2). Adding a value
 * is a breaking change &mdash; readers switch on it exhaustively (design 01
 * &sect;5).
 *
 * <p>{@link #LEASE_EXPIRED} is recorded when a claim's lease lapses without a
 * reported outcome; it counts against {@code max_attempts} (poison-pill
 * protection).
 */
public enum Outcome {
    SUCCESS("SUCCESS"),
    RETRYABLE_FAILURE("RETRYABLE_FAILURE"),
    PERMANENT_FAILURE("PERMANENT_FAILURE"),
    LEASE_EXPIRED("LEASE_EXPIRED");

    private final String wire;

    Outcome(String wire) {
        this.wire = wire;
    }

    /** The canonical wire token for this outcome. */
    @JsonValue
    public String wire() {
        return wire;
    }

    /** Returns the outcome named by {@code raw}, rejecting any unknown value. */
    @JsonCreator
    public static Outcome fromWire(String raw) {
        for (Outcome o : values()) {
            if (o.wire.equals(raw)) {
                return o;
            }
        }
        throw new IllegalArgumentException("envelope: invalid outcome \"" + raw + "\"");
    }
}
