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
 * The lifecycle state of a task (design 01 &sect;2). Adding a value is a
 * breaking change &mdash; readers switch on it exhaustively (design 01 &sect;5),
 * so an unknown wire value is rejected rather than silently accepted.
 */
public enum Status {
    PENDING("PENDING"),
    IN_FLIGHT("IN_FLIGHT"),
    SUCCEEDED("SUCCEEDED"),
    DEAD("DEAD");

    private final String wire;

    Status(String wire) {
        this.wire = wire;
    }

    /** The canonical wire token for this status. */
    @JsonValue
    public String wire() {
        return wire;
    }

    /** Returns the status named by {@code raw}, rejecting any unknown value. */
    @JsonCreator
    public static Status fromWire(String raw) {
        for (Status s : values()) {
            if (s.wire.equals(raw)) {
                return s;
            }
        }
        throw new IllegalArgumentException("envelope: invalid status \"" + raw + "\"");
    }
}
