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

package io.github.srjn45.rdq.worker.spi;

import java.util.Objects;

/**
 * The fencing token minted by {@link Storage#claimDue} for a single claim
 * (design 02 &sect;1, &sect;3). It authorizes exactly one live claim of a task:
 * {@link Storage#extendLease}, {@link Storage#reschedule},
 * {@link Storage#complete} and {@link Storage#deadLetter} reject any other token
 * with {@link StaleClaimException}. The value is opaque to the engine &mdash; its
 * structure is the backend's concern (the Postgres binding uses the row's
 * {@code claim_token} UUID as text).
 *
 * @param value the opaque token string; never {@code null}
 */
public record ClaimToken(String value) {

    public ClaimToken {
        Objects.requireNonNull(value, "value");
    }

    /** Wraps {@code value} as a {@code ClaimToken}. */
    public static ClaimToken of(String value) {
        return new ClaimToken(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
