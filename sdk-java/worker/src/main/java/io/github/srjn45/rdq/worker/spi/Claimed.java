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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.srjn45.rdq.client.envelope.Envelope;

import java.util.Objects;

/**
 * One task handed out by {@link Storage#claimDue}: the leased envelope &mdash;
 * already {@code IN_FLIGHT} with {@code leaseExpiresAt} set by the backend's
 * clock (G9) &mdash; paired with its fencing token.
 *
 * <p>The {@link Envelope} is effectively immutable through its own defensive
 * accessors (its payload and maps are copied on every read), so this record holds
 * and returns the reference directly &mdash; the {@code EI_EXPOSE_REP} that
 * SpotBugs would otherwise report against a "mutable" field is not a real
 * exposure here.
 *
 * @param task  the claimed envelope
 * @param token the fencing token authorizing this claim's outcome
 */
@SuppressFBWarnings(
    value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"},
    justification = "Envelope is effectively immutable through its defensive accessors")
public record Claimed(Envelope task, ClaimToken token) {

    public Claimed {
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(token, "token");
    }
}
