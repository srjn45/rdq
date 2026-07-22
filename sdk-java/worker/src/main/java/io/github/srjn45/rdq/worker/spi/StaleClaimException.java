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

/**
 * Thrown by {@link Storage#extendLease}/{@link Storage#reschedule}/{@link
 * Storage#complete}/{@link Storage#deadLetter} when the supplied {@link
 * ClaimToken} is no longer valid &mdash; the lease expired and the task was
 * reclaimed elsewhere (design 02 &sect;3 fencing invariant). The operation
 * changes nothing; the handler must abandon its work.
 */
public final class StaleClaimException extends StorageException {

    private static final long serialVersionUID = 1L;

    public StaleClaimException() {
        super("spi: stale claim token");
    }
}
