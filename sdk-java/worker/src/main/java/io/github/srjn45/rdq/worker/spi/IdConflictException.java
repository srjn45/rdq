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

package io.github.srjn45.rdq.worker.spi;

/**
 * Thrown by {@link Storage#enqueue} when the task id already exists in a
 * DIFFERENT queue. Re-enqueue within the SAME queue is an idempotent no-op; a
 * cross-queue collision is rejected rather than silently returning a foreign
 * envelope (G8). Maps to HTTP 409 at the API.
 */
public final class IdConflictException extends StorageException {

    private static final long serialVersionUID = 1L;

    public IdConflictException(String id) {
        super("spi: id exists in a different queue: " + id);
    }
}
