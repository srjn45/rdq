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

package io.github.srjn45.rdq.worker.engine;

/**
 * The queue's {@code handler_version} mismatch policy (design 03 &sect;2). Mirrors
 * Go {@code registry.Policy}.
 */
public enum VersionPolicy {

    /**
     * Ignores a {@code handler_version} pin mismatch and runs the registered
     * (latest) handler anyway. The safe default: an unconfigured queue drains
     * rather than silently mass-dead-lettering.
     */
    RUN_LATEST,

    /**
     * Refuses to run a handler whose version differs from the task's pin and
     * dead-letters the task with {@code rdq.HandlerVersionMismatch}. Use when
     * strict version pinning is required.
     */
    DEAD_LETTER;

    /** Maps a config string to a {@link VersionPolicy}, defaulting to {@link #RUN_LATEST}. */
    public static VersionPolicy from(String s) {
        if ("dead-letter".equals(s)) return DEAD_LETTER;
        return RUN_LATEST;
    }
}
