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

package io.github.srjn45.rdq.worker.engine;

import io.github.srjn45.rdq.client.envelope.Envelope;

/**
 * The unit of work a worker invokes for a claimed task. Registered under a stable
 * {@code handler_ref} in a {@link HandlerRegistry}. Mirrors Go
 * {@code registry.Handler}.
 *
 * <p><b>Interruptibility.</b> The engine enforces a {@code handler_timeout} by
 * interrupting the handler thread (and cancelling heartbeat). Implementations should
 * respond to {@code Thread.interrupted()} or {@link InterruptedException} by
 * cleaning up and returning so the task can be rescheduled.
 */
public interface Handler {

    /**
     * Reports this handler's implementation version (e.g. {@code "v3"}). Compared
     * against {@link Envelope#handlerVersion()} under the queue's version-mismatch
     * policy. An empty pin on the task always matches.
     */
    String version();

    /**
     * Executes the task. A normal return is success; any thrown exception is
     * classified by the outcome ladder to decide retry vs permanent failure.
     */
    void handle(Envelope task) throws Exception;
}
