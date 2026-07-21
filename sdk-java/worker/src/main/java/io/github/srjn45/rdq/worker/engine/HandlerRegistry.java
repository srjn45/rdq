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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe name&rarr;handler map and routing logic (design 05, FR-11&ndash;13).
 * Mirrors Go {@code core/registry.Registry}.
 *
 * <p>Two routing failure modes produce distinct {@code error.type} values so
 * operators can tell them apart:
 * <ul>
 *   <li><b>Unroutable</b> &mdash; no handler registered for the task's
 *       {@code handler_ref}. Dead-lettered immediately, never rescheduled (a
 *       reschedule would produce a tight claim hot-loop).</li>
 *   <li><b>Version mismatch</b> &mdash; a handler exists but its {@code version()}
 *       differs from the task's pin. What happens depends on
 *       {@link VersionPolicy}.</li>
 * </ul>
 *
 * <p>Handlers are registered at startup and looked up on every claim (read-heavy),
 * so lookups take a read lock.
 */
public final class HandlerRegistry {

    /** Error type recorded on the dead-lettered attempt when no handler is registered. */
    public static final String ERROR_TYPE_UNROUTABLE = "rdq.Unroutable";

    /** Error type recorded when a handler version pin does not match under {@link VersionPolicy#DEAD_LETTER}. */
    public static final String ERROR_TYPE_VERSION_MISMATCH = "rdq.HandlerVersionMismatch";

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, Handler> handlers = new HashMap<>();

    /** Creates an empty registry. */
    public HandlerRegistry() {}

    /**
     * Binds {@code handlerRef} to {@code handler}. Rejects an empty ref, a null
     * handler, and a duplicate ref — registration is one-shot so the effective
     * handler never depends on init ordering.
     *
     * @throws IllegalArgumentException if {@code handlerRef} is empty or already registered
     * @throws NullPointerException     if {@code handler} is null
     */
    public void register(String handlerRef, Handler handler) {
        Objects.requireNonNull(handler, "handler");
        if (handlerRef == null || handlerRef.isEmpty()) {
            throw new IllegalArgumentException("registry: empty handler_ref");
        }
        lock.writeLock().lock();
        try {
            if (handlers.containsKey(handlerRef)) {
                throw new IllegalArgumentException(
                    "registry: handler_ref already registered: \"" + handlerRef + "\"");
            }
            handlers.put(handlerRef, handler);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Returns the handler registered for {@code handlerRef}, or {@code null} if none. */
    public Handler lookup(String handlerRef) {
        lock.readLock().lock();
        try {
            return handlers.get(handlerRef);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Returns the number of registered handlers. */
    public int size() {
        lock.readLock().lock();
        try {
            return handlers.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Resolves {@code task} to an {@link Action} under {@code policy}. Pure:
     * no I/O, no clock. The caller invokes the returned handler or dead-letters
     * with the returned error info.
     *
     * <p>Decision ladder:
     * <ol>
     *   <li>No handler for {@code task.handlerRef()} &rarr; {@link Action#DEAD_LETTER}, unroutable.</li>
     *   <li>Handler found and the task pins no version, or the pin equals the
     *       handler's {@code version()} &rarr; {@link Action#RUN}.</li>
     *   <li>Pin mismatch:
     *     <ul>
     *       <li>{@link VersionPolicy#RUN_LATEST} &rarr; {@link Action#RUN}.</li>
     *       <li>{@link VersionPolicy#DEAD_LETTER} &rarr; {@link Action#DEAD_LETTER}, version mismatch.</li>
     *     </ul>
     *   </li>
     * </ol>
     */
    public Resolution resolve(Envelope task, VersionPolicy policy) {
        Handler h = lookup(task.handlerRef());
        if (h == null) {
            return Resolution.deadLetter(
                ERROR_TYPE_UNROUTABLE,
                "no handler registered for handler_ref \"" + task.handlerRef() + "\"");
        }
        String pin = task.handlerVersion();
        if (pin == null || pin.isEmpty() || pin.equals(h.version())) {
            return Resolution.run(h);
        }
        if (policy == VersionPolicy.DEAD_LETTER) {
            return Resolution.deadLetter(
                ERROR_TYPE_VERSION_MISMATCH,
                "handler_version \"" + pin + "\" does not match registered handler \""
                    + h.version() + "\" for handler_ref \"" + task.handlerRef() + "\"");
        }
        return Resolution.run(h);
    }

    /** The routing decision for one claimed task. */
    public enum Action {
        /** Invoke {@link Resolution#handler()}. */
        RUN,
        /** Route to DLQ with {@link Resolution#errorType()} / {@link Resolution#errorMessage()}. */
        DEAD_LETTER
    }

    /** The outcome of routing one claimed task. */
    public static final class Resolution {
        private final Action action;
        private final Handler handler;
        private final String errorType;
        private final String errorMessage;

        private Resolution(Action action, Handler handler, String errorType, String errorMessage) {
            this.action = action;
            this.handler = handler;
            this.errorType = errorType;
            this.errorMessage = errorMessage;
        }

        static Resolution run(Handler handler) {
            return new Resolution(Action.RUN, handler, null, null);
        }

        static Resolution deadLetter(String errorType, String errorMessage) {
            return new Resolution(Action.DEAD_LETTER, null, errorType, errorMessage);
        }

        public Action action() {
            return action;
        }

        /** Non-null when {@link #action()} is {@link Action#RUN}. */
        public Handler handler() {
            return handler;
        }

        /** Non-null when {@link #action()} is {@link Action#DEAD_LETTER}. */
        public String errorType() {
            return errorType;
        }

        /** Non-null when {@link #action()} is {@link Action#DEAD_LETTER}. */
        public String errorMessage() {
            return errorMessage;
        }
    }
}
