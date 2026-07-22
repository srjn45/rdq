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

import java.util.Objects;

/**
 * One entry of classification layer 3 (design 03 &sect;4): a hierarchy-aware
 * class-list rule. A rule matches when the thrown exception (or any exception in
 * its cause chain) is an instance of the registered class &mdash; so a subclass of
 * the registered class matches just as its superclass would. Rules are tried in
 * declaration order; the first match wins.
 *
 * <p>Mirrors Go {@code policy.AsRule[T error](decision Decision)}: {@code errors.As}
 * walks the chain and matches by type (including subtype).
 */
public final class ClassRule {

    private final Class<? extends Throwable> type;
    private final Decision decision;

    private ClassRule(Class<? extends Throwable> type, Decision decision) {
        this.type = Objects.requireNonNull(type, "type");
        this.decision = Objects.requireNonNull(decision, "decision");
    }

    /** Creates a rule that classifies any exception assignable to {@code type}. */
    public static ClassRule of(Class<? extends Throwable> type, Decision decision) {
        return new ClassRule(type, decision);
    }

    public Decision decision() {
        return decision;
    }

    /**
     * Reports whether {@code ex} (or any exception in its cause chain) is an
     * instance of this rule's type. Hierarchy-aware: a {@code FileNotFoundException}
     * matches a rule for {@code IOException}.
     */
    boolean matches(Throwable ex) {
        Throwable t = ex;
        while (t != null) {
            if (type.isInstance(t)) {
                return true;
            }
            Throwable cause = t.getCause();
            if (cause == t) break; // cycle guard
            t = cause;
        }
        return false;
    }
}
