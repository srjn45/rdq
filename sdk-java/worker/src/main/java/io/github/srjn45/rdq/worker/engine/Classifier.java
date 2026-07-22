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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Resolves a failed attempt to a {@link Verdict} through the five-layer precedence
 * ladder (design 03 &sect;4). Mirrors Go {@code policy.Classifier}.
 *
 * <ol>
 *   <li><b>OutcomeMapper</b> &mdash; authoritative per-queue hook; short-circuits
 *       when it returns a non-empty {@link Optional}.</li>
 *   <li><b>Classified wrapper</b> &mdash; outermost {@link PermanentException} /
 *       {@link RetryableException} in the cause chain.</li>
 *   <li><b>ClassRules</b> &mdash; hierarchy-aware class-list rules, first match
 *       wins.</li>
 *   <li><b>Config globs</b> &mdash; {@code permanent} patterns checked before
 *       {@code retryable} (an explicit "never retry" bounds poison pills).</li>
 *   <li><b>Default</b> &mdash; retryable; a failure with no classification is
 *       always retried up to {@code max_attempts}.</li>
 * </ol>
 *
 * <p>Instances carry no mutable state and are safe to share across worker threads.
 */
public final class Classifier {

    private final OutcomeMapper outcomeMapper;
    private final List<ClassRule> classRules;
    private final List<String> permanentPatterns;
    private final List<String> retryablePatterns;

    private Classifier(Builder b) {
        this.outcomeMapper = b.outcomeMapper;
        this.classRules = Collections.unmodifiableList(new ArrayList<>(b.classRules));
        this.permanentPatterns = Collections.unmodifiableList(new ArrayList<>(b.permanentPatterns));
        this.retryablePatterns = Collections.unmodifiableList(new ArrayList<>(b.retryablePatterns));
    }

    /** A zero {@link Classifier}: no mapper, no rules, no globs &mdash; pure default (retryable). */
    public static Classifier empty() {
        return new Builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Walks the five-layer ladder and returns the winning {@link Verdict}.
     *
     * @param ex      the exception the handler threw (may be {@code null} in
     *                server/callback mode — layers 1–3 are skipped when null)
     * @param errType the reported {@code error.type} string used by layer 4; pass
     *                {@code ""} to disable glob matching
     */
    public Verdict classify(Exception ex, String errType) {
        // Layer 1 — OutcomeMapper is authoritative when it claims the error.
        if (outcomeMapper != null && ex != null) {
            Optional<Decision> d = outcomeMapper.apply(ex);
            if (d != null && d.isPresent()) {
                return new Verdict(d.get(), Verdict.Layer.OUTCOME_MAPPER);
            }
        }

        // Layer 2 — outermost Classified wrapper in the cause chain.
        if (ex != null) {
            Classified wrapper = findClassified(ex);
            if (wrapper != null) {
                return new Verdict(wrapper.classification(), Verdict.Layer.WRAPPER);
            }
        }

        // Layer 3 — class rules, first match wins.
        if (ex != null) {
            for (ClassRule rule : classRules) {
                if (rule.matches(ex)) {
                    return new Verdict(rule.decision(), Verdict.Layer.CODE_CLASSIFIER);
                }
            }
        }

        // Layer 4 — config globs on the reported error.type. Permanent tested first
        // so an explicit "never retry" cannot be silently undone by a retryable glob.
        if (errType != null && !errType.isEmpty()) {
            for (String pattern : permanentPatterns) {
                if (Glob.matches(pattern, errType)) {
                    return new Verdict(Decision.PERMANENT, Verdict.Layer.CONFIG_GLOB);
                }
            }
            for (String pattern : retryablePatterns) {
                if (Glob.matches(pattern, errType)) {
                    return new Verdict(Decision.RETRYABLE, Verdict.Layer.CONFIG_GLOB);
                }
            }
        }

        // Layer 5 — default: retryable.
        return new Verdict(Decision.RETRYABLE, Verdict.Layer.DEFAULT);
    }

    /**
     * Derives the {@code error.type} string for a thrown exception: the fully
     * qualified class name of the innermost (root-cause) exception after unwrapping
     * the cause chain. Mirrors Go {@code policy.ErrorType(err)}.
     */
    public static String errorType(Throwable ex) {
        if (ex == null) return "";
        Throwable t = ex;
        while (true) {
            Throwable cause = t.getCause();
            if (cause == null || cause == t) break;
            t = cause;
        }
        return t.getClass().getName();
    }

    private static Classified findClassified(Throwable ex) {
        Throwable t = ex;
        while (t != null) {
            if (t instanceof Classified c) {
                return c;
            }
            Throwable cause = t.getCause();
            if (cause == t) break;
            t = cause;
        }
        return null;
    }

    /** Fluent builder for {@link Classifier}. */
    public static final class Builder {

        private OutcomeMapper outcomeMapper;
        private final List<ClassRule> classRules = new ArrayList<>();
        private final List<String> permanentPatterns = new ArrayList<>();
        private final List<String> retryablePatterns = new ArrayList<>();

        private Builder() {}

        /** Sets the layer-1 {@link OutcomeMapper} (replaces any previously set). */
        public Builder outcomeMapper(OutcomeMapper mapper) {
            this.outcomeMapper = mapper;
            return this;
        }

        /** Appends a layer-3 class rule. Rules are tried in declaration order. */
        public Builder classRule(ClassRule rule) {
            classRules.add(Objects.requireNonNull(rule, "rule"));
            return this;
        }

        /** Appends a layer-4 {@code permanent} glob pattern. */
        public Builder permanentPattern(String pattern) {
            permanentPatterns.add(Objects.requireNonNull(pattern, "pattern"));
            return this;
        }

        /** Appends a layer-4 {@code retryable} glob pattern. */
        public Builder retryablePattern(String pattern) {
            retryablePatterns.add(Objects.requireNonNull(pattern, "pattern"));
            return this;
        }

        public Classifier build() {
            return new Classifier(this);
        }
    }
}
