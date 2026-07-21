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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the five-layer classification ladder. Each layer is tested in
 * isolation, and the precedence ordering is verified.
 */
class ClassifierTest {

    // ---- layer 5: default --------------------------------------------------

    @Test
    void default_retryable_whenNothingMatches() {
        Verdict v = Classifier.empty().classify(new RuntimeException("boom"), "some.type");
        assertThat(v.decision()).isEqualTo(Decision.RETRYABLE);
        assertThat(v.layer()).isEqualTo(Verdict.Layer.DEFAULT);
    }

    @Test
    void default_nullEx_retryable() {
        Verdict v = Classifier.empty().classify(null, "some.type");
        assertThat(v.decision()).isEqualTo(Decision.RETRYABLE);
    }

    // ---- layer 4: config globs ---------------------------------------------

    @Test
    void configGlob_permanentPatternMatches() {
        Classifier c = Classifier.builder()
            .permanentPattern("java.lang.*")
            .build();
        Verdict v = c.classify(null, "java.lang.IllegalArgumentException");
        assertThat(v.decision()).isEqualTo(Decision.PERMANENT);
        assertThat(v.layer()).isEqualTo(Verdict.Layer.CONFIG_GLOB);
    }

    @Test
    void configGlob_retryablePatternMatches() {
        Classifier c = Classifier.builder()
            .retryablePattern("java.net.*")
            .build();
        Verdict v = c.classify(null, "java.net.SocketTimeoutException");
        assertThat(v.decision()).isEqualTo(Decision.RETRYABLE);
        assertThat(v.layer()).isEqualTo(Verdict.Layer.CONFIG_GLOB);
    }

    @Test
    void configGlob_permanent_beatsRetryable_whenBothMatch() {
        Classifier c = Classifier.builder()
            .retryablePattern("java.lang.*")
            .permanentPattern("java.lang.IllegalArgumentException")
            .build();
        Verdict v = c.classify(null, "java.lang.IllegalArgumentException");
        assertThat(v.decision()).isEqualTo(Decision.PERMANENT);
    }

    @Test
    void configGlob_emptyErrType_noMatch() {
        Classifier c = Classifier.builder()
            .permanentPattern("*")
            .build();
        Verdict v = c.classify(null, "");
        assertThat(v.layer()).isEqualTo(Verdict.Layer.DEFAULT);
    }

    // ---- layer 3: class rules (hierarchy-aware) ----------------------------

    @Test
    void classRule_exactTypeMatches() {
        Classifier c = Classifier.builder()
            .classRule(ClassRule.of(IllegalStateException.class, Decision.PERMANENT))
            .build();
        Verdict v = c.classify(new IllegalStateException("x"), "");
        assertThat(v.decision()).isEqualTo(Decision.PERMANENT);
        assertThat(v.layer()).isEqualTo(Verdict.Layer.CODE_CLASSIFIER);
    }

    @Test
    void classRule_subclassMatchesSuperclassRule_hierarchyAware() {
        // Register IOException — FileNotFoundException (subclass) must match
        Classifier c = Classifier.builder()
            .classRule(ClassRule.of(IOException.class, Decision.RETRYABLE))
            .build();
        Verdict v = c.classify(new FileNotFoundException("x"), "");
        assertThat(v.decision()).isEqualTo(Decision.RETRYABLE);
        assertThat(v.layer()).isEqualTo(Verdict.Layer.CODE_CLASSIFIER);
    }

    @Test
    void classRule_firstMatchWins() {
        Classifier c = Classifier.builder()
            .classRule(ClassRule.of(IOException.class, Decision.RETRYABLE))
            .classRule(ClassRule.of(FileNotFoundException.class, Decision.PERMANENT))
            .build();
        // IOException rule comes first, so FileNotFoundException → RETRYABLE
        Verdict v = c.classify(new FileNotFoundException("x"), "");
        assertThat(v.decision()).isEqualTo(Decision.RETRYABLE);
    }

    @Test
    void classRule_causeChainWalked() {
        // The exception itself doesn't match, but its cause does
        Classifier c = Classifier.builder()
            .classRule(ClassRule.of(IOException.class, Decision.PERMANENT))
            .build();
        RuntimeException wrapped = new RuntimeException("wrap", new IOException("inner"));
        Verdict v = c.classify(wrapped, "");
        assertThat(v.decision()).isEqualTo(Decision.PERMANENT);
    }

    @Test
    void classRule_noMatch_fallsThroughToDefault() {
        Classifier c = Classifier.builder()
            .classRule(ClassRule.of(IOException.class, Decision.PERMANENT))
            .build();
        Verdict v = c.classify(new RuntimeException("boom"), "");
        assertThat(v.layer()).isEqualTo(Verdict.Layer.DEFAULT);
    }

    // ---- layer 2: Classified wrapper ---------------------------------------

    @Test
    void wrapper_permanentException_overridesClassRule() {
        Classifier c = Classifier.builder()
            .classRule(ClassRule.of(RuntimeException.class, Decision.RETRYABLE))
            .build();
        Verdict v = c.classify(new PermanentException(new RuntimeException("x")), "");
        assertThat(v.decision()).isEqualTo(Decision.PERMANENT);
        assertThat(v.layer()).isEqualTo(Verdict.Layer.WRAPPER);
    }

    @Test
    void wrapper_retryableException_overridesClassRule() {
        Classifier c = Classifier.builder()
            .classRule(ClassRule.of(RuntimeException.class, Decision.PERMANENT))
            .build();
        Verdict v = c.classify(new RetryableException(new RuntimeException("x")), "");
        assertThat(v.decision()).isEqualTo(Decision.RETRYABLE);
        assertThat(v.layer()).isEqualTo(Verdict.Layer.WRAPPER);
    }

    @Test
    void wrapper_inCauseChain() {
        // PermanentException buried in the cause chain — layer 2 still finds it
        Exception outer = new RuntimeException("outer", new PermanentException(new IOException("inner")));
        Verdict v = Classifier.empty().classify(outer, "");
        assertThat(v.decision()).isEqualTo(Decision.PERMANENT);
        assertThat(v.layer()).isEqualTo(Verdict.Layer.WRAPPER);
    }

    // ---- layer 1: OutcomeMapper --------------------------------------------

    @Test
    void outcomeMapper_authoritativeWhenPresent() {
        Classifier c = Classifier.builder()
            .classRule(ClassRule.of(RuntimeException.class, Decision.RETRYABLE))
            .outcomeMapper(ex -> Optional.of(Decision.PERMANENT))
            .build();
        Verdict v = c.classify(new RuntimeException("boom"), "");
        assertThat(v.decision()).isEqualTo(Decision.PERMANENT);
        assertThat(v.layer()).isEqualTo(Verdict.Layer.OUTCOME_MAPPER);
    }

    @Test
    void outcomeMapper_emptyDeclines_fallsThroughToLayer2() {
        Classifier c = Classifier.builder()
            .outcomeMapper(ex -> Optional.empty())
            .build();
        Verdict v = c.classify(new PermanentException(new RuntimeException("x")), "");
        assertThat(v.layer()).isEqualTo(Verdict.Layer.WRAPPER);
    }

    @Test
    void outcomeMapper_nullReturn_treatedAsDecline() {
        Classifier c = Classifier.builder()
            .outcomeMapper(ClassifierTest::nullMapper)
            .build();
        Verdict v = c.classify(new RuntimeException("x"), "some.type");
        assertThat(v.layer()).isEqualTo(Verdict.Layer.DEFAULT);
    }

    @SuppressFBWarnings(
        value = "NP_OPTIONAL_RETURN_NULL",
        justification = "deliberately testing null tolerance: null from mapper must be treated as decline")
    private static java.util.Optional<Decision> nullMapper(Exception ex) {
        return null;
    }
}
