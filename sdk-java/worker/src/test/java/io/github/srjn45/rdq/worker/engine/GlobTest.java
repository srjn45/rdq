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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the two-pointer glob matcher — mirrors Go {@code policy.Glob} tests. */
class GlobTest {

    @Test
    void exactMatch_noWildcards() {
        assertThat(Glob.matches("TIMEOUT", "TIMEOUT")).isTrue();
        assertThat(Glob.matches("TIMEOUT", "TIMEOUT2")).isFalse();
        assertThat(Glob.matches("TIMEOUT2", "TIMEOUT")).isFalse();
    }

    @Test
    void trailingStar_matchesDottedSuffix() {
        assertThat(Glob.matches("java.net.*", "java.net.SocketTimeoutException")).isTrue();
        assertThat(Glob.matches("java.net.*", "java.net.ConnectException")).isTrue();
        assertThat(Glob.matches("java.net.*", "java.io.IOException")).isFalse();
        assertThat(Glob.matches("java.net.*", "java.net.")).isTrue();
    }

    @Test
    void leadingStar_matchesDottedPrefix() {
        assertThat(Glob.matches("*.ValidationException", "com.acme.ValidationException")).isTrue();
        assertThat(Glob.matches("*.ValidationException", "ValidationException")).isFalse();
    }

    @Test
    void starMatchesEmpty() {
        assertThat(Glob.matches("*", "")).isTrue();
        assertThat(Glob.matches("*", "anything")).isTrue();
    }

    @Test
    void starInMiddle_matchesAcrossDots() {
        assertThat(Glob.matches("java.*.Exception", "java.net.Exception")).isTrue();
        assertThat(Glob.matches("java.*.Exception", "java.io.IOException")).isFalse();
    }

    @Test
    void questionMark_matchesExactlyOneChar() {
        assertThat(Glob.matches("java.net.?", "java.net.X")).isTrue();
        assertThat(Glob.matches("java.net.?", "java.net.")).isFalse();
        assertThat(Glob.matches("java.net.?", "java.net.XY")).isFalse();
    }

    @Test
    void emptyPattern_matchesOnlyEmptyString() {
        assertThat(Glob.matches("", "")).isTrue();
        assertThat(Glob.matches("", "x")).isFalse();
    }

    @Test
    void multipleStar_collapses() {
        assertThat(Glob.matches("java.**", "java.net.Socket")).isTrue();
        assertThat(Glob.matches("**", "anything.at.all")).isTrue();
    }

    @Test
    void backtrackingCase() {
        // 'a*b' must backtrack when '*' is greedy
        assertThat(Glob.matches("a*b", "aXb")).isTrue();
        assertThat(Glob.matches("a*b", "aXXb")).isTrue();
        assertThat(Glob.matches("a*b", "ab")).isTrue();
        assertThat(Glob.matches("a*b", "a")).isFalse();
    }

    @Test
    void caseSensitive() {
        assertThat(Glob.matches("timeout", "TIMEOUT")).isFalse();
        assertThat(Glob.matches("TIMEOUT", "timeout")).isFalse();
    }
}
