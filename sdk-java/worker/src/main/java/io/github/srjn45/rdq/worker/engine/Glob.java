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
 * Language-neutral wildcard glob matching for the config-glob classification
 * layer (layer 4, design 03 &sect;4). Two metacharacters: {@code *} matches any
 * run of characters (including dots and the empty string); {@code ?} matches
 * exactly one character. Matching is case-sensitive and anchored at both ends.
 *
 * <p>This is a direct Java port of {@code core/policy/glob.go}: the two-pointer
 * backtracking algorithm, same semantics. Both ends of the language cross-language
 * contract must agree on which error.type patterns match which strings.
 */
final class Glob {

    private Glob() {}

    /** Reports whether {@code pattern} matches {@code s}. */
    static boolean matches(String pattern, String s) {
        int px = 0, sx = 0, star = -1, mark = 0;
        while (sx < s.length()) {
            if (px < pattern.length() && pattern.charAt(px) == '*') {
                star = px;
                mark = sx;
                px++;
            } else if (px < pattern.length()
                    && (pattern.charAt(px) == '?' || pattern.charAt(px) == s.charAt(sx))) {
                px++;
                sx++;
            } else if (star >= 0) {
                px = star + 1;
                mark++;
                sx = mark;
            } else {
                return false;
            }
        }
        while (px < pattern.length() && pattern.charAt(px) == '*') {
            px++;
        }
        return px == pattern.length();
    }
}
