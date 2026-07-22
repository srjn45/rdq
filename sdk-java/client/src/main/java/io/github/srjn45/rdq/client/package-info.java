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

/**
 * rdq Java client — the submit-side API.
 *
 * <p>This artifact ({@code io.github.srjn45:rdq-java-client}) lets an
 * application enqueue work without depending on the worker/engine. The
 * envelope model and submit surface land here in later tasks (T7.3+); the
 * package seam is drawn now so "submit here, execute there" never needs a
 * retrofit (design 05, OQ-1).
 */
package io.github.srjn45.rdq.client;
