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

package io.github.srjn45.rdq.worker.postgres;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for the pure schema-version comparison ({@link SchemaGate#check}),
 * the logic behind the startup gate (design 05 G5). These need no database, so
 * they run everywhere; the end-to-end gate against a real Postgres is {@link
 * SchemaGateTest}.
 */
class SchemaGateUnitTest {

    @Test
    void acceptsMatchingVersion() {
        assertThatCode(() -> SchemaGate.check(SchemaGate.SCHEMA_VERSION)).doesNotThrowAnyException();
    }

    @Test
    void rejectsNewerVersionWithUpgradeGuidance() {
        assertThatExceptionOfType(SchemaVersionMismatchException.class)
            .isThrownBy(() -> SchemaGate.check(SchemaGate.SCHEMA_VERSION + 1))
            .withMessageContaining("newer than this engine");
    }

    @Test
    void rejectsOlderVersionWithMigrateGuidance() {
        assertThatExceptionOfType(SchemaVersionMismatchException.class)
            .isThrownBy(() -> SchemaGate.check(SchemaGate.SCHEMA_VERSION - 1))
            .withMessageContaining("older than this engine");
    }

    @Test
    void mismatchExposesDatabaseAndEngineVersions() {
        SchemaVersionMismatchException ex = assertThrows(
            SchemaVersionMismatchException.class, () -> SchemaGate.check(999));
        assertThat(ex.databaseVersion()).isEqualTo(999);
        assertThat(ex.engineVersion()).isEqualTo(SchemaGate.SCHEMA_VERSION);
    }
}
