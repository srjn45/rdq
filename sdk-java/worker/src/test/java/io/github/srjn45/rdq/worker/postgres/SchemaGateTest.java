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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Verifies the end-to-end {@code rdq_schema_version} startup gate (design 02
 * &sect;4, design 05 G5): {@link PostgresStorage#open} refuses to run against an
 * unmigrated or version-mismatched database rather than corrupting rows it does
 * not understand. Each case owns a fresh testcontainers Postgres so it fully
 * controls the migration state; the pure comparison lives in {@link
 * SchemaGateUnitTest}. Skipped (not failed) when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class SchemaGateTest {

    @Test
    void open_refusesUninitializedSchema() {
        try (PostgreSQLContainer<?> pg = TestPostgres.container()) {
            pg.start();
            DataSource ds = TestPostgres.dataSource(pg);
            // No migrations applied: rdq_schema_version does not exist.
            assertThatExceptionOfType(SchemaNotInitializedException.class)
                .isThrownBy(() -> PostgresStorage.open(ds));
        }
    }

    @Test
    void open_acceptsMigratedSchema() {
        try (PostgreSQLContainer<?> pg = TestPostgres.container()) {
            pg.start();
            DataSource ds = TestPostgres.dataSource(pg);
            TestPostgres.applyMigrations(ds);
            assertThatCode(() -> PostgresStorage.open(ds)).doesNotThrowAnyException();
        }
    }

    @Test
    void open_refusesMismatchedVersion() throws SQLException {
        try (PostgreSQLContainer<?> pg = TestPostgres.container()) {
            pg.start();
            DataSource ds = TestPostgres.dataSource(pg);
            TestPostgres.applyMigrations(ds);
            // Tamper the recorded version to a value this build does not understand.
            try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
                st.executeUpdate("UPDATE rdq_schema_version SET version = 999 WHERE singleton");
            }
            assertThatExceptionOfType(SchemaVersionMismatchException.class)
                .isThrownBy(() -> PostgresStorage.open(ds));
        }
    }
}
