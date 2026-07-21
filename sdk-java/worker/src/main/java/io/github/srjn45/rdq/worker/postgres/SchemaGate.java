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

import io.github.srjn45.rdq.worker.spi.StorageException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The {@code rdq_schema_version} startup gate (design 02 &sect;4, design 05 G5).
 *
 * <p>The Postgres schema is a cross-language contract: this Java binding and the
 * Go plugin bind to the SAME tables, gated by the same version number. Before an
 * engine touches any row it reads {@code rdq_schema_version} and refuses to run
 * against an unknown (newer) or unmigrated (older) schema rather than corrupting
 * rows it does not understand. Bump {@link #SCHEMA_VERSION} in lockstep with the
 * Go {@code postgres.SchemaVersion} whenever a migration changes the contract.
 */
final class SchemaGate {

    /**
     * The schema-contract version this build understands. Tracks the Go
     * {@code storage/postgres.SchemaVersion}; the T2.1 migrations write this
     * number into {@code rdq_schema_version}.
     */
    static final int SCHEMA_VERSION = 1;

    // "42P01" is the SQLSTATE for undefined_table: rdq_schema_version has not
    // been created, so the migrations have not run.
    private static final String UNDEFINED_TABLE = "42P01";

    private SchemaGate() {
    }

    /**
     * Reads {@code rdq_schema_version} on {@code conn} and verifies this build may
     * run against the database.
     *
     * @throws SchemaNotInitializedException  if the migrations have not been applied
     * @throws SchemaVersionMismatchException if the recorded version differs
     */
    static void verify(Connection conn) {
        int version;
        try (Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT version FROM rdq_schema_version WHERE singleton")) {
            if (!rs.next()) {
                throw new SchemaNotInitializedException();
            }
            version = rs.getInt(1);
        } catch (SQLException ex) {
            if (UNDEFINED_TABLE.equals(ex.getSQLState())) {
                throw new SchemaNotInitializedException();
            }
            throw new StorageException("rdq/postgres: reading schema version", ex);
        }
        check(version);
    }

    /**
     * The pure comparison behind {@link #verify}, split out so the gate logic is
     * unit-testable without a database.
     *
     * @throws SchemaVersionMismatchException if {@code databaseVersion} differs
     *                                        from {@link #SCHEMA_VERSION}
     */
    static void check(int databaseVersion) {
        if (databaseVersion != SCHEMA_VERSION) {
            throw new SchemaVersionMismatchException(databaseVersion, SCHEMA_VERSION);
        }
    }
}
