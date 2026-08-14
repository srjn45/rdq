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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** Postgres setup helpers for engine integration tests. Mirrors {@code TestPostgres}. */
final class EngineTestHelper {

    static final DockerImageName IMAGE = DockerImageName.parse("postgres:16-alpine");

    private EngineTestHelper() {}

    static DataSource dataSource(PostgreSQLContainer<?> container) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(container.getJdbcUrl());
        ds.setUser(container.getUsername());
        ds.setPassword(container.getPassword());
        return ds;
    }

    /**
     * The frozen up-migration sequence, applied in order — matching a production
     * database (all migrations via {@code rdq migrate}) so the exact-match
     * {@code rdq_schema_version} gate is satisfied.
     */
    private static final String[] MIGRATIONS = {
        "0001_init.up.sql",
        "0002_config.up.sql",
        "0003_audit.up.sql",
        "0004_schema_contract.up.sql",
    };

    @SuppressFBWarnings(
        value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
        justification = "frozen trusted migration SQL read from repo")
    static void applyMigrations(DataSource ds) {
        try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
            for (String name : MIGRATIONS) {
                st.execute(readMigration(name));
            }
        } catch (SQLException ex) {
            throw new RuntimeException("applying migrations", ex);
        }
    }

    static void truncate(DataSource ds) {
        try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
            st.execute("TRUNCATE rdq_task, rdq_dlq_task, rdq_attempt RESTART IDENTITY");
        } catch (SQLException ex) {
            throw new RuntimeException("truncating rdq tables", ex);
        }
    }

    private static String readMigration(String name) {
        Path file = findRepoFile("storage/postgres/migrations/" + name);
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new UncheckedIOException("reading migration " + file, ex);
        }
    }

    private static Path findRepoFile(String relative) {
        Path dir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve(relative);
            if (Files.isRegularFile(candidate)) return candidate;
            dir = dir.getParent();
        }
        throw new IllegalStateException("could not locate " + relative);
    }
}
