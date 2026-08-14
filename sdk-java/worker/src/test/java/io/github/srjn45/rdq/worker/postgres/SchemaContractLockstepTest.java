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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-language lockstep guard (issue #54). The Java worker gate
 * ({@link SchemaGate#TASK_CONTRACT_VERSION}) and the Go plugin
 * ({@code storage/postgres.TaskContractVersion}) bind to the SAME task tables
 * and MUST agree on the contract version — otherwise a worker refuses a database
 * the other language happily migrated, which is exactly the drift that shipped
 * twice before this test existed.
 *
 * <p>This reads the Go source directly and needs NO Docker, so it runs on every
 * build (unlike the testcontainers gate tests, which skip without Docker). It is
 * the fast tripwire: if a Go migration bumps a schema constant and the Java
 * binding is not updated in the same change, this fails in seconds.
 */
class SchemaContractLockstepTest {

    private static final Pattern TASK_CONTRACT =
        Pattern.compile("(?m)^const\\s+TaskContractVersion\\s*=\\s*(\\d+)");
    private static final Pattern SCHEMA_VERSION =
        Pattern.compile("(?m)^const\\s+SchemaVersion\\s*=\\s*(\\d+)");

    @Test
    void javaTaskContractMatchesGo() {
        String goSource = readRepoText("storage/postgres/schema.go");
        int goTaskContract = extract(TASK_CONTRACT, goSource, "TaskContractVersion");

        assertThat(SchemaGate.TASK_CONTRACT_VERSION)
            .as("Java SchemaGate.TASK_CONTRACT_VERSION must equal Go storage/postgres.TaskContractVersion "
                + "(bump both in the same change when a migration alters the task tables)")
            .isEqualTo(goTaskContract);
    }

    @Test
    void taskContractDoesNotExceedOverallSchemaVersion() {
        String goSource = readRepoText("storage/postgres/schema.go");
        int goTaskContract = extract(TASK_CONTRACT, goSource, "TaskContractVersion");
        int goSchemaVersion = extract(SCHEMA_VERSION, goSource, "SchemaVersion");

        assertThat(goTaskContract)
            .as("the task-contract version cannot exceed the overall schema version")
            .isLessThanOrEqualTo(goSchemaVersion);
    }

    @Test
    void migrationCountMatchesOverallSchemaVersion() {
        int goSchemaVersion = extract(SCHEMA_VERSION, readRepoText("storage/postgres/schema.go"), "SchemaVersion");
        long upMigrations = countUpMigrations();

        assertThat(upMigrations)
            .as("the number of up-migration files must equal SchemaVersion "
                + "(the migrations are gap-free from 1); if this fails, a migration was added "
                + "without bumping SchemaVersion, or the Java test harnesses have not been updated")
            .isEqualTo((long) goSchemaVersion);
    }

    private static int extract(Pattern pattern, String source, String label) {
        Matcher m = pattern.matcher(source);
        if (!m.find()) {
            throw new AssertionError("could not find `const " + label + "` in storage/postgres/schema.go");
        }
        return Integer.parseInt(m.group(1));
    }

    private static long countUpMigrations() {
        Path dir = findRepoFile("storage/postgres/migrations");
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(Path::getFileName)
                .filter(name -> name != null && name.toString().endsWith(".up.sql"))
                .count();
        } catch (IOException ex) {
            throw new UncheckedIOException("listing migrations in " + dir, ex);
        }
    }

    private static String readRepoText(String relative) {
        Path file = findRepoFile(relative);
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new UncheckedIOException("reading " + file, ex);
        }
    }

    /** Locates a repo-relative path by walking up from the test working directory. */
    private static Path findRepoFile(String relative) {
        Path dir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve(relative);
            if (Files.exists(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException(
            "could not locate " + relative + " from " + System.getProperty("user.dir"));
    }
}
