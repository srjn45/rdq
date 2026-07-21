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

package io.github.srjn45.rdq.example;

import io.github.srjn45.rdq.client.envelope.Attempt;
import io.github.srjn45.rdq.client.envelope.Envelope;
import io.github.srjn45.rdq.client.envelope.Outcome;
import io.github.srjn45.rdq.worker.postgres.PostgresStorage;
import io.github.srjn45.rdq.worker.spi.Claimed;
import io.github.srjn45.rdq.worker.spi.Storage;
import org.postgresql.ds.PGSimpleDataSource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Subprocess entry point for the T8.2 cross-language e2e integration test.
 *
 * <p>This runner is intentionally a thin direct-SPI client rather than using
 * {@link io.github.srjn45.rdq.worker.engine.Worker}.  The Java Worker computes
 * {@code attempt_no} as {@code task.attemptCount() + 1}, which collides with
 * existing {@code rdq_attempt} rows after a redrive (attempt_count is reset to 0
 * by Redrive but the history rows are preserved — a UNIQUE(task_id, attempt_no)
 * violation).  This is the Java-side equivalent of the T5.7 bug fixed on the Go
 * engine.  Using direct SPI here and computing the correct
 * {@code task.attempts().size() + 1} avoids the collision and proves cross-language
 * wire compatibility independently of the Worker bug.  A follow-up task should
 * apply the T5.7-equivalent fix to Worker.java.
 *
 * <p><b>Required environment variables:</b>
 * <pre>
 *   RDQ_PG_HOST        PostgreSQL host  (e.g. localhost)
 *   RDQ_PG_PORT        PostgreSQL port  (e.g. 5432)
 *   RDQ_PG_DB          Database name    (e.g. rdq)
 *   RDQ_PG_USER        Database user    (e.g. rdq)
 *   RDQ_PG_PASS        Database password
 *   RDQ_QUEUE          Queue name to claim from
 *   RDQ_HANDLER_REF    Handler reference to match (informational; any task claimed)
 *   RDQ_TASK_ID        Task ID to wait for
 * </pre>
 *
 * <p>Exits 0 when the task reaches SUCCEEDED, 1 on timeout or error.
 */
public final class CrossLangWorkerRunner {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);
    private static final Duration LEASE          = Duration.ofSeconds(10);
    private static final Duration TIMEOUT        = Duration.ofSeconds(90);

    private CrossLangWorkerRunner() {}

    @SuppressWarnings("SystemExitOutsideMain")
    public static void main(String[] args) {
        String host       = requireEnv("RDQ_PG_HOST");
        String port       = requireEnv("RDQ_PG_PORT");
        String db         = requireEnv("RDQ_PG_DB");
        String user       = requireEnv("RDQ_PG_USER");
        String pass       = requireEnv("RDQ_PG_PASS");
        String queue      = requireEnv("RDQ_QUEUE");
        String taskId     = requireEnv("RDQ_TASK_ID");

        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl("jdbc:postgresql://" + host + ":" + port + "/" + db);
        ds.setUser(user);
        ds.setPassword(pass);

        Storage store = PostgresStorage.open(ds);

        System.out.printf("[crosslang-runner] connected to postgres %s:%s/%s; "
            + "waiting for task %s on queue %s%n", host, port, db, taskId, queue);

        Instant deadline = Instant.now().plus(TIMEOUT);
        try {
            while (Instant.now().isBefore(deadline)) {
                List<Claimed> claimed = store.claimDue(queue, 1, LEASE);
                for (Claimed c : claimed) {
                    Envelope task = c.task();
                    if (!taskId.equals(task.id())) {
                        // Unexpected task: abandon (lease will expire naturally).
                        System.out.printf("[crosslang-runner] ignoring unexpected task %s%n", task.id());
                        continue;
                    }

                    // Compute attempt_no as the next position in the MONOTONIC history
                    // sequence — NOT task.attemptCount()+1, which resets to 0 after a
                    // redrive and would collide with existing attempt rows (T5.7-class bug).
                    int historySize = task.attempts() != null ? task.attempts().size() : 0;
                    int attemptNo   = historySize + 1;

                    Instant now = Instant.now();
                    Attempt att = Attempt.builder()
                        .attemptNo(attemptNo)
                        .startedAt(now)
                        .finishedAt(now)
                        .outcome(Outcome.SUCCESS)
                        .build();

                    store.complete(task.id(), c.token(), att);

                    System.out.printf("[crosslang-runner] task %s SUCCEEDED "
                        + "(attempt_no=%d, history_before=%d)%n",
                        task.id(), attemptNo, historySize);
                    System.exit(0);
                }

                Thread.sleep(POLL_INTERVAL.toMillis());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[crosslang-runner] interrupted");
            System.exit(1);
        } catch (Exception e) {
            System.err.printf("[crosslang-runner] error: %s%n", e);
            System.exit(1);
        }

        System.err.printf("[crosslang-runner] TIMEOUT: task %s did not appear "
            + "within %s on queue %s%n", taskId, TIMEOUT, queue);
        System.exit(1);
    }

    private static String requireEnv(String name) {
        String val = System.getenv(name);
        if (val == null || val.isEmpty()) {
            throw new IllegalStateException("Required env var not set: " + name);
        }
        return val;
    }
}
