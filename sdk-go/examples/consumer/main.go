// SPDX-License-Identifier: Apache-2.0

// Command consumer is a runnable end-to-end demo of the rdq Go SDK: it submits
// one task, watches the handler fail on every attempt, and then prints the DLQ
// entry with its full attempt history once the task exhausts its retry budget.
//
// Prerequisites: a running Postgres instance with the rdq schema applied (or let
// the demo apply it automatically).
//
//	RDQ_POSTGRES_DSN=postgres://rdq:rdq@localhost:5432/rdq go run .
//
// Set RDQ_SKIP_DOCKER=1 to skip the testcontainers-based e2e test in CI
// environments without a container runtime.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/policy"
	"github.com/srjn45/rdq/core/registry"
	rdq "github.com/srjn45/rdq/sdk-go"
	"github.com/srjn45/rdq/sdk-go/submit"
	pgstore "github.com/srjn45/rdq/storage/postgres"
)

const (
	demoQueue      = "example.demo"
	demoHandlerRef = "example.flaky"

	// maxAttempts is small so the demo finishes quickly.
	maxAttempts = 3
)

func main() {
	dsn := os.Getenv("RDQ_POSTGRES_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "usage: RDQ_POSTGRES_DSN=postgres://rdq:rdq@localhost:5432/rdq go run .")
		os.Exit(1)
	}

	db, err := pgstore.Open(dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := pgstore.Migrate(ctx, db); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	store := pgstore.New(db)
	taskID, err := runDemo(ctx, store)
	if err != nil {
		log.Fatalf("demo: %v", err)
	}

	env, err := store.Get(ctx, taskID)
	if err != nil {
		log.Fatalf("get task: %v", err)
	}
	printResult(env)
}

// runDemo registers the flaky handler, submits one task, and drives the worker
// until the task is dead-lettered. It returns the task id for inspection.
func runDemo(ctx context.Context, store *pgstore.Store) (string, error) {
	// ── 1. Register a handler that always fails (retryable). ─────────────────
	// rdq.Retryable(err) wraps the error so the classification ladder (design 03
	// §4) schedules another attempt instead of dead-lettering immediately.
	err := rdq.Register(demoHandlerRef, func(_ context.Context, t envelope.Envelope) error {
		fmt.Printf("  handler called: attempt #%d for task %s — returning retryable error\n",
			t.AttemptCount+1, t.ID)
		return rdq.Retryable(errors.New("transient: service unavailable"))
	})
	if err != nil && !errors.Is(err, registry.ErrDuplicateHandler) {
		return "", fmt.Errorf("register: %w", err)
	}

	// ── 2. Build and enqueue a task via the submit package. ──────────────────
	// submit.Submit constructs an envelope without touching storage; the caller
	// hands it to the store. An idempotency key makes the submit safely retryable.
	env, err := submit.Submit(demoQueue, demoHandlerRef, []byte(`{"msg":"hello rdq"}`),
		submit.WithIdempotencyKey("demo-task-001"),
		submit.WithHeader("origin", "example/consumer"),
	)
	if err != nil {
		return "", fmt.Errorf("submit: %w", err)
	}
	if err := store.Enqueue(ctx, *env); err != nil {
		return "", fmt.Errorf("enqueue: %w", err)
	}
	fmt.Printf("submitted task %s to queue %q\n", env.ID, env.Queue)

	// ── 3. (Optional) Show the Queue builder producing config.QueueConfig. ───
	// The builder is the code-native alternative to a YAML config file. It does
	// NOT directly produce a QueueSpec for the worker; that assembly step is
	// intentional (config vs runtime are separate). See sdk-go/README.md §Config.
	_, qcfg, _ := rdq.Queue(demoQueue).
		MaxAttempts(maxAttempts).
		InitialBackoff(50 * time.Millisecond).
		Build()
	_ = qcfg // would be saved to disk or passed to a server; unused in this demo

	// ── 4. Build the runtime QueueSpec and start the worker. ─────────────────
	// QueueSpec drives the claim-process-outcome loop: retry budget, backoff
	// ladder, concurrency, lease window, and the classification hook.
	spec := rdq.QueueSpec{
		Queue:       demoQueue,
		MaxAttempts: maxAttempts,
		Backoff: policy.Backoff{
			Initial:    50 * time.Millisecond,
			Multiplier: 2,
			Max:        5 * time.Second,
		},
		// Classifier.Mapper can be used to override retry/permanent decisions at the
		// top of the classification ladder (design 03 §4 layer 1). Here we leave it
		// nil to fall through to the rdq.Retryable wrapper set by the handler.
		Classifier:     policy.Classifier{Mapper: rdq.OutcomeMapper(nil)},
		Lease:          10 * time.Second,
		HandlerTimeout: 9 * time.Second,
		BatchSize:      4,
		Concurrency:    2,
		PollInterval:   20 * time.Millisecond,
	}

	w, err := rdq.NewWorker(store, []rdq.QueueSpec{spec})
	if err != nil {
		return "", fmt.Errorf("new worker: %w", err)
	}

	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = w.Run(wCtx) }()

	// ── 5. Wait for the task to land in the DLQ. ─────────────────────────────
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		e, err := store.Get(ctx, env.ID)
		if err != nil {
			return "", fmt.Errorf("poll Get: %w", err)
		}
		if e.Status == envelope.StatusSucceeded || e.Status == envelope.StatusDead {
			cancel()
			return env.ID, nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return "", errors.New("timed out waiting for task to reach terminal state")
}

// printResult prints the final status and every recorded attempt so the DLQ
// entry with its full attempt history is visible — the T4.4 acceptance criterion.
func printResult(env envelope.Envelope) {
	fmt.Printf("\n── result ───────────────────────────────────────────────\n")
	fmt.Printf("  id           : %s\n", env.ID)
	fmt.Printf("  queue        : %s\n", env.Queue)
	fmt.Printf("  status       : %s\n", env.Status)
	fmt.Printf("  attempt_count: %d\n", env.AttemptCount)
	if len(env.Attempts) == 0 {
		fmt.Println("  attempts     : (none recorded — was Get called with IncludeAttempts?)")
		return
	}
	fmt.Printf("  attempts     :\n")
	for _, a := range env.Attempts {
		msg := ""
		if a.Error != nil {
			msg = a.Error.Message
		}
		fmt.Printf("    #%-2d  outcome=%-22s  error=%q\n", a.AttemptNo, a.Outcome, msg)
	}
}
