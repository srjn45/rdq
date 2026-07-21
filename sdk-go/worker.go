// SPDX-License-Identifier: Apache-2.0

package rdq

import (
	"context"

	"github.com/srjn45/rdq/core/engine"
	"github.com/srjn45/rdq/core/spi"
)

// QueueSpec is the per-queue execution contract passed to NewWorker: the
// effective configuration (lease, retry ladder, concurrency, OutcomeMapper,
// etc.) the worker applies for one queue. It is a type alias of
// engine.QueueSpec; callers that import only sdk-go do not need to reference
// core/engine directly.
type QueueSpec = engine.QueueSpec

// WorkerOption configures a Worker. It is a type alias of engine.Option;
// pass engine.WithClock, engine.WithRNG, etc. directly.
type WorkerOption = engine.Option

// Worker drives the claim-process-outcome loop for one or more queues,
// dispatching claimed tasks to handlers registered via Register. Construct
// with NewWorker; drive with Run.
type Worker struct {
	inner *engine.Worker
}

// NewWorker builds a Worker bound to store, dispatching to the process-level
// handler registry. specs supply the per-queue execution contract; opts are
// forwarded to engine.NewWorker unchanged (use engine.WithClock, engine.WithRNG,
// engine.WithSweepInterval, etc.).
//
// All handler_refs that tasks in specs' queues may carry must be registered
// via Register before Run is called. An unregistered handler_ref causes that
// task to be dead-lettered with error type rdq.Unroutable.
func NewWorker(store spi.Storage, specs []QueueSpec, opts ...WorkerOption) (*Worker, error) {
	inner, err := engine.NewWorker(store, defaultReg, specs, opts...)
	if err != nil {
		return nil, err
	}
	return &Worker{inner: inner}, nil
}

// Run drives the worker until ctx is cancelled, then drains in-flight
// handlers for at most one lease window before returning. It always returns
// nil; the error return exists for future use.
func (w *Worker) Run(ctx context.Context) error {
	return w.inner.Run(ctx)
}
