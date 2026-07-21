// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/srjn45/rdq/core/config"
	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/policy"
	"github.com/srjn45/rdq/core/registry"
	"github.com/srjn45/rdq/core/spi"
)

// errorTypeMaxAttempts is the error.type stamped on the terminal attempt when a
// task is dead-lettered for exhausting max_attempts without a completed run — the
// poison-pill case where a task only ever expired its lease (design 03 §4).
const errorTypeMaxAttempts = "rdq.MaxAttemptsExceeded"

// Default sweeper cadence when the caller does not override it (G19). The sweeper
// is a low-priority janitor; a jittered ~30s tick keeps SUCCEEDED-task retention
// (task_ttl) enforced without hammering the store.
const (
	defaultSweepInterval = 30 * time.Second
	defaultSweepJitter   = 0.2
)

// QueueSpec is the resolved per-queue execution contract the worker runs against:
// the effective config values (already merged and validated) turned into concrete
// engine inputs. Build one by hand in a test, or via SpecFromConfig from a
// resolved *config.QueueConfig.
type QueueSpec struct {
	Queue string

	// MaxAttempts caps total attempts before dead-lettering. Attempts include
	// LEASE_EXPIRED reclaims (poison-pill protection), so a task that never
	// completes a run still exhausts.
	MaxAttempts int
	// Backoff is the retry ladder used to compute next_attempt_at on a retryable
	// failure.
	Backoff policy.Backoff
	// Classifier resolves a failed attempt to retry-vs-permanent (design 03 §4).
	Classifier policy.Classifier

	// Lease is the visibility timeout requested from ClaimDue.
	Lease time.Duration
	// HandlerTimeout bounds a single handler invocation; it must be <= Lease so a
	// handler cannot still be running past the point its task becomes reclaimable.
	HandlerTimeout time.Duration
	// Heartbeat, when true, extends the lease via ExtendLease while a handler runs
	// so a long job keeps its claim.
	Heartbeat bool

	// BatchSize is the ClaimDue limit per poll. Concurrency caps simultaneous
	// handler invocations for this queue. PollInterval is the poll cadence when
	// the backend cannot Notify.
	BatchSize    int
	Concurrency  int
	PollInterval time.Duration

	// RateLimit is the optional per-instance token-bucket cap on invocations
	// (G12); nil means unlimited.
	RateLimit *config.Rate

	// VersionPolicy decides what happens on a handler_version mismatch.
	VersionPolicy registry.Policy

	// TTLSucceeded is the retention window for SUCCEEDED tasks the sweeper
	// enforces; a non-positive value disables sweeping for this queue.
	TTLSucceeded time.Duration
}

// SpecFromConfig derives a QueueSpec from a queue's *resolved* config (the value
// config.Config.Resolved returns, with defaults deep-merged). It fills sensible
// fallbacks for optional worker/execution knobs but requires the fields that have
// no safe default: a lease, and a fully specified retry ladder. The Classifier it
// builds carries only the config-glob layer; code classifiers and OutcomeMappers
// are supplied by the SDK at a higher layer.
func SpecFromConfig(queue string, qc *config.QueueConfig) (QueueSpec, error) {
	if qc == nil {
		return QueueSpec{}, fmt.Errorf("engine: nil config for queue %q", queue)
	}
	if qc.Execution == nil || qc.Execution.Lease == nil {
		return QueueSpec{}, fmt.Errorf("engine: queue %q missing execution.lease", queue)
	}
	if qc.Retry == nil || qc.Retry.MaxAttempts == nil {
		return QueueSpec{}, fmt.Errorf("engine: queue %q missing retry.max_attempts", queue)
	}
	backoff, ok := policy.BackoffFromConfig(qc.Retry)
	if !ok {
		return QueueSpec{}, fmt.Errorf("engine: queue %q has an incomplete retry backoff ladder", queue)
	}

	spec := QueueSpec{
		Queue:       queue,
		MaxAttempts: *qc.Retry.MaxAttempts,
		Backoff:     backoff,
		Classifier:  policy.Classifier{Config: qc.Classification},
		Lease:       qc.Execution.Lease.Std(),
		// Sensible fallbacks for the optional worker knobs.
		HandlerTimeout: qc.Execution.Lease.Std(),
		BatchSize:      1,
		Concurrency:    1,
		PollInterval:   500 * time.Millisecond,
		VersionPolicy:  registry.PolicyRunLatest,
	}
	if qc.Execution.HandlerTimeout != nil {
		spec.HandlerTimeout = qc.Execution.HandlerTimeout.Std()
	}
	if qc.Execution.Heartbeat != nil {
		spec.Heartbeat = *qc.Execution.Heartbeat
	}
	if qc.Worker != nil {
		if qc.Worker.BatchSize != nil {
			spec.BatchSize = *qc.Worker.BatchSize
		}
		if qc.Worker.Concurrency != nil {
			spec.Concurrency = *qc.Worker.Concurrency
		}
		if qc.Worker.PollInterval != nil {
			spec.PollInterval = qc.Worker.PollInterval.Std()
		}
		spec.RateLimit = qc.Worker.RateLimit
	}
	if qc.Handler != nil && qc.Handler.VersionMismatch != nil {
		spec.VersionPolicy = registry.PolicyFrom(*qc.Handler.VersionMismatch)
	}
	if qc.Limits != nil && qc.Limits.TTLSucceeded != nil {
		spec.TTLSucceeded = qc.Limits.TTLSucceeded.Std()
	}
	return spec, nil
}

// queueState is a QueueSpec plus the worker's live per-queue machinery: a
// concurrency semaphore (a buffered channel of Concurrency slots) and the queue's
// rate limiter.
type queueState struct {
	spec    QueueSpec
	sem     chan struct{}
	limiter *Limiter
}

// Worker is the claim loop: it polls the store for due tasks across its queues,
// fans them out to registered handlers under a per-queue concurrency cap, and
// resolves each outcome (Complete / Reschedule / DeadLetter). It also runs the
// jittered PurgeSucceeded sweeper (G19). Construct with NewWorker; drive with Run.
type Worker struct {
	store spi.Storage
	reg   *registry.Registry
	clock Clock
	rng   policy.RNG

	queues []*queueState

	sweepInterval time.Duration
	sweepJitter   float64
	drainTimeout  time.Duration

	// abandonHook, if set, is called whenever an outcome write is rejected —
	// almost always ErrStaleClaim, meaning the lease was lost mid-run and the
	// work must be abandoned (the store already reclaimed the task). It exists for
	// observability and tests; nil disables it.
	abandonHook func(task envelope.Envelope, err error)

	// handlers tracks in-flight handler goroutines so Run can drain them on stop.
	// loops tracks the claim loops and the sweeper.
	handlers sync.WaitGroup
	loops    sync.WaitGroup
}

// Option configures a Worker.
type Option func(*Worker)

// WithClock sets the injectable clock (default: the real wall clock). Tests inject
// a fake clock — shared with the store — so timing is deterministic.
func WithClock(c Clock) Option {
	return func(w *Worker) {
		if c != nil {
			w.clock = c
		}
	}
}

// WithRNG sets the jitter source used by the backoff ladder and the sweeper's
// jittered tick (default: the process-global source). The worker guards it so
// concurrent handler goroutines may share a non-thread-safe *rand.Rand.
func WithRNG(r policy.RNG) Option {
	return func(w *Worker) {
		if r != nil {
			w.rng = r
		}
	}
}

// WithSweepInterval overrides the sweeper's base tick (default 30s). A
// non-positive value disables the sweeper entirely.
func WithSweepInterval(d time.Duration) Option {
	return func(w *Worker) { w.sweepInterval = d }
}

// WithSweepJitter sets the sweeper's tick jitter fraction in [0, 1) (default 0.2)
// so a fleet of workers does not sweep in lockstep.
func WithSweepJitter(f float64) Option {
	return func(w *Worker) { w.sweepJitter = f }
}

// WithAbandonHook installs a callback invoked when an outcome write is rejected
// (typically ErrStaleClaim after a lost lease). The task is abandoned regardless;
// the hook is purely for observation.
func WithAbandonHook(fn func(task envelope.Envelope, err error)) Option {
	return func(w *Worker) { w.abandonHook = fn }
}

// NewWorker builds a worker for the given queues. store and reg are required; each
// QueueSpec supplies the effective config for one queue.
func NewWorker(store spi.Storage, reg *registry.Registry, specs []QueueSpec, opts ...Option) (*Worker, error) {
	if store == nil {
		return nil, errors.New("engine: nil store")
	}
	if reg == nil {
		return nil, errors.New("engine: nil registry")
	}
	w := &Worker{
		store:         store,
		reg:           reg,
		clock:         systemClock{},
		sweepInterval: defaultSweepInterval,
		sweepJitter:   defaultSweepJitter,
	}
	for _, opt := range opts {
		opt(w)
	}
	if w.rng == nil {
		w.rng = globalRNG{}
	}
	// Always guard the RNG: it is read from every handler goroutine and the
	// sweeper, and an injected *rand.Rand is not safe for concurrent use.
	w.rng = &syncRNG{r: w.rng}

	for _, spec := range specs {
		if spec.Queue == "" {
			return nil, errors.New("engine: queue spec with empty name")
		}
		if spec.Concurrency < 1 {
			spec.Concurrency = 1
		}
		if spec.BatchSize < 1 {
			spec.BatchSize = 1
		}
		if spec.PollInterval <= 0 {
			spec.PollInterval = 500 * time.Millisecond
		}
		if spec.Lease <= 0 {
			return nil, fmt.Errorf("engine: queue %q has a non-positive lease", spec.Queue)
		}
		if spec.HandlerTimeout <= 0 || spec.HandlerTimeout > spec.Lease {
			spec.HandlerTimeout = spec.Lease
		}
		if spec.MaxAttempts < 1 {
			spec.MaxAttempts = 1
		}
		w.queues = append(w.queues, &queueState{
			spec:    spec,
			sem:     make(chan struct{}, spec.Concurrency),
			limiter: NewLimiter(spec.RateLimit, w.clock),
		})
		if spec.Lease > w.drainTimeout {
			w.drainTimeout = spec.Lease
		}
	}
	if w.drainTimeout <= 0 {
		w.drainTimeout = defaultSweepInterval
	}
	return w, nil
}

// Run drives the worker until ctx is cancelled, then drains: it stops claiming,
// waits for the claim loops and the sweeper to exit, and gives in-flight handlers
// until the drain deadline (one lease) to finish before returning (G10). It always
// returns nil today — the signature leaves room for a fatal error later.
func (w *Worker) Run(ctx context.Context) error {
	w.loops.Add(len(w.queues) + 1)
	for _, q := range w.queues {
		go w.runQueue(ctx, q)
	}
	go w.runSweeper(ctx)

	<-ctx.Done()

	// Graceful drain (G10): the claim loops and sweeper observe ctx.Done and stop;
	// wait for them, then let in-flight handlers finish within the lease.
	w.loops.Wait()
	w.drainHandlers()
	return nil
}

// runQueue is one queue's claim loop: poll immediately, then on every tick, until
// ctx is cancelled (at which point it stops claiming — draining is the caller's
// job).
func (w *Worker) runQueue(ctx context.Context, q *queueState) {
	defer w.loops.Done()

	ticker := w.clock.NewTicker(q.spec.PollInterval)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return
		}
		w.pollOnce(ctx, q)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
		}
	}
}

// pollOnce claims up to the free-slot/batch/rate-limit minimum and dispatches each
// claimed task to a handler goroutine.
func (w *Worker) pollOnce(ctx context.Context, q *queueState) {
	if ctx.Err() != nil {
		return
	}
	free := cap(q.sem) - len(q.sem)
	if free <= 0 {
		return // all concurrency slots busy
	}
	limit := free
	if q.spec.BatchSize < limit {
		limit = q.spec.BatchSize
	}
	// Rate-limit gate: only claim as many tasks as we may immediately dispatch, so
	// a leased task never sits idle burning its lease while waiting for a token.
	// Spending a token per claim over-throttles slightly when fewer tasks are due,
	// which is acceptable and self-correcting.
	limit = w.grantTokens(q, limit)
	if limit <= 0 {
		return
	}

	claimed, err := w.store.ClaimDue(ctx, q.spec.Queue, limit, q.spec.Lease)
	if err != nil {
		return
	}
	for _, c := range claimed {
		// Reserve a slot. This never blocks: len(claimed) <= limit <= the free
		// snapshot, and only handler goroutines (receivers) touch the semaphore
		// concurrently, so available space only grows between snapshot and send.
		q.sem <- struct{}{}
		w.handlers.Add(1)
		go w.process(q, c)
	}
}

// grantTokens consumes up to want rate-limit tokens and returns how many it got.
// An unlimited limiter grants all of them.
func (w *Worker) grantTokens(q *queueState, want int) int {
	granted := 0
	for granted < want && q.limiter.Allow() {
		granted++
	}
	return granted
}

// process runs one claimed task to a durable outcome. Store outcome calls use a
// background context, not the run context, so an outcome is still persisted while
// the worker drains.
func (w *Worker) process(q *queueState, c spi.Claimed) {
	defer w.handlers.Done()
	defer func() { <-q.sem }() // release the concurrency slot

	task := c.Task
	token := c.Token
	ctx := context.Background()

	// Poison-pill guard: a task that already reached max_attempts (e.g. via
	// repeated LEASE_EXPIRED reclaims) is dead-lettered without another run.
	if task.AttemptCount >= q.spec.MaxAttempts {
		w.deadLetter(ctx, q, task, token, &envelope.Error{
			Type:    errorTypeMaxAttempts,
			Message: fmt.Sprintf("max_attempts (%d) reached without a successful attempt", q.spec.MaxAttempts),
		})
		return
	}

	// Route: run the registered handler, or dead-letter (unroutable / version).
	res := w.reg.Resolve(task, q.spec.VersionPolicy)
	if res.Action == registry.ActionDeadLetter {
		w.deadLetter(ctx, q, task, token, res.Error)
		return
	}

	started := w.clock.Now()
	err := w.runHandler(q, res.Handler, task, token)
	finished := w.clock.Now()

	attemptNo := task.AttemptCount + 1

	if err == nil {
		att := envelope.Attempt{
			AttemptNo:  attemptNo,
			StartedAt:  started,
			FinishedAt: ptr(finished),
			Outcome:    envelope.OutcomeSuccess,
		}
		if cerr := w.store.Complete(ctx, task.ID, token, att); cerr != nil {
			w.abandon(task, cerr)
		}
		return
	}

	// Failure: classify, record the attempt, then reschedule or dead-letter.
	errType := policy.ErrorType(err)
	verdict := q.spec.Classifier.Classify(err, errType)
	att := envelope.Attempt{
		AttemptNo:  attemptNo,
		StartedAt:  started,
		FinishedAt: ptr(finished),
		Outcome:    verdict.Decision.Outcome(),
		Error: &envelope.Error{
			Type:    errType,
			Message: err.Error(),
		},
	}

	exhausted := attemptNo >= q.spec.MaxAttempts
	if verdict.Decision == policy.DecisionPermanent || exhausted {
		if derr := w.store.DeadLetter(ctx, task.ID, token, att); derr != nil {
			w.abandon(task, derr)
		}
		return
	}

	// Retryable and attempts remain: schedule the next attempt after the backoff
	// delay. The delay argument is the retry index, which equals this attempt's
	// number (attempt 1 failing schedules the 1st retry, delay = initial_backoff).
	delay := q.spec.Backoff.Delay(attemptNo, w.rng)
	nextAt := finished.Add(delay)
	if rerr := w.store.Reschedule(ctx, task.ID, token, att, nextAt); rerr != nil {
		w.abandon(task, rerr)
	}
}

// runHandler invokes the handler under a handler_timeout deadline and, if enabled,
// a lease-extending heartbeat. The handler context is rooted in context.Background
// — NOT the run context — so a drain lets an in-flight handler finish within its
// lease rather than cancelling it.
func (w *Worker) runHandler(q *queueState, h registry.Handler, task envelope.Envelope, token spi.ClaimToken) error {
	hctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Enforce the handler timeout with the injected clock (context.WithTimeout
	// would bind to the real monotonic clock, defeating a fake clock in tests).
	if q.spec.HandlerTimeout > 0 {
		timer := w.clock.NewTimer(q.spec.HandlerTimeout)
		defer timer.Stop()
		go func() {
			select {
			case <-hctx.Done():
			case <-timer.C():
				cancel()
			}
		}()
	}

	// Heartbeat: extend the lease while the handler runs; a lost lease cancels the
	// handler context so it abandons its work (ErrStaleClaim).
	if q.spec.Heartbeat {
		stop := w.startHeartbeat(hctx, cancel, q, task.ID, token)
		defer stop()
	}

	return invokeHandler(hctx, h, task)
}

// startHeartbeat runs a lease-extending loop until the handler context is done or
// the returned stop func is called. Returns a stop func the caller defers.
func (w *Worker) startHeartbeat(hctx context.Context, cancel context.CancelFunc, q *queueState, id spi.TaskID, token spi.ClaimToken) func() {
	ticker := w.clock.NewTicker(heartbeatInterval(q.spec.Lease))
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-hctx.Done():
				return
			case <-done:
				return
			case <-ticker.C():
				err := w.store.ExtendLease(context.Background(), id, token, q.spec.Lease)
				if errors.Is(err, spi.ErrStaleClaim) {
					cancel() // lease lost — tell the handler to abandon
					return
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			ticker.Stop()
			close(done)
		})
	}
}

// deadLetter records a terminal PERMANENT_FAILURE attempt with e and moves the
// task to the DLQ. Used for routing failures (unroutable / version mismatch) and
// the max-attempts poison-pill guard.
func (w *Worker) deadLetter(ctx context.Context, q *queueState, task envelope.Envelope, token spi.ClaimToken, e *envelope.Error) {
	now := w.clock.Now()
	att := envelope.Attempt{
		AttemptNo:  task.AttemptCount + 1,
		StartedAt:  now,
		FinishedAt: ptr(now),
		Outcome:    envelope.OutcomePermanentFailure,
		Error:      e,
	}
	if err := w.store.DeadLetter(ctx, task.ID, token, att); err != nil {
		w.abandon(task, err)
	}
}

// abandon reports that an outcome write was rejected (the lease was lost and the
// task reclaimed elsewhere). The work is dropped; the store's at-least-once
// guarantee means the task runs again after its lease expires.
func (w *Worker) abandon(task envelope.Envelope, err error) {
	if w.abandonHook != nil {
		w.abandonHook(task, err)
	}
}

// runSweeper is the jittered PurgeSucceeded ticker (G19). It enforces task_ttl by
// removing SUCCEEDED tasks older than their queue's retention window.
func (w *Worker) runSweeper(ctx context.Context) {
	defer w.loops.Done()
	if w.sweepInterval <= 0 {
		return // sweeping disabled
	}
	timer := w.clock.NewTimer(w.jitteredSweep())
	defer func() { timer.Stop() }()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C():
			w.sweepOnce(context.Background())
			timer = w.clock.NewTimer(w.jitteredSweep())
		}
	}
}

// sweepOnce purges expired SUCCEEDED tasks across every queue with a configured
// retention window.
func (w *Worker) sweepOnce(ctx context.Context) {
	now := w.clock.Now()
	for _, q := range w.queues {
		if q.spec.TTLSucceeded <= 0 {
			continue // no retention configured for this queue
		}
		olderThan := now.Add(-q.spec.TTLSucceeded)
		_, _ = w.store.PurgeSucceeded(ctx, q.spec.Queue, olderThan)
	}
}

// jitteredSweep spreads the sweep interval by ±sweepJitter so a fleet does not
// sweep in lockstep.
func (w *Worker) jitteredSweep() time.Duration {
	base := w.sweepInterval
	if w.sweepJitter <= 0 {
		return base
	}
	factor := 1 + w.sweepJitter*(2*w.rng.Float64()-1)
	d := time.Duration(float64(base) * factor)
	if d <= 0 {
		d = base
	}
	return d
}

// drainHandlers waits for in-flight handlers to finish, bounded by the drain
// deadline (one lease). A handler that overruns its lease is left orphaned — its
// lease expires and the task is reclaimed elsewhere (at-least-once).
func (w *Worker) drainHandlers() {
	done := make(chan struct{})
	go func() {
		w.handlers.Wait()
		close(done)
	}()
	timer := w.clock.NewTimer(w.drainTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C():
	}
}

// invokeHandler calls h.Handle, converting a panic into a retryable error so a
// buggy handler dead-letters after max_attempts rather than crashing the worker.
func invokeHandler(ctx context.Context, h registry.Handler, task envelope.Envelope) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return h.Handle(ctx, task)
}

// heartbeatInterval is the lease-extension cadence: a third of the lease, so two
// heartbeats land before expiry even if one is delayed.
func heartbeatInterval(lease time.Duration) time.Duration {
	iv := lease / 3
	if iv <= 0 {
		iv = lease
	}
	if iv <= 0 {
		iv = time.Second
	}
	return iv
}

// syncRNG guards a policy.RNG so it is safe to share across handler goroutines and
// the sweeper (an injected *rand.Rand is not concurrency-safe).
type syncRNG struct {
	mu sync.Mutex
	r  policy.RNG
}

func (s *syncRNG) Float64() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.r.Float64()
}

// globalRNG is the default jitter source: the process-global math/rand, which is
// already safe for concurrent use.
type globalRNG struct{}

func (globalRNG) Float64() float64 { return rand.Float64() }

// ptr returns a pointer to v — a tiny helper for the many *time.Time fields.
func ptr[T any](v T) *T { return &v }
