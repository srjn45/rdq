// SPDX-License-Identifier: Apache-2.0

// Package registry is the worker-side name→handler map and the routing policy
// that decides, for a claimed task, whether to run a handler or dead-letter the
// task (design 05, FR-11–13).
//
// Two failure modes are routed to the DLQ with DISTINCT error classes so an
// operator can tell them apart at 3am:
//
//   - Unroutable — no handler is registered for the task's handler_ref. No
//     amount of retrying makes a missing handler appear on this worker, so the
//     task is dead-lettered immediately. It is NEVER rescheduled: a reschedule
//     would make the task due again at once, and the worker would re-claim,
//     re-fail, and re-schedule in a tight hot-loop (the invariant this package
//     exists to prevent).
//   - Version mismatch — a handler IS registered but its Version() differs from
//     the task's pinned handler_version. What happens then is the queue's
//     version_mismatch policy: run-latest ignores the pin and runs the
//     registered handler, dead-letter refuses to run possibly-wrong code and
//     routes the task to the DLQ.
//
// Resolve is pure and clock-free: it returns a Resolution describing the
// decision, and the worker runtime (T3.6) is what actually invokes the handler
// or calls Storage.DeadLetter. That keeps the routing logic trivially testable
// and lets the store remain the single time authority (G9).
package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/srjn45/rdq/core/envelope"
)

// Distinct error.type classes recorded on the dead-lettering attempt. They are
// deliberately different strings so the DLQ, metrics, and DLQFilter can
// separate "you deployed a worker fleet missing this handler" (Unroutable) from
// "you have a version-pinned task this worker can't honor" (VersionMismatch).
const (
	// ErrorTypeUnroutable is the error.type for a task whose handler_ref has no
	// registered handler.
	ErrorTypeUnroutable = "rdq.Unroutable"
	// ErrorTypeVersionMismatch is the error.type for a task dead-lettered
	// because its handler_version does not match the registered handler under
	// the dead-letter version_mismatch policy.
	ErrorTypeVersionMismatch = "rdq.HandlerVersionMismatch"
)

// Sentinel errors from Register. Callers match with errors.Is.
var (
	// ErrEmptyRef is returned by Register when the handler_ref is empty.
	ErrEmptyRef = errors.New("registry: empty handler_ref")
	// ErrNilHandler is returned by Register when the handler is nil.
	ErrNilHandler = errors.New("registry: nil handler")
	// ErrDuplicateHandler is returned by Register when a handler is already
	// registered under the same handler_ref. Registration is one-shot per name;
	// silently overwriting would make the effective handler depend on init
	// order.
	ErrDuplicateHandler = errors.New("registry: handler_ref already registered")
)

// Handler is the unit of work a worker invokes for a claimed task. It is
// registered under a stable handler_ref and reports the implementation Version
// matched against Envelope.HandlerVersion by the version_mismatch policy.
type Handler interface {
	// Version reports this handler's implementation version (e.g. "v3"). It is
	// compared against Envelope.HandlerVersion; an empty pin on the task always
	// matches. The value is opaque — equality, not ordering, decides a match.
	Version() string

	// Handle executes the task. A nil error is success; a non-nil error is
	// handed to the outcome-classification ladder (T3.3) to decide retry vs
	// permanent failure. The worker owns the surrounding lease and timeout.
	Handle(ctx context.Context, task envelope.Envelope) error
}

// Policy is a queue's handler version_mismatch policy. The string values mirror
// the config constants (design 03 §2) so a resolved config value passes through
// unchanged.
type Policy string

const (
	// PolicyRunLatest ignores a handler_version mismatch and runs the registered
	// (latest) handler anyway.
	PolicyRunLatest Policy = "run-latest"
	// PolicyDeadLetter refuses to run a mismatched handler and dead-letters the
	// task with ErrorTypeVersionMismatch.
	PolicyDeadLetter Policy = "dead-letter"
)

// PolicyFrom maps a config version_mismatch string to a Policy. An empty or
// unrecognized value defaults to PolicyRunLatest: handler_version is optional
// and rarely pinned, so an unconfigured queue keeps draining rather than
// silently mass-dead-lettering. A queue that wants strict pinning sets
// version_mismatch: dead-letter explicitly (design 03 §2).
func PolicyFrom(s string) Policy {
	switch Policy(s) {
	case PolicyDeadLetter:
		return PolicyDeadLetter
	default:
		return PolicyRunLatest
	}
}

// Action is the routing decision Resolve makes for a claimed task.
type Action int

const (
	// ActionRun means invoke Resolution.Handler.
	ActionRun Action = iota
	// ActionDeadLetter means route the task to the DLQ with Resolution.Error
	// and do NOT reschedule it — no retry, no hot-loop.
	ActionDeadLetter
)

// Resolution is the outcome of routing one claimed task. Exactly one of Handler
// (when Action == ActionRun) or Error (when Action == ActionDeadLetter) is set.
type Resolution struct {
	Action  Action
	Handler Handler
	Error   *envelope.Error
}

// Registry is a concurrency-safe name→handler map. The zero value is not
// usable; construct with New. It is read-mostly: handlers are registered at
// startup and looked up on every claim, so lookups take a read lock.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// New returns an empty Registry.
func New() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register binds a handler to a handler_ref. It rejects an empty ref, a nil
// handler, and a duplicate ref (ErrDuplicateHandler) — registration is one-shot
// so the effective handler never depends on init ordering.
func (r *Registry) Register(handlerRef string, h Handler) error {
	if handlerRef == "" {
		return ErrEmptyRef
	}
	if h == nil {
		return ErrNilHandler
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[handlerRef]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateHandler, handlerRef)
	}
	r.handlers[handlerRef] = h
	return nil
}

// Lookup returns the handler registered for handlerRef and whether one exists.
func (r *Registry) Lookup(handlerRef string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[handlerRef]
	return h, ok
}

// Len reports the number of registered handlers.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers)
}

// Resolve decides how a claimed task should be routed under the queue's
// version_mismatch policy. It is pure: no I/O, no clock. The caller invokes the
// returned Handler or dead-letters with the returned Error.
//
// The decision ladder:
//
//  1. No handler for task.HandlerRef → ActionDeadLetter, ErrorTypeUnroutable.
//  2. Handler found and the task pins no version, or the pin equals the
//     handler's Version → ActionRun.
//  3. Handler found but the pinned version differs:
//     - PolicyRunLatest  → ActionRun (pin ignored, latest handler runs).
//     - PolicyDeadLetter → ActionDeadLetter, ErrorTypeVersionMismatch.
func (r *Registry) Resolve(task envelope.Envelope, policy Policy) Resolution {
	h, ok := r.Lookup(task.HandlerRef)
	if !ok {
		return Resolution{
			Action: ActionDeadLetter,
			Error: &envelope.Error{
				Type: ErrorTypeUnroutable,
				Message: fmt.Sprintf(
					"no handler registered for handler_ref %q", task.HandlerRef),
			},
		}
	}

	// An unpinned task, or one whose pin matches, always runs.
	if task.HandlerVersion == "" || task.HandlerVersion == h.Version() {
		return Resolution{Action: ActionRun, Handler: h}
	}

	// Pinned version mismatch: policy decides.
	if PolicyFrom(string(policy)) == PolicyDeadLetter {
		return Resolution{
			Action: ActionDeadLetter,
			Error: &envelope.Error{
				Type: ErrorTypeVersionMismatch,
				Message: fmt.Sprintf(
					"handler_version %q does not match registered handler %q for handler_ref %q",
					task.HandlerVersion, h.Version(), task.HandlerRef),
			},
		}
	}
	return Resolution{Action: ActionRun, Handler: h}
}
