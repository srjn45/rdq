// SPDX-License-Identifier: Apache-2.0

package rdq

import (
	"context"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/registry"
)

// defaultReg is the process-level handler registry populated by Register.
// It is shared by all NewWorker calls in the same process, so Register must
// be called before NewWorker for each handler_ref the worker will claim.
var defaultReg = registry.New()

// HandlerFunc is the signature a registered task handler implements. A nil
// error is success; a non-nil error is handed to the classification ladder
// (Permanent/Retryable wrappers, OutcomeMapper, config globs, default) to
// decide retry vs permanent failure.
type HandlerFunc func(ctx context.Context, task envelope.Envelope) error

// funcHandler adapts a HandlerFunc to the registry.Handler interface with
// an empty (unpinned) version string so it matches every claimed task
// regardless of handler_version.
type funcHandler struct {
	fn HandlerFunc
}

func (h *funcHandler) Version() string { return "" }
func (h *funcHandler) Handle(ctx context.Context, task envelope.Envelope) error {
	return h.fn(ctx, task)
}

// Register binds fn to name in the process-level handler registry. name is
// used as the handler_ref: tasks enqueued with that HandlerRef are dispatched
// to fn by the worker. Errors:
//
//   - registry.ErrEmptyRef   — name is empty
//   - registry.ErrNilHandler — fn is nil
//   - registry.ErrDuplicateHandler — name is already registered (one-shot)
func Register(name string, fn HandlerFunc) error {
	if fn == nil {
		return registry.ErrNilHandler
	}
	return defaultReg.Register(name, &funcHandler{fn: fn})
}
