// SPDX-License-Identifier: Apache-2.0

package policy

import (
	"errors"
	"fmt"

	"github.com/srjn45/rdq/core/config"
	"github.com/srjn45/rdq/core/envelope"
)

// Decision is the terminal retry/permanent verdict for a failed attempt — the
// only two outcomes the classification ladder produces (SUCCESS and
// LEASE_EXPIRED are decided elsewhere, by the worker runtime). It maps to an
// envelope.Outcome via Outcome.
type Decision uint8

const (
	// DecisionRetryable schedules another attempt (until max_attempts); it is
	// also the ladder's default when nothing else matches.
	DecisionRetryable Decision = iota
	// DecisionPermanent dead-letters the task immediately, skipping remaining
	// attempts.
	DecisionPermanent
)

// Outcome projects a Decision onto the attempt Outcome recorded in the envelope
// (design 01 §2).
func (d Decision) Outcome() envelope.Outcome {
	if d == DecisionPermanent {
		return envelope.OutcomePermanentFailure
	}
	return envelope.OutcomeRetryableFailure
}

func (d Decision) String() string {
	switch d {
	case DecisionPermanent:
		return "permanent"
	case DecisionRetryable:
		return "retryable"
	default:
		return fmt.Sprintf("Decision(%d)", uint8(d))
	}
}

// Layer identifies which rung of the precedence ladder produced a Verdict
// (design 03 §4). It exists for logging and tests: the engine acts on the
// Decision, but knowing *why* a failure was classified the way it was is what
// makes a misconfiguration debuggable.
type Layer uint8

const (
	// LayerDefault is the bottom rung: nothing matched, so failure is retryable.
	LayerDefault Layer = iota
	// LayerConfigGlob is layer 4: a classification.retryable/.permanent glob
	// matched the reported error.type. The only rung expressible in YAML.
	LayerConfigGlob
	// LayerCodeClassifier is layer 3: an errors.Is/As CodeRule matched.
	LayerCodeClassifier
	// LayerWrapper is layer 2: the error was wrapped by Permanent/Retryable.
	LayerWrapper
	// LayerOutcomeMapper is layer 1, the top: an OutcomeMapper claimed the error.
	LayerOutcomeMapper
)

func (l Layer) String() string {
	switch l {
	case LayerOutcomeMapper:
		return "outcome-mapper"
	case LayerWrapper:
		return "wrapper"
	case LayerCodeClassifier:
		return "code-classifier"
	case LayerConfigGlob:
		return "config-glob"
	case LayerDefault:
		return "default"
	default:
		return fmt.Sprintf("Layer(%d)", uint8(l))
	}
}

// Verdict is a classification result: the Decision the engine acts on plus the
// Layer that produced it.
type Verdict struct {
	Decision Decision
	Layer    Layer
}

// OutcomeMapper is the top of the ladder (layer 1, FR-28): a per-queue code hook
// that inspects a handler error and either claims it — returning a Decision with
// ok=true, which is authoritative and short-circuits every lower layer — or
// declines with ok=false to defer to the wrapper/code/glob/default layers. A nil
// OutcomeMapper on a Classifier is simply absent.
type OutcomeMapper func(err error) (decision Decision, ok bool)

// Classified is implemented by the per-call wrapper errors of layer 2. The SDK's
// rdq.Permanent / rdq.Retryable (and their engine-side primitives Permanent /
// Retryable below) wrap a handler error in a value whose Classification reports
// the forced Decision; the classifier finds the outermost such wrapper in the
// error chain with errors.As. Custom wrappers may implement this interface too —
// it is the layer-2 contract, not a closed set.
type Classified interface {
	error
	Classification() Decision
}

// wrapped is the concrete layer-2 wrapper behind Permanent / Retryable. It keeps
// the original error unwrappable (errors.Is/As, %w) so lower-layer inspection
// and the error.type derivation still see through to the underlying cause.
type wrapped struct {
	err error
	dec Decision
}

func (w wrapped) Error() string {
	if w.err == nil {
		return ""
	}
	return w.err.Error()
}

func (w wrapped) Unwrap() error            { return w.err }
func (w wrapped) Classification() Decision { return w.dec }

// Permanent wraps err so the classifier dead-letters the failure without further
// retries, overriding code classifiers and config globs (ladder layer 2). It is
// the engine-side primitive the SDK re-exports as rdq.Permanent. The wrapped
// error stays unwrappable, so errors.Is/As against the cause still work.
func Permanent(err error) error { return wrapped{err: err, dec: DecisionPermanent} }

// Retryable wraps err so the classifier keeps retrying the failure, overriding
// code classifiers and config globs that would otherwise mark it permanent
// (ladder layer 2). It is the engine-side primitive behind rdq.Retryable.
func Retryable(err error) error { return wrapped{err: err, dec: DecisionRetryable} }

// CodeRule is one entry of ladder layer 3: a predicate over the Go error chain
// and the Decision to apply when it matches. Build rules with IsRule (for
// sentinel errors, errors.Is) or AsRule (for error types, errors.As); a
// Classifier tries its rules in slice order and the first match wins, so order
// them most-specific-first.
type CodeRule struct {
	match    func(error) bool
	decision Decision
}

// IsRule matches when the error chain contains target (errors.Is) — the tool for
// sentinel errors such as context.DeadlineExceeded or a package's Err… value.
func IsRule(target error, decision Decision) CodeRule {
	return CodeRule{
		decision: decision,
		match:    func(err error) bool { return errors.Is(err, target) },
	}
}

// AsRule matches when the error chain contains a value of type T (errors.As) —
// the tool for error *types*, e.g. AsRule[*net.OpError](DecisionRetryable) or
// AsRule[net.Error](DecisionRetryable) for an interface. A fresh target is
// allocated per call, so a shared Classifier is safe under concurrent Classify.
func AsRule[T error](decision Decision) CodeRule {
	return CodeRule{
		decision: decision,
		match: func(err error) bool {
			var target T
			return errors.As(err, &target)
		},
	}
}

// Classifier resolves a failed attempt to a Verdict through the precedence
// ladder of design 03 §4:
//
//  1. OutcomeMapper       (code, authoritative)
//  2. Permanent/Retryable per-call wrappers (code)
//  3. CodeRules           (errors.Is / errors.As)
//  4. config globs         on the reported error.type
//  5. default              → retryable
//
// The zero Classifier (no mapper, no rules, nil config) collapses to layers 4–5,
// and with a nil config to the bare default — which is exactly a server-mode
// classifier before any classification block is set. It carries no mutable state
// and is safe to share across worker goroutines.
type Classifier struct {
	// Mapper is layer 1; nil means absent.
	Mapper OutcomeMapper
	// CodeRules is layer 3, tried in order; empty means absent.
	CodeRules []CodeRule
	// Config is layer 4; nil means no globs (only the default remains).
	Config *config.ClassificationConfig
}

// Classify walks the ladder for a failed attempt and returns the winning
// Verdict. errType is the *reported* error.type — the string that will be
// written to the attempt's error.type (design 05, G6: an SDK-supplied name, else
// ErrorType(err)). Only layer 4 consults it; passing "" simply means no glob can
// match. In server/callback mode there is no Go error: pass err == nil, leave
// Mapper/CodeRules empty, and classification reduces to globs-then-default over
// errType.
func (c Classifier) Classify(err error, errType string) Verdict {
	// Layer 1 — OutcomeMapper is authoritative when it claims the error.
	if c.Mapper != nil {
		if d, ok := c.Mapper(err); ok {
			return Verdict{Decision: d, Layer: LayerOutcomeMapper}
		}
	}
	// Layer 2 — a per-call Permanent/Retryable wrapper anywhere in the chain.
	// errors.As returns the outermost match, so the most recent wrapper wins.
	var w Classified
	if errors.As(err, &w) {
		return Verdict{Decision: w.Classification(), Layer: LayerWrapper}
	}
	// Layer 3 — code classifiers, first match in declared order.
	for _, r := range c.CodeRules {
		if r.match(err) {
			return Verdict{Decision: r.decision, Layer: LayerCodeClassifier}
		}
	}
	// Layer 4 — config globs on the reported error.type.
	if d, ok := matchGlobs(c.Config, errType); ok {
		return Verdict{Decision: d, Layer: LayerConfigGlob}
	}
	// Layer 5 — default: a failure with no explicit classification is retryable.
	return Verdict{Decision: DecisionRetryable, Layer: LayerDefault}
}

// matchGlobs applies only ladder layer 4: the config globs matched against
// errType. When a type matches globs in *both* lists, permanent wins — an
// explicit "never retry this" is a safety decision (it bounds a poison pill)
// that must not be silently undone by an also-matching retryable glob — so the
// permanent list is tested first. Returns ok=false when nothing matches, so the
// caller falls through to the default.
func matchGlobs(cfg *config.ClassificationConfig, errType string) (Decision, bool) {
	if cfg == nil {
		return DecisionRetryable, false
	}
	for _, pattern := range cfg.Permanent {
		if Glob(pattern, errType) {
			return DecisionPermanent, true
		}
	}
	for _, pattern := range cfg.Retryable {
		if Glob(pattern, errType) {
			return DecisionRetryable, true
		}
	}
	return DecisionRetryable, false
}

// ErrorType returns the language-native type name recorded as an attempt's
// error.type when no classifier supplies a name (design 05, G6): the %T of the
// innermost unwrapped error. It follows single-error Unwrap chains to the leaf,
// so a fmt.Errorf("…: %w", cause) — or a Permanent/Retryable wrapper — reports
// the cause's concrete type rather than the wrapper's. A nil error yields "".
func ErrorType(err error) string {
	if err == nil {
		return ""
	}
	for {
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			break
		}
		inner := u.Unwrap()
		if inner == nil {
			break
		}
		err = inner
	}
	return fmt.Sprintf("%T", err)
}
