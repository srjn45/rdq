// SPDX-License-Identifier: Apache-2.0

package rdq

import "github.com/srjn45/rdq/core/policy"

// Decision is the verdict for a failed attempt: retry or dead-letter.
// It is an alias of policy.Decision from core/policy.
type Decision = policy.Decision

// Outcome decisions a handler or OutcomeMapper may return.
const (
	// DecisionRetryable schedules another attempt until MaxAttempts is
	// exhausted. It is the default when no classifier matches.
	DecisionRetryable = policy.DecisionRetryable
	// DecisionPermanent dead-letters the task immediately, skipping all
	// remaining attempts.
	DecisionPermanent = policy.DecisionPermanent
)

// OutcomeMapper is the per-queue authoritative classifier (layer 1 of the
// five-layer classification ladder, design 03 §4). Return (decision, true)
// to claim the error authoritatively and short-circuit every lower layer;
// return (_, false) to defer to the Permanent/Retryable wrapper, code
// classifier, config-glob, and default layers below.
//
// Set OutcomeMapper in QueueSpec.Classifier.Mapper when building the
// QueueSpec for NewWorker.
type OutcomeMapper = policy.OutcomeMapper

// Permanent wraps err so the classifier dead-letters the task without
// further retries, overriding code classifiers and config globs (layer 2
// of the classification ladder, design 03 §4). The wrapped error remains
// fully unwrappable via errors.Is / errors.As.
func Permanent(err error) error { return policy.Permanent(err) }

// Retryable wraps err so the classifier schedules another attempt,
// overriding classifiers that would otherwise mark the failure permanent
// (layer 2 of the ladder). The wrapped error remains unwrappable.
func Retryable(err error) error { return policy.Retryable(err) }
