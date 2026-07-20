// SPDX-License-Identifier: Apache-2.0

package envelope

import "fmt"

// Outcome is the result recorded for a single attempt (design 01 §2). Adding a
// value is a breaking change — readers switch on it exhaustively (design 01 §5).
//
// OutcomeLeaseExpired is recorded when a claim's lease lapses without a reported
// outcome; it counts against max_attempts (poison-pill protection).
type Outcome string

const (
	OutcomeSuccess          Outcome = "SUCCESS"
	OutcomeRetryableFailure Outcome = "RETRYABLE_FAILURE"
	OutcomePermanentFailure Outcome = "PERMANENT_FAILURE"
	OutcomeLeaseExpired     Outcome = "LEASE_EXPIRED"
)

// Valid reports whether o is one of the defined Outcome values.
func (o Outcome) Valid() bool {
	switch o {
	case OutcomeSuccess, OutcomeRetryableFailure, OutcomePermanentFailure, OutcomeLeaseExpired:
		return true
	default:
		return false
	}
}

// ParseOutcome returns the Outcome named by raw, rejecting any unknown value.
func ParseOutcome(raw string) (Outcome, error) {
	o := Outcome(raw)
	if !o.Valid() {
		return "", fmt.Errorf("envelope: invalid outcome %q", raw)
	}
	return o, nil
}
