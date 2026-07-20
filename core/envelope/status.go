// SPDX-License-Identifier: Apache-2.0

package envelope

import "fmt"

// Status is the lifecycle state of a task (design 01 §2). Adding a value is a
// breaking change — readers switch on it exhaustively (design 01 §5).
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusInFlight  Status = "IN_FLIGHT"
	StatusSucceeded Status = "SUCCEEDED"
	StatusDead      Status = "DEAD"
)

// Valid reports whether s is one of the defined Status values.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusInFlight, StatusSucceeded, StatusDead:
		return true
	default:
		return false
	}
}

// ParseStatus returns the Status named by raw, rejecting any unknown value.
func ParseStatus(raw string) (Status, error) {
	s := Status(raw)
	if !s.Valid() {
		return "", fmt.Errorf("envelope: invalid status %q", raw)
	}
	return s, nil
}
