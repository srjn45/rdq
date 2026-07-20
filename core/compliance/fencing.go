// SPDX-License-Identifier: Apache-2.0

// This file implements design 02 §3 invariant 2 (fencing). See claims.go for why
// the invariant bodies live in regular .go files rather than fencing_test.go.
package compliance

import (
	"context"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// testFencing verifies invariant 2 (design 02 §3): once a claim's token is stale
// — because the lease expired and the task was reclaimed — Reschedule, Complete,
// DeadLetter, and ExtendLease with that token all fail with ErrStaleClaim and
// change nothing. A fabricated token that was never issued is likewise rejected.
func testFencing(t *testing.T, factory func() spi.Storage) {
	const queue = "q.fence"
	s := factory()
	ctx := context.Background()
	mustEnqueue(t, s, newPendingTask("t", queue))

	first := mustClaimOne(t, s, queue, shortLease)

	// A token that was never minted is stale from the start.
	assertStaleToken(t, s, "t", first.Token+"-never-issued")

	// Let the lease lapse and reclaim; `first` is now stale.
	time.Sleep(expireWait)
	second := mustClaimOne(t, s, queue, longLease)
	if second.Token == first.Token {
		t.Fatalf("reclaim reused token %q", first.Token)
	}

	// Snapshot state, fire every stale mutation, and require it to be untouched.
	before, err := s.Get(ctx, "t")
	if err != nil {
		t.Fatalf("Get before stale calls: %v", err)
	}
	assertStaleToken(t, s, "t", first.Token)
	after, err := s.Get(ctx, "t")
	if err != nil {
		t.Fatalf("Get after stale calls: %v", err)
	}
	if fingerprint(before) != fingerprint(after) {
		t.Fatalf("stale mutations changed state:\n before %s\n after  %s", fingerprint(before), fingerprint(after))
	}
	if after.Status != envelope.StatusInFlight {
		t.Fatalf("status = %s after stale calls, want IN_FLIGHT (the live second claim)", after.Status)
	}

	// The live (second) token still resolves the task.
	if err := s.Complete(ctx, "t", second.Token, retryAttempt(2, "done")); err != nil {
		t.Fatalf("Complete with live token: %v", err)
	}
}
