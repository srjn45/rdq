// SPDX-License-Identifier: Apache-2.0

// This file implements design 02 §3 invariant 6 (lossless envelope round-trip,
// including unknown-field preservation). See claims.go for why the body lives in
// a regular .go file rather than roundtrip_test.go.
package compliance

import (
	"context"
	"embed"
	"testing"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// fixtures are the frozen T1.2 golden fixtures (design 01 §1), embedded so the
// kit stays self-contained and import-portable — a plugin in another module runs
// Run without any files on disk. They are byte-identical copies of
// core/envelope/testdata/*.json; the round-trip invariant is verified against the
// same canonical bytes the codec and every language binding are held to.
//
//go:embed testdata/*.json
var fixtures embed.FS

// testLosslessRoundTrip verifies invariant 6 (design 02 §3): a task admitted to a
// backend and read back is byte-for-byte identical in canonical form, unknown
// (residual) fields included. Enqueue is only defined for freshly-submitted
// tasks, so only the PENDING fixtures are driven through storage; among them
// unknown_fields.json carries both top-level and per-attempt unknown fields,
// which is the crux of the invariant.
func testLosslessRoundTrip(t *testing.T, factory func() spi.Storage) {
	entries, err := fixtures.ReadDir("testdata")
	if err != nil {
		t.Fatalf("read embedded fixtures: %v", err)
	}

	covered := 0
	sawUnknownFields := false
	for _, e := range entries {
		name := e.Name()
		want, err := fixtures.ReadFile("testdata/" + name)
		if err != nil {
			t.Fatalf("read fixture %s: %v", name, err)
		}
		env, err := envelope.Unmarshal(want)
		if err != nil {
			t.Fatalf("Unmarshal fixture %s: %v", name, err)
		}
		if env.Status != envelope.StatusPending {
			continue // Enqueue admits new PENDING tasks; DEAD fixtures aren't storable this way.
		}

		t.Run(name, func(t *testing.T) {
			s := factory()
			mustEnqueue(t, s, *env)
			got, err := s.Get(context.Background(), env.ID)
			if err != nil {
				t.Fatalf("Get(%s): %v", env.ID, err)
			}
			out, err := envelope.Marshal(&got)
			if err != nil {
				t.Fatalf("Marshal round-tripped envelope: %v", err)
			}
			if string(out) != string(want) {
				t.Fatalf("round-trip not byte-stable\n got: %s\nwant: %s", out, want)
			}
		})

		covered++
		if len(env.Residual) > 0 {
			sawUnknownFields = true
		}
	}

	if covered == 0 {
		t.Fatal("no PENDING fixtures exercised; invariant 6 not covered")
	}
	if !sawUnknownFields {
		t.Fatal("no fixture with unknown top-level fields exercised; residual preservation not covered")
	}
}
