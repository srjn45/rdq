// SPDX-License-Identifier: Apache-2.0

package compliance_test

import (
	"testing"

	"github.com/srjn45/rdq/core/compliance"
	"github.com/srjn45/rdq/core/memstore"
	"github.com/srjn45/rdq/core/spi"
)

// TestMemstoreCompliance runs the full compliance kit against the in-memory
// reference store — the kit's first subject (design 02 §3, backlog T1.7). This is
// the same call a third-party plugin makes from its own test package, so a green
// run here is also the template for wiring the kit against Postgres (M2) and Java
// (M7).
func TestMemstoreCompliance(t *testing.T) {
	compliance.Run(t, func() spi.Storage { return memstore.New() })
}
