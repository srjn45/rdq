// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
)

// runMigrate implements `rdq migrate` (G17).
// It applies the T2.1 Postgres schema migrations. This command is only
// available in direct-storage mode (--dsn); API mode has no migration endpoint.
func runMigrate(ctx context.Context, w io.Writer, tr Transport, args []string) error {
	fs := flag.NewFlagSet("rdq migrate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: rdq migrate")
		fmt.Fprintln(os.Stderr, "  Apply T2.1 schema migrations (requires --dsn; not available in API mode).")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	m, ok := tr.(Migrator)
	if !ok {
		return fmt.Errorf("migrate is only available in direct-storage mode (--dsn); use --dsn instead of --server")
	}

	if err := m.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	fmt.Fprintln(w, "migrations applied")
	return nil
}
