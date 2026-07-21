// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
)

// runStats implements `rdq stats <queue>`.
func runStats(ctx context.Context, w io.Writer, tr Transport, args []string) error {
	fs := flag.NewFlagSet("rdq stats", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: rdq stats <queue>")
		fmt.Fprintln(os.Stderr, "  Print pending/in-flight/DLQ depth and oldest-pending age for <queue>.")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		fs.Usage()
		return fmt.Errorf("queue name required")
	}
	queue := fs.Arg(0)

	st, err := tr.Stats(ctx, queue)
	if err != nil {
		return fmt.Errorf("stats: %w", err)
	}

	fmt.Fprintf(w, "Queue:               %s\n", queue)
	fmt.Fprintf(w, "Pending:             %d\n", st.Pending)
	fmt.Fprintf(w, "In-flight:           %d\n", st.InFlight)
	fmt.Fprintf(w, "DLQ depth:           %d\n", st.DLQDepth)
	if st.OldestPendingAge > 0 {
		fmt.Fprintf(w, "Oldest pending age:  %s\n", st.OldestPendingAge.Round(0))
	} else {
		fmt.Fprintf(w, "Oldest pending age:  (none)\n")
	}
	return nil
}
