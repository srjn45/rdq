// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/srjn45/rdq/core/spi"
)

// multiString is a flag.Value that accumulates repeated --id flags.
type multiString []string

func (m *multiString) String() string {
	if m == nil || len(*m) == 0 {
		return ""
	}
	return (*m)[0]
}

func (m *multiString) Set(s string) error {
	*m = append(*m, s)
	return nil
}

// filterFlags holds the shared DLQ filter flags for list, redrive, and purge.
type filterFlags struct {
	errorType  string
	handlerRef string
	fromStr    string
	toStr      string
}

func (ff *filterFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&ff.errorType, "error-type", "", "Filter by error type of the final attempt")
	fs.StringVar(&ff.handlerRef, "handler-ref", "", "Filter by handler ref")
	fs.StringVar(&ff.fromStr, "from", "", "Filter: dead-lettered at or after this RFC3339 time (inclusive)")
	fs.StringVar(&ff.toStr, "to", "", "Filter: dead-lettered before this RFC3339 time (exclusive)")
}

// buildFilter converts filterFlags into a spi.DLQFilter.
func (ff *filterFlags) buildFilter() (spi.DLQFilter, error) {
	f := spi.DLQFilter{
		ErrorType:  ff.errorType,
		HandlerRef: ff.handlerRef,
	}
	if ff.fromStr != "" {
		t, err := time.Parse(time.RFC3339, ff.fromStr)
		if err != nil {
			return spi.DLQFilter{}, fmt.Errorf("--from: must be RFC3339 time: %w", err)
		}
		f.DeadLetteredAfter = &t
	}
	if ff.toStr != "" {
		t, err := time.Parse(time.RFC3339, ff.toStr)
		if err != nil {
			return spi.DLQFilter{}, fmt.Errorf("--to: must be RFC3339 time: %w", err)
		}
		f.DeadLetteredBefore = &t
	}
	return f, nil
}

// buildSelector builds a Selector from the given ids and filter flags.
// ids XOR non-empty filter: supply one or the other, never both.
func buildSelector(ids []string, ff *filterFlags) (spi.Selector, error) {
	hasIDs := len(ids) > 0
	hasFilter := ff.errorType != "" || ff.handlerRef != "" || ff.fromStr != "" || ff.toStr != ""
	if hasIDs && hasFilter {
		return spi.Selector{}, fmt.Errorf("supply --id or filter flags (--error-type, --handler-ref, --from, --to), not both")
	}
	if hasIDs {
		return spi.Selector{IDs: ids}, nil
	}
	if !hasFilter {
		// empty selector: selects nothing, returns count 0
		return spi.Selector{}, nil
	}
	f, err := ff.buildFilter()
	if err != nil {
		return spi.Selector{}, err
	}
	return spi.Selector{Filter: &f}, nil
}

// ──────────────────────────────────────────────────────────── dlq list ──

// runDLQList implements `rdq dlq list <queue> [flags]`.
func runDLQList(ctx context.Context, w io.Writer, tr Transport, args []string) error {
	fs := flag.NewFlagSet("rdq dlq list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		limit  = fs.Int("limit", 20, "Maximum tasks to return per page")
		cursor = fs.String("cursor", "", "Pagination cursor from a prior listing")
		ff     filterFlags
	)
	ff.register(fs)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: rdq dlq list <queue> [flags]")
		fs.PrintDefaults()
	}
	// Extract the queue positional arg before calling fs.Parse so that flags
	// that follow the queue name (e.g. --limit, --error-type) are parsed
	// correctly. Go's flag package stops at the first non-flag argument, so
	// passing args[1:] ensures all flag arguments are seen.
	if len(args) < 1 {
		fs.Usage()
		return fmt.Errorf("queue name required")
	}
	queue := args[0]
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	f, err := ff.buildFilter()
	if err != nil {
		return err
	}

	tasks, next, err := tr.DLQList(ctx, queue, f, spi.Page{Limit: *limit, After: spi.Cursor(*cursor)})
	if err != nil {
		return fmt.Errorf("dlq list: %w", err)
	}

	if len(tasks) == 0 {
		fmt.Fprintln(w, "(no tasks in DLQ)")
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tHANDLER\tATTEMPTS\tREDRIVES\tSTATUS")
	for _, env := range tasks {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\n",
			env.ID, env.HandlerRef, env.AttemptCount, env.RedriveCount, env.Status)
	}
	tw.Flush()

	if next != "" {
		fmt.Fprintf(w, "\nnext-cursor: %s\n", next)
	}
	return nil
}

// ──────────────────────────────────────────────────────── dlq inspect ──

// runDLQInspect implements `rdq dlq inspect <id>`.
func runDLQInspect(ctx context.Context, w io.Writer, tr Transport, args []string) error {
	fs := flag.NewFlagSet("rdq dlq inspect", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: rdq dlq inspect <id>")
		fmt.Fprintln(os.Stderr, "  Print the full envelope (including attempt history) for one task.")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		fs.Usage()
		return fmt.Errorf("task id required")
	}
	id := fs.Arg(0)

	env, err := tr.GetTask(ctx, id)
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(env)
}

// ──────────────────────────────────────────────────────── dlq redrive ──

// runDLQRedrive implements `rdq dlq redrive <queue> [flags]`.
func runDLQRedrive(ctx context.Context, w io.Writer, tr Transport, args []string) error {
	fs := flag.NewFlagSet("rdq dlq redrive", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		ids multiString
		ff  filterFlags
	)
	fs.Var(&ids, "id", "Task ID to redrive (repeatable)")
	ff.register(fs)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: rdq dlq redrive <queue> [--id ID]... [filter flags]")
		fs.PrintDefaults()
	}
	if len(args) < 1 {
		fs.Usage()
		return fmt.Errorf("queue name required")
	}
	queue := args[0]
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	sel, err := buildSelector([]string(ids), &ff)
	if err != nil {
		return err
	}

	n, err := tr.Redrive(ctx, queue, sel)
	if err != nil {
		return fmt.Errorf("redrive: %w", err)
	}
	fmt.Fprintf(w, "redrived: %d\n", n)
	return nil
}

// ────────────────────────────────────────────────────────── dlq purge ──

// runDLQPurge implements `rdq dlq purge <queue> [flags]`.
func runDLQPurge(ctx context.Context, w io.Writer, tr Transport, args []string) error {
	fs := flag.NewFlagSet("rdq dlq purge", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		ids multiString
		ff  filterFlags
	)
	fs.Var(&ids, "id", "Task ID to purge (repeatable)")
	ff.register(fs)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: rdq dlq purge <queue> [--id ID]... [filter flags]")
		fs.PrintDefaults()
	}
	if len(args) < 1 {
		fs.Usage()
		return fmt.Errorf("queue name required")
	}
	queue := args[0]
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	sel, err := buildSelector([]string(ids), &ff)
	if err != nil {
		return err
	}

	n, err := tr.Purge(ctx, queue, sel)
	if err != nil {
		return fmt.Errorf("purge: %w", err)
	}
	fmt.Fprintf(w, "purged: %d\n", n)
	return nil
}
