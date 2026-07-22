// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
)

const usageText = `rdq — rdq ops CLI

Usage:
  rdq [--server URL [--token TOKEN] | --dsn DSN] <command> [flags]

Global flags:
  --server URL    rdq-server base URL (API mode, e.g. http://localhost:8080)
  --token TOKEN   Bearer token for API auth (optional)
  --dsn DSN       Postgres DSN for direct-storage mode

Commands:
  stats <queue>
      Print pending/in-flight/DLQ depth and oldest-pending age.

  dlq list <queue> [--limit N] [--cursor C]
           [--error-type E] [--handler-ref H]
           [--from RFC3339] [--to RFC3339]
      Page the dead-letter queue, optionally filtered.

  dlq inspect <id>
      Print the full envelope (all fields + attempt history) for one task.

  dlq redrive <queue> [--id ID]... [--error-type E] [--handler-ref H]
              [--from RFC3339] [--to RFC3339]
      Move matching DLQ tasks back to PENDING. Supply --id or filter flags.

  dlq purge <queue> [--id ID]... [--error-type E] [--handler-ref H]
            [--from RFC3339] [--to RFC3339]
      Permanently remove matching DLQ tasks. Supply --id or filter flags.

  migrate
      Apply T2.1 schema migrations (direct-storage mode only).
`

// Run is the CLI entry point. It parses global flags, builds a transport, and
// dispatches the subcommand. All output is written to w (os.Stdout in main).
func Run(args []string, w io.Writer) error {
	fs := flag.NewFlagSet("rdq", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		serverURL = fs.String("server", "", "rdq-server base URL (API mode)")
		token     = fs.String("token", "", "Bearer token for API auth")
		dsn       = fs.String("dsn", "", "Postgres DSN (direct-storage mode)")
	)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usageText) }

	if err := fs.Parse(args); err != nil {
		return err
	}

	sub := fs.Args()
	if len(sub) == 0 {
		fs.Usage()
		return fmt.Errorf("command required")
	}

	tr, err := buildTransport(*serverURL, *token, *dsn)
	if err != nil {
		return err
	}
	defer tr.Close() //nolint:errcheck

	ctx := context.Background()

	switch sub[0] {
	case "stats":
		return runStats(ctx, w, tr, sub[1:])
	case "dlq":
		if len(sub) < 2 {
			return fmt.Errorf("dlq requires a subcommand: list, inspect, redrive, purge")
		}
		switch sub[1] {
		case "list":
			return runDLQList(ctx, w, tr, sub[2:])
		case "inspect":
			return runDLQInspect(ctx, w, tr, sub[2:])
		case "redrive":
			return runDLQRedrive(ctx, w, tr, sub[2:])
		case "purge":
			return runDLQPurge(ctx, w, tr, sub[2:])
		default:
			return fmt.Errorf("unknown dlq subcommand %q (want: list, inspect, redrive, purge)", sub[1])
		}
	case "migrate":
		return runMigrate(ctx, w, tr, sub[1:])
	default:
		return fmt.Errorf("unknown command %q", sub[0])
	}
}

// buildTransport selects and constructs the transport based on the global flags.
func buildTransport(serverURL, token, dsn string) (Transport, error) {
	if serverURL != "" && dsn != "" {
		return nil, fmt.Errorf("specify --server or --dsn, not both")
	}
	if serverURL != "" {
		return newAPITransport(serverURL, token)
	}
	if dsn != "" {
		return newDirectTransport(dsn)
	}
	return nil, fmt.Errorf("specify --server URL (API mode) or --dsn DSN (direct-storage mode)")
}
