// SPDX-License-Identifier: Apache-2.0

// Command rdq-server is the rdq central retry hub: REST intake, DLQ & admin
// APIs, HTTP callback delivery, wired to a PostgreSQL backend (design 04).
//
// # Configuration
//
//	RDQ_DSN   — PostgreSQL DSN (required). Example: postgres://u:p@host/db
//	RDQ_ADDR  — listen address (default :8080)
//
// The server applies schema migrations on start and refuses to run against a
// schema version it does not understand (design 02 §4, G5).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	srvhttp "github.com/srjn45/rdq/server/http"
	srvconfig "github.com/srjn45/rdq/server/config"
	"github.com/srjn45/rdq/storage/postgres"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "rdq-server:", err)
		os.Exit(1)
	}
}

func run() error {
	dsn := os.Getenv("RDQ_DSN")
	if dsn == "" {
		return errors.New("RDQ_DSN environment variable is required")
	}
	addr := os.Getenv("RDQ_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := postgres.Open(dsn)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()

	if err := postgres.Migrate(ctx, db); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	store := postgres.New(db)
	cfgStore := srvconfig.NewMemStore()

	srv := srvhttp.New(
		srvhttp.WithStorage(store),
		srvhttp.WithConfigStore(cfgStore),
		srvhttp.WithReadinessProbe("postgres", srvhttp.ProbeFunc(func(c context.Context) error {
			return db.PingContext(c)
		})),
	)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	httpSrv := &http.Server{Handler: srv.Handler()}
	slog.Info("rdq-server started", "addr", ln.Addr())

	errc := make(chan error, 1)
	go func() { errc <- httpSrv.Serve(ln) }()

	select {
	case <-ctx.Done():
		slog.Info("shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return httpSrv.Shutdown(shutCtx)
	case err := <-errc:
		return err
	}
}
