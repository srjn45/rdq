// SPDX-License-Identifier: Apache-2.0

// Package http is the rdq-server REST surface: a /v1 router, the RFC 9457
// application/problem+json error contract with stable machine codes, and the
// out-of-band liveness/readiness endpoints (design 04). The OpenAPI spec at
// server/openapi.yaml is the normative contract; this package implements it.
//
// This is the T5.1 scaffolding: routing tree, error model, health, and spec.
// The data plane (T5.2), DLQ/ops (T5.3), admin (T5.4), callbacks (T5.5), and
// auth (T5.6) mount onto the seams established here.
package http

import (
	"net/http"
	"sync"

	"github.com/srjn45/rdq/core/spi"
	"github.com/srjn45/rdq/server/auth"
	srvconfig "github.com/srjn45/rdq/server/config"
)

// Server is the assembled HTTP surface. Construct it with New and mount its
// Handler on an http.Server (lifecycle/graceful-drain wiring lands with the
// server binary, design 04 §8).
type Server struct {
	probes          []namedProbe
	handler         http.Handler
	storage         spi.Storage
	configStore     srvconfig.Store  // T5.4: queue config CRUD + pause persistence
	authz           *auth.Authorizer // T5.6: /v1 authN/Z; nil ⇒ open boundary (dev/embedded)
	maxPayloadBytes int64
	metricsHandler  http.Handler // /metrics — set via WithMetricsHandler (T6.1)
	paused          sync.Map     // set[queue] → struct{}; in-process cache (ops.go)
}

// Option configures a Server at construction.
type Option func(*Server)

// WithReadinessProbe registers a named readiness check evaluated by /readyz.
// The storage backend supplies the probe that makes readiness reflect storage
// reachability (design 04 §8). Multiple probes may be registered; readyz reports
// ready only when all pass.
func WithReadinessProbe(name string, probe ReadinessProbe) Option {
	return func(s *Server) {
		s.probes = append(s.probes, namedProbe{name: name, probe: probe})
	}
}

// WithStorage injects the task storage backend used by the data plane (T5.2).
func WithStorage(st spi.Storage) Option {
	return func(s *Server) { s.storage = st }
}

// WithConfigStore injects the ConfigStore used by the admin plane (T5.4).
// When set, pause/resume state is persisted there and IsPaused reads from it.
func WithConfigStore(cs srvconfig.Store) Option {
	return func(s *Server) { s.configStore = cs }
}

// WithAuthorizer enables the /v1 authN/Z boundary (T5.6, design 04 §5). When
// set, every /v1 request must carry a valid bearer token and hold the
// per-queue×role grant its operation requires; when unset, withAuth stays a
// pass-through (dev/embedded mode) so the boundary is opt-in.
func WithAuthorizer(a *auth.Authorizer) Option {
	return func(s *Server) { s.authz = a }
}

// WithMaxPayloadBytes overrides the server-wide decoded-payload ceiling (default
// defaultMaxPayloadBytes). Per-queue limits layer on top once T5.4 lands.
func WithMaxPayloadBytes(n int64) Option {
	return func(s *Server) { s.maxPayloadBytes = n }
}

// New builds a Server with the given options and wires the routing tree.
func New(opts ...Option) *Server {
	s := &Server{maxPayloadBytes: defaultMaxPayloadBytes}
	for _, opt := range opts {
		opt(s)
	}
	s.handler = s.buildHandler()
	return s
}

// Handler returns the root http.Handler for the whole API surface.
func (s *Server) Handler() http.Handler { return s.handler }

// ServeHTTP lets a Server be used directly as an http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}
