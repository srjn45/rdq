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

	"github.com/srjn45/rdq/core/spi"
)

// Server is the assembled HTTP surface. Construct it with New and mount its
// Handler on an http.Server (lifecycle/graceful-drain wiring lands with the
// server binary, design 04 §8).
type Server struct {
	probes          []namedProbe
	handler         http.Handler
	storage         spi.Storage
	maxPayloadBytes int64
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
