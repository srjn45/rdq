// SPDX-License-Identifier: Apache-2.0

package http

import (
	"context"
	"encoding/json"
	"net/http"
)

// ReadinessProbe reports whether a dependency is reachable. readyz aggregates
// one probe per critical dependency; a nil error means ready. The storage
// backend supplies the probe that makes /readyz reflect storage reachability
// (design 04 §8), so a rollout waits for a warm instance.
type ReadinessProbe interface {
	Ready(ctx context.Context) error
}

// ProbeFunc adapts a plain function to ReadinessProbe.
type ProbeFunc func(ctx context.Context) error

// Ready implements ReadinessProbe.
func (f ProbeFunc) Ready(ctx context.Context) error { return f(ctx) }

// namedProbe pairs a probe with the dependency name reported in /readyz output.
type namedProbe struct {
	name  string
	probe ReadinessProbe
}

// handleHealthz is liveness: it reports only that the process is up and serving,
// never touching dependencies, so a wedged dependency does not trigger a
// pod restart (design 04 §8). It lives OUTSIDE /v1 and needs no auth (G11).
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz is readiness: it runs every registered probe and reports ready
// only when all pass. Storage unreachable → 503 STORAGE_UNAVAILABLE (with
// Retry-After) so a Kubernetes rollout holds until the instance is warm (design
// 04 §8). It lives OUTSIDE /v1 and needs no auth (G11).
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	checks := make(map[string]string, len(s.probes))
	ready := true
	for _, p := range s.probes {
		if err := p.probe.Ready(r.Context()); err != nil {
			ready = false
			checks[p.name] = err.Error()
		} else {
			checks[p.name] = "ok"
		}
	}
	if !ready {
		WriteProblem(w, NewProblem(CodeStorageUnavailable, r.URL.Path,
			WithDetail("one or more dependencies are not reachable")))
		return
	}
	writeJSON(w, http.StatusOK, readyReport{Status: "ready", Checks: checks})
}

// readyReport is the /readyz success body: an overall status plus a per-check
// map so operators can see which dependencies were probed.
type readyReport struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// writeJSON serialises v as application/json with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
