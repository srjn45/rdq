// SPDX-License-Identifier: Apache-2.0

package http

import "net/http"

// WithMetricsHandler installs a handler for the /metrics endpoint. Pass
// promhttp.HandlerFor(registry.PrometheusRegistry(), promhttp.HandlerOpts{})
// to serve a core/metrics.Registry. The endpoint is unauthenticated (Prometheus
// scrape) and mounted outside the /v1 auth boundary alongside /healthz and /readyz.
func WithMetricsHandler(h http.Handler) Option {
	return func(s *Server) { s.metricsHandler = h }
}

// handleMetrics delegates to the configured metricsHandler, or responds 404
// when no registry has been wired (e.g. embedded or test deployments).
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.metricsHandler == nil {
		Error(w, r, CodeNotFound, WithDetail("/metrics not configured"))
		return
	}
	s.metricsHandler.ServeHTTP(w, r)
}
