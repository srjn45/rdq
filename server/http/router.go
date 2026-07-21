// SPDX-License-Identifier: Apache-2.0

package http

import "net/http"

// buildHandler wires the top-level routing tree. Two boundaries matter here:
//
//   - Liveness/readiness (/healthz, /readyz) sit OUTSIDE /v1 and its auth (G11,
//     design 04 §8) so probes never need credentials and never depend on the
//     API surface being healthy.
//   - Everything under /v1 sits behind the auth boundary (design 04 §5). Auth
//     itself lands in T5.6; withAuth marks the boundary today so /v1 is never
//     accidentally wired outside it.
//
// Route bodies (data plane, DLQ, admin) land in T5.2–T5.4; this scaffolding owns
// only the tree shape and the error contract for unmatched routes.
func (s *Server) buildHandler() http.Handler {
	mux := http.NewServeMux()

	// Outside the auth boundary — health probes.
	mux.HandleFunc("/healthz", only(http.MethodGet, s.handleHealthz))
	mux.HandleFunc("/readyz", only(http.MethodGet, s.handleReadyz))

	// Inside the auth boundary — the versioned API.
	mux.Handle("/v1/", http.StripPrefix("/v1", s.withAuth(s.v1Handler())))

	// Anything else is an unknown route.
	mux.HandleFunc("/", s.handleNotFound)

	return mux
}

// v1Handler serves the /v1 subtree (prefix already stripped). It is the seam
// T5.2–T5.4 extend with the real routes; until then every path under /v1 is an
// unknown route and returns the same problem+json 404 as the top-level catch-all.
func (s *Server) v1Handler() http.Handler {
	mux := http.NewServeMux()
	// registerV1Routes(mux) — data plane / DLQ / admin land in later tasks.
	mux.HandleFunc("/", s.handleNotFound)
	return mux
}

// withAuth marks the /v1 authentication boundary (design 04 §5). It is a
// pass-through today; T5.6 replaces the body with static-token authN plus
// per-queue×role authZ, emitting UNAUTHENTICATED/FORBIDDEN problems. Keeping the
// seam here guarantees no /v1 route is ever mounted outside auth.
func (s *Server) withAuth(next http.Handler) http.Handler {
	return next // TODO(T5.6): resolve principal + enforce grants.
}

// only restricts a handler to a single HTTP method, emitting a problem+json 405
// (with an Allow header) for anything else — so even the unauthenticated health
// endpoints speak the standard error contract.
func only(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			Error(w, r, CodeMethodNotAllowed,
				WithDetail("only "+method+" is allowed on this path"))
			return
		}
		next(w, r)
	}
}

// handleNotFound is the problem+json 404 for any unmatched path, so clients get
// one consistent error shape instead of the net/http plaintext default.
func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	Error(w, r, CodeNotFound, WithDetail("no route matches "+r.URL.Path))
}
