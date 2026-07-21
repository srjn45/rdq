// SPDX-License-Identifier: Apache-2.0

package http

import (
	"net/http"
	"time"

	rdqlog "github.com/srjn45/rdq/core/log"
)

// WithLogger installs the structured logger used for request logging and, when
// threaded into the engine, task transition logs (T6.2). A nil logger disables
// request logging while still propagating trace context. Kept in this file (not
// server.go) so the request-logging concern stays self-contained and out of the
// shared bootstrap that other server tasks edit.
func WithLogger(l *rdqlog.Logger) Option {
	return func(s *Server) { s.logger = l }
}

// withLogging is the outermost middleware. It does two things on every request:
//
//   - Propagates W3C trace context: the inbound `traceparent` header is lifted
//     into the request context so downstream handlers (and the task built by a
//     submit) carry the same trace_id — the server end of submit → retry →
//     handler (T6.2).
//   - Emits one structured access record per request (method, path, status,
//     duration, trace_id). Never logs the body, so payloads stay unlogged
//     (FR-25).
//
// It wraps the whole handler tree (including /healthz and /metrics) so trace
// context is available everywhere; the access log itself is skipped for a nil
// logger.
func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tp := r.Header.Get(rdqlog.HeaderTraceparent)
		ctx := rdqlog.ContextWithTraceparent(r.Context(), tp)
		r = r.WithContext(ctx)

		lg := s.logger.Slog()
		if lg == nil {
			next.ServeHTTP(w, r)
			return
		}

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r)

		attrs := []any{
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
		}
		if tid, sid, ok := rdqlog.ParseTraceparent(tp); ok {
			attrs = append(attrs, rdqlog.KeyTraceID, tid, rdqlog.KeySpanID, sid)
		}
		lg.Info("http.request", attrs...)
	})
}

// statusRecorder captures the response status code for the access log while
// otherwise delegating to the wrapped ResponseWriter. It intentionally does not
// buffer the body — the payload never enters the log (FR-25).
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	// A Write without an explicit WriteHeader implies 200 (already the default).
	r.wroteHeader = true
	return r.ResponseWriter.Write(b)
}
