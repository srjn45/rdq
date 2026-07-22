// SPDX-License-Identifier: Apache-2.0

package audit

import (
	"context"
	"io"
	"log/slog"
)

// LogSink is the default AuditSink for embedded mode: it writes one structured
// JSON log line per record using log/slog, matching the core/log style (T6.2).
// Use NewLogSink(os.Stderr) as the production default when no Postgres store is
// available.
type LogSink struct {
	l *slog.Logger
}

// NewLogSink returns a LogSink writing JSON audit records to w.
func NewLogSink(w io.Writer) *LogSink {
	return &LogSink{
		l: slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}
}

// auditMsg is the slog message for every audit record; the machine-meaningful
// discriminator is the "action" attribute.
const auditMsg = "audit.event"

// Emit writes one structured JSON audit record to the sink's writer.
func (s *LogSink) Emit(r Record) error {
	s.l.LogAttrs(context.Background(), slog.LevelInfo, auditMsg,
		slog.Time("timestamp", r.Timestamp.UTC()),
		slog.String("principal", r.Principal),
		slog.String("action", string(r.Action)),
		slog.String("queue", r.Queue),
		slog.String("selector", r.Selector),
		slog.Int("count", r.Count),
		slog.String("outcome", string(r.Outcome)),
		slog.String("error", r.ErrorMessage),
	)
	return nil
}
