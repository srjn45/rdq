// SPDX-License-Identifier: Apache-2.0

// Package audit is the server-side audit sinks (design 06, T6.3). It ships the
// Postgres sink that writes audit records to the rdq_audit table (same DB as
// ConfigStore), making audit history queryable without a separate service. The
// AuditSink interface and the default JSON-log sink live in core/audit; this
// package is the server-only Postgres variant.
package audit

import (
	"context"
	"database/sql"

	coreaudit "github.com/srjn45/rdq/core/audit"
)

// PGSink is an AuditSink that writes records to the rdq_audit table in the
// same Postgres database as the ConfigStore. Call NewPGSink with an open
// *sql.DB (already migrated to schema v3+). The caller owns the DB lifetime.
type PGSink struct {
	db *sql.DB
}

// NewPGSink returns a PGSink backed by db. The rdq_audit table must already
// exist (apply migration 0003_audit.up.sql via storage/postgres.Migrate).
func NewPGSink(db *sql.DB) *PGSink {
	return &PGSink{db: db}
}

const insertAudit = `
INSERT INTO rdq_audit
	(timestamp, principal, action, queue, selector, count, outcome, error_message)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

// Emit inserts one audit record into rdq_audit. It uses a background context
// with no deadline so a slow DB write does not propagate into the request path;
// errors are returned to the caller to log but do not fail the operation.
func (s *PGSink) Emit(r coreaudit.Record) error {
	_, err := s.db.ExecContext(
		context.Background(),
		insertAudit,
		r.Timestamp.UTC(),
		r.Principal,
		string(r.Action),
		r.Queue,
		r.Selector,
		r.Count,
		string(r.Outcome),
		r.ErrorMessage,
	)
	return err
}
