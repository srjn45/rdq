// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// SchemaVersion is the version of the rdq Postgres schema this build understands
// (design 05 G5). The value is written into the rdq_schema_version row by the
// migrations; CheckSchemaVersion refuses to run an engine against a database
// carrying a different version. Bump it whenever a migration changes the schema
// contract — the Java Postgres binding (T7.4) tracks the same number.
//
// v1 → v2: added rdq_queue_config (ConfigStore, T5.4, design 04 §3).
// v2 → v3: added rdq_audit (audit log, T6.3, design 06).
const SchemaVersion = 3

// migrationLedger is the runner's bookkeeping table: which migration files have
// been applied. It is distinct from rdq_schema_version (the startup gate) —
// this table tracks the runner, rdq_schema_version records the contract version.
const migrationLedger = "rdq_migration"

// Sentinel errors returned by the schema-version gate (design 02 §4, G5).
var (
	// ErrSchemaNotInitialized means rdq_schema_version is absent — the
	// migrations have not been applied to this database.
	ErrSchemaNotInitialized = errors.New("rdq/postgres: schema not initialized (run migrations)")
	// ErrSchemaVersionMismatch means the database schema version differs from
	// SchemaVersion; the engine refuses to run against it (G5).
	ErrSchemaVersionMismatch = errors.New("rdq/postgres: schema version mismatch")
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migration is one versioned up/down pair loaded from the embedded migrations/
// directory (files 000N_name.up.sql and 000N_name.down.sql).
type migration struct {
	version int
	name    string
	up      string
	down    string
}

// loadMigrations parses the embedded migration files into an ordered slice,
// verifying that every migration has both an up and a down script and that
// versions are unique and gap-free from 1.
func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("rdq/postgres: reading embedded migrations: %w", err)
	}

	byVersion := map[int]*migration{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		version, name, direction, err := parseMigrationName(e.Name())
		if err != nil {
			return nil, err
		}
		body, err := fs.ReadFile(migrationFS, "migrations/"+e.Name())
		if err != nil {
			return nil, fmt.Errorf("rdq/postgres: reading %s: %w", e.Name(), err)
		}
		m := byVersion[version]
		if m == nil {
			m = &migration{version: version, name: name}
			byVersion[version] = m
		}
		if m.name != name {
			return nil, fmt.Errorf("rdq/postgres: migration %04d has conflicting names %q and %q", version, m.name, name)
		}
		switch direction {
		case "up":
			m.up = string(body)
		case "down":
			m.down = string(body)
		}
	}

	migs := make([]migration, 0, len(byVersion))
	for _, m := range byVersion {
		if strings.TrimSpace(m.up) == "" {
			return nil, fmt.Errorf("rdq/postgres: migration %04d (%s) is missing its up script", m.version, m.name)
		}
		if strings.TrimSpace(m.down) == "" {
			return nil, fmt.Errorf("rdq/postgres: migration %04d (%s) is missing its down script", m.version, m.name)
		}
		migs = append(migs, *m)
	}
	sort.Slice(migs, func(i, j int) bool { return migs[i].version < migs[j].version })

	for i, m := range migs {
		if m.version != i+1 {
			return nil, fmt.Errorf("rdq/postgres: migration versions must be gap-free from 1; got %d at position %d", m.version, i+1)
		}
	}
	return migs, nil
}

// parseMigrationName splits "0001_init.up.sql" into (1, "init", "up").
func parseMigrationName(filename string) (version int, name, direction string, err error) {
	base := strings.TrimSuffix(filename, ".sql")
	dot := strings.LastIndex(base, ".")
	if dot < 0 {
		return 0, "", "", fmt.Errorf("rdq/postgres: migration %q missing .up/.down suffix", filename)
	}
	direction = base[dot+1:]
	if direction != "up" && direction != "down" {
		return 0, "", "", fmt.Errorf("rdq/postgres: migration %q has unknown direction %q", filename, direction)
	}
	rest := base[:dot]
	underscore := strings.Index(rest, "_")
	if underscore < 0 {
		return 0, "", "", fmt.Errorf("rdq/postgres: migration %q missing NNNN_name prefix", filename)
	}
	version, err = strconv.Atoi(rest[:underscore])
	if err != nil {
		return 0, "", "", fmt.Errorf("rdq/postgres: migration %q has non-numeric version: %w", filename, err)
	}
	name = rest[underscore+1:]
	return version, name, direction, nil
}

// Open connects to Postgres via the pgx database/sql driver. The caller owns the
// returned *sql.DB and must Close it. It is a thin convenience over sql.Open so
// callers need not import the driver themselves.
func Open(dsn string) (*sql.DB, error) {
	return sql.Open("pgx", dsn)
}

// Migrate applies every pending up migration in version order, each in its own
// transaction, recording it in the migration ledger. It is idempotent: already
// applied migrations are skipped, so it is safe to call on every startup.
func Migrate(ctx context.Context, db *sql.DB) error {
	migs, err := loadMigrations()
	if err != nil {
		return err
	}
	if err := ensureLedger(ctx, db); err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, db)
	if err != nil {
		return err
	}
	for _, m := range migs {
		if applied[m.version] {
			continue
		}
		if err := runInTx(ctx, db, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, m.up); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx,
				"INSERT INTO "+migrationLedger+" (version, name) VALUES ($1, $2)", m.version, m.name)
			return err
		}); err != nil {
			return fmt.Errorf("rdq/postgres: applying migration %04d (%s): %w", m.version, m.name, err)
		}
	}
	return nil
}

// MigrateDown rolls migrations back to targetVersion (0 = fully down),
// running each down script in descending version order, each in its own
// transaction. Migrations at or below targetVersion are left in place.
func MigrateDown(ctx context.Context, db *sql.DB, targetVersion int) error {
	migs, err := loadMigrations()
	if err != nil {
		return err
	}
	if err := ensureLedger(ctx, db); err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, db)
	if err != nil {
		return err
	}
	for i := len(migs) - 1; i >= 0; i-- {
		m := migs[i]
		if !applied[m.version] || m.version <= targetVersion {
			continue
		}
		if err := runInTx(ctx, db, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, m.down); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx,
				"DELETE FROM "+migrationLedger+" WHERE version = $1", m.version)
			return err
		}); err != nil {
			return fmt.Errorf("rdq/postgres: rolling back migration %04d (%s): %w", m.version, m.name, err)
		}
	}
	return nil
}

// ensureLedger creates the migration ledger table if it does not yet exist.
func ensureLedger(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+migrationLedger+` (
		version    integer     PRIMARY KEY,
		name       text        NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`)
	if err != nil {
		return fmt.Errorf("rdq/postgres: creating migration ledger: %w", err)
	}
	return nil
}

// appliedVersions returns the set of migration versions already applied.
func appliedVersions(ctx context.Context, db *sql.DB) (map[int]bool, error) {
	rows, err := db.QueryContext(ctx, "SELECT version FROM "+migrationLedger)
	if err != nil {
		return nil, fmt.Errorf("rdq/postgres: reading migration ledger: %w", err)
	}
	defer rows.Close()
	applied := map[int]bool{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

// runInTx runs fn inside a transaction, committing on success and rolling back
// on any error.
func runInTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// CheckSchemaVersion reads rdq_schema_version and reports whether this build may
// run against the database (design 05 G5). It returns ErrSchemaNotInitialized if
// the migrations have not been applied, ErrSchemaVersionMismatch if the recorded
// version differs from SchemaVersion, and nil when they match.
func CheckSchemaVersion(ctx context.Context, db *sql.DB) error {
	var version int
	err := db.QueryRowContext(ctx,
		"SELECT version FROM rdq_schema_version WHERE singleton").Scan(&version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSchemaNotInitialized
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" { // undefined_table
			return ErrSchemaNotInitialized
		}
		return fmt.Errorf("rdq/postgres: reading schema version: %w", err)
	}
	return checkSchemaVersion(version)
}

// checkSchemaVersion is the pure comparison behind CheckSchemaVersion, split out
// so the gate logic is unit-testable without a database.
func checkSchemaVersion(dbVersion int) error {
	switch {
	case dbVersion == SchemaVersion:
		return nil
	case dbVersion > SchemaVersion:
		return fmt.Errorf("%w: database schema version %d is newer than this engine's %d; upgrade the engine",
			ErrSchemaVersionMismatch, dbVersion, SchemaVersion)
	default:
		return fmt.Errorf("%w: database schema version %d is older than this engine's %d; run migrations",
			ErrSchemaVersionMismatch, dbVersion, SchemaVersion)
	}
}
