// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"testing"
)

// TestLoadMigrations checks the embedded migration set parses, is gap-free from
// version 1, and pairs every up with a down — the invariants Migrate relies on.
func TestLoadMigrations(t *testing.T) {
	migs, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	if len(migs) == 0 {
		t.Fatal("expected at least one migration")
	}
	for i, m := range migs {
		if m.version != i+1 {
			t.Errorf("migration %d has version %d, want %d", i, m.version, i+1)
		}
		if m.up == "" || m.down == "" {
			t.Errorf("migration %04d (%s) missing up or down script", m.version, m.name)
		}
	}
	// The final migration must define the schema version the code claims.
	last := migs[len(migs)-1]
	if last.version != SchemaVersion {
		t.Errorf("highest migration version %d != SchemaVersion %d", last.version, SchemaVersion)
	}
}

func TestParseMigrationName(t *testing.T) {
	cases := []struct {
		file        string
		wantVersion int
		wantName    string
		wantDir     string
		wantErr     bool
	}{
		{file: "0001_init.up.sql", wantVersion: 1, wantName: "init", wantDir: "up"},
		{file: "0012_add_index.down.sql", wantVersion: 12, wantName: "add_index", wantDir: "down"},
		{file: "0001_init.sql", wantErr: true},          // missing direction
		{file: "0001_init.sideways.sql", wantErr: true}, // bad direction
		{file: "init.up.sql", wantErr: true},            // no numeric prefix
		{file: "xx_init.up.sql", wantErr: true},         // non-numeric version
	}
	for _, c := range cases {
		v, name, dir, err := parseMigrationName(c.file)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseMigrationName(%q): expected error", c.file)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseMigrationName(%q): unexpected error %v", c.file, err)
			continue
		}
		if v != c.wantVersion || name != c.wantName || dir != c.wantDir {
			t.Errorf("parseMigrationName(%q) = (%d,%q,%q), want (%d,%q,%q)",
				c.file, v, name, dir, c.wantVersion, c.wantName, c.wantDir)
		}
	}
}

// TestCheckSchemaVersion exercises the G5 gate's pure comparison: equal passes,
// newer and older both fail with ErrSchemaVersionMismatch.
func TestCheckSchemaVersion(t *testing.T) {
	if err := checkSchemaVersion(SchemaVersion); err != nil {
		t.Errorf("matching version should pass, got %v", err)
	}
	for _, v := range []int{SchemaVersion + 1, SchemaVersion + 99} {
		if err := checkSchemaVersion(v); !errors.Is(err, ErrSchemaVersionMismatch) {
			t.Errorf("newer version %d should mismatch, got %v", v, err)
		}
	}
	if SchemaVersion > 1 {
		if err := checkSchemaVersion(SchemaVersion - 1); !errors.Is(err, ErrSchemaVersionMismatch) {
			t.Errorf("older version should mismatch, got %v", err)
		}
	}
}
