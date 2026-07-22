// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"errors"
	"testing"

	coreconfig "github.com/srjn45/rdq/core/config"
	srvconfig "github.com/srjn45/rdq/server/config"
)

func pint(n int) *int { return &n }

func validQC() *coreconfig.QueueConfig {
	return &coreconfig.QueueConfig{
		Retry: &coreconfig.RetryConfig{MaxAttempts: pint(3)},
	}
}

// --- MemStore basic CRUD ---

func TestMemStore_PutAndGet(t *testing.T) {
	m := srvconfig.NewMemStore()
	qc := validQC()
	if err := m.Put("payments.charge", qc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	entry, err := m.Get("payments.charge")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if entry.Config == nil {
		t.Error("entry.Config must not be nil")
	}
	if *entry.Config.Retry.MaxAttempts != 3 {
		t.Errorf("max_attempts = %d, want 3", *entry.Config.Retry.MaxAttempts)
	}
}

func TestMemStore_Get_ErrNotFound(t *testing.T) {
	m := srvconfig.NewMemStore()
	_, err := m.Get("nonexistent")
	if !errors.Is(err, srvconfig.ErrNotFound) {
		t.Errorf("Get unknown queue: got %v, want ErrNotFound", err)
	}
}

func TestMemStore_Delete(t *testing.T) {
	m := srvconfig.NewMemStore()
	_ = m.Put("q", validQC())
	if err := m.Delete("q"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := m.Get("q"); !errors.Is(err, srvconfig.ErrNotFound) {
		t.Errorf("after Delete: got %v, want ErrNotFound", err)
	}
}

func TestMemStore_Delete_ErrNotFound(t *testing.T) {
	m := srvconfig.NewMemStore()
	if err := m.Delete("nope"); !errors.Is(err, srvconfig.ErrNotFound) {
		t.Errorf("Delete nonexistent: got %v, want ErrNotFound", err)
	}
}

func TestMemStore_List_Sorted(t *testing.T) {
	m := srvconfig.NewMemStore()
	_ = m.Put("z-queue", validQC())
	_ = m.Put("a-queue", validQC())
	_ = m.Put("m-queue", validQC())

	names, err := m.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(names) != 3 {
		t.Fatalf("len = %d, want 3", len(names))
	}
	want := []string{"a-queue", "m-queue", "z-queue"}
	for i, n := range names {
		if n != want[i] {
			t.Errorf("names[%d] = %q, want %q", i, n, want[i])
		}
	}
}

// --- pause state ---

func TestMemStore_PauseResume(t *testing.T) {
	m := srvconfig.NewMemStore()
	_ = m.Put("q", validQC())

	if m.IsPaused("q") {
		t.Error("IsPaused must be false initially")
	}
	_ = m.SetPaused("q", true)
	if !m.IsPaused("q") {
		t.Error("IsPaused must be true after SetPaused(true)")
	}
	_ = m.SetPaused("q", false)
	if m.IsPaused("q") {
		t.Error("IsPaused must be false after SetPaused(false)")
	}
}

func TestMemStore_SetPaused_UnconfiguredQueue(t *testing.T) {
	m := srvconfig.NewMemStore()
	// SetPaused works even when the queue has no config entry.
	if err := m.SetPaused("unconfigured-q", true); err != nil {
		t.Fatalf("SetPaused unconfigured: %v", err)
	}
	if !m.IsPaused("unconfigured-q") {
		t.Error("IsPaused must be true after SetPaused even for unconfigured queue")
	}
}

func TestMemStore_PausePreservedOnPut(t *testing.T) {
	m := srvconfig.NewMemStore()
	_ = m.Put("q", validQC())
	_ = m.SetPaused("q", true)

	// Updating config must preserve existing pause state.
	_ = m.Put("q", validQC())
	if !m.IsPaused("q") {
		t.Error("pause state must be preserved when config is updated via Put")
	}
}

// --- Seed / G16 API-wins-over-YAML ---

func TestMemStore_Seed_Basic(t *testing.T) {
	m := srvconfig.NewMemStore()
	yaml := []byte(`
config_version: 1
queues:
  seeded-queue:
    retry:
      max_attempts: 7
`)
	cfg, err := coreconfig.Load(yaml)
	if err != nil {
		t.Fatalf("Load YAML: %v", err)
	}
	m.Seed(cfg)

	entry, err := m.Get("seeded-queue")
	if err != nil {
		t.Fatalf("Get seeded queue: %v", err)
	}
	if *entry.Config.Retry.MaxAttempts != 7 {
		t.Errorf("max_attempts = %d, want 7", *entry.Config.Retry.MaxAttempts)
	}
}

func TestMemStore_Seed_APIWins(t *testing.T) {
	m := srvconfig.NewMemStore()
	// Write via API first.
	ma := 99
	apiQC := &coreconfig.QueueConfig{Retry: &coreconfig.RetryConfig{MaxAttempts: &ma}}
	_ = m.Put("q", apiQC)

	// Seed from YAML: must NOT overwrite the API-written config (G16).
	yaml := []byte(`
config_version: 1
queues:
  q:
    retry:
      max_attempts: 1
`)
	cfg, err := coreconfig.Load(yaml)
	if err != nil {
		t.Fatalf("Load YAML: %v", err)
	}
	m.Seed(cfg)

	entry, err := m.Get("q")
	if err != nil {
		t.Fatalf("Get after Seed: %v", err)
	}
	if *entry.Config.Retry.MaxAttempts != 99 {
		t.Errorf("API config overwritten by YAML: max_attempts = %d, want 99", *entry.Config.Retry.MaxAttempts)
	}
}
