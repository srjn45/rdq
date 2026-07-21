// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// loadFixture reads and loads a testdata YAML file, failing the test on a load
// error unless wantErr is set.
func loadFixture(t *testing.T, name string) *Config {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	cfg, err := Load(data)
	if err != nil {
		t.Fatalf("load %s: %v", name, err)
	}
	return cfg
}

func TestLoadValidFull(t *testing.T) {
	cfg := loadFixture(t, "valid_full.yaml")

	if cfg.ConfigVersion != 1 {
		t.Fatalf("config_version got %d, want 1", cfg.ConfigVersion)
	}
	if got := cfg.QueueNames(); len(got) != 2 || got[0] != "payments.charge" || got[1] != "reports.nightly" {
		t.Fatalf("QueueNames got %v", got)
	}

	pc, err := cfg.Resolved("payments.charge")
	if err != nil {
		t.Fatalf("Resolved: %v", err)
	}
	// Queue-set override.
	if got := *pc.Retry.MaxAttempts; got != 8 {
		t.Errorf("max_attempts got %d, want 8 (queue override)", got)
	}
	if got := pc.Retry.InitialBackoff.Std(); got != 2*time.Second {
		t.Errorf("initial_backoff got %s, want 2s (queue override)", got)
	}
	// Inherited from defaults.retry via deep-merge.
	if got := *pc.Retry.Jitter; got != 0.2 {
		t.Errorf("jitter got %g, want 0.2 (inherited)", got)
	}
	if got := pc.Retry.MaxBackoff.Std(); got != 10*time.Minute {
		t.Errorf("max_backoff got %s, want 10m (inherited)", got)
	}
	// Whole blocks inherited from defaults.
	if got := pc.Execution.Lease.Std(); got != 60*time.Second {
		t.Errorf("lease got %s, want 60s (inherited)", got)
	}
	if got := pc.Worker.RateLimit; got == nil || got.Count != 100 || got.Per != time.Second {
		t.Errorf("rate_limit got %v, want 100/s (inherited)", got)
	}
	if got := pc.Limits.MaxPayloadSize.Bytes(); got != 1<<20 {
		t.Errorf("max_payload_size got %d, want 1MiB (inherited)", got)
	}
	// Queue-only blocks present.
	if pc.Callback == nil || pc.Callback.URL == nil || *pc.Callback.URL != "https://payments.internal/rdq/charge" {
		t.Errorf("callback.url missing or wrong: %+v", pc.Callback)
	}
	if pc.Classification == nil || len(pc.Classification.Retryable) != 2 {
		t.Errorf("classification.retryable got %+v", pc.Classification)
	}
	if len(pc.Callback.ResponseMapping.RetryableStatus) != 3 {
		t.Errorf("response_mapping.retryable_status got %+v", pc.Callback.ResponseMapping.RetryableStatus)
	}

	// reports.nightly overrides only execution.heartbeat; inherits the rest.
	rn, err := cfg.Resolved("reports.nightly")
	if err != nil {
		t.Fatalf("Resolved: %v", err)
	}
	if rn.Execution.Heartbeat == nil || !*rn.Execution.Heartbeat {
		t.Errorf("reports.nightly heartbeat got %v, want true", rn.Execution.Heartbeat)
	}
	if rn.Execution.Lease == nil || rn.Execution.Lease.Std() != 60*time.Second {
		t.Errorf("reports.nightly lease got %v, want 60s (inherited)", rn.Execution.Lease)
	}
	if rn.Retry == nil || *rn.Retry.MaxAttempts != 5 {
		t.Errorf("reports.nightly max_attempts want 5 (inherited)")
	}
}

func TestResolvedUnknownQueue(t *testing.T) {
	cfg := loadFixture(t, "valid_full.yaml")
	if _, err := cfg.Resolved("does.not.exist"); !errors.Is(err, ErrUnknownQueue) {
		t.Fatalf("got %v, want ErrUnknownQueue", err)
	}
}

func TestLoadRejectsUnknownKey(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "unknown_key.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Load(data); err == nil {
		t.Fatal("expected a load error for the max_attempt typo, got nil")
	}
}

func TestLoadVersionGate(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		bad  bool
	}{
		{name: "known", yaml: "config_version: 1\n", bad: false},
		{name: "missing", yaml: "queues: {}\n", bad: true},
		{name: "zero", yaml: "config_version: 0\n", bad: true},
		{name: "newer", yaml: "config_version: 999\n", bad: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Load([]byte(c.yaml))
			if c.bad != (err != nil) {
				t.Fatalf("Load(%q) err=%v, wantErr=%v", c.yaml, err, c.bad)
			}
		})
	}
}

func TestLoadRejectsEmptyAndMultiDoc(t *testing.T) {
	if _, err := Load(nil); err == nil {
		t.Error("expected error for empty document")
	}
	multi := "config_version: 1\n---\nconfig_version: 1\n"
	if _, err := Load([]byte(multi)); err == nil {
		t.Error("expected error for a multi-document stream")
	}
}

func TestLoadRejectsUnknownTopLevelKey(t *testing.T) {
	if _, err := Load([]byte("config_version: 1\nqeueus: {}\n")); err == nil {
		t.Fatal("expected error for the top-level typo qeueus")
	}
}
