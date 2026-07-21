// SPDX-License-Identifier: Apache-2.0

// Package config is the server-side ConfigStore: CRUD + watch for per-queue
// configuration, separate from the task Storage SPI (design 04 §3). The v1
// production backend is Postgres (same database as the task store, its own
// tables — see the migration in storage/postgres/migrations/0002_config); an
// in-memory MemStore is provided for tests and YAML-only deployments.
//
// Config changes take effect at the next claim (design 03 §1). API-written
// config wins over YAML boot-seed: once a queue's config is written via
// PUT /admin/queues/{queue}/config, a server restart re-seeds from YAML but
// skips queues already held in the durable store (G16, design 03 §1).
package config

import (
	"errors"
	"sort"
	"sync"

	coreconfig "github.com/srjn45/rdq/core/config"
)

// ErrNotFound is returned when a queue has no config entry in the store.
var ErrNotFound = errors.New("serverconfig: queue not found")

// Entry is one queue's stored state: its validated queue config plus the
// operational pause flag that persists across restarts (design 04 §2).
type Entry struct {
	Config *coreconfig.QueueConfig
	Paused bool
}

// Store is the CRUD interface for per-queue configuration (design 04 §3).
// Implementations must be safe for concurrent use.
type Store interface {
	// Get returns the Entry for queue. Returns ErrNotFound if not configured.
	Get(queue string) (*Entry, error)
	// Put upserts the queue's config. This write is flagged as API-sourced so
	// a subsequent YAML re-seed at startup does not overwrite it (G16).
	Put(queue string, qc *coreconfig.QueueConfig) error
	// Delete removes the queue's config. Returns ErrNotFound if not configured.
	Delete(queue string) error
	// List returns all configured queue names in sorted order.
	List() ([]string, error)
	// IsPaused reports whether claiming is suspended for queue.
	IsPaused(queue string) bool
	// SetPaused persists the pause/resume state for queue. It creates a
	// placeholder entry when the queue has no config so pause state survives
	// even before the queue config is written.
	SetPaused(queue string, paused bool) error
}

// MemStore is a thread-safe in-memory Store for tests and YAML-only
// deployments. API writes (Put/SetPaused) mark entries as API-written; Seed
// skips those on re-load so API-sourced config wins over YAML (G16).
type MemStore struct {
	mu      sync.RWMutex
	configs map[string]*memCfg // keyed by queue name; non-nil only for configured queues
	paused  map[string]bool    // pause state, independent of config presence
}

type memCfg struct {
	qc         *coreconfig.QueueConfig
	apiWritten bool
}

// NewMemStore returns an empty, ready-to-use MemStore.
func NewMemStore() *MemStore {
	return &MemStore{
		configs: make(map[string]*memCfg),
		paused:  make(map[string]bool),
	}
}

// Seed loads queues from a parsed Config into the store, skipping any queues
// that have already been written via the API (G16 — API wins over YAML).
// It is called at server startup after a YAML boot file is parsed.
func (m *MemStore) Seed(cfg *coreconfig.Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, qc := range cfg.Queues {
		if existing := m.configs[name]; existing != nil && existing.apiWritten {
			continue // API-written config wins; YAML does not overwrite.
		}
		m.configs[name] = &memCfg{qc: qc}
	}
}

// Get returns a copy of the Entry for queue, or ErrNotFound.
func (m *MemStore) Get(queue string) (*Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.configs[queue]
	if !ok {
		return nil, ErrNotFound
	}
	return &Entry{Config: c.qc, Paused: m.paused[queue]}, nil
}

// Put upserts the queue config and marks it as API-written (G16).
func (m *MemStore) Put(queue string, qc *coreconfig.QueueConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.configs[queue] = &memCfg{qc: qc, apiWritten: true}
	return nil
}

// Delete removes the queue config. Returns ErrNotFound if not configured.
// Any associated pause state is also cleared.
func (m *MemStore) Delete(queue string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.configs[queue]; !ok {
		return ErrNotFound
	}
	delete(m.configs, queue)
	delete(m.paused, queue)
	return nil
}

// List returns all configured queue names in sorted order.
func (m *MemStore) List() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.configs))
	for name := range m.configs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// IsPaused reports whether claiming is suspended for queue.
func (m *MemStore) IsPaused(queue string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.paused[queue]
}

// SetPaused persists the pause/resume state. It works for any queue name,
// even one not yet in the config store.
func (m *MemStore) SetPaused(queue string, paused bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if paused {
		m.paused[queue] = true
	} else {
		delete(m.paused, queue)
	}
	return nil
}
