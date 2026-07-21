// SPDX-License-Identifier: Apache-2.0

// Package cmd implements the rdq CLI commands and their two transport modes.
package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
	"github.com/srjn45/rdq/storage/postgres"
)

// Transport abstracts the two execution modes (G2):
//   - API mode: HTTP client of the public rdq-server REST API (--server URL)
//   - Direct mode: Postgres client via the storage plugin (--dsn DSN)
type Transport interface {
	Stats(ctx context.Context, queue string) (spi.Stats, error)
	DLQList(ctx context.Context, queue string, f spi.DLQFilter, page spi.Page) ([]envelope.Envelope, spi.Cursor, error)
	GetTask(ctx context.Context, id string) (envelope.Envelope, error)
	Redrive(ctx context.Context, queue string, sel spi.Selector) (int, error)
	Purge(ctx context.Context, queue string, sel spi.Selector) (int, error)
	Close() error
}

// Migrator is an optional extension of Transport for direct-storage backends
// that can apply the T2.1 schema migrations (G17).
type Migrator interface {
	Transport
	Migrate(ctx context.Context) error
}

// ──────────────────────────────────────────────────────────── API transport ──

// apiTransport is an ordinary HTTP client of the rdq-server REST API.
// It is the future web-UI contract: only public /v1 routes, never server internals.
type apiTransport struct {
	base   string
	token  string
	client *http.Client
}

// newAPITransport constructs an apiTransport that calls serverURL.
func newAPITransport(serverURL, token string) (Transport, error) {
	if _, err := url.ParseRequestURI(serverURL); err != nil {
		return nil, fmt.Errorf("invalid --server URL: %w", err)
	}
	return &apiTransport{
		base:   strings.TrimRight(serverURL, "/"),
		token:  token,
		client: &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (a *apiTransport) req(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var buf *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		buf = bytes.NewReader(b)
	} else {
		buf = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+"/v1"+path, buf)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	return a.client.Do(req)
}

func apiErr(resp *http.Response) error {
	var prob struct {
		Title  string `json:"title"`
		Detail string `json:"detail"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&prob)
	if prob.Detail != "" {
		return fmt.Errorf("server error %d: %s", resp.StatusCode, prob.Detail)
	}
	if prob.Title != "" {
		return fmt.Errorf("server error %d: %s", resp.StatusCode, prob.Title)
	}
	return fmt.Errorf("server error %d", resp.StatusCode)
}

func (a *apiTransport) Stats(ctx context.Context, queue string) (spi.Stats, error) {
	resp, err := a.req(ctx, http.MethodGet, "/queues/"+url.PathEscape(queue)+"/stats", nil)
	if err != nil {
		return spi.Stats{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return spi.Stats{}, apiErr(resp)
	}
	var sr struct {
		Pending            int64 `json:"pending"`
		InFlight           int64 `json:"in_flight"`
		DLQDepth           int64 `json:"dlq_depth"`
		OldestPendingAgeMs int64 `json:"oldest_pending_age_ms"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return spi.Stats{}, err
	}
	return spi.Stats{
		Pending:          sr.Pending,
		InFlight:         sr.InFlight,
		DLQDepth:         sr.DLQDepth,
		OldestPendingAge: time.Duration(sr.OldestPendingAgeMs) * time.Millisecond,
	}, nil
}

func (a *apiTransport) DLQList(ctx context.Context, queue string, f spi.DLQFilter, page spi.Page) ([]envelope.Envelope, spi.Cursor, error) {
	q := url.Values{}
	if f.ErrorType != "" {
		q.Set("error_type", f.ErrorType)
	}
	if f.HandlerRef != "" {
		q.Set("handler_ref", f.HandlerRef)
	}
	if f.DeadLetteredAfter != nil {
		q.Set("from", f.DeadLetteredAfter.Format(time.RFC3339))
	}
	if f.DeadLetteredBefore != nil {
		q.Set("to", f.DeadLetteredBefore.Format(time.RFC3339))
	}
	if page.Limit > 0 {
		q.Set("limit", strconv.Itoa(page.Limit))
	}
	if page.After != "" {
		q.Set("cursor", string(page.After))
	}
	path := "/queues/" + url.PathEscape(queue) + "/dlq"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	resp, err := a.req(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", apiErr(resp)
	}
	var lr struct {
		Tasks      []envelope.Envelope `json:"tasks"`
		NextCursor spi.Cursor          `json:"next_cursor,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
		return nil, "", err
	}
	if lr.Tasks == nil {
		lr.Tasks = []envelope.Envelope{}
	}
	return lr.Tasks, lr.NextCursor, nil
}

func (a *apiTransport) GetTask(ctx context.Context, id string) (envelope.Envelope, error) {
	resp, err := a.req(ctx, http.MethodGet, "/tasks/"+url.PathEscape(id), nil)
	if err != nil {
		return envelope.Envelope{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return envelope.Envelope{}, apiErr(resp)
	}
	var env envelope.Envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return envelope.Envelope{}, err
	}
	return env, nil
}

func (a *apiTransport) Redrive(ctx context.Context, queue string, sel spi.Selector) (int, error) {
	return a.dlqMutate(ctx, queue, "redrive", sel)
}

func (a *apiTransport) Purge(ctx context.Context, queue string, sel spi.Selector) (int, error) {
	return a.dlqMutate(ctx, queue, "purge", sel)
}

func (a *apiTransport) dlqMutate(ctx context.Context, queue, action string, sel spi.Selector) (int, error) {
	body := struct {
		IDs    []string       `json:"ids,omitempty"`
		Filter *spi.DLQFilter `json:"filter,omitempty"`
	}{IDs: sel.IDs, Filter: sel.Filter}
	resp, err := a.req(ctx, http.MethodPost, "/queues/"+url.PathEscape(queue)+"/dlq:"+action, body)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, apiErr(resp)
	}
	var cr struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return 0, err
	}
	return cr.Count, nil
}

func (a *apiTransport) Close() error { return nil }

// ──────────────────────────────────────────────────────── Direct transport ──

// directTransport talks directly to Postgres via the storage/postgres plugin.
// It also implements Migrator so `rdq migrate` can apply the T2.1 migrations.
type directTransport struct {
	db    *sql.DB
	store *postgres.Store
}

// newDirectTransport opens a Postgres connection and returns a Transport.
func newDirectTransport(dsn string) (Transport, error) {
	db, err := postgres.Open(dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return &directTransport{db: db, store: postgres.New(db)}, nil
}

func (d *directTransport) Stats(ctx context.Context, queue string) (spi.Stats, error) {
	return d.store.Stats(ctx, queue)
}

func (d *directTransport) DLQList(ctx context.Context, queue string, f spi.DLQFilter, page spi.Page) ([]envelope.Envelope, spi.Cursor, error) {
	return d.store.DLQList(ctx, queue, f, page)
}

func (d *directTransport) GetTask(ctx context.Context, id string) (envelope.Envelope, error) {
	return d.store.Get(ctx, id)
}

func (d *directTransport) Redrive(ctx context.Context, queue string, sel spi.Selector) (int, error) {
	return d.store.Redrive(ctx, queue, sel)
}

func (d *directTransport) Purge(ctx context.Context, queue string, sel spi.Selector) (int, error) {
	return d.store.Purge(ctx, queue, sel)
}

func (d *directTransport) Migrate(ctx context.Context) error {
	return postgres.Migrate(ctx, d.db)
}

func (d *directTransport) Close() error {
	return d.db.Close()
}
