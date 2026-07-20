// SPDX-License-Identifier: Apache-2.0

// Package memstore is the in-memory reference implementation of spi.Storage: a
// correct, mutex-guarded backend that is both the compliance kit's first
// subject and the engine's test substrate (design 02 §3, backlog T1.6). It
// trades durability for simplicity — everything lives in a map behind a single
// lock — but implements every correctness invariant the SPI requires: fenced
// claims, lease-expiry crash recovery, idempotent enqueue, redrive reset, and
// stable cursor pagination.
//
// Time authority (G9). The store owns the clock: due-ness and lease expiry are
// evaluated against Store.clock, which defaults to time.Now and is injectable
// via WithClock for deterministic tests. Callers never pass "now" in.
package memstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// defaultPageLimit is the DLQList page size when Page.Limit is zero or negative.
const defaultPageLimit = 100

// cursorPrefix tags an encoded DLQList cursor so a foreign/garbage token is
// rejected with ErrStaleCursor rather than silently mis-paging.
const cursorPrefix = "mc1:"

// leaseExpiredType is the error.type recorded on a reclaimed-lease attempt (G7).
const leaseExpiredType = "rdq.LeaseExpired"

// Store is a mutex-guarded, in-memory spi.Storage. The zero value is not usable;
// construct with New.
type Store struct {
	mu    sync.Mutex
	clock func() time.Time

	// tasks holds every live task keyed by id, across all statuses
	// (PENDING/IN_FLIGHT/SUCCEEDED/DEAD). Values are pointers so mutations are
	// in place under the lock; envelopes are cloned on the way in and out so no
	// caller ever aliases the stored state.
	tasks map[spi.TaskID]*record

	// claimSeq mints monotonic fencing tokens: every claim gets a strictly
	// larger token, so a reclaim invalidates every prior token (fencing).
	claimSeq uint64
	// dlqSeq orders dead-letter entries. Assigned at DeadLetter time (not at
	// enqueue) so new DLQ entries always sort after existing ones — the
	// property that makes cursor pagination stable under concurrent arrivals.
	dlqSeq uint64
}

// record is the store's per-task bookkeeping around the wire envelope.
type record struct {
	env        envelope.Envelope
	token      spi.ClaimToken // current valid fencing token while IN_FLIGHT; "" otherwise
	claimedAt  time.Time      // when the current claim began; the LEASE_EXPIRED attempt's StartedAt
	dlqSeq     uint64         // dead-letter ordering key; 0 unless DEAD
	terminalAt time.Time      // Complete/DeadLetter time; drives PurgeSucceeded age and DLQ time filters
}

// Option configures a Store.
type Option func(*Store)

// WithClock overrides the store's time source (default time.Now). The store is
// the time authority for due-ness and lease expiry (G9); tests inject a
// controllable clock to exercise lease expiry deterministically.
func WithClock(fn func() time.Time) Option {
	return func(s *Store) {
		if fn != nil {
			s.clock = fn
		}
	}
}

// New returns an empty Store.
func New(opts ...Option) *Store {
	s := &Store{
		clock: time.Now,
		tasks: make(map[spi.TaskID]*record),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Compile-time assertion that Store satisfies the frozen contract.
var _ spi.Storage = (*Store)(nil)

// --- lifecycle ---

// Enqueue admits a task. It is idempotent by id within a queue (re-enqueue of an
// existing id in the SAME queue is a no-op) and rejects the same id in a
// DIFFERENT queue with ErrIDConflict (G8).
func (s *Store) Enqueue(ctx context.Context, task envelope.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.tasks[task.ID]; ok {
		if existing.env.Queue != task.Queue {
			return spi.ErrIDConflict
		}
		return nil // idempotent no-op: safe submit retry
	}
	s.tasks[task.ID] = &record{env: cloneEnvelope(task)}
	return nil
}

// ClaimDue atomically claims up to limit due tasks for queue. Due =
// (PENDING AND next_attempt_at <= now) OR (IN_FLIGHT AND lease_expires_at <=
// now). Reclaiming an expired lease appends a LEASE_EXPIRED attempt before the
// task is re-leased. Each claim mints a fresh fencing token.
func (s *Store) ClaimDue(ctx context.Context, queue string, limit int, lease time.Duration) ([]spi.Claimed, error) {
	if limit <= 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock()

	due := make([]*record, 0)
	for _, r := range s.tasks {
		if r.env.Queue != queue {
			continue
		}
		if isDue(r, now) {
			due = append(due, r)
		}
	}
	// Best-effort ordering by next_attempt_at ascending; id breaks ties so the
	// order is deterministic.
	sort.Slice(due, func(i, j int) bool {
		ti, tj := dueKey(due[i]), dueKey(due[j])
		if ti.Equal(tj) {
			return due[i].env.ID < due[j].env.ID
		}
		return ti.Before(tj)
	})
	if len(due) > limit {
		due = due[:limit]
	}

	claimed := make([]spi.Claimed, 0, len(due))
	for _, r := range due {
		// Reclaiming an expired lease counts as a failed attempt (poison-pill
		// protection): append LEASE_EXPIRED atomically with the re-claim.
		if r.env.Status == envelope.StatusInFlight {
			s.appendAttempt(r, leaseExpiredAttempt(r, now))
		}

		s.claimSeq++
		token := spi.ClaimToken("ct-" + strconv.FormatUint(s.claimSeq, 10))
		leaseExpiry := now.Add(lease)

		r.env.Status = envelope.StatusInFlight
		r.env.LeaseExpiresAt = &leaseExpiry
		r.token = token
		r.claimedAt = now

		claimed = append(claimed, spi.Claimed{Task: cloneEnvelope(r.env), Token: token})
	}
	return claimed, nil
}

// ExtendLease renews the lease for a live claim. It fails with ErrStaleClaim
// (changing nothing) if the token is not the task's current claim token.
func (s *Store) ExtendLease(ctx context.Context, id spi.TaskID, token spi.ClaimToken, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, err := s.resolveClaim(id, token)
	if err != nil {
		return err
	}
	leaseExpiry := s.clock().Add(lease)
	r.env.LeaseExpiresAt = &leaseExpiry
	return nil
}

// --- outcome resolution (all require a valid token; ErrStaleClaim otherwise) ---

// Reschedule is the failure path: append the attempt and set the task PENDING
// with next_attempt_at = nextAt.
func (s *Store) Reschedule(ctx context.Context, id spi.TaskID, token spi.ClaimToken, attempt spi.Attempt, nextAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, err := s.resolveClaim(id, token)
	if err != nil {
		return err
	}
	s.appendAttempt(r, attempt)
	next := nextAt
	r.env.Status = envelope.StatusPending
	r.env.NextAttemptAt = &next
	r.env.LeaseExpiresAt = nil
	r.token = ""
	return nil
}

// Complete is the success path: append the attempt and mark the task SUCCEEDED.
func (s *Store) Complete(ctx context.Context, id spi.TaskID, token spi.ClaimToken, attempt spi.Attempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, err := s.resolveClaim(id, token)
	if err != nil {
		return err
	}
	s.appendAttempt(r, attempt)
	r.env.Status = envelope.StatusSucceeded
	r.env.NextAttemptAt = nil
	r.env.LeaseExpiresAt = nil
	r.token = ""
	r.terminalAt = s.clock()
	return nil
}

// DeadLetter is exhaustion / permanent failure: append the attempt and move the
// task to the DLQ.
func (s *Store) DeadLetter(ctx context.Context, id spi.TaskID, token spi.ClaimToken, attempt spi.Attempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, err := s.resolveClaim(id, token)
	if err != nil {
		return err
	}
	s.appendAttempt(r, attempt)
	now := s.clock()
	s.dlqSeq++
	r.env.Status = envelope.StatusDead
	r.env.NextAttemptAt = nil
	r.env.LeaseExpiresAt = nil
	r.token = ""
	r.dlqSeq = s.dlqSeq
	r.terminalAt = now
	return nil
}

// --- DLQ ---

// DLQList pages the dead-letter queue for queue with stable cursor pagination.
// Attempt histories are omitted unless f.IncludeAttempts is set (G13). The
// returned Cursor is empty on the last page; an unparseable cursor yields
// ErrStaleCursor.
func (s *Store) DLQList(ctx context.Context, queue string, f spi.DLQFilter, page spi.Page) ([]envelope.Envelope, spi.Cursor, error) {
	after, err := decodeCursor(page.After)
	if err != nil {
		return nil, "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	matched := make([]*record, 0)
	for _, r := range s.tasks {
		if r.env.Queue != queue || r.env.Status != envelope.StatusDead {
			continue
		}
		if r.dlqSeq <= after {
			continue
		}
		if !matchFilter(r, f) {
			continue
		}
		matched = append(matched, r)
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].dlqSeq < matched[j].dlqSeq })

	limit := page.Limit
	if limit <= 0 {
		limit = defaultPageLimit
	}

	var next spi.Cursor
	if len(matched) > limit {
		matched = matched[:limit]
		next = encodeCursor(matched[len(matched)-1].dlqSeq)
	}

	out := make([]envelope.Envelope, 0, len(matched))
	for _, r := range matched {
		env := cloneEnvelope(r.env)
		if !f.IncludeAttempts {
			env.Attempts = nil
		}
		out = append(out, env)
	}
	return out, next, nil
}

// Get fetches one task by id in any status with full attempt history;
// ErrNotFound if absent.
func (s *Store) Get(ctx context.Context, id spi.TaskID) (envelope.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, ok := s.tasks[id]
	if !ok {
		return envelope.Envelope{}, spi.ErrNotFound
	}
	return cloneEnvelope(r.env), nil
}

// Redrive returns the selected DLQ tasks to PENDING with attempt_count=0 and
// redrive_count incremented, preserving prior attempt history. Returns the
// count affected.
func (s *Store) Redrive(ctx context.Context, queue string, sel spi.Selector) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock()
	selected := s.selectDead(queue, sel)
	for _, r := range selected {
		next := now
		r.env.Status = envelope.StatusPending
		r.env.AttemptCount = 0
		r.env.RedriveCount++
		r.env.NextAttemptAt = &next
		r.env.LeaseExpiresAt = nil
		r.dlqSeq = 0
		r.terminalAt = time.Time{}
		// Attempts history is deliberately kept (invariant 7).
	}
	return len(selected), nil
}

// Purge permanently removes the selected DLQ tasks. Returns the count removed.
func (s *Store) Purge(ctx context.Context, queue string, sel spi.Selector) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	selected := s.selectDead(queue, sel)
	for _, r := range selected {
		delete(s.tasks, r.env.ID)
	}
	return len(selected), nil
}

// --- ops ---

// Stats returns a per-queue operational snapshot.
func (s *Store) Stats(ctx context.Context, queue string) (spi.Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock()
	var st spi.Stats
	var oldestPending time.Time
	havePending := false
	for _, r := range s.tasks {
		if r.env.Queue != queue {
			continue
		}
		switch r.env.Status {
		case envelope.StatusPending:
			st.Pending++
			if !havePending || r.env.CreatedAt.Before(oldestPending) {
				oldestPending = r.env.CreatedAt
				havePending = true
			}
		case envelope.StatusInFlight:
			st.InFlight++
		case envelope.StatusDead:
			st.DLQDepth++
		}
	}
	if havePending {
		if age := now.Sub(oldestPending); age > 0 {
			st.OldestPendingAge = age
		}
	}
	return st, nil
}

// PurgeSucceeded removes SUCCEEDED tasks older than olderThan. Returns the count
// removed.
func (s *Store) PurgeSucceeded(ctx context.Context, queue string, olderThan time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for id, r := range s.tasks {
		if r.env.Queue != queue || r.env.Status != envelope.StatusSucceeded {
			continue
		}
		if r.terminalAt.Before(olderThan) {
			delete(s.tasks, id)
			removed++
		}
	}
	return removed, nil
}

// Capabilities reports no optional accelerations: the reference store is the
// mandatory floor (design 02 §2).
func (s *Store) Capabilities() spi.Capabilities {
	return spi.Capabilities{}
}

// --- internal helpers (callers hold s.mu) ---

// resolveClaim returns the record for id iff token is its current claim token.
// Otherwise it returns ErrStaleClaim and the caller must change nothing.
func (s *Store) resolveClaim(id spi.TaskID, token spi.ClaimToken) (*record, error) {
	r, ok := s.tasks[id]
	if !ok {
		return nil, spi.ErrStaleClaim
	}
	if r.env.Status != envelope.StatusInFlight || r.token == "" || r.token != token {
		return nil, spi.ErrStaleClaim
	}
	return r, nil
}

// appendAttempt appends a clone of att to the task's history and increments
// attempt_count (every recorded attempt counts against max_attempts).
func (s *Store) appendAttempt(r *record, att spi.Attempt) {
	r.env.Attempts = append(r.env.Attempts, cloneAttempt(att))
	r.env.AttemptCount++
}

// selectDead resolves a Selector to the DEAD records in queue. IDs and Filter
// are mutually exclusive; an empty Selector selects nothing (design 02 §2).
func (s *Store) selectDead(queue string, sel spi.Selector) []*record {
	out := make([]*record, 0)
	if len(sel.IDs) > 0 {
		for _, id := range sel.IDs {
			if r, ok := s.tasks[id]; ok && r.env.Queue == queue && r.env.Status == envelope.StatusDead {
				out = append(out, r)
			}
		}
		return out
	}
	if sel.Filter != nil {
		for _, r := range s.tasks {
			if r.env.Queue == queue && r.env.Status == envelope.StatusDead && matchFilter(r, *sel.Filter) {
				out = append(out, r)
			}
		}
	}
	return out
}

// isDue reports whether r is claimable at now.
func isDue(r *record, now time.Time) bool {
	switch r.env.Status {
	case envelope.StatusPending:
		return r.env.NextAttemptAt != nil && !r.env.NextAttemptAt.After(now)
	case envelope.StatusInFlight:
		return r.env.LeaseExpiresAt != nil && !r.env.LeaseExpiresAt.After(now)
	default:
		return false
	}
}

// dueKey is the ordering key for ClaimDue (next_attempt_at, zero time if unset).
func dueKey(r *record) time.Time {
	if r.env.NextAttemptAt != nil {
		return *r.env.NextAttemptAt
	}
	return time.Time{}
}

// leaseExpiredAttempt builds the LEASE_EXPIRED record appended on lease reclaim
// (G7): error.type = "rdq.LeaseExpired", spanning the lost claim's window.
func leaseExpiredAttempt(r *record, now time.Time) spi.Attempt {
	finished := now
	return envelope.Attempt{
		AttemptNo:  r.env.AttemptCount + 1,
		StartedAt:  r.claimedAt,
		FinishedAt: &finished,
		Outcome:    envelope.OutcomeLeaseExpired,
		Error: &envelope.Error{
			Type:    leaseExpiredType,
			Message: "lease expired before an outcome was reported",
		},
	}
}

// matchFilter reports whether r satisfies f (zero-valued fields unconstrained).
func matchFilter(r *record, f spi.DLQFilter) bool {
	if f.HandlerRef != "" && r.env.HandlerRef != f.HandlerRef {
		return false
	}
	if f.ErrorType != "" {
		if !finalErrorTypeIs(r.env, f.ErrorType) {
			return false
		}
	}
	if f.DeadLetteredAfter != nil && r.terminalAt.Before(*f.DeadLetteredAfter) {
		return false // inclusive lower bound
	}
	if f.DeadLetteredBefore != nil && !r.terminalAt.Before(*f.DeadLetteredBefore) {
		return false // exclusive upper bound
	}
	return true
}

// finalErrorTypeIs reports whether the dead-lettering (final) attempt carries an
// error of the given type.
func finalErrorTypeIs(env envelope.Envelope, errorType string) bool {
	if len(env.Attempts) == 0 {
		return false
	}
	last := env.Attempts[len(env.Attempts)-1]
	return last.Error != nil && last.Error.Type == errorType
}

// --- cursor codec ---

func encodeCursor(seq uint64) spi.Cursor {
	raw := cursorPrefix + strconv.FormatUint(seq, 10)
	return spi.Cursor(base64.RawURLEncoding.EncodeToString([]byte(raw)))
}

// decodeCursor resolves a cursor to its seq threshold. The empty cursor starts
// from the beginning (0). Anything that does not decode to a valid tagged token
// is ErrStaleCursor.
func decodeCursor(c spi.Cursor) (uint64, error) {
	if c == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(string(c))
	if err != nil {
		return 0, spi.ErrStaleCursor
	}
	s := string(raw)
	if !strings.HasPrefix(s, cursorPrefix) {
		return 0, spi.ErrStaleCursor
	}
	n, err := strconv.ParseUint(s[len(cursorPrefix):], 10, 64)
	if err != nil {
		return 0, spi.ErrStaleCursor
	}
	return n, nil
}

// --- deep copies (isolate stored state from caller-held references) ---

func cloneEnvelope(in envelope.Envelope) envelope.Envelope {
	out := in
	out.Payload = cloneBytes(in.Payload)
	out.PayloadRef = clonePtrString(in.PayloadRef)
	out.Headers = cloneStringMap(in.Headers)
	out.NextAttemptAt = clonePtrTime(in.NextAttemptAt)
	out.LeaseExpiresAt = clonePtrTime(in.LeaseExpiresAt)
	out.Residual = cloneRawMap(in.Residual)
	if in.Attempts != nil {
		out.Attempts = make([]envelope.Attempt, len(in.Attempts))
		for i, a := range in.Attempts {
			out.Attempts[i] = cloneAttempt(a)
		}
	}
	return out
}

func cloneAttempt(in envelope.Attempt) envelope.Attempt {
	out := in
	out.FinishedAt = clonePtrTime(in.FinishedAt)
	out.Residual = cloneRawMap(in.Residual)
	if in.Error != nil {
		e := *in.Error
		e.Detail = cloneBytes(in.Error.Detail)
		out.Error = &e
	}
	return out
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func clonePtrTime(in *time.Time) *time.Time {
	if in == nil {
		return nil
	}
	t := *in
	return &t
}

func clonePtrString(in *string) *string {
	if in == nil {
		return nil
	}
	v := *in
	return &v
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneRawMap(in map[string]json.RawMessage) map[string]json.RawMessage {
	if in == nil {
		return nil
	}
	out := make(map[string]json.RawMessage, len(in))
	for k, v := range in {
		out[k] = cloneBytes(v)
	}
	return out
}
