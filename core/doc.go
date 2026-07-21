// Package core is the rdq engine and the home of the frozen storage contract:
// the language-neutral task/envelope model, the storage service-provider
// interface every backend implements, and the compliance kit that verifies a
// backend against that interface. Retry-policy evaluation and outcome
// classification (M3) build on these packages.
//
// As of milestone M1 the contract below is FROZEN: the Postgres binding (M2),
// the Go SDK/server (M4–M6), and the Java SDK (M7) are all written against it,
// and the golden fixtures under envelope/testdata are replayed byte-for-byte in
// every language. Changing a wire field, a Storage signature, an error
// sentinel, or a fixture is a contract break, not a refactor. Contracts are
// specified in docs/design/01-wire-envelope.md and 02-storage-spi.md.
//
// # Sub-packages
//
//   - envelope  — the wire model (Envelope, Attempt, Error, Status, Outcome)
//     with the canonical RFC-3339-millis / integer-ms / base64 codec, unknown-
//     field preservation, and validation/truncation helpers. The golden
//     fixtures in envelope/testdata are the cross-language JSON contract.
//   - spi       — the frozen Storage interface plus its value types (Claimed,
//     ClaimToken, DLQFilter, Selector, Page, Cursor, Stats, Capabilities) and
//     error sentinels (ErrStaleClaim, ErrNotFound, ErrStaleCursor,
//     ErrIDConflict).
//   - compliance — Run(t, factory), the eight-invariant conformance suite a
//     backend must pass to be considered a correct Storage.
//   - memstore  — the in-memory reference Storage: correct, mutex-guarded, and
//     the substrate the engine's own tests run against.
//
// # Writing a storage plugin
//
// Implement spi.Storage, then in a test call compliance.Run against a factory
// that constructs your store. When the kit is green your backend is a
// drop-in — the engine assumes nothing beyond the SPI floor. See core/README.md
// for the walkthrough.
//
// # Invariants a backend guarantees
//
// The storage backend's clock is the single authority for due-ness and lease
// expiry (G9). Every mutating method is atomic (all-or-nothing), and claims are
// fenced by ClaimToken so at most one live claim of a task exists at any moment;
// a stale token fails with ErrStaleClaim and changes nothing. These are the
// properties that make at-least-once delivery hold across crashes.
package core
