// SPDX-License-Identifier: Apache-2.0

package spi

// Compile-time check that a value type satisfies the interface shape at all.
// This is a smoke assertion for the frozen contract; real backends (T1.6+)
// carry their own conformance tests via the compliance kit.
var _ Storage = (*noopStorage)(nil)

// noopStorage is an unimplemented placeholder used only to pin the interface
// signatures at compile time.
type noopStorage struct{ Storage }
