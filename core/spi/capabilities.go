// SPDX-License-Identifier: Apache-2.0

package spi

// Capabilities advertises optional backend features. The engine always works
// against the mandatory floor of the Storage interface; capabilities only
// remove latency or transfer cost, never change correctness (design 02 §2).
type Capabilities struct {
	// Notify indicates the backend can block until a task may be due
	// (WaitDue-style push); claims still go through ClaimDue. Absent it, the
	// engine polls.
	Notify bool
	// FilterPushdown indicates DLQFilter is evaluated natively by the backend;
	// absent it, core paginates and filters client-side.
	FilterPushdown bool
	// BatchEnqueue indicates the backend has a native multi-task enqueue path.
	BatchEnqueue bool
}
