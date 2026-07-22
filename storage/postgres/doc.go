// SPDX-License-Identifier: Apache-2.0

// Package postgres is the reference storage plugin for rdq (PostgreSQL).
// Atomic claims via FOR UPDATE SKIP LOCKED; see docs/design/02-storage-spi.md §4.
package postgres
