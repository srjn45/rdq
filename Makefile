# SPDX-License-Identifier: Apache-2.0
# Makefile for rdq — common build targets.

.PHONY: build release-build docker-build test

# Build all Go modules in the workspace.
build:
	go build ./...

# Cross-compile CLI binaries for Linux/macOS/Windows (amd64 + arm64).
# Output lands in dist/ — matches what goreleaser --snapshot would produce.
# This is what CI runs; no upload, no tag, no publish.
release-build:
	goreleaser build --snapshot --clean --config .goreleaser.yaml

# Build the rdq-server Docker image (no push).
docker-build:
	docker build -t rdq-server:dev .

# Run tests for all Go modules in the workspace.
test:
	for mod in core storage/postgres server cli sdk-go; do \
		echo "--- $$mod"; \
		(cd "$$mod" && go test ./...); \
	done
