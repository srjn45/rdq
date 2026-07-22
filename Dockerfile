# SPDX-License-Identifier: Apache-2.0
# Multi-stage build for rdq-server.
# Stage 1 compiles the binary inside a Go workspace so cross-module imports
# (server → storage/postgres, server → core, …) resolve without network access.
# Stage 2 produces a minimal runtime image with just the binary.

# ─── Stage 1: build ───────────────────────────────────────────────────────────
# golang:1.23 base with GOTOOLCHAIN=auto so the go.work toolchain requirement
# (go 1.26) is downloaded automatically during the build.
FROM golang:1.23-alpine AS builder

WORKDIR /src

# Copy workspace manifest first (layer-cache friendly: only invalidates when
# workspace layout changes, not on every source edit).
COPY go.work go.work.sum ./

# Copy each module directory.
COPY core/       ./core/
COPY storage/    ./storage/
COPY server/     ./server/
COPY sdk-go/     ./sdk-go/
COPY cli/        ./cli/
COPY integration/ ./integration/

# Allow the Go toolchain manager to upgrade to the workspace-required version.
ENV GOTOOLCHAIN=auto

# Download workspace dependencies.
RUN go work sync

# Build the server binary; CGO_ENABLED=0 for a fully static binary.
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /rdq-server \
      ./server/cmd/rdq-server/

# ─── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

COPY --from=builder /rdq-server /rdq-server

# Expose the default API port.  Override with RDQ_ADDR at runtime.
EXPOSE 8080

ENTRYPOINT ["/rdq-server"]
