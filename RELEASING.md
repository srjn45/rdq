# Releasing rdq

> **PREP ONLY** — real publishes are approval-gated (T8.4, design 06).  
> No tags or uploads happen automatically. Every step below requires a human operator.

---

## Go module tags

rdq is a **go.work multi-module workspace**. Each publishable module is tagged
independently so consumers can depend on just the subset they need:

| Module path | Tag prefix | Example |
|---|---|---|
| `core/` | `core/v` | `core/v2.1.0` |
| `storage/postgres/` | `storage/postgres/v` | `storage/postgres/v2.1.0` |
| `server/` | `server/v` | `server/v2.1.0` |
| `cli/` | `cli/v` | `cli/v2.1.0` |
| `sdk-go/` | `sdk-go/v` | `sdk-go/v2.1.0` |

All modules share the same version number for a given release round.

### Tag procedure (operator, after PR merged to main)

```bash
VERSION=2.1.0

for mod in core storage/postgres server cli sdk-go; do
  git tag "${mod}/v${VERSION}"
done

# Push all tags at once (requires a human to run):
git push origin --tags
```

> **Never push tags from CI** — tag pushes are intentionally absent from every
> workflow file. Each `git push origin --tags` above is a human action that
> constitutes release approval.

---

## Java SDK (Maven Central)

### Prerequisites

1. **T0.2 — Sonatype namespace registered** (`io.github.srjn45`). Not yet done;
   blocks all Maven publishes. File the namespace claim at
   <https://central.sonatype.com/> once T0.2 is approved.
2. **GPG signing key** — an ASCII-armored GPG private key and its passphrase
   stored as repository secrets `MAVEN_SIGNING_KEY` and `MAVEN_SIGNING_PASSWORD`.
3. **Sonatype credentials** — stored as `MAVEN_CENTRAL_USERNAME` and
   `MAVEN_CENTRAL_PASSWORD`.

### Publish procedure

1. Bump `version` in `sdk-java/build.gradle.kts` (propagates to all subprojects).
2. Merge the version bump PR to `main`.
3. Navigate to **Actions → Publish to Maven Central → Run workflow** in GitHub.
4. Enter the version (e.g. `2.1.0`) and set **dry_run** to `true` first.
5. Inspect the dry-run artifacts; if correct, re-run with **dry_run = false**.
6. Release the staging repository in Sonatype OSSRH (required for Maven Central
   promotion).

Artifacts published:
- `io.github.srjn45:rdq-client:<version>`
- `io.github.srjn45:rdq-worker:<version>`

The `:example` subproject is **not published** — it is a runnable demo only.

---

## CLI binary release

```bash
# After tagging cli/vX.Y.Z:
goreleaser release --config .goreleaser.yaml
```

CI builds snapshot binaries on every PR (`goreleaser build --snapshot --clean`)
to verify cross-compilation. The actual release (`goreleaser release`) is
operator-run and requires a valid `cli/vX.Y.Z` tag to be pushed first.

---

## Docker image

```bash
# Build locally:
docker build -t ghcr.io/srjn45/rdq-server:2.1.0 .

# Push (approval-gated — never done by CI):
docker push ghcr.io/srjn45/rdq-server:2.1.0
docker tag ghcr.io/srjn45/rdq-server:2.1.0 ghcr.io/srjn45/rdq-server:latest
docker push ghcr.io/srjn45/rdq-server:latest
```

CI builds the image (`docker build`) on every PR to verify the Dockerfile; it
never pushes.
