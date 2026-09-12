set shell := ["bash", "-cu"]

# Lints and runs all tests
default: lint test

# Ensures that all tools required for local development are installed
install-tools:
    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.10.1
    go install gotest.tools/gotestsum@v1.13.0

# Builds and installs all tools to the GOBIN on the local machine
install:
    go install ./cmd/...

# Lints the code
lint:
    golangci-lint run --timeout 3m ./...

# Updates to latest go practices
modernize:
    modernize -w ./...

# Runs the tests
test *ARGS="./...":
    just test-long -short {{ARGS}}

# Runs the tests (including long-running tests)
test-long *ARGS="./...":
    gotestsum --format-hide-empty-pkg --format-icons hivis -- -count=1 {{ARGS}}

# Runs the tests with the race detector enabled
test-race *ARGS="./...":
    just test-long -race {{ARGS}}

# Runs the opt-in wire gate against immutable local atproto and Bulletin pins
test-spaces-interop:
    bash ./scripts/test-spaces-interop.sh

# Regenerates all API types from the cached lexicon schemas
lexgen:
    test -d lexicons || { echo "lexicon cache is absent; run just update-lexicons" >&2; exit 1; }
    test -d lexicons-space || { echo "space lexicon overlay is absent" >&2; exit 1; }
    go run ./cmd/lexgen -lexdir lexicons -lexdir lexicons-space -config lexgen.json

# Hydrates the ignored main lexicon cache from immutable lock-file commits
hydrate-lexicons:
    ./scripts/hydrate-lexicons.sh

# Runs benchmarks
bench *ARGS="./...":
    go test -bench=. -benchmem -count=1 -run='^$' {{ARGS}}

# Builds the WASM binary, gzips it, and copies wasm_exec.js
wasm:
    GOOS=js GOARCH=wasm go build -ldflags="-s -w" -o wasm/atmos.wasm ./wasm/
    gzip -k -f wasm/atmos.wasm
    cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" wasm/

# Runs tests under GOOS=js/wasm via Node (closest to in-browser WASM)
test-wasm:
    env -i HOME="$HOME" PATH="$PATH:$(go env GOROOT)/lib/wasm" GOOS=js GOARCH=wasm just test

# Runs fuzz tests for the given duration (default 10s per target)
fuzz DURATION="10s" *ARGS="./...":
    #!/usr/bin/env bash
    set -euo pipefail
    pkgs="{{ARGS}}"
    for pkg in $(go list $pkgs); do
        targets=$(go test "$pkg" -list '^Fuzz' -run '^$' -count=1 2>/dev/null | grep '^Fuzz' || true)
        for t in $targets; do
            echo "=== FUZZ $t ($pkg) ==="
            go test "$pkg" -run='^$' -fuzz="^${t}$" -fuzztime={{DURATION}}
        done
    done

# Fetches, vendors, and generates from the latest upstream lexicons.
#
# `lexgen.lock` records the immutable upstream commits used to
# generate the checked-in API. The bsky repository is authoritative for the
# app.bsky and chat.bsky namespaces; atproto supplies every other namespace.
update-lexicons:
    ./scripts/update-lexicons.sh

# Updates the checked-in spaces overlay to an explicit immutable atproto commit
update-space-lexicons SHA:
    ./scripts/update-space-lexicons.sh {{SHA}}
