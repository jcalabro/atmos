set shell := ["bash", "-cu"]

# Lints and runs all tests
default: lint test

# Ensures that all tools required for local development are installed
install-tools:
    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
    go install gotest.tools/gotestsum@v1.13.0

# Lints the code
lint:
    golangci-lint run --timeout 1m ./...

# Runs the tests
test *ARGS="./...":
    gotestsum --format-hide-empty-pkg --format-icons hivis -- -count=1 {{ARGS}}

# Runs the tests with the race detector enabled
test-rac *ARGS="./...":
    gotestsum --format-hide-empty-pkg --format-icons hivis -- -race -count=1 {{ARGS}}

# Regenerates all API types from vendored lexicon schemas
lexgen:
    go run ./cmd/lexgen -lexdir lexicons -config lexgen.json

# Runs benchmarks
bench *ARGS="./...":
    go test -bench=. -benchmem -count=1 -run='^$' {{ARGS}}

# Builds the WASM binary and copies wasm_exec.js
wasm:
    GOOS=js GOARCH=wasm go build -ldflags="-s -w" -o wasm/atproto.wasm ./wasm/
    cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" wasm/

# Runs tests under GOOS=js/wasm via Node (closest to in-browser WASM)
test-wasm:
    PATH="$PATH:$(go env GOROOT)/lib/wasm" GOOS=js GOARCH=wasm go test -short -count=1 ./...

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

# Pulls and builds the latest lexicons from the atproto repo (assuming well-structured GOPATH)
update-lexicons:
    #!/usr/bin/env bash

    rm -rf lexicons/*
    cp -r ../../bluesky-social/atproto/lexicons/* lexicons
    just lexgen
