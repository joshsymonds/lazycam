# lazycam — task runner.
#
# `just` (with no recipe) lists everything available.

default:
    @just --list

# Build the binary via nix flake. Output at ./result/bin/lazycam.
build:
    nix build

# Run the binary forwarding extra args, e.g. `just run --device /dev/video10`.
run *args: build
    ./result/bin/lazycam {{ args }}

# Lint Go code: golangci-lint (v2 config in .golangci.yml) + deadcode.
# deadcode catches unused funcs/types across packages — golangci's
# `unused` linter is conservative on cross-package reach analysis.
lint:
    golangci-lint run ./...
    deadcode -test ./...

# Run gofmt + goimports over our own .go files, writing fixes in place.
# Excludes the devenv/direnv/git state dirs (which contain the Go module
# cache and intentionally-malformed test fixtures from x/tools).
fmt:
    #!/usr/bin/env bash
    set -euo pipefail
    mapfile -t files < <(just _go-files)
    if [ "${#files[@]}" -eq 0 ]; then exit 0; fi
    gofmt -w "${files[@]}"
    goimports -w "${files[@]}"

# Verify formatting WITHOUT writing changes. Fails if any file would be
# reformatted. Used by `just check` and the pre-push git hook.
fmt-check:
    #!/usr/bin/env bash
    set -euo pipefail
    mapfile -t files < <(just _go-files)
    if [ "${#files[@]}" -eq 0 ]; then exit 0; fi
    out=$(gofmt -l "${files[@]}")
    if [ -n "$out" ]; then
        echo "gofmt would change:"
        echo "$out"
        exit 1
    fi
    out=$(goimports -l "${files[@]}")
    if [ -n "$out" ]; then
        echo "goimports would change:"
        echo "$out"
        exit 1
    fi

# (private) Emit our own .go files one per line, pruning build state.
_go-files:
    @find . \
        -type d \( -name .devenv -o -name .direnv -o -name .git -o -name vendor \) -prune \
        -o -type f -name '*.go' -print

# Run all Go tests.
test:
    go test -count=1 ./...

# Run tests with the race detector. Slower; not in `just check`.
test-race:
    go test -race -count=1 ./...

# Tidy go.mod / go.sum.
tidy:
    go mod tidy

# Run lint + fmt-check + test in parallel. Same recipe the pre-push
# git-hook fires. Fail-fast aggregation pattern lifted from savecraft.gg.
check:
    #!/usr/bin/env bash
    set -uo pipefail
    tmpdir=$(mktemp -d)
    trap 'rm -rf "$tmpdir"' EXIT
    pids=()
    names=()
    spawn() {
        local name=$1; shift
        "$@" >"$tmpdir/$name.out" 2>&1 &
        pids+=($!)
        names+=("$name")
    }
    spawn lint       just lint
    spawn fmt-check  just fmt-check
    spawn test       just test
    failed=0
    for i in "${!pids[@]}"; do
        if ! wait "${pids[$i]}"; then
            echo "==> FAIL: ${names[$i]}"
            cat "$tmpdir/${names[$i]}.out"
            failed=1
        else
            echo "==> OK: ${names[$i]}"
        fi
    done
    exit $failed

# Remove nix build artifacts.
clean:
    rm -rf result result-*

# Show the current vendor hash (useful when bumping deps + needing to
# update package.nix). Strips the SRI prefix.
vendor-hash:
    #!/usr/bin/env bash
    set -euo pipefail
    # Nix prints the correct hash in the error when vendorHash is wrong.
    # Force a mismatch by clobbering it through an override, then read
    # the "got: sha256-..." line out of stderr.
    nix build --no-link \
        --override-input nixpkgs nixpkgs \
        --impure --expr "
          let
            flake = builtins.getFlake (toString ./.);
            pkgs = flake.inputs.nixpkgs.legacyPackages.\${builtins.currentSystem};
          in
          (pkgs.callPackage ./package.nix {}).overrideAttrs (_: {
            vendorHash = pkgs.lib.fakeHash;
          })
        " 2>&1 | awk '/got:[[:space:]]*sha256-/ {print $2; exit}'
