{pkgs, ...}: {
  # Pre-push hook runs the same `just check` parallel suite a developer
  # runs locally: lint + fmt-check + test. Bypass with `git push --no-verify`
  # if you need to.
  git-hooks.hooks.check = {
    enable = true;
    name = "check";
    description = "Run lint, format check, and tests";
    entry = "just check";
    language = "system";
    pass_filenames = false;
    stages = ["pre-push"];
  };

  packages = [
    # Go toolchain — pinned to the latest minor so dev/CI/nix-build all
    # see the same compiler. Bump explicitly when adopting new releases.
    pkgs.go_1_26
    pkgs.gopls
    pkgs.go-tools # staticcheck (used by golangci-lint internally too)
    pkgs.golangci-lint # version-2 config in .golangci.yml
    pkgs.delve

    # Project automation
    pkgs.just # Justfile runner

    # Shell linting (Justfile recipes use bash)
    pkgs.shellcheck
    pkgs.shfmt
  ];

  enterShell = ''
    export GOEXPERIMENT=jsonv2
    export GOPATH="$DEVENV_STATE/go"
    export GOMODCACHE="$GOPATH/pkg/mod"
    export PATH="$GOPATH/bin:$PATH"

    # Install Go tools with the project's Go version. nixpkgs's prebuilt
    # gotools are built against whatever Go that channel settled on — using
    # `go install` keeps these in lockstep with the project's go.mod toolchain.
    if ! command -v goimports &>/dev/null; then
      go install golang.org/x/tools/cmd/goimports@latest
    fi
    if ! command -v deadcode &>/dev/null; then
      go install golang.org/x/tools/cmd/deadcode@latest
    fi
  '';
}
