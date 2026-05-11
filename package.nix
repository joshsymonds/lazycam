{
  buildGo126Module,
}:
# Pinned to Go 1.26 to match devenv.nix's pkgs.go_1_26 — keeps dev,
# CI, and nix build using the same compiler. Bump in lockstep with
# devenv.nix when adopting a new minor.
buildGo126Module {
  pname = "lazycam";
  version = "1.0.0";
  src = ./.;
  # vendorHash auto-discovered by `just vendor-hash` (or `nix build`,
  # which prints the "got: sha256-..." line on mismatch). Update when
  # go.mod/go.sum changes.
  vendorHash = "sha256-86S0gZ4p4VTZhqwWq0XaCjCBqvsEbdXMIBxsxA86woQ=";
  meta.mainProgram = "lazycam";
}
