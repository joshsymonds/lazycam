{
  description = "lazycam — on-demand v4l2loopback producer gating, for OBS virtual-cam privacy";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    devenv.url = "github:cachix/devenv";
    git-hooks = {
      url = "github:cachix/git-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  nixConfig = {
    extra-trusted-public-keys = "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=";
    extra-substituters = "https://devenv.cachix.org";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    devenv,
    ...
  } @ inputs:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {inherit system;};
      lazycam-package = pkgs.callPackage ./package.nix {};
    in {
      packages = rec {
        lazycam = lazycam-package;
        default = lazycam;
      };

      apps = rec {
        lazycam = flake-utils.lib.mkApp {
          drv = self.packages.${system}.lazycam;
        };
        default = lazycam;
      };

      devShells.default = devenv.lib.mkShell {
        inherit inputs pkgs;
        modules = [./devenv.nix];
      };
    });
}
