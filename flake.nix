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
  # System-agnostic outputs (modules, overlays, plain paths) live
  # outside eachDefaultSystem; per-system outputs (packages, apps,
  # devShells) live inside.
    {
      homeManagerModules = {
        default = import ./nix/home-manager-module.nix;
        lazycam = import ./nix/home-manager-module.nix;
      };

      # Quickshell "modules" are import paths — there is no formal
      # module system. Exposing the QML directory directly lets a
      # consuming flake (DMS) reach the .qml files via
      # `inputs.lazycam.quickshellModules.lazycam-indicator` and pass
      # that path into its own Quickshell import context.
      quickshellModules = {
        default = ./quickshell/lazycam-indicator;
        lazycam-indicator = ./quickshell/lazycam-indicator;
      };
    }
    // flake-utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {inherit system;};
      lazycam-package = pkgs.callPackage ./package.nix {};
      lazycam-indicator-package = pkgs.callPackage ./nix/quickshell-package.nix {};
    in {
      packages = rec {
        lazycam = lazycam-package;
        lazycam-indicator = lazycam-indicator-package;
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
