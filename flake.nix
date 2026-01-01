{
  description = "A resource-efficient indexer";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flake-utils = {
      url = "github:numtide/flake-utils";
    };
    nil = {
      url = "github:oxalica/nil";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crate2nix = {
      url = "github:nix-community/crate2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, nil, rust-overlay, crate2nix }:
    flake-utils.lib.eachDefaultSystem (system:
      let pkgs = import nixpkgs {
            inherit system;

            overlays = [ nil.overlays.nil
                         rust-overlay.overlays.default
                       ];
          };

          crate2nixTools = crate2nix.tools.${system};

          minidex = import ./default.nix { inherit pkgs crate2nixTools; };
      in {
        packages = {
          minidex-bin = minidex.server;
        };

        devShells = {
          default = minidex.devShell;
        };
      }
    );
}
