{ pkgs, crate2nixTools }:
let
  rustPlatform = pkgs.makeRustPlatform {
    cargo = pkgs.rust-bin.stable.latest.default;
    rustc = pkgs.rust-bin.stable.latest.default;
  };
  buildRustCrateForPkgs = crate: pkgs.buildRustCrate.override {
    rustc = pkgs.rust-bin.stable.latest.default;
    cargo = pkgs.rust-bin.stable.latest.default;
  };
  generatedCargoNix = crate2nixTools.generatedCargoNix {
    name = "minidex";
    src = ./.;
  };
  cargoNix = import generatedCargoNix {
    inherit pkgs buildRustCrateForPkgs;
  };
in {
  server = cargoNix.rootCrate.build;

  devShell = pkgs.mkShell {
    packages = with pkgs; [
      rust-bin.stable.latest.default
      rust-analyzer
    ];
  };
}
