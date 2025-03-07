let

  # This could be "nixos-24.11", "nixos-unstable", a git hash, etc.
  nixpkgsVersion = "d69ab0d71b22fa1ce3dbeff666e6deb4917db049";
  nixpkgsTarball = "https://github.com/NixOS/nixpkgs/archive/${nixpkgsVersion}.tar.gz";

  # This could be "master", a git hash, etc.
  rustOverlayVersion = "4e9af61c1a631886cdc7e13032af4fc9e75bb76b";
  rustOverlayTarball = "https://github.com/oxalica/rust-overlay/archive/${rustOverlayVersion}.tar.gz";

  rustOverlay = import (fetchTarball rustOverlayTarball);

  pkgs =
    import (fetchTarball nixpkgsTarball) {
      overlays = [
        rustOverlay
      ];
    };

  rustPlatform =
    pkgs.makeRustPlatform {
      cargo = pkgs.rust-bin.stable."1.85.0".minimal;
      rustc = pkgs.rust-bin.stable."1.85.0".minimal;
    };

in

  rustPlatform.buildRustPackage rec {
    name = "bigtent";
    version = "0.7.4";

    src = ./.;

    useFetchCargoVendor = true;
    cargoHash = "sha256-BeKjLcj5WG5x/0XsvN5KSI0XmNgISbLJKuPnxdq2I1Y=";
  }
