{
  description = "native-link";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    pre-commit-hooks.url = "github:cachix/pre-commit-hooks.nix";
  };

  outputs = inputs@{ nixpkgs, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" ];
      imports = [ inputs.pre-commit-hooks.flakeModule ];
      perSystem = { config, self', inputs', pkgs, system, ... }:
        let
          hooks = import ./tools/pre-commit-hooks.nix { inherit pkgs; };
        in
        {
          pre-commit.settings = { inherit hooks; };
          devShells.default = pkgs.mkShell {
            nativeBuildInputs = [
              # Development tooling goes here.
              pkgs.cargo
              pkgs.rustc
              pkgs.pre-commit
              pkgs.bazel
              pkgs.awscli2
              pkgs.buck2
              pkgs.reindeer
            ];
            shellHook = ''
              # Generate the .pre-commit-config.yaml symlink when entering the
              # development shell.
              ${config.pre-commit.installationScript}

              # The Bazel and Cargo builds in nix require a Clang toolchain.
              # TODO(aaronmondal): The Bazel build currently uses the
              #                    irreproducible host C++ toolchain. Provide
              #                    this toolchain via nix for bitwise identical
              #                    binaries across machines.
              export CC=clang
            '';
          };
        };
    };
}
