{pkgs, ...}: let
  excludes = ["^nativelink-proto/genproto"];
in {
  # Default hooks
  trailing-whitespace-fixer = {
    inherit excludes;
    enable = true;
    name = "trailing-whitespace";
    description = "Remove trailing whitespace";
    entry = "${pkgs.python311Packages.pre-commit-hooks}/bin/trailing-whitespace-fixer";
    types = ["text"];
  };
  end-of-file-fixer = {
    inherit excludes;
    enable = true;
    name = "end-of-file-fixer";
    description = "Remove trailing whitespace";
    entry = "${pkgs.python311Packages.pre-commit-hooks}/bin/end-of-file-fixer";
    types = ["text"];
  };
  fix-byte-order-marker = {
    inherit excludes;
    enable = true;
    name = "fix-byte-order-marker";
    entry = "${pkgs.python311Packages.pre-commit-hooks}/bin/fix-byte-order-marker";
  };
  mixed-line-ending = {
    inherit excludes;
    enable = true;
    name = "mixed-line-ending";
    entry = "${pkgs.python311Packages.pre-commit-hooks}/bin/mixed-line-ending";
    types = ["text"];
  };
  check-case-conflict = {
    inherit excludes;
    enable = true;
    name = "check-case-conflict";
    entry = "${pkgs.python311Packages.pre-commit-hooks}/bin/check-case-conflict";
    types = ["text"];
  };
  detect-private-key = {
    excludes =
      excludes
      ++ [
        # Integration testfiles not intended for production.
        "deployment-examples/docker-compose/example-do-not-use-in-prod-key.pem"
        "deployment-examples/kubernetes/example-do-not-use-in-prod-key.pem"
        "deployment-examples/kubernetes-classic/example-do-not-use-in-prod-key.pem"
      ];
    enable = true;
    name = "detect-private-key";
    entry = "${pkgs.python311Packages.pre-commit-hooks}/bin/detect-private-key";
    types = ["text"];
  };
  forbid-binary-files = {
    excludes = [
      # Landing page image for the website.
      "nativelink-docs/static/img/hero-dark.png"

      # Testdata for fastcdc.
      "nativelink-util/tests/data/SekienAkashita.jpg"
    ];
    enable = true;
    types = ["binary"];
    entry = let
      script = pkgs.writeShellScriptBin "forbid-binary-files" ''
        set -eu

        if [ $# -gt 0 ]; then
          for filename in "''${@}"; do
            printf "[\033[31mERROR\033[0m] Found binary file: ''${filename}"
          done
          exit 1
        fi
      '';
    in "${script}/bin/forbid-binary-files";
  };

  # Documentation
  vale.enable = true;

  # Nix
  alejandra.enable = true;
  statix.enable = true;
  deadnix.enable = true;

  # Rust
  rustfmt.enable = true;

  # Starlark
  bazel-buildifier-format = {
    enable = true;
    name = "buildifier format";
    description = "Format Starlark";
    entry = "${pkgs.bazel-buildtools}/bin/buildifier -lint=fix";
    types = ["bazel"];
  };
  bazel-buildifier-lint = {
    enable = true;
    name = "buildifier lint";
    description = "Lint Starlark";
    entry = "${pkgs.bazel-buildtools}/bin/buildifier -lint=warn";
    types = ["bazel"];
  };

  # Dockerfile
  hadolint.enable = true;
}
