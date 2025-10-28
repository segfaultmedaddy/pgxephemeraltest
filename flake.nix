{
  description = "foundations - a modular library designed to build maintainable production-grade systems.";

  inputs = {
    devenv.url = "github:cachix/devenv";
    nixpkgs.url = "github:cachix/devenv-nixpkgs/rolling";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-root.url = "github:srid/flake-root";
  };

  outputs =
    {
      flake-parts,
      ...
    }@inputs:
    flake-parts.lib.mkFlake { inherit inputs; } {
      flake = { };

      systems = [
        "x86_64-linux"
        "x86_64-darwin"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      imports = [
        inputs.flake-root.flakeModule
        inputs.treefmt-nix.flakeModule
        inputs.devenv.flakeModule
      ];

      perSystem =
        {
          pkgs,
          lib,
          config,
          ...
        }:
        {
          formatter = config.treefmt.build.wrapper;

          treefmt.config = {
            inherit (config.flake-root) projectRootFile;
            package = pkgs.treefmt;

            programs = {
              nixfmt.enable = true;
              gofumpt.enable = true;
              yamlfmt.enable = true;
              typos.enable = true;
            };
          };

          devenv.shells.default = {
            cachix.enable = true;
            cachix.pull = [
              "devenv"
              "pre-commit-hooks"
            ];

            containers = lib.mkForce { };

            packages = with pkgs; [
              go
              gopls
              govulncheck
              golangci-lint
            ];

            scripts = {
              lint = {
                exec = ''
                  modernize ./...
                  govulncheck ./...
                  golangci-lint run ./...
                '';
              };
              lint-fix = {
                exec = ''
                  modernize --fix ./...
                  golangci-lint run --fix ./...
                '';
              };
            };

            git-hooks = {
              hooks = {
                lint = {
                  enable = true;
                  name = "lint";
                  description = "Lint";
                  entry = ''
                    lint
                  '';
                  pass_filenames = false;
                };
              };
            };

            languages.go = {
              enable = true;
              package = pkgs.go_1_25;
            };

            env.GOTOOLCHAIN = lib.mkForce "local";
            env.GOFUMPT_SPLIT_LONG_LINES = lib.mkForce "on";
          }
          // (
            let
              user = "test";
              password = "test";
              db = "test";
            in
            rec {
              env.TEST_DATABASE_URL = lib.mkForce "postgres://${user}:${password}@localhost:${toString services.postgres.port}/${db}";
              services.postgres = {
                enable = true;
                package = pkgs.postgresql_17;

                initialScript = ''
                  CREATE USER ${user} SUPERUSER PASSWORD '${password}';
                  CREATE DATABASE ${db} OWNER ${user};
                '';
                listen_addresses = "localhost";
                port = 6543;
              };
            }
          );
        };
    };
}
