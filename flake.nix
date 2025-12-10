{
  inputs = {
    devenv.url = "github:cachix/devenv";
    nixpkgs.url = "nixpkgs/nixos-unstable";
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
            containers = lib.mkForce { };

            cachix.enable = true;
            cachix.pull = [
              "devenv"
              "pre-commit-hooks"
            ];

            packages = with pkgs; [
              go
              gotools
              gopls
              govulncheck
              golangci-lint
              mockgen
              just
            ];

            git-hooks = {
              hooks = {
                check = {
                  enable = true;
                  name = "check";
                  description = "Nix Check";
                  entry = ''
                    nix flake check . --no-pure-eval
                  '';
                  pass_filenames = false;
                };

                lint = {
                  enable = true;
                  name = "lint";
                  description = "Go Lint";
                  entry = ''
                    just lint
                  '';
                  pass_filenames = false;
                };
              };
            };

            languages.go = {
              enable = true;
              package = pkgs.go;
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

                settings = {
                  fsync = "off";
                  full_page_writes = "off";
                  synchronous_commit = "off";
                  log_statement = "all";
                  shared_buffers = "128MB";
                };

                initialScript = ''
                  CREATE USER u1 SUPERUSER PASSWORD 'u1';
                  CREATE USER u2 SUPERUSER PASSWORD 'u2';

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
