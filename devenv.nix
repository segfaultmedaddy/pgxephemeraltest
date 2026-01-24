{
  pkgs,
  config,
  ...
}:
{
  # Packages
  packages = with pkgs; [
    go
    gotools
    gopls
    govulncheck
    golangci-lint
    mockgen
    treefmt
    gofumpt
  ];

  # Cachix
  cachix.enable = false;

  # Go
  env.GOFUMPT_SPLIT_LONG_LINES = "on";
  languages.go = {
    enable = true;
    package = pkgs.go;
  };

  treefmt = {
    enable = true;
    config.programs = {
      nixfmt.enable = true;
      gofumpt.enable = true;
      yamlfmt.enable = true;
      typos.enable = true;
    };
  };

  # Tasks (migrated from Justfile)
  tasks = {
    "go:mod" = {
      exec = ''
        go mod tidy
        go mod download
      '';
      description = "Tidy and download Go modules";
    };

    "go:gen" = {
      exec = ''
        go generate ./...
      '';
      description = "Run Go generate";
    };

    "go:lint" = {
      exec = ''
        modernize ./...
        golangci-lint run --new-from-rev=HEAD ./...
      '';
      description = "Run Go linters";
    };

    "go:lint-fix" = {
      exec = ''
        modernize --fix ./...
        golangci-lint run --fix ./...
        treefmt
      '';
      description = "Fix Go lint issues and format";
    };

    "go:test" = {
      exec = ''
        go test -race -count=1 -timeout=90s -cover ./...
      '';
      description = "Run Go tests";
    };

    "go:test-ci" = {
      exec = ''
        devenv up -d
        go test -race -timeout=90s -cover ./...
        devenv processes down
      '';
      description = "Run Go tests with services";
    };
  };

  # Git hooks
  git-hooks.hooks = {
    treefmt.enable = true;

    lint = {
      enable = true;
      name = "lint";
      description = "Go Lint";
      entry = "devenv tasks run go:lint";
      types = [ "go" ];
      pass_filenames = false;
    };
  };
}
// (
  let
    user = "test";
    password = "test";
    db = "test";
  in
  {
    env.TEST_DATABASE_URL = "postgres://${user}:${password}@localhost:${toString config.services.postgres.port}/${db}";
    services.postgres = {
      enable = true;
      package = pkgs.postgresql_17;
      port = 6543;
      listen_addresses = "localhost";

      settings = {
        fsync = "off";
        full_page_writes = "off";
        synchronous_commit = "off";
        log_statement = "all";
        shared_buffers = "128MB";
        max_connections = "10000";
      };

      initialScript = ''
        CREATE USER u1 SUPERUSER PASSWORD 'u1';
        CREATE USER u2 SUPERUSER PASSWORD 'u2';

        CREATE USER ${user} SUPERUSER PASSWORD '${password}';
        CREATE DATABASE ${db} OWNER ${user};
      '';
    };
  }
)
