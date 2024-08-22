{ pkgs, lib, config, inputs, ... }:

{
  # https://devenv.sh/packages/
  packages = [ pkgs.git ];

  enterShell = ''
    git --version
  '';

  # https://devenv.sh/tests/
  enterTest = ''
    echo "Running tests"
    git --version | grep --color=auto "${pkgs.git.version}"
  '';

  languages.python.enable = true;
  languages.python.version = "3.12.4";
  languages.python.poetry.enable = false;

  pre-commit.hooks = {
    check-added-large-files.enable = true;
    check-toml.enable = true;
    check-yaml.enable = true;
    end-of-file-fixer.enable = true;
    mixed-line-endings.enable = true;
    trim-trailing-whitespace.enable = true;

    # lint shell scripts
    shellcheck.enable = true;

    # format Python code
    black.enable = true;
    black.settings.flags = "-l 79";

    isort.enable = true;
    isort.settings.flags = "--float-to-top";

    nixfmt.enable = true;

    markdownlint.enable = true;
  };
}
