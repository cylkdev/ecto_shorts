# Create Elixir Project

This workflow is the source of truth for creating a new Elixir or Phoenix project from a clean macOS machine with `zsh` and getting the same result every time.

Treat the reader as a complete beginner to this repository. They have only the current working tree and this workflow file. There is no memory of prior setup and no external context.

Follow the steps in order. Do not skip verification steps. Use the documented default values unless you intentionally override them through the input parameters in this workflow. If a command fails, fix that failure before you continue to the next step.

This workflow stays deterministic by fixing the toolchain first, choosing the project shape once, checking whether the exact required tools already exist before installing anything, and telling you exactly which files own the post-generation baseline changes.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

---

## Input Parameters

Accept the following input parameters:

- `project_shape`: Required. Allowed values are `mix_app`, `mix_app_sup`, `umbrella_root`, `phoenix_app`, and `phoenix_web_child`.
- `project_name`: Required. This is the generated directory name for the standalone project or child app. Use lowercase `snake_case`.
- `workspace_dir`: Optional. This is the parent directory where generated projects are created. If it is not provided, use `~/code/elixir-projects`.
- `otp_app`: Optional. Use this for `mix_app`, `mix_app_sup`, `phoenix_app`, and `phoenix_web_child`. If it is not provided, use `{project_name}`.
- `base_module`: Optional. This is the base Elixir module name for the generated app. If it is not provided, use the CamelCase form of `{project_name}`.
- `phoenix_database`: Optional. Use this only when `project_shape` is `phoenix_app`. Allowed values are `sqlite3` and `postgres`. If it is not provided, use `sqlite3`.
- `umbrella_root_name`: Required when `project_shape` is `phoenix_web_child`. This is the umbrella root directory name. Use lowercase `snake_case`.
- `umbrella_root_exists`: Optional. Use this only when `project_shape` is `phoenix_web_child`. Allowed values are `true` and `false`. If it is not provided, use `false`.
- `umbrella_root_module`: Optional. Use this only when `project_shape` is `phoenix_web_child` and `umbrella_root_exists` is `false`. If it is not provided, use the CamelCase form of `{umbrella_root_name}`.
- `erlang_version`: Optional. If it is not provided, use `25.3.2`.
- `elixir_version`: Optional. If it is not provided, use `1.15.2-otp-25`.
- `phoenix_generator_version`: Optional. Use this only when `project_shape` is `phoenix_app` or `phoenix_web_child`. If it is not provided, use `1.8.5`.
- `postgres_formula`: Optional. Use this only when `project_shape` is `phoenix_app` and `phoenix_database` is `postgres`. If it is not provided, use `postgresql@17`.

In the instructions below, any pattern written as `{<name>}` means the value provided for that input parameter. If an optional parameter was not provided, replace it with the default value defined in this section before you run any command.

Use these derived values in the instructions below:

- For `mix_app`, `mix_app_sup`, `umbrella_root`, and `phoenix_app`, `{project_root}` means `{workspace_dir}/{project_name}`.
- For `phoenix_web_child`, `{umbrella_root}` means `{workspace_dir}/{umbrella_root_name}`, `{umbrella_apps_dir}` means `{umbrella_root}/apps`, and `{project_root}` means `{umbrella_apps_dir}/{project_name}`.
- When a baseline section creates `coveralls.json` in an application root, the top-level module file path is `lib/{otp_app}.ex`.

---

## Supported Environment

This workflow supports macOS with `zsh` only.

This workflow uses the following documented default versions:

- Erlang `25.3.2`
- Elixir `1.15.2-otp-25`
- Phoenix generator `phx_new 1.8.5`
- PostgreSQL Homebrew formula `postgresql@17` for the standalone Phoenix PostgreSQL branch

These default Erlang and Elixir versions come from the checked-in `.tool-versions` file in this repository. Use them unless you intentionally override them with the version input parameters and keep the versions compatible.

This workflow uses the new `asdf set` command. Do not use `asdf local` or `asdf global`.

---

## Purpose / Big Picture

Use this workflow when you need one of the following outcomes:

- a standalone Mix project without a supervisor
- a standalone Mix project with a supervisor
- an umbrella root project
- a standalone Phoenix project
- a Phoenix web child inside an umbrella project

The workflow handles machine setup, toolchain installation, project generation, baseline project files, and first validation.

---

## Choose Parameter Values Before You Run Any Command

Choose the values once before you run any generator command.

Use lowercase `snake_case` for `project_name`, `otp_app`, and `umbrella_root_name`.

Use `CamelCase` for `base_module` and `umbrella_root_module`.

If you do not provide `base_module`, the default is the CamelCase form of `{project_name}`. If you do not provide `umbrella_root_module`, the default is the CamelCase form of `{umbrella_root_name}`.

Examples:

- if `{project_name}` is `billing_api`, the default `{base_module}` is `BillingApi`
- if `{umbrella_root_name}` is `platform_umbrella`, the default `{umbrella_root_module}` is `PlatformUmbrella`

Do not rename a generated project later. If the chosen name is wrong, delete the generated directory and run the generator again with the correct parameter values.

---

## Before Installing

Before every install step, check whether the exact required thing already exists.

If it already exists, skip the install and continue to the next step.

If it does not already exist, install only the missing thing.

For Erlang and Elixir, if the exact versions already exist, do not reinstall them. In that case you only need to write `.tool-versions` files in the parent workspace and in the generated project or app root with `asdf set`.

---

## 1. Install macOS Prerequisites

Start in any terminal window running `zsh`.

Check whether Xcode Command Line Tools are already installed:

    xcode-select -p

If the command prints a path, continue.

If the command fails, install the tools:

    xcode-select --install

Wait for the installer to finish before you continue.

Check whether Homebrew is already installed:

    brew --version

If the command prints a version, continue.

If the command fails, install Homebrew with the official install command:

    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

After Homebrew finishes, put it on your shell path. Run `uname -m` first:

    uname -m

If the command prints `arm64`, add Homebrew like this:

    echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
    eval "$(/opt/homebrew/bin/brew shellenv)"

If the command prints `x86_64`, add Homebrew like this:

    echo 'eval "$(/usr/local/bin/brew shellenv)"' >> ~/.zprofile
    eval "$(/usr/local/bin/brew shellenv)"

Install the system packages this workflow needs for `asdf`, `asdf-erlang`, and `asdf-elixir`, but only if each formula is missing:

    for formula in coreutils git bash unzip autoconf openssl wxwidgets libxslt fop; do
      brew list --versions "$formula" >/dev/null 2>&1 || brew install "$formula"
    done

Verify the basics before you continue:

    git --version
    unzip -v
    openssl version

---

## 2. Install asdf and Configure zsh

Install `asdf` only if the formula is missing:

    brew list --versions asdf >/dev/null 2>&1 || brew install asdf

Add the required asdf shims path to `~/.zshrc`:

    export PATH="${ASDF_DATA_DIR:-$HOME/.asdf}/shims:$PATH"

If that exact line is not already present in `~/.zshrc`, append it once:

    grep -Fqx 'export PATH="${ASDF_DATA_DIR:-$HOME/.asdf}/shims:$PATH"' ~/.zshrc || echo 'export PATH="${ASDF_DATA_DIR:-$HOME/.asdf}/shims:$PATH"' >> ~/.zshrc

Reload your shell:

    source ~/.zshrc

Verify that `asdf` is on your path:

    type -a asdf
    asdf version

---

## 3. Install Erlang and Elixir Through asdf

Install the official plugins only if each plugin is missing:

    asdf plugin list | grep -qx 'erlang' || asdf plugin add erlang https://github.com/asdf-vm/asdf-erlang.git
    asdf plugin list | grep -qx 'elixir' || asdf plugin add elixir https://github.com/asdf-vm/asdf-elixir.git

Create the parent workspace if it does not already exist and move into it:

    mkdir -p "{workspace_dir}"
    cd "{workspace_dir}"

Write the requested toolchain into the parent directory with the new `asdf set` command:

    asdf set erlang "{erlang_version}"
    asdf set elixir "{elixir_version}"

Install only the exact versions that are missing:

    asdf where erlang "{erlang_version}" >/dev/null 2>&1 || asdf install erlang "{erlang_version}"
    asdf where elixir "{elixir_version}" >/dev/null 2>&1 || asdf install elixir "{elixir_version}"

If Erlang fails to compile with an OpenSSL error, retry only the missing Erlang install with the macOS OpenSSL path:

    export KERL_CONFIGURE_OPTIONS="--without-javac --with-ssl=$(brew --prefix openssl)"
    asdf where erlang "{erlang_version}" >/dev/null 2>&1 || asdf install erlang "{erlang_version}"
    unset KERL_CONFIGURE_OPTIONS

The `asdf-erlang` guide shows `openssl@1.1` for older Erlang releases and says Erlang `25.1` and newer support OpenSSL `3.0`. This workflow defaults to Erlang `25.3.2`, so `brew --prefix openssl` is the correct retry path for the default version.

Bootstrap Hex and Rebar only if they are missing for the active Elixir installation:

    mix local.hex --if-missing --force
    mix local.rebar --if-missing --force

Verify the toolchain before you generate a project:

    asdf current
    elixir --version
    mix --version

Expected result:

- `asdf current` shows `erlang {erlang_version}` and `elixir {elixir_version}`
- `elixir --version` reports the requested Elixir version
- `mix --version` succeeds without prompting you to install Hex or Rebar

If `asdf where erlang "{erlang_version}"` and `asdf where elixir "{elixir_version}"` both succeeded before the install step, you already had the exact requested versions installed and no additional Erlang or Elixir installation was needed.

---

## 4. Choose Exactly One Project Shape

Run the generator command from `{workspace_dir}` unless the branch below tells you to change directories first.

Choose one branch only.

Use this decision table to confirm the exact command and stopping point before you run anything:

### `project_shape` options

Use `project_shape` to choose the kind of project to generate.

#### `mix_app`

Creates a standard Mix application.

- **Run from:** `{workspace_dir}`
- **Command:** `mix new {project_name} --app {otp_app} --module {base_module}`
- **Creates:** `{project_root}`
- **Next:** `Single-Project Baseline`

#### `mix_app_sup`

Creates a standard Mix application with a supervisor.

- **Run from:** `{workspace_dir}`
- **Command:** `mix new {project_name} --app {otp_app} --module {base_module} --sup`
- **Creates:** `{project_root}`
- **Next:** `Single-Project Baseline`

#### `umbrella_root`

Creates the root of an umbrella project.

- **Run from:** `{workspace_dir}`
- **Command:** `mix new {project_name} --module {base_module} --umbrella`
- **Creates:** `{project_root}`
- **Next:** `Umbrella Root Baseline`

#### `phoenix_app`

Creates a standalone Phoenix application.

- **Run from:** `{workspace_dir}`
- **Command:** `mix phx.new {project_name} --module {base_module} --app {otp_app} --database {phoenix_database} --no-install`
- **Creates:** `{project_root}`
- **Next:** `Phoenix Standalone Baseline`

#### `phoenix_web_child`

Creates a Phoenix web child app inside an umbrella project's `apps` directory.

- **Run from:** `{umbrella_apps_dir}`
- **Command:** `mix phx.new.web {project_name} --module {base_module} --app {otp_app} --no-install`
- **Creates:** `{project_root}`
- **Next:** `Umbrella Root Baseline`, then `Phoenix Web Child Baseline`

#### Branch A: `mix_app`

Run:

    mix new "{project_name}" --app "{otp_app}" --module "{base_module}"

Move into the generated project and write the requested toolchain into the project root:

    cd "{project_root}"
    asdf set erlang "{erlang_version}"
    asdf set elixir "{elixir_version}"

Continue with `Single-Project Baseline`.

#### Branch B: `mix_app_sup`

Run:

    mix new "{project_name}" --app "{otp_app}" --module "{base_module}" --sup

Move into the generated project and write the requested toolchain into the project root:

    cd "{project_root}"
    asdf set erlang "{erlang_version}"
    asdf set elixir "{elixir_version}"

Continue with `Single-Project Baseline`.

#### Branch C: `umbrella_root`

Run:

    mix new "{project_name}" --module "{base_module}" --umbrella

Move into the generated umbrella root and write the requested toolchain there:

    cd "{project_root}"
    asdf set erlang "{erlang_version}"
    asdf set elixir "{elixir_version}"

Continue with `Umbrella Root Baseline`.

#### Branch D: `phoenix_app`

Install the requested Phoenix generator archive only if that exact version is missing:

    mix archive | grep -Fq "phx_new-{phoenix_generator_version}" || mix archive.install hex phx_new "{phoenix_generator_version}" --force

If `{phoenix_database}` is `postgres`, install and start PostgreSQL only when needed:

    brew list --versions "{postgres_formula}" >/dev/null 2>&1 || brew install "{postgres_formula}"
    pg_isready >/dev/null 2>&1 || brew services start "{postgres_formula}"
    pg_isready

Run:

    mix phx.new "{project_name}" --module "{base_module}" --app "{otp_app}" --database "{phoenix_database}" --no-install

Move into the generated project and write the requested toolchain into the project root:

    cd "{project_root}"
    asdf set erlang "{erlang_version}"
    asdf set elixir "{elixir_version}"

Continue with `Phoenix Standalone Baseline`.

#### Branch E: `phoenix_web_child`

`mix phx.new.web` creates a bare Phoenix web project inside an umbrella project's `apps/` directory. It does not generate database integration. Do not add a `--database` flag to this branch.

If `{umbrella_root_exists}` is `false`, create the umbrella root first:

    cd "{workspace_dir}"
    mix new "{umbrella_root_name}" --module "{umbrella_root_module}" --umbrella
    cd "{umbrella_root}"
    asdf set erlang "{erlang_version}"
    asdf set elixir "{elixir_version}"

If `{umbrella_root_exists}` is `true`, require the umbrella root to already exist:

    [ -d "{umbrella_root}" ] || { echo "Missing umbrella root: {umbrella_root}" >&2; exit 1; }
    cd "{umbrella_root}"
    asdf set erlang "{erlang_version}"
    asdf set elixir "{elixir_version}"

Install the requested Phoenix generator archive only if that exact version is missing:

    mix archive | grep -Fq "phx_new-{phoenix_generator_version}" || mix archive.install hex phx_new "{phoenix_generator_version}" --force

Move into the umbrella `apps/` directory and generate the web child:

    cd "{umbrella_apps_dir}"
    mix phx.new.web "{project_name}" --module "{base_module}" --app "{otp_app}" --no-install

Return to the umbrella root:

    cd "{umbrella_root}"

Continue with `Umbrella Root Baseline` and then `Phoenix Web Child Baseline`.

---

## 5. Single-Project Baseline

Use this section for `mix_app` and `mix_app_sup`.

Apply these changes in `{project_root}`.

### `mix.exs`

Keep the generated `app`, `version`, `elixir`, and `application` values.

In `project/0`, ensure these keys exist:

    elixirc_paths: elixirc_paths(Mix.env()),
    test_coverage: [tool: ExCoveralls],
    preferred_cli_env: [
      coveralls: :test,
      coverage: :test,
      dialyzer: :test,
      "coveralls.cobertura": :test,
      "coveralls.detail": :test,
      "coveralls.html": :test,
      "coveralls.json": :test,
      "coveralls.lcov": :test,
      "coveralls.post": :test
    ],
    dialyzer: [
      plt_add_apps: [:ex_unit, :mix],
      plt_local_path: "dialyzer",
      plt_core_path: "dialyzer",
      plt_ignore_apps: [],
      list_unused_filters: true,
      ignore_warnings: ".dialyzer-ignore.exs",
      flags: [:unmatched_returns, :no_improper_lists]
    ]

Add these functions if they do not already exist:

    defp elixirc_paths(:test), do: ["lib", "test/support"]
    defp elixirc_paths(_), do: ["lib"]

Add these dependencies to `deps/0` if they are missing:

    {:ex_doc, "~> 0.40.1"},
    {:credo, "~> 1.4", runtime: false},
    {:blitz_credo_checks, "~> 0.1.10", runtime: false},
    {:dialyxir, "~> 1.4", runtime: false},
    {:excoveralls, "~> 0.13", only: :test},
    {:rexbug, "~> 1.0"},
    {:observer_cli, "~> 1.8"},
    {:etop, "~> 0.7"}

Do not remove any dependency the generator already added.

### `.formatter.exs`

Replace the file with:

    [
      import_deps: [],
      line_length: 110,
      inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
    ]

### `.dialyzer-ignore.exs`

Replace the file with:

    []

### `.credo.exs`

If the project does not already have a `.credo.exs` file, create it with this content.

If the project already has a `.credo.exs` file, replace it with this content so the baseline is deterministic:

    allowed_imports = [
      [:ChannelCase],
      [:ConnCase],
      [:DataCase],
      [:Ecto],
      [:ExUnit],
      [:ExUnit, :CaptureLog],
      [:Mix],
      [:Phoenix],
      [:Phoenix, :Controller],
      [:Phoenix, :LiveView, :Router],
      [:Plug],
      [:Swoosh, :TestAssertions],
      [:Telemetry, :Metrics]
    ]

    %{
      configs: [
        %{
          name: "default",
          files: %{
            included: [
              "lib/",
              "src/",
              "test/",
              "web/"
            ],
            excluded: [~r"_build/", ~r"deps/"]
          },
          plugins: [],
          requires: ["deps/blitz_credo/lib/blitz_credo/"],
          strict: true,
          parse_timeout: 10000,
          color: true,
          checks: [
            {BlitzCredoChecks.SetWarningsAsErrorsInTest, false},
            {BlitzCredoChecks.DocsBeforeSpecs, []},
            {BlitzCredoChecks.DoctestIndent, []},
            {BlitzCredoChecks.NoAsyncFalse, []},
            {BlitzCredoChecks.NoDSLParentheses, []},
            {BlitzCredoChecks.NoIsBitstring, []},
            {BlitzCredoChecks.StrictComparison, []},
            {BlitzCredoChecks.LowercaseTestNames, []},
            {BlitzCredoChecks.ImproperImport, allowed_modules: allowed_imports},
            {Credo.Check.Consistency.ExceptionNames, []},
            {Credo.Check.Consistency.LineEndings, []},
            {Credo.Check.Consistency.ParameterPatternMatching, []},
            {Credo.Check.Consistency.SpaceAroundOperators, []},
            {Credo.Check.Consistency.SpaceInParentheses, []},
            {Credo.Check.Consistency.TabsOrSpaces, []},
            {Credo.Check.Design.AliasUsage, [if_nested_deeper_than: 0, if_called_more_often_than: 0]},
            {Credo.Check.Design.TagTODO, []},
            {Credo.Check.Design.TagFIXME, []},
            {Credo.Check.Readability.AliasOrder, false},
            {Credo.Check.Readability.FunctionNames, []},
            {Credo.Check.Readability.LargeNumbers, []},
            {Credo.Check.Readability.MaxLineLength, [max_length: 120]},
            {Credo.Check.Readability.ModuleAttributeNames, []},
            {Credo.Check.Readability.ModuleDoc, false},
            {Credo.Check.Readability.ModuleNames, []},
            {Credo.Check.Readability.NestedFunctionCalls, []},
            {Credo.Check.Readability.ParenthesesInCondition, []},
            {Credo.Check.Readability.ParenthesesOnZeroArityDefs, []},
            {Credo.Check.Readability.PredicateFunctionNames, []},
            {Credo.Check.Readability.PreferImplicitTry, []},
            {Credo.Check.Readability.RedundantBlankLines, false},
            {Credo.Check.Readability.Semicolons, []},
            {Credo.Check.Readability.SpaceAfterCommas, false},
            {Credo.Check.Readability.StringSigils, []},
            {Credo.Check.Readability.TrailingBlankLine, false},
            {Credo.Check.Readability.TrailingWhiteSpace, false},
            {Credo.Check.Readability.UnnecessaryAliasExpansion, []},
            {Credo.Check.Readability.VariableNames, []},
            {Credo.Check.Refactor.CondStatements, []},
            {Credo.Check.Refactor.CyclomaticComplexity, false},
            {Credo.Check.Refactor.FunctionArity, []},
            {Credo.Check.Refactor.LongQuoteBlocks, false},
            {Credo.Check.Refactor.MapInto, false},
            {Credo.Check.Refactor.MatchInCondition, []},
            {Credo.Check.Refactor.NegatedConditionsInUnless, []},
            {Credo.Check.Refactor.NegatedConditionsWithElse, []},
            {Credo.Check.Refactor.Nesting, false},
            {Credo.Check.Refactor.UnlessWithElse, []},
            {Credo.Check.Refactor.WithClauses, []},
            {Credo.Check.Warning.BoolOperationOnSameValues, []},
            {Credo.Check.Warning.ExpensiveEmptyEnumCheck, []},
            {Credo.Check.Warning.IExPry, []},
            {Credo.Check.Warning.IoInspect, []},
            {Credo.Check.Warning.LazyLogging, false},
            {Credo.Check.Warning.MixEnv, false},
            {Credo.Check.Warning.OperationOnSameValues, []},
            {Credo.Check.Warning.OperationWithConstantResult, []},
            {Credo.Check.Warning.RaiseInsideRescue, []},
            {Credo.Check.Warning.UnusedEnumOperation, []},
            {Credo.Check.Warning.UnusedFileOperation, []},
            {Credo.Check.Warning.UnusedKeywordOperation, []},
            {Credo.Check.Warning.UnusedListOperation, []},
            {Credo.Check.Warning.UnusedPathOperation, []},
            {Credo.Check.Warning.UnusedRegexOperation, []},
            {Credo.Check.Warning.UnusedStringOperation, []},
            {Credo.Check.Warning.UnusedTupleOperation, []},
            {Credo.Check.Warning.UnsafeExec, []},
            {Credo.Check.Readability.StrictModuleLayout, false},
            {Credo.Check.Consistency.MultiAliasImportRequireUse, false},
            {Credo.Check.Consistency.UnusedVariableNames, false},
            {Credo.Check.Design.DuplicatedCode, false},
            {Credo.Check.Readability.AliasAs, false},
            {Credo.Check.Readability.MultiAlias, false},
            {Credo.Check.Readability.Specs, false},
            {Credo.Check.Readability.SinglePipe, []},
            {Credo.Check.Readability.WithCustomTaggedTuple, []},
            {Credo.Check.Refactor.ABCSize, false},
            {Credo.Check.Refactor.AppendSingleItem, false},
            {Credo.Check.Refactor.DoubleBooleanNegation, false},
            {Credo.Check.Refactor.ModuleDependencies, false},
            {Credo.Check.Refactor.NegatedIsNil, false},
            {Credo.Check.Refactor.PipeChainStart, []},
            {Credo.Check.Refactor.VariableRebinding, false},
            {Credo.Check.Warning.LeakyEnvironment, false},
            {Credo.Check.Warning.MapGetUnsafePass, false},
            {Credo.Check.Warning.UnsafeToAtom, false}
          ]
        }
      ]
    }

### `coveralls.json`

Create `coveralls.json` at the project root with this content:

    {
      "terminal_options": {
        "file_column_width": 75
      },
      "coverage_options": {
        "html_filter_full_covered": true,
        "treat_no_relevant_lines_as_covered": true,
        "minimum_coverage": 0
      },
      "custom_stop_words": [
        "defdelegate"
      ],
      "skip_files": [
        "lib/{otp_app}.ex",
        "test/support/*"
      ]
    }

---

## 6. Phoenix Standalone Baseline

Use this section for `phoenix_app`.

Apply every instruction from `Single-Project Baseline` with these changes in `{project_root}`:

### `mix.exs`

Keep the Phoenix dependencies the generator already added.

Add the same baseline keys and the same extra dependencies from `Single-Project Baseline`.

### `.formatter.exs`

Replace the file with:

    [
      import_deps: [:ecto, :phoenix],
      line_length: 110,
      inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
    ]

### `coveralls.json`

Create `coveralls.json` at the project root with this content:

    {
      "terminal_options": {
        "file_column_width": 75
      },
      "coverage_options": {
        "html_filter_full_covered": true,
        "treat_no_relevant_lines_as_covered": true,
        "minimum_coverage": 0
      },
      "custom_stop_words": [
        "defdelegate"
      ],
      "skip_files": [
        "lib/{otp_app}.ex",
        "test/support/*"
      ]
    }

---

## 7. Umbrella Root Baseline

Use this section for `umbrella_root` and `phoenix_web_child`.

Apply these changes in the umbrella root. For `umbrella_root`, that location is `{project_root}`. For `phoenix_web_child`, that location is `{umbrella_root}`.

### Root `mix.exs`

Do not add `elixirc_paths` to the umbrella root.

In `project/0`, ensure these keys exist:

    test_coverage: [tool: ExCoveralls],
    preferred_cli_env: [
      coveralls: :test,
      coverage: :test,
      dialyzer: :test,
      "coveralls.cobertura": :test,
      "coveralls.detail": :test,
      "coveralls.html": :test,
      "coveralls.json": :test,
      "coveralls.lcov": :test,
      "coveralls.post": :test
    ],
    dialyzer: [
      plt_add_apps: [:ex_unit, :mix],
      plt_local_path: "dialyzer",
      plt_core_path: "dialyzer",
      plt_ignore_apps: [],
      list_unused_filters: true,
      ignore_warnings: ".dialyzer-ignore.exs",
      flags: [:unmatched_returns, :no_improper_lists]
    ]

Add these dependencies to `deps/0` if they are missing:

    {:ex_doc, "~> 0.40.1"},
    {:credo, "~> 1.4", runtime: false},
    {:blitz_credo_checks, "~> 0.1.10", runtime: false},
    {:dialyxir, "~> 1.4", runtime: false},
    {:excoveralls, "~> 0.13", only: :test},
    {:rexbug, "~> 1.0"},
    {:observer_cli, "~> 1.8"},
    {:etop, "~> 0.7"}

### Root `.formatter.exs`

Replace the file with:

    [
      subdirectories: ["apps/*"],
      inputs: ["{mix,.formatter}.exs"]
    ]

### Root `.dialyzer-ignore.exs`

Replace the file with:

    []

### Root `.credo.exs`

Create or replace the file with the same content from `Single-Project Baseline`, but change the `included` list to this exact value:

    included: [
      "lib/",
      "src/",
      "test/",
      "web/",
      "apps/*/lib/",
      "apps/*/src/",
      "apps/*/test/",
      "apps/*/web/"
    ]

Keep the rest of the Credo file identical to the `Single-Project Baseline`.

### Root `coveralls.json`

Create `coveralls.json` at the umbrella root with this content:

    {
      "terminal_options": {
        "file_column_width": 75
      },
      "coverage_options": {
        "html_filter_full_covered": true,
        "treat_no_relevant_lines_as_covered": true,
        "minimum_coverage": 0
      },
      "custom_stop_words": [
        "defdelegate"
      ],
      "skip_files": [
        "test/support/*"
      ]
    }

---

## 8. Phoenix Web Child Baseline

Use this section only for `phoenix_web_child`.

Apply these changes in `{project_root}`.

### Child `mix.exs`

Keep the generated Phoenix web dependencies.

Add these dependencies to `deps/0` if they are missing:

    {:ex_doc, "~> 0.40.1"},
    {:rexbug, "~> 1.0"},
    {:observer_cli, "~> 1.8"},
    {:etop, "~> 0.7"}

### Child `.formatter.exs`

Create or replace the file with:

    [
      import_deps: [:phoenix],
      line_length: 110,
      inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
    ]

### Child `coveralls.json`

Create `coveralls.json` in `{project_root}` with this content:

    {
      "terminal_options": {
        "file_column_width": 75
      },
      "coverage_options": {
        "html_filter_full_covered": true,
        "treat_no_relevant_lines_as_covered": true,
        "minimum_coverage": 0
      },
      "custom_stop_words": [
        "defdelegate"
      ],
      "skip_files": [
        "lib/{otp_app}.ex",
        "test/support/*"
      ]
    }

---

## 9. Fetch Dependencies and Run First Validation

Run these commands only after you finish the baseline changes for the branch you chose.

### `mix_app` and `mix_app_sup` Validation

Run from `{project_root}`:

    mix deps.get
    mix deps.compile
    mix format
    mix test
    mix credo --strict

Run `mix dialyzer` after the project compiles cleanly and the first test run passes.

Expected result:

- `mix deps.get` finishes without missing Hex or Rebar prompts
- `mix test` succeeds
- `mix credo --strict` succeeds

### `umbrella_root` Validation

Run from `{project_root}`:

    mix deps.get
    mix deps.compile
    mix format
    mix test
    mix credo --strict

Run `mix dialyzer` after the first compile and test pass succeeds.

### `phoenix_app` Validation

If `{phoenix_database}` is `postgres`, confirm PostgreSQL is accepting connections before validation:

    pg_isready

Then run from `{project_root}`:

    mix deps.get
    mix deps.compile
    mix ecto.create
    mix format
    mix test
    mix credo --strict

Run `mix dialyzer` after the first compile and test pass succeeds.

### `phoenix_web_child` Validation

Run from `{umbrella_root}`:

    mix deps.get
    mix deps.compile
    mix format
    mix test
    mix credo --strict

Run `mix dialyzer` after the first compile and test pass succeeds.

---

## 10. Restart and Failure Rules

If you are still in the parent directory and generated the wrong project shape, delete the generated directory under `{workspace_dir}` and run the correct generator command with the correct parameter values.

If `asdf current` shows the wrong versions inside a project root, rerun:

    asdf set erlang "{erlang_version}"
    asdf set elixir "{elixir_version}"

Then verify again:

    asdf current

If `mix` cannot find Hex or Rebar later, rerun:

    mix local.hex --if-missing --force
    mix local.rebar --if-missing --force

If `mix deps.get` fails because the generated files were edited incorrectly, fix the first syntax error that `mix` reports and rerun the same command.

If `project_shape` is `phoenix_web_child`, do not try to force database flags into `mix phx.new.web`. The official Phoenix task is intentionally web-only and does not generate database integration in the umbrella web child.

If the exact Erlang and Elixir versions already existed before Section `3`, do not reinstall them. In that case the only required toolchain write step is `asdf set` in the parent workspace and then `asdf set` again in the generated project or app root so the correct `.tool-versions` files exist.

---

## 11. References Used for This Workflow

This workflow is based on these primary sources:

- Elixir introduction: `https://hexdocs.pm/elixir/introduction.html`
- Elixir install guide: `https://elixir-lang.org/install.html`
- asdf getting started guide: `https://asdf-vm.com/guide/getting-started.html`
- asdf-erlang plugin README: `https://github.com/asdf-vm/asdf-erlang`
- asdf-elixir plugin README: `https://github.com/asdf-vm/asdf-elixir`
- Phoenix `mix phx.new` docs: `https://hexdocs.pm/phoenix/Mix.Tasks.Phx.New.html`
- Phoenix `mix phx.new.web` docs: `https://hexdocs.pm/phoenix/Mix.Tasks.Phx.New.Web.html`
- `phx_new` package page: `https://hex.pm/packages/phx_new`
- Homebrew: `https://brew.sh/`
