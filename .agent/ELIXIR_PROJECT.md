## How to Create a New Elixir Application

- Use this command to create a new elixir application:

    mix new my_app

- Use this command to create a new elixir application with an application supervisor:

    mix new my_app --sup

- Use this command to create a new elixir umbrella application:

    mix new my_umbrella --umbrella

- Use this command to create a new phoenix web application outside of an umbrella application:

    mix phx.new my_app_web --module MyAppWeb --app my_app_web

- Use this command to create a new phoenix web application in an elixir umbrella application:

    mix phx.new.web my_app_web --module MyAppWeb --app my_app_web

Run this command from the `apps/` directory.

## Directory Layout

Example layout of an elixir application:

    MyApp/
      .credo.exs
      .dialyzer-ignore.exs
      .formatter.exs
      mix.exs
      README.md
      config/
        config.exs
        ...
      lib/
        my_app.ex
        my_app/
          application.ex
          ...
      priv/
        ...
      test/
        my_app/
          ...
        test_helper.exs

Example layout of an elixir umbrella project:

    MyUmbrella/
      .credo.exs
      .dialyzer-ignore.exs
      .formatter.exs
      mix.exs
      README.md
      config/
        config.exs
        ...
      apps/
        my_app/
          mix.exs
          lib/
            my_app.ex
            my_app/
              application.ex
              ...
          priv/
            ...
          test/
            my_app/
              ...
            test_helper.exs

        my_app_web/
          mix.exs
          lib/
            my_app_web.ex
            my_app_web/
              endpoint.ex
              router.ex
              controllers/
              live/
              components/
              views/
              templates/
          priv/
            static/
              assets/
          test/
            my_app_web/
              ...
            test_helper.exs

## mix.exs

### Dependencies

Do not remove any dependencies that already exist.

Only add dependencies that do not already exist.

The following dependencies are required:

- `{:excoveralls, "~> 0.13", only: :test}`
- `{:rexbug, "~> 1.0"}`
- `{:observer_cli, "~> 1.8"}`
- `{:etop, "~> 0.7"}`

Add the dependencies to the `mix.exs` file:

    # mix.exs
    defp deps do
      [
        {:excoveralls, "~> 0.13", only: :test},
        {:rexbug, "~> 1.0"},
        {:observer_cli, "~> 1.8"},
        {:etop, "~> 0.7"}
      ]
    end

### elixirc_paths

Use `elixirc_paths` to allow files in `test/support` to be compiled for the specified environment.

Add the following to the `mix.exs` file:

    # mix.exs
    def project do
      [
        # ...
        elixirc_paths: elixirc_paths(Mix.env())
        # ...
      ]
    end

    defp elixirc_paths(:test), do: ["lib", "test/support"]
    defp elixirc_paths(_), do: ["lib"]

### Dialyzer

Use the following dialyzer configuration in the `mix.exs` file:

    # mix.exs
    def project do
      [
        # ...
        dialyzer: [
          plt_add_apps: [:ex_unit, :mix],
          plt_local_path: "dialyzer",
          plt_core_path: "dialyzer",
          plt_ignore_apps: [],
          list_unused_filters: true,
          ignore_warnings: ".dialyzer-ignore.exs",
          flags: [:unmatched_returns, :no_improper_lists]
        ]
        # ...
      ]
    end

Use `dialyxir` as the dependency for `dialyzer`.
The `dialyxir` dependency must set `runtime: false`.

    defp deps do
      [
        # ...
        {:dialyxir, "~> 1.4", runtime: false}
        # ...
      ]
    end

Use the `.dialyzer-ignore.exs` file to ignore any dialyzer warnings.

Add the following to the `.dialyzer-ignore.exs` file:
  
    # .dialyzer-ignore.exs
    []

### .formatter.exs

All applications must have a `.formatter.exs` file. It is required to run `mix format`.

#### Skeleton of a Good .formatter.exs file

    # .formatter.exs
    [
      # Replace `import_deps` with the dependencies used in the application.
      import_deps: [:absinthe, :ecto, :phoenix],
      line_length: 110,
      inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
    ]

### Credo

Use `credo` for code linting to ensure code is written consistently in the project.

Do not remove any checks from the existing `.credo.exs` file.

Use the skeleton of a good `.credo.exs` file as a reference for the content of the file. If it is a new project, copy the entire content of the skeleton into `.credo.exs`. If the project already has a `.credo.exs`, add any missing checks from the skeleton.

#### Dependencies

The following dependencies are required for `credo`:

    # mix.exs
    defp deps do
      [
        {:credo, "~> 1.4", runtime: false},
        {:blitz_credo_checks, "~> 0.1.5", runtime: false}
      ]
    end

#### Skeleton of a Good .credo.exs file

    # .credo.exs
    allowed_imports = [
      [:Absinthe],
      [:ChannelCase],
      [:ConnCase],
      [:DataCase],
      [:EctoEnum],
      [:Ecto],
      [:ExUnit, :CaptureLog],
      [:ExUnit],
      [:Mix],
      [:ErrorHelpers],
      [:Phoenix],
      [:Phoenix, :Controller],
      [:Phoenix, :LiveView, :Router],
      [:Plug],
      [:Router, :Helpers],
      [:Swoosh, :TestAssertions],
      [:Telemetry, :Metrics],
      [:SharedUtils, :Support, :HTTPSandbox],
      [:LearnElixirLanderWeb, :Gettext],
      [:LearnElixirLanderWeb, :CoreComponents],
      [:LearnElixirLanderWeb, :AlpineComponents],
      [:TeachingPlatformWeb, :Gettext],
      [:TeachingPlatformWeb, :CoreComponents],
      [:TeachingPlatformWeb, :DisplayComponents],
      [:TeachingPlatformWeb, :InteractiveComponents],
      [:TeachingPlatformWeb, :AlpineComponents],
      [:TeachingPlatformWeb, :FormComponents]
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
              "web/",
              "apps/*/lib/",
              "apps/*/src/",
              "apps/*/test/",
              "apps/*/web/"
            ],
            excluded: [~r"_build/", ~r"deps/"]
          },
          plugins: [],
          requires: ["deps/blitz_credo/lib/blitz_credo/"],
          strict: true,
          parse_timeout: 10000,
          color: true,
          checks: [

            # BlitzCredoChecks

            {BlitzCredoChecks.SetWarningsAsErrorsInTest, false},
            {BlitzCredoChecks.DocsBeforeSpecs, []},
            {BlitzCredoChecks.DoctestIndent, []},
            {BlitzCredoChecks.NoAsyncFalse, []},
            {BlitzCredoChecks.NoDSLParentheses, []},
            {BlitzCredoChecks.NoIsBitstring, []},
            {BlitzCredoChecks.StrictComparison, []},
            {BlitzCredoChecks.LowercaseTestNames, []},
            {BlitzCredoChecks.ImproperImport, allowed_modules: allowed_imports},

            # Consistency Checks
            {Credo.Check.Consistency.ExceptionNames, []},
            {Credo.Check.Consistency.LineEndings, []},
            {Credo.Check.Consistency.ParameterPatternMatching, []},
            {Credo.Check.Consistency.SpaceAroundOperators, []},
            {Credo.Check.Consistency.SpaceInParentheses, []},
            {Credo.Check.Consistency.TabsOrSpaces, []},

            # Design Checks
            {Credo.Check.Design.AliasUsage,
            [
              if_nested_deeper_than: 0,
              if_called_more_often_than: 0
            ]},

            # No outstanding TODOs
            {Credo.Check.Design.TagTODO, []},
            {Credo.Check.Design.TagFIXME, []},

            # # Readability Checks
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
            #
            # Refactoring Opportunities
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

            # Warnings
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

            # Controversial and experimental checks
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

### ExCoveralls

Every application must have a `coveralls.json` file at its root. In an umbrella project, place a `coveralls.json` at both the umbrella root and the root of each child application.

#### Skeleton of a Good coveralls.json file

    // coveralls.json
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
        "lib/ecto_shorts.ex",
        "test/support/*"
      ]
    }

Adjust `skip_files` to match the application. Typically skip the top-level application module (e.g. `lib/my_app.ex`) and test support files (`test/support/*`).