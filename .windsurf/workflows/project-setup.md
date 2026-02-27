---
auto_execution_mode: 3
description: Run to setup a project.
---

## What to do

### mix.exs

1. Add the following dependencies to your `mix.exs`. If a dependency already exists, skip it.

```elixir
defp deps do
  [
    {:credo, "~> 1.4", runtime: false},
    {:blitz_credo_checks, "~> 0.1.5", runtime: false},
    {:dialyxir, "~> 1.4", runtime: false},
    {:excoveralls, "~> 0.13", only: :test},
    {:rexbug, "~> 1.0"},
    {:observer_cli, "~> 1.8"},
    {:etop, "~> 0.7"}
  ]
end
```

2. Add the following to your `mix.exs` file:

```elixir
def project do
  [
    # ... other project config
    elixirc_paths: elixirc_paths(Mix.env()),
    # ... other project config
  ]
end
```

```elixir
defp elixirc_paths(:test), do: ["lib", "test/support"]
defp elixirc_paths(_), do: ["lib"]
```

3. Set Excoveralls as the test coverage tool and preferred CLI environment in your `mix.exs` file:

```elixir
def project do
  [
    # ... other project config
    test_coverage: [tool: ExCoveralls],
    preferred_cli_env: [
      coveralls: :test,
      "coveralls.detail": :test,
      "coveralls.post": :test,
      "coveralls.html": :test,
      "coveralls.cobertura": :test
    ],
    # ... other project config
  ]
end
```

### Formatter

1. Create a `.formatter.exs` file at the root of the project or umbrella project if one does not already exist.

2. Ensure the file follows the following guidelines:

The file should look like this:

```elixir
[
  import_deps: [:absinthe, :ecto, :phoenix],
  line_length: 110,
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
]
```

- The inputs must include `{mix,.formatter}.exs` and `{config,lib,test}/**/*.{ex,exs}`
- The `import_deps` must include the dependencies used in the project.
- The `line_length` must be set to `110`.

### Credo

1. Create a `.credo.exs` file at the root of the application or umbrella project if one does not already exist.

2. Use `.agent/templates/.credo.exs.tmpl` as a reference for the content of the file. If it's a new project, you can copy the entire content of the template file to the `.credo.exs` file. If the project already has a `.credo.exs` file, you can update it add any missing checks from the template file. Do not remove any checks from the existing file.