# Do Not Put Configuration in Runtime Branching Code

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Requirements

- Put environment-specific values in `config/*.exs`.
In application code, read a single config value and provide a safe default.

- Do not branch inside the functions of a config module (e.g. `MyApp.Config`).

- The function must have the same name as the :key it is retrieving from the application environment.

- Application code must describe behaviour. Configuration files must describe environment differences.

- Keep the code easy to read at a glance. The function must only return the configured value or a set default.

- Avoids hidden behaviour. The user must be able to look in `config/` and see what changes per environment.

## What to do

1. Choose a single config key for the value (example: `:api_base_url`).
2. Set that key in `config/config.exs` and override it in environment configs (or with a `Mix.env()` conditional if you must).
3. In code, read the key and optionally fall back to a default.

## Examples

Don’t do this (runtime branching to pick a config value):

    # BAD EXAMPLE, DO NOT COPY!
    defmodule MyApp.Config do
      @spec api_base_url() :: String.t()
      def api_base_url do
        sandbox_enabled = Application.get_env(:my_app, :sandbox_enabled, true)

        case sandbox_enabled do
          true -> "https://example.sandbox.com"
          false -> "https://example.com"
        end
      end
    end

Do this (read a single config key, with a default):

    # GOOD EXAMPLE, COPY THIS
    defmodule MyApp.Config do
      @moduledoc false

      # The `@app :my_app` module attribute prevents
      # repeating the app name everywhere.
      @app :my_app

      @spec api_base_url() :: String.t()
      def api_base_url do
        Application.get_env(@app, :api_base_url) || "https://example.sandbox.com"
      end

      # some configuration values can also be nil when
      # you don't know the default value ahead of time.
      @spec can_be_nil :: String.t() | nil
      def can_be_nil do
        Application.get_env(@app, :can_be_nil)
      end
    end
