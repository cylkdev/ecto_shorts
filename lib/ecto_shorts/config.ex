defmodule EctoShorts.Config do
  @moduledoc """
  Provides helper functions for reading the EctoShorts configuration from
  the application environment.
  """

  @app :ecto_shorts

  @doc since: "3.0.0"
  @doc """
  Returns the configured `:error_module` value from the `:ecto_shorts` application environment.

  Defaults to `EctoShorts.Actions.Error` when not set.

  ## Examples

      iex> EctoShorts.Config.error_module()
      EctoShorts.Actions.Error

  See also `EctoShorts.Actions.Error` and `repo!/1`.
  """
  @spec error_module :: module()
  def error_module do
    Application.get_env(@app, :error_module) || EctoShorts.Actions.Error
  end

  @doc """
  Returns the configured `:repo` value from the `:ecto_shorts` application environment.

  Defaults to `nil` if not set.

  ## Examples

      iex> EctoShorts.Config.repo()
      EctoShorts.Repo

  See also `repo!/1` and `replica/0`.
  """
  @spec repo :: module() | nil
  def repo do
    Application.get_env(@app, :repo)
  end

  @doc since: "3.0.0"
  @doc """
  Returns the configured `:replica` value from the `:ecto_shorts` application environment.

  Defaults to `nil` if not set.

  ## Examples

      iex> EctoShorts.Config.replica()
      nil

  See also `replica!/1` and `repo/0`.
  """
  @spec replica :: module() | nil
  def replica do
    Application.get_env(@app, :replica)
  end

  @doc since: "3.0.0"
  @doc """
  Returns the `Ecto.Repo` module to use.

  Looks for the `:repo` option first, falling back to the configured value in the
  `:ecto_shorts` application environment.

  Raises if no repo is found.

  ## Examples

      iex> EctoShorts.Config.repo!()
      EctoShorts.Repo

      iex> EctoShorts.Config.repo!(repo: MyApp.Repo)
      MyApp.Repo
  """
  @spec repo!(opts :: keyword()) :: module()
  @spec repo! :: module()
  def repo!(opts \\ []) do
    with nil <- Keyword.get(opts, :repo, repo()) do
      raise """
      EctoShorts repo not configured!

      Expected one of the following:

        * Pass the `:repo` option at runtime:

          ```
          EctoShorts.Actions.all(MyApp.Schema, %{id: [1, 2, 3]}, repo: MyApp.Repo)
          ```

        * Configure a default repo in your application config:

          ```
          # config/config.exs
          import Config

          config :ecto_shorts, :repo, MyApp.Repo
          ```
      """
    end
  end

  @doc since: "3.0.0"
  @doc """
  Returns the `Ecto.Repo` module to use for read (replica) operations.

  Checks the `:replica` option first, then falls back to the `:replica`
  or `:repo` key in the `:ecto_shorts` application configuration.

  Raises if no suitable repo is found.

  ## Examples

      iex> EctoShorts.Config.replica!()
      EctoShorts.Repo

      iex> EctoShorts.Config.replica!(replica: MyApp.Repo.Replica)
      MyApp.Repo.Replica
  """
  @spec replica!(opts :: keyword()) :: module()
  @spec replica! :: module()
  def replica!(opts \\ []) do
    with nil <- Keyword.get(opts, :replica, replica()),
         nil <- Keyword.get(opts, :repo, repo()) do
      raise """
      EctoShorts replica and repo not configured!

      Expected one of the following to be set:

        * Pass the `:replica` option at runtime:

          ```
          EctoShorts.Actions.all(MyApp.Schema, %{id: [1, 2, 3]}, replica: MyApp.Repo.Replica)
          ```

        * Configure a replica in your application config:

          ```
          # config/config.exs
          import Config

          config :ecto_shorts, :replica, MyApp.Repo.Replica
          ```

        * Pass the `:repo` option at runtime (used as a fallback if no replica is set):

          ```
          EctoShorts.Actions.all(MyApp.Schema, %{id: [1, 2, 3]}, repo: MyApp.Repo)
          ```

        * Configure a default repo in your application config:

          ```
          # config/config.exs
          import Config

          config :ecto_shorts, :repo, MyApp.Repo
          ```
      """
    end
  end

  @doc since: "3.0.0"
  @doc """
  Returns the configured `:dynamic_adapter` module from the `:ecto_shorts` application environment.

  Defaults to `nil`. When `nil`, `EctoShorts.Dynamics` auto-resolves the
  adapter from the repo's database adapter (Postgres only, out of the box).
  Set this to a custom module implementing `EctoShorts.Dynamic` to
  override expression-building behaviour.

  ## Examples

      iex> EctoShorts.Config.dynamic_adapter()
      nil

  See also `EctoShorts.Dynamic` and `repo!/1`.
  """
  @spec dynamic_adapter :: module() | nil
  def dynamic_adapter do
    Application.get_env(@app, :dynamic_adapter)
  end

  @doc since: "3.0.0"
  @doc """
  Returns the configured `:query_provider` module from the `:ecto_shorts` application environment.

  Defaults to `nil`. When `nil`, `EctoShorts.QueryProvider` falls back to
  `EctoShorts.CommonFilters.QueryProviders.NoOp`. Set this to a custom module
  that exports `build_fragment_expression/3` to control how join and lock
  expressions are resolved at runtime.

  ## Examples

      iex> EctoShorts.Config.query_provider()
      nil

  See also `EctoShorts.QueryProvider` and `dynamic_adapter/0`.
  """
  @spec query_provider :: module() | nil
  def query_provider do
    Application.get_env(@app, :query_provider)
  end

  @doc since: "3.0.0"
  @doc """
  Returns the configured `:max_positional_bindings` value from the `:ecto_shorts` application environment.

  Defaults to `5`. Used by `EctoShorts.Generator` to determine how many
  positional binding clauses to generate. Increase when your queries join
  more than ten tables.

  See also `EctoShorts.Generator` and `dynamic_adapter/0`.
  """
  @spec max_positional_bindings :: integer()
  def max_positional_bindings do
    Application.get_env(@app, :max_positional_bindings) || 5
  end
end
