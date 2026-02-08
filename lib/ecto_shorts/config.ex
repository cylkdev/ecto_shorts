defmodule EctoShorts.Config do
  @moduledoc false

  @app :ecto_shorts

  @doc """
  Returns the configured `:error_module` value from the `:ecto_shorts` application environment.

  Defaults to `nil` if not set.

  ## Examples

      iex> EctoShorts.Config.error_module()
      EctoShorts.Actions.Error
  """
  @spec error_module :: module() | nil
  def error_module, do: Application.get_env(@app, :error_module)

  @doc """
  Returns the configured `:repo` value from the `:ecto_shorts` application environment.

  Defaults to `nil` if not set.

  ## Examples

      iex> EctoShorts.Config.repo()
      EctoShorts.Repo
  """
  @spec repo :: module() | nil
  def repo do
    Application.get_env(@app, :repo)
  end

  @doc since: "2.5.0"
  @doc """
  Returns the configured `:replica` value from the `:ecto_shorts` application environment.

  Defaults to `nil` if not set.

  ## Examples

      iex> EctoShorts.Config.replica()
      nil
  """
  @spec replica :: module() | nil
  def replica do
    Application.get_env(@app, :replica)
  end

  @doc since: "2.5.0"
  @doc """
  Returns the configured join source module.
  """
  @spec join_source_module :: module() | nil
  def join_source_module do
    Application.get_env(@app, :join_source_module)
  end

  @doc since: "2.5.0"
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

  @doc since: "2.5.0"
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

  @doc since: "2.5.0"
  @doc """
  Returns the compiler options.

  Defaults to `[]`.

  ## Examples

      iex> EctoShorts.Config.compiler()
      []
  """
  @spec compiler :: keyword()
  def compiler do
    Application.get_env(@app, :compiler) || []
  end

  @doc since: "2.5.0"
  @doc """
  Returns the dynamic adapter module.

  Defaults to `nil`.

  ## Examples

      iex> EctoShorts.Config.dynamic_adapter()
  """
  @spec dynamic_adapter :: module() | nil
  def dynamic_adapter do
    Application.get_env(@app, :dynamic_adapter)
  end
end
