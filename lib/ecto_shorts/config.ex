defmodule EctoShorts.Config do
  @moduledoc false

  @app :ecto_shorts

  @doc """
  Returns the value of `ecto_shorts` config key `:repo`.

  ### Examples

      iex> EctoShorts.Config.repo()
      EctoShorts.Support.Repo
  """
  @spec repo :: module() | nil
  def repo do
    Application.get_env(@app, :repo)
  end

  @doc """
  Returns the value of `ecto_shorts` config key `:replica`.

  ### Examples

      iex> EctoShorts.Config.replica()
      nil
  """
  @doc since: "2.5.0"
  @spec replica :: module() | nil
  def replica do
    Application.get_env(@app, :replica)
  end

  @doc """
  Returns a `Ecto.Repo` module.

  Raises if the repo is not configured and the option `:repo` is not set.

  ### Examples

      iex> EctoShorts.Config.repo!()
      EctoShorts.Support.Repo

      iex> EctoShorts.Config.repo!(repo: MyApp.Repo)
      MyApp.Repo
  """
  @doc since: "2.5.0"
  @spec repo!(opts :: keyword()) :: module()
  @spec repo! :: module()
  def repo!(opts \\ []) do
    with nil <- Keyword.get(opts, :repo, repo()) do
      raise ArgumentError, """
      EctoShorts repo not configured!

      Expected one of the following:

      * The option `:repo` is specified at runtime.

        ```
        EctoShorts.Actions.all(MyApp.Schema, %{id: [1, 2, 3]}, repo: MyApp.Repo)
        ```

      * The option `:repo` is set in configuration.

        ```
        # config.exs
        import Config

        config :ecto_shorts, :repo, MyApp.Repo
        ```
      """
    end
  end

  @doc """
  Returns a `Ecto.Repo` module.

  Raises if the key `:replica` and `:repo` is not specified in
  configuration and the option `:replica` and `:repo` is not
  specified at runtime.

  ### Examples

      iex> EctoShorts.Config.replica!()
      EctoShorts.Support.Repo

      iex> EctoShorts.Config.replica!(replica: MyApp.Repo.Replica)
      MyApp.Repo.Replica
  """
  @doc since: "2.5.0"
  @spec replica!(opts :: keyword()) :: module()
  @spec replica! :: module()
  def replica!(opts \\ []) do
    with nil <- Keyword.get(opts, :replica, replica()),
      nil <- Keyword.get(opts, :repo, repo()) do
      raise ArgumentError, """
      EctoShorts replica and repo not configured!

      Expected one of the following:

      * The option `:replica` is specified at runtime.

        ```
        EctoShorts.Actions.all(MyApp.Schema, %{id: [1, 2, 3]}, replica: MyApp.Repo.Replica)
        ```

      * The option `:replica` is set in configuration.

        ```
        # config.exs
        import Config

        config :ecto_shorts, :replica, MyApp.Repo.Replica
        ```

      * The option `:repo` is specified at runtime.

        ```
        EctoShorts.Actions.all(MyApp.Schema, %{id: [1, 2, 3]}, repo: MyApp.Repo)
        ```

      * The option `:repo` is set in configuration.

        ```
        # config.exs
        import Config

        config :ecto_shorts, :repo, MyApp.Repo
        ```
      """
    end
  end
end
