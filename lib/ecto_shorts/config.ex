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
  """
  @spec replica :: module() | nil
  def replica do
    Application.get_env(@app, :replica)
  end

  @doc since: "3.0.0"
  @doc """
  Returns the configured `:dynamic_adapter` value from the
  application environment. Defaults to `nil`.

  ## Examples

      iex> EctoShorts.Config.dynamic_adapter()
      nil
  """
  @spec dynamic_adapter :: module() | nil
  def dynamic_adapter do
    Application.get_env(@app, :dynamic_adapter)
  end

  @doc since: "3.0.0"
  @doc """
  Returns the configured `:query_builder` value from the
  application environment. Defaults to `nil`.

  ## Examples

      iex> EctoShorts.Config.query_builder()
      nil
  """
  @spec query_builder :: module() | nil
  def query_builder do
    Application.get_env(@app, :query_builder)
  end

  @doc since: "3.0.0"
  @doc """
  Returns the configured `:query_provider` value from the
  application environment. Defaults to `nil`.

  ## Examples

      iex> EctoShorts.Config.query_provider()
      nil
  """
  @spec query_provider :: module() | nil
  def query_provider do
    Application.get_env(@app, :query_provider)
  end

  @doc since: "3.0.0"
  @doc """
  Returns the configured `:max_positional_bindings` value from the
  application environment. Defaults to `nil`.

  ## Examples

      iex> EctoShorts.Config.max_positional_bindings()
      nil
  """
  @spec max_positional_bindings :: integer()
  def max_positional_bindings do
    Application.get_env(@app, :max_positional_bindings)
  end
end
