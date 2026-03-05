defmodule EctoShorts.Dynamics do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Converts filter parameter maps into composable dynamic expressions.

  Used internally by `EctoShorts.CommonFilters.Filter` and other query
  builders to translate `{field, value}` pairs and boolean operator trees
  into composable dynamic expressions. Delegates the actual expression
  construction to the configured dynamic adapter.

  ## Key concepts

  ### Params format

  Filter params are plain maps or keyword lists of `{field, value}` pairs:

      %{title: "hello", published: true}

  Boolean sub-groups use the `:and` and `:or` keys with a list of sub-params:

      %{or: [%{published: true}, %{published: false}]}

  Helper operators such as `:datetime_add` and `:date_add` can be embedded
  inside field values to produce relative date expressions at the database level.

  Query-builder payload resolution (for example `%{from: ...}`) is handled
  upstream by `EctoShorts.CommonFilters` helpers before values are passed
  to this module.

  ### Adapter delegation

  `EctoShorts.Dynamics` does not build Ecto expressions itself. Instead it
  resolves the configured `EctoShorts.Dynamic` implementation and
  calls `build_dynamic/4` for each field. The built-in adapter is
  `EctoShorts.Dynamics.Postgres`. Override it by configuring
  `:dynamic_adapter` in your application config.

  ### Schema-aware filtering

  When a source with a schema module is provided, the module's
  `:query_fields` reflection is used to warn on unknown filter keys
  rather than silently ignoring them.

  ## Getting started

      import Ecto.Query

      params = %{title: "hello", published: true}

      dynamic_expr =
        EctoShorts.Dynamics.convert_to_dynamic(EctoShorts.Schema.Post, nil, params)

      from p in EctoShorts.Schema.Post, where: ^dynamic_expr

  ## Configuration

  * `:dynamic_adapter` - A module that implements the `EctoShorts.Dynamic` behaviour.
    Configurable globally via `config :ecto_shorts, dynamic_adapter: MyApp.Adapter`
    or at runtime via the `:dynamic_adapter` option on `convert_to_dynamic/4`.
    Defaults to resolved from repo adapter.

  See also `EctoShorts.Dynamic`, `EctoShorts.CommonFilters`, and
  `EctoShorts.Config`.
  """

  alias EctoShorts.CommonSchema
  alias EctoShorts.Config

  @adapters %{
    Ecto.Adapters.Postgres => EctoShorts.Dynamics.Postgres
  }

  @doc false
  def adapters, do: @adapters

  @doc """
  Converts filter params into a single composable dynamic expression.

  Accepts a source (schema module, `{table_name, schema}` tuple, or
  `Ecto.Query`), a `binding_selector` used to resolve the correct query
  binding inside `dynamic/2`, a `params` map or keyword list of
  `{field, value}` pairs, and an optional `opts` keyword list.

  Returns `nil` when `params` is empty. Otherwise returns a
  dynamic expression that can be spliced into a query with `^`.

  ## Options

  * `:dynamic_adapter` - A module that implements the `EctoShorts.Dynamic`
    behaviour to use for this call. Defaults to resolved from repo adapter.

  * `:repo` - The `Ecto.Repo` module used to resolve the dynamic expression adapter
    when the option `:dynamic_adapter` is not set. Defaults to `EctoShorts.Config.repo/0`.

  ## Examples

      iex> import Ecto.Query
      ...> dyn = EctoShorts.Dynamics.convert_to_dynamic(EctoShorts.Schema.Post, nil, %{published: true})
      ...> is_struct(dyn, Ecto.Query.DynamicExpr)
      true

      iex> EctoShorts.Dynamics.convert_to_dynamic(EctoShorts.Schema.Post, nil, %{})
      nil

  See also `EctoShorts.Dynamic`.
  """
  @spec convert_to_dynamic(
          source :: term(),
          binding_selector :: term(),
          value :: term(),
          opts :: keyword()
        ) :: Ecto.Query.dynamic_expr() | nil
  def convert_to_dynamic(source, binding_selector, term, opts \\ []) do
    adapter = adapter_for_repo!(opts)
    source = CommonSchema.normalize_source(source)
    adapter.convert_to_dynamic(source, binding_selector, term)
  end

  defp adapter_for_repo!(opts) do
    builder = opts[:dynamic_adapter] || Config.dynamic_adapter()

    if not is_nil(builder) do
      if Code.ensure_loaded?(builder) and function_exported?(builder, :build_dynamic, 4) do
        builder
      else
        raise ArgumentError,
              "The specified dynamic adapter #{inspect(builder)} does not implement build_dynamic/4"
      end
    else
      repo = Config.repo!(opts)

      unless is_atom(repo) and Code.ensure_loaded?(repo) and
               function_exported?(repo, :__adapter__, 0) do
        raise ArgumentError,
              "Expected :repo to be an Ecto.Repo module that exports __adapter__/0, got: #{inspect(repo)}"
      end

      with nil <- Map.get(@adapters, repo.__adapter__()) do
        raise ArgumentError, """
        Unsupported Ecto repo adapter: #{inspect(repo.__adapter__())} (repo: #{inspect(repo)}).

        EctoShorts currently supports dynamic expressions for Postgres only.

        Use an Ecto SQL adapter with Postgres, or provide a custom dynamic expression adapter module via:

            dynamic_adapter: MyApp.DynamicExpressionAdapter
        """
      end
    end
  end
end
