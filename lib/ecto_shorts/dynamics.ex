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

  alias Ecto.Query
  alias EctoShorts.CommonSchema
  alias EctoShorts.Config
  alias EctoShorts.Logger

  require Ecto.Query

  @logger_prefix "EctoShorts.Dynamics"

  @boolean_operators [:and, :or]

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
    source
    |> CommonSchema.normalize_source()
    |> append_predicates(nil, binding_selector, term, opts)
  end

  defp append_predicates(source, dyn_left, binding_selector, {key, map}, opts)
       when is_map(map) and not is_struct(map) do
    append_predicates(source, dyn_left, binding_selector, {key, Map.to_list(map)}, opts)
  end

  defp append_predicates(source, dyn_left, binding_selector, {key, {boolean_op, sub_entries}}, opts)
       when boolean_op in @boolean_operators and is_list(sub_entries) do
    Enum.reduce(sub_entries, dyn_left, fn entry, dyn_acc ->
      dyn_right = append_predicates(source, nil, binding_selector, {key, entry}, opts)
      merge_dynamic(dyn_acc, boolean_op, dyn_right)
    end)
  end

  defp append_predicates(source, dyn_left, binding_selector, {key, value}, opts) do
    adapter = adapter_for_repo!(opts)

    cond do
      key in adapter.operators() ->
        build_dynamic(adapter, source, binding_selector, key, value, dyn_left)

      key in @boolean_operators ->
        Enum.reduce(value, dyn_left, fn entry, dyn_acc ->
          dyn_right = append_predicates(source, nil, binding_selector, entry, opts)
          merge_dynamic(dyn_acc, key, dyn_right)
        end)

      source_has_schema?(source) ->
        schema_fields = CommonSchema.get_schema_reflection(source, :query_fields)

        if key not in schema_fields do
          Logger.warning(
            @logger_prefix,
            "Expected a query field for schema #{inspect(source)}, got: #{inspect(key)}"
          )

          dyn_left
        else
          if Keyword.keyword?(value) do
            Enum.reduce(value, dyn_left, fn entry, dyn_acc ->
              append_predicates(source, dyn_acc, binding_selector, {key, entry}, opts)
            end)
          else
            build_dynamic(adapter, source, binding_selector, key, value, dyn_left)
          end
        end

      true ->
        if Keyword.keyword?(value) do
          Enum.reduce(value, dyn_left, fn entry, dyn_acc ->
            append_predicates(source, dyn_acc, binding_selector, {key, entry}, opts)
          end)
        else
          build_dynamic(adapter, source, binding_selector, key, value, dyn_left)
        end
    end
  end

  defp append_predicates(source, dyn_left, binding_selector, params, opts) do
    if (is_map(params) and not is_struct(params)) or Keyword.keyword?(params) do
      Enum.reduce(params, dyn_left, fn {k, v}, dyn_acc ->
        append_predicates(source, dyn_acc, binding_selector, {k, v}, opts)
      end)
    else
      raise "Expected a map or keyword-list, got: #{inspect(params)}"
    end
  end

  defp build_dynamic(adapter, source, binding_selector, key, expr, dyn_left) do
    case adapter.build_dynamic(source, binding_selector, key, expr) do
      nil ->
        Logger.warning(
          @logger_prefix,
          "Adapter #{inspect(adapter)} returned nil for field #{inspect(key)} with expression: #{inspect(expr)}"
        )

        dyn_left

      dyn_right ->
        merge_dynamic(dyn_left, :and, dyn_right)
    end
  end

  defp merge_dynamic(nil, _, dyn_right) do
    dyn_right
  end

  defp merge_dynamic(dyn_left, :and, dyn_right) do
    Query.dynamic([], ^dyn_left and ^dyn_right)
  end

  defp merge_dynamic(dyn_left, :or, dyn_right) do
    Query.dynamic([], ^dyn_left or ^dyn_right)
  end

  defp source_has_schema?({_, schema}) when is_atom(schema) and not is_nil(schema), do: true
  defp source_has_schema?(_), do: false

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
