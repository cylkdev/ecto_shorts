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

  ### Adapter delegation

  `EctoShorts.Dynamics` does not build Ecto expressions itself. Instead it
  resolves the configured `EctoShorts.Dynamics.Adapter` implementation and
  calls `build_dynamic/4` for each field. The built-in adapter is
  `EctoShorts.Dynamics.Adapters.Postgres`. Override it by configuring
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

  * `:dynamic_adapter` - A module that implements the `EctoShorts.Dynamics.Adapter` behaviour.
    Configurable globally via `config :ecto_shorts, dynamic_adapter: MyApp.Adapter`
    or at runtime via the `:dynamic_adapter` option on `convert_to_dynamic/4`.
    Defaults to resolved from repo adapter.

  See also `EctoShorts.Dynamics.Adapter`, `EctoShorts.CommonFilters`, and
  `EctoShorts.Config`.
  """

  alias Ecto.Query
  alias EctoShorts.CommonFilters
  alias EctoShorts.CommonSchema
  alias EctoShorts.Config
  alias EctoShorts.Dynamics.Adapters.Postgres
  alias EctoShorts.Logger

  require Ecto.Query

  @logger_prefix "EctoShorts.Dynamics"

  @boolean_operators [:and, :or]

  @doc """
  Converts filter params into a single composable dynamic expression.

  Accepts a source (schema module, `{table_name, schema}` tuple, or
  `Ecto.Query`), a `binding_selector` used to resolve the correct query
  binding inside `dynamic/2`, a `params` map or keyword list of
  `{field, value}` pairs, and an optional `opts` keyword list.

  Returns `nil` when `params` is empty. Otherwise returns a
  dynamic expression that can be spliced into a query with `^`.

  ## Options

  * `:dynamic_adapter` - A module that implements the `EctoShorts.Dynamics.Adapter`
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

  See also `EctoShorts.Dynamics.Adapter`.
  """
  @spec convert_to_dynamic(
          source :: term(),
          binding_selector :: term(),
          params :: term(),
          opts :: keyword()
        ) :: Ecto.Query.dynamic_expr() | nil
  def convert_to_dynamic(source, binding_selector, params, opts \\ []) do
    source = CommonSchema.normalize_source(source)

    if (is_map(params) and not is_struct(params)) or is_list(params) do
      Enum.reduce(params, nil, fn entry, dyn_acc ->
        append_predicates(source, dyn_acc, binding_selector, entry, opts)
      end)
    else
      append_predicates(source, nil, binding_selector, params, opts)
    end
  end

  defp append_predicates(source, dyn_a, binding_selector, {key, value}, opts)
       when key in @boolean_operators do
    merge_boolean_predicates(source, dyn_a, binding_selector, key, value, opts)
  end

  defp append_predicates(source, dyn_a, binding_selector, params, opts)
       when is_map(params) and not is_struct(params) do
    append_predicates(source, dyn_a, binding_selector, Map.to_list(params), opts)
  end

  defp append_predicates(source, dyn_a, binding_selector, list, opts) when is_list(list) do
    Enum.reduce(list, dyn_a, fn entry, dyn_acc ->
      append_predicates(source, dyn_acc, binding_selector, entry, opts)
    end)
  end

  defp append_predicates(source, dyn_a, binding_selector, {key, value}, opts) do
    adapter = dynamic_adapter!(opts)
    value = resolve_subqueries(source, key, value, opts)

    cond do
      key in adapter.operators() ->
        build_and_merge(adapter, source, binding_selector, key, value, dyn_a)

      source_has_schema?(source) ->
        schema_fields = CommonSchema.get_schema_reflection(source, :query_fields)

        if is_list(schema_fields) and key not in schema_fields do
          Logger.warning(
            @logger_prefix,
            "Expected a query field for schema #{inspect(source)}, got: #{inspect(key)}"
          )

          dyn_a
        else
          build_field_predicate(source, dyn_a, binding_selector, key, value, adapter, opts)
        end

      true ->
        build_field_predicate(source, dyn_a, binding_selector, key, value, adapter, opts)
    end
  end

  defp build_field_predicate(source, dyn_a, binding_selector, key, value, adapter, _opts)
       when is_map(value) and not is_struct(value) do
    reduce_field_value(source, dyn_a, binding_selector, key, value, adapter)
  end

  defp build_field_predicate(source, dyn_a, binding_selector, key, value, adapter, _opts)
       when is_list(value) do
    if Keyword.keyword?(value) do
      reduce_field_value(source, dyn_a, binding_selector, key, value, adapter)
    else
      build_and_merge(adapter, source, binding_selector, key, value, dyn_a)
    end
  end

  defp build_field_predicate(source, dyn_a, binding_selector, key, {op, entries}, adapter, opts)
       when op in @boolean_operators and is_list(entries) do
    if Keyword.keyword?(entries) do
      Enum.reduce(entries, dyn_a, fn entry, inner_acc ->
        case adapter.build_dynamic(source, binding_selector, key, entry) do
          nil -> inner_acc
          dyn -> merge_dynamic(inner_acc, op, dyn)
        end
      end)
    else
      merge_boolean_predicates(source, dyn_a, binding_selector, op, entries, opts)
    end
  end

  defp build_field_predicate(source, dyn_a, binding_selector, key, {op, inner}, adapter, _opts)
       when op in @boolean_operators do
    case adapter.build_dynamic(source, binding_selector, key, inner) do
      nil -> dyn_a
      dyn -> merge_dynamic(dyn_a, op, dyn)
    end
  end

  defp build_field_predicate(source, dyn_a, binding_selector, key, value, adapter, _opts) do
    build_and_merge(adapter, source, binding_selector, key, value, dyn_a)
  end

  defp reduce_field_value(source, dyn_a, binding_selector, key, entries, adapter) do
    Enum.reduce(entries, dyn_a, fn
      {op, list}, acc when op in @boolean_operators and is_list(list) ->
        Enum.reduce(list, acc, fn entry, inner_acc ->
          case adapter.build_dynamic(source, binding_selector, key, entry) do
            nil -> inner_acc
            dyn -> merge_dynamic(inner_acc, op, dyn)
          end
        end)

      {op, inner}, acc when op in @boolean_operators ->
        case adapter.build_dynamic(source, binding_selector, key, inner) do
          nil -> acc
          dyn -> merge_dynamic(acc, op, dyn)
        end

      {op, inner}, acc ->
        build_and_merge(adapter, source, binding_selector, key, {op, inner}, acc)
    end)
  end

  defp build_and_merge(adapter, source, binding_selector, key, expr, dyn_a) do
    case adapter.build_dynamic(source, binding_selector, key, expr) do
      nil ->
        Logger.warning(
          @logger_prefix,
          "No dynamic expression generated for field #{inspect(key)} with expression: #{inspect(expr)}"
        )

        dyn_a

      dyn_b ->
        merge_dynamic(dyn_a, :and, dyn_b)
    end
  end

  defp merge_boolean_predicates(
         source,
         dyn_a,
         binding_selector,
         boolean_operator,
         entries,
         opts
       ) do
    Enum.reduce(entries, dyn_a, fn entry, dyn_acc ->
      entry =
        if is_map(entry) and not is_struct(entry) do
          Map.to_list(entry)
        else
          entry
        end

      dyn_b =
        append_predicates(source, nil, binding_selector, entry, opts)

      merge_dynamic(dyn_acc, boolean_operator, dyn_b)
    end)
  end

  defp merge_dynamic(nil, _, dyn_b) do
    dyn_b
  end

  defp merge_dynamic(dyn_a, :and, dyn_b) do
    Query.dynamic([], ^dyn_a and ^dyn_b)
  end

  defp merge_dynamic(dyn_a, :or, dyn_b) do
    Query.dynamic([], ^dyn_a or ^dyn_b)
  end

  defp source_has_schema?({_, schema}) when is_atom(schema) and not is_nil(schema), do: true
  defp source_has_schema?(_), do: false

  defp resolve_subqueries(source, field_name, map, opts)
       when is_map(map) and not is_struct(map) do
    if Map.has_key?(map, :from) do
      build_subquery(source, field_name, Map.to_list(map), opts)
    else
      Map.new(map, fn {k, v} ->
        {k, resolve_subqueries(source, field_name, v, opts)}
      end)
    end
  end

  defp resolve_subqueries(source, field_name, list, opts) when is_list(list) do
    cond do
      Keyword.keyword?(list) and Keyword.has_key?(list, :from) ->
        build_subquery(source, field_name, list, opts)

      Keyword.keyword?(list) ->
        Enum.map(list, fn {k, v} ->
          {k, resolve_subqueries(source, field_name, v, opts)}
        end)

      true ->
        list
    end
  end

  defp resolve_subqueries(source, field_name, {k, v}, opts) do
    {k, resolve_subqueries(source, field_name, v, opts)}
  end

  defp resolve_subqueries(_source, _field_name, value, _opts), do: value

  defp build_subquery(source, field_name, params, opts) do
    {schema_source, rest_params} = Keyword.pop(params, :from, source)

    default_select = if field_name === :exists, do: true, else: field_name
    select_value = Keyword.get(rest_params, :select, default_select)

    final_params = Keyword.put(rest_params, :select, select_value)

    CommonFilters.convert_params_to_filter(schema_source, final_params, opts)
  end

  defp dynamic_adapter!(opts) do
    adapter = opts[:dynamic_adapter] || Config.dynamic_adapter()

    if adapter !== nil do
      adapter
    else
      repo = Config.repo!(opts)

      unless is_atom(repo) and Code.ensure_loaded?(repo) and
               function_exported?(repo, :__adapter__, 0) do
        raise ArgumentError,
              "Expected :repo to be an Ecto.Repo module that exports __adapter__/0, got: #{inspect(repo)}"
      end

      case repo.__adapter__() do
        Ecto.Adapters.Postgres ->
          Postgres

        other ->
          raise ArgumentError, """
          Unsupported Ecto repo adapter: #{inspect(other)} (repo: #{inspect(repo)}).

          EctoShorts currently supports dynamic expressions for Postgres only.

          Use an Ecto SQL adapter with Postgres, or provide a custom dynamic expression adapter module via:

              dynamic_adapter: MyApp.DynamicExpressionAdapter
          """
      end
    end
  end
end
