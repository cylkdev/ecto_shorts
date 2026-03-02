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

  @equal :==
  @boolean_operators [:and, :or]
  @map_payload_helper_operators [:datetime, :date]

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

  See also `apply_helper_expressions/4` and `EctoShorts.Dynamics.Adapter`.
  """
  @spec convert_to_dynamic(
          source :: term(),
          binding_selector :: term(),
          params :: term(),
          opts :: keyword()
        ) :: Ecto.Query.dynamic_expr() | nil
  def convert_to_dynamic(source, binding_selector, params, opts \\ []) do
    source = CommonSchema.normalize_source(source)

    if is_map(params) or is_list(params) do
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
    dynamic_adapter = dynamic_adapter!(opts)
    adapter_operators = dynamic_adapter.operators()

    cond do
      key in adapter_operators ->
        build_operator_predicates(source, dyn_a, binding_selector, key, value, dynamic_adapter, opts)

      source_has_schema?(source) ->
        schema_fields = CommonSchema.get_schema_reflection(source, :query_fields)

        if is_list(schema_fields) and key not in schema_fields do
          Logger.warning(
            @logger_prefix,
            "Expected a query field for schema #{inspect(source)}, got: #{inspect(key)}"
          )

          dyn_a
        else
          build_field_predicates(
            source,
            dyn_a,
            binding_selector,
            key,
            value,
            dynamic_adapter,
            opts
          )
        end

      true ->
        build_field_predicates(source, dyn_a, binding_selector, key, value, dynamic_adapter, opts)
    end
  end

  defp build_field_predicates(source, dyn_a, binding_selector, key, value, dynamic_adapter, opts) do
    reduce_predicates(source, dyn_a, binding_selector, key, value, dynamic_adapter, opts, :field)
  end

  defp build_operator_predicates(source, dyn_a, binding_selector, key, value, dynamic_adapter, opts) do
    reduce_predicates(source, dyn_a, binding_selector, key, value, dynamic_adapter, opts, :operator)
  end

  defp reduce_predicates(source, dyn_a, binding_selector, key, value, dynamic_adapter, opts, mode) do
    value = apply_helper_expressions(source, key, value, opts)
    label = if mode == :field, do: "field", else: "operator"

    value
    |> normalize_expression_params()
    |> Enum.reduce(dyn_a, fn
      {boolean_op, inner}, acc when boolean_op in @boolean_operators and mode == :field ->
        if is_list(inner) do
          merge_boolean_predicates(source, acc, binding_selector, boolean_op, inner, opts)
        else
          build_and_merge(dynamic_adapter, source, binding_selector, key, inner, acc, boolean_op, label)
        end

      item, acc ->
        expr =
          if mode == :field and not is_tuple(item) do
            {@equal, item}
          else
            item
          end

        build_and_merge(dynamic_adapter, source, binding_selector, key, expr, acc, :and, label)
    end)
  end

  defp build_and_merge(dynamic_adapter, source, binding_selector, key, expr, dyn_a, merge_op, label) do
    case dynamic_adapter.build_dynamic(source, binding_selector, key, expr) do
      nil ->
        Logger.warning(
          @logger_prefix,
          "No dynamic expression generated for #{label} #{inspect(key)} with expression: #{inspect(expr)}"
        )

        dyn_a

      dyn_b ->
        merge_dynamic(dyn_a, merge_op, dyn_b)
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

  @doc """
  Applies helper expression transformations to a field value before
  building a dynamic expression.

  Accepts a `source` (used for subquery building), the `field_name` atom,
  the raw `expression` value, and an `opts` keyword list. Recursively
  walks the expression, replacing helper operator tuples (such as
  `{:datetime_add, [...]}`, `{:date_add, [...]}`, `{:from_now, [...]}`,
  `{:ago, [...]}`) with their corresponding Ecto sub-expressions.

  When the expression is a keyword list containing a `:from` key, the
  function builds a subquery via
  `EctoShorts.CommonFilters.convert_params_to_filter/3` and returns the
  result as the expression value.

  Returns the transformed expression, or the original expression unchanged
  when no helper operators are found.

  ## Examples

      iex> EctoShorts.Dynamics.apply_helper_expressions(EctoShorts.Schema.Post, :title, "hello", [])
      "hello"

  See also `convert_to_dynamic/4`.
  """
  @spec apply_helper_expressions(
          source :: term(),
          field_name :: atom(),
          expression :: term(),
          opts :: keyword()
        ) :: term()
  def apply_helper_expressions(source, field_name, expression, opts) do
    case expression do
      map when is_map(map) and not is_struct(map) ->
        apply_helper_expressions(source, field_name, Map.to_list(map), opts)

      list when is_list(list) ->
        cond do
          Keyword.keyword?(list) and Keyword.has_key?(list, :from) ->
            build_helper_expr_subquery(source, field_name, list, opts)

          Keyword.keyword?(list) ->
            Enum.map(list, fn {key, value} ->
              {key, apply_helper_expressions(source, field_name, value, opts)}
            end)

          true ->
            expression
        end

      {key, value} ->
        {key, apply_helper_expressions(source, field_name, value, opts)}

      _ ->
        expression
    end
  end

  defp build_helper_expr_subquery(source, field_name, params, opts) do
    from_value = params[:from]

    {schema_source, filter_params} =
      extract_from_params(from_value, source)

    default_select = if field_name == :exists, do: true, else: field_name
    select_value = helper_expr_select(filter_params, default_select)

    filter_params =
      if is_map(filter_params) and not is_struct(filter_params) do
        Map.to_list(filter_params)
      else
        filter_params
      end

    from_map =
      filter_params
      |> Map.new()
      |> Map.put(:query, schema_source)

    CommonFilters.convert_params_to_filter(
      source,
      [from: from_map, select: select_value],
      opts
    )
  end

  defp extract_from_params(from_map, fallback_source)
       when is_map(from_map) and not is_struct(from_map) do
    {query_source, filter_params} = Map.pop(from_map, :query)
    {query_source || fallback_source, filter_params}
  end

  defp extract_from_params(from_list, fallback_source) when is_list(from_list) do
    {query_source, filter_params} = Keyword.pop(from_list, :query)
    {query_source || fallback_source, filter_params}
  end

  defp extract_from_params(nil, fallback_source) do
    {fallback_source, []}
  end

  defp helper_expr_select(params, default_select)
       when is_map(params) and not is_struct(params) do
    Map.get(params, :select, default_select)
  end

  defp helper_expr_select(params, default_select) when is_list(params) do
    if Keyword.keyword?(params) do
      Keyword.get(params, :select, default_select)
    else
      default_select
    end
  end

  defp helper_expr_select(_params, default_select), do: default_select

  defp normalize_expression_params(term) do
    term
    |> flatten_expression_params([])
    |> Enum.reverse()
  end

  defp flatten_expression_params(term, acc) when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> flatten_expression_params(acc)
  end

  defp flatten_expression_params([], acc), do: acc

  defp flatten_expression_params(list, acc) when is_list(list) do
    case list do
      [head | _] when is_map(head) ->
        flatten_expression_entries(list, acc)

      _ ->
        if Keyword.keyword?(list) do
          flatten_expression_entries(list, acc)
        else
          [list | acc]
        end
    end
  end

  defp flatten_expression_params({k, v}, acc) when is_map(v) and not is_struct(v) do
    if k in @map_payload_helper_operators do
      [{k, v} | acc]
    else
      flatten_expression_params({k, Map.to_list(v)}, acc)
    end
  end

  defp flatten_expression_params({k, v}, acc)
       when k in @map_payload_helper_operators and is_list(v),
       do: [{k, v} | acc]

  defp flatten_expression_params({k, v}, acc) when is_list(v) do
    case v do
      [head | _] when is_map(head) ->
        flatten_keyed_expression_entries(k, v, acc)

      _ ->
        if Keyword.keyword?(v) do
          flatten_keyed_expression_entries(k, v, acc)
        else
          [{k, v} | acc]
        end
    end
  end

  defp flatten_expression_params(v, acc) do
    [v | acc]
  end

  defp flatten_expression_entries(list, acc) do
    Enum.reduce(list, acc, fn entry, acc_inner ->
      flatten_expression_params(entry, acc_inner)
    end)
  end

  defp flatten_keyed_expression_entries(key, list, acc) do
    list
    |> normalize_expression_params()
    |> Enum.map(&{key, &1})
    |> flatten_expression_params(acc)
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
