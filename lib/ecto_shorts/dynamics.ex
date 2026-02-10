defmodule EctoShorts.Dynamics do
  @moduledoc false

  alias EctoShorts.CommonFilters
  alias EctoShorts.CommonSchema
  alias EctoShorts.Config
  alias EctoShorts.Dynamics.Adapters.Postgres

  require Ecto.Query

  @logger_prefix "EctoShorts.Dynamics"

  @equal :==
  @boolean_operators [:and, :or]
  @aggregate_operators [:avg, :count, :max, :min, :sum]

  def convert_to_dynamic(source, binding_selector, params, opts \\ []) do
    source = CommonSchema.normalize_source(source)

    if is_map(params) or is_list(params) do
      Enum.reduce(params, nil, fn entry, dyn_acc ->
        append_param_predicates(source, dyn_acc, binding_selector, entry, opts)
      end)
    else
      append_param_predicates(source, nil, binding_selector, params, opts)
    end
  end

  defp append_param_predicates(source, dyn_a, binding_selector, params, opts)
       when is_map(params) or is_list(params) do
    Enum.reduce(params, dyn_a, fn entry, dyn_acc ->
      append_param_predicates(source, dyn_acc, binding_selector, entry, opts)
    end)
  end

  defp append_param_predicates(source, dyn_a, binding_selector, {key, value}, opts)
       when key in @boolean_operators do
    if is_map(value) and not is_struct(value) do
      append_param_predicates(
        source,
        dyn_a,
        binding_selector,
        {key, Map.to_list(value)},
        opts
      )
    else
      merge_boolean_predicates(
        source,
        dyn_a,
        binding_selector,
        key,
        value,
        opts
      )
    end
  end

  defp append_param_predicates(source, dyn_a, binding_selector, {key, value}, opts) do
    dynamic_adapter = dynamic_adapter!(opts)

    cond do
      key in @aggregate_operators ->
        append_aggregate_predicates(source, dyn_a, binding_selector, key, value, opts)

      source_has_schema?(source) and key not in dynamic_adapter.operators() ->
        append_schema_predicate(source, dyn_a, binding_selector, {key, value}, opts)

      true ->
        append_operator_predicates(
          source,
          dyn_a,
          binding_selector,
          key,
          value,
          dynamic_adapter
        )
    end
  end

  defp append_aggregate_predicates(
         source,
         dyn_a,
         binding_selector,
         helper_op,
         params,
         opts
       ) do
    case params do
      map when is_map(map) and not is_struct(map) ->
        append_aggregate_predicates(
          source,
          dyn_a,
          binding_selector,
          helper_op,
          Map.to_list(map),
          opts
        )

      list when is_list(list) ->
        if Keyword.keyword?(list) do
          Enum.reduce(list, dyn_a, fn {field, expr}, dyn_acc ->
            append_aggregate_predicate(
              source,
              dyn_acc,
              binding_selector,
              helper_op,
              field,
              expr,
              opts
            )
          end)
        else
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected params for #{helper_op} to be a map or keyword list, got: #{inspect(params)}"
          )

          dyn_a
        end

      {field, expr} ->
        append_aggregate_predicate(
          source,
          dyn_a,
          binding_selector,
          helper_op,
          field,
          expr,
          opts
        )

      _ ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected params for #{helper_op} to be a map or keyword list, got: #{inspect(params)}"
        )

        dyn_a
    end
  end

  defp append_aggregate_predicate(
         source,
         dyn_a,
         binding_selector,
         helper_op,
         field,
         expr,
         opts
       ) do
    helper_expr =
      if is_map(expr) and not is_struct(expr) do
        {helper_op, Map.to_list(expr)}
      else
        {helper_op, expr}
      end

    if source_has_schema?(source) do
      append_schema_predicate(source, dyn_a, binding_selector, {field, helper_expr}, opts)
    else
      append_field_predicate(source, dyn_a, binding_selector, field, helper_expr, opts)
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
        append_param_predicates(source, nil, binding_selector, entry, opts)

      merge_dynamic(dyn_acc, boolean_operator, dyn_b)
    end)
  end

  defp append_schema_predicate(source, dyn_a, binding_selector, {key, value}, opts) do
    schema_fields = CommonSchema.get_schema_reflection(source, :query_fields)

    if is_list(schema_fields) and key not in schema_fields do
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected a query field for schema #{inspect(source)}, got: #{inspect(key)}"
      )

      dyn_a
    else
      append_schema_predicate_value(source, dyn_a, binding_selector, key, value, opts)
    end
  end

  defp append_schema_predicate_value(source, dyn_a, binding_selector, key, value, opts) do
    case value do
      map when is_map(map) and not is_struct(map) ->
        append_schema_predicate(
          source,
          dyn_a,
          binding_selector,
          {key, Map.to_list(map)},
          opts
        )

      list when is_list(list) ->
        if Keyword.keyword?(list) do
          Enum.reduce(list, dyn_a, fn entry, dyn_acc ->
            append_schema_predicate(source, dyn_acc, binding_selector, {key, entry}, opts)
          end)
        else
          append_field_predicate(source, dyn_a, binding_selector, key, value, opts)
        end

      _ ->
        append_field_predicate(source, dyn_a, binding_selector, key, value, opts)
    end
  end

  defp append_field_predicate(
         source,
         dyn_a,
         binding_selector,
         key,
         {boolean_operator, values},
         opts
       )
       when boolean_operator in @boolean_operators and is_list(values) do
    entries =
      if has_map_or_kwd?(values) do
        values
      else
        Enum.map(values, &{key, &1})
      end

    dyn_b =
      merge_boolean_predicates(
        source,
        nil,
        binding_selector,
        boolean_operator,
        entries,
        opts
      )

    merge_dynamic(dyn_a, :and, dyn_b)
  end

  defp append_field_predicate(
         source,
         dyn_a,
         binding_selector,
         key,
         {inner_key, value},
         opts
       )
       when inner_key in [:any, :all] do
    value = apply_helper_expressions(source, key, value, opts)

    append_predicate_items(
      source,
      dyn_a,
      binding_selector,
      key,
      value,
      opts,
      fn item -> {inner_key, item} end
    )
  end

  defp append_field_predicate(
         source,
         dyn_a,
         binding_selector,
         key,
         {:not, {inner_key, value}},
         opts
       ) do
    if inner_key in [:any, :all] do
      value = apply_helper_expressions(source, key, value, opts)

      append_predicate_items(
        source,
        dyn_a,
        binding_selector,
        key,
        value,
        opts,
        fn item -> {:not, {inner_key, item}} end
      )
    else
      append_predicate_items(
        source,
        dyn_a,
        binding_selector,
        key,
        value,
        opts,
        fn item -> {:not, {inner_key, item}} end
      )
    end
  end

  defp append_field_predicate(
         source,
         dyn_a,
         binding_selector,
         key,
         {:not, value},
         opts
       ) do
    case value do
      map when is_map(map) and not is_struct(map) ->
        append_field_predicate(
          source,
          dyn_a,
          binding_selector,
          key,
          {:not, Map.to_list(map)},
          opts
        )

      list when is_list(list) ->
        Enum.reduce(list, dyn_a, fn entry, dyn_acc ->
          append_field_predicate(source, dyn_acc, binding_selector, key, {:not, entry}, opts)
        end)

      _ ->
        append_predicate_items(
          source,
          dyn_a,
          binding_selector,
          key,
          value,
          opts,
          fn item -> {:not, item} end
        )
    end
  end

  defp append_field_predicate(
         source,
         dyn_a,
         binding_selector,
         key,
         {op, value},
         opts
       ) do
    append_predicate_items(source, dyn_a, binding_selector, key, value, opts, fn item ->
      {op, item}
    end)
  end

  defp append_field_predicate(source, dyn_a, binding_selector, key, value, opts) do
    append_field_predicate(
      source,
      dyn_a,
      binding_selector,
      key,
      {@equal, value},
      opts
    )
  end

  defp append_predicate_items(
         source,
         dyn_a,
         binding_selector,
         key,
         value,
         opts,
         fun
       ) do
    dynamic_adapter = dynamic_adapter!(opts)

    value
    |> normalize_expression_params()
    |> Enum.reduce(dyn_a, fn params, dyn_acc ->
      dyn_b =
        dynamic_adapter.build_dynamic(
          source,
          binding_selector,
          key,
          fun.(params)
        )

      merge_dynamic(dyn_acc, :and, dyn_b)
    end)
  end

  defp append_operator_predicates(
         source,
         dyn_a,
         binding_selector,
         key,
         value,
         dynamic_adapter
       ) do
    value
    |> normalize_expression_params()
    |> Enum.reduce(dyn_a, fn item, dyn_acc ->
      dyn_b = dynamic_adapter.build_dynamic(source, binding_selector, key, item)
      merge_dynamic(dyn_acc, :and, dyn_b)
    end)
  end

  defp merge_dynamic(nil, _, dyn_b) do
    dyn_b
  end

  defp merge_dynamic(dyn_a, :and, dyn_b) do
    Ecto.Query.dynamic([], ^dyn_a and ^dyn_b)
  end

  defp merge_dynamic(dyn_a, :or, dyn_b) do
    Ecto.Query.dynamic([], ^dyn_a or ^dyn_b)
  end

  defp has_map_or_kwd?(values) when is_list(values) do
    case values do
      [entry | _] -> (is_map(entry) and not is_struct(entry)) or Keyword.keyword?(entry)
      [] -> false
    end
  end

  defp source_has_schema?({_, schema}) when is_atom(schema) and not is_nil(schema), do: true
  defp source_has_schema?(_), do: false

  def apply_helper_expressions(source, field_name, expression, opts) do
    case expression do
      map when is_map(map) and not is_struct(map) ->
        apply_helper_expressions(source, field_name, Map.to_list(map), opts)

      list when is_list(list) ->
        if Keyword.keyword?(list) and
             (Keyword.has_key?(list, :source) or Keyword.has_key?(list, :query)) do
          build_helper_expr_subquery(source, field_name, list, opts)
        else
          Enum.map(list, fn {key, value} ->
            {key, apply_helper_expressions(source, field_name, value, opts)}
          end)
        end

      {key, value} ->
        {key, apply_helper_expressions(source, field_name, value, opts)}

      _ ->
        expression
    end
  end

  defp build_helper_expr_subquery(source, field_name, params, opts) do
    schema_source = params[:source] || source
    filter_params = params[:query] || []

    CommonFilters.convert_params_to_filter(
      source,
      [
        source: schema_source,
        query: filter_params,
        select: filter_params[:select] || field_name
      ],
      opts
    )
  end

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
    flatten_expression_params({k, Map.to_list(v)}, acc)
  end

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

    if not is_nil(adapter) do
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
