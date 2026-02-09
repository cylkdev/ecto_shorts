defmodule EctoShorts.Dynamics do
  @moduledoc false

  alias EctoShorts.CommonSchema
  alias EctoShorts.Config
  alias EctoShorts.Dynamics.Adapters.Postgres

  import Ecto.Query, only: [dynamic: 2, select: 3]

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

  defp append_param_predicates(source, left_dynamic, binding_selector, params, opts)
       when is_map(params) or is_list(params) do
    Enum.reduce(params, left_dynamic, fn entry, dyn_acc ->
      append_param_predicates(source, dyn_acc, binding_selector, entry, opts)
    end)
  end

  defp append_param_predicates(source, left_dynamic, binding_selector, {key, value}, opts)
       when key in @boolean_operators do
    if is_map(value) and not is_struct(value) do
      append_param_predicates(
        source,
        left_dynamic,
        binding_selector,
        {key, Map.to_list(value)},
        opts
      )
    else
      merge_boolean_predicates(
        source,
        left_dynamic,
        binding_selector,
        key,
        value,
        opts
      )
    end
  end

  defp append_param_predicates(source, left_dynamic, binding_selector, {key, value}, opts) do
    dynamic_adapter = dynamic_adapter!(opts)

    cond do
      key in @aggregate_operators ->
        append_aggregate_predicates(source, left_dynamic, binding_selector, key, value, opts)

      source_has_schema?(source) and not expression_operator?(key, dynamic_adapter) ->
        append_schema_predicate(source, left_dynamic, binding_selector, {key, value}, opts)

      true ->
        append_operator_predicates(
          source,
          left_dynamic,
          binding_selector,
          key,
          value,
          dynamic_adapter
        )
    end
  end

  defp append_aggregate_predicates(
         source,
         left_dynamic,
         binding_selector,
         helper_op,
         params,
         opts
       ) do
    case params do
      map when is_map(map) and not is_struct(map) ->
        append_aggregate_predicates(
          source,
          left_dynamic,
          binding_selector,
          helper_op,
          Map.to_list(map),
          opts
        )

      list when is_list(list) ->
        if Keyword.keyword?(list) do
          Enum.reduce(list, left_dynamic, fn {field, expr}, dyn_acc ->
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
          warn_invalid_aggregate_params(helper_op, params)
          left_dynamic
        end

      {field, expr} ->
        append_aggregate_predicate(
          source,
          left_dynamic,
          binding_selector,
          helper_op,
          field,
          expr,
          opts
        )

      _ ->
        warn_invalid_aggregate_params(helper_op, params)
        left_dynamic
    end
  end

  defp append_aggregate_predicate(
         source,
         left_dynamic,
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
      append_schema_predicate(source, left_dynamic, binding_selector, {field, helper_expr}, opts)
    else
      append_field_predicate(source, left_dynamic, binding_selector, field, helper_expr, opts)
    end
  end

  # Reduces `entries` into a single boolean `dynamic/2` expression.
  #
  # Each entry is first converted into an independent predicate
  # (starting from `nil`), and then combined with the accumulator
  # using `boolean_operator` (`:and` or `:or`).
  #
  # Returns the combined `dynamic()` expression (or the first built
  # predicate when the accumulator is `nil`).
  #
  # ## Examples
  #
  #     # boolean_operator:  :or
  #     # input:    [left_dynamic, right_dynamic]
  #     # output:   left_dynamic or right_dynamic
  #
  #     # boolean_operator:  :and
  #     # input:    [left_dynamic, right_dynamic]
  #     # output:   left_dynamic and right_dynamic
  #
  # ## Nesting
  #
  # This reducer can build “outer” groups (across a list of entries),
  # and it can also participate in “inner” groups when entries contain
  # nested boolean structures (since entries are built via `append_param_predicates/5`,
  # which may call back into this reducer for nested `:and` / `:or`).
  defp merge_boolean_predicates(
         source,
         left_dynamic,
         binding_selector,
         boolean_operator,
         entries,
         opts
       ) do
    Enum.reduce(entries, left_dynamic, fn entry, dyn_acc ->
      entry =
        if is_map(entry) and not is_struct(entry) do
          Map.to_list(entry)
        else
          entry
        end

      right_dynamic =
        append_param_predicates(source, nil, binding_selector, entry, opts)

      merge_dynamic(dyn_acc, boolean_operator, right_dynamic)
    end)
  end

  defp append_schema_predicate(source, left_dynamic, binding_selector, {key, value}, opts) do
    schema_fields = CommonSchema.get_schema_reflection(source, :query_fields)

    if is_list(schema_fields) and key not in schema_fields do
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected a query field for schema #{inspect(source)}, got: #{inspect(key)}"
      )

      left_dynamic
    else
      append_schema_predicate_value(source, left_dynamic, binding_selector, key, value, opts)
    end
  end

  defp append_schema_predicate_value(source, left_dynamic, binding_selector, key, value, opts) do
    case value do
      map when is_map(map) and not is_struct(map) ->
        append_schema_predicate(
          source,
          left_dynamic,
          binding_selector,
          {key, Map.to_list(map)},
          opts
        )

      list when is_list(list) ->
        if Keyword.keyword?(list) do
          Enum.reduce(list, left_dynamic, fn entry, dyn_acc ->
            append_schema_predicate(source, dyn_acc, binding_selector, {key, entry}, opts)
          end)
        else
          append_field_predicate(source, left_dynamic, binding_selector, key, value, opts)
        end

      _ ->
        append_field_predicate(source, left_dynamic, binding_selector, key, value, opts)
    end
  end

  #
  # `{boolean_operator, values}` can mean two different things:
  #
  # 1) Same-field comparisons (apply to `key`):
  #
  #    %{published: %{or: %{==: true, ==: false}}}
  #    -> build predicates like: published == true OR published == false
  #
  # 2) Composite predicates (each entry is its own field map/keyword list):
  #
  #    %{id: %{or: [%{published: true, views: 20}, %{published: false, views: 10}]}}
  #    -> each list item is a full "AND group" across multiple fields
  #
  defp append_field_predicate(
         source,
         left_dynamic,
         binding_selector,
         key,
         {boolean_operator, values},
         opts
       )
       when boolean_operator in @boolean_operators and is_list(values) do
    entries =
      if composite_predicate_entries?(values) do
        values
      else
        Enum.map(values, &{key, &1})
      end

    right_dynamic =
      merge_boolean_predicates(
        source,
        nil,
        binding_selector,
        boolean_operator,
        entries,
        opts
      )

    merge_dynamic(left_dynamic, :and, right_dynamic)
  end

  defp append_field_predicate(
         source,
         left_dynamic,
         binding_selector,
         key,
         {:all, value},
         opts
       ) do
    value = apply_all_operator(source, key, value, opts)

    append_predicate_items(
      source,
      left_dynamic,
      binding_selector,
      key,
      value,
      opts,
      fn item -> {:all, item} end
    )
  end

  defp append_field_predicate(
         source,
         left_dynamic,
         binding_selector,
         key,
         {:not, {inner_key, value}},
         opts
       ) do
    case inner_key do
      :all ->
        value = apply_all_operator(source, key, value, opts)

        append_predicate_items(
          source,
          left_dynamic,
          binding_selector,
          key,
          value,
          opts,
          fn item -> {:not, {:all, item}} end
        )

      _ ->
        append_predicate_items(
          source,
          left_dynamic,
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
         left_dynamic,
         binding_selector,
         key,
         {:not, value},
         opts
       ) do
    case value do
      map when is_map(map) and not is_struct(map) ->
        Enum.reduce(Map.to_list(map), left_dynamic, fn entry, dyn_acc ->
          append_field_predicate(source, dyn_acc, binding_selector, key, {:not, entry}, opts)
        end)

      list when is_list(list) ->
        Enum.reduce(list, left_dynamic, fn entry, dyn_acc ->
          append_field_predicate(source, dyn_acc, binding_selector, key, {:not, entry}, opts)
        end)

      _ ->
        append_predicate_items(
          source,
          left_dynamic,
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
         left_dynamic,
         binding_selector,
         key,
         {op, value},
         opts
       ) do
    append_predicate_items(source, left_dynamic, binding_selector, key, value, opts, fn item ->
      {op, item}
    end)
  end

  defp append_field_predicate(source, left_dynamic, binding_selector, key, value, opts) do
    append_field_predicate(
      source,
      left_dynamic,
      binding_selector,
      key,
      {@equal, value},
      opts
    )
  end

  defp append_predicate_items(
         source,
         left_dynamic,
         binding_selector,
         key,
         value,
         opts,
         build_expr
       ) do
    dynamic_adapter = dynamic_adapter!(opts)

    value
    |> normalize_expression_params()
    |> Enum.reduce(left_dynamic, fn item, dyn_acc ->
      right_dynamic =
        dynamic_adapter.build_dynamic(source, binding_selector, key, build_expr.(item))

      merge_dynamic(dyn_acc, :and, right_dynamic)
    end)
  end

  defp apply_all_operator(source, key, value, opts)
       when is_map(value) and not is_struct(value) do
    apply_all_operator(source, key, Map.to_list(value), opts)
  end

  defp apply_all_operator(source, key, value, opts) when is_list(value) do
    if Keyword.keyword?(value) do
      if Keyword.has_key?(value, :source) or Keyword.has_key?(value, :query) do
        all_operator_query_from_payload(source, key, value, opts)
      else
        Enum.map(value, fn {inner_op, rhs} ->
          {inner_op, resolve_all_operator_rhs(source, key, rhs, opts)}
        end)
      end
    else
      value
    end
  end

  defp apply_all_operator(source, key, {inner_op, rhs}, opts) when is_atom(inner_op) do
    {inner_op, resolve_all_operator_rhs(source, key, rhs, opts)}
  end

  defp apply_all_operator(_source, _key, value, _opts), do: value

  defp resolve_all_operator_rhs(source, key, rhs, opts)
       when is_map(rhs) and not is_struct(rhs) do
    resolve_all_operator_rhs(source, key, Map.to_list(rhs), opts)
  end

  defp resolve_all_operator_rhs(source, key, rhs, opts) when is_list(rhs) do
    if Keyword.keyword?(rhs) and (Keyword.has_key?(rhs, :source) or Keyword.has_key?(rhs, :query)) do
      payload_source = Keyword.get(rhs, :source, source)
      filter_params = Keyword.get(rhs, :query, [])

      payload_source
      |> EctoShorts.CommonFilters.convert_params_to_filter(filter_params, opts)
      |> ensure_all_operator_scalar_select(key)
    else
      rhs
    end
  end

  defp resolve_all_operator_rhs(_source, _key, rhs, _opts), do: rhs

  defp all_operator_query_from_payload(source, key, payload, opts) do
    payload_source = Keyword.get(payload, :source, source)
    filter_params = Keyword.get(payload, :query, [])

    payload_source
    |> EctoShorts.CommonFilters.convert_params_to_filter(filter_params, opts)
    |> ensure_all_operator_scalar_select(key)
  end

  defp ensure_all_operator_scalar_select(query, field_name)
       when is_struct(query, Ecto.Query) and is_atom(field_name) and not is_nil(field_name) do
    case query.select do
      nil ->
        select(query, [q], field(q, ^field_name))

      _ ->
        query
    end
  end

  defp ensure_all_operator_scalar_select(query, _field_name), do: query

  defp append_operator_predicates(
         source,
         left_dynamic,
         binding_selector,
         key,
         value,
         dynamic_adapter
       ) do
    value
    |> normalize_expression_params()
    |> Enum.reduce(left_dynamic, fn item, dyn_acc ->
      right_dynamic = dynamic_adapter.build_dynamic(source, binding_selector, key, item)
      merge_dynamic(dyn_acc, :and, right_dynamic)
    end)
  end

  defp expression_operator?(key, dynamic_adapter) do
    key in Postgres.operators() or key in dynamic_adapter.operators()
  end

  defp warn_invalid_aggregate_params(helper_op, params) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected params for #{helper_op} to be a map or keyword list, got: #{inspect(params)}"
    )
  end

  defp merge_dynamic(nil, _, right_dynamic) do
    right_dynamic
  end

  defp merge_dynamic(left_dynamic, :and, right_dynamic) do
    dynamic([], ^left_dynamic and ^right_dynamic)
  end

  defp merge_dynamic(left_dynamic, :or, right_dynamic) do
    dynamic([], ^left_dynamic or ^right_dynamic)
  end

  defp composite_predicate_entries?(values) when is_list(values) do
    case values do
      [entry | _] -> (is_map(entry) and not is_struct(entry)) or Keyword.keyword?(entry)
      [] -> false
    end
  end

  defp source_has_schema?({_, schema}) when is_atom(schema) and not is_nil(schema), do: true
  defp source_has_schema?(_), do: false

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
        Enum.reduce(list, acc, fn entry, acc_inner ->
          flatten_expression_params(entry, acc_inner)
        end)

      _ ->
        if Keyword.keyword?(list) do
          Enum.reduce(list, acc, fn entry, acc_inner ->
            flatten_expression_params(entry, acc_inner)
          end)
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
        v
        |> normalize_expression_params()
        |> Enum.map(&{k, &1})
        |> flatten_expression_params(acc)

      _ ->
        if Keyword.keyword?(v) do
          v
          |> normalize_expression_params()
          |> Enum.map(&{k, &1})
          |> flatten_expression_params(acc)
        else
          [{k, v} | acc]
        end
    end
  end

  defp flatten_expression_params(v, acc) do
    [v | acc]
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
