defmodule EctoShorts.Dynamics do
  @moduledoc false

  alias EctoShorts.CommonSchema
  alias EctoShorts.Config
  alias EctoShorts.Dynamics.Adapters.Postgres

  import Ecto.Query, only: [dynamic: 2]

  @logger_prefix "EctoShorts.Dynamics"

  @equal :==
  @boolean_operators [:and, :or]

  def convert_to_dynamic(source, binding_selector, params, opts \\ []) do
    source = CommonSchema.normalize_source(source)

    if is_map(params) or is_list(params) do
      Enum.reduce(params, nil, fn entry, dyn_acc ->
        reduce_dynamic_params(source, dyn_acc, binding_selector, entry, opts)
      end)
    else
      reduce_dynamic_params(source, nil, binding_selector, params, opts)
    end
  end

  defp reduce_dynamic_params(source, left_dynamic, binding_selector, params, opts)
       when is_map(params) or is_list(params) do
    Enum.reduce(params, left_dynamic, fn entry, dyn_acc ->
      reduce_dynamic_params(source, dyn_acc, binding_selector, entry, opts)
    end)
  end

  defp reduce_dynamic_params(source, left_dynamic, binding_selector, {key, value}, opts)
       when key in @boolean_operators do
    if is_map(value) and not is_struct(value) do
      reduce_dynamic_params(
        source,
        left_dynamic,
        binding_selector,
        {key, Map.to_list(value)},
        opts
      )
    else
      reduce_merge_dynamic_predicates(
        source,
        left_dynamic,
        binding_selector,
        key,
        value,
        opts
      )
    end
  end

  defp reduce_dynamic_params(source, left_dynamic, binding_selector, {key, value}, opts) do
    if key in Postgres.operators() do
      dynamic_adapter = dynamic_adapter!(opts)

      value
      |> normalize_expression_params()
      |> Enum.reduce(left_dynamic, fn item, dyn_acc ->
        right_dynamic =
          dynamic_adapter.build_dynamic(source, binding_selector, key, item)

        merge_dynamic(dyn_acc, :and, right_dynamic)
      end)
    else
      dynamic_adapter = dynamic_adapter!(opts)

      if key in dynamic_adapter.operators() do
        value
        |> normalize_expression_params()
        |> Enum.reduce(left_dynamic, fn item, dyn_acc ->
          right_dynamic =
            dynamic_adapter.build_dynamic(source, binding_selector, key, item)

          merge_dynamic(dyn_acc, :and, right_dynamic)
        end)
      else
        if source_has_schema?(source) do
          build_schema_dynamic(source, left_dynamic, binding_selector, {key, value}, opts)
        else
          value
          |> normalize_expression_params()
          |> Enum.reduce(left_dynamic, fn item, dyn_acc ->
            right_dynamic =
              dynamic_adapter.build_dynamic(source, binding_selector, key, item)

            merge_dynamic(dyn_acc, :and, right_dynamic)
          end)
        end
      end
    end
  end

  # Reduces `entries` into a single boolean `dynamic/2` expression.
  #
  # Each entry is first converted into an independent predicate
  # (starting from `nil`), and then combined with the accumulator
  # using `bool_op` (`:and` or `:or`).
  #
  # Returns the combined `dynamic()` expression (or the first built
  # predicate when the accumulator is `nil`).
  #
  # ## Examples
  #
  #     # bool_op:  :or
  #     # input:    [left_dynamic, right_dynamic]
  #     # output:   left_dynamic or right_dynamic
  #
  #     # bool_op:  :and
  #     # input:    [left_dynamic, right_dynamic]
  #     # output:   left_dynamic and right_dynamic
  #
  # ## Nesting
  #
  # This reducer can build “outer” groups (across a list of entries),
  # and it can also participate in “inner” groups when entries contain
  # nested boolean structures (since entries are built via `reduce_dynamic_params/5`,
  # which may call back into this reducer for nested `:and` / `:or`).
  defp reduce_merge_dynamic_predicates(
         source,
         left_dynamic,
         binding_selector,
         bool_op,
         entries,
         opts
       ) do
    Enum.reduce(entries, left_dynamic, fn entry, dyn_acc ->
      right_dynamic =
        cond do
          is_map(entry) and not is_struct(entry) ->
            reduce_dynamic_params(
              source,
              nil,
              binding_selector,
              Map.to_list(entry),
              opts
            )

          Keyword.keyword?(entry) ->
            reduce_dynamic_params(source, nil, binding_selector, entry, opts)

          true ->
            reduce_dynamic_params(source, nil, binding_selector, entry, opts)
        end

      merge_dynamic(dyn_acc, bool_op, right_dynamic)
    end)
  end

  defp build_schema_dynamic(source, left_dynamic, binding_selector, {key, value}, opts) do
    schema_fields = CommonSchema.get_schema_reflection(source, :query_fields)

    if is_list(schema_fields) and key not in schema_fields do
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected a query field for schema #{inspect(source)}, got: #{inspect(key)}"
      )

      left_dynamic
    else
      build_schema_dynamic_value(source, left_dynamic, binding_selector, key, value, opts)
    end
  end

  defp build_schema_dynamic_value(source, left_dynamic, binding_selector, key, value, opts)
       when is_map(value) and not is_struct(value) do
    build_schema_dynamic(
      source,
      left_dynamic,
      binding_selector,
      {key, Map.to_list(value)},
      opts
    )
  end

  defp build_schema_dynamic_value(source, left_dynamic, binding_selector, key, value, opts)
       when is_list(value) do
    if Keyword.keyword?(value) do
      Enum.reduce(value, left_dynamic, fn entry, dyn_acc ->
        build_schema_dynamic(source, dyn_acc, binding_selector, {key, entry}, opts)
      end)
    else
      reduce_dynamic_expr(source, left_dynamic, binding_selector, key, value, opts)
    end
  end

  defp build_schema_dynamic_value(source, left_dynamic, binding_selector, key, value, opts) do
    reduce_dynamic_expr(source, left_dynamic, binding_selector, key, value, opts)
  end

  #
  # `{bool_op, values}` can mean two different things:
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
  defp reduce_dynamic_expr(
         source,
         left_dynamic,
         binding_selector,
         key,
         {bool_op, values},
         opts
       )
       when bool_op in @boolean_operators and is_list(values) do
    entries =
      if composite_predicate_entries?(values) do
        values
      else
        Enum.map(values, &{key, &1})
      end

    right_dynamic =
      reduce_merge_dynamic_predicates(
        source,
        nil,
        binding_selector,
        bool_op,
        entries,
        opts
      )

    merge_dynamic(left_dynamic, :and, right_dynamic)
  end

  defp reduce_dynamic_expr(
         source,
         left_dynamic,
         binding_selector,
         key,
         {op, value},
         opts
       ) do
    dynamic_adapter = dynamic_adapter!(opts)

    value
    |> normalize_expression_params()
    |> Enum.reduce(left_dynamic, fn item, dyn_acc ->
      right_dynamic =
        dynamic_adapter.build_dynamic(source, binding_selector, key, {op, item})

      merge_dynamic(dyn_acc, :and, right_dynamic)
    end)
  end

  defp reduce_dynamic_expr(source, left_dynamic, binding_selector, key, value, opts) do
    reduce_dynamic_expr(
      source,
      left_dynamic,
      binding_selector,
      key,
      {@equal, value},
      opts
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
    |> reduce_expression_params([])
    |> Enum.reverse()
  end

  defp reduce_expression_params(term, acc) when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> reduce_expression_params(acc)
  end

  defp reduce_expression_params([], acc), do: acc

  defp reduce_expression_params(list, acc) when is_list(list) do
    if flatten?(list) do
      Enum.reduce(list, acc, fn entry, acc_inner ->
        reduce_expression_params(entry, acc_inner)
      end)
    else
      [list | acc]
    end
  end

  defp reduce_expression_params({k, v}, acc) when is_map(v) and not is_struct(v) do
    reduce_expression_params({k, Map.to_list(v)}, acc)
  end

  defp reduce_expression_params({k, v}, acc) when is_list(v) do
    if flatten?(v) do
      v
      |> normalize_expression_params()
      |> Enum.map(&{k, &1})
      |> reduce_expression_params(acc)
    else
      [{k, v} | acc]
    end
  end

  defp reduce_expression_params(v, acc) do
    [v | acc]
  end

  defp flatten?(list) do
    Keyword.keyword?(list) or Enum.any?(list, &is_map/1)
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
