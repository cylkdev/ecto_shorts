defmodule EctoShorts.QueryBuilders.Postgres.Dynamics do
  @moduledoc false

  alias EctoShorts.CommonSchema
  alias EctoShorts.QueryBuilder.ParamPreprocessor

  alias EctoShorts.QueryBuilders.Postgres.Dynamics.{
    ArrayExpr,
    CommonExpr,
    ScalarExpr
  }

  import Ecto.Query, only: [dynamic: 2]

  @logger_prefix "EctoShorts.QueryBuilders.Postgres.Dynamics"

  @equal :==
  @common_operators [:ids, :before, :after, :start_date, :end_date]
  @boolean_operators [:and, :or]

  def merge_dynamic(nil, _, dyn_right) do
    dyn_right
  end

  def merge_dynamic(dyn_l, :and, dyn_r) do
    dynamic([], ^dyn_l and ^dyn_r)
  end

  def merge_dynamic(dyn_l, :or, dyn_r) do
    dynamic([], ^dyn_l or ^dyn_r)
  end

  def convert_to_dynamic(source, binding_selector, args) do
    source = CommonSchema.normalize_source(source)

    if is_map(args) or is_list(args) do
      reduce_params(source, binding_selector, args, nil)
    else
      reduce_dynamic_expr(source, nil, binding_selector, args)
    end
  end

  defp reduce_params(source, binding_selector, args, dyn_l) do
    Enum.reduce(args, dyn_l, fn entry, dyn_acc ->
      reduce_dynamic_expr(source, dyn_acc, binding_selector, entry)
    end)
  end

  defp reduce_dynamic_expr(source, dyn_l, binding_selector, args)
       when is_map(args) or is_list(args) do
    reduce_params(source, binding_selector, args, dyn_l)
  end

  defp reduce_dynamic_expr(_source, dyn_l, binding_selector, {key, value})
       when key in @common_operators do
    value
    |> ParamPreprocessor.normalize_params()
    |> Enum.reduce(dyn_l, fn item, dyn_acc ->
      dyn_r = CommonExpr.dynamic_field_expr(binding_selector, key, item)
      merge_dynamic(dyn_acc, :and, dyn_r)
    end)
  end

  defp reduce_dynamic_expr(source, dyn_l, binding_selector, {key, value})
       when key in @boolean_operators do
    if is_map(value) do
      reduce_dynamic_expr(source, dyn_l, binding_selector, {key, Map.to_list(value)})
    else
      reduce_merge_dynamic_predicates(source, dyn_l, binding_selector, key, value)
    end
  end

  defp reduce_dynamic_expr(source, dyn_l, binding_selector, {key, value}) do
    if source_has_schema?(source) do
      build_schema_dynamic(source, dyn_l, binding_selector, {key, value})
    else
      value
      |> ParamPreprocessor.normalize_params()
      |> Enum.reduce(dyn_l, fn item, dyn_acc ->
        dyn_r = ScalarExpr.dynamic_field_expr(binding_selector, key, item)
        merge_dynamic(dyn_acc, :and, dyn_r)
      end)
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
  #     # input:    [dyn1, dyn2]
  #     # output:   dyn1 or dyn2
  #
  #     # bool_op:  :and
  #     # input:    [dyn1, dyn2]
  #     # output:   dyn1 and dyn2
  #
  # ## Nesting
  #
  # This reducer can build “outer” groups (across a list of entries),
  # and it can also participate in “inner” groups when entries contain
  # nested boolean structures (since entries are built via `reduce_dynamic_expr/4`,
  # which may call back into this reducer for nested `:and` / `:or`).
  defp reduce_merge_dynamic_predicates(source, dyn_l, binding_selector, bool_op, entries) do
    Enum.reduce(entries, dyn_l, fn entry, dyn_acc ->
      dyn_r =
        cond do
          is_map(entry) -> reduce_dynamic_expr(source, nil, binding_selector, Map.to_list(entry))
          Keyword.keyword?(entry) -> reduce_dynamic_expr(source, nil, binding_selector, entry)
          true -> reduce_dynamic_expr(source, nil, binding_selector, entry)
        end

      merge_dynamic(dyn_acc, bool_op, dyn_r)
    end)
  end

  defp build_schema_dynamic(source, dyn_l, binding_selector, {key, value}) do
    schema_fields = CommonSchema.get_schema_reflection(source, :query_fields)

    if is_list(schema_fields) and key not in schema_fields do
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected a query field for schema #{inspect(source)}, got: #{inspect(key)}"
      )

      dyn_l
    else
      cond do
        is_map(value) ->
          build_schema_dynamic(
            source,
            dyn_l,
            binding_selector,
            {key, Map.to_list(value)}
          )

        is_list(value) ->
          if Keyword.keyword?(value) do
            Enum.reduce(value, dyn_l, fn entry, dyn_acc ->
              build_schema_dynamic(source, dyn_acc, binding_selector, {key, entry})
            end)
          else
            build_dynamic_field_expr(source, dyn_l, binding_selector, key, value)
          end

        true ->
          build_dynamic_field_expr(source, dyn_l, binding_selector, key, value)
      end
    end
  end

  # NOTE: `{bool_op, values}` can mean two different things:
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
  # If we always wrap entries as `{key, entry}` (ex: `{id, %{published: true}}`),
  # the inner field tuples (`{:published, true}`) can accidentally get treated as
  # values for `:id`, which will likely crash in ScalarExpr/ArrayExpr due to there
  # being no matching clause.
  #
  # So: for composite entries we pass `values` through unchanged and let the
  # existing reducer recurse into each field; for same-field comparisons we
  # wrap them as `{key, comparison}`.
  defp build_dynamic_field_expr(source, dyn_l, binding_selector, key, {bool_op, values})
       when bool_op in @boolean_operators and is_list(values) do
    entries =
      if composite_predicate_entries?(values) do
        values
      else
        Enum.map(values, &{key, &1})
      end

    dyn_r =
      reduce_merge_dynamic_predicates(
        source,
        nil,
        binding_selector,
        bool_op,
        entries
      )

    merge_dynamic(dyn_l, :and, dyn_r)
  end

  defp build_dynamic_field_expr(source, dyn_l, binding_selector, key, {op, value}) do
    value
    |> ParamPreprocessor.normalize_params()
    |> Enum.reduce(dyn_l, fn item, dyn_acc ->
      dyn_r =
        if field_type_of_array?(source, key) do
          ArrayExpr.dynamic_field_expr(binding_selector, key, {op, item})
        else
          ScalarExpr.dynamic_field_expr(binding_selector, key, {op, item})
        end

      merge_dynamic(dyn_acc, :and, dyn_r)
    end)
  end

  defp build_dynamic_field_expr(source, dyn_l, binding_selector, key, value) do
    build_dynamic_field_expr(source, dyn_l, binding_selector, key, {@equal, value})
  end

  defp field_type_of_array?(source, key) do
    case CommonSchema.get_schema_reflection(source, :type, key) do
      {:array, _} -> true
      _ -> false
    end
  end

  defp composite_predicate_entries?(values) when is_list(values) do
    case values do
      [entry | _] -> (is_map(entry) and not is_struct(entry)) or Keyword.keyword?(entry)
      [] -> false
    end
  end

  defp source_has_schema?({_, schema}) when is_atom(schema) and not is_nil(schema), do: true
  defp source_has_schema?(_), do: false
end
