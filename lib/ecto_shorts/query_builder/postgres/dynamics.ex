defmodule EctoShorts.QueryBuilder.Postgres.Dynamics do
  @moduledoc false

  alias EctoShorts.CommonSchema
  alias EctoShorts.QueryBuilder.ExprBuilder

  alias EctoShorts.QueryBuilder.Postgres.Dynamics.{
    ArrayExpr,
    CommonExpr,
    ScalarExpr
  }

  import Ecto.Query, only: [dynamic: 2]

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

  def build_dynamic(source, binding_selector, args) do
    source = CommonSchema.normalize_source(source)

    if is_map(args) or is_list(args) do
      Enum.reduce(args, nil, fn entry, dyn_acc ->
        reduce_dynamic(source, dyn_acc, binding_selector, entry)
      end)
    else
      reduce_dynamic(source, nil, binding_selector, args)
    end
  end

  defp reduce_dynamic(source, dyn_l, binding_selector, params)
       when is_map(params) or is_list(params) do
    Enum.reduce(params, dyn_l, fn entry, dyn_acc ->
      reduce_dynamic(source, dyn_acc, binding_selector, entry)
    end)
  end

  defp reduce_dynamic(_source, dyn_l, binding_selector, {key, value})
       when key in @common_operators do
    value
    |> ExprBuilder.expand()
    |> Enum.reduce(dyn_l, fn item, dyn_acc ->
      dyn_r = CommonExpr.dynamic_field_expr(binding_selector, key, item)
      merge_dynamic(dyn_acc, :and, dyn_r)
    end)
  end

  defp reduce_dynamic(source, dyn_l, binding_selector, {key, value})
       when key in @boolean_operators do
    if is_map(value) do
      reduce_dynamic(source, dyn_l, binding_selector, {key, Map.to_list(value)})
    else
      reduce_merge_dynamic(source, dyn_l, binding_selector, key, value)
    end
  end

  defp reduce_dynamic(source, dyn_l, binding_selector, {key, value}) do
    if source_has_schema?(source) do
      build_schema_dynamic(source, dyn_l, binding_selector, {key, value})
    else
      value
      |> ExprBuilder.expand()
      |> Enum.reduce(dyn_l, fn item, dyn_acc ->
        dyn_r = ScalarExpr.dynamic_field_expr(binding_selector, key, item)
        merge_dynamic(dyn_acc, :and, dyn_r)
      end)
    end
  end

  defp reduce_merge_dynamic(source, dyn_l, binding_selector, bool_op, entries) do
    Enum.reduce(entries, dyn_l, fn entry, dyn_acc ->
      dyn_r =
        cond do
          is_map(entry) -> reduce_dynamic(source, nil, binding_selector, Map.to_list(entry))
          Keyword.keyword?(entry) -> reduce_dynamic(source, nil, binding_selector, entry)
          true -> reduce_dynamic(source, nil, binding_selector, entry)
        end

      merge_dynamic(dyn_acc, bool_op, dyn_r)
    end)
  end

  defp build_schema_dynamic(source, dyn_l, binding_selector, {key, value}) do
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
          apply_schema_expr(source, dyn_l, binding_selector, key, value)
        end

      true ->
        apply_schema_expr(source, dyn_l, binding_selector, key, value)
    end
  end

  defp apply_schema_expr(source, dyn_l, binding_selector, key, {op, value}) do
    value
    |> ExprBuilder.expand()
    |> Enum.reduce(dyn_l, fn item, dyn_acc ->
      dyn_r =
        if array_type?(source, key) do
          ArrayExpr.dynamic_field_expr(binding_selector, key, {op, item})
        else
          ScalarExpr.dynamic_field_expr(binding_selector, key, {op, item})
        end

      merge_dynamic(dyn_acc, :and, dyn_r)
    end)
  end

  defp apply_schema_expr(source, dyn_l, binding_selector, key, value) do
    apply_schema_expr(source, dyn_l, binding_selector, key, {@equal, value})
  end

  defp array_type?(source, key) do
    case CommonSchema.get_schema_reflection(source, :type, key) do
      {:array, _} -> true
      _ -> false
    end
  end

  defp source_has_schema?({_, schema}) when is_atom(schema) and not is_nil(schema), do: true
  defp source_has_schema?(_), do: false
end

# defmodule EctoShorts.QueryBuilder.Postgres.Dynamics do
#   @moduledoc false
#   alias Ecto.Query

#   alias EctoShorts.QueryBuilder.Postgres.Dynamics.{
#     ArrayExpr,
#     CommonExpr,
#     ScalarExpr
#   }

#   require Ecto.Query

#   @equal :==
#   @common_operators [:ids, :before, :after, :start_date, :end_date]
#   @boolean_operators [:and, :or]

#   # Shared "params -> dynamic" compiler.
#   #
#   # This function is intentionally generic so it can be used by joins (for :on)
#   # and by any other query builder component that needs a dynamic expression.
#   #
#   # NOTE: This stays within the "dynamic-only" boundary: it does not add joins,
#   # selects, ordering, etc.
#   def convert_params_to_dynamic(schema, dynamic, binding_selector, params) do
#     reduce_dynamic(schema, dynamic, binding_selector, params)
#   end

#   defp reduce_dynamic(schema, dynamic, binding_selector, {bool_op, values})
#        when bool_op in @boolean_operators and is_list(values) do
#     right_dynamic =
#       reduce_boolean(values, bool_op, fn value ->
#         reduce_dynamic(schema, nil, binding_selector, value)
#       end)

#     merge_dynamic(dynamic, :and, right_dynamic)
#   end

#   defp reduce_dynamic(schema, dynamic, binding_selector, {key, value}) do
#     right_dynamic = dynamic_field_expr(schema, binding_selector, key, value)
#     merge_dynamic(dynamic, :and, right_dynamic)
#   end

#   defp reduce_dynamic(schema, dynamic, binding_selector, enum) when is_map(enum) do
#     Enum.reduce(enum, dynamic, fn pair, dyn ->
#       reduce_dynamic(schema, dyn, binding_selector, pair)
#     end)
#   end

#   defp reduce_dynamic(schema, dynamic, binding_selector, enum) when is_list(enum) do
#     Enum.reduce(enum, dynamic, fn value, dyn ->
#       reduce_dynamic(schema, dyn, binding_selector, value)
#     end)
#   end

#   defp reduce_dynamic(_schema, _dynamic, _binding_selector, term) do
#     raise ArgumentError,
#           "Expected dynamic params to be a map, keyword list, or boolean expression, got: #{inspect(term)}"
#   end

#   @doc """
#   Convert a field key and value to a dynamic expression.
#   """
#   def dynamic_field_expr(schema, binding_selector, key, value) do
#     IO.inspect(binding())

#     cond do
#       key in @common_operators ->
#         CommonExpr.dynamic_field_expr(binding_selector, key, value)

#       map_or_kw?(value) ->
#         # Support nested boolean ops at the field level, e.g.
#         # %{published: %{or: [==: true, ==: false]}}
#         Enum.reduce(value, nil, fn {operator, value}, dyn ->
#           right_dynamic =
#             if operator in @boolean_operators and is_list(value) do
#               boolean_dynamic(schema, binding_selector, key, {operator, value})
#             else
#               build_dynamic(schema, binding_selector, key, {operator, value})
#             end

#           merge_dynamic(dyn, :and, right_dynamic)
#         end)

#       true ->
#         {operator, value} = normalize_operator_value(value)

#         build_dynamic(schema, binding_selector, key, {operator, value})
#     end
#   end

#   defp boolean_dynamic(schema, binding_selector, key, {bool_op, values})
#        when bool_op in @boolean_operators and is_list(values) do
#     reduce_boolean(values, bool_op, fn value ->
#       if composite?(values) do
#         # values are maps/kw: each element is a composite branch
#         reduce_dynamic(schema, nil, binding_selector, value)
#       else
#         # values are operator tuples: e.g. [==: true, ==: false]
#         build_dynamic(schema, binding_selector, key, value)
#       end
#     end)
#   end

#   defp build_dynamic(schema, binding_selector, key, {operator, value}) do
#     case schema_field_type(schema, key) do
#       {:array, _} -> ArrayExpr.dynamic_field_expr(binding_selector, key, {operator, value})
#       _ -> ScalarExpr.dynamic_field_expr(binding_selector, key, {operator, value})
#     end
#   end

#   defp schema_field_type(nil, _key), do: nil

#   defp schema_field_type(schema, key) when is_atom(schema) do
#     schema.__schema__(:type, key)
#   end

#   defp schema_field_type(_, _) do
#     nil
#   end

#   defp reduce_boolean(values, bool_op, fun) when is_list(values) and is_function(fun, 1) do
#     Enum.reduce(values, nil, fn value, dyn ->
#       right_dynamic = fun.(value)
#       merge_dynamic(dyn, bool_op, right_dynamic)
#     end)
#   end

#   defp merge_dynamic(nil, _, right_dynamic), do: right_dynamic

#   defp merge_dynamic(dyn, :and, right_dynamic),
#     do: Query.dynamic([], ^dyn and ^right_dynamic)

#   defp merge_dynamic(dyn, :or, right_dynamic),
#     do: Query.dynamic([], ^dyn or ^right_dynamic)

#   defp composite?(values) when is_list(values) do
#     case values do
#       [v] -> is_map(v) or Keyword.keyword?(v)
#       [v | _] -> is_map(v) or Keyword.keyword?(v)
#       _ -> false
#     end
#   end

#   defp map_or_kw?(term) do
#     (is_map(term) and not is_struct(term)) or Keyword.keyword?(term)
#   end

#   defp normalize_operator_value(term) do
#     case term do
#       {nil, value} -> {@equal, value}
#       {op, value} -> {op, value}
#       value -> {@equal, value}
#     end
#   end
# end
