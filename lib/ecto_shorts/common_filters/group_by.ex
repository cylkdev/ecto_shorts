defmodule EctoShorts.CommonFilters.GroupBy do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.Compiler

  require Ecto.Query
  require EctoShorts.Compiler

  @boolean_operators [:as, :at]

  @doc false
  def build(_schema_source, :group_by, query, binding_selector, params, _opts) do
    reduce_group_by(query, binding_selector, params)
  end

  defp reduce_group_by(query, binding_selector, {key, params})
       when is_map(params) and not is_struct(params) do
    reduce_group_by(query, binding_selector, {key, Map.to_list(params)})
  end

  defp reduce_group_by(query, binding_selector, {key, value}) do
    if key in @boolean_operators do
      Enum.reduce(value, query, fn {binding_target, next_value}, q ->
        reduce_group_by(q, {key, binding_target}, next_value)
      end)
    else
      apply_group_by_expr(query, binding_selector, {key, value})
    end
  end

  defp reduce_group_by(query, binding_selector, params)
       when is_map(params) and not is_struct(params) do
    reduce_group_by(query, binding_selector, Map.to_list(params))
  end

  defp reduce_group_by(query, binding_selector, entries) when is_list(entries) do
    if Keyword.keyword?(entries) do
      Enum.reduce(entries, query, fn {key, value}, q ->
        reduce_group_by(q, binding_selector, {key, value})
      end)
    else
      apply_group_by_expr(query, binding_selector, entries)
    end
  end

  defp reduce_group_by(query, binding_selector, expr) do
    apply_group_by_expr(query, binding_selector, expr)
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      defp apply_group_by_expr(query, unquote(quoted_binding_head), field_name)
           when is_atom(field_name) do
        Query.group_by(
          query,
          [unquote_splicing(quoted_binding_body)],
          field(unquote(target_binding_var), ^field_name)
        )
      end

      defp apply_group_by_expr(query, unquote(quoted_binding_head), entries)
           when is_list(entries) do
        group_by_exprs =
          Enum.map(entries, fn
            field_name when is_atom(field_name) ->
              Query.dynamic(
                [unquote_splicing(quoted_binding_body)],
                field(unquote(target_binding_var), ^field_name)
              )

            %Ecto.Query.DynamicExpr{} = dynamic_expr ->
              dynamic_expr

            other ->
              other
          end)

        Query.group_by(query, ^group_by_exprs)
      end
  end

  defp apply_group_by_expr(query, _binding_selector, expr) do
    Query.group_by(query, ^expr)
  end
end
