defmodule EctoShorts.CommonFilters.OrderBy do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.Compiler

  require Ecto.Query
  require EctoShorts.Compiler

  @boolean_directives [:as, :at]

  @order_directions [
    :asc,
    :asc_nulls_last,
    :asc_nulls_first,
    :desc,
    :desc_nulls_last,
    :desc_nulls_first
  ]

  @doc false
  def build(_schema_source, filter_op, query, binding_selector, params, _opts)
      when filter_op in [:order_by, :prepend_order_by] do
    reduce_order_by(filter_op, query, binding_selector, params)
  end

  defp reduce_order_by(filter_op, query, binding_selector, {key, params})
       when is_map(params) and not is_struct(params) do
    reduce_order_by(filter_op, query, binding_selector, {key, Map.to_list(params)})
  end

  defp reduce_order_by(filter_op, query, binding_selector, {key, value}) do
    if key in @boolean_directives do
      Enum.reduce(value, query, fn {binding_target, next_value}, q ->
        reduce_order_by(filter_op, q, {key, binding_target}, next_value)
      end)
    else
      reduce_order_by_expr(filter_op, query, binding_selector, {key, value})
    end
  end

  defp reduce_order_by(filter_op, query, binding_selector, params)
       when is_map(params) and not is_struct(params) do
    reduce_order_by(filter_op, query, binding_selector, Map.to_list(params))
  end

  defp reduce_order_by(filter_op, query, binding_selector, entries) when is_list(entries) do
    if Keyword.keyword?(entries) do
      case Enum.split_with(entries, fn {k, _} -> k in @boolean_directives end) do
        {[], order_entries} ->
          reduce_order_by_expr(filter_op, query, binding_selector, order_entries)

        {boolean_directives, []} ->
          Enum.reduce(boolean_directives, query, fn entry, query_acc ->
            reduce_order_by(filter_op, query_acc, binding_selector, entry)
          end)

        {boolean_directives, order_entries} ->
          query_with_order =
            reduce_order_by_expr(filter_op, query, binding_selector, order_entries)

          Enum.reduce(boolean_directives, query_with_order, fn entry, query_acc ->
            reduce_order_by(filter_op, query_acc, binding_selector, entry)
          end)
      end
    else
      reduce_order_by_expr(filter_op, query, binding_selector, entries)
    end
  end

  defp reduce_order_by(filter_op, query, binding_selector, expr) do
    reduce_order_by_expr(filter_op, query, binding_selector, expr)
  end

  defp reduce_order_by_expr(:order_by, query, binding_selector, expr) do
    apply_order_by_expr(query, binding_selector, expr)
  end

  defp reduce_order_by_expr(:prepend_order_by, query, binding_selector, expr) do
    apply_prepend_order_by_expr(query, binding_selector, expr)
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      defp apply_order_by_expr(query, unquote(quoted_binding_head), field_name)
           when is_atom(field_name) do
        Query.order_by(
          query,
          [unquote_splicing(quoted_binding_body)],
          desc: field(unquote(target_binding_var), ^field_name)
        )
      end

      defp apply_order_by_expr(query, unquote(quoted_binding_head), {dir, field_name})
           when dir in @order_directions and is_atom(field_name) do
        Query.order_by(
          query,
          [unquote_splicing(quoted_binding_body)],
          [{^dir, field(unquote(target_binding_var), ^field_name)}]
        )
      end

      defp apply_order_by_expr(query, unquote(quoted_binding_head), entries)
           when is_list(entries) do
        order_exprs =
          Enum.map(entries, fn
            {dir, field_name} when dir in @order_directions and is_atom(field_name) ->
              {dir,
               Query.dynamic(
                 [unquote_splicing(quoted_binding_body)],
                 field(unquote(target_binding_var), ^field_name)
               )}

            field_name when is_atom(field_name) ->
              {:desc,
               Query.dynamic(
                 [unquote_splicing(quoted_binding_body)],
                 field(unquote(target_binding_var), ^field_name)
               )}

            %Ecto.Query.DynamicExpr{} = dynamic_expr ->
              dynamic_expr

            other ->
              other
          end)

        Query.order_by(query, ^order_exprs)
      end

      defp apply_prepend_order_by_expr(query, unquote(quoted_binding_head), field_name)
           when is_atom(field_name) do
        Query.prepend_order_by(
          query,
          [unquote_splicing(quoted_binding_body)],
          desc: field(unquote(target_binding_var), ^field_name)
        )
      end

      defp apply_prepend_order_by_expr(query, unquote(quoted_binding_head), {dir, field_name})
           when dir in @order_directions and is_atom(field_name) do
        Query.prepend_order_by(
          query,
          [unquote_splicing(quoted_binding_body)],
          [{^dir, field(unquote(target_binding_var), ^field_name)}]
        )
      end

      defp apply_prepend_order_by_expr(query, unquote(quoted_binding_head), entries)
           when is_list(entries) do
        order_exprs =
          Enum.map(entries, fn
            {dir, field_name} when dir in @order_directions and is_atom(field_name) ->
              {dir,
               Query.dynamic(
                 [unquote_splicing(quoted_binding_body)],
                 field(unquote(target_binding_var), ^field_name)
               )}

            field_name when is_atom(field_name) ->
              {:desc,
               Query.dynamic(
                 [unquote_splicing(quoted_binding_body)],
                 field(unquote(target_binding_var), ^field_name)
               )}

            %Ecto.Query.DynamicExpr{} = dynamic_expr ->
              dynamic_expr

            other ->
              other
          end)

        Query.prepend_order_by(query, ^order_exprs)
      end
  end

  defp apply_order_by_expr(query, _binding_selector, expr) do
    Query.order_by(query, ^expr)
  end

  defp apply_prepend_order_by_expr(query, _binding_selector, expr) do
    Query.prepend_order_by(query, ^expr)
  end
end
