defmodule EctoShorts.CommonFilters.OrderBy do
  @moduledoc """
  Builds `:order_by` and `:prepend_order_by` expressions from data-driven params.

  Accepts single field atoms, `{direction, field}` tuples, lists of either,
  and dynamic expressions. Supports binding-scoped params via the `:bind` key.
  """

  alias Ecto.Query
  alias EctoShorts.CommonFilters.BindParams
  alias EctoShorts.Compiler

  require Ecto.Query
  require EctoShorts.Compiler

  @binding_selector_key :bind

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

  defp reduce_order_by(filter_op, query, binding_selector, {@binding_selector_key, bind_params}) do
    reduce_order_by_bind(filter_op, query, binding_selector, bind_params)
  end

  defp reduce_order_by(filter_op, query, binding_selector, {key, value}) do
    reduce_order_by_expr(filter_op, query, binding_selector, {key, value})
  end

  defp reduce_order_by(filter_op, query, binding_selector, params)
       when is_map(params) and not is_struct(params) do
    reduce_order_by(filter_op, query, binding_selector, Map.to_list(params))
  end

  defp reduce_order_by(filter_op, query, binding_selector, entries) when is_list(entries) do
    if Keyword.keyword?(entries) do
      case Enum.split_with(entries, fn {k, _} -> k === @binding_selector_key end) do
        {[], order_entries} ->
          reduce_order_by_expr(filter_op, query, binding_selector, order_entries)

        {bind_entries, []} ->
          Enum.reduce(bind_entries, query, fn entry, query_acc ->
            reduce_order_by(filter_op, query_acc, binding_selector, entry)
          end)

        {bind_entries, order_entries} ->
          query_with_order =
            reduce_order_by_expr(filter_op, query, binding_selector, order_entries)

          Enum.reduce(bind_entries, query_with_order, fn entry, query_acc ->
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

  defp reduce_order_by_bind(filter_op, query, _binding_selector, bind_params) do
    BindParams.reduce_submodule_bind_params(query, bind_params, fn q, {mode, target}, value ->
      reduce_order_by(filter_op, q, {mode, target}, value)
    end)
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
