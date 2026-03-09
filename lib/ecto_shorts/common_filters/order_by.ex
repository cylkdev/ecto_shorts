# defmodule EctoShorts.CommonFilters.OrderBy do
#   @moduledoc since: "3.0.0"
#   @moduledoc """
#   Builds `:order_by` and `:prepend_order_by` expressions from data-driven params.

#   Accepts single field atoms, `{direction, field}` tuples, lists of either,
#   and dynamic expressions. Supports binding-scoped params via the `:bind` key.
#   """

#   alias Ecto.Query
#   alias EctoShorts.CommonFilters.BindingParams
#   alias EctoShorts.Generator

#   require Ecto.Query
#   require EctoShorts.Generator

#   @selected_binding_key :bind

#   @order_directions [
#     :asc,
#     :asc_nulls_last,
#     :asc_nulls_first,
#     :desc,
#     :desc_nulls_last,
#     :desc_nulls_first
#   ]

#   @doc false
#   def build(_schema_source, filter_op, query, selected_binding, params, _opts)
#       when filter_op in [:order_by, :prepend_order_by] do
#     reduce_order_by(filter_op, query, selected_binding, params)
#   end

#   defp reduce_order_by(filter_op, query, selected_binding, {key, params})
#        when is_map(params) and not is_struct(params) do
#     reduce_order_by(filter_op, query, selected_binding, {key, Map.to_list(params)})
#   end

#   defp reduce_order_by(filter_op, query, selected_binding, {@selected_binding_key, bind_params}) do
#     reduce_order_by_bind(filter_op, query, selected_binding, bind_params)
#   end

#   defp reduce_order_by(filter_op, query, selected_binding, {key, value}) do
#     reduce_order_by_expr(filter_op, query, selected_binding, {key, value})
#   end

#   defp reduce_order_by(filter_op, query, selected_binding, params)
#        when is_map(params) and not is_struct(params) do
#     reduce_order_by(filter_op, query, selected_binding, Map.to_list(params))
#   end

#   defp reduce_order_by(filter_op, query, selected_binding, entries) when is_list(entries) do
#     if Keyword.keyword?(entries) do
#       {bind_entries, order_entries} =
#         Enum.split_with(entries, fn {k, _} -> k === @selected_binding_key end)

#       query =
#         if order_entries !== [],
#           do: reduce_order_by_expr(filter_op, query, selected_binding, order_entries),
#           else: query

#       Enum.reduce(bind_entries, query, fn entry, query_acc ->
#         reduce_order_by(filter_op, query_acc, selected_binding, entry)
#       end)
#     else
#       reduce_order_by_expr(filter_op, query, selected_binding, entries)
#     end
#   end

#   defp reduce_order_by(filter_op, query, selected_binding, expr) do
#     reduce_order_by_expr(filter_op, query, selected_binding, expr)
#   end

#   defp reduce_order_by_bind(filter_op, query, _selected_binding, bind_params) do
#     bind_params
#     |> BindingParams.normalize_bind_params(query)
#     |> Enum.reduce(query, fn {selected_binding, value}, q ->
#       reduce_order_by(filter_op, q, selected_binding, value)
#     end)
#   end

#   defp reduce_order_by_expr(:order_by, query, selected_binding, expr) do
#     apply_order_by_expr(query, selected_binding, expr)
#   end

#   defp reduce_order_by_expr(:prepend_order_by, query, selected_binding, expr) do
#     apply_prepend_order_by_expr(query, selected_binding, expr)
#   end

#   Compiler.define_clauses do
#     quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
#       defp apply_order_by_expr(query, unquote(quoted_binding_head), field_name)
#            when is_atom(field_name) do
#         Query.order_by(
#           query,
#           [unquote_splicing(quoted_binding_body)],
#           desc: field(unquote(target_binding_var), ^field_name)
#         )
#       end

#       defp apply_order_by_expr(query, unquote(quoted_binding_head), {dir, field_name})
#            when dir in @order_directions and is_atom(field_name) do
#         Query.order_by(
#           query,
#           [unquote_splicing(quoted_binding_body)],
#           [{^dir, field(unquote(target_binding_var), ^field_name)}]
#         )
#       end

#       defp apply_order_by_expr(query, unquote(quoted_binding_head), entries)
#            when is_list(entries) do
#         order_exprs =
#           Enum.map(entries, fn
#             {dir, field_name} when dir in @order_directions and is_atom(field_name) ->
#               {dir,
#                Query.dynamic(
#                  [unquote_splicing(quoted_binding_body)],
#                  field(unquote(target_binding_var), ^field_name)
#                )}

#             field_name when is_atom(field_name) ->
#               {:desc,
#                Query.dynamic(
#                  [unquote_splicing(quoted_binding_body)],
#                  field(unquote(target_binding_var), ^field_name)
#                )}

#             %Ecto.Query.DynamicExpr{} = dynamic_expr ->
#               dynamic_expr

#             other ->
#               other
#           end)

#         Query.order_by(query, ^order_exprs)
#       end

#       defp apply_prepend_order_by_expr(query, unquote(quoted_binding_head), field_name)
#            when is_atom(field_name) do
#         Query.prepend_order_by(
#           query,
#           [unquote_splicing(quoted_binding_body)],
#           desc: field(unquote(target_binding_var), ^field_name)
#         )
#       end

#       defp apply_prepend_order_by_expr(query, unquote(quoted_binding_head), {dir, field_name})
#            when dir in @order_directions and is_atom(field_name) do
#         Query.prepend_order_by(
#           query,
#           [unquote_splicing(quoted_binding_body)],
#           [{^dir, field(unquote(target_binding_var), ^field_name)}]
#         )
#       end

#       defp apply_prepend_order_by_expr(query, unquote(quoted_binding_head), entries)
#            when is_list(entries) do
#         order_exprs =
#           Enum.map(entries, fn
#             {dir, field_name} when dir in @order_directions and is_atom(field_name) ->
#               {dir,
#                Query.dynamic(
#                  [unquote_splicing(quoted_binding_body)],
#                  field(unquote(target_binding_var), ^field_name)
#                )}

#             field_name when is_atom(field_name) ->
#               {:desc,
#                Query.dynamic(
#                  [unquote_splicing(quoted_binding_body)],
#                  field(unquote(target_binding_var), ^field_name)
#                )}

#             %Ecto.Query.DynamicExpr{} = dynamic_expr ->
#               dynamic_expr

#             other ->
#               other
#           end)

#         Query.prepend_order_by(query, ^order_exprs)
#       end
#   end

#   defp apply_order_by_expr(query, _selected_binding, expr) do
#     Query.order_by(query, ^expr)
#   end

#   defp apply_prepend_order_by_expr(query, _selected_binding, expr) do
#     Query.prepend_order_by(query, ^expr)
#   end
# end
