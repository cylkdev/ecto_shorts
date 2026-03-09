# defmodule EctoShorts.CommonFilters.GroupBy do
#   @moduledoc since: "3.0.0"
#   @moduledoc """
#   Builds `:group_by` expressions from data-driven params.

#   Accepts single field atoms, lists of fields, and dynamic expressions.
#   Supports binding-scoped params via the `:bind` key.
#   """

#   alias Ecto.Query
#   alias EctoShorts.CommonFilters.BindingParams
#   alias EctoShorts.Generator

#   require Ecto.Query
#   require EctoShorts.Generator

#   @selected_binding_key :bind

#   @doc false
#   def build(_schema_source, :group_by, query, selected_binding, params, _opts) do
#     reduce_group_by(query, selected_binding, params)
#   end

#   defp reduce_group_by(query, selected_binding, {key, params})
#        when is_map(params) and not is_struct(params) do
#     reduce_group_by(query, selected_binding, {key, Map.to_list(params)})
#   end

#   defp reduce_group_by(query, selected_binding, {@selected_binding_key, bind_params}) do
#     reduce_group_by_bind(query, selected_binding, bind_params)
#   end

#   defp reduce_group_by(query, selected_binding, {key, value}) do
#     apply_group_by_expr(query, selected_binding, {key, value})
#   end

#   defp reduce_group_by(query, selected_binding, params)
#        when is_map(params) and not is_struct(params) do
#     reduce_group_by(query, selected_binding, Map.to_list(params))
#   end

#   defp reduce_group_by(query, selected_binding, entries) when is_list(entries) do
#     if Keyword.keyword?(entries) do
#       {bind_entries, group_entries} =
#         Enum.split_with(entries, fn {k, _} -> k === @selected_binding_key end)

#       query =
#         if group_entries !== [],
#           do: apply_group_by_expr(query, selected_binding, group_entries),
#           else: query

#       Enum.reduce(bind_entries, query, fn entry, query_acc ->
#         reduce_group_by(query_acc, selected_binding, entry)
#       end)
#     else
#       apply_group_by_expr(query, selected_binding, entries)
#     end
#   end

#   defp reduce_group_by(query, selected_binding, expr) do
#     apply_group_by_expr(query, selected_binding, expr)
#   end

#   defp reduce_group_by_bind(query, _selected_binding, bind_params) do
#     bind_params
#     |> BindingParams.normalize_bind_params(query)
#     |> Enum.reduce(query, fn {selected_binding, value}, q ->
#       reduce_group_by(q, selected_binding, value)
#     end)
#   end

#   Compiler.define_clauses do
#     quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
#       defp apply_group_by_expr(query, unquote(quoted_binding_head), field_name)
#            when is_atom(field_name) do
#         Query.group_by(
#           query,
#           [unquote_splicing(quoted_binding_body)],
#           field(unquote(target_binding_var), ^field_name)
#         )
#       end

#       defp apply_group_by_expr(query, unquote(quoted_binding_head), entries)
#            when is_list(entries) do
#         group_by_exprs =
#           Enum.map(entries, fn
#             field_name when is_atom(field_name) ->
#               Query.dynamic(
#                 [unquote_splicing(quoted_binding_body)],
#                 field(unquote(target_binding_var), ^field_name)
#               )

#             %Ecto.Query.DynamicExpr{} = dynamic_expr ->
#               dynamic_expr

#             other ->
#               other
#           end)

#         Query.group_by(query, ^group_by_exprs)
#       end
#   end

#   defp apply_group_by_expr(query, _selected_binding, expr) do
#     Query.group_by(query, ^expr)
#   end
# end
