# defmodule EctoShorts.CommonFilters.Distinct do
#   @moduledoc since: "3.0.0"
#   @moduledoc """
#   Builds `:distinct` expressions from data-driven params.

#   Accepts booleans, single field atoms, lists of fields, and
#   `{direction, field}` tuples. Supports binding-scoped params via the
#   `:bind` key.
#   """

#   alias Ecto.Query
#   alias EctoShorts.CommonFilters.BindingParams
#   alias EctoShorts.Generator
#   alias EctoShorts.Logger

#   require Ecto.Query
#   require EctoShorts.Generator

#   @logger_prefix "EctoShorts.CommonFilters.Distinct"
#   @binding_selector_key :bind

#   @order_directions [
#     :asc,
#     :asc_nulls_last,
#     :asc_nulls_first,
#     :desc,
#     :desc_nulls_last,
#     :desc_nulls_first
#   ]

#   @doc false
#   def build(_schema_source, :distinct, query, binding_selector, params, _opts) do
#     reduce_params(query, binding_selector, params)
#   end

#   defp reduce_params(query, binding_selector, {key, params})
#        when is_map(params) and not is_struct(params) do
#     reduce_params(query, binding_selector, {key, Map.to_list(params)})
#   end

#   defp reduce_params(query, binding_selector, {@binding_selector_key, bind_params}) do
#     reduce_params_bind(query, binding_selector, bind_params)
#   end

#   defp reduce_params(query, binding_selector, {key, value}) do
#     apply_distinct_expr(query, binding_selector, {key, value})
#   end

#   defp reduce_params(query, binding_selector, params)
#        when is_map(params) and not is_struct(params) do
#     reduce_params(query, binding_selector, Map.to_list(params))
#   end

#   defp reduce_params(query, binding_selector, entries) when is_list(entries) do
#     if Keyword.keyword?(entries) do
#       {bind_entries, distinct_entries} =
#         Enum.split_with(entries, fn {k, _} -> k === @binding_selector_key end)

#       query =
#         if distinct_entries !== [],
#           do: apply_distinct_expr(query, binding_selector, distinct_entries),
#           else: query

#       Enum.reduce(bind_entries, query, fn entry, query_acc ->
#         reduce_params(query_acc, binding_selector, entry)
#       end)
#     else
#       apply_distinct_expr(query, binding_selector, entries)
#     end
#   end

#   defp reduce_params(query, binding_selector, expr) do
#     apply_distinct_expr(query, binding_selector, expr)
#   end

#   defp reduce_params_bind(query, _binding_selector, bind_params) do
#     bind_params
#     |> BindingParams.normalize_bind_params(query)
#     |> Enum.reduce(query, fn {binding_selector, value}, q ->
#       reduce_params(q, binding_selector, value)
#     end)
#   end

#   Compiler.define_clauses do
#     quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
#       defp apply_distinct_expr(query, unquote(quoted_binding_head), expr) when is_boolean(expr) do
#         Query.distinct(query, ^expr)
#       end

#       defp apply_distinct_expr(query, unquote(quoted_binding_head), field_name)
#            when is_atom(field_name) do
#         Query.distinct(
#           query,
#           [unquote_splicing(quoted_binding_body)],
#           field(unquote(target_binding_var), ^field_name)
#         )
#       end

#       defp apply_distinct_expr(query, unquote(quoted_binding_head), entries)
#            when is_list(entries) do
#         distinct_exprs =
#           Enum.map(entries, fn
#             {dir, field_name} when dir in @order_directions and is_atom(field_name) ->
#               {dir,
#                Query.dynamic(
#                  [unquote_splicing(quoted_binding_body)],
#                  field(unquote(target_binding_var), ^field_name)
#                )}

#             field_name when is_atom(field_name) ->
#               {:asc,
#                Query.dynamic(
#                  [unquote_splicing(quoted_binding_body)],
#                  field(unquote(target_binding_var), ^field_name)
#                )}

#             other ->
#               other
#           end)

#         Query.distinct(query, ^distinct_exprs)
#       end
#   end

#   defp apply_distinct_expr(query, _binding_selector, expr) do
#     Logger.warning(
#       @logger_prefix,
#       "Expected :distinct value to be a boolean, atom, list, or {direction, field} tuple, got: #{inspect(expr)}"
#     )

#     query
#   end
# end
