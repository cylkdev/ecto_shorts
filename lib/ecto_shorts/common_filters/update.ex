# defmodule EctoShorts.CommonFilters.Update do
#   @moduledoc since: "3.0.0"
#   @moduledoc """
#   Builds `:update` expressions from data-driven params.

#   Accepts keyword lists of `{operation, fields}` entries (e.g.,
#   `[set: [name: \"new\"], inc: [counter: 1]]`) and maps. Supports
#   binding-scoped params via the `:bind` key.
#   """

#   alias Ecto.Query
#   alias EctoShorts.CommonFilters.BindingParams
#   alias EctoShorts.Generator

#   require Ecto.Query
#   require EctoShorts.Generator

#   @selected_binding_key :bind

#   @doc false
#   def build(_schema_source, :update, query, selected_binding, term, opts)
#       when is_map(term) and not is_struct(term) do
#     term
#     |> Map.to_list()
#     |> Enum.reduce(query, fn entry, query_acc ->
#       build(nil, :update, query_acc, selected_binding, entry, opts)
#     end)
#   end

#   def build(
#         _schema_source,
#         :update,
#         query,
#         selected_binding,
#         {@selected_binding_key, bind_params},
#         opts
#       ) do
#     reduce_update_bind(query, selected_binding, bind_params, opts)
#   end

#   def build(_schema_source, :update, query, selected_binding, term, opts) when is_list(term) do
#     if Keyword.keyword?(term) do
#       case Enum.split_with(term, fn {k, _} -> k === @selected_binding_key end) do
#         {[], entries} ->
#           apply_update_expr(query, selected_binding, normalize_update_entries(entries))

#         {bind_entries, []} ->
#           Enum.reduce(bind_entries, query, fn entry, query_acc ->
#             build(nil, :update, query_acc, selected_binding, entry, opts)
#           end)

#         {bind_entries, entries} ->
#           query_with_update =
#             apply_update_expr(query, selected_binding, normalize_update_entries(entries))

#           Enum.reduce(bind_entries, query_with_update, fn entry, query_acc ->
#             build(nil, :update, query_acc, selected_binding, entry, opts)
#           end)
#       end
#     else
#       apply_update_expr(query, selected_binding, term)
#     end
#   end

#   def build(_schema_source, :update, query, selected_binding, term, _opts) do
#     apply_update_expr(query, selected_binding, term)
#   end

#   defp normalize_update_entries(entries) when is_list(entries) do
#     Enum.map(entries, fn
#       {op, value} when is_map(value) and not is_struct(value) ->
#         {op, Map.to_list(value)}

#       {op, value} ->
#         {op, value}
#     end)
#   end

#   defp reduce_update_bind(query, _selected_binding, bind_params, opts) do
#     bind_params
#     |> BindingParams.normalize_bind_params(query)
#     |> Enum.reduce(query, fn {selected_binding, value}, q ->
#       build(nil, :update, q, selected_binding, value, opts)
#     end)
#   end

#   Compiler.define_clauses do
#     quoted_binding_head, quoted_binding_body, _target_binding_var, _binding_patterns ->
#       defp apply_update_expr(query, unquote(quoted_binding_head), expr) do
#         Query.update(query, [unquote_splicing(quoted_binding_body)], ^expr)
#       end
#   end
# end
