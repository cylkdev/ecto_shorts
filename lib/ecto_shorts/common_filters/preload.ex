# defmodule EctoShorts.CommonFilters.Preload do
#   @moduledoc since: "3.0.0"
#   @moduledoc """
#   Builds `:preload` expressions from data-driven params.

#   Accepts atoms, lists of atoms, and keyword lists for nested preloads.
#   Supports binding-scoped params via the `:bind` key.
#   """

#   alias Ecto.Query
#   alias EctoShorts.CommonFilters.BindingParams
#   alias EctoShorts.Generator
#   alias EctoShorts.Logger

#   require Ecto.Query
#   require EctoShorts.Generator

#   @logger_prefix "EctoShorts.CommonFilters.Preload"
#   @selected_binding_key :bind

#   def build(schema_source, :preload, query, selected_binding, arg, _opts) do
#     reduce_preload(schema_source, query, selected_binding, arg)
#   end

#   defp reduce_preload(
#          schema_source,
#          query,
#          selected_binding,
#          {@selected_binding_key, bind_params}
#        ) do
#     reduce_preload_bind(schema_source, query, selected_binding, bind_params)
#   end

#   defp reduce_preload(schema_source, query, selected_binding, values) when is_list(values) do
#     if Keyword.keyword?(values) do
#       case Enum.split_with(values, fn {k, _} -> k === @selected_binding_key end) do
#         {[], entries} ->
#           apply_preload_expr(query, selected_binding, entries)

#         {bind_entries, []} ->
#           Enum.reduce(bind_entries, query, fn entry, query_acc ->
#             reduce_preload(schema_source, query_acc, selected_binding, entry)
#           end)

#         {bind_entries, entries} ->
#           Enum.reduce(bind_entries, query, fn {@selected_binding_key, bind_params}, query_acc ->
#             reduce_preload_bind(schema_source, query_acc, selected_binding, bind_params, entries)
#           end)
#       end
#     else
#       apply_preload_expr(query, selected_binding, values)
#     end
#   end

#   defp reduce_preload(_schema_source, query, selected_binding, key) do
#     apply_preload_expr(query, selected_binding, [key])
#   end

#   defp reduce_preload_bind(_schema_source, query, _selected_binding, bind_params, entries \\ nil) do
#     bind_params
#     |> BindingParams.normalize_bind_params(query)
#     |> Enum.reduce(query, fn {selected_binding, value}, q ->
#       if valid_binding?(q, selected_binding) do
#         apply_preload_expr(q, selected_binding, value, entries)
#       else
#         Logger.warning(
#           @logger_prefix,
#           "unknown bind name `#{inspect(elem(selected_binding, 1))}` in query"
#         )

#         q
#       end
#     end)
#   end

#   defp valid_binding?(_query, {:as, nil}), do: false

#   defp valid_binding?(query, {:as, alias}) when is_atom(alias) do
#     Query.has_named_binding?(query, alias)
#   end

#   defp valid_binding?(_query, _selected_binding), do: true

#   Compiler.define_clauses do
#     quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
#       defp apply_preload_expr(query, unquote(quoted_binding_head), expr) do
#         Query.preload(query, [unquote_splicing(quoted_binding_body)], ^expr)
#       end

#       defp apply_preload_expr(query, unquote(quoted_binding_head), assoc_key, nil) do
#         Query.preload(
#           query,
#           [unquote_splicing(quoted_binding_body)],
#           [{^assoc_key, unquote(target_binding_var)}]
#         )
#       end

#       defp apply_preload_expr(query, unquote(quoted_binding_head), assoc_key, nested) do
#         if Keyword.keyword?(nested) do
#           Enum.reduce(nested, query, fn nested_entry, query_acc ->
#             Query.preload(
#               query_acc,
#               [unquote_splicing(quoted_binding_body)],
#               [{^assoc_key, {unquote(target_binding_var), ^nested_entry}}]
#             )
#           end)
#         else
#           Query.preload(
#             query,
#             [unquote_splicing(quoted_binding_body)],
#             [{^assoc_key, {unquote(target_binding_var), ^nested}}]
#           )
#         end
#       end
#   end
# end
