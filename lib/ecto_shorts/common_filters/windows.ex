# defmodule EctoShorts.CommonFilters.Windows do
#   @moduledoc since: "3.0.0"
#   @moduledoc """
#   Builds `:windows` expressions from data-driven params.

#   Accepts keyword lists of `{window_name, definition}` entries where each
#   definition contains `:partition_by`, `:order_by`, and optional `:frame`
#   keys. Supports binding-scoped params via the `:bind` key.
#   """

#   alias Ecto.Query
#   alias EctoShorts.CommonFilters.BindingParams
#   alias EctoShorts.Generator
#   alias EctoShorts.Logger

#   require Ecto.Query
#   require Compiler

#   @logger_prefix "EctoShorts.CommonFilters.Windows"
#   @selected_binding_key :bind
#   @window_keys [:partition_by, :order_by, :frame]

#   @doc false
#   def build(_schema_source, :windows, query, selected_binding, params, _opts) do
#     reduce_windows(query, selected_binding, params)
#   end

#   defp reduce_windows(query, selected_binding, params)
#        when is_map(params) and not is_struct(params) do
#     reduce_windows(query, selected_binding, Map.to_list(params))
#   end

#   defp reduce_windows(query, selected_binding, {@selected_binding_key, bind_params}) do
#     reduce_windows_bind(query, selected_binding, bind_params)
#   end

#   defp reduce_windows(query, selected_binding, params) when is_list(params) do
#     cond do
#       Keyword.keyword?(params) ->
#         case Enum.split_with(params, fn {k, _} -> k === @selected_binding_key end) do
#           {[], window_entries} ->
#             reduce_window_entries(query, selected_binding, window_entries)

#           {binding_entries, []} ->
#             Enum.reduce(binding_entries, query, fn entry, query_acc ->
#               reduce_windows(query_acc, selected_binding, entry)
#             end)

#           {binding_entries, window_entries} ->
#             query_with_windows = reduce_window_entries(query, selected_binding, window_entries)

#             Enum.reduce(binding_entries, query_with_windows, fn entry, query_acc ->
#               reduce_windows(query_acc, selected_binding, entry)
#             end)
#         end

#       not Keyword.keyword?(params) ->
#         Logger.warning(
#           @logger_prefix,
#           "Expected :windows params to be a keyword list of window definitions, got: #{inspect(params)}"
#         )

#         query
#     end
#   end

#   defp reduce_windows(query, selected_binding, {window_name, window_definition}) do
#     apply_windows_expr(query, selected_binding, window_name, window_definition)
#   end

#   defp reduce_windows(query, _selected_binding, value) do
#     Logger.warning(
#       @logger_prefix,
#       "Expected :windows params to be a keyword list/map of window definitions, got: #{inspect(value)}"
#     )

#     query
#   end

#   defp reduce_windows_bind(query, _selected_binding, bind_params) do
#     bind_params
#     |> BindingParams.normalize_bind_params(query)
#     |> Enum.reduce(query, fn {selected_binding, value}, q ->
#       reduce_windows(q, selected_binding, value)
#     end)
#   end

#   defp reduce_window_entries(query, selected_binding, entries) do
#     Enum.reduce(entries, query, fn entry, query_acc ->
#       reduce_windows(query_acc, selected_binding, entry)
#     end)
#   end

#   defp apply_windows_expr(query, _selected_binding, window_name, _window_definition)
#        when not is_atom(window_name) do
#     Logger.warning(
#       @logger_prefix,
#       "Expected window name to be an atom, got: #{inspect(window_name)}"
#     )

#     query
#   end

#   defp apply_windows_expr(query, selected_binding, window_name, window_definition) do
#     case normalize_window_definition(window_definition) do
#       {:ok, normalized_definition} ->
#         unknown_keys =
#           normalized_definition
#           |> Keyword.keys()
#           |> Enum.reject(&(&1 in @window_keys))

#         if unknown_keys !== [] do
#           Logger.warning(
#             @logger_prefix,
#             "Ignoring unsupported window keys #{inspect(unknown_keys)} for #{inspect(window_name)}"
#           )
#         end

#         definition = Keyword.take(normalized_definition, @window_keys)

#         partition_by =
#           definition
#           |> Keyword.get(:partition_by, [])
#           |> normalize_partition_by(selected_binding)

#         order_by =
#           definition
#           |> Keyword.get(:order_by, [])
#           |> normalize_order_by(selected_binding)

#         frame = Keyword.get(definition, :frame)

#         if is_nil(frame) do
#           compose_window(query, selected_binding, window_name, partition_by, order_by)
#         else
#           compose_window(query, selected_binding, window_name, partition_by, order_by, frame)
#         end

#       :error ->
#         Logger.warning(
#           @logger_prefix,
#           "Expected window definition for #{inspect(window_name)} to be a map or keyword list, got: #{inspect(window_definition)}"
#         )

#         query
#     end
#   end

#   defp normalize_window_definition(value) when is_map(value) and not is_struct(value),
#     do: {:ok, Map.to_list(value)}

#   defp normalize_window_definition(value) when is_list(value) do
#     if Keyword.keyword?(value), do: {:ok, value}, else: :error
#   end

#   defp normalize_window_definition(_value), do: :error

#   defp normalize_partition_by(nil, _selected_binding), do: []

#   defp normalize_partition_by(value, selected_binding) when is_atom(value) do
#     [compose(selected_binding, value)]
#   end

#   defp normalize_partition_by(value, selected_binding)
#        when is_map(value) and not is_struct(value) do
#     normalize_partition_by(Map.to_list(value), selected_binding)
#   end

#   defp normalize_partition_by(values, selected_binding) when is_list(values) do
#     if Keyword.keyword?(values) do
#       values
#     else
#       Enum.map(values, fn
#         value when is_atom(value) ->
#           compose(selected_binding, value)

#         other ->
#           other
#       end)
#     end
#   end

#   defp normalize_partition_by(value, _selected_binding), do: value

#   defp normalize_order_by(nil, _selected_binding), do: []

#   defp normalize_order_by(value, selected_binding) when is_atom(value) do
#     [compose(selected_binding, value)]
#   end

#   defp normalize_order_by({direction, field_name}, selected_binding) when is_atom(field_name) do
#     [{direction, compose(selected_binding, field_name)}]
#   end

#   defp normalize_order_by(value, selected_binding)
#        when is_map(value) and not is_struct(value) do
#     normalize_order_by(Map.to_list(value), selected_binding)
#   end

#   defp normalize_order_by(values, selected_binding) when is_list(values) do
#     if Keyword.keyword?(values) do
#       Enum.map(values, fn
#         {direction, field_name} when is_atom(field_name) ->
#           {direction, compose(selected_binding, field_name)}

#         other ->
#           other
#       end)
#     else
#       Enum.map(values, fn
#         value when is_atom(value) ->
#           compose(selected_binding, value)

#         other ->
#           other
#       end)
#     end
#   end

#   defp normalize_order_by(value, _selected_binding), do: value

#   Compiler.define_clauses do
#     quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
#       defp compose_window(
#              query,
#              unquote(quoted_binding_head),
#              window_name,
#              partition_by,
#              order_by
#            ) do
#         Query.windows(
#           query,
#           [unquote_splicing(quoted_binding_body)],
#           [{window_name, [partition_by: ^partition_by, order_by: ^order_by]}]
#         )
#       end

#       defp compose_window(
#              query,
#              unquote(quoted_binding_head),
#              window_name,
#              partition_by,
#              order_by,
#              frame
#            ) do
#         Query.windows(
#           query,
#           [unquote_splicing(quoted_binding_body)],
#           [{window_name, [partition_by: ^partition_by, order_by: ^order_by, frame: ^frame]}]
#         )
#       end

#       defp compose(unquote(quoted_binding_head), field_name) do
#         Query.dynamic(
#           [unquote_splicing(quoted_binding_body)],
#           field(unquote(target_binding_var), ^field_name)
#         )
#       end
#   end
# end
