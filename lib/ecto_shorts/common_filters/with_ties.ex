# defmodule EctoShorts.CommonFilters.WithTies do
#   @moduledoc since: "3.0.0"
#   @moduledoc """
#   Builds `:with_ties` expressions from data-driven params.

#   Accepts boolean values to enable or disable `WITH TIES` on the query.
#   Supports binding-scoped params via the `:bind` key.
#   """

#   alias Ecto.Query
#   alias EctoShorts.CommonQuery
#   alias EctoShorts.Generator
#   alias EctoShorts.Logger

#   require Ecto.Query
#   require EctoShorts.Generator

#   @logger_prefix "EctoShorts.CommonFilters.WithTies"
#   @default_limit 1000

#   @doc false
#   def build(_schema_source, :with_ties, query, selected_binding, params, _opts) do
#     apply_with_ties(query, selected_binding, params)
#   end

#   defp apply_with_ties(query, selected_binding, params) when is_list(params) do
#     apply_with_ties(query, selected_binding, Map.new(params))
#   end

#   defp apply_with_ties(query, selected_binding, %{limit: limit}) do
#     limit = limit || @default_limit

#     query
#     |> Query.limit(^limit)
#     |> apply_with_ties_expr(selected_binding, true)
#   end

#   defp apply_with_ties(query, _selected_binding, %{bind: bind_params}) do
#     entries =
#       case bind_params do
#         map when is_map(map) and not is_struct(map) ->
#           [map]

#         list when is_list(list) ->
#           if Keyword.keyword?(list) and
#                (Keyword.has_key?(list, :as) or Keyword.has_key?(list, :at)) do
#             [Map.new(list)]
#           else
#             list
#           end

#         _ ->
#           []
#       end

#     Enum.reduce(entries, query, fn entry, q ->
#       entry_kw = if is_map(entry), do: Map.to_list(entry), else: entry

#       cond do
#         Keyword.has_key?(entry_kw, :as) ->
#           {bind_alias, rest} = Keyword.pop(entry_kw, :as)
#           value = Keyword.get(rest, :value, true)
#           apply_with_ties(q, {:as, bind_alias}, value)

#         Keyword.has_key?(entry_kw, :at) ->
#           {bind_target, rest} = Keyword.pop(entry_kw, :at)
#           value = Keyword.get(rest, :value, true)
#           selected_binding = resolve_at_target(bind_target, q)
#           apply_with_ties(q, selected_binding, value)

#         true ->
#           Logger.warning(
#             @logger_prefix,
#             "Expected :bind entry to have :as or :at key, got: #{inspect(entry)}"
#           )

#           q
#       end
#     end)
#   end

#   defp apply_with_ties(query, selected_binding, true) do
#     query =
#       if Query.exclude(query, :limit) === query,
#         do: Query.limit(query, ^@default_limit),
#         else: query

#     apply_with_ties_expr(query, selected_binding, true)
#   end

#   defp apply_with_ties(query, selected_binding, false) do
#     apply_with_ties_expr(query, selected_binding, false)
#   end

#   defp apply_with_ties(query, _selected_binding, value) do
#     Logger.warning(
#       @logger_prefix,
#       "Expected :with_ties value to be a boolean, got: #{inspect(value)}"
#     )

#     query
#   end

#   Compiler.define_clauses do
#     quoted_binding_head, quoted_binding_body, _target_binding_var, _binding_patterns ->
#       defp apply_with_ties_expr(query, unquote(quoted_binding_head), value)
#            when is_boolean(value) do
#         Query.with_ties(query, [unquote_splicing(quoted_binding_body)], ^value)
#       end
#   end

#   defp apply_with_ties_expr(query, _selected_binding, value) do
#     Logger.warning(
#       @logger_prefix,
#       "Expected :with_ties value to be a boolean, got: #{inspect(value)}"
#     )

#     query
#   end

#   defp resolve_at_target(:first, _query), do: {:at, 1}
#   defp resolve_at_target(:last, query), do: {:at, CommonQuery.query_binding_count(query)}
#   defp resolve_at_target(index, _query) when is_integer(index), do: {:at, index}

#   defp resolve_at_target(other, _query) do
#     Logger.warning(
#       @logger_prefix,
#       "Expected :at value to be an integer, :first, or :last, got: #{inspect(other)}"
#     )

#     {:at, other}
#   end
# end
defmodule EctoShorts.CommonFilters.WithTies do
  alias Ecto.Query
  alias EctoShorts.CommonFilters.{Limit, OrderBy}
  alias EctoShorts.{CommonSchema, Compiler, Utils}

  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.WithTies"
  @default_limit 1000

  {_, binding_patterns} =
    Compiler.query_binding_contracts(__MODULE__, positions: 10)

  def build_query(:with_ties, source, query, selected_binding, params, opts) do
    apply_with_ties(source, query, selected_binding, params, opts)
  end

  defp apply_with_ties(source, query, selected_binding, params, opts)
       when is_map(params) and not is_struct(params) do
    apply_with_ties(source, query, selected_binding, Map.to_list(params), opts)
  end

  defp apply_with_ties(source, query, selected_binding, params, opts) when is_list(params) do
    normalized_params = Utils.normalize_input(params)

    if Keyword.keyword?(normalized_params) do
      unknown_keys =
        normalized_params
        |> Keyword.keys()
        |> Enum.reject(&(&1 === :limit))

      cond do
        unknown_keys !== [] ->
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected :with_ties params to only include :limit, got unsupported keys: #{inspect(unknown_keys)}"
          )

          query

        true ->
          apply_limit_payload(source, query, selected_binding, Keyword.get(normalized_params, :limit), opts)
      end
    else
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected :with_ties value to be a boolean or keyword/map payload, got: #{inspect(params)}"
      )

      query
    end
  end

  defp apply_with_ties(source, query, selected_binding, true, opts) do
    query
    |> ensure_limit(source, selected_binding, @default_limit, opts)
    |> ensure_order(source, selected_binding, opts)
    |> apply_with_ties_expr(selected_binding, true)
  end

  defp apply_with_ties(_source, query, selected_binding, false, _opts) do
    if has_limit?(query) do
      apply_with_ties_expr(query, selected_binding, false)
    else
      query
    end
  end

  defp apply_with_ties(_source, query, _selected_binding, value, _opts) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :with_ties value to be a boolean or keyword/map payload, got: #{inspect(value)}"
    )

    query
  end

  defp apply_limit_payload(source, query, selected_binding, nil, opts) do
    query
    |> ensure_limit(source, selected_binding, @default_limit, opts)
    |> ensure_order(source, selected_binding, opts)
    |> apply_with_ties_expr(selected_binding, true)
  end

  defp apply_limit_payload(source, query, selected_binding, limit, opts) when is_integer(limit) do
    query =
      Limit.build_query(
        :limit,
        source,
        query,
        selected_binding,
        limit,
        opts
      )

    query
    |> ensure_order(source, selected_binding, opts)
    |> apply_with_ties_expr(selected_binding, true)
  end

  defp apply_limit_payload(_source, query, _selected_binding, value, _opts) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :with_ties :limit to be an integer or nil, got: #{inspect(value)}"
    )

    query
  end

  defp ensure_limit(query, source, selected_binding, limit, opts) do
    if has_limit?(query) do
      query
    else
      Limit.build_query(:limit, source, query, selected_binding, limit, opts)
    end
  end

  defp ensure_order(query, source, selected_binding, opts) do
    if has_order?(query) do
      query
    else
      sort_keys =
        List.wrap(CommonSchema.get_schema_reflection(source, :primary_key) || :id)

      order_entries = Enum.map(sort_keys, &{:asc, &1})
      OrderBy.build_query(:order_by, source, query, selected_binding, order_entries, opts)
    end
  end

  defp has_limit?(query), do: not is_nil(query.limit)
  defp has_order?(query), do: query.order_bys !== []

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp apply_with_ties_expr(query, unquote(quoted_binding_head), value) when is_boolean(value) do
      Query.with_ties(
        query,
        [unquote_splicing(quoted_binding_body)],
        ^value
      )
    end
  end

  defp apply_with_ties_expr(query, _selected_binding, value) when is_boolean(value) do
    Query.with_ties(query, ^value)
  end
end
