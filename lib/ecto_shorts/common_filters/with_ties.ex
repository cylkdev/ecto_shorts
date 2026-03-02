defmodule EctoShorts.CommonFilters.WithTies do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Builds `:with_ties` expressions from data-driven params.

  Accepts boolean values to enable or disable `WITH TIES` on the query.
  Supports binding-scoped params via the `:bind` key.
  """

  alias Ecto.Query
  alias EctoShorts.CommonQuery
  alias EctoShorts.Compiler
  alias EctoShorts.Logger

  require Ecto.Query
  require EctoShorts.Compiler

  @logger_prefix "EctoShorts.CommonFilters.WithTies"
  @default_limit 1000

  @doc false
  def build(_schema_source, :with_ties, query, binding_selector, params, _opts) do
    apply_with_ties(query, binding_selector, params)
  end

  defp apply_with_ties(query, binding_selector, params) when is_list(params) do
    apply_with_ties(query, binding_selector, Map.new(params))
  end

  defp apply_with_ties(query, binding_selector, %{limit: limit}) do
    limit = limit || @default_limit

    query
    |> Query.limit(^limit)
    |> apply_with_ties_expr(binding_selector, true)
  end

  defp apply_with_ties(query, _binding_selector, %{bind: bind_params}) do
    entries =
      case bind_params do
        map when is_map(map) and not is_struct(map) ->
          [map]

        list when is_list(list) ->
          if Keyword.keyword?(list) and
               (Keyword.has_key?(list, :as) or Keyword.has_key?(list, :at)) do
            [Map.new(list)]
          else
            list
          end

        _ ->
          []
      end

    Enum.reduce(entries, query, fn entry, q ->
      entry_kw = if is_map(entry), do: Map.to_list(entry), else: entry

      cond do
        Keyword.has_key?(entry_kw, :as) ->
          {bind_alias, rest} = Keyword.pop(entry_kw, :as)
          value = Keyword.get(rest, :value, true)
          apply_with_ties(q, {:as, bind_alias}, value)

        Keyword.has_key?(entry_kw, :at) ->
          {bind_target, rest} = Keyword.pop(entry_kw, :at)
          value = Keyword.get(rest, :value, true)
          binding_selector = resolve_at_target(bind_target, q)
          apply_with_ties(q, binding_selector, value)

        true ->
          Logger.warning(
            @logger_prefix,
            "Expected :bind entry to have :as or :at key, got: #{inspect(entry)}"
          )

          q
      end
    end)
  end

  defp apply_with_ties(query, binding_selector, true) do
    query =
      if Query.exclude(query, :limit) === query,
        do: Query.limit(query, ^@default_limit),
        else: query

    apply_with_ties_expr(query, binding_selector, true)
  end

  defp apply_with_ties(query, binding_selector, false) do
    apply_with_ties_expr(query, binding_selector, false)
  end

  defp apply_with_ties(query, _binding_selector, value) do
    Logger.warning(
      @logger_prefix,
      "Expected :with_ties value to be a boolean, got: #{inspect(value)}"
    )

    query
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, _target_binding_var, _binding_patterns ->
      defp apply_with_ties_expr(query, unquote(quoted_binding_head), value)
           when is_boolean(value) do
        Query.with_ties(query, [unquote_splicing(quoted_binding_body)], ^value)
      end
  end

  defp apply_with_ties_expr(query, _binding_selector, value) do
    Logger.warning(
      @logger_prefix,
      "Expected :with_ties value to be a boolean, got: #{inspect(value)}"
    )

    query
  end

  defp resolve_at_target(:first, _query), do: {:at, 1}
  defp resolve_at_target(:last, query), do: {:at, CommonQuery.query_binding_count(query)}
  defp resolve_at_target(index, _query) when is_integer(index), do: {:at, index}

  defp resolve_at_target(other, _query) do
    Logger.warning(
      @logger_prefix,
      "Expected :at value to be an integer, :first, or :last, got: #{inspect(other)}"
    )

    {:at, other}
  end
end
