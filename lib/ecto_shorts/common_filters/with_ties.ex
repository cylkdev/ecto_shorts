defmodule EctoShorts.CommonFilters.WithTies do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Builds `:with_ties` expressions from data-driven params.

  Accepts boolean values to enable or disable `WITH TIES` on the query.
  Supports binding-scoped params via the `:bind` key.
  """

  alias Ecto.Query
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
    bind_entries = if is_map(bind_params), do: Map.to_list(bind_params), else: bind_params

    Enum.reduce(bind_entries, query, fn {mode, scoped}, q ->
      scoped_entries = if is_map(scoped), do: Map.to_list(scoped), else: scoped

      Enum.reduce(scoped_entries, q, fn {target, value}, q2 ->
        apply_with_ties(q2, {mode, target}, value)
      end)
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
end
