defmodule EctoShorts.CommonFilters.WithTies do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Builds `:with_ties` expressions from data-driven params.

  Accepts boolean values to enable or disable `WITH TIES` on the query.
  Supports binding-scoped params via the `:bind` key.
  """

  alias Ecto.Query
  alias EctoShorts.CommonFilters.BindingParams
  alias EctoShorts.Compiler
  alias EctoShorts.Logger

  require Ecto.Query
  require EctoShorts.Compiler

  @logger_prefix "EctoShorts.CommonFilters.WithTies"
  @binding_selector_key :bind

  @doc false
  def build(_schema_source, :with_ties, query, binding_selector, params, _opts) do
    reduce_with_ties(query, binding_selector, params)
  end

  defp reduce_with_ties(query, binding_selector, params)
       when is_map(params) and not is_struct(params) do
    reduce_with_ties(query, binding_selector, Map.to_list(params))
  end

  defp reduce_with_ties(query, binding_selector, {@binding_selector_key, bind_params}) do
    reduce_with_ties_bind(query, binding_selector, bind_params)
  end

  defp reduce_with_ties(query, binding_selector, {key, value}) do
    apply_with_ties_expr(query, binding_selector, {key, value})
  end

  defp reduce_with_ties(query, binding_selector, params) when is_list(params) do
    if Keyword.keyword?(params) do
      case Enum.split_with(params, fn {k, _} -> k === @binding_selector_key end) do
        {[], entries} ->
          Enum.reduce(entries, query, fn entry, query_acc ->
            reduce_with_ties(query_acc, binding_selector, entry)
          end)

        {bind_entries, []} ->
          Enum.reduce(bind_entries, query, fn entry, query_acc ->
            reduce_with_ties(query_acc, binding_selector, entry)
          end)

        {bind_entries, entries} ->
          query_with_ties =
            Enum.reduce(entries, query, fn entry, query_acc ->
              reduce_with_ties(query_acc, binding_selector, entry)
            end)

          Enum.reduce(bind_entries, query_with_ties, fn entry, query_acc ->
            reduce_with_ties(query_acc, binding_selector, entry)
          end)
      end
    else
      apply_with_ties_expr(query, binding_selector, params)
    end
  end

  defp reduce_with_ties(query, binding_selector, value) do
    apply_with_ties_expr(query, binding_selector, value)
  end

  defp reduce_with_ties_bind(query, _binding_selector, bind_params) do
    BindingParams.reduce_submodule_bind_params(query, bind_params, fn q, {mode, target}, value ->
      reduce_with_ties(q, {mode, target}, value)
    end)
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
