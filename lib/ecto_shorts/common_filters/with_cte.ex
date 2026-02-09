defmodule EctoShorts.CommonFilters.WithCte do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.CommonFilters

  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.WithCte"

  @doc false
  def build(schema_source, :with_cte, query, _binding_selector, params, opts) do
    reduce_cte(schema_source, query, params, opts)
  end

  defp reduce_cte(schema_source, query, params, opts)
       when is_map(params) and not is_struct(params) do
    reduce_cte(schema_source, query, Map.to_list(params), opts)
  end

  defp reduce_cte(schema_source, query, params, opts) when is_list(params) do
    if Keyword.keyword?(params) do
      Enum.reduce(params, query, fn {cte_name, cte_definition}, query_acc ->
        apply_cte(schema_source, query_acc, cte_name, cte_definition, opts)
      end)
    else
      Enum.reduce(params, query, fn entry, query_acc ->
        reduce_cte(schema_source, query_acc, entry, opts)
      end)
    end
  end

  defp reduce_cte(schema_source, query, {cte_name, cte_definition}, opts) do
    apply_cte(schema_source, query, cte_name, cte_definition, opts)
  end

  defp reduce_cte(_schema_source, query, value, _opts) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :with_cte params to be a map or keyword list, got: #{inspect(value)}"
    )

    query
  end

  defp apply_cte(schema_source, query, cte_name, cte_definition, opts) do
    case build_query(schema_source, cte_name, cte_definition, opts) do
      {:ok, cte_query} ->
        materialized = Keyword.get(cte_definition, :materialized)
        operation = Keyword.get(cte_definition, :operation)

        Query.with_cte(
          query,
          ^cte_name,
          as: ^cte_query,
          materialized: materialized,
          operation: operation
        )

      :error ->
        query
    end
  end

  defp build_query(schema_source, cte_name, cte_definition, opts) do
    case Keyword.get(cte_definition, :as) do
      %Ecto.Query{} = query ->
        {:ok, query}

      %Ecto.SubQuery{} = query ->
        {:ok, query}

      query_params when is_map(query_params) and not is_struct(query_params) ->
        from_source = Keyword.get(query_params, :source, schema_source)
        filter_params = Keyword.get(query_params, :query, %{})
        {:ok, CommonFilters.convert_params_to_filter(from_source, filter_params, opts)}

      query_params when is_list(query_params) ->
        from_source = Keyword.get(query_params, :source, schema_source)
        filter_params = Keyword.get(query_params, :query, %{})
        {:ok, CommonFilters.convert_params_to_filter(from_source, filter_params, opts)}

      term ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected CTE :as query params for #{inspect(cte_name)} to be a query, subquery, or keyword/map payload, got: #{inspect(term)}"
        )

        :error
    end
  end
end
