defmodule EctoShorts.CommonFilters.WithCte do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.CommonFilters

  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.WithCte"

  @doc false
  def build(schema_source, :with_cte, query, _binding_selector, params, opts) do
    reduce_with_cte(schema_source, query, params, opts)
  end

  defp reduce_with_cte(schema_source, query, params, opts)
       when is_map(params) and not is_struct(params) do
    reduce_with_cte(schema_source, query, Map.to_list(params), opts)
  end

  defp reduce_with_cte(schema_source, query, params, opts) when is_list(params) do
    if Keyword.keyword?(params) do
      Enum.reduce(params, query, fn {cte_name, cte_definition}, query_acc ->
        apply_cte_entry(schema_source, query_acc, cte_name, cte_definition, opts)
      end)
    else
      Enum.reduce(params, query, fn entry, query_acc ->
        reduce_with_cte(schema_source, query_acc, entry, opts)
      end)
    end
  end

  defp reduce_with_cte(schema_source, query, {cte_name, cte_definition}, opts) do
    apply_cte_entry(schema_source, query, cte_name, cte_definition, opts)
  end

  defp reduce_with_cte(_schema_source, query, value, _opts) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :with_cte params to be a map or keyword list, got: #{inspect(value)}"
    )

    query
  end

  defp apply_cte_entry(schema_source, query, cte_name, cte_definition, opts) do
    with {:ok, cte_query} <-
           build_cte_query(schema_source, cte_name, cte_definition, opts) do
      materialized = Keyword.get(cte_definition, :materialized)
      operation = Keyword.get(cte_definition, :operation)

      Query.with_cte(
        query,
        ^cte_name,
        as: ^cte_query,
        materialized: materialized,
        operation: operation
      )
    else
      :error -> query
    end
  end

  defp build_cte_query(schema_source, cte_name, cte_definition, opts) do
    case Keyword.fetch(cte_definition, :as) do
      {:ok, as_value} ->
        case as_value do
          %Ecto.Query{} = query ->
            {:ok, query}

          %Ecto.SubQuery{} = query ->
            {:ok, query}

          query_params when is_map(query_params) and not is_struct(query_params) ->
            from_source = Map.get(query_params, :source, schema_source)
            filter_params = Map.get(query_params, :query, [])
            {:ok, CommonFilters.convert_params_to_filter(from_source, filter_params, opts)}

          query_params when is_list(query_params) ->
            if Keyword.keyword?(query_params) do
              from_source = Keyword.get(query_params, :source, schema_source)

              filter_params = Keyword.get(query_params, :query, [])
              {:ok, CommonFilters.convert_params_to_filter(from_source, filter_params, opts)}
            else
              EctoShorts.Logger.warning(
                @logger_prefix,
                "Expected CTE :as query payload for #{inspect(cte_name)} to be a query, subquery, or keyword/map payload, got: #{inspect(as_value)}"
              )

              :error
            end

          _ ->
            EctoShorts.Logger.warning(
              @logger_prefix,
              "Expected CTE :as query payload for #{inspect(cte_name)} to be a query, subquery, or keyword/map payload, got: #{inspect(as_value)}"
            )

            :error
        end

      :error ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected CTE definition for #{inspect(cte_name)} to include an :as key, got: #{inspect(cte_definition)}"
        )

        :error
    end
  end
end
