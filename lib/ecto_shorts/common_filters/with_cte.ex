defmodule EctoShorts.CommonFilters.WithCte do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.CommonFilters

  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.WithCte"
  @cte_operations [:all, :update_all, :delete_all]

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
    with {:ok, normalized_name} <- normalize_cte_name(cte_name),
         {:ok, normalized_definition} <- normalize_cte_definition(cte_name, cte_definition),
         {:ok, cte_query} <-
           normalize_cte_query(schema_source, normalized_name, normalized_definition, opts),
         {:ok, materialized} <- normalize_materialized(normalized_name, normalized_definition),
         {:ok, operation} <- normalize_operation(normalized_name, normalized_definition) do
      Query.with_cte(
        query,
        ^normalized_name,
        as: ^cte_query,
        materialized: materialized,
        operation: operation
      )
    else
      :error -> query
    end
  end

  defp normalize_cte_name(cte_name) when is_binary(cte_name), do: {:ok, cte_name}
  defp normalize_cte_name(cte_name) when is_atom(cte_name), do: {:ok, Atom.to_string(cte_name)}

  defp normalize_cte_name(cte_name) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected CTE name to be an atom or string, got: #{inspect(cte_name)}"
    )

    :error
  end

  defp normalize_cte_definition(cte_name, cte_definition)
       when is_map(cte_definition) and not is_struct(cte_definition) do
    normalize_cte_definition(cte_name, Map.to_list(cte_definition))
  end

  defp normalize_cte_definition(_cte_name, cte_definition) when is_list(cte_definition) do
    if Keyword.keyword?(cte_definition) do
      {:ok, cte_definition}
    else
      :error
    end
  end

  defp normalize_cte_definition(cte_name, cte_definition) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected CTE definition for #{inspect(cte_name)} to be a keyword list/map, got: #{inspect(cte_definition)}"
    )

    :error
  end

  defp normalize_cte_query(schema_source, cte_name, cte_definition, opts) do
    case Keyword.fetch(cte_definition, :as) do
      {:ok, as_value} ->
        case as_value do
          %Ecto.Query{} = query ->
            {:ok, query}

          %Ecto.SubQuery{} = query ->
            {:ok, query}

          query_params when is_map(query_params) and not is_struct(query_params) ->
            from_source = Map.get(query_params, :from, schema_source)
            filter_params = Map.get(query_params, :query, [])
            {:ok, CommonFilters.convert_params_to_filter(from_source, filter_params, opts)}

          query_params when is_list(query_params) ->
            if Keyword.keyword?(query_params) do
              from_source = Keyword.get(query_params, :from, schema_source)
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

  defp normalize_materialized(cte_name, cte_definition) do
    case Keyword.get(cte_definition, :materialized) do
      value when is_nil(value) or is_boolean(value) ->
        {:ok, value}

      value ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected :materialized for #{inspect(cte_name)} to be nil or boolean, got: #{inspect(value)}"
        )

        :error
    end
  end

  defp normalize_operation(cte_name, cte_definition) do
    case Keyword.get(cte_definition, :operation) do
      nil ->
        {:ok, nil}

      operation when operation in @cte_operations ->
        {:ok, operation}

      operation ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected :operation for #{inspect(cte_name)} to be one of #{inspect(@cte_operations)}, got: #{inspect(operation)}"
        )

        :error
    end
  end
end
