defmodule EctoShorts.CommonFilters.WithCte do
  alias Ecto.Query
  alias EctoShorts.CommonFilters
  alias EctoShorts.Logger

  @logger_prefix "EctoShorts.CommonFilters.WithCte"

  def build_query(:with_cte, schema_source, query, _selected_binding, params, opts) do
    reduce_entries(schema_source, query, params, opts)
  end

  defp reduce_entries(schema_source, query, params, opts)
       when is_map(params) and not is_struct(params) do
    reduce_entries(schema_source, query, Map.to_list(params), opts)
  end

  defp reduce_entries(schema_source, query, params, opts) when is_list(params) do
    if Keyword.keyword?(params) do
      Enum.reduce(params, query, fn {cte_name, cte_definition}, query_acc ->
        apply_entry(schema_source, query_acc, cte_name, cte_definition, opts)
      end)
    else
      Enum.reduce(params, query, fn
        {cte_name, cte_definition}, query_acc ->
          apply_entry(schema_source, query_acc, cte_name, cte_definition, opts)

        other, query_acc ->
          Logger.warning(
            @logger_prefix,
            "Expected :with_cte params to be a map or keyword list, got: #{inspect(other)}"
          )

          query_acc
      end)
    end
  end

  defp reduce_entries(_schema_source, query, value, _opts) do
    Logger.warning(
      @logger_prefix,
      "Expected :with_cte params to be a map or keyword list, got: #{inspect(value)}"
    )

    query
  end

  defp apply_entry(schema_source, query, cte_name, cte_definition, opts) do
    with {:ok, normalized_name} <- normalize_cte_name(cte_name),
         {:ok, normalized_definition} <- normalize_definition(cte_name, cte_definition),
         {:ok, cte_query} <-
           build_cte_query(schema_source, normalized_name, normalized_definition, opts),
         {:ok, materialized} <- fetch_materialized(normalized_name, normalized_definition) do
      apply_cte(query, normalized_name, cte_query, materialized)
    else
      :error -> query
    end
  end

  defp normalize_cte_name(cte_name) when is_atom(cte_name), do: {:ok, Atom.to_string(cte_name)}
  defp normalize_cte_name(cte_name) when is_binary(cte_name), do: {:ok, cte_name}

  defp normalize_cte_name(cte_name) do
    Logger.warning(
      @logger_prefix,
      "Expected :with_cte key to be an atom or string, got: #{inspect(cte_name)}"
    )

    :error
  end

  defp normalize_definition(cte_name, cte_definition)
       when is_map(cte_definition) and not is_struct(cte_definition) do
    normalize_definition(cte_name, Map.to_list(cte_definition))
  end

  defp normalize_definition(_cte_name, cte_definition) when is_list(cte_definition) do
    if Keyword.keyword?(cte_definition) do
      {:ok, cte_definition}
    else
      :error
    end
  end

  defp normalize_definition(cte_name, cte_definition) do
    Logger.warning(
      @logger_prefix,
      "Expected :with_cte params for #{inspect(cte_name)} to be a map or keyword list, got: #{inspect(cte_definition)}"
    )

    :error
  end

  defp build_cte_query(schema_source, cte_name, cte_definition, opts) do
    case Keyword.fetch(cte_definition, :as) do
      {:ok, %Ecto.Query{} = query} ->
        {:ok, query}

      {:ok, %Ecto.SubQuery{} = query} ->
        {:ok, query}

      {:ok, query_params} when is_map(query_params) and not is_struct(query_params) ->
        {:ok, params_to_query(schema_source, query_params, opts)}

      {:ok, query_params} when is_list(query_params) ->
        {:ok, params_to_query(schema_source, query_params, opts)}

      {:ok, term} ->
        Logger.warning(
          @logger_prefix,
          "Expected CTE :as query params for #{inspect(cte_name)} to be a query, subquery, or keyword/map payload, got: #{inspect(term)}"
        )

        :error

      :error ->
        Logger.warning(
          @logger_prefix,
          "Expected :with_cte params for #{inspect(cte_name)} to include an :as key"
        )

        :error
    end
  end

  defp fetch_materialized(cte_name, cte_definition) do
    case Keyword.fetch(cte_definition, :materialized) do
      {:ok, value} when is_boolean(value) ->
        {:ok, value}

      {:ok, nil} ->
        {:ok, nil}

      {:ok, value} ->
        Logger.warning(
          @logger_prefix,
          "Expected :materialized for #{inspect(cte_name)} to be a boolean, got: #{inspect(value)}"
        )

        :error

      :error ->
        {:ok, nil}
    end
  end

  defp apply_cte(query, cte_name, cte_query, nil) do
    Query.with_cte(query, ^cte_name, as: ^cte_query)
  end

  defp apply_cte(query, cte_name, cte_query, materialized) do
    Query.with_cte(query, ^cte_name, as: ^cte_query, materialized: materialized)
  end

  defp params_to_query(schema_source, query_params, opts)
       when is_map(query_params) and not is_struct(query_params) do
    params_to_query(schema_source, Map.to_list(query_params), opts)
  end

  defp params_to_query(schema_source, query_params, opts) do
    {from_source, filter_params} = Keyword.pop(query_params, :from, schema_source)
    CommonFilters.convert_params_to_filter(from_source, filter_params, opts)
  end
end
