defmodule EctoShorts.CommonFilters do
  @moduledoc """
  ...
  """

  alias EctoShorts.CommonSchemas
  alias EctoShorts.{QueryBuilder, QueryBuilders}

  @behaviour EctoShorts.QueryBuilder

  def convert_params_to_filter(query, params, opts \\ [])

  def convert_params_to_filter(query, params, opts) do
    convert_params_to_filter(
      query,
      CommonSchemas.get_schema_queryable(query),
      params,
      opts
    )
  end

  def convert_params_to_filter(query, schema_module, params, opts) when is_list(params) do
    convert_params_to_filter(query, schema_module, Map.new(params), opts)
  end

  def convert_params_to_filter(query, _schema_module, params, _opts) when params === %{} do
    query
  end

  def convert_params_to_filter(query, schema_module, params, opts) do
    params
    |> Map.to_list()
    |> ensure_last_is_final_filter()
    |> Enum.reduce(query, fn {key, value}, query ->
      QueryBuilder.build_query(
        query,
        params[:as],
        schema_module,
        key,
        value,
        opts
      )
    end)
  end

  @impl EctoShorts.QueryBuilder
  @doc """
  ...
  """
  def build_query(query, current_binding, schema_module, key, value) do
    if QueryBuilders.common_filter?(key) do
      QueryBuilders.build_common_query(query, current_binding, schema_module, key, value)
    else
      QueryBuilders.build_schema_query(query, current_binding, schema_module, key, value)
    end
  end

  defp ensure_last_is_final_filter(params) do
    if Keyword.has_key?(params, :last) do
      params
      |> Keyword.delete(:last)
      |> Kernel.++(last: params[:last])
    else
      params
    end
  end
end
