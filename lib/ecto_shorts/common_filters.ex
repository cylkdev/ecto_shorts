defmodule EctoShorts.CommonFilters do
  @moduledoc """
  ...
  """

  alias EctoShorts.{
    CommonSchemas,
    QueryBuilder,
    QueryBuilder.Common,
    QueryBuilder.Schema
  }

  @behaviour EctoShorts.QueryBuilder

  @common_filters Common.filters()

  @doc """
  ...
  """
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
    convert_params_to_filter(
      query,
      schema_module,
      Map.new(params),
      opts
    )
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
  def build_query(query, current_binding, schema_module, key, value)
      when key in @common_filters do
    Common.build_query(query, current_binding, schema_module, key, value)
  end

  def build_query(query, current_binding, schema_module, key, value) do
    Schema.build_query(query, current_binding, schema_module, key, value)
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
