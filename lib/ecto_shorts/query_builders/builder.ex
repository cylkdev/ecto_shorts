defmodule EctoShorts.QueryBuilders.Builder do
  @moduledoc """
  ...
  """

  alias EctoShorts.QueryBuilder.Common
  alias EctoShorts.{
    CommonSchemas,
    QueryBuilders.CommonQueryBuilder,
    QueryBuilders.SchemaQueryBuilder
  }

  @common_filters CommonQueryBuilder.filters()

  def schema_filter_fields(queryable) do
    queryable.__schema__(:query_fields) ++ queryable.__schema__(:associations)
  end

  def schema_filter_field?(queryable, field) do
    field in schema_filter_fields(queryable)
  end

  @doc """
  EctoShorts.QueryBuilders.Builder.convert_params_to_filter(EctoShorts.Schemas.Post, %{id: 1, likes: %{>: 2}})
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, where: p0.id == ^1, where: p0.likes > ^2>

  EctoShorts.QueryBuilders.Builder.convert_params_to_filter(EctoShorts.Schemas.Post, %{id: 1, or: %{id: 2}})
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, where: p0.id == ^1, or_where: p0.id == ^2>

  EctoShorts.QueryBuilders.Builder.convert_params_to_filter(EctoShorts.Schemas.Post, %{id: 1, or: %{id: %{<: 2}}})
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, where: p0.id == ^1, or_where: p0.id < ^2>

  EctoShorts.QueryBuilders.Builder.convert_params_to_filter(EctoShorts.Schemas.Post, %{comments: %{id: 1}})
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, join: c1 in assoc(p0, :comments), on: c1.post_id == p0.id, where: c1.id == ^1>

  EctoShorts.Schemas.Post |> EctoShorts.QueryBuilders.Builder.convert_params_to_filter(comments: %{id: 1, body: "comment_body", count: %{>: 10}}) |> EctoShorts.QueryBuilders.Builder.convert_params_to_filter(%{title: "post_title"})

  EctoShorts.QueryBuilders.Builder.convert_params_to_filter(EctoShorts.Schemas.Post, %{id: 1, comments: %{id: 2, user: %{id: 3}}})
  """
  def convert_params_to_filter(query, params) when is_map(params) do
    convert_params_to_filter(query, Map.to_list(params))
  end

  def convert_params_to_filter(query, params) do
    Enum.reduce(params, query, &reduce_query/2)
  end

  defp reduce_query({key, value}, query) when key in @common_filters do
    CommonQueryBuilder.build_query(query, key, value)
  end

  defp reduce_query({key, value}, query) do
    SchemaQueryBuilder.build_query(query, key, value, :first)
  end
end
