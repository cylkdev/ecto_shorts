defmodule EctoShorts.QueryBuilder.QueryExpressions do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Ecto Query expression builder API

  This module provides an api for composing ecto queries and building dynamic query expressions.
  """

  alias EctoShorts.{
    QueryBuilder.Helpers,
    QueryBuilder.QueryAPI,
    QueryBuilder.QueryExpressions.Postgres
  }

  @doc """
  ...
  """
  def select(query, current_binding, params) do
    Helpers.apply_expressions(query, params, fn query, value ->
      QueryAPI.select(query, current_binding, value)
    end)
  end

  @doc """
  ...
  """
  def select_merge(query, current_binding, params) do
    Helpers.apply_expressions(query, params, fn query, value ->
      QueryAPI.select_merge(query, current_binding, value)
    end)
  end

  @doc """
  ...
  """
  def or_where(query, current_binding, params) do
    Postgres.or_where(query, current_binding, params)
  end

  @doc """
  ...
  """
  def or_where(query, current_binding, key, value) do
    Postgres.or_where(query, current_binding, key, value)
  end

  @doc """
  ...
  """
  def or_where(query, current_binding, key, operator, value) do
    Postgres.or_where(query, current_binding, key, operator, value)
  end

  @doc """
  ...
  """
  def where(query, current_binding, params) do
    Postgres.where(query, current_binding, params)
  end

  @doc """
  ...
  """
  def where(query, current_binding, key, value) do
    Postgres.where(query, current_binding, key, value)
  end

  @doc """
  ...
  """
  def where(query, current_binding, key, operator, value) do
    Postgres.where(query, current_binding, key, operator, value)
  end
end
