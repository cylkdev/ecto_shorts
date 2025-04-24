defmodule EctoShorts.QueryBuilder.Common do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  alias EctoShorts.QueryBuilder.{
    QueryExpression,
    ExpressionBuilder
  }

  @filters ~w(
    after
    before
    end_date
    first
    ids
    last
    limit
    offset
    order_by
    preload
    search
    since
    start_date
    until
  )a

  @behaviour EctoShorts.QueryBuilder

  @doc """
  Returns a list of supported filters.
  """
  def filters, do: @filters

  @impl EctoShorts.QueryBuilder
  @doc """
  ...
  """
  def build_query(query, current_binding, _schema_module, :ids, values) do
    ExpressionBuilder.where(query, current_binding, :id, :==, values)
  end

  def build_query(query, current_binding, _schema_module, :first, value) do
    QueryExpression.limit(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :last, value) do
    query
    |> QueryExpression.exclude(:order_by)
    |> QueryExpression.order_by(current_binding, order_by: [desc: :inserted_at])
    |> QueryExpression.limit(current_binding, value)
    |> QueryExpression.subquery()
    |> QueryExpression.order_by(current_binding, :id)
  end

  def build_query(query, current_binding, _schema_module, :limit, value) do
    QueryExpression.limit(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :offset, value) do
    QueryExpression.offset(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :order_by, value) do
    QueryExpression.order_by(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :preload, value) do
    QueryExpression.preload(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :after, value) do
    ExpressionBuilder.where(query, current_binding, :id, :>, value)
  end

  def build_query(query, current_binding, _schema_module, :before, value) do
    ExpressionBuilder.where(query, current_binding, :id, :<, value)
  end

  def build_query(query, current_binding, _schema_module, :since, value) do
    ExpressionBuilder.where(query, current_binding, :inserted_at, :>=, value)
  end

  def build_query(query, current_binding, _schema_module, :until, value) do
    ExpressionBuilder.where(query, current_binding, :inserted_at, :<=, value)
  end

  def build_query(query, current_binding, _schema_module, :start_date, value) do
    ExpressionBuilder.where(query, current_binding, :inserted_at, :>=, value)
  end

  def build_query(query, current_binding, _schema_module, :end_date, value) do
    ExpressionBuilder.where(query, current_binding, :inserted_at, :<=, value)
  end

  def build_query(query, schema_module, :search, value) do
    if function_exported?(schema_module, :by_search, 2) do
      schema_module.by_search(query, value)
    else
      query
    end
  end
end
