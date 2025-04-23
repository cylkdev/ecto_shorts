defmodule EctoShorts.QueryBuilders.Common do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  alias EctoShorts.QueryBuilders.{Expression, DynamicExpression}

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
  ...
  """
  def filters, do: @filters

  @impl EctoShorts.QueryBuilder
  @doc """
  ...
  """
  def build_query(query, current_binding, _schema_module, :ids, values) do
    DynamicExpression.where(query, current_binding, :id, :==, values)
  end

  def build_query(query, current_binding, _schema_module, :first, value) do
    Expression.limit(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :last, value) do
    query
    |> Expression.exclude(:order_by)
    |> Expression.order_by(current_binding, order_by: [desc: :inserted_at])
    |> Expression.limit(current_binding, value)
    |> Expression.subquery()
    |> Expression.order_by(current_binding, :id)
  end

  def build_query(query, current_binding, _schema_module, :limit, value) do
    Expression.limit(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :offset, value) do
    Expression.offset(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :order_by, value) do
    Expression.order_by(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :preload, value) do
    Expression.preload(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :after, value) do
    DynamicExpression.where(query, current_binding, :id, :>, value)
  end

  def build_query(query, current_binding, _schema_module, :before, value) do
    DynamicExpression.where(query, current_binding, :id, :<, value)
  end

  def build_query(query, current_binding, _schema_module, :since, value) do
    DynamicExpression.where(query, current_binding, :inserted_at, :>=, value)
  end

  def build_query(query, current_binding, _schema_module, :until, value) do
    DynamicExpression.where(query, current_binding, :inserted_at, :<=, value)
  end

  def build_query(query, current_binding, _schema_module, :start_date, value) do
    DynamicExpression.where(query, current_binding, :inserted_at, :>=, value)
  end

  def build_query(query, current_binding, _schema_module, :end_date, value) do
    DynamicExpression.where(query, current_binding, :inserted_at, :<=, value)
  end

  def build_query(query, schema_module, :search, value) do
    if function_exported?(schema_module, :by_search, 2) do
      schema_module.by_search(query, value)
    else
      query
    end
  end
end
