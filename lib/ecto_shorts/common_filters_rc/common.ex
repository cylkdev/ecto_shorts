defmodule EctoShorts.CommonFiltersRc.Common do
  @moduledoc """
  ...
  """

  alias EctoShorts.CommonFiltersRc.QueryExpression

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

  def filters, do: @filters

  def create_schema_filter(query, _schema_module, :preload, expr, current_binding) do
    expr = if is_map(expr), do: Map.to_list(expr), else: expr

    QueryExpression.preload(query, current_binding, expr)
  end

  def create_schema_filter(query, _schema_module, :after, value, current_binding) do
    QueryExpression.where(query, current_binding, %{id: %{>: value}})
  end

  def create_schema_filter(query, _schema_module, :before, value, current_binding) do
    QueryExpression.where(query, current_binding, %{id: %{<: value}})
  end

  def create_schema_filter(query, _schema_module, :since, value, current_binding) do
    QueryExpression.where(query, current_binding, %{inserted_at: %{>=: value}})
  end

  def create_schema_filter(query, _schema_module, :until, value, current_binding) do
    QueryExpression.where(query, current_binding, %{inserted_at: %{<=: value}})
  end

  def create_schema_filter(query, schema_module, :start_date, value, current_binding) do
    create_schema_filter(query, schema_module, :since, value, current_binding)
  end

  def create_schema_filter(query, schema_module, :end_date, value, current_binding) do
    create_schema_filter(query, schema_module, :until, value, current_binding)
  end

  def create_schema_filter(query, _schema_module, :ids, values, current_binding) do
    QueryExpression.where(query, current_binding, %{id: %{==: values}})
  end

  def create_schema_filter(query, _schema_module, :offset, value, current_binding) do
    QueryExpression.offset(query, current_binding, value)
  end

  def create_schema_filter(query, _schema_module, :limit, value, current_binding) do
    QueryExpression.limit(query, current_binding, value)
  end

  def create_schema_filter(query, _schema_module, :first, value, current_binding) do
    QueryExpression.limit(query, current_binding, value)
  end

  def create_schema_filter(query, _schema_module, :last, value, current_binding) do
    query
    |> QueryExpression.exclude(:order_by)
    |> QueryExpression.from(order_by: [desc: :inserted_at, limit: value])
    |> QueryExpression.subquery([])
    |> QueryExpression.order_by(current_binding, :id)
  end

  def create_schema_filter(query, schema_module, :search, value, current_binding) do
    cond do
      Code.ensure_loaded?(schema_module) and function_exported?(schema_module, :by_search, 3) ->
        schema_module.by_search(query, value, current_binding)

      Code.ensure_loaded?(schema_module) and function_exported?(schema_module, :by_search, 2) ->
        schema_module.by_search(query, value)

      true ->
        EctoShorts.Utils.Logger.warning(
          __MODULE__,
          "The schema module #{inspect(schema_module)} does not export the function `by_search/2`."
        )

        query
    end
  end
end
