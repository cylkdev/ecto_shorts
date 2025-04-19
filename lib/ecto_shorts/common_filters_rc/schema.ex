defmodule EctoShorts.CommonFiltersRc.Schema do
  @moduledoc false

  alias EctoShorts.CommonFiltersRc.QueryExpression

  def create_schema_filter(query, _schema_module, :from, value, _current_binding) do
    QueryExpression.from(query, value)
  end

  def create_schema_filter(query, _schema_module, :join, value, current_binding) do
    QueryExpression.join(query, current_binding, value)
  end

  def create_schema_filter(query, _schema_module, :select, value, current_binding) do
    QueryExpression.select(query, current_binding, value)
  end

  def create_schema_filter(query, _schema_module, :select_merge, value, current_binding) do
    QueryExpression.select_merge(query, current_binding, value)
  end

  def create_schema_filter(query, _schema_module, :or_where, value, current_binding) do
    QueryExpression.or_where(query, current_binding, value)
  end

  def create_schema_filter(query, _schema_module, :where, value, current_binding) do
    QueryExpression.where(query, current_binding, value)
  end

  def create_schema_filter(query, schema_module, key, value, current_binding) do
    if association?(schema_module, key) do
      create_schema_filter(
        query,
        schema_module,
        :join,
        %{association: %{key => value}},
        current_binding
      )
    else
      create_schema_filter(query, schema_module, :where, %{key => value}, current_binding)
    end
  end

  defp association?(schema_module, key) do
    key in schema_module.__schema__(:associations)
  end
end
