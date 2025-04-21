defmodule EctoShorts.CommonFilters.CommonQueryBuilders.Dynamic.Schema do
  alias Ecto.Query

  require Ecto.Query

  def where(current_binding, true) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], q)
    else
      Query.dynamic([q], q)
    end
  end

  def where(current_binding, schema_field, :==, values) when is_list(values) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) in ^values)
    else
      Query.dynamic([q], field(q, ^schema_field) in ^values)
    end
  end

  def where(current_binding, schema_field, :==, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) == ^value)
    else
      Query.dynamic([q], field(q, ^schema_field) == ^value)
    end
  end
end
