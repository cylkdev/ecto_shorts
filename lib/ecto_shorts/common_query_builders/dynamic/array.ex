defmodule EctoShorts.CommonFilters.CommonQueryBuilders.Dynamic.Array do
  alias Ecto.Query

  require Ecto.Query

  def where(current_binding, value, :==, schema_field) when is_atom(schema_field) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], ^value in field(q, ^schema_field))
    else
      Query.dynamic([q], ^value in field(q, ^schema_field))
    end
  end

  def where(current_binding, schema_field, :==, values) when is_list(values) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) == ^values)
    else
      Query.dynamic([q], field(q, ^schema_field) == ^values)
    end
  end
end
