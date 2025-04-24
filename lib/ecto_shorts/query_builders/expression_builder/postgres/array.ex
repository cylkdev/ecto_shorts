defmodule EctoShorts.QueryBuilder.ExpressionBuilder.Postgres.Array do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  alias Ecto.Query

  require Ecto.Query

  def where(current_binding, value, :=~, schema_field) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("? ~* ANY(?)", ^value, field(q, ^schema_field))
      )
    else
      Query.dynamic([q], fragment("? ~* ANY(?)", ^value, field(q, ^schema_field)))
    end
  end

  def where(current_binding, value, :ilike, schema_field) do
    pattern = "%#{value}%"

    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("? ILIKE ANY(?)", ^pattern, field(q, ^schema_field))
      )
    else
      Query.dynamic([q], fragment("? ILIKE ANY(?)", ^pattern, field(q, ^schema_field)))
    end
  end

  def where(current_binding, value, :like, schema_field) do
    pattern = "%#{value}%"

    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("? LIKE ANY(?)", ^pattern, field(q, ^schema_field))
      )
    else
      Query.dynamic([q], fragment("? LIKE ANY(?)", ^pattern, field(q, ^schema_field)))
    end
  end

  def where(current_binding, schema_field, :<, values) when is_list(values) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) < ^values)
    else
      Query.dynamic([q], field(q, ^schema_field) < ^values)
    end
  end

  def where(current_binding, value, :<, schema_field) when is_atom(schema_field) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("? < ANY(?)", ^value, field(q, ^schema_field))
      )
    else
      Query.dynamic([q], fragment("? < ANY(?)", ^value, field(q, ^schema_field)))
    end
  end

  def where(current_binding, schema_field, :>, values) when is_list(values) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) > ^values)
    else
      Query.dynamic([q], field(q, ^schema_field) > ^values)
    end
  end

  def where(current_binding, value, :>, schema_field) when is_atom(schema_field) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("? > ANY(?)", ^value, field(q, ^schema_field))
      )
    else
      Query.dynamic([q], fragment("? > ANY(?)", ^value, field(q, ^schema_field)))
    end
  end

  def where(current_binding, schema_field, :<=, values) when is_list(values) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) <= ^values)
    else
      Query.dynamic([q], field(q, ^schema_field) <= ^values)
    end
  end

  def where(current_binding, value, :<=, schema_field) when is_atom(schema_field) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("? <= ANY(?)", ^value, field(q, ^schema_field))
      )
    else
      Query.dynamic([q], fragment("? <= ANY(?)", ^value, field(q, ^schema_field)))
    end
  end

  def where(current_binding, schema_field, :>=, values) when is_list(values) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) >= ^values)
    else
      Query.dynamic([q], field(q, ^schema_field) >= ^values)
    end
  end

  def where(current_binding, value, :>=, schema_field) when is_atom(schema_field) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("? >= ANY(?)", ^value, field(q, ^schema_field))
      )
    else
      Query.dynamic([q], fragment("? >= ANY(?)", ^value, field(q, ^schema_field)))
    end
  end

  def where(current_binding, schema_field, :!=, nil) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], not is_nil(field(q, ^schema_field)))
    else
      Query.dynamic([q], not is_nil(field(q, ^schema_field)))
    end
  end

  def where(current_binding, schema_field, :!=, values) when is_list(values) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) != ^values)
    else
      Query.dynamic([q], field(q, ^schema_field) != ^values)
    end
  end

  def where(current_binding, value, :!=, schema_field) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], ^value not in field(q, ^schema_field))
    else
      Query.dynamic([q], ^value not in field(q, ^schema_field))
    end
  end

  def where(current_binding, schema_field, :==, nil) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], is_nil(field(q, ^schema_field)))
    else
      Query.dynamic([q], is_nil(field(q, ^schema_field)))
    end
  end

  def where(current_binding, schema_field, :==, values) when is_list(values) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) == ^values)
    else
      Query.dynamic([q], field(q, ^schema_field) == ^values)
    end
  end

  def where(current_binding, value, :==, schema_field) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], ^value in field(q, ^schema_field))
    else
      Query.dynamic([q], ^value in field(q, ^schema_field))
    end
  end
end
