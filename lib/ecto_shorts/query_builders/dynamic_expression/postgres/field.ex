defmodule EctoShorts.QueryBuilders.DynamicExpression.Postgres.Field do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  alias Ecto.Query

  require Ecto.Query

  def dynamic_expression(current_binding, true) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], q)
    else
      Query.dynamic([q], q)
    end
  end

  def dynamic_expression(current_binding, schema_field, :=~, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], fragment("? ~* ?", field(q, ^schema_field), ^value))
    else
      Query.dynamic([q], fragment("? ~* ?", field(q, ^schema_field), ^value))
    end
  end

  def dynamic_expression(current_binding, schema_field, :ilike, value) do
    pattern = "%#{value}%"

    if current_binding do
      Query.dynamic([{^current_binding, q}], ilike(field(q, ^schema_field), ^pattern))
    else
      Query.dynamic([q], ilike(field(q, ^schema_field), ^pattern))
    end
  end

  def dynamic_expression(current_binding, schema_field, :like, value) do
    pattern = "%#{value}%"

    if current_binding do
      Query.dynamic([{^current_binding, q}], like(field(q, ^schema_field), ^pattern))
    else
      Query.dynamic([q], like(field(q, ^schema_field), ^pattern))
    end
  end

  def dynamic_expression(current_binding, schema_field, :<, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) < ^value)
    else
      Query.dynamic([q], field(q, ^schema_field) < ^value)
    end
  end

  def dynamic_expression(current_binding, schema_field, :>, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) > ^value)
    else
      Query.dynamic([q], field(q, ^schema_field) > ^value)
    end
  end

  def dynamic_expression(current_binding, schema_field, :<=, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) <= ^value)
    else
      Query.dynamic([q], field(q, ^schema_field) <= ^value)
    end
  end

  def dynamic_expression(current_binding, schema_field, :>=, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) >= ^value)
    else
      Query.dynamic([q], field(q, ^schema_field) >= ^value)
    end
  end

  def dynamic_expression(current_binding, schema_field, :!=, {:lower, value}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("LOWER(?)", field(q, ^schema_field)) != ^value
      )
    else
      Query.dynamic([q], fragment("LOWER(?)", field(q, ^schema_field)) != ^value)
    end
  end

  def dynamic_expression(current_binding, schema_field, :!=, {:upper, value}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("UPPER(?)", field(q, ^schema_field)) != ^value
      )
    else
      Query.dynamic([q], fragment("UPPER(?)", field(q, ^schema_field)) != ^value)
    end
  end

  def dynamic_expression(current_binding, schema_field, :==, {:lower, value}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("LOWER(?)", field(q, ^schema_field)) == ^value
      )
    else
      Query.dynamic([q], fragment("LOWER(?)", field(q, ^schema_field)) == ^value)
    end
  end

  def dynamic_expression(current_binding, schema_field, :==, {:upper, value}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("UPPER(?)", field(q, ^schema_field)) == ^value
      )
    else
      Query.dynamic([q], fragment("UPPER(?)", field(q, ^schema_field)) == ^value)
    end
  end

  def dynamic_expression(current_binding, schema_field, :!=, nil) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], not is_nil(field(q, ^schema_field)))
    else
      Query.dynamic([q], not is_nil(field(q, ^schema_field)))
    end
  end

  def dynamic_expression(current_binding, schema_field, :!=, values) when is_list(values) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) not in ^values)
    else
      Query.dynamic([q], field(q, ^schema_field) not in ^values)
    end
  end

  def dynamic_expression(current_binding, schema_field, :!=, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) != ^value)
    else
      Query.dynamic([q], field(q, ^schema_field) != ^value)
    end
  end

  def dynamic_expression(current_binding, schema_field, :==, nil) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], is_nil(field(q, ^schema_field)))
    else
      Query.dynamic([q], is_nil(field(q, ^schema_field)))
    end
  end

  def dynamic_expression(current_binding, schema_field, :==, values) when is_list(values) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) in ^values)
    else
      Query.dynamic([q], field(q, ^schema_field) in ^values)
    end
  end

  def dynamic_expression(current_binding, schema_field, :==, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) == ^value)
    else
      Query.dynamic([q], field(q, ^schema_field) == ^value)
    end
  end
end
