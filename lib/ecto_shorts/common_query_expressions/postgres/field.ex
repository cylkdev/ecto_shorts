defmodule EctoShorts.CommonQueryExpressions.Postgres.Field do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Provides helper functions for building Postgres-specific field query
  expressions in Ecto. These helpers enable advanced filtering and
  comparison operations on individual fields, supporting various Postgres
  operators and use cases.

  These functions are used internally by the QueryBuilder to support
  Postgres-specific filtering, enabling more complex and nuanced queries.
  """

  alias Ecto.Query

  require Ecto.Query

  @type dynamic_expr :: Ecto.Query.dynamic_expr()

  @type binding() :: atom()

  @type operator :: atom()

  @type field :: atom()

  @doc """
  Builds a dynamic expression for a comparison or match on a field.

  Supported operators:
  - `:<`, `:>`, `:<=`, `:>=` for less/greater-than comparisons
  - `:==`, `:!=` for equality/inequality (including nil and list values)
  - `:=~` for case-insensitive regex match using Postgres `~*`
  - `:like`, `:ilike` for (case-insensitive) string pattern matches
  - `{:lower, value}`/`{:upper, value}` for case-insensitive equality/inequality

  ## Examples
      iex> where(:user, :age, :<, 30)
      #Ecto.Query.DynamicExpr<...>
      iex> where(:user, :name, :ilike, "foo")
      #Ecto.Query.DynamicExpr<...>
      iex> where(:user, :name, :==, {:lower, "john"})
      #Ecto.Query.DynamicExpr<...>
      iex> where(:user, :name, :!=, nil)
      #Ecto.Query.DynamicExpr<...>
      iex> where(:user, :age, :==, [18, 21])
      #Ecto.Query.DynamicExpr<...>
  """
  @spec where(binding() | nil, any(), operator(), any()) :: dynamic_expr()
  def where(current_binding, schema_field, :=~, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], fragment("? ~* ?", field(q, ^schema_field), ^value))
    else
      Query.dynamic([q], fragment("? ~* ?", field(q, ^schema_field), ^value))
    end
  end

  def where(current_binding, schema_field, :ilike, value) do
    pattern = "%#{value}%"

    if current_binding do
      Query.dynamic([{^current_binding, q}], ilike(field(q, ^schema_field), ^pattern))
    else
      Query.dynamic([q], ilike(field(q, ^schema_field), ^pattern))
    end
  end

  def where(current_binding, schema_field, :like, value) do
    pattern = "%#{value}%"

    if current_binding do
      Query.dynamic([{^current_binding, q}], like(field(q, ^schema_field), ^pattern))
    else
      Query.dynamic([q], like(field(q, ^schema_field), ^pattern))
    end
  end

  def where(current_binding, schema_field, :<, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) < ^value)
    else
      Query.dynamic([q], field(q, ^schema_field) < ^value)
    end
  end

  def where(current_binding, schema_field, :>, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) > ^value)
    else
      Query.dynamic([q], field(q, ^schema_field) > ^value)
    end
  end

  def where(current_binding, schema_field, :<=, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) <= ^value)
    else
      Query.dynamic([q], field(q, ^schema_field) <= ^value)
    end
  end

  def where(current_binding, schema_field, :>=, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) >= ^value)
    else
      Query.dynamic([q], field(q, ^schema_field) >= ^value)
    end
  end

  def where(current_binding, schema_field, :!=, {:lower, value}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("LOWER(?)", field(q, ^schema_field)) != ^value
      )
    else
      Query.dynamic([q], fragment("LOWER(?)", field(q, ^schema_field)) != ^value)
    end
  end

  def where(current_binding, schema_field, :!=, {:upper, value}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("UPPER(?)", field(q, ^schema_field)) != ^value
      )
    else
      Query.dynamic([q], fragment("UPPER(?)", field(q, ^schema_field)) != ^value)
    end
  end

  def where(current_binding, schema_field, :==, {:lower, value}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("LOWER(?)", field(q, ^schema_field)) == ^value
      )
    else
      Query.dynamic([q], fragment("LOWER(?)", field(q, ^schema_field)) == ^value)
    end
  end

  def where(current_binding, schema_field, :==, {:upper, value}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("UPPER(?)", field(q, ^schema_field)) == ^value
      )
    else
      Query.dynamic([q], fragment("UPPER(?)", field(q, ^schema_field)) == ^value)
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
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) not in ^values)
    else
      Query.dynamic([q], field(q, ^schema_field) not in ^values)
    end
  end

  def where(current_binding, schema_field, :!=, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^schema_field) != ^value)
    else
      Query.dynamic([q], field(q, ^schema_field) != ^value)
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
