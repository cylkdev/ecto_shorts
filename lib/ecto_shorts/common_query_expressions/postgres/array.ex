defmodule EctoShorts.CommonQueryExpressions.Postgres.Array do
  @moduledoc since: "2.5.0"
  @moduledoc """
  # EctoShorts.CommonQueryExpressions.Postgres.Array

  Provides helper functions for building Postgres-specific array field
  query expressions in Ecto. These helpers enable pattern matching,
  case-insensitive matching, and partial matching on array fields using
  Postgres operators such as `~*`, `ILIKE ANY`, and `LIKE ANY`.

  These functions are used internally by the QueryBuilder to support
  Postgres-specific filtering, including comparisons and pattern
  matches on array fields.
  """

  alias Ecto.Query

  require Ecto.Query

  @type dynamic_expr :: Ecto.Query.dynamic_expr()

  @type binding() :: atom()

  @type operator :: atom()

  @type field :: atom()

  @doc """
  Builds a dynamic expression for comparisons and pattern matches on Postgres array fields.

  Supported operators:

  - `:=~`, `:ilike`, `:like` for case-insensitive regex and string pattern matches on any element (`~* ANY`, `ILIKE ANY`, `LIKE ANY`)

  - `:<`, `:>`, `:<=`, `:>=`, `:==`, `:!=` for value and array comparisons (with both field and value on either side)

  ## Examples
      iex> where(:user, "foo", :=~, :tags)
      #Ecto.Query.DynamicExpr<...>

      iex> where(:user, "foo", :ilike, :tags)
      #Ecto.Query.DynamicExpr<...>

      iex> where(:user, :tags, :<, [1, 2, 3])
      #Ecto.Query.DynamicExpr<...>

      iex> where(:user, 2, :<, :tags)
      #Ecto.Query.DynamicExpr<...>

      iex> where(:user, :tags, :==, nil)
      #Ecto.Query.DynamicExpr<...>

      iex> where(:user, 2, :==, :tags)
      #Ecto.Query.DynamicExpr<...>
  """
  @spec where(binding() | nil, any(), operator(), any()) :: dynamic_expr()
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
