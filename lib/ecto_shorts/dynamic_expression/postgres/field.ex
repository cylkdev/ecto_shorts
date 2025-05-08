defmodule EctoShorts.DynamicExpression.Postgres.Field do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Provides dynamic filter expressions for scalar fields in Postgres
  schemas using Ecto's query DSL.

  This module supports common comparison and pattern-matching operators,
  including case-insensitive variants and explicit casing control using
  `{:lower, value}` or `{:upper, value}` formats.

  These helpers are designed to be used internally by
  `EctoShorts.DynamicExpression.Postgres` to support advanced
  `where` and `or_where` filtering logic.

  ## Supported operators

    * `:==`, `:!=` — equality and inequality (supports `nil`, lists, and casing tuples)
    * `:<`, `:>`, `:<=`, `:>=` — numeric comparisons
    * `:like`, `:ilike` — string pattern matching
    * `:=~` — regex-style case-insensitive match (uses `~*`)

  ## Examples

      iex> Field.where(:user, :age, :<, 30)
      #Ecto.Query.DynamicExpr<...>

      iex> Field.where(:user, :name, :ilike, "alice")
      #Ecto.Query.DynamicExpr<...>

      iex> Field.where(:user, :status, :==, {:lower, "active"})
      #Ecto.Query.DynamicExpr<...>
  """
  alias Ecto.Query

  require Ecto.Query

  @type dynamic_expr :: Ecto.Query.dynamic_expr()

  @type binding() :: atom()

  @type operator :: atom()

  @type field :: atom()

  @doc """
  Builds a dynamic query expression for a scalar field and operator.

  This function supports:

    * Comparison operators:
      * `:==`, `:!=`, `:<`, `:>`, `:<=`, `:>=`

    * Case-insensitive pattern matches:
      * `:like`, `:ilike`, `:=~`

    * Case-specific equality:
      * `{:lower, value}` or `{:upper, value}` with `:==` or `:!=`

    * Null-safe operations:
      * `:==` or `:!=` with `nil`

    * List operations:
      * `:==` or `:!=` with a list for `IN` or `NOT IN` clauses

  If a binding name is provided, the query will use it in the pattern
  `[^{binding}, q]`; otherwise it uses `[q]`.

  ## Examples

      iex> Field.where(:user, :name, :ilike, "foo")
      iex> Field.where(:user, :name, :==, {:lower, "foo"})
      iex> Field.where(:user, :tags, :=~, "bar")
      iex> Field.where(nil, :score, :>=, 100)
      iex> Field.where(:user, :status, :!=, nil)
      iex> Field.where(:user, :role, :==, ["admin", "guest"])
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
