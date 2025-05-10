defmodule EctoShorts.DynamicBuilders.Postgres.Field do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Provides dynamic filter expressions for scalar fields in Postgres
  schemas using Ecto's query DSL.

  This module supports common comparison and pattern-matching operators,
  including case-insensitive variants and explicit casing control using
  `{:lower, value}` or `{:upper, value}` formats.

  These helpers are designed to be used internally by
  `EctoShorts.DynamicBuilders.Postgres` to support advanced
  `where` and `or_where` filtering logic.

  ## Supported operators

    * `:==`, `:!=` — equality and inequality (supports `nil`, lists, and casing tuples)
    * `:<`, `:>`, `:<=`, `:>=` — numeric comparisons
    * `:like`, `:ilike` — string pattern matching
    * `:=~` — regex-style case-insensitive match (uses `~*`)

  ## Examples

      iex> Field.dynamic(:user, :age, :<, 30)
      #Ecto.Query.DynamicExpr<...>

      iex> Field.dynamic(:user, :name, :ilike, "alice")
      #Ecto.Query.DynamicExpr<...>

      iex> Field.dynamic(:user, :status, :==, {:lower, "active"})
      #Ecto.Query.DynamicExpr<...>
  """
  alias Ecto.Query

  require Ecto.Query

  @type dynamic_expr :: Ecto.Query.dynamic_expr()
  @type binding_alias :: atom()

  @type operator :: atom()

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

      iex> Field.dynamic(:user, :name, :ilike, "foo")
      iex> Field.dynamic(:user, :name, :==, {:lower, "foo"})
      iex> Field.dynamic(:user, :tags, :=~, "bar")
      iex> Field.dynamic(nil, :score, :>=, 100)
      iex> Field.dynamic(:user, :status, :!=, nil)
      iex> Field.dynamic(:user, :role, :==, ["admin", "guest"])
  """
  @spec dynamic(binding_alias(), any(), operator(), any()) :: dynamic_expr()
  def dynamic(binding_alias, key, :=~, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], fragment("? ~* ?", field(q, ^key), ^value))
    else
      Query.dynamic([q], fragment("? ~* ?", field(q, ^key), ^value))
    end
  end

  def dynamic(binding_alias, key, :ilike, value) do
    pattern = "%#{value}%"

    if binding_alias do
      Query.dynamic([{^binding_alias, q}], ilike(field(q, ^key), ^pattern))
    else
      Query.dynamic([q], ilike(field(q, ^key), ^pattern))
    end
  end

  def dynamic(binding_alias, key, :like, value) do
    pattern = "%#{value}%"

    if binding_alias do
      Query.dynamic([{^binding_alias, q}], like(field(q, ^key), ^pattern))
    else
      Query.dynamic([q], like(field(q, ^key), ^pattern))
    end
  end

  def dynamic(binding_alias, key, :<, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) < ^value)
    else
      Query.dynamic([q], field(q, ^key) < ^value)
    end
  end

  def dynamic(binding_alias, key, :>, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) > ^value)
    else
      Query.dynamic([q], field(q, ^key) > ^value)
    end
  end

  def dynamic(binding_alias, key, :<=, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) <= ^value)
    else
      Query.dynamic([q], field(q, ^key) <= ^value)
    end
  end

  def dynamic(binding_alias, key, :>=, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) >= ^value)
    else
      Query.dynamic([q], field(q, ^key) >= ^value)
    end
  end

  def dynamic(binding_alias, key, :!=, {:lower, value}) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("LOWER(?)", field(q, ^key)) != ^value
      )
    else
      Query.dynamic([q], fragment("LOWER(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic(binding_alias, key, :!=, {:upper, value}) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("UPPER(?)", field(q, ^key)) != ^value
      )
    else
      Query.dynamic([q], fragment("UPPER(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic(binding_alias, key, :==, {:lower, value}) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("LOWER(?)", field(q, ^key)) == ^value
      )
    else
      Query.dynamic([q], fragment("LOWER(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic(binding_alias, key, :==, {:upper, value}) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("UPPER(?)", field(q, ^key)) == ^value
      )
    else
      Query.dynamic([q], fragment("UPPER(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic(binding_alias, key, :!=, nil) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
    else
      Query.dynamic([q], not is_nil(field(q, ^key)))
    end
  end

  def dynamic(binding_alias, key, :!=, values) when is_list(values) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) not in ^values)
    else
      Query.dynamic([q], field(q, ^key) not in ^values)
    end
  end

  def dynamic(binding_alias, key, :!=, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
    else
      Query.dynamic([q], field(q, ^key) != ^value)
    end
  end

  def dynamic(binding_alias, key, :==, nil) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    else
      Query.dynamic([q], is_nil(field(q, ^key)))
    end
  end

  def dynamic(binding_alias, key, :==, values) when is_list(values) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) in ^values)
    else
      Query.dynamic([q], field(q, ^key) in ^values)
    end
  end

  def dynamic(binding_alias, key, :==, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    else
      Query.dynamic([q], field(q, ^key) == ^value)
    end
  end
end
