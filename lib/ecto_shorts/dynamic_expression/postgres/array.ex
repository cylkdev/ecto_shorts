defmodule EctoShorts.DynamicExpression.Postgres.Array do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Provides dynamic filter expressions for Postgres array fields using
  Ecto's query DSL.

  This module supports both element-wise pattern matching and array
  comparison operators. It's used by the `QueryBuilder` layer to
  compose dynamic `where` and `or_where` clauses involving array fields.

  ## Supported operations

  - Pattern matches:
    * `:like` — case-sensitive substring match on any array element
    * `:ilike` — case-insensitive substring match on any array element
    * `:=~` — case-insensitive regex match on any array element

  - Comparisons:
    * `:==`, `:!=` — equality and inequality, supports `nil`, lists, or single values
    * `:<`, `:>`, `:<=`, `:>=` — scalar-to-array or array-to-array comparisons

  ## Examples

      iex> Array.where(:user, "foo", :ilike, :tags)
      # matches if any tag ILIKE '%foo%'

      iex> Array.where(:user, 2, :==, :roles)
      # true if 2 is in roles

      iex> Array.where(:user, :roles, :!=, [1, 2])
      # true if roles != [1, 2]

      iex> Array.where(:user, :tags, :==, nil)
      # true if tags is null
  """

  alias Ecto.Query

  require Ecto.Query

  @type dynamic_expr :: Ecto.Query.dynamic_expr()
  @type binding() :: atom()
  @type operator :: atom()
  @type field :: atom()

  @doc """
  Builds a dynamic expression for comparing or pattern-matching values against a
  Postgres array field.

  This function handles scalar-to-array comparisons (`value in array`),
  array-to-array comparisons (`array == array`), null checks, and
  `LIKE`/`ILIKE`/regex matches on array elements.

  ## Operator behavior

    * `:==`, `:!=` — Supports `nil`, single values, and full array comparisons
    * `:<`, `:>`, `:<=`, `:>=` — Can compare scalar to any element or full arrays
    * `:like`, `:ilike`, `:=~` — Perform `LIKE ANY`, `ILIKE ANY`, or `~* ANY`

  ## Binding behavior

  If `binding` is provided, a named binding is used in the query;
  otherwise the default `[q]` is used.

  ## Examples

      iex> Array.where(:user, "foo", :ilike, :tags)
      iex> Array.where(:user, :roles, :!=, [1, 2])
      iex> Array.where(nil, :tags, :==, nil)
      iex> Array.where(nil, 3, :==, :permissions)

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
