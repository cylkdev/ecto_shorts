defmodule EctoShorts.DynamicBuilders.Postgres.Array do
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

      iex> Array.create_dynamic(:user, "foo", :ilike, :tags)
      # matches if any tag ILIKE '%foo%'

      iex> Array.create_dynamic(:user, 2, :==, :roles)
      # true if 2 is in roles

      iex> Array.create_dynamic(:user, :roles, :!=, [1, 2])
      # true if roles != [1, 2]

      iex> Array.create_dynamic(:user, :tags, :==, nil)
      # true if tags is null
  """

  alias Ecto.Query

  require Ecto.Query

  @type dynamic_expr :: Ecto.Query.dynamic_expr()
  @type binding_alias :: atom()

  @type operator :: atom()

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

      iex> Array.create_dynamic(:user, "foo", :ilike, :tags)
      iex> Array.create_dynamic(:user, :roles, :!=, [1, 2])
      iex> Array.create_dynamic(nil, :tags, :==, nil)
      iex> Array.create_dynamic(nil, 3, :==, :permissions)

  """
  @spec create_dynamic(binding_alias(), any(), operator(), any()) :: dynamic_expr()
  def create_dynamic(binding_alias, value, :=~, key) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("? ~* ANY(?)", ^value, field(q, ^key))
      )
    else
      Query.dynamic([q], fragment("? ~* ANY(?)", ^value, field(q, ^key)))
    end
  end

  def create_dynamic(binding_alias, value, :ilike, key) do
    pattern = "%#{value}%"

    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("? ILIKE ANY(?)", ^pattern, field(q, ^key))
      )
    else
      Query.dynamic([q], fragment("? ILIKE ANY(?)", ^pattern, field(q, ^key)))
    end
  end

  def create_dynamic(binding_alias, value, :like, key) do
    pattern = "%#{value}%"

    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("? LIKE ANY(?)", ^pattern, field(q, ^key))
      )
    else
      Query.dynamic([q], fragment("? LIKE ANY(?)", ^pattern, field(q, ^key)))
    end
  end

  def create_dynamic(binding_alias, key, :<, values) when is_list(values) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) < ^values)
    else
      Query.dynamic([q], field(q, ^key) < ^values)
    end
  end

  def create_dynamic(binding_alias, value, :<, key) when is_atom(key) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("? < ANY(?)", ^value, field(q, ^key))
      )
    else
      Query.dynamic([q], fragment("? < ANY(?)", ^value, field(q, ^key)))
    end
  end

  def create_dynamic(binding_alias, key, :>, values) when is_list(values) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) > ^values)
    else
      Query.dynamic([q], field(q, ^key) > ^values)
    end
  end

  def create_dynamic(binding_alias, value, :>, key) when is_atom(key) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("? > ANY(?)", ^value, field(q, ^key))
      )
    else
      Query.dynamic([q], fragment("? > ANY(?)", ^value, field(q, ^key)))
    end
  end

  def create_dynamic(binding_alias, key, :<=, values) when is_list(values) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) <= ^values)
    else
      Query.dynamic([q], field(q, ^key) <= ^values)
    end
  end

  def create_dynamic(binding_alias, value, :<=, key) when is_atom(key) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("? <= ANY(?)", ^value, field(q, ^key))
      )
    else
      Query.dynamic([q], fragment("? <= ANY(?)", ^value, field(q, ^key)))
    end
  end

  def create_dynamic(binding_alias, key, :>=, values) when is_list(values) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) >= ^values)
    else
      Query.dynamic([q], field(q, ^key) >= ^values)
    end
  end

  def create_dynamic(binding_alias, value, :>=, key) when is_atom(key) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("? >= ANY(?)", ^value, field(q, ^key))
      )
    else
      Query.dynamic([q], fragment("? >= ANY(?)", ^value, field(q, ^key)))
    end
  end

  def create_dynamic(binding_alias, key, :!=, nil) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
    else
      Query.dynamic([q], not is_nil(field(q, ^key)))
    end
  end

  def create_dynamic(binding_alias, key, :!=, values) when is_list(values) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) != ^values)
    else
      Query.dynamic([q], field(q, ^key) != ^values)
    end
  end

  def create_dynamic(binding_alias, value, :!=, key) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], ^value not in field(q, ^key))
    else
      Query.dynamic([q], ^value not in field(q, ^key))
    end
  end

  def create_dynamic(binding_alias, key, :==, nil) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    else
      Query.dynamic([q], is_nil(field(q, ^key)))
    end
  end

  def create_dynamic(binding_alias, key, :==, values) when is_list(values) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) == ^values)
    else
      Query.dynamic([q], field(q, ^key) == ^values)
    end
  end

  def create_dynamic(binding_alias, value, :==, key) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], ^value in field(q, ^key))
    else
      Query.dynamic([q], ^value in field(q, ^key))
    end
  end
end
