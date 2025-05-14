defmodule EctoShorts.DynamicBuilders.Postgres.Array do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  alias Ecto.Query

  require Ecto.Query

  @type dynamic_expr :: Ecto.Query.dynamic_expr()
  @type binding_alias :: atom()

  @type operator :: atom()

  @doc """
  ...
  """
  @spec create_dynamic(binding_alias() | nil, any(), operator(), any()) :: dynamic_expr()
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

  def create_dynamic(binding_alias, key, :!=, {:lower, value}) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment(
          "EXISTS (SELECT 1 FROM unnest(?) AS value WHERE LOWER(value) != LOWER(?))",
          field(q, ^key),
          ^value
        )
      )
    else
      Query.dynamic(
        [q],
        fragment(
          "EXISTS (SELECT 1 FROM unnest(?) AS value WHERE LOWER(value) != LOWER(?))",
          field(q, ^key),
          ^value
        )
      )
    end
  end

  def create_dynamic(binding_alias, key, :!=, {:upper, value}) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment(
          "EXISTS (SELECT 1 FROM unnest(?) AS value WHERE UPPER(value) != UPPER(?))",
          field(q, ^key),
          ^value
        )
      )
    else
      Query.dynamic(
        [q],
        fragment(
          "EXISTS (SELECT 1 FROM unnest(?) AS value WHERE UPPER(value) != UPPER(?))",
          field(q, ^key),
          ^value
        )
      )
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

  def create_dynamic(binding_alias, key, :==, {:lower, value}) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment(
          "EXISTS (SELECT 1 FROM unnest(?) AS value WHERE LOWER(value) = LOWER(?))",
          field(q, ^key),
          ^value
        )
      )
    else
      Query.dynamic(
        [q],
        fragment(
          "EXISTS (SELECT 1 FROM unnest(?) AS value WHERE LOWER(value) = LOWER(?))",
          field(q, ^key),
          ^value
        )
      )
    end
  end

  def create_dynamic(binding_alias, key, :==, {:upper, value}) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment(
          "EXISTS (SELECT 1 FROM unnest(?) AS value WHERE UPPER(value) = UPPER(?))",
          field(q, ^key),
          ^value
        )
      )
    else
      Query.dynamic(
        [q],
        fragment(
          "EXISTS (SELECT 1 FROM unnest(?) AS value WHERE UPPER(value) = UPPER(?))",
          field(q, ^key),
          ^value
        )
      )
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
