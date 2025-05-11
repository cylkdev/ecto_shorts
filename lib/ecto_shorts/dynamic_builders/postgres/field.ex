defmodule EctoShorts.DynamicBuilders.Postgres.Field do
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
  @spec create_dynamic(binding_alias(), any(), operator(), any()) :: dynamic_expr()
  def create_dynamic(binding_alias, key, :=~, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], fragment("? ~* ?", field(q, ^key), ^value))
    else
      Query.dynamic([q], fragment("? ~* ?", field(q, ^key), ^value))
    end
  end

  def create_dynamic(binding_alias, key, :ilike, value) do
    pattern = "%#{value}%"

    if binding_alias do
      Query.dynamic([{^binding_alias, q}], ilike(field(q, ^key), ^pattern))
    else
      Query.dynamic([q], ilike(field(q, ^key), ^pattern))
    end
  end

  def create_dynamic(binding_alias, key, :like, value) do
    pattern = "%#{value}%"

    if binding_alias do
      Query.dynamic([{^binding_alias, q}], like(field(q, ^key), ^pattern))
    else
      Query.dynamic([q], like(field(q, ^key), ^pattern))
    end
  end

  def create_dynamic(binding_alias, key, :<, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) < ^value)
    else
      Query.dynamic([q], field(q, ^key) < ^value)
    end
  end

  def create_dynamic(binding_alias, key, :>, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) > ^value)
    else
      Query.dynamic([q], field(q, ^key) > ^value)
    end
  end

  def create_dynamic(binding_alias, key, :<=, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) <= ^value)
    else
      Query.dynamic([q], field(q, ^key) <= ^value)
    end
  end

  def create_dynamic(binding_alias, key, :>=, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) >= ^value)
    else
      Query.dynamic([q], field(q, ^key) >= ^value)
    end
  end

  def create_dynamic(binding_alias, key, :!=, {:lower, value}) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("LOWER(?)", field(q, ^key)) != ^value
      )
    else
      Query.dynamic([q], fragment("LOWER(?)", field(q, ^key)) != ^value)
    end
  end

  def create_dynamic(binding_alias, key, :!=, {:upper, value}) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("UPPER(?)", field(q, ^key)) != ^value
      )
    else
      Query.dynamic([q], fragment("UPPER(?)", field(q, ^key)) != ^value)
    end
  end

  def create_dynamic(binding_alias, key, :==, {:lower, value}) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("LOWER(?)", field(q, ^key)) == ^value
      )
    else
      Query.dynamic([q], fragment("LOWER(?)", field(q, ^key)) == ^value)
    end
  end

  def create_dynamic(binding_alias, key, :==, {:upper, value}) do
    if binding_alias do
      Query.dynamic(
        [{^binding_alias, q}],
        fragment("UPPER(?)", field(q, ^key)) == ^value
      )
    else
      Query.dynamic([q], fragment("UPPER(?)", field(q, ^key)) == ^value)
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
      Query.dynamic([{^binding_alias, q}], field(q, ^key) not in ^values)
    else
      Query.dynamic([q], field(q, ^key) not in ^values)
    end
  end

  def create_dynamic(binding_alias, key, :!=, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
    else
      Query.dynamic([q], field(q, ^key) != ^value)
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
      Query.dynamic([{^binding_alias, q}], field(q, ^key) in ^values)
    else
      Query.dynamic([q], field(q, ^key) in ^values)
    end
  end

  def create_dynamic(binding_alias, key, :==, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    else
      Query.dynamic([q], field(q, ^key) == ^value)
    end
  end
end
