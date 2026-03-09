defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type binding_selector :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, key, {:==, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:==, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:eq, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:eq, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:==, nil}) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 1}, key, {:==, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 1}, key, {:eq, nil}) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 1}, key, {:eq, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 2}, key, {:==, nil}) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 2}, key, {:==, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 2}, key, {:eq, nil}) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 2}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 3}, key, {:==, nil}) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 3}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 3}, key, {:eq, nil}) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 3}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 4}, key, {:==, nil}) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 4}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 4}, key, {:eq, nil}) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 4}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 5}, key, {:==, nil}) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 5}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 5}, key, {:eq, nil}) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 5}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 6}, key, {:==, nil}) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 6}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 6}, key, {:eq, nil}) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 6}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 7}, key, {:==, nil}) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 7}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 7}, key, {:eq, nil}) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 7}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 8}, key, {:==, nil}) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 8}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 8}, key, {:eq, nil}) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 8}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 9}, key, {:==, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 9}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 9}, key, {:eq, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 9}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 10}, key, {:==, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 10}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 10}, key, {:eq, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 10}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end