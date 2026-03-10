defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, key, {:==, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, ^key) == ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, ^key) == ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) == ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:eq, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, ^key) == ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, ^key) == ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) == ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:!=, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, ^key) != ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, ^key) != ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) != ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ne, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, ^key) != ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, ^key) != ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) != ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, ^key) > ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, ^key) > ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) > ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) > ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>=, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, ^key) >= ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) >= ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) >= ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, ^key) < ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, ^key) < ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) < ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) < ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<=, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, ^key) <= ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) <= ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) <= ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gt, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, ^key) > ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, ^key) > ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) > ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) > ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gte, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, ^key) >= ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) >= ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) >= ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lt, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, ^key) < ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, ^key) < ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) < ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) < ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lte, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, ^key) <= ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) <= ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) <= ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:in, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) not in ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) not in ^value)
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, ^key) in ^value)
      else
        dynamic([{^binding_alias, q}], field(q, ^key) in ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:like, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not like(field(q, ^key), ^"%#{value}%"))
      else
        dynamic([{^binding_alias, q}], not like(field(q, ^key), ^"%#{value}%"))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], like(field(q, ^key), ^"%#{value}%"))
      else
        dynamic([{^binding_alias, q}], like(field(q, ^key), ^"%#{value}%"))
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ilike, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))
      else
        dynamic([{^binding_alias, q}], not ilike(field(q, ^key), ^"%#{value}%"))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
      else
        dynamic([{^binding_alias, q}], ilike(field(q, ^key), ^"%#{value}%"))
      end
    end
  end

  def dynamic_expr({:at, 1}, key, {:==, value}) do
    if key === :not do
      dynamic([q], not (field(q, ^key) == ^value))
    else
      dynamic([q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:eq, value}) do
    if key === :not do
      dynamic([q], not (field(q, ^key) == ^value))
    else
      dynamic([q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:!=, value}) do
    if key === :not do
      dynamic([q], not (field(q, ^key) != ^value))
    else
      dynamic([q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:ne, value}) do
    if key === :not do
      dynamic([q], not (field(q, ^key) != ^value))
    else
      dynamic([q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:>, value}) do
    if key === :not do
      dynamic([q], not (field(q, ^key) > ^value))
    else
      dynamic([q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:>=, value}) do
    if key === :not do
      dynamic([q], not (field(q, ^key) >= ^value))
    else
      dynamic([q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:<, value}) do
    if key === :not do
      dynamic([q], not (field(q, ^key) < ^value))
    else
      dynamic([q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:<=, value}) do
    if key === :not do
      dynamic([q], not (field(q, ^key) <= ^value))
    else
      dynamic([q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:gt, value}) do
    if key === :not do
      dynamic([q], not (field(q, ^key) > ^value))
    else
      dynamic([q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:gte, value}) do
    if key === :not do
      dynamic([q], not (field(q, ^key) >= ^value))
    else
      dynamic([q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:lt, value}) do
    if key === :not do
      dynamic([q], not (field(q, ^key) < ^value))
    else
      dynamic([q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:lte, value}) do
    if key === :not do
      dynamic([q], not (field(q, ^key) <= ^value))
    else
      dynamic([q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:in, value}) do
    if key === :not do
      dynamic([q], field(q, ^key) not in ^value)
    else
      dynamic([q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:like, value}) do
    if key === :not do
      dynamic([q], not like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 1}, key, {:ilike, value}) do
    if key === :not do
      dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 2}, key, {:==, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:eq, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:!=, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:ne, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:>, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:>=, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:<, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:<=, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:gt, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:gte, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:lt, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:lte, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:in, value}) do
    if key === :not do
      dynamic([_, q], field(q, ^key) not in ^value)
    else
      dynamic([_, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:like, value}) do
    if key === :not do
      dynamic([_, q], not like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 2}, key, {:ilike, value}) do
    if key === :not do
      dynamic([_, q], not ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 3}, key, {:==, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:eq, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:!=, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:ne, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:>, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:>=, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:<, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:<=, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:gt, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:gte, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:lt, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:lte, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:in, value}) do
    if key === :not do
      dynamic([_, _, q], field(q, ^key) not in ^value)
    else
      dynamic([_, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:like, value}) do
    if key === :not do
      dynamic([_, _, q], not like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 3}, key, {:ilike, value}) do
    if key === :not do
      dynamic([_, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 4}, key, {:==, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:eq, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:!=, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:ne, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:>, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:>=, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:<, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:<=, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:gt, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:gte, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:lt, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:lte, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:in, value}) do
    if key === :not do
      dynamic([_, _, _, q], field(q, ^key) not in ^value)
    else
      dynamic([_, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:like, value}) do
    if key === :not do
      dynamic([_, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 4}, key, {:ilike, value}) do
    if key === :not do
      dynamic([_, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 5}, key, {:==, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:eq, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:!=, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:ne, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:>, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:>=, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:<, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:<=, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:gt, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:gte, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:lt, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:lte, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:in, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], field(q, ^key) not in ^value)
    else
      dynamic([_, _, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:like, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 5}, key, {:ilike, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 6}, key, {:==, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:eq, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:!=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:ne, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:>, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:>=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:<, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:<=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:gt, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:gte, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:lt, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:lte, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:in, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], field(q, ^key) not in ^value)
    else
      dynamic([_, _, _, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:like, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 6}, key, {:ilike, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 7}, key, {:==, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:eq, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:!=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:ne, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:>, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:>=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:<, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:<=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:gt, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:gte, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:lt, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:lte, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:in, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^value)
    else
      dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:like, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 7}, key, {:ilike, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 8}, key, {:==, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:eq, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:!=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:ne, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:>, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:>=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:<, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:<=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:gt, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:gte, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:lt, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:lte, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:in, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:like, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 8}, key, {:ilike, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 9}, key, {:==, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:eq, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:!=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:ne, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:>, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:>=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:<, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:<=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:gt, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:gte, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:lt, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:lte, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:in, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:like, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 9}, key, {:ilike, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 10}, key, {:==, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:eq, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:!=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:ne, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:>, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:>=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:<, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:<=, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:gt, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:gte, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:lt, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:lte, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:in, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:like, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 10}, key, {:ilike, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end