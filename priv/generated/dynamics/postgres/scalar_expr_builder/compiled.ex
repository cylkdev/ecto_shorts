defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, key, {:not, {:==, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:==, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:==, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (fragment("lower(?)", field(q, ^key)) == ^value))
    else
      dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:==, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
    else
      dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:==, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    else
      dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:==, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
    else
      dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:==, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) == ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) == ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:==, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:eq, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:eq, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:eq, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (fragment("lower(?)", field(q, ^key)) == ^value))
    else
      dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:eq, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
    else
      dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:eq, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    else
      dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:eq, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
    else
      dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:eq, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) == ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) == ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:eq, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:!=, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not not is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], not not is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:!=, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], not is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:!=, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (fragment("lower(?)", field(q, ^key)) != ^value))
    else
      dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:!=, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
    else
      dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:!=, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    else
      dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:!=, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
    else
      dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:!=, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) != ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) != ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:!=, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) != ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:ne, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not not is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], not not is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ne, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], not is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:ne, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (fragment("lower(?)", field(q, ^key)) != ^value))
    else
      dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ne, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
    else
      dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:ne, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    else
      dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ne, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
    else
      dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:ne, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) != ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) != ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ne, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) != ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:>, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) > ^nil))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) > ^nil))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) > ^nil)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) > ^nil)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:>, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) > ^{:lower, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) > ^{:lower, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) > ^{:lower, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) > ^{:lower, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:>, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) > ^{:upper, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) > ^{:upper, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) > ^{:upper, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) > ^{:upper, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:>, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) > ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) > ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) > ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:>=, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) >= ^nil))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^nil))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>=, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) >= ^nil)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) >= ^nil)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:>=, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) >= ^{:lower, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^{:lower, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>=, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) >= ^{:lower, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) >= ^{:lower, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:>=, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) >= ^{:upper, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^{:upper, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>=, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) >= ^{:upper, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) >= ^{:upper, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:>=, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) >= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>=, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) >= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:<, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) < ^nil))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) < ^nil))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) < ^nil)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) < ^nil)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:<, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) < ^{:lower, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) < ^{:lower, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) < ^{:lower, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) < ^{:lower, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:<, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) < ^{:upper, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) < ^{:upper, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) < ^{:upper, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) < ^{:upper, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:<, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) < ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) < ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) < ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:<=, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) <= ^nil))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^nil))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<=, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) <= ^nil)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) <= ^nil)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:<=, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) <= ^{:lower, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^{:lower, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<=, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) <= ^{:lower, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) <= ^{:lower, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:<=, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) <= ^{:upper, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^{:upper, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<=, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) <= ^{:upper, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) <= ^{:upper, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:<=, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) <= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<=, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) <= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:gt, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) > ^nil))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) > ^nil))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gt, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) > ^nil)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) > ^nil)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:gt, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) > ^{:lower, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) > ^{:lower, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gt, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) > ^{:lower, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) > ^{:lower, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:gt, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) > ^{:upper, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) > ^{:upper, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gt, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) > ^{:upper, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) > ^{:upper, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:gt, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) > ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) > ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gt, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) > ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:gte, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) >= ^nil))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^nil))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gte, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) >= ^nil)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) >= ^nil)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:gte, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) >= ^{:lower, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^{:lower, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gte, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) >= ^{:lower, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) >= ^{:lower, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:gte, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) >= ^{:upper, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^{:upper, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gte, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) >= ^{:upper, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) >= ^{:upper, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:gte, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) >= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gte, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) >= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:lt, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) < ^nil))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) < ^nil))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lt, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) < ^nil)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) < ^nil)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:lt, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) < ^{:lower, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) < ^{:lower, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lt, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) < ^{:lower, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) < ^{:lower, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:lt, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) < ^{:upper, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) < ^{:upper, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lt, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) < ^{:upper, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) < ^{:upper, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:lt, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) < ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) < ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lt, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) < ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:lte, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) <= ^nil))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^nil))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lte, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) <= ^nil)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) <= ^nil)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:lte, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) <= ^{:lower, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^{:lower, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lte, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) <= ^{:lower, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) <= ^{:lower, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:lte, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) <= ^{:upper, value}))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^{:upper, value}))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lte, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) <= ^{:upper, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) <= ^{:upper, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:lte, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) <= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lte, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) <= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:in, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) not in ^nil)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) not in ^nil)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:in, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) in ^nil)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) in ^nil)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:in, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) not in ^{:lower, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) not in ^{:lower, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:in, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) in ^{:lower, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) in ^{:lower, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:in, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) not in ^{:upper, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) not in ^{:upper, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:in, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) in ^{:upper, value})
    else
      dynamic([{^binding_alias, q}], field(q, ^key) in ^{:upper, value})
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:in, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) not in ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:in, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) in ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:like, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not like(field(q, ^key), ^"%#{nil}%"))
    else
      dynamic([{^binding_alias, q}], not like(field(q, ^key), ^"%#{nil}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:like, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], like(field(q, ^key), ^"%#{nil}%"))
    else
      dynamic([{^binding_alias, q}], like(field(q, ^key), ^"%#{nil}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:like, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not like(field(q, ^key), ^"%#{{:lower, value}}%"))
    else
      dynamic([{^binding_alias, q}], not like(field(q, ^key), ^"%#{{:lower, value}}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:like, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], like(field(q, ^key), ^"%#{{:lower, value}}%"))
    else
      dynamic([{^binding_alias, q}], like(field(q, ^key), ^"%#{{:lower, value}}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:like, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not like(field(q, ^key), ^"%#{{:upper, value}}%"))
    else
      dynamic([{^binding_alias, q}], not like(field(q, ^key), ^"%#{{:upper, value}}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:like, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], like(field(q, ^key), ^"%#{{:upper, value}}%"))
    else
      dynamic([{^binding_alias, q}], like(field(q, ^key), ^"%#{{:upper, value}}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:like, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([{^binding_alias, q}], not like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:like, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([{^binding_alias, q}], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:ilike, nil}}) do
    if is_nil(binding_alias) do
      dynamic([q], not ilike(field(q, ^key), ^"%#{nil}%"))
    else
      dynamic([{^binding_alias, q}], not ilike(field(q, ^key), ^"%#{nil}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ilike, nil}) do
    if is_nil(binding_alias) do
      dynamic([q], ilike(field(q, ^key), ^"%#{nil}%"))
    else
      dynamic([{^binding_alias, q}], ilike(field(q, ^key), ^"%#{nil}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:ilike, {:lower, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
    else
      dynamic([{^binding_alias, q}], not ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ilike, {:lower, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
    else
      dynamic([{^binding_alias, q}], ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:ilike, {:upper, value}}}) do
    if is_nil(binding_alias) do
      dynamic([q], not ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
    else
      dynamic([{^binding_alias, q}], not ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ilike, {:upper, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
    else
      dynamic([{^binding_alias, q}], ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:ilike, value}}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([{^binding_alias, q}], not ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ilike, value}) when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([{^binding_alias, q}], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:==, nil}}) do
    dynamic([q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 1}, key, {:==, nil}) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 1}, key, {:not, {:==, {:lower, value}}}) do
    dynamic([q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 1}, key, {:==, {:lower, value}}) do
    dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:==, {:upper, value}}}) do
    dynamic([q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 1}, key, {:==, {:upper, value}}) do
    dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:==, value}}) when not is_list(value) do
    dynamic([q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 1}, key, {:==, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:eq, nil}}) do
    dynamic([q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 1}, key, {:eq, nil}) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 1}, key, {:not, {:eq, {:lower, value}}}) do
    dynamic([q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 1}, key, {:eq, {:lower, value}}) do
    dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:eq, {:upper, value}}}) do
    dynamic([q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 1}, key, {:eq, {:upper, value}}) do
    dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:eq, value}}) when not is_list(value) do
    dynamic([q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 1}, key, {:eq, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:!=, nil}}) do
    dynamic([q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 1}, key, {:!=, nil}) do
    dynamic([q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 1}, key, {:not, {:!=, {:lower, value}}}) do
    dynamic([q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 1}, key, {:!=, {:lower, value}}) do
    dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:!=, {:upper, value}}}) do
    dynamic([q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 1}, key, {:!=, {:upper, value}}) do
    dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:!=, value}}) when not is_list(value) do
    dynamic([q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 1}, key, {:!=, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:ne, nil}}) do
    dynamic([q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 1}, key, {:ne, nil}) do
    dynamic([q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 1}, key, {:not, {:ne, {:lower, value}}}) do
    dynamic([q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 1}, key, {:ne, {:lower, value}}) do
    dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:ne, {:upper, value}}}) do
    dynamic([q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 1}, key, {:ne, {:upper, value}}) do
    dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:ne, value}}) when not is_list(value) do
    dynamic([q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 1}, key, {:ne, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:>, nil}}) do
    dynamic([q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 1}, key, {:>, nil}) do
    dynamic([q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:>, {:lower, value}}}) do
    dynamic([q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 1}, key, {:>, {:lower, value}}) do
    dynamic([q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:>, {:upper, value}}}) do
    dynamic([q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 1}, key, {:>, {:upper, value}}) do
    dynamic([q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:>, value}}) when not is_list(value) do
    dynamic([q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 1}, key, {:>, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:>=, nil}}) do
    dynamic([q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 1}, key, {:>=, nil}) do
    dynamic([q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:>=, {:lower, value}}}) do
    dynamic([q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 1}, key, {:>=, {:lower, value}}) do
    dynamic([q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:>=, {:upper, value}}}) do
    dynamic([q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 1}, key, {:>=, {:upper, value}}) do
    dynamic([q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:>=, value}}) when not is_list(value) do
    dynamic([q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 1}, key, {:>=, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:<, nil}}) do
    dynamic([q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 1}, key, {:<, nil}) do
    dynamic([q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:<, {:lower, value}}}) do
    dynamic([q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 1}, key, {:<, {:lower, value}}) do
    dynamic([q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:<, {:upper, value}}}) do
    dynamic([q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 1}, key, {:<, {:upper, value}}) do
    dynamic([q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:<, value}}) when not is_list(value) do
    dynamic([q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 1}, key, {:<, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:<=, nil}}) do
    dynamic([q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 1}, key, {:<=, nil}) do
    dynamic([q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:<=, {:lower, value}}}) do
    dynamic([q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 1}, key, {:<=, {:lower, value}}) do
    dynamic([q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:<=, {:upper, value}}}) do
    dynamic([q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 1}, key, {:<=, {:upper, value}}) do
    dynamic([q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:<=, value}}) when not is_list(value) do
    dynamic([q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 1}, key, {:<=, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:gt, nil}}) do
    dynamic([q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 1}, key, {:gt, nil}) do
    dynamic([q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:gt, {:lower, value}}}) do
    dynamic([q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 1}, key, {:gt, {:lower, value}}) do
    dynamic([q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:gt, {:upper, value}}}) do
    dynamic([q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 1}, key, {:gt, {:upper, value}}) do
    dynamic([q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:gt, value}}) when not is_list(value) do
    dynamic([q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 1}, key, {:gt, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:gte, nil}}) do
    dynamic([q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 1}, key, {:gte, nil}) do
    dynamic([q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:gte, {:lower, value}}}) do
    dynamic([q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 1}, key, {:gte, {:lower, value}}) do
    dynamic([q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:gte, {:upper, value}}}) do
    dynamic([q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 1}, key, {:gte, {:upper, value}}) do
    dynamic([q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:gte, value}}) when not is_list(value) do
    dynamic([q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 1}, key, {:gte, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:lt, nil}}) do
    dynamic([q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 1}, key, {:lt, nil}) do
    dynamic([q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:lt, {:lower, value}}}) do
    dynamic([q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 1}, key, {:lt, {:lower, value}}) do
    dynamic([q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:lt, {:upper, value}}}) do
    dynamic([q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 1}, key, {:lt, {:upper, value}}) do
    dynamic([q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:lt, value}}) when not is_list(value) do
    dynamic([q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 1}, key, {:lt, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:lte, nil}}) do
    dynamic([q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 1}, key, {:lte, nil}) do
    dynamic([q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:lte, {:lower, value}}}) do
    dynamic([q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 1}, key, {:lte, {:lower, value}}) do
    dynamic([q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:lte, {:upper, value}}}) do
    dynamic([q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 1}, key, {:lte, {:upper, value}}) do
    dynamic([q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:lte, value}}) when not is_list(value) do
    dynamic([q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 1}, key, {:lte, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:in, nil}}) do
    dynamic([q], field(q, ^key) not in ^nil)
  end

  def dynamic_expr({:at, 1}, key, {:in, nil}) do
    dynamic([q], field(q, ^key) in ^nil)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:in, {:lower, value}}}) do
    dynamic([q], field(q, ^key) not in ^{:lower, value})
  end

  def dynamic_expr({:at, 1}, key, {:in, {:lower, value}}) do
    dynamic([q], field(q, ^key) in ^{:lower, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:in, {:upper, value}}}) do
    dynamic([q], field(q, ^key) not in ^{:upper, value})
  end

  def dynamic_expr({:at, 1}, key, {:in, {:upper, value}}) do
    dynamic([q], field(q, ^key) in ^{:upper, value})
  end

  def dynamic_expr({:at, 1}, key, {:not, {:in, value}}) when not is_list(value) do
    dynamic([q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 1}, key, {:in, value}) when not is_list(value) do
    dynamic([q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:like, nil}}) do
    dynamic([q], not like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 1}, key, {:like, nil}) do
    dynamic([q], like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 1}, key, {:not, {:like, {:lower, value}}}) do
    dynamic([q], not like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 1}, key, {:like, {:lower, value}}) do
    dynamic([q], like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 1}, key, {:not, {:like, {:upper, value}}}) do
    dynamic([q], not like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 1}, key, {:like, {:upper, value}}) do
    dynamic([q], like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 1}, key, {:not, {:like, value}}) when not is_list(value) do
    dynamic([q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 1}, key, {:like, value}) when not is_list(value) do
    dynamic([q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 1}, key, {:not, {:ilike, nil}}) do
    dynamic([q], not ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 1}, key, {:ilike, nil}) do
    dynamic([q], ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 1}, key, {:not, {:ilike, {:lower, value}}}) do
    dynamic([q], not ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 1}, key, {:ilike, {:lower, value}}) do
    dynamic([q], ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 1}, key, {:not, {:ilike, {:upper, value}}}) do
    dynamic([q], not ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 1}, key, {:ilike, {:upper, value}}) do
    dynamic([q], ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 1}, key, {:not, {:ilike, value}}) when not is_list(value) do
    dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 1}, key, {:ilike, value}) when not is_list(value) do
    dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:==, nil}}) do
    dynamic([_, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 2}, key, {:==, nil}) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:==, {:lower, value}}}) do
    dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 2}, key, {:==, {:lower, value}}) do
    dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:==, {:upper, value}}}) do
    dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 2}, key, {:==, {:upper, value}}) do
    dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:==, value}}) when not is_list(value) do
    dynamic([_, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 2}, key, {:==, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:eq, nil}}) do
    dynamic([_, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 2}, key, {:eq, nil}) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:eq, {:lower, value}}}) do
    dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 2}, key, {:eq, {:lower, value}}) do
    dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:eq, {:upper, value}}}) do
    dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 2}, key, {:eq, {:upper, value}}) do
    dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:eq, value}}) when not is_list(value) do
    dynamic([_, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 2}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:!=, nil}}) do
    dynamic([_, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 2}, key, {:!=, nil}) do
    dynamic([_, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:!=, {:lower, value}}}) do
    dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 2}, key, {:!=, {:lower, value}}) do
    dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:!=, {:upper, value}}}) do
    dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 2}, key, {:!=, {:upper, value}}) do
    dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:!=, value}}) when not is_list(value) do
    dynamic([_, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 2}, key, {:!=, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:ne, nil}}) do
    dynamic([_, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 2}, key, {:ne, nil}) do
    dynamic([_, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:ne, {:lower, value}}}) do
    dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 2}, key, {:ne, {:lower, value}}) do
    dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:ne, {:upper, value}}}) do
    dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 2}, key, {:ne, {:upper, value}}) do
    dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:ne, value}}) when not is_list(value) do
    dynamic([_, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 2}, key, {:ne, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:>, nil}}) do
    dynamic([_, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 2}, key, {:>, nil}) do
    dynamic([_, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:>, {:lower, value}}}) do
    dynamic([_, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 2}, key, {:>, {:lower, value}}) do
    dynamic([_, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:>, {:upper, value}}}) do
    dynamic([_, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 2}, key, {:>, {:upper, value}}) do
    dynamic([_, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:>, value}}) when not is_list(value) do
    dynamic([_, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 2}, key, {:>, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:>=, nil}}) do
    dynamic([_, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 2}, key, {:>=, nil}) do
    dynamic([_, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:>=, {:lower, value}}}) do
    dynamic([_, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 2}, key, {:>=, {:lower, value}}) do
    dynamic([_, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:>=, {:upper, value}}}) do
    dynamic([_, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 2}, key, {:>=, {:upper, value}}) do
    dynamic([_, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:>=, value}}) when not is_list(value) do
    dynamic([_, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 2}, key, {:>=, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:<, nil}}) do
    dynamic([_, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 2}, key, {:<, nil}) do
    dynamic([_, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:<, {:lower, value}}}) do
    dynamic([_, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 2}, key, {:<, {:lower, value}}) do
    dynamic([_, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:<, {:upper, value}}}) do
    dynamic([_, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 2}, key, {:<, {:upper, value}}) do
    dynamic([_, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:<, value}}) when not is_list(value) do
    dynamic([_, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 2}, key, {:<, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:<=, nil}}) do
    dynamic([_, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 2}, key, {:<=, nil}) do
    dynamic([_, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:<=, {:lower, value}}}) do
    dynamic([_, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 2}, key, {:<=, {:lower, value}}) do
    dynamic([_, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:<=, {:upper, value}}}) do
    dynamic([_, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 2}, key, {:<=, {:upper, value}}) do
    dynamic([_, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:<=, value}}) when not is_list(value) do
    dynamic([_, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 2}, key, {:<=, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:gt, nil}}) do
    dynamic([_, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 2}, key, {:gt, nil}) do
    dynamic([_, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:gt, {:lower, value}}}) do
    dynamic([_, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 2}, key, {:gt, {:lower, value}}) do
    dynamic([_, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:gt, {:upper, value}}}) do
    dynamic([_, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 2}, key, {:gt, {:upper, value}}) do
    dynamic([_, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:gt, value}}) when not is_list(value) do
    dynamic([_, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 2}, key, {:gt, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:gte, nil}}) do
    dynamic([_, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 2}, key, {:gte, nil}) do
    dynamic([_, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:gte, {:lower, value}}}) do
    dynamic([_, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 2}, key, {:gte, {:lower, value}}) do
    dynamic([_, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:gte, {:upper, value}}}) do
    dynamic([_, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 2}, key, {:gte, {:upper, value}}) do
    dynamic([_, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:gte, value}}) when not is_list(value) do
    dynamic([_, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 2}, key, {:gte, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:lt, nil}}) do
    dynamic([_, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 2}, key, {:lt, nil}) do
    dynamic([_, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:lt, {:lower, value}}}) do
    dynamic([_, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 2}, key, {:lt, {:lower, value}}) do
    dynamic([_, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:lt, {:upper, value}}}) do
    dynamic([_, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 2}, key, {:lt, {:upper, value}}) do
    dynamic([_, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:lt, value}}) when not is_list(value) do
    dynamic([_, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 2}, key, {:lt, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:lte, nil}}) do
    dynamic([_, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 2}, key, {:lte, nil}) do
    dynamic([_, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:lte, {:lower, value}}}) do
    dynamic([_, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 2}, key, {:lte, {:lower, value}}) do
    dynamic([_, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:lte, {:upper, value}}}) do
    dynamic([_, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 2}, key, {:lte, {:upper, value}}) do
    dynamic([_, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:lte, value}}) when not is_list(value) do
    dynamic([_, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 2}, key, {:lte, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:in, nil}}) do
    dynamic([_, q], field(q, ^key) not in ^nil)
  end

  def dynamic_expr({:at, 2}, key, {:in, nil}) do
    dynamic([_, q], field(q, ^key) in ^nil)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:in, {:lower, value}}}) do
    dynamic([_, q], field(q, ^key) not in ^{:lower, value})
  end

  def dynamic_expr({:at, 2}, key, {:in, {:lower, value}}) do
    dynamic([_, q], field(q, ^key) in ^{:lower, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:in, {:upper, value}}}) do
    dynamic([_, q], field(q, ^key) not in ^{:upper, value})
  end

  def dynamic_expr({:at, 2}, key, {:in, {:upper, value}}) do
    dynamic([_, q], field(q, ^key) in ^{:upper, value})
  end

  def dynamic_expr({:at, 2}, key, {:not, {:in, value}}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 2}, key, {:in, value}) when not is_list(value) do
    dynamic([_, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:like, nil}}) do
    dynamic([_, q], not like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 2}, key, {:like, nil}) do
    dynamic([_, q], like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:like, {:lower, value}}}) do
    dynamic([_, q], not like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 2}, key, {:like, {:lower, value}}) do
    dynamic([_, q], like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:like, {:upper, value}}}) do
    dynamic([_, q], not like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 2}, key, {:like, {:upper, value}}) do
    dynamic([_, q], like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:like, value}}) when not is_list(value) do
    dynamic([_, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 2}, key, {:like, value}) when not is_list(value) do
    dynamic([_, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:ilike, nil}}) do
    dynamic([_, q], not ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 2}, key, {:ilike, nil}) do
    dynamic([_, q], ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:ilike, {:lower, value}}}) do
    dynamic([_, q], not ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 2}, key, {:ilike, {:lower, value}}) do
    dynamic([_, q], ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:ilike, {:upper, value}}}) do
    dynamic([_, q], not ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 2}, key, {:ilike, {:upper, value}}) do
    dynamic([_, q], ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:ilike, value}}) when not is_list(value) do
    dynamic([_, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 2}, key, {:ilike, value}) when not is_list(value) do
    dynamic([_, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:==, nil}}) do
    dynamic([_, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 3}, key, {:==, nil}) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:==, {:lower, value}}}) do
    dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 3}, key, {:==, {:lower, value}}) do
    dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:==, {:upper, value}}}) do
    dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 3}, key, {:==, {:upper, value}}) do
    dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:==, value}}) when not is_list(value) do
    dynamic([_, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 3}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:eq, nil}}) do
    dynamic([_, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 3}, key, {:eq, nil}) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:eq, {:lower, value}}}) do
    dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 3}, key, {:eq, {:lower, value}}) do
    dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:eq, {:upper, value}}}) do
    dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 3}, key, {:eq, {:upper, value}}) do
    dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:eq, value}}) when not is_list(value) do
    dynamic([_, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 3}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:!=, nil}}) do
    dynamic([_, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 3}, key, {:!=, nil}) do
    dynamic([_, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:!=, {:lower, value}}}) do
    dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 3}, key, {:!=, {:lower, value}}) do
    dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:!=, {:upper, value}}}) do
    dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 3}, key, {:!=, {:upper, value}}) do
    dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:!=, value}}) when not is_list(value) do
    dynamic([_, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 3}, key, {:!=, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:ne, nil}}) do
    dynamic([_, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 3}, key, {:ne, nil}) do
    dynamic([_, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:ne, {:lower, value}}}) do
    dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 3}, key, {:ne, {:lower, value}}) do
    dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:ne, {:upper, value}}}) do
    dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 3}, key, {:ne, {:upper, value}}) do
    dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:ne, value}}) when not is_list(value) do
    dynamic([_, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 3}, key, {:ne, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:>, nil}}) do
    dynamic([_, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 3}, key, {:>, nil}) do
    dynamic([_, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:>, {:lower, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 3}, key, {:>, {:lower, value}}) do
    dynamic([_, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:>, {:upper, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 3}, key, {:>, {:upper, value}}) do
    dynamic([_, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:>, value}}) when not is_list(value) do
    dynamic([_, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 3}, key, {:>, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:>=, nil}}) do
    dynamic([_, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 3}, key, {:>=, nil}) do
    dynamic([_, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:>=, {:lower, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 3}, key, {:>=, {:lower, value}}) do
    dynamic([_, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:>=, {:upper, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 3}, key, {:>=, {:upper, value}}) do
    dynamic([_, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:>=, value}}) when not is_list(value) do
    dynamic([_, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 3}, key, {:>=, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:<, nil}}) do
    dynamic([_, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 3}, key, {:<, nil}) do
    dynamic([_, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:<, {:lower, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 3}, key, {:<, {:lower, value}}) do
    dynamic([_, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:<, {:upper, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 3}, key, {:<, {:upper, value}}) do
    dynamic([_, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:<, value}}) when not is_list(value) do
    dynamic([_, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 3}, key, {:<, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:<=, nil}}) do
    dynamic([_, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 3}, key, {:<=, nil}) do
    dynamic([_, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:<=, {:lower, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 3}, key, {:<=, {:lower, value}}) do
    dynamic([_, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:<=, {:upper, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 3}, key, {:<=, {:upper, value}}) do
    dynamic([_, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:<=, value}}) when not is_list(value) do
    dynamic([_, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 3}, key, {:<=, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:gt, nil}}) do
    dynamic([_, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 3}, key, {:gt, nil}) do
    dynamic([_, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:gt, {:lower, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 3}, key, {:gt, {:lower, value}}) do
    dynamic([_, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:gt, {:upper, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 3}, key, {:gt, {:upper, value}}) do
    dynamic([_, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:gt, value}}) when not is_list(value) do
    dynamic([_, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 3}, key, {:gt, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:gte, nil}}) do
    dynamic([_, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 3}, key, {:gte, nil}) do
    dynamic([_, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:gte, {:lower, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 3}, key, {:gte, {:lower, value}}) do
    dynamic([_, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:gte, {:upper, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 3}, key, {:gte, {:upper, value}}) do
    dynamic([_, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:gte, value}}) when not is_list(value) do
    dynamic([_, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 3}, key, {:gte, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:lt, nil}}) do
    dynamic([_, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 3}, key, {:lt, nil}) do
    dynamic([_, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:lt, {:lower, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 3}, key, {:lt, {:lower, value}}) do
    dynamic([_, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:lt, {:upper, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 3}, key, {:lt, {:upper, value}}) do
    dynamic([_, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:lt, value}}) when not is_list(value) do
    dynamic([_, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 3}, key, {:lt, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:lte, nil}}) do
    dynamic([_, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 3}, key, {:lte, nil}) do
    dynamic([_, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:lte, {:lower, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 3}, key, {:lte, {:lower, value}}) do
    dynamic([_, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:lte, {:upper, value}}}) do
    dynamic([_, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 3}, key, {:lte, {:upper, value}}) do
    dynamic([_, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:lte, value}}) when not is_list(value) do
    dynamic([_, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 3}, key, {:lte, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:in, nil}}) do
    dynamic([_, _, q], field(q, ^key) not in ^nil)
  end

  def dynamic_expr({:at, 3}, key, {:in, nil}) do
    dynamic([_, _, q], field(q, ^key) in ^nil)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:in, {:lower, value}}}) do
    dynamic([_, _, q], field(q, ^key) not in ^{:lower, value})
  end

  def dynamic_expr({:at, 3}, key, {:in, {:lower, value}}) do
    dynamic([_, _, q], field(q, ^key) in ^{:lower, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:in, {:upper, value}}}) do
    dynamic([_, _, q], field(q, ^key) not in ^{:upper, value})
  end

  def dynamic_expr({:at, 3}, key, {:in, {:upper, value}}) do
    dynamic([_, _, q], field(q, ^key) in ^{:upper, value})
  end

  def dynamic_expr({:at, 3}, key, {:not, {:in, value}}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 3}, key, {:in, value}) when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:like, nil}}) do
    dynamic([_, _, q], not like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 3}, key, {:like, nil}) do
    dynamic([_, _, q], like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:like, {:lower, value}}}) do
    dynamic([_, _, q], not like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 3}, key, {:like, {:lower, value}}) do
    dynamic([_, _, q], like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:like, {:upper, value}}}) do
    dynamic([_, _, q], not like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 3}, key, {:like, {:upper, value}}) do
    dynamic([_, _, q], like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:like, value}}) when not is_list(value) do
    dynamic([_, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 3}, key, {:like, value}) when not is_list(value) do
    dynamic([_, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:ilike, nil}}) do
    dynamic([_, _, q], not ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 3}, key, {:ilike, nil}) do
    dynamic([_, _, q], ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:ilike, {:lower, value}}}) do
    dynamic([_, _, q], not ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 3}, key, {:ilike, {:lower, value}}) do
    dynamic([_, _, q], ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:ilike, {:upper, value}}}) do
    dynamic([_, _, q], not ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 3}, key, {:ilike, {:upper, value}}) do
    dynamic([_, _, q], ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:ilike, value}}) when not is_list(value) do
    dynamic([_, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 3}, key, {:ilike, value}) when not is_list(value) do
    dynamic([_, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:==, nil}}) do
    dynamic([_, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 4}, key, {:==, nil}) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:==, {:lower, value}}}) do
    dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 4}, key, {:==, {:lower, value}}) do
    dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:==, {:upper, value}}}) do
    dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 4}, key, {:==, {:upper, value}}) do
    dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:==, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 4}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:eq, nil}}) do
    dynamic([_, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 4}, key, {:eq, nil}) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:eq, {:lower, value}}}) do
    dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 4}, key, {:eq, {:lower, value}}) do
    dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:eq, {:upper, value}}}) do
    dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 4}, key, {:eq, {:upper, value}}) do
    dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:eq, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 4}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:!=, nil}}) do
    dynamic([_, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 4}, key, {:!=, nil}) do
    dynamic([_, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:!=, {:lower, value}}}) do
    dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 4}, key, {:!=, {:lower, value}}) do
    dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:!=, {:upper, value}}}) do
    dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 4}, key, {:!=, {:upper, value}}) do
    dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:!=, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 4}, key, {:!=, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:ne, nil}}) do
    dynamic([_, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 4}, key, {:ne, nil}) do
    dynamic([_, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:ne, {:lower, value}}}) do
    dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 4}, key, {:ne, {:lower, value}}) do
    dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:ne, {:upper, value}}}) do
    dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 4}, key, {:ne, {:upper, value}}) do
    dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:ne, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 4}, key, {:ne, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:>, nil}}) do
    dynamic([_, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 4}, key, {:>, nil}) do
    dynamic([_, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:>, {:lower, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 4}, key, {:>, {:lower, value}}) do
    dynamic([_, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:>, {:upper, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 4}, key, {:>, {:upper, value}}) do
    dynamic([_, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:>, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 4}, key, {:>, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:>=, nil}}) do
    dynamic([_, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 4}, key, {:>=, nil}) do
    dynamic([_, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:>=, {:lower, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 4}, key, {:>=, {:lower, value}}) do
    dynamic([_, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:>=, {:upper, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 4}, key, {:>=, {:upper, value}}) do
    dynamic([_, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:>=, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 4}, key, {:>=, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:<, nil}}) do
    dynamic([_, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 4}, key, {:<, nil}) do
    dynamic([_, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:<, {:lower, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 4}, key, {:<, {:lower, value}}) do
    dynamic([_, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:<, {:upper, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 4}, key, {:<, {:upper, value}}) do
    dynamic([_, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:<, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 4}, key, {:<, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:<=, nil}}) do
    dynamic([_, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 4}, key, {:<=, nil}) do
    dynamic([_, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:<=, {:lower, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 4}, key, {:<=, {:lower, value}}) do
    dynamic([_, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:<=, {:upper, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 4}, key, {:<=, {:upper, value}}) do
    dynamic([_, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:<=, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 4}, key, {:<=, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:gt, nil}}) do
    dynamic([_, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 4}, key, {:gt, nil}) do
    dynamic([_, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:gt, {:lower, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 4}, key, {:gt, {:lower, value}}) do
    dynamic([_, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:gt, {:upper, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 4}, key, {:gt, {:upper, value}}) do
    dynamic([_, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:gt, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 4}, key, {:gt, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:gte, nil}}) do
    dynamic([_, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 4}, key, {:gte, nil}) do
    dynamic([_, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:gte, {:lower, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 4}, key, {:gte, {:lower, value}}) do
    dynamic([_, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:gte, {:upper, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 4}, key, {:gte, {:upper, value}}) do
    dynamic([_, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:gte, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 4}, key, {:gte, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:lt, nil}}) do
    dynamic([_, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 4}, key, {:lt, nil}) do
    dynamic([_, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:lt, {:lower, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 4}, key, {:lt, {:lower, value}}) do
    dynamic([_, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:lt, {:upper, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 4}, key, {:lt, {:upper, value}}) do
    dynamic([_, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:lt, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 4}, key, {:lt, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:lte, nil}}) do
    dynamic([_, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 4}, key, {:lte, nil}) do
    dynamic([_, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:lte, {:lower, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 4}, key, {:lte, {:lower, value}}) do
    dynamic([_, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:lte, {:upper, value}}}) do
    dynamic([_, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 4}, key, {:lte, {:upper, value}}) do
    dynamic([_, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:lte, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 4}, key, {:lte, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:in, nil}}) do
    dynamic([_, _, _, q], field(q, ^key) not in ^nil)
  end

  def dynamic_expr({:at, 4}, key, {:in, nil}) do
    dynamic([_, _, _, q], field(q, ^key) in ^nil)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:in, {:lower, value}}}) do
    dynamic([_, _, _, q], field(q, ^key) not in ^{:lower, value})
  end

  def dynamic_expr({:at, 4}, key, {:in, {:lower, value}}) do
    dynamic([_, _, _, q], field(q, ^key) in ^{:lower, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:in, {:upper, value}}}) do
    dynamic([_, _, _, q], field(q, ^key) not in ^{:upper, value})
  end

  def dynamic_expr({:at, 4}, key, {:in, {:upper, value}}) do
    dynamic([_, _, _, q], field(q, ^key) in ^{:upper, value})
  end

  def dynamic_expr({:at, 4}, key, {:not, {:in, value}}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 4}, key, {:in, value}) when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:like, nil}}) do
    dynamic([_, _, _, q], not like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 4}, key, {:like, nil}) do
    dynamic([_, _, _, q], like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:like, {:lower, value}}}) do
    dynamic([_, _, _, q], not like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 4}, key, {:like, {:lower, value}}) do
    dynamic([_, _, _, q], like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:like, {:upper, value}}}) do
    dynamic([_, _, _, q], not like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 4}, key, {:like, {:upper, value}}) do
    dynamic([_, _, _, q], like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:like, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 4}, key, {:like, value}) when not is_list(value) do
    dynamic([_, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:ilike, nil}}) do
    dynamic([_, _, _, q], not ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 4}, key, {:ilike, nil}) do
    dynamic([_, _, _, q], ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:ilike, {:lower, value}}}) do
    dynamic([_, _, _, q], not ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 4}, key, {:ilike, {:lower, value}}) do
    dynamic([_, _, _, q], ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:ilike, {:upper, value}}}) do
    dynamic([_, _, _, q], not ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 4}, key, {:ilike, {:upper, value}}) do
    dynamic([_, _, _, q], ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:ilike, value}}) when not is_list(value) do
    dynamic([_, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 4}, key, {:ilike, value}) when not is_list(value) do
    dynamic([_, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:==, nil}}) do
    dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 5}, key, {:==, nil}) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:==, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 5}, key, {:==, {:lower, value}}) do
    dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:==, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 5}, key, {:==, {:upper, value}}) do
    dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:==, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 5}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:eq, nil}}) do
    dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 5}, key, {:eq, nil}) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:eq, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 5}, key, {:eq, {:lower, value}}) do
    dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:eq, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 5}, key, {:eq, {:upper, value}}) do
    dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:eq, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 5}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:!=, nil}}) do
    dynamic([_, _, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 5}, key, {:!=, nil}) do
    dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:!=, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 5}, key, {:!=, {:lower, value}}) do
    dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:!=, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 5}, key, {:!=, {:upper, value}}) do
    dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:!=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 5}, key, {:!=, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:ne, nil}}) do
    dynamic([_, _, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 5}, key, {:ne, nil}) do
    dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:ne, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 5}, key, {:ne, {:lower, value}}) do
    dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:ne, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 5}, key, {:ne, {:upper, value}}) do
    dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:ne, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 5}, key, {:ne, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:>, nil}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 5}, key, {:>, nil}) do
    dynamic([_, _, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:>, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 5}, key, {:>, {:lower, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:>, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 5}, key, {:>, {:upper, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:>, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 5}, key, {:>, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:>=, nil}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 5}, key, {:>=, nil}) do
    dynamic([_, _, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:>=, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 5}, key, {:>=, {:lower, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:>=, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 5}, key, {:>=, {:upper, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:>=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 5}, key, {:>=, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:<, nil}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 5}, key, {:<, nil}) do
    dynamic([_, _, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:<, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 5}, key, {:<, {:lower, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:<, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 5}, key, {:<, {:upper, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:<, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 5}, key, {:<, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:<=, nil}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 5}, key, {:<=, nil}) do
    dynamic([_, _, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:<=, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 5}, key, {:<=, {:lower, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:<=, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 5}, key, {:<=, {:upper, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:<=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 5}, key, {:<=, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:gt, nil}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 5}, key, {:gt, nil}) do
    dynamic([_, _, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:gt, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 5}, key, {:gt, {:lower, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:gt, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 5}, key, {:gt, {:upper, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:gt, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 5}, key, {:gt, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:gte, nil}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 5}, key, {:gte, nil}) do
    dynamic([_, _, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:gte, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 5}, key, {:gte, {:lower, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:gte, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 5}, key, {:gte, {:upper, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:gte, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 5}, key, {:gte, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:lt, nil}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 5}, key, {:lt, nil}) do
    dynamic([_, _, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:lt, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 5}, key, {:lt, {:lower, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:lt, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 5}, key, {:lt, {:upper, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:lt, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 5}, key, {:lt, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:lte, nil}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 5}, key, {:lte, nil}) do
    dynamic([_, _, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:lte, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 5}, key, {:lte, {:lower, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:lte, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 5}, key, {:lte, {:upper, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:lte, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 5}, key, {:lte, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:in, nil}}) do
    dynamic([_, _, _, _, q], field(q, ^key) not in ^nil)
  end

  def dynamic_expr({:at, 5}, key, {:in, nil}) do
    dynamic([_, _, _, _, q], field(q, ^key) in ^nil)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:in, {:lower, value}}}) do
    dynamic([_, _, _, _, q], field(q, ^key) not in ^{:lower, value})
  end

  def dynamic_expr({:at, 5}, key, {:in, {:lower, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) in ^{:lower, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:in, {:upper, value}}}) do
    dynamic([_, _, _, _, q], field(q, ^key) not in ^{:upper, value})
  end

  def dynamic_expr({:at, 5}, key, {:in, {:upper, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) in ^{:upper, value})
  end

  def dynamic_expr({:at, 5}, key, {:not, {:in, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 5}, key, {:in, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:like, nil}}) do
    dynamic([_, _, _, _, q], not like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 5}, key, {:like, nil}) do
    dynamic([_, _, _, _, q], like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:like, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 5}, key, {:like, {:lower, value}}) do
    dynamic([_, _, _, _, q], like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:like, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 5}, key, {:like, {:upper, value}}) do
    dynamic([_, _, _, _, q], like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:like, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 5}, key, {:like, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:ilike, nil}}) do
    dynamic([_, _, _, _, q], not ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 5}, key, {:ilike, nil}) do
    dynamic([_, _, _, _, q], ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:ilike, {:lower, value}}}) do
    dynamic([_, _, _, _, q], not ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 5}, key, {:ilike, {:lower, value}}) do
    dynamic([_, _, _, _, q], ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:ilike, {:upper, value}}}) do
    dynamic([_, _, _, _, q], not ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 5}, key, {:ilike, {:upper, value}}) do
    dynamic([_, _, _, _, q], ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:ilike, value}}) when not is_list(value) do
    dynamic([_, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 5}, key, {:ilike, value}) when not is_list(value) do
    dynamic([_, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:==, nil}}) do
    dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 6}, key, {:==, nil}) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:==, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 6}, key, {:==, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:==, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 6}, key, {:==, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:==, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 6}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:eq, nil}}) do
    dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 6}, key, {:eq, nil}) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:eq, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 6}, key, {:eq, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:eq, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 6}, key, {:eq, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:eq, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 6}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:!=, nil}}) do
    dynamic([_, _, _, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 6}, key, {:!=, nil}) do
    dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:!=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 6}, key, {:!=, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:!=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 6}, key, {:!=, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:!=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 6}, key, {:!=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:ne, nil}}) do
    dynamic([_, _, _, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 6}, key, {:ne, nil}) do
    dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:ne, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 6}, key, {:ne, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:ne, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 6}, key, {:ne, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:ne, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 6}, key, {:ne, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:>, nil}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 6}, key, {:>, nil}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:>, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 6}, key, {:>, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:>, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 6}, key, {:>, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:>, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 6}, key, {:>, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:>=, nil}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 6}, key, {:>=, nil}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:>=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 6}, key, {:>=, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:>=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 6}, key, {:>=, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:>=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 6}, key, {:>=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:<, nil}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 6}, key, {:<, nil}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:<, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 6}, key, {:<, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:<, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 6}, key, {:<, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:<, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 6}, key, {:<, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:<=, nil}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 6}, key, {:<=, nil}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:<=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 6}, key, {:<=, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:<=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 6}, key, {:<=, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:<=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 6}, key, {:<=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:gt, nil}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 6}, key, {:gt, nil}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:gt, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 6}, key, {:gt, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:gt, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 6}, key, {:gt, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:gt, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 6}, key, {:gt, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:gte, nil}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 6}, key, {:gte, nil}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:gte, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 6}, key, {:gte, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:gte, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 6}, key, {:gte, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:gte, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 6}, key, {:gte, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:lt, nil}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 6}, key, {:lt, nil}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:lt, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 6}, key, {:lt, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:lt, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 6}, key, {:lt, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:lt, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 6}, key, {:lt, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:lte, nil}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 6}, key, {:lte, nil}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:lte, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 6}, key, {:lte, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:lte, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 6}, key, {:lte, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:lte, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 6}, key, {:lte, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:in, nil}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) not in ^nil)
  end

  def dynamic_expr({:at, 6}, key, {:in, nil}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) in ^nil)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:in, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) not in ^{:lower, value})
  end

  def dynamic_expr({:at, 6}, key, {:in, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) in ^{:lower, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:in, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) not in ^{:upper, value})
  end

  def dynamic_expr({:at, 6}, key, {:in, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) in ^{:upper, value})
  end

  def dynamic_expr({:at, 6}, key, {:not, {:in, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 6}, key, {:in, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:like, nil}}) do
    dynamic([_, _, _, _, _, q], not like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 6}, key, {:like, nil}) do
    dynamic([_, _, _, _, _, q], like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:like, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 6}, key, {:like, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:like, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 6}, key, {:like, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:like, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 6}, key, {:like, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:ilike, nil}}) do
    dynamic([_, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 6}, key, {:ilike, nil}) do
    dynamic([_, _, _, _, _, q], ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:ilike, {:lower, value}}}) do
    dynamic([_, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 6}, key, {:ilike, {:lower, value}}) do
    dynamic([_, _, _, _, _, q], ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:ilike, {:upper, value}}}) do
    dynamic([_, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 6}, key, {:ilike, {:upper, value}}) do
    dynamic([_, _, _, _, _, q], ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:ilike, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 6}, key, {:ilike, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:==, nil}}) do
    dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 7}, key, {:==, nil}) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:==, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 7}, key, {:==, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:==, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 7}, key, {:==, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:==, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 7}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:eq, nil}}) do
    dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 7}, key, {:eq, nil}) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:eq, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 7}, key, {:eq, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:eq, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 7}, key, {:eq, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:eq, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 7}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:!=, nil}}) do
    dynamic([_, _, _, _, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 7}, key, {:!=, nil}) do
    dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:!=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 7}, key, {:!=, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:!=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 7}, key, {:!=, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:!=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 7}, key, {:!=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:ne, nil}}) do
    dynamic([_, _, _, _, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 7}, key, {:ne, nil}) do
    dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:ne, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 7}, key, {:ne, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:ne, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 7}, key, {:ne, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:ne, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 7}, key, {:ne, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:>, nil}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 7}, key, {:>, nil}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:>, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 7}, key, {:>, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:>, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 7}, key, {:>, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:>, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 7}, key, {:>, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:>=, nil}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 7}, key, {:>=, nil}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:>=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 7}, key, {:>=, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:>=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 7}, key, {:>=, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:>=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 7}, key, {:>=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:<, nil}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 7}, key, {:<, nil}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:<, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 7}, key, {:<, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:<, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 7}, key, {:<, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:<, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 7}, key, {:<, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:<=, nil}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 7}, key, {:<=, nil}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:<=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 7}, key, {:<=, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:<=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 7}, key, {:<=, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:<=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 7}, key, {:<=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:gt, nil}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 7}, key, {:gt, nil}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:gt, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 7}, key, {:gt, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:gt, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 7}, key, {:gt, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:gt, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 7}, key, {:gt, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:gte, nil}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 7}, key, {:gte, nil}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:gte, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 7}, key, {:gte, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:gte, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 7}, key, {:gte, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:gte, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 7}, key, {:gte, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:lt, nil}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 7}, key, {:lt, nil}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:lt, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 7}, key, {:lt, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:lt, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 7}, key, {:lt, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:lt, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 7}, key, {:lt, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:lte, nil}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 7}, key, {:lte, nil}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:lte, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 7}, key, {:lte, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:lte, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 7}, key, {:lte, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:lte, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 7}, key, {:lte, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:in, nil}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^nil)
  end

  def dynamic_expr({:at, 7}, key, {:in, nil}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^nil)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:in, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^{:lower, value})
  end

  def dynamic_expr({:at, 7}, key, {:in, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^{:lower, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:in, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^{:upper, value})
  end

  def dynamic_expr({:at, 7}, key, {:in, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^{:upper, value})
  end

  def dynamic_expr({:at, 7}, key, {:not, {:in, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 7}, key, {:in, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:like, nil}}) do
    dynamic([_, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 7}, key, {:like, nil}) do
    dynamic([_, _, _, _, _, _, q], like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:like, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 7}, key, {:like, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:like, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 7}, key, {:like, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:like, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 7}, key, {:like, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:ilike, nil}}) do
    dynamic([_, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 7}, key, {:ilike, nil}) do
    dynamic([_, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:ilike, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 7}, key, {:ilike, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:ilike, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 7}, key, {:ilike, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:ilike, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 7}, key, {:ilike, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:==, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 8}, key, {:==, nil}) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:==, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 8}, key, {:==, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:==, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 8}, key, {:==, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:==, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 8}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:eq, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 8}, key, {:eq, nil}) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:eq, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 8}, key, {:eq, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:eq, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 8}, key, {:eq, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:eq, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 8}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:!=, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 8}, key, {:!=, nil}) do
    dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:!=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 8}, key, {:!=, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:!=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 8}, key, {:!=, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:!=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 8}, key, {:!=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:ne, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 8}, key, {:ne, nil}) do
    dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:ne, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 8}, key, {:ne, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:ne, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 8}, key, {:ne, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:ne, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 8}, key, {:ne, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:>, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 8}, key, {:>, nil}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:>, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 8}, key, {:>, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:>, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 8}, key, {:>, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:>, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 8}, key, {:>, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:>=, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 8}, key, {:>=, nil}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:>=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 8}, key, {:>=, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:>=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 8}, key, {:>=, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:>=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 8}, key, {:>=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:<, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 8}, key, {:<, nil}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:<, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 8}, key, {:<, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:<, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 8}, key, {:<, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:<, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 8}, key, {:<, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:<=, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 8}, key, {:<=, nil}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:<=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 8}, key, {:<=, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:<=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 8}, key, {:<=, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:<=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 8}, key, {:<=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:gt, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 8}, key, {:gt, nil}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:gt, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 8}, key, {:gt, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:gt, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 8}, key, {:gt, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:gt, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 8}, key, {:gt, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:gte, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 8}, key, {:gte, nil}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:gte, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 8}, key, {:gte, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:gte, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 8}, key, {:gte, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:gte, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 8}, key, {:gte, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:lt, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 8}, key, {:lt, nil}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:lt, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 8}, key, {:lt, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:lt, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 8}, key, {:lt, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:lt, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 8}, key, {:lt, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:lte, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 8}, key, {:lte, nil}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:lte, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 8}, key, {:lte, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:lte, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 8}, key, {:lte, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:lte, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 8}, key, {:lte, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:in, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^nil)
  end

  def dynamic_expr({:at, 8}, key, {:in, nil}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^nil)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:in, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^{:lower, value})
  end

  def dynamic_expr({:at, 8}, key, {:in, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^{:lower, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:in, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^{:upper, value})
  end

  def dynamic_expr({:at, 8}, key, {:in, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^{:upper, value})
  end

  def dynamic_expr({:at, 8}, key, {:not, {:in, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 8}, key, {:in, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:like, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 8}, key, {:like, nil}) do
    dynamic([_, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:like, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 8}, key, {:like, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:like, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 8}, key, {:like, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:like, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 8}, key, {:like, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:ilike, nil}}) do
    dynamic([_, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 8}, key, {:ilike, nil}) do
    dynamic([_, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:ilike, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 8}, key, {:ilike, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:ilike, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 8}, key, {:ilike, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:ilike, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 8}, key, {:ilike, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:==, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 9}, key, {:==, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:==, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 9}, key, {:==, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:==, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 9}, key, {:==, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:==, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 9}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:eq, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 9}, key, {:eq, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:eq, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 9}, key, {:eq, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:eq, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 9}, key, {:eq, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:eq, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 9}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:!=, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 9}, key, {:!=, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:!=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 9}, key, {:!=, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:!=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 9}, key, {:!=, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:!=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 9}, key, {:!=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:ne, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 9}, key, {:ne, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:ne, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 9}, key, {:ne, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:ne, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 9}, key, {:ne, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:ne, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 9}, key, {:ne, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:>, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 9}, key, {:>, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:>, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 9}, key, {:>, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:>, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 9}, key, {:>, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:>, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 9}, key, {:>, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:>=, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 9}, key, {:>=, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:>=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 9}, key, {:>=, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:>=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 9}, key, {:>=, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:>=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 9}, key, {:>=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:<, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 9}, key, {:<, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:<, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 9}, key, {:<, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:<, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 9}, key, {:<, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:<, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 9}, key, {:<, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:<=, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 9}, key, {:<=, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:<=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 9}, key, {:<=, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:<=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 9}, key, {:<=, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:<=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 9}, key, {:<=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:gt, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 9}, key, {:gt, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:gt, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 9}, key, {:gt, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:gt, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 9}, key, {:gt, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:gt, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 9}, key, {:gt, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:gte, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 9}, key, {:gte, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:gte, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 9}, key, {:gte, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:gte, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 9}, key, {:gte, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:gte, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 9}, key, {:gte, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:lt, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 9}, key, {:lt, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:lt, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 9}, key, {:lt, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:lt, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 9}, key, {:lt, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:lt, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 9}, key, {:lt, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:lte, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 9}, key, {:lte, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:lte, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 9}, key, {:lte, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:lte, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 9}, key, {:lte, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:lte, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 9}, key, {:lte, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:in, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^nil)
  end

  def dynamic_expr({:at, 9}, key, {:in, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^nil)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:in, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^{:lower, value})
  end

  def dynamic_expr({:at, 9}, key, {:in, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^{:lower, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:in, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^{:upper, value})
  end

  def dynamic_expr({:at, 9}, key, {:in, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^{:upper, value})
  end

  def dynamic_expr({:at, 9}, key, {:not, {:in, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 9}, key, {:in, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:like, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 9}, key, {:like, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:like, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 9}, key, {:like, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:like, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 9}, key, {:like, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:like, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 9}, key, {:like, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:ilike, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 9}, key, {:ilike, nil}) do
    dynamic([_, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:ilike, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 9}, key, {:ilike, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:ilike, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 9}, key, {:ilike, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:ilike, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 9}, key, {:ilike, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:==, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 10}, key, {:==, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:==, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 10}, key, {:==, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:==, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 10}, key, {:==, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:==, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 10}, key, {:==, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:eq, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 10}, key, {:eq, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:eq, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 10}, key, {:eq, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:eq, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
  end

  def dynamic_expr({:at, 10}, key, {:eq, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:eq, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 10}, key, {:eq, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:!=, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 10}, key, {:!=, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:!=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 10}, key, {:!=, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:!=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 10}, key, {:!=, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:!=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 10}, key, {:!=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:ne, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 10}, key, {:ne, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:ne, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 10}, key, {:ne, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:ne, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
  end

  def dynamic_expr({:at, 10}, key, {:ne, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:ne, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 10}, key, {:ne, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:>, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 10}, key, {:>, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:>, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 10}, key, {:>, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:>, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 10}, key, {:>, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:>, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 10}, key, {:>, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:>=, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 10}, key, {:>=, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:>=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 10}, key, {:>=, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:>=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 10}, key, {:>=, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:>=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 10}, key, {:>=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:<, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 10}, key, {:<, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:<, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 10}, key, {:<, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:<, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 10}, key, {:<, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:<, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 10}, key, {:<, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:<=, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 10}, key, {:<=, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:<=, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 10}, key, {:<=, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:<=, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 10}, key, {:<=, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:<=, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 10}, key, {:<=, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:gt, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^nil))
  end

  def dynamic_expr({:at, 10}, key, {:gt, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^nil)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:gt, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^{:lower, value}))
  end

  def dynamic_expr({:at, 10}, key, {:gt, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^{:lower, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:gt, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^{:upper, value}))
  end

  def dynamic_expr({:at, 10}, key, {:gt, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^{:upper, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:gt, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 10}, key, {:gt, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:gte, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^nil))
  end

  def dynamic_expr({:at, 10}, key, {:gte, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^nil)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:gte, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^{:lower, value}))
  end

  def dynamic_expr({:at, 10}, key, {:gte, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^{:lower, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:gte, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^{:upper, value}))
  end

  def dynamic_expr({:at, 10}, key, {:gte, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^{:upper, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:gte, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 10}, key, {:gte, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:lt, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^nil))
  end

  def dynamic_expr({:at, 10}, key, {:lt, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^nil)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:lt, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^{:lower, value}))
  end

  def dynamic_expr({:at, 10}, key, {:lt, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^{:lower, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:lt, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^{:upper, value}))
  end

  def dynamic_expr({:at, 10}, key, {:lt, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^{:upper, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:lt, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 10}, key, {:lt, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:lte, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^nil))
  end

  def dynamic_expr({:at, 10}, key, {:lte, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^nil)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:lte, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^{:lower, value}))
  end

  def dynamic_expr({:at, 10}, key, {:lte, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^{:lower, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:lte, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^{:upper, value}))
  end

  def dynamic_expr({:at, 10}, key, {:lte, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^{:upper, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:lte, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 10}, key, {:lte, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:in, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^nil)
  end

  def dynamic_expr({:at, 10}, key, {:in, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^nil)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:in, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^{:lower, value})
  end

  def dynamic_expr({:at, 10}, key, {:in, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^{:lower, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:in, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^{:upper, value})
  end

  def dynamic_expr({:at, 10}, key, {:in, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^{:upper, value})
  end

  def dynamic_expr({:at, 10}, key, {:not, {:in, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 10}, key, {:in, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:like, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 10}, key, {:like, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:like, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 10}, key, {:like, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:like, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 10}, key, {:like, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:like, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 10}, key, {:like, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:ilike, nil}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 10}, key, {:ilike, nil}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{nil}%"))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:ilike, {:lower, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 10}, key, {:ilike, {:lower, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{{:lower, value}}%"))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:ilike, {:upper, value}}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 10}, key, {:ilike, {:upper, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{{:upper, value}}%"))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:ilike, value}}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 10}, key, {:ilike, value}) when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end