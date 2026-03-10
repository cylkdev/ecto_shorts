defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled.Comparison do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, key, value) do
    case value do
      {:not, {:==, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], not is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
        end

      {:==, nil} ->
        if is_nil(binding_alias) do
          dynamic([q], is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
        end

      {:not, {:==, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) != ^value)
        end

      {:==, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) == ^value)
        end

      {:not, {:==, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) != ^value)
        end

      {:==, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) == ^value)
        end

      {:not, {:==, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) != ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
        end

      {:==, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) == ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
        end

      {:not, {:eq, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], not is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
        end

      {:eq, nil} ->
        if is_nil(binding_alias) do
          dynamic([q], is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
        end

      {:not, {:eq, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) != ^value)
        end

      {:eq, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) == ^value)
        end

      {:not, {:eq, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) != ^value)
        end

      {:eq, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) == ^value)
        end

      {:not, {:eq, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) != ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
        end

      {:eq, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) == ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
        end

      {:not, {:!=, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
        end

      {:!=, nil} ->
        if is_nil(binding_alias) do
          dynamic([q], not is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
        end

      {:not, {:!=, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) == ^value)
        end

      {:!=, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) != ^value)
        end

      {:not, {:!=, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) == ^value)
        end

      {:!=, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) != ^value)
        end

      {:not, {:!=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) == ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
        end

      {:!=, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) != ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
        end

      {:not, {:ne, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
        end

      {:ne, nil} ->
        if is_nil(binding_alias) do
          dynamic([q], not is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
        end

      {:not, {:ne, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) == ^value)
        end

      {:ne, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) != ^value)
        end

      {:not, {:ne, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) == ^value)
        end

      {:ne, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) != ^value)
        end

      {:not, {:ne, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) == ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
        end

      {:ne, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) != ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
        end

      {:not, {:>, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) > ^value))
        end

      {:>, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) > ^value)
        end

      {:not, {:>, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) > ^value))
        end

      {:>, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) > ^value)
        end

      {:not, {:>, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) > ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) > ^value))
        end

      {:>, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) > ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) > ^value)
        end

      {:not, {:>=, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) >= ^value))
        end

      {:>=, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) >= ^value)
        end

      {:not, {:>=, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) >= ^value))
        end

      {:>=, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) >= ^value)
        end

      {:not, {:>=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^value))
        end

      {:>=, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) >= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) >= ^value)
        end

      {:not, {:<, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) < ^value))
        end

      {:<, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) < ^value)
        end

      {:not, {:<, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) < ^value))
        end

      {:<, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) < ^value)
        end

      {:not, {:<, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) < ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) < ^value))
        end

      {:<, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) < ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) < ^value)
        end

      {:not, {:<=, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) <= ^value))
        end

      {:<=, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) <= ^value)
        end

      {:not, {:<=, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) <= ^value))
        end

      {:<=, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) <= ^value)
        end

      {:not, {:<=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^value))
        end

      {:<=, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) <= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) <= ^value)
        end

      {:not, {:gt, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) > ^value))
        end

      {:gt, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) > ^value)
        end

      {:not, {:gt, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) > ^value))
        end

      {:gt, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) > ^value)
        end

      {:not, {:gt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) > ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) > ^value))
        end

      {:gt, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) > ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) > ^value)
        end

      {:not, {:gte, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) >= ^value))
        end

      {:gte, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) >= ^value)
        end

      {:not, {:gte, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) >= ^value))
        end

      {:gte, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) >= ^value)
        end

      {:not, {:gte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^value))
        end

      {:gte, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) >= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) >= ^value)
        end

      {:not, {:lt, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) < ^value))
        end

      {:lt, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) < ^value)
        end

      {:not, {:lt, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) < ^value))
        end

      {:lt, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) < ^value)
        end

      {:not, {:lt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) < ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) < ^value))
        end

      {:lt, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) < ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) < ^value)
        end

      {:not, {:lte, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) <= ^value))
        end

      {:lte, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) <= ^value)
        end

      {:not, {:lte, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) <= ^value))
        end

      {:lte, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) <= ^value)
        end

      {:not, {:lte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^value))
        end

      {:lte, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) <= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) <= ^value)
        end
    end
  end

  def dynamic_expr({:at, 1}, key, value) do
    case value do
      {:not, {:==, nil}} -> dynamic([q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([q], is_nil(field(q, ^key)))
      {:not, {:==, {:lower, value}}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:==, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:not, {:==, {:upper, value}}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:==, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:not, {:==, value}} -> dynamic([q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([q], is_nil(field(q, ^key)))
      {:not, {:eq, {:lower, value}}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:eq, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:not, {:eq, {:upper, value}}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:eq, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:not, {:eq, value}} -> dynamic([q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([q], not is_nil(field(q, ^key)))
      {:not, {:!=, {:lower, value}}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:!=, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:not, {:!=, {:upper, value}}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:!=, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:not, {:!=, value}} -> dynamic([q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([q], not is_nil(field(q, ^key)))
      {:not, {:ne, {:lower, value}}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:ne, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:not, {:ne, {:upper, value}}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:ne, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:not, {:ne, value}} -> dynamic([q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([q], field(q, ^key) != ^value)
      {:not, {:>, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      {:>, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)
      {:not, {:>, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))
      {:>, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)
      {:not, {:>, value}} -> dynamic([q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([q], field(q, ^key) > ^value)
      {:not, {:>=, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      {:>=, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)
      {:not, {:>=, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
      {:>=, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)
      {:not, {:>=, value}} -> dynamic([q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([q], field(q, ^key) >= ^value)
      {:not, {:<, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      {:<, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)
      {:not, {:<, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))
      {:<, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)
      {:not, {:<, value}} -> dynamic([q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([q], field(q, ^key) < ^value)
      {:not, {:<=, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      {:<=, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)
      {:not, {:<=, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
      {:<=, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)
      {:not, {:<=, value}} -> dynamic([q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([q], field(q, ^key) <= ^value)
      {:not, {:gt, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      {:gt, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)
      {:not, {:gt, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))
      {:gt, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)
      {:not, {:gt, value}} -> dynamic([q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([q], field(q, ^key) > ^value)
      {:not, {:gte, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      {:gte, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)
      {:not, {:gte, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
      {:gte, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)
      {:not, {:gte, value}} -> dynamic([q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([q], field(q, ^key) >= ^value)
      {:not, {:lt, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      {:lt, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)
      {:not, {:lt, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))
      {:lt, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)
      {:not, {:lt, value}} -> dynamic([q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([q], field(q, ^key) < ^value)
      {:not, {:lte, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      {:lte, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)
      {:not, {:lte, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
      {:lte, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)
      {:not, {:lte, value}} -> dynamic([q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, value) do
    case value do
      {:not, {:==, nil}} -> dynamic([_, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, q], is_nil(field(q, ^key)))
      {:not, {:==, {:lower, value}}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:==, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:not, {:==, {:upper, value}}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:==, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:not, {:==, value}} -> dynamic([_, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, q], is_nil(field(q, ^key)))
      {:not, {:eq, {:lower, value}}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:eq, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:not, {:eq, {:upper, value}}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:eq, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:not, {:eq, value}} -> dynamic([_, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, q], not is_nil(field(q, ^key)))
      {:not, {:!=, {:lower, value}}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:!=, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:not, {:!=, {:upper, value}}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:!=, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:not, {:!=, value}} -> dynamic([_, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, q], not is_nil(field(q, ^key)))
      {:not, {:ne, {:lower, value}}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:ne, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:not, {:ne, {:upper, value}}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:ne, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:not, {:ne, value}} -> dynamic([_, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, q], field(q, ^key) != ^value)
      {:not, {:>, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      {:>, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) > ^value)
      {:not, {:>, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
      {:>, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) > ^value)
      {:not, {:>, value}} -> dynamic([_, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, q], field(q, ^key) > ^value)
      {:not, {:>=, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      {:>=, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      {:not, {:>=, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
      {:>=, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) >= ^value)
      {:not, {:>=, value}} -> dynamic([_, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, q], field(q, ^key) >= ^value)
      {:not, {:<, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      {:<, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) < ^value)
      {:not, {:<, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
      {:<, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) < ^value)
      {:not, {:<, value}} -> dynamic([_, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, q], field(q, ^key) < ^value)
      {:not, {:<=, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      {:<=, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      {:not, {:<=, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
      {:<=, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) <= ^value)
      {:not, {:<=, value}} -> dynamic([_, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, q], field(q, ^key) <= ^value)
      {:not, {:gt, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      {:gt, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) > ^value)
      {:not, {:gt, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
      {:gt, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) > ^value)
      {:not, {:gt, value}} -> dynamic([_, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, q], field(q, ^key) > ^value)
      {:not, {:gte, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      {:gte, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      {:not, {:gte, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
      {:gte, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) >= ^value)
      {:not, {:gte, value}} -> dynamic([_, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, q], field(q, ^key) >= ^value)
      {:not, {:lt, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      {:lt, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) < ^value)
      {:not, {:lt, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
      {:lt, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) < ^value)
      {:not, {:lt, value}} -> dynamic([_, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, q], field(q, ^key) < ^value)
      {:not, {:lte, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      {:lte, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      {:not, {:lte, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
      {:lte, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) <= ^value)
      {:not, {:lte, value}} -> dynamic([_, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, value) do
    case value do
      {:not, {:==, nil}} ->
        dynamic([_, _, q], not is_nil(field(q, ^key)))

      {:==, nil} ->
        dynamic([_, _, q], is_nil(field(q, ^key)))

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:==, value}} ->
        dynamic([_, _, q], field(q, ^key) != ^value)

      {:==, value} ->
        dynamic([_, _, q], field(q, ^key) == ^value)

      {:not, {:eq, nil}} ->
        dynamic([_, _, q], not is_nil(field(q, ^key)))

      {:eq, nil} ->
        dynamic([_, _, q], is_nil(field(q, ^key)))

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, value}} ->
        dynamic([_, _, q], field(q, ^key) != ^value)

      {:eq, value} ->
        dynamic([_, _, q], field(q, ^key) == ^value)

      {:not, {:!=, nil}} ->
        dynamic([_, _, q], is_nil(field(q, ^key)))

      {:!=, nil} ->
        dynamic([_, _, q], not is_nil(field(q, ^key)))

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:!=, value}} ->
        dynamic([_, _, q], field(q, ^key) == ^value)

      {:!=, value} ->
        dynamic([_, _, q], field(q, ^key) != ^value)

      {:not, {:ne, nil}} ->
        dynamic([_, _, q], is_nil(field(q, ^key)))

      {:ne, nil} ->
        dynamic([_, _, q], not is_nil(field(q, ^key)))

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, value}} ->
        dynamic([_, _, q], field(q, ^key) == ^value)

      {:ne, value} ->
        dynamic([_, _, q], field(q, ^key) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, value) do
    case value do
      {:not, {:==, nil}} ->
        dynamic([_, _, _, q], not is_nil(field(q, ^key)))

      {:==, nil} ->
        dynamic([_, _, _, q], is_nil(field(q, ^key)))

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:==, value}} ->
        dynamic([_, _, _, q], field(q, ^key) != ^value)

      {:==, value} ->
        dynamic([_, _, _, q], field(q, ^key) == ^value)

      {:not, {:eq, nil}} ->
        dynamic([_, _, _, q], not is_nil(field(q, ^key)))

      {:eq, nil} ->
        dynamic([_, _, _, q], is_nil(field(q, ^key)))

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, value}} ->
        dynamic([_, _, _, q], field(q, ^key) != ^value)

      {:eq, value} ->
        dynamic([_, _, _, q], field(q, ^key) == ^value)

      {:not, {:!=, nil}} ->
        dynamic([_, _, _, q], is_nil(field(q, ^key)))

      {:!=, nil} ->
        dynamic([_, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:!=, value}} ->
        dynamic([_, _, _, q], field(q, ^key) == ^value)

      {:!=, value} ->
        dynamic([_, _, _, q], field(q, ^key) != ^value)

      {:not, {:ne, nil}} ->
        dynamic([_, _, _, q], is_nil(field(q, ^key)))

      {:ne, nil} ->
        dynamic([_, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, value}} ->
        dynamic([_, _, _, q], field(q, ^key) == ^value)

      {:ne, value} ->
        dynamic([_, _, _, q], field(q, ^key) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, value) do
    case value do
      {:not, {:==, nil}} ->
        dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))

      {:==, nil} ->
        dynamic([_, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:==, value}} ->
        dynamic([_, _, _, _, q], field(q, ^key) != ^value)

      {:==, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:eq, nil}} ->
        dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))

      {:eq, nil} ->
        dynamic([_, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, value}} ->
        dynamic([_, _, _, _, q], field(q, ^key) != ^value)

      {:eq, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:!=, nil}} ->
        dynamic([_, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, nil} ->
        dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:!=, value}} ->
        dynamic([_, _, _, _, q], field(q, ^key) == ^value)

      {:!=, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:ne, nil}} ->
        dynamic([_, _, _, _, q], is_nil(field(q, ^key)))

      {:ne, nil} ->
        dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, value}} ->
        dynamic([_, _, _, _, q], field(q, ^key) == ^value)

      {:ne, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, value) do
    case value do
      {:not, {:==, nil}} ->
        dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:==, nil} ->
        dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:==, value}} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)

      {:==, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:eq, nil}} ->
        dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:eq, nil} ->
        dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, value}} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)

      {:eq, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:!=, nil}} ->
        dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, nil} ->
        dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:!=, value}} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)

      {:!=, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:ne, nil}} ->
        dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))

      {:ne, nil} ->
        dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, value}} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)

      {:ne, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, value) do
    case value do
      {:not, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:==, nil} ->
        dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:==, value}} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:==, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:eq, nil} ->
        dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:eq, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, nil} ->
        dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:!=, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:ne, nil} ->
        dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:ne, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, value) do
    case value do
      {:not, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:==, nil} ->
        dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:==, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:eq, nil} ->
        dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:eq, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, nil} ->
        dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:!=, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:ne, nil} ->
        dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:ne, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, value) do
    case value do
      {:not, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:==, nil} ->
        dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:==, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:eq, nil} ->
        dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:eq, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, nil} ->
        dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:!=, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:ne, nil} ->
        dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:ne, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, value) do
    case value do
      {:not, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:==, nil} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:==, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:eq, nil} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:eq, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, nil} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:!=, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:ne, nil} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:ne, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end