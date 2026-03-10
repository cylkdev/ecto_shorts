defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled.StringUpperLower do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type negated :: :not | nil
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr(
        {:as, binding_alias},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
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
    end
  end

  def dynamic_expr(
        {:at, 1},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:==, {:lower, value}}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:==, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:not, {:==, {:upper, value}}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:==, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:not, {:eq, {:lower, value}}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:eq, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:not, {:eq, {:upper, value}}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:eq, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:not, {:!=, {:lower, value}}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:!=, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:not, {:!=, {:upper, value}}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:!=, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:not, {:ne, {:lower, value}}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:ne, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:not, {:ne, {:upper, value}}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:ne, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:not, {:>, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      {:>, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)
      {:not, {:>, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))
      {:>, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)
      {:not, {:>=, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      {:>=, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)
      {:not, {:>=, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
      {:>=, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)
      {:not, {:<, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      {:<, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)
      {:not, {:<, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))
      {:<, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)
      {:not, {:<=, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      {:<=, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)
      {:not, {:<=, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
      {:<=, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)
      {:not, {:gt, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      {:gt, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)
      {:not, {:gt, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))
      {:gt, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)
      {:not, {:gte, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      {:gte, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)
      {:not, {:gte, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
      {:gte, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)
      {:not, {:lt, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      {:lt, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)
      {:not, {:lt, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))
      {:lt, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)
      {:not, {:lte, {:lower, value}}} -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      {:lte, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)
      {:not, {:lte, {:upper, value}}} -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
      {:lte, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 2},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:==, {:lower, value}}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:==, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:not, {:==, {:upper, value}}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:==, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:not, {:eq, {:lower, value}}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:eq, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:not, {:eq, {:upper, value}}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:eq, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:not, {:!=, {:lower, value}}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:!=, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:not, {:!=, {:upper, value}}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:!=, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:not, {:ne, {:lower, value}}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:ne, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:not, {:ne, {:upper, value}}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:ne, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:not, {:>, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      {:>, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) > ^value)
      {:not, {:>, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
      {:>, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) > ^value)
      {:not, {:>=, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      {:>=, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      {:not, {:>=, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
      {:>=, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) >= ^value)
      {:not, {:<, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      {:<, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) < ^value)
      {:not, {:<, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
      {:<, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) < ^value)
      {:not, {:<=, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      {:<=, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      {:not, {:<=, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
      {:<=, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) <= ^value)
      {:not, {:gt, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      {:gt, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) > ^value)
      {:not, {:gt, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
      {:gt, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) > ^value)
      {:not, {:gte, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      {:gte, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      {:not, {:gte, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
      {:gte, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) >= ^value)
      {:not, {:lt, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      {:lt, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) < ^value)
      {:not, {:lt, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
      {:lt, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) < ^value)
      {:not, {:lte, {:lower, value}}} -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      {:lte, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      {:not, {:lte, {:upper, value}}} -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
      {:lte, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 3},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 4},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 5},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 6},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 7},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 8},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 9},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 10},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:eq, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:eq, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:eq, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:ne, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:ne, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:ne, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr(_, _, _, _) do
    nil
  end
end