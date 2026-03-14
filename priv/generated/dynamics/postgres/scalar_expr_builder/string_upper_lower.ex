defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled.StringUpperLower do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type negated :: :not | nil
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
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
    end
  end

  def dynamic_expr({:at, 1}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
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
      {:not, {:==, {:lower, value}}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:==, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:not, {:==, {:upper, value}}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:==, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:not, {:!=, {:lower, value}}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:!=, {:lower, value}} -> dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:not, {:!=, {:upper, value}}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:!=, {:upper, value}} -> dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
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
      {:not, {:==, {:lower, value}}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:==, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:not, {:==, {:upper, value}}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
      {:==, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:not, {:!=, {:lower, value}}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
      {:!=, {:lower, value}} -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
      {:not, {:!=, {:upper, value}}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
      {:!=, {:upper, value}} -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
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

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
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

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
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

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
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

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
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

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
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

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
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

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
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

      {:not, {:==, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:==, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:not, {:==, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)

      {:==, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:not, {:!=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)

      {:!=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)

      {:not, {:!=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)

      {:!=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr(_, _, _, _) do
    nil
  end
end