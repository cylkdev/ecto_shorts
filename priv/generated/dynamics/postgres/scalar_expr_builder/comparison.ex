defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled.Comparison do
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
      {:not, {:>, value}} -> dynamic([q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([q], field(q, ^key) != ^value)
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
      {:not, {:>, value}} -> dynamic([_, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, q], field(q, ^key) != ^value)
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
      {:not, {:>, value}} -> dynamic([_, _, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, _, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, _, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, _, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, _, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, _, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, _, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, _, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, _, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, _, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, _, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, _, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, _, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, _, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, _, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, _, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, _, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, _, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, _, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, _, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, _, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, _, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, _, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, _, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, _, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, _, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, _, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, _, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, _, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, _, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, _, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, _, q], field(q, ^key) != ^value)
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
      {:not, {:>, value}} -> dynamic([_, _, _, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, _, _, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, _, _, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, _, _, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, _, _, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, _, _, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, _, _, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, _, _, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, _, _, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, _, _, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, _, _, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, _, _, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, _, _, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, _, _, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, _, _, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, _, _, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, _, _, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, _, _, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, _, _, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, _, _, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, _, _, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, _, _, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, _, _, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, _, _, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, _, _, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, _, _, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, _, _, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, _, _, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, _, _, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, _, _, q], field(q, ^key) != ^value)
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
      {:not, {:>, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, _, _, _, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, _, _, _, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, _, _, _, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, _, _, _, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, _, _, _, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, _, _, _, q], field(q, ^key) != ^value)
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
      {:not, {:>, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
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
      {:not, {:>, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
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
      {:not, {:>, value}} -> dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)
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
      {:not, {:>, value}} -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
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
      {:not, {:>, value}} -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr(_, _, _, _) do
    nil
  end
end