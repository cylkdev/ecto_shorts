defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:==, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:==, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:eq, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:eq, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:!=, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:!=, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:ne, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:ne, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:>, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:>, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:>=, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:>=, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:<, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:<, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:<=, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:<=, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:gt, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:gt, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:gte, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:gte, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:lt, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:lt, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:lte, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:lte, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:in, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:in, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:like, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:like, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:ilike, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:ilike, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:not, nil}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], is_nil(field(q, ^key)))
    else
      dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:not, value}
      )
      when not is_list(value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:==, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:==, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:eq, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:eq, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:!=, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:!=, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:ne, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:ne, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:>, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:>, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:>=, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:>=, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:<, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:<, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:<=, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:<=, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:gt, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:gt, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:gte, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:gte, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:lt, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:lt, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:lte, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:lte, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:in, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:in, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:like, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:like, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:ilike, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:ilike, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:not, nil}
      ) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:not, value}
      )
      when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:==, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:==, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:eq, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:eq, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:!=, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:!=, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:ne, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:ne, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:>, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:>, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:>=, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:>=, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:<, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:<, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:<=, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:<=, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:gt, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:gt, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:gte, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:gte, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:lt, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:lt, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:lte, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:lte, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:in, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:in, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:like, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:like, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:ilike, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:ilike, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:not, nil}
      ) do
    dynamic([_, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:not, value}
      )
      when not is_list(value) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:==, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:==, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:eq, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:eq, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:!=, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:!=, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:ne, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:ne, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:>, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:>, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:>=, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:>=, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:<, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:<, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:<=, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:<=, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:gt, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:gt, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:gte, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:gte, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:lt, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:lt, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:lte, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:lte, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:in, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:in, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:like, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:like, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:ilike, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:ilike, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:not, nil}
      ) do
    dynamic([_, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:not, value}
      )
      when not is_list(value) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:==, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:==, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:eq, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:eq, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:!=, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:!=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:ne, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:ne, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:>, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:>, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:>=, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:>=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:<, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:<, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:<=, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:<=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:gt, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:gt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:gte, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:gte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:lt, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:lt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:lte, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:lte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:in, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:in, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:like, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:like, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:ilike, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:ilike, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:not, nil}
      ) do
    dynamic([_, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:not, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:==, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:==, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:eq, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:eq, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:!=, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:!=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:ne, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:ne, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:>, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:>, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:>=, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:>=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:<, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:<, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:<=, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:<=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:gt, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:gt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:gte, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:gte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:lt, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:lt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:lte, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:lte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:in, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:in, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:like, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:like, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:ilike, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:ilike, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:not, nil}
      ) do
    dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:not, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:==, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:==, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:eq, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:eq, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:!=, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:!=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:ne, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:ne, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:>, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:>, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:>=, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:>=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:<, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:<, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:<=, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:<=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:gt, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:gt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:gte, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:gte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:lt, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:lt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:lte, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:lte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:in, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:in, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:like, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:like, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:ilike, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:ilike, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:not, nil}
      ) do
    dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:not, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:==, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:==, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:eq, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:eq, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:!=, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:!=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:ne, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:ne, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:>, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:>, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:>=, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:>=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:<, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:<, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:<=, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:<=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:gt, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:gt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:gte, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:gte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:lt, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:lt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:lte, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:lte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:in, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:in, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:like, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:like, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:ilike, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:ilike, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:not, nil}
      ) do
    dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:not, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:==, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:==, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:eq, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:eq, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:!=, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:!=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:ne, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:ne, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:>, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:>, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:>=, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:>=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:<, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:<, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:<=, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:<=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:gt, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:gt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:gte, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:gte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:lt, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:lt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:lte, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:lte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:in, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:in, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:like, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:like, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:ilike, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:ilike, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:not, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:not, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:==, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:==, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:eq, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:eq, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:!=, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:!=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:ne, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:ne, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:>, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:>, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:>=, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:>=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:<, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:<, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:<=, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:<=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:gt, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:gt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:gte, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:gte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:lt, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:lt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:lte, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:lte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:in, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:in, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:like, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:like, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:ilike, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:ilike, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:not, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:not, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:==, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:==, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:eq, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:eq, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:!=, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:!=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:ne, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:ne, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:>, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:>, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:>=, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:>=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:<, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:<, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:<=, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:<=, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:gt, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:gt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:gte, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:gte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:lt, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:lt, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:lte, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:lte, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:in, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:in, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:like, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:like, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:ilike, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:ilike, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:not, nil}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:not, value}
      )
      when not is_list(value) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end