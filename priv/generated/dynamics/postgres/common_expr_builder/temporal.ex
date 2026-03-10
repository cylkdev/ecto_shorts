defmodule EctoShorts.Dynamics.Postgres.CommonExpr.Compiled.Temporal do
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
        {:not, {:start_date, value}}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :inserted_at) >= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :inserted_at) >= ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:start_date, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :inserted_at) >= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:not, {:end_date, value}}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :inserted_at) <= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :inserted_at) <= ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:end_date, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :inserted_at) <= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:not, {:since_date, value}}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :inserted_at) >= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :inserted_at) >= ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:since_date, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :inserted_at) >= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:not, {:until_date, value}}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :inserted_at) <= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :inserted_at) <= ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:until_date, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :inserted_at) <= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:not, {:start_date, value}}
      ) do
    dynamic([q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:start_date, value}
      ) do
    dynamic([q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:not, {:end_date, value}}
      ) do
    dynamic([q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:end_date, value}
      ) do
    dynamic([q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:not, {:since_date, value}}
      ) do
    dynamic([q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:since_date, value}
      ) do
    dynamic([q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:not, {:until_date, value}}
      ) do
    dynamic([q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:until_date, value}
      ) do
    dynamic([q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:not, {:start_date, value}}
      ) do
    dynamic([_, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:start_date, value}
      ) do
    dynamic([_, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:not, {:end_date, value}}
      ) do
    dynamic([_, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:end_date, value}
      ) do
    dynamic([_, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:not, {:since_date, value}}
      ) do
    dynamic([_, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:since_date, value}
      ) do
    dynamic([_, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:not, {:until_date, value}}
      ) do
    dynamic([_, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:until_date, value}
      ) do
    dynamic([_, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:not, {:start_date, value}}
      ) do
    dynamic([_, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:start_date, value}
      ) do
    dynamic([_, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:not, {:end_date, value}}
      ) do
    dynamic([_, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:end_date, value}
      ) do
    dynamic([_, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:not, {:since_date, value}}
      ) do
    dynamic([_, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:since_date, value}
      ) do
    dynamic([_, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:not, {:until_date, value}}
      ) do
    dynamic([_, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:until_date, value}
      ) do
    dynamic([_, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:not, {:start_date, value}}
      ) do
    dynamic([_, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:start_date, value}
      ) do
    dynamic([_, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:not, {:end_date, value}}
      ) do
    dynamic([_, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:end_date, value}
      ) do
    dynamic([_, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:not, {:since_date, value}}
      ) do
    dynamic([_, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:since_date, value}
      ) do
    dynamic([_, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:not, {:until_date, value}}
      ) do
    dynamic([_, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:until_date, value}
      ) do
    dynamic([_, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:not, {:start_date, value}}
      ) do
    dynamic([_, _, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:start_date, value}
      ) do
    dynamic([_, _, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:not, {:end_date, value}}
      ) do
    dynamic([_, _, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:end_date, value}
      ) do
    dynamic([_, _, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:not, {:since_date, value}}
      ) do
    dynamic([_, _, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:since_date, value}
      ) do
    dynamic([_, _, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:not, {:until_date, value}}
      ) do
    dynamic([_, _, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:until_date, value}
      ) do
    dynamic([_, _, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:not, {:start_date, value}}
      ) do
    dynamic([_, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:start_date, value}
      ) do
    dynamic([_, _, _, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:not, {:end_date, value}}
      ) do
    dynamic([_, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:end_date, value}
      ) do
    dynamic([_, _, _, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:not, {:since_date, value}}
      ) do
    dynamic([_, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:since_date, value}
      ) do
    dynamic([_, _, _, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:not, {:until_date, value}}
      ) do
    dynamic([_, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:until_date, value}
      ) do
    dynamic([_, _, _, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:not, {:start_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:start_date, value}
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:not, {:end_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:end_date, value}
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:not, {:since_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:since_date, value}
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:not, {:until_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:until_date, value}
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:not, {:start_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:start_date, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:not, {:end_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:end_date, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:not, {:since_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:since_date, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:not, {:until_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:until_date, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:not, {:start_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:start_date, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:not, {:end_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:end_date, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:not, {:since_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:since_date, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:not, {:until_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:until_date, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:not, {:start_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:start_date, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:not, {:end_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:end_date, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:not, {:since_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:since_date, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:not, {:until_date, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:until_date, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end