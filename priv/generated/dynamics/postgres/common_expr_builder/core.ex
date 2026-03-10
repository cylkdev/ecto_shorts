defmodule EctoShorts.Dynamics.Postgres.CommonExpr.Compiled.Core do
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
        {:not, {:ids, value}}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) not in ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) not in ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:ids, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) in ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) in ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:not, {:before, value}}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :id) < ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :id) < ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:before, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) < ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) < ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:not, {:after, value}}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :id) > ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :id) > ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:after, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) > ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) > ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:not, {:until, value}}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :id) <= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :id) <= ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:until, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) <= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:not, {:since, value}}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :id) >= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :id) >= ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:since, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) >= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:not, {:exists, value}}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not exists(value))
    else
      dynamic([{^binding_alias, q}], not exists(value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        key,
        {:exists, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], exists(value))
    else
      dynamic([{^binding_alias, q}], exists(value))
    end
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:not, {:ids, value}}
      ) do
    dynamic([q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:ids, value}
      ) do
    dynamic([q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:not, {:before, value}}
      ) do
    dynamic([q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:before, value}
      ) do
    dynamic([q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:not, {:after, value}}
      ) do
    dynamic([q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:after, value}
      ) do
    dynamic([q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:not, {:until, value}}
      ) do
    dynamic([q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:until, value}
      ) do
    dynamic([q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:not, {:since, value}}
      ) do
    dynamic([q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:since, value}
      ) do
    dynamic([q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:not, {:exists, value}}
      ) do
    dynamic([q], not exists(value))
  end

  def dynamic_expr(
        {:at, 1},
        key,
        {:exists, value}
      ) do
    dynamic([q], exists(value))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:not, {:ids, value}}
      ) do
    dynamic([_, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:ids, value}
      ) do
    dynamic([_, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:not, {:before, value}}
      ) do
    dynamic([_, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:before, value}
      ) do
    dynamic([_, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:not, {:after, value}}
      ) do
    dynamic([_, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:after, value}
      ) do
    dynamic([_, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:not, {:until, value}}
      ) do
    dynamic([_, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:until, value}
      ) do
    dynamic([_, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:not, {:since, value}}
      ) do
    dynamic([_, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:since, value}
      ) do
    dynamic([_, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:not, {:exists, value}}
      ) do
    dynamic([_, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 2},
        key,
        {:exists, value}
      ) do
    dynamic([_, q], exists(value))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:not, {:ids, value}}
      ) do
    dynamic([_, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:ids, value}
      ) do
    dynamic([_, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:not, {:before, value}}
      ) do
    dynamic([_, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:before, value}
      ) do
    dynamic([_, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:not, {:after, value}}
      ) do
    dynamic([_, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:after, value}
      ) do
    dynamic([_, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:not, {:until, value}}
      ) do
    dynamic([_, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:until, value}
      ) do
    dynamic([_, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:not, {:since, value}}
      ) do
    dynamic([_, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:since, value}
      ) do
    dynamic([_, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:not, {:exists, value}}
      ) do
    dynamic([_, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 3},
        key,
        {:exists, value}
      ) do
    dynamic([_, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:not, {:ids, value}}
      ) do
    dynamic([_, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:ids, value}
      ) do
    dynamic([_, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:not, {:before, value}}
      ) do
    dynamic([_, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:before, value}
      ) do
    dynamic([_, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:not, {:after, value}}
      ) do
    dynamic([_, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:after, value}
      ) do
    dynamic([_, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:not, {:until, value}}
      ) do
    dynamic([_, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:until, value}
      ) do
    dynamic([_, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:not, {:since, value}}
      ) do
    dynamic([_, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:since, value}
      ) do
    dynamic([_, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:not, {:exists, value}}
      ) do
    dynamic([_, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 4},
        key,
        {:exists, value}
      ) do
    dynamic([_, _, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:not, {:ids, value}}
      ) do
    dynamic([_, _, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:ids, value}
      ) do
    dynamic([_, _, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:not, {:before, value}}
      ) do
    dynamic([_, _, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:before, value}
      ) do
    dynamic([_, _, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:not, {:after, value}}
      ) do
    dynamic([_, _, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:after, value}
      ) do
    dynamic([_, _, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:not, {:until, value}}
      ) do
    dynamic([_, _, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:until, value}
      ) do
    dynamic([_, _, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:not, {:since, value}}
      ) do
    dynamic([_, _, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:since, value}
      ) do
    dynamic([_, _, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:not, {:exists, value}}
      ) do
    dynamic([_, _, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 5},
        key,
        {:exists, value}
      ) do
    dynamic([_, _, _, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:not, {:ids, value}}
      ) do
    dynamic([_, _, _, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:ids, value}
      ) do
    dynamic([_, _, _, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:not, {:before, value}}
      ) do
    dynamic([_, _, _, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:before, value}
      ) do
    dynamic([_, _, _, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:not, {:after, value}}
      ) do
    dynamic([_, _, _, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:after, value}
      ) do
    dynamic([_, _, _, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:not, {:until, value}}
      ) do
    dynamic([_, _, _, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:until, value}
      ) do
    dynamic([_, _, _, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:not, {:since, value}}
      ) do
    dynamic([_, _, _, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:since, value}
      ) do
    dynamic([_, _, _, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:not, {:exists, value}}
      ) do
    dynamic([_, _, _, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 6},
        key,
        {:exists, value}
      ) do
    dynamic([_, _, _, _, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:not, {:ids, value}}
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:ids, value}
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:not, {:before, value}}
      ) do
    dynamic([_, _, _, _, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:before, value}
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:not, {:after, value}}
      ) do
    dynamic([_, _, _, _, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:after, value}
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:not, {:until, value}}
      ) do
    dynamic([_, _, _, _, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:until, value}
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:not, {:since, value}}
      ) do
    dynamic([_, _, _, _, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:since, value}
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:not, {:exists, value}}
      ) do
    dynamic([_, _, _, _, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 7},
        key,
        {:exists, value}
      ) do
    dynamic([_, _, _, _, _, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:not, {:ids, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:ids, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:not, {:before, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:before, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:not, {:after, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:after, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:not, {:until, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:until, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:not, {:since, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:since, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:not, {:exists, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 8},
        key,
        {:exists, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:not, {:ids, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:ids, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:not, {:before, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:before, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:not, {:after, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:after, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:not, {:until, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:until, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:not, {:since, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:since, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:not, {:exists, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 9},
        key,
        {:exists, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:not, {:ids, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:ids, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:not, {:before, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:before, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:not, {:after, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:after, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:not, {:until, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:until, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:not, {:since, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:since, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:not, {:exists, value}}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 10},
        key,
        {:exists, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], exists(value))
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end