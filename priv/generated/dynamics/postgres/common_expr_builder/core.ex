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
        :ids,
        {:not, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) not in ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) not in ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :ids,
        value
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) in ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) in ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :before,
        {:not, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :id) < ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :id) < ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :before,
        value
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) < ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) < ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :after,
        {:not, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :id) > ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :id) > ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :after,
        value
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) > ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) > ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :until,
        {:not, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :id) <= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :id) <= ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :until,
        value
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) <= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :since,
        {:not, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :id) >= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :id) >= ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :since,
        value
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) >= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :exists,
        {:not, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not exists(value))
    else
      dynamic([{^binding_alias, q}], not exists(value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :exists,
        value
      ) do
    if is_nil(binding_alias) do
      dynamic([q], exists(value))
    else
      dynamic([{^binding_alias, q}], exists(value))
    end
  end

  def dynamic_expr(
        {:at, 1},
        :ids,
        {:not, value}
      ) do
    dynamic([q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 1},
        :ids,
        value
      ) do
    dynamic([q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 1},
        :before,
        {:not, value}
      ) do
    dynamic([q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 1},
        :before,
        value
      ) do
    dynamic([q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 1},
        :after,
        {:not, value}
      ) do
    dynamic([q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 1},
        :after,
        value
      ) do
    dynamic([q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 1},
        :until,
        {:not, value}
      ) do
    dynamic([q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 1},
        :until,
        value
      ) do
    dynamic([q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 1},
        :since,
        {:not, value}
      ) do
    dynamic([q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 1},
        :since,
        value
      ) do
    dynamic([q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 1},
        :exists,
        {:not, value}
      ) do
    dynamic([q], not exists(value))
  end

  def dynamic_expr(
        {:at, 1},
        :exists,
        value
      ) do
    dynamic([q], exists(value))
  end

  def dynamic_expr(
        {:at, 2},
        :ids,
        {:not, value}
      ) do
    dynamic([_, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 2},
        :ids,
        value
      ) do
    dynamic([_, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 2},
        :before,
        {:not, value}
      ) do
    dynamic([_, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 2},
        :before,
        value
      ) do
    dynamic([_, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 2},
        :after,
        {:not, value}
      ) do
    dynamic([_, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 2},
        :after,
        value
      ) do
    dynamic([_, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 2},
        :until,
        {:not, value}
      ) do
    dynamic([_, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 2},
        :until,
        value
      ) do
    dynamic([_, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 2},
        :since,
        {:not, value}
      ) do
    dynamic([_, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 2},
        :since,
        value
      ) do
    dynamic([_, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 2},
        :exists,
        {:not, value}
      ) do
    dynamic([_, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 2},
        :exists,
        value
      ) do
    dynamic([_, q], exists(value))
  end

  def dynamic_expr(
        {:at, 3},
        :ids,
        {:not, value}
      ) do
    dynamic([_, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 3},
        :ids,
        value
      ) do
    dynamic([_, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 3},
        :before,
        {:not, value}
      ) do
    dynamic([_, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 3},
        :before,
        value
      ) do
    dynamic([_, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 3},
        :after,
        {:not, value}
      ) do
    dynamic([_, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 3},
        :after,
        value
      ) do
    dynamic([_, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 3},
        :until,
        {:not, value}
      ) do
    dynamic([_, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 3},
        :until,
        value
      ) do
    dynamic([_, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 3},
        :since,
        {:not, value}
      ) do
    dynamic([_, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 3},
        :since,
        value
      ) do
    dynamic([_, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 3},
        :exists,
        {:not, value}
      ) do
    dynamic([_, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 3},
        :exists,
        value
      ) do
    dynamic([_, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 4},
        :ids,
        {:not, value}
      ) do
    dynamic([_, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 4},
        :ids,
        value
      ) do
    dynamic([_, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 4},
        :before,
        {:not, value}
      ) do
    dynamic([_, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 4},
        :before,
        value
      ) do
    dynamic([_, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 4},
        :after,
        {:not, value}
      ) do
    dynamic([_, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 4},
        :after,
        value
      ) do
    dynamic([_, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 4},
        :until,
        {:not, value}
      ) do
    dynamic([_, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 4},
        :until,
        value
      ) do
    dynamic([_, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 4},
        :since,
        {:not, value}
      ) do
    dynamic([_, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 4},
        :since,
        value
      ) do
    dynamic([_, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 4},
        :exists,
        {:not, value}
      ) do
    dynamic([_, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 4},
        :exists,
        value
      ) do
    dynamic([_, _, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 5},
        :ids,
        {:not, value}
      ) do
    dynamic([_, _, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 5},
        :ids,
        value
      ) do
    dynamic([_, _, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 5},
        :before,
        {:not, value}
      ) do
    dynamic([_, _, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 5},
        :before,
        value
      ) do
    dynamic([_, _, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 5},
        :after,
        {:not, value}
      ) do
    dynamic([_, _, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 5},
        :after,
        value
      ) do
    dynamic([_, _, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 5},
        :until,
        {:not, value}
      ) do
    dynamic([_, _, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 5},
        :until,
        value
      ) do
    dynamic([_, _, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 5},
        :since,
        {:not, value}
      ) do
    dynamic([_, _, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 5},
        :since,
        value
      ) do
    dynamic([_, _, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 5},
        :exists,
        {:not, value}
      ) do
    dynamic([_, _, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 5},
        :exists,
        value
      ) do
    dynamic([_, _, _, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 6},
        :ids,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 6},
        :ids,
        value
      ) do
    dynamic([_, _, _, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 6},
        :before,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 6},
        :before,
        value
      ) do
    dynamic([_, _, _, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 6},
        :after,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 6},
        :after,
        value
      ) do
    dynamic([_, _, _, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 6},
        :until,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 6},
        :until,
        value
      ) do
    dynamic([_, _, _, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 6},
        :since,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 6},
        :since,
        value
      ) do
    dynamic([_, _, _, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 6},
        :exists,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 6},
        :exists,
        value
      ) do
    dynamic([_, _, _, _, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 7},
        :ids,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 7},
        :ids,
        value
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 7},
        :before,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 7},
        :before,
        value
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 7},
        :after,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 7},
        :after,
        value
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 7},
        :until,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 7},
        :until,
        value
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 7},
        :since,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 7},
        :since,
        value
      ) do
    dynamic([_, _, _, _, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 7},
        :exists,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 7},
        :exists,
        value
      ) do
    dynamic([_, _, _, _, _, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 8},
        :ids,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 8},
        :ids,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 8},
        :before,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 8},
        :before,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 8},
        :after,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 8},
        :after,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 8},
        :until,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 8},
        :until,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 8},
        :since,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 8},
        :since,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 8},
        :exists,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 8},
        :exists,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 9},
        :ids,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 9},
        :ids,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 9},
        :before,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 9},
        :before,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 9},
        :after,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 9},
        :after,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 9},
        :until,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 9},
        :until,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 9},
        :since,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 9},
        :since,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 9},
        :exists,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 9},
        :exists,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, _, q], exists(value))
  end

  def dynamic_expr(
        {:at, 10},
        :ids,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr(
        {:at, 10},
        :ids,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr(
        {:at, 10},
        :before,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr(
        {:at, 10},
        :before,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr(
        {:at, 10},
        :after,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr(
        {:at, 10},
        :after,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr(
        {:at, 10},
        :until,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr(
        {:at, 10},
        :until,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr(
        {:at, 10},
        :since,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr(
        {:at, 10},
        :since,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(
        {:at, 10},
        :exists,
        {:not, value}
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not exists(value))
  end

  def dynamic_expr(
        {:at, 10},
        :exists,
        value
      ) do
    dynamic([_, _, _, _, _, _, _, _, _, q], exists(value))
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end