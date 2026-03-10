defmodule EctoShorts.Dynamics.Postgres.CommonExpr.Compiled.Core do
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
        :ids,
        negated,
        value
      ) do
    case negated do
      :not ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, :id) not in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, :id) not in ^value)
        end

      _ ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, :id) in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, :id) in ^value)
        end
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :before,
        negated,
        value
      ) do
    case negated do
      :not ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, :id) < ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, :id) < ^value))
        end

      _ ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, :id) < ^value)
        else
          dynamic([{^binding_alias, q}], field(q, :id) < ^value)
        end
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :after,
        negated,
        value
      ) do
    case negated do
      :not ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, :id) > ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, :id) > ^value))
        end

      _ ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, :id) > ^value)
        else
          dynamic([{^binding_alias, q}], field(q, :id) > ^value)
        end
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :until,
        negated,
        value
      ) do
    case negated do
      :not ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, :id) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, :id) <= ^value))
        end

      _ ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, :id) <= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, :id) <= ^value)
        end
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :since,
        negated,
        value
      ) do
    case negated do
      :not ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, :id) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, :id) >= ^value))
        end

      _ ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, :id) >= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, :id) >= ^value)
        end
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :exists,
        negated,
        value
      ) do
    case negated do
      :not ->
        if is_nil(binding_alias) do
          dynamic([q], not exists(value))
        else
          dynamic([{^binding_alias, q}], not exists(value))
        end

      _ ->
        if is_nil(binding_alias) do
          dynamic([q], exists(value))
        else
          dynamic([{^binding_alias, q}], exists(value))
        end
    end
  end

  def dynamic_expr(
        {:at, 1},
        :ids,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([q], field(q, :id) not in ^value)
      _ -> dynamic([q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr(
        {:at, 1},
        :before,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([q], not (field(q, :id) < ^value))
      _ -> dynamic([q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr(
        {:at, 1},
        :after,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([q], not (field(q, :id) > ^value))
      _ -> dynamic([q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr(
        {:at, 1},
        :until,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([q], not (field(q, :id) <= ^value))
      _ -> dynamic([q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 1},
        :since,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([q], not (field(q, :id) >= ^value))
      _ -> dynamic([q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr(
        {:at, 1},
        :exists,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([q], not exists(value))
      _ -> dynamic([q], exists(value))
    end
  end

  def dynamic_expr(
        {:at, 2},
        :ids,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, q], field(q, :id) not in ^value)
      _ -> dynamic([_, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr(
        {:at, 2},
        :before,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, q], not (field(q, :id) < ^value))
      _ -> dynamic([_, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr(
        {:at, 2},
        :after,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, q], not (field(q, :id) > ^value))
      _ -> dynamic([_, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr(
        {:at, 2},
        :until,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, q], not (field(q, :id) <= ^value))
      _ -> dynamic([_, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 2},
        :since,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, q], not (field(q, :id) >= ^value))
      _ -> dynamic([_, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr(
        {:at, 2},
        :exists,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, q], not exists(value))
      _ -> dynamic([_, q], exists(value))
    end
  end

  def dynamic_expr(
        {:at, 3},
        :ids,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, q], field(q, :id) not in ^value)
      _ -> dynamic([_, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr(
        {:at, 3},
        :before,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, q], not (field(q, :id) < ^value))
      _ -> dynamic([_, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr(
        {:at, 3},
        :after,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, q], not (field(q, :id) > ^value))
      _ -> dynamic([_, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr(
        {:at, 3},
        :until,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, q], not (field(q, :id) <= ^value))
      _ -> dynamic([_, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 3},
        :since,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, q], not (field(q, :id) >= ^value))
      _ -> dynamic([_, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr(
        {:at, 3},
        :exists,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, q], not exists(value))
      _ -> dynamic([_, _, q], exists(value))
    end
  end

  def dynamic_expr(
        {:at, 4},
        :ids,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, q], field(q, :id) not in ^value)
      _ -> dynamic([_, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr(
        {:at, 4},
        :before,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, q], not (field(q, :id) < ^value))
      _ -> dynamic([_, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr(
        {:at, 4},
        :after,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, q], not (field(q, :id) > ^value))
      _ -> dynamic([_, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr(
        {:at, 4},
        :until,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, q], not (field(q, :id) <= ^value))
      _ -> dynamic([_, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 4},
        :since,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, q], not (field(q, :id) >= ^value))
      _ -> dynamic([_, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr(
        {:at, 4},
        :exists,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, q], not exists(value))
      _ -> dynamic([_, _, _, q], exists(value))
    end
  end

  def dynamic_expr(
        {:at, 5},
        :ids,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, q], field(q, :id) not in ^value)
      _ -> dynamic([_, _, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr(
        {:at, 5},
        :before,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, q], not (field(q, :id) < ^value))
      _ -> dynamic([_, _, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr(
        {:at, 5},
        :after,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, q], not (field(q, :id) > ^value))
      _ -> dynamic([_, _, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr(
        {:at, 5},
        :until,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, q], not (field(q, :id) <= ^value))
      _ -> dynamic([_, _, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 5},
        :since,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, q], not (field(q, :id) >= ^value))
      _ -> dynamic([_, _, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr(
        {:at, 5},
        :exists,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, q], not exists(value))
      _ -> dynamic([_, _, _, _, q], exists(value))
    end
  end

  def dynamic_expr(
        {:at, 6},
        :ids,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, q], field(q, :id) not in ^value)
      _ -> dynamic([_, _, _, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr(
        {:at, 6},
        :before,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, q], not (field(q, :id) < ^value))
      _ -> dynamic([_, _, _, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr(
        {:at, 6},
        :after,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, q], not (field(q, :id) > ^value))
      _ -> dynamic([_, _, _, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr(
        {:at, 6},
        :until,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, q], not (field(q, :id) <= ^value))
      _ -> dynamic([_, _, _, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 6},
        :since,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, q], not (field(q, :id) >= ^value))
      _ -> dynamic([_, _, _, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr(
        {:at, 6},
        :exists,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, q], not exists(value))
      _ -> dynamic([_, _, _, _, _, q], exists(value))
    end
  end

  def dynamic_expr(
        {:at, 7},
        :ids,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, q], field(q, :id) not in ^value)
      _ -> dynamic([_, _, _, _, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr(
        {:at, 7},
        :before,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, q], not (field(q, :id) < ^value))
      _ -> dynamic([_, _, _, _, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr(
        {:at, 7},
        :after,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, q], not (field(q, :id) > ^value))
      _ -> dynamic([_, _, _, _, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr(
        {:at, 7},
        :until,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, q], not (field(q, :id) <= ^value))
      _ -> dynamic([_, _, _, _, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 7},
        :since,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, q], not (field(q, :id) >= ^value))
      _ -> dynamic([_, _, _, _, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr(
        {:at, 7},
        :exists,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, q], not exists(value))
      _ -> dynamic([_, _, _, _, _, _, q], exists(value))
    end
  end

  def dynamic_expr(
        {:at, 8},
        :ids,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, q], field(q, :id) not in ^value)
      _ -> dynamic([_, _, _, _, _, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr(
        {:at, 8},
        :before,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) < ^value))
      _ -> dynamic([_, _, _, _, _, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr(
        {:at, 8},
        :after,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) > ^value))
      _ -> dynamic([_, _, _, _, _, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr(
        {:at, 8},
        :until,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) <= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 8},
        :since,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) >= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr(
        {:at, 8},
        :exists,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, q], not exists(value))
      _ -> dynamic([_, _, _, _, _, _, _, q], exists(value))
    end
  end

  def dynamic_expr(
        {:at, 9},
        :ids,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) not in ^value)
      _ -> dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr(
        {:at, 9},
        :before,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) < ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr(
        {:at, 9},
        :after,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) > ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr(
        {:at, 9},
        :until,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) <= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 9},
        :since,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) >= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr(
        {:at, 9},
        :exists,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, q], not exists(value))
      _ -> dynamic([_, _, _, _, _, _, _, _, q], exists(value))
    end
  end

  def dynamic_expr(
        {:at, 10},
        :ids,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) not in ^value)
      _ -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr(
        {:at, 10},
        :before,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) < ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr(
        {:at, 10},
        :after,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) > ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr(
        {:at, 10},
        :until,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) <= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr(
        {:at, 10},
        :since,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) >= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr(
        {:at, 10},
        :exists,
        negated,
        value
      ) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, _, q], not exists(value))
      _ -> dynamic([_, _, _, _, _, _, _, _, _, q], exists(value))
    end
  end

  def dynamic_expr(_, _, _, _) do
    nil
  end
end