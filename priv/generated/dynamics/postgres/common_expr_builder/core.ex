defmodule EctoShorts.Dynamics.Postgres.CommonExpr.Compiled.Core do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, key, {:ids, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], field(q, :id) not in ^value)
      else
        dynamic([{^binding_alias, q}], field(q, :id) not in ^value)
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, :id) in ^value)
      else
        dynamic([{^binding_alias, q}], field(q, :id) in ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:before, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, :id) < ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, :id) < ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, :id) < ^value)
      else
        dynamic([{^binding_alias, q}], field(q, :id) < ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:after, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, :id) > ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, :id) > ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, :id) > ^value)
      else
        dynamic([{^binding_alias, q}], field(q, :id) > ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:until, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, :id) <= ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, :id) <= ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, :id) <= ^value)
      else
        dynamic([{^binding_alias, q}], field(q, :id) <= ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:since, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not (field(q, :id) >= ^value))
      else
        dynamic([{^binding_alias, q}], not (field(q, :id) >= ^value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], field(q, :id) >= ^value)
      else
        dynamic([{^binding_alias, q}], field(q, :id) >= ^value)
      end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:exists, value}) do
    if key === :not do
      if is_nil(binding_alias) do
        dynamic([q], not exists(value))
      else
        dynamic([{^binding_alias, q}], not exists(value))
      end
    else
      if is_nil(binding_alias) do
        dynamic([q], exists(value))
      else
        dynamic([{^binding_alias, q}], exists(value))
      end
    end
  end

  def dynamic_expr({:at, 1}, key, {:ids, value}) do
    if key === :not do
      dynamic([q], field(q, :id) not in ^value)
    else
      dynamic([q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:before, value}) do
    if key === :not do
      dynamic([q], not (field(q, :id) < ^value))
    else
      dynamic([q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:after, value}) do
    if key === :not do
      dynamic([q], not (field(q, :id) > ^value))
    else
      dynamic([q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:until, value}) do
    if key === :not do
      dynamic([q], not (field(q, :id) <= ^value))
    else
      dynamic([q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:since, value}) do
    if key === :not do
      dynamic([q], not (field(q, :id) >= ^value))
    else
      dynamic([q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:exists, value}) do
    if key === :not do
      dynamic([q], not exists(value))
    else
      dynamic([q], exists(value))
    end
  end

  def dynamic_expr({:at, 2}, key, {:ids, value}) do
    if key === :not do
      dynamic([_, q], field(q, :id) not in ^value)
    else
      dynamic([_, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:before, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, :id) < ^value))
    else
      dynamic([_, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:after, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, :id) > ^value))
    else
      dynamic([_, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:until, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, :id) <= ^value))
    else
      dynamic([_, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:since, value}) do
    if key === :not do
      dynamic([_, q], not (field(q, :id) >= ^value))
    else
      dynamic([_, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:exists, value}) do
    if key === :not do
      dynamic([_, q], not exists(value))
    else
      dynamic([_, q], exists(value))
    end
  end

  def dynamic_expr({:at, 3}, key, {:ids, value}) do
    if key === :not do
      dynamic([_, _, q], field(q, :id) not in ^value)
    else
      dynamic([_, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:before, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, :id) < ^value))
    else
      dynamic([_, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:after, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, :id) > ^value))
    else
      dynamic([_, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:until, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, :id) <= ^value))
    else
      dynamic([_, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:since, value}) do
    if key === :not do
      dynamic([_, _, q], not (field(q, :id) >= ^value))
    else
      dynamic([_, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:exists, value}) do
    if key === :not do
      dynamic([_, _, q], not exists(value))
    else
      dynamic([_, _, q], exists(value))
    end
  end

  def dynamic_expr({:at, 4}, key, {:ids, value}) do
    if key === :not do
      dynamic([_, _, _, q], field(q, :id) not in ^value)
    else
      dynamic([_, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:before, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, :id) < ^value))
    else
      dynamic([_, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:after, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, :id) > ^value))
    else
      dynamic([_, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:until, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, :id) <= ^value))
    else
      dynamic([_, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:since, value}) do
    if key === :not do
      dynamic([_, _, _, q], not (field(q, :id) >= ^value))
    else
      dynamic([_, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:exists, value}) do
    if key === :not do
      dynamic([_, _, _, q], not exists(value))
    else
      dynamic([_, _, _, q], exists(value))
    end
  end

  def dynamic_expr({:at, 5}, key, {:ids, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], field(q, :id) not in ^value)
    else
      dynamic([_, _, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:before, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, :id) < ^value))
    else
      dynamic([_, _, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:after, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, :id) > ^value))
    else
      dynamic([_, _, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:until, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, :id) <= ^value))
    else
      dynamic([_, _, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:since, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not (field(q, :id) >= ^value))
    else
      dynamic([_, _, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:exists, value}) do
    if key === :not do
      dynamic([_, _, _, _, q], not exists(value))
    else
      dynamic([_, _, _, _, q], exists(value))
    end
  end

  def dynamic_expr({:at, 6}, key, {:ids, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], field(q, :id) not in ^value)
    else
      dynamic([_, _, _, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:before, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, :id) < ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:after, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, :id) > ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:until, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, :id) <= ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:since, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not (field(q, :id) >= ^value))
    else
      dynamic([_, _, _, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:exists, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, q], not exists(value))
    else
      dynamic([_, _, _, _, _, q], exists(value))
    end
  end

  def dynamic_expr({:at, 7}, key, {:ids, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], field(q, :id) not in ^value)
    else
      dynamic([_, _, _, _, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:before, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, :id) < ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:after, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, :id) > ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:until, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, :id) <= ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:since, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not (field(q, :id) >= ^value))
    else
      dynamic([_, _, _, _, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:exists, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, q], not exists(value))
    else
      dynamic([_, _, _, _, _, _, q], exists(value))
    end
  end

  def dynamic_expr({:at, 8}, key, {:ids, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], field(q, :id) not in ^value)
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:before, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) < ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:after, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) > ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:until, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) <= ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:since, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not (field(q, :id) >= ^value))
    else
      dynamic([_, _, _, _, _, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:exists, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, q], not exists(value))
    else
      dynamic([_, _, _, _, _, _, _, q], exists(value))
    end
  end

  def dynamic_expr({:at, 9}, key, {:ids, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) not in ^value)
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:before, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) < ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:after, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) > ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:until, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) <= ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:since, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :id) >= ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:exists, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, q], not exists(value))
    else
      dynamic([_, _, _, _, _, _, _, _, q], exists(value))
    end
  end

  def dynamic_expr({:at, 10}, key, {:ids, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) not in ^value)
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) in ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:before, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) < ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) < ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:after, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) > ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) > ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:until, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) <= ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:since, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :id) >= ^value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:exists, value}) do
    if key === :not do
      dynamic([_, _, _, _, _, _, _, _, _, q], not exists(value))
    else
      dynamic([_, _, _, _, _, _, _, _, _, q], exists(value))
    end
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end