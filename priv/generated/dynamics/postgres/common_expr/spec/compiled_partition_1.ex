defmodule EctoShorts.Dynamics.Postgres.CommonExpr.Compiled.Partition1 do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type binding_selector :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, :ids, {:not, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) not in ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) not in ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, :ids, value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) in ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) in ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, :before, {:not, value}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :id) < ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :id) < ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, :before, value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) < ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) < ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, :after, {:not, value}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :id) > ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :id) > ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, :after, value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) > ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) > ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, :until, {:not, value}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :id) <= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :id) <= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, :until, value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) <= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) <= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, :since, {:not, value}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :id) >= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :id) >= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, :since, value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :id) >= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :id) >= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, :start_date, {:not, value}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :inserted_at) >= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :inserted_at) >= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, :start_date, value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :inserted_at) >= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, :end_date, {:not, value}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :inserted_at) <= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :inserted_at) <= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, :end_date, value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :inserted_at) <= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, :since_date, {:not, value}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :inserted_at) >= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :inserted_at) >= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, :since_date, value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :inserted_at) >= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, :until_date, {:not, value}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :inserted_at) <= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :inserted_at) <= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, :until_date, value) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :inserted_at) <= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, :exists, {:not, value}) do
    if is_nil(binding_alias) do
      dynamic([q], not exists(value))
    else
      dynamic([{^binding_alias, q}], not exists(value))
    end
  end

  def dynamic_expr({:as, binding_alias}, :exists, value) do
    if is_nil(binding_alias) do
      dynamic([q], exists(value))
    else
      dynamic([{^binding_alias, q}], exists(value))
    end
  end

  def dynamic_expr({:at, 1}, :ids, {:not, value}) do
    dynamic([q], field(q, :id) not in ^value)
  end

  def dynamic_expr({:at, 1}, :ids, value) do
    dynamic([q], field(q, :id) in ^value)
  end

  def dynamic_expr({:at, 1}, :before, {:not, value}) do
    dynamic([q], not (field(q, :id) < ^value))
  end

  def dynamic_expr({:at, 1}, :before, value) do
    dynamic([q], field(q, :id) < ^value)
  end

  def dynamic_expr({:at, 1}, :after, {:not, value}) do
    dynamic([q], not (field(q, :id) > ^value))
  end

  def dynamic_expr({:at, 1}, :after, value) do
    dynamic([q], field(q, :id) > ^value)
  end

  def dynamic_expr({:at, 1}, :until, {:not, value}) do
    dynamic([q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr({:at, 1}, :until, value) do
    dynamic([q], field(q, :id) <= ^value)
  end

  def dynamic_expr({:at, 1}, :since, {:not, value}) do
    dynamic([q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr({:at, 1}, :since, value) do
    dynamic([q], field(q, :id) >= ^value)
  end

  def dynamic_expr({:at, 1}, :start_date, {:not, value}) do
    dynamic([q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr({:at, 1}, :start_date, value) do
    dynamic([q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr({:at, 1}, :end_date, {:not, value}) do
    dynamic([q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr({:at, 1}, :end_date, value) do
    dynamic([q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr({:at, 1}, :since_date, {:not, value}) do
    dynamic([q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr({:at, 1}, :since_date, value) do
    dynamic([q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr({:at, 1}, :until_date, {:not, value}) do
    dynamic([q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr({:at, 1}, :until_date, value) do
    dynamic([q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr({:at, 1}, :exists, {:not, value}) do
    dynamic([q], not exists(value))
  end

  def dynamic_expr({:at, 1}, :exists, value) do
    dynamic([q], exists(value))
  end

  def dynamic_expr({:at, 2}, :ids, {:not, value}) do
    dynamic([_, q], field(q, :id) not in ^value)
  end

  def dynamic_expr({:at, 2}, :ids, value) do
    dynamic([_, q], field(q, :id) in ^value)
  end

  def dynamic_expr({:at, 2}, :before, {:not, value}) do
    dynamic([_, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr({:at, 2}, :before, value) do
    dynamic([_, q], field(q, :id) < ^value)
  end

  def dynamic_expr({:at, 2}, :after, {:not, value}) do
    dynamic([_, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr({:at, 2}, :after, value) do
    dynamic([_, q], field(q, :id) > ^value)
  end

  def dynamic_expr({:at, 2}, :until, {:not, value}) do
    dynamic([_, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr({:at, 2}, :until, value) do
    dynamic([_, q], field(q, :id) <= ^value)
  end

  def dynamic_expr({:at, 2}, :since, {:not, value}) do
    dynamic([_, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr({:at, 2}, :since, value) do
    dynamic([_, q], field(q, :id) >= ^value)
  end

  def dynamic_expr({:at, 2}, :start_date, {:not, value}) do
    dynamic([_, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr({:at, 2}, :start_date, value) do
    dynamic([_, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr({:at, 2}, :end_date, {:not, value}) do
    dynamic([_, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr({:at, 2}, :end_date, value) do
    dynamic([_, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr({:at, 2}, :since_date, {:not, value}) do
    dynamic([_, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr({:at, 2}, :since_date, value) do
    dynamic([_, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr({:at, 2}, :until_date, {:not, value}) do
    dynamic([_, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr({:at, 2}, :until_date, value) do
    dynamic([_, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr({:at, 2}, :exists, {:not, value}) do
    dynamic([_, q], not exists(value))
  end

  def dynamic_expr({:at, 2}, :exists, value) do
    dynamic([_, q], exists(value))
  end

  def dynamic_expr({:at, 3}, :ids, {:not, value}) do
    dynamic([_, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr({:at, 3}, :ids, value) do
    dynamic([_, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr({:at, 3}, :before, {:not, value}) do
    dynamic([_, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr({:at, 3}, :before, value) do
    dynamic([_, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr({:at, 3}, :after, {:not, value}) do
    dynamic([_, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr({:at, 3}, :after, value) do
    dynamic([_, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr({:at, 3}, :until, {:not, value}) do
    dynamic([_, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr({:at, 3}, :until, value) do
    dynamic([_, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr({:at, 3}, :since, {:not, value}) do
    dynamic([_, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr({:at, 3}, :since, value) do
    dynamic([_, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr({:at, 3}, :start_date, {:not, value}) do
    dynamic([_, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr({:at, 3}, :start_date, value) do
    dynamic([_, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr({:at, 3}, :end_date, {:not, value}) do
    dynamic([_, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr({:at, 3}, :end_date, value) do
    dynamic([_, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr({:at, 3}, :since_date, {:not, value}) do
    dynamic([_, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr({:at, 3}, :since_date, value) do
    dynamic([_, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr({:at, 3}, :until_date, {:not, value}) do
    dynamic([_, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr({:at, 3}, :until_date, value) do
    dynamic([_, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr({:at, 3}, :exists, {:not, value}) do
    dynamic([_, _, q], not exists(value))
  end

  def dynamic_expr({:at, 3}, :exists, value) do
    dynamic([_, _, q], exists(value))
  end

  def dynamic_expr({:at, 4}, :ids, {:not, value}) do
    dynamic([_, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr({:at, 4}, :ids, value) do
    dynamic([_, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr({:at, 4}, :before, {:not, value}) do
    dynamic([_, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr({:at, 4}, :before, value) do
    dynamic([_, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr({:at, 4}, :after, {:not, value}) do
    dynamic([_, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr({:at, 4}, :after, value) do
    dynamic([_, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr({:at, 4}, :until, {:not, value}) do
    dynamic([_, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr({:at, 4}, :until, value) do
    dynamic([_, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr({:at, 4}, :since, {:not, value}) do
    dynamic([_, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr({:at, 4}, :since, value) do
    dynamic([_, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr({:at, 4}, :start_date, {:not, value}) do
    dynamic([_, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr({:at, 4}, :start_date, value) do
    dynamic([_, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr({:at, 4}, :end_date, {:not, value}) do
    dynamic([_, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr({:at, 4}, :end_date, value) do
    dynamic([_, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr({:at, 4}, :since_date, {:not, value}) do
    dynamic([_, _, _, q], not (field(q, :inserted_at) >= ^value))
  end

  def dynamic_expr({:at, 4}, :since_date, value) do
    dynamic([_, _, _, q], field(q, :inserted_at) >= ^value)
  end

  def dynamic_expr({:at, 4}, :until_date, {:not, value}) do
    dynamic([_, _, _, q], not (field(q, :inserted_at) <= ^value))
  end

  def dynamic_expr({:at, 4}, :until_date, value) do
    dynamic([_, _, _, q], field(q, :inserted_at) <= ^value)
  end

  def dynamic_expr({:at, 4}, :exists, {:not, value}) do
    dynamic([_, _, _, q], not exists(value))
  end

  def dynamic_expr({:at, 4}, :exists, value) do
    dynamic([_, _, _, q], exists(value))
  end

  def dynamic_expr({:at, 5}, :ids, {:not, value}) do
    dynamic([_, _, _, _, q], field(q, :id) not in ^value)
  end

  def dynamic_expr({:at, 5}, :ids, value) do
    dynamic([_, _, _, _, q], field(q, :id) in ^value)
  end

  def dynamic_expr({:at, 5}, :before, {:not, value}) do
    dynamic([_, _, _, _, q], not (field(q, :id) < ^value))
  end

  def dynamic_expr({:at, 5}, :before, value) do
    dynamic([_, _, _, _, q], field(q, :id) < ^value)
  end

  def dynamic_expr({:at, 5}, :after, {:not, value}) do
    dynamic([_, _, _, _, q], not (field(q, :id) > ^value))
  end

  def dynamic_expr({:at, 5}, :after, value) do
    dynamic([_, _, _, _, q], field(q, :id) > ^value)
  end

  def dynamic_expr({:at, 5}, :until, {:not, value}) do
    dynamic([_, _, _, _, q], not (field(q, :id) <= ^value))
  end

  def dynamic_expr({:at, 5}, :until, value) do
    dynamic([_, _, _, _, q], field(q, :id) <= ^value)
  end

  def dynamic_expr({:at, 5}, :since, {:not, value}) do
    dynamic([_, _, _, _, q], not (field(q, :id) >= ^value))
  end

  def dynamic_expr({:at, 5}, :since, value) do
    dynamic([_, _, _, _, q], field(q, :id) >= ^value)
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end