defmodule EctoShorts.Dynamics.Postgres.CommonExpr.Compiled.Temporal do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type negated :: :not | nil
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, :start_date, negated, value) do
    case negated do
      :not ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, :inserted_at) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, :inserted_at) >= ^value))
        end

      _ ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, :inserted_at) >= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, :inserted_at) >= ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, :end_date, negated, value) do
    case negated do
      :not ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, :inserted_at) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, :inserted_at) <= ^value))
        end

      _ ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, :inserted_at) <= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, :inserted_at) <= ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, :since_date, negated, value) do
    case negated do
      :not ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, :inserted_at) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, :inserted_at) >= ^value))
        end

      _ ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, :inserted_at) >= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, :inserted_at) >= ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, :until_date, negated, value) do
    case negated do
      :not ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, :inserted_at) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, :inserted_at) <= ^value))
        end

      _ ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, :inserted_at) <= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, :inserted_at) <= ^value)
        end
    end
  end

  def dynamic_expr({:at, 1}, :start_date, negated, value) do
    case negated do
      :not -> dynamic([q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 1}, :end_date, negated, value) do
    case negated do
      :not -> dynamic([q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 1}, :since_date, negated, value) do
    case negated do
      :not -> dynamic([q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 1}, :until_date, negated, value) do
    case negated do
      :not -> dynamic([q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 2}, :start_date, negated, value) do
    case negated do
      :not -> dynamic([_, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 2}, :end_date, negated, value) do
    case negated do
      :not -> dynamic([_, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 2}, :since_date, negated, value) do
    case negated do
      :not -> dynamic([_, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 2}, :until_date, negated, value) do
    case negated do
      :not -> dynamic([_, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 3}, :start_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 3}, :end_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 3}, :since_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 3}, :until_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 4}, :start_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 4}, :end_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 4}, :since_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 4}, :until_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 5}, :start_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 5}, :end_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 5}, :since_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 5}, :until_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 6}, :start_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 6}, :end_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 6}, :since_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 6}, :until_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 7}, :start_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 7}, :end_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 7}, :since_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 7}, :until_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 8}, :start_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 8}, :end_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 8}, :since_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 8}, :until_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 9}, :start_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 9}, :end_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 9}, :since_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 9}, :until_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 10}, :start_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 10}, :end_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr({:at, 10}, :since_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) >= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr({:at, 10}, :until_date, negated, value) do
    case negated do
      :not -> dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, :inserted_at) <= ^value))
      _ -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr(_, _, _, _) do
    nil
  end
end