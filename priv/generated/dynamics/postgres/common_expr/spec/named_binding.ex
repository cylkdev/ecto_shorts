defmodule EctoShorts.Dynamics.Postgres.CommonExpr.Compiled.NamedBinding do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type binding_selector :: {:as | :at, term()}
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
        :start_date,
        {:not, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :inserted_at) >= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :inserted_at) >= ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :start_date,
        value
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :inserted_at) >= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :end_date,
        {:not, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :inserted_at) <= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :inserted_at) <= ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :end_date,
        value
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :inserted_at) <= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :inserted_at) <= ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :since_date,
        {:not, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :inserted_at) >= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :inserted_at) >= ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :since_date,
        value
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :inserted_at) >= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :inserted_at) >= ^value)
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :until_date,
        {:not, value}
      ) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, :inserted_at) <= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, :inserted_at) <= ^value))
    end
  end

  def dynamic_expr(
        {:as, binding_alias},
        :until_date,
        value
      ) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, :inserted_at) <= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, :inserted_at) <= ^value)
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

  def dynamic_expr(_, _, _) do
    nil
  end
end