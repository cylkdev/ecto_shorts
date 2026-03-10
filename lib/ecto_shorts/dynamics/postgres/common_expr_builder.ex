defmodule EctoShorts.Dynamics.Postgres.CommonExprBuilder do
  @moduledoc since: "3.0.0"
  @moduledoc false

  alias EctoShorts.Generator.Blueprint
  alias EctoShorts.Dynamics.Helpers

  @keys [
    :ids,
    :before,
    :after,
    :until,
    :since,
    :start_date,
    :end_date,
    :since_date,
    :until_date,
    :exists
  ]

  @behaviour EctoShorts.Generator.ClauseSpec

  @impl true
  def keys, do: @keys

  @impl true
  def specs_for(key, {binding_directive, target_var}, q_var, opts) do
    context = opts[:context]

    value_var = Macro.var(:value, context)
    field_expr = field_expr(key, q_var, value_var)

    [
      %Blueprint{
        guard: nil,
        key: key,
        head: {:not, quote(do: unquote(value_var))},
        body:
          Helpers.dyn_expr(
            {binding_directive, target_var},
            q_var,
            Helpers.negated_expr(field_expr),
            context
          )
      },
      %Blueprint{
        guard: nil,
        key: key,
        head: value_var,
        body:
          Helpers.dyn_expr(
            {binding_directive, target_var},
            q_var,
            field_expr,
            context
          )
      }
    ]
  end

  @doc false
  def field_expr(:ids, q_var, value_var) do
    quote do
      field(unquote(q_var), :id) in ^unquote(value_var)
    end
  end

  def field_expr(:after, q_var, value_var) do
    quote do
      field(unquote(q_var), :id) > ^unquote(value_var)
    end
  end

  def field_expr(:before, q_var, value_var) do
    quote do
      field(unquote(q_var), :id) < ^unquote(value_var)
    end
  end

  def field_expr(:since, q_var, value_var) do
    quote do
      field(unquote(q_var), :id) >= ^unquote(value_var)
    end
  end

  def field_expr(:until, q_var, value_var) do
    quote do
      field(unquote(q_var), :id) <= ^unquote(value_var)
    end
  end

  def field_expr(key, q_var, value_var) when key in [:start_date, :since_date] do
    quote do
      field(unquote(q_var), :inserted_at) >= ^unquote(value_var)
    end
  end

  def field_expr(key, q_var, value_var) when key in [:end_date, :until_date] do
    quote do
      field(unquote(q_var), :inserted_at) <= ^unquote(value_var)
    end
  end

  def field_expr(:after_date, q_var, value_var) do
    quote do
      field(unquote(q_var), :inserted_at) > ^unquote(value_var)
    end
  end

  def field_expr(:before_date, q_var, value_var) do
    quote do
      field(unquote(q_var), :inserted_at) < ^unquote(value_var)
    end
  end

  def field_expr(:exists, _q_var, value_var) do
    quote do
      exists(unquote(value_var))
    end
  end
end
