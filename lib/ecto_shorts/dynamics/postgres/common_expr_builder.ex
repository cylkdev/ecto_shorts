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
  def specs_for(directive, {bind_op, bind_to_var}, q_var, opts) do
    context = opts[:context]

    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)
    field_expr = expr_for(directive, q_var, value_var)

    [
      %Blueprint{
        guard: nil,
        key: key_var,
        head: {directive, value_var},
        body:
          quote do
            if unquote(key_var) === :not do
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  Helpers.negated_expr(field_expr),
                  context
                )
              )
            else
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  field_expr,
                  context
                )
              )
            end
          end
      }
    ]
  end

  @doc false
  def expr_for(:ids, q_var, value_var) do
    quote do
      field(unquote(q_var), :id) in ^unquote(value_var)
    end
  end

  def expr_for(:after, q_var, value_var) do
    quote do
      field(unquote(q_var), :id) > ^unquote(value_var)
    end
  end

  def expr_for(:before, q_var, value_var) do
    quote do
      field(unquote(q_var), :id) < ^unquote(value_var)
    end
  end

  def expr_for(:since, q_var, value_var) do
    quote do
      field(unquote(q_var), :id) >= ^unquote(value_var)
    end
  end

  def expr_for(:until, q_var, value_var) do
    quote do
      field(unquote(q_var), :id) <= ^unquote(value_var)
    end
  end

  def expr_for(key, q_var, value_var) when key in [:start_date, :since_date] do
    quote do
      field(unquote(q_var), :inserted_at) >= ^unquote(value_var)
    end
  end

  def expr_for(key, q_var, value_var) when key in [:end_date, :until_date] do
    quote do
      field(unquote(q_var), :inserted_at) <= ^unquote(value_var)
    end
  end

  def expr_for(:after_date, q_var, value_var) do
    quote do
      field(unquote(q_var), :inserted_at) > ^unquote(value_var)
    end
  end

  def expr_for(:before_date, q_var, value_var) do
    quote do
      field(unquote(q_var), :inserted_at) < ^unquote(value_var)
    end
  end

  def expr_for(:exists, _q_var, value_var) do
    quote do
      exists(unquote(value_var))
    end
  end
end
