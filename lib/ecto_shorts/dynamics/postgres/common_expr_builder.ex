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
  def specs_for(directive, {bind_op, target_var}, q_var, opts) do
    context = opts[:context]

    key_var = Macro.var(:key, context)
    negated_var = Macro.var(:negated, context)
    value_var = Macro.var(:value, context)

    [
      %Blueprint{
        guard: nil,
        key: key,
        head: [negated_var, value_var],
        body: quote_body(key, binding_selector_ast, {q_var, key_var, negated_var, value_var}, context)
      }
    ]
  end

  @doc false
  def quote_body(key, {bind_op, bind_to_var}, {q_var, _key_var, negated_var, value_var}, context) do
    field_expr = field_expr(key, q_var, value_var)

    quote do
      case unquote(negated_var) do
        :not ->
          unquote(
            Helpers.dyn_expr(
              {bind_op, bind_to_var},
              q_var,
              Helpers.negated_expr(field_expr),
              context
            )
          )

        _ ->
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
