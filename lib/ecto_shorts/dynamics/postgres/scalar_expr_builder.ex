defmodule EctoShorts.Dynamics.Postgres.ScalarExprBuilder do
  @moduledoc since: "3.0.0"
  @moduledoc false

  alias EctoShorts.Generator.Blueprint
  alias EctoShorts.Dynamics.Helpers

  @keys [
    :==,
    :eq,
    :!=,
    :ne,
    :>,
    :>=,
    :<,
    :<=,
    :gt,
    :gte,
    :lt,
    :lte,
    :in,
    :like,
    :ilike
  ]

  @behaviour EctoShorts.Generator.ClauseSpec

  @impl true
  def keys, do: @keys

  @impl true
  def specs_for(directive, {bind_op, bind_to_var}, q_var, opts) do
    context = opts[:context]

    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)
    field_expr = expr_for(directive, q_var, {key_var, value_var})

    [
      %Blueprint{
        guard: nil,
        key: key_var,
        head: {:not, {directive, value_var}},
        body:
          quote do
            unquote(
              Helpers.dyn_expr(
                {bind_op, bind_to_var},
                q_var,
                Helpers.negated_expr(field_expr),
                context
              )
            )
          end
      },
      %Blueprint{
        guard: nil,
        key: key_var,
        head: {directive, value_var},
        body:
          quote do
            unquote(
              Helpers.dyn_expr(
                {bind_op, bind_to_var},
                q_var,
                field_expr,
                context
              )
            )
          end
      }
    ]
  end

  @doc false
  def expr_for(key, q_var, {key_var, nil}) when key in [:==, :eq] do
    quote do
      is_nil(field(unquote(q_var), ^unquote(key_var)))
    end
  end

  def expr_for(key, q_var, {key_var, nil}) when key in [:!=, :ne] do
    quote do
      not is_nil(field(unquote(q_var), ^unquote(key_var)))
    end
  end

  def expr_for(key, q_var, {key_var, {transform, value_var}})
      when key in [:==, :eq] and transform in [:lower, :upper] do
    transform_expr = "#{transform}(?)"

    quote do
      fragment(unquote(transform_expr), field(unquote(q_var), ^unquote(key_var))) ==
        ^unquote(value_var)
    end
  end

  def expr_for(key, q_var, {key_var, value_var}) when key in [:==, :eq] do
    quote do
      field(unquote(q_var), ^unquote(key_var)) == ^unquote(value_var)
    end
  end

  def expr_for(key, q_var, {key_var, {transform, value_var}})
      when key in [:!=, :ne] and transform in [:lower, :upper] do
    transform_expr = "#{transform}(?)"

    quote do
      fragment(unquote(transform_expr), field(unquote(q_var), ^unquote(key_var))) !=
        ^unquote(value_var)
    end
  end

  def expr_for(key, q_var, {key_var, value_var}) when key in [:!=, :ne] do
    quote do
      field(unquote(q_var), ^unquote(key_var)) != ^unquote(value_var)
    end
  end

  def expr_for(key, q_var, {key_var, value_var}) when key in [:>, :gt] do
    quote do
      field(unquote(q_var), ^unquote(key_var)) > ^unquote(value_var)
    end
  end

  def expr_for(key, q_var, {key_var, value_var}) when key in [:>=, :gte] do
    quote do
      field(unquote(q_var), ^unquote(key_var)) >= ^unquote(value_var)
    end
  end

  def expr_for(key, q_var, {key_var, value_var}) when key in [:<, :lt] do
    quote do
      field(unquote(q_var), ^unquote(key_var)) < ^unquote(value_var)
    end
  end

  def expr_for(key, q_var, {key_var, value_var}) when key in [:<=, :lte] do
    quote do
      field(unquote(q_var), ^unquote(key_var)) <= ^unquote(value_var)
    end
  end

  def expr_for(:in, q_var, {key_var, value_var}) do
    quote do
      field(unquote(q_var), ^unquote(key_var)) in ^unquote(value_var)
    end
  end

  def expr_for(:like, q_var, {key_var, value_var}) do
    quote do
      like(field(unquote(q_var), ^unquote(key_var)), ^"%#{unquote(value_var)}%")
    end
  end

  def expr_for(:ilike, q_var, {key_var, value_var}) do
    quote do
      ilike(field(unquote(q_var), ^unquote(key_var)), ^"%#{unquote(value_var)}%")
    end
  end
end
