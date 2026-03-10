defmodule EctoShorts.Dynamics.Postgres.ScalarExprBuilder do
  @moduledoc since: "3.0.0"
  @moduledoc false

  alias EctoShorts.Generator.Blueprint
  alias EctoShorts.Dynamics.Helpers

  @comparison_operators [
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
    :lte
  ]

  @membership_operators [
    :in
  ]

  @string_operators [
    :like,
    :ilike
  ]

  @keys @comparison_operators ++ @membership_operators ++ @string_operators

  @behaviour EctoShorts.Generator.ClauseSpec

  @impl true
  def keys, do: @keys

  @impl true
  def specs_for(op, {bind_op, bind_to_var}, q_var, opts) do
    context = opts[:context]

    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)
    transform_var = Macro.var(:transform, context)

    field_expr = expr_for(op, q_var, nil, {key_var, value_var})
    field_lower_expr = expr_for(op, q_var, :lower, {key_var, value_var})
    field_upper_expr = expr_for(op, q_var, :upper, {key_var, value_var})

    [
      %Blueprint{
        guard: quote(do: unquote(transform_var) in [:lower, :upper]),
        key: key_var,
        head: {:not, {op, {transform_var, value_var}}},
        body:
          quote do
            case unquote(transform_var) do
              :lower ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    Helpers.negated_expr(field_lower_expr),
                    context
                  )
                )

              :upper ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    Helpers.negated_expr(field_upper_expr),
                    context
                  )
                )
            end
          end
      },
      %Blueprint{
        guard: quote(do: unquote(transform_var) in [:lower, :upper]),
        key: key_var,
        head: {op, {transform_var, value_var}},
        body:
          quote do
            case unquote(transform_var) do
              :lower ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    field_lower_expr,
                    context
                  )
                )

              :upper ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    field_upper_expr,
                    context
                  )
                )
            end
          end
      },
      %Blueprint{
        guard: nil,
        key: key_var,
        head: {:not, {op, value_var}},
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
        head: {op, value_var},
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
  def expr_for(op, q_var, _, {key_var, nil}) when op in [:==, :eq] do
    quote do
      is_nil(field(unquote(q_var), ^unquote(key_var)))
    end
  end

  def expr_for(op, q_var, _, {key_var, nil}) when op in [:!=, :ne] do
    quote do
      not is_nil(field(unquote(q_var), ^unquote(key_var)))
    end
  end

  def expr_for(op, q_var, meta, {key_var, value_var})
      when op in @comparison_operators and meta in [:lower, :upper] do
    content =
      if meta === :lower do
        "lower(?)"
      else
        "upper(?)"
      end

    fragment_expr =
      quote do
        fragment(unquote(content), field(unquote(q_var), ^unquote(key_var)))
      end

    quote do
      unquote(special_form_ast(fragment_expr, op, pinned_ast(value_var)))
    end
  end

  def expr_for(:like, q_var, _, {key_var, value_var}) do
    quote do
      like(field(unquote(q_var), ^unquote(key_var)), ^"%#{unquote(value_var)}%")
    end
  end

  def expr_for(:ilike, q_var, _, {key_var, value_var}) do
    quote do
      ilike(field(unquote(q_var), ^unquote(key_var)), ^"%#{unquote(value_var)}%")
    end
  end

  def expr_for(op, q_var, _, {key_var, value_var}) do
    field_expr =
      quote do
        field(unquote(q_var), ^unquote(key_var))
      end

    quote do
      unquote(special_form_ast(field_expr, op, pinned_ast(value_var)))
    end
  end

  defp special_form_ast(left, :in, right) do
    quote do
      unquote(left) in unquote(right)
    end
  end

  defp special_form_ast(left, op, right) when op in [:==, :eq] do
    quote do
      unquote(left) == unquote(right)
    end
  end

  defp special_form_ast(left, op, right) when op in [:!=, :ne] do
    quote do
      unquote(left) != unquote(right)
    end
  end

  defp special_form_ast(left, op, right) when op in [:>, :gt] do
    quote do
      unquote(left) > unquote(right)
    end
  end

  defp special_form_ast(left, op, right) when op in [:<, :lt] do
    quote do
      unquote(left) < unquote(right)
    end
  end

  defp special_form_ast(left, op, right) when op in [:>=, :gte] do
    quote do
      unquote(left) >= unquote(right)
    end
  end

  defp special_form_ast(left, op, right) when op in [:<=, :lte] do
    quote do
      unquote(left) <= unquote(right)
    end
  end

  defp pinned_ast(var) do
    quote do
      ^unquote(var)
    end
  end
end
