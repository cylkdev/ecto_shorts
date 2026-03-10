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

  @operators @comparison_operators ++ @membership_operators ++ @string_operators

  @behaviour EctoShorts.Generator.ClauseSpec

  @impl true
  def keys, do: [nil]

  @impl true
  def specs_for(_, {bind_op, bind_to_var}, q_var, opts) do
    context = opts[:context]

    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)

    conditions =
      Enum.flat_map(@operators, fn
        :in ->
          [
            quote do
              {:not, {:in, unquote(value_var)}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    Helpers.negated_expr(expr_for(:in, q_var, nil, {key_var, value_var})),
                    context
                  )
                )

              {:in, unquote(value_var)} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    expr_for(:in, q_var, nil, {key_var, value_var}),
                    context
                  )
                )
            end
          ]

        string_op when string_op in @string_operators ->
          [
            quote do
              {:not, {unquote(string_op), unquote(value_var)}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    Helpers.negated_expr(expr_for(string_op, q_var, nil, {key_var, value_var})),
                    context
                  )
                )
            end,
            quote do
              {unquote(string_op), unquote(value_var)} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    expr_for(string_op, q_var, nil, {key_var, value_var}),
                    context
                  )
                )
            end
          ]

        op when op in [:==, :eq] ->
          [
            quote do
              {unquote(op), nil} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    expr_for(op, q_var, nil, {key_var, nil}),
                    context
                  )
                )
            end
          ]

        op ->
          [
            quote do
              {:not, {unquote(op), {:lower, unquote(value_var)}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    Helpers.negated_expr(expr_for(op, q_var, :lower, {key_var, value_var})),
                    context
                  )
                )
            end,
            quote do
              {unquote(op), {:lower, unquote(value_var)}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    expr_for(op, q_var, :lower, {key_var, value_var}),
                    context
                  )
                )
            end,
            quote do
              {:not, {unquote(op), {:upper, unquote(value_var)}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    Helpers.negated_expr(expr_for(op, q_var, :upper, {key_var, value_var})),
                    context
                  )
                )
            end,
            quote do
              {unquote(op), {:upper, unquote(value_var)}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    expr_for(op, q_var, :upper, {key_var, value_var}),
                    context
                  )
                )
            end,
            quote do
              {:not, {unquote(op), unquote(value_var)}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    Helpers.negated_expr(expr_for(op, q_var, nil, {key_var, value_var})),
                    context
                  )
                )
            end,
            quote do
              {unquote(op), unquote(value_var)} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    expr_for(op, q_var, nil, {key_var, value_var}),
                    context
                  )
                )
            end
          ]
      end)

    conditions = List.flatten(conditions)

    [
      %Blueprint{
        guard: nil,
        key: key_var,
        head: value_var,
        body:
          quote do
            case unquote(value_var) do
              unquote(conditions)
            end
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
