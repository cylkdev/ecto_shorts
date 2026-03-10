defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  alias EctoShorts.Dynamics.Postgres.ScalarExprBuilder

  @max_binding_positions 10

  use EctoShorts.Compiler,
    modules: [
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.Comparison,
        directives: [:comparison],
        positions: @max_binding_positions
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.Membership,
        directives: [:membership],
        positions: @max_binding_positions
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.StringUpperLower,
        directives: [:string_transform],
        positions: @max_binding_positions
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.String,
        directives: [:string],
        positions: @max_binding_positions
      ]
    ]

  @directives ScalarExprBuilder.directives()
  @comparison_directives ScalarExprBuilder.directives(:comparison)
  @equality_directives ScalarExprBuilder.directives(:equality)
  @string_directives ScalarExprBuilder.directives(:string)

  def directives, do: @directives

  def dynamic_expr(selected_binding, key, negated, term, _opts) do
    {op, term} = normalize_input(term)
    module = resolver(op, term)
    module.dynamic_expr(selected_binding, key, negated, {op, term})
  end

  defp normalize_input({_op, _value} = term), do: term
  defp normalize_input(value), do: {:==, value}

  defp resolver(:in, _) do
    __MODULE__.Compiled.Membership
  end

  defp resolver(op, value) when op in @equality_directives and is_list(value) do
    __MODULE__.Compiled.Membership
  end

  defp resolver(op, {transform, _})
       when op in @comparison_directives and transform in [:lower, :upper] do
    __MODULE__.Compiled.StringUpperLower
  end

  defp resolver(op, {transform, _}) when op in @string_directives do
    if transform in [:lower, :upper] do
      __MODULE__.Compiled.StringUpperLower
    else
      __MODULE__.Compiled.String
    end
  end

  defp resolver(op, _value) when op in @string_directives do
    __MODULE__.Compiled.String
  end

  defp resolver(_op, _term) do
    __MODULE__.Compiled.Comparison
  end
end
