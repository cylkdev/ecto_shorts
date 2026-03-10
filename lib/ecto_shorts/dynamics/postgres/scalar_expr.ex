defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  alias EctoShorts.Dynamics.Postgres.ScalarExprBuilder

  @max_binding_positions 10

  use EctoShorts.Compiler,
    modules: [
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.Comparison,
        keys: [:comparison],
        positions: @max_binding_positions
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.Membership,
        keys: [:membership],
        positions: @max_binding_positions
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.StringUpperLower,
        keys: [:string_comparison],
        positions: @max_binding_positions
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.String,
        keys: [:string],
        positions: @max_binding_positions
      ]
    ]

  @keys ScalarExprBuilder.keys()

  def keys, do: @keys

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

  defp resolver(op, value) when op in [:==, :eq, :!=, :ne] and is_list(value) do
    __MODULE__.Compiled.Membership
  end

  defp resolver(op, {transform, _}) when op in [:like, :ilike] do
    if transform in [:lower, :upper] do
      __MODULE__.Compiled.StringUpperLower
    else
      __MODULE__.Compiled.String
    end
  end

  defp resolver(_) do
    __MODULE__.Compiled.Comparison
  end
end
