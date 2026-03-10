defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  alias EctoShorts.Dynamics.Postgres.ScalarExprBuilder

  use EctoShorts.Compiler,
    modules: [
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.Comparison,
        keys: [:comparison],
        positions: 10
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.Membership,
        keys: [:membership],
        positions: 10
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.String,
        keys: [:string],
        positions: 10
      ]
    ]

  @keys ScalarExprBuilder.keys()

  def keys, do: @keys

  def dynamic_expr(selected_binding, key, term, negated, _opts) do
    routed_term = normalize_scalar_term(term)

    {module, routed_term} = route_scalar_term(routed_term)

    module.dynamic_expr(selected_binding, key, routed_term, negated)
  end

  defp normalize_scalar_term({_op, _value} = tuple_term), do: tuple_term
  defp normalize_scalar_term(value), do: {:==, value}

  defp route_scalar_term({:in, _value} = term) do
    {__MODULE__.Compiled.Membership, term}
  end

  defp route_scalar_term({op, value} = term) when op in [:==, :eq, :!=, :ne] and is_list(value) do
    {__MODULE__.Compiled.Membership, term}
  end

  defp route_scalar_term({op, _value} = term) when op in [:like, :ilike] do
    {__MODULE__.Compiled.String, term}
  end

  defp route_scalar_term({op, _value} = term)
       when op in [:==, :eq, :!=, :ne, :>, :>=, :<, :<=, :gt, :gte, :lt, :lte] do
    {__MODULE__.Compiled.Comparison, term}
  end

  defp route_scalar_term(term) do
    {__MODULE__.Compiled.Comparison, term}
  end
end
