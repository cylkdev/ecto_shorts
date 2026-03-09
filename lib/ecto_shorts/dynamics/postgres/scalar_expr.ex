defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  alias EctoShorts.Dynamics.Postgres.ScalarExprBuilder

  @keys ScalarExprBuilder.keys()

  use EctoShorts.Compiler,
    modules: [
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled,
        positions: 10
      ]
    ]

  def keys, do: @keys

  def dynamic_expr(binding_selector, key, term, _opts) do
    case term do
      {op, value} when op in @keys ->
        __MODULE__.Compiled.dynamic_expr(binding_selector, key, {op, value})

      value ->
        __MODULE__.Compiled.dynamic_expr(binding_selector, key, {:==, value})
    end
  end
end
