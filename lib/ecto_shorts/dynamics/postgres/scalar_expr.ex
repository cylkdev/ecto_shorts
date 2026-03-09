defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  alias EctoShorts.Dynamics.Postgres.ScalarExprBuilder

  use EctoShorts.Compiler,
    modules: [
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled,
        positions: 10
      ]
    ]

  @keys ScalarExprBuilder.keys()

  def keys, do: @keys

  def dynamic_expr(selected_binding, key, term, _opts) do
    case term do
      {op, value} when op in @keys ->
        __MODULE__.Compiled.dynamic_expr(selected_binding, key, {op, value})

      value ->
        __MODULE__.Compiled.dynamic_expr(selected_binding, key, {:==, value})
    end
  end
end
