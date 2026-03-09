defmodule EctoShorts.Dynamics.Postgres.CommonExpr do
  alias EctoShorts.Dynamics.Postgres.CommonExprBuilder

  @keys CommonExprBuilder.keys()

  use EctoShorts.Compiler,
    modules: [
      [
        builder: CommonExprBuilder,
        module: __MODULE__.Compiled.Core,
        keys: [:ids, :before, :after, :until, :since, :exists],
        positions: 10
      ],
      [
        builder: CommonExprBuilder,
        module: __MODULE__.Compiled.Temporal,
        keys: [:start_date, :end_date, :since_date, :until_date],
        positions: 10
      ]
    ]

  def keys, do: @keys

  def dynamic_expr({kind, _} = binding_selector, key, value, _opts) when kind in [:as, :at] do
    dynamic_expr(binding_selector, key, value)
  end
end
