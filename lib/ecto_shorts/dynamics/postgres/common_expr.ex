defmodule EctoShorts.Dynamics.Postgres.CommonExpr do
  alias EctoShorts.Dynamics.Postgres.CommonExprBuilder

  @core_keys [:ids, :before, :after, :until, :since, :exists]
  @temporal_keys [:start_date, :end_date, :since_date, :until_date]

  use EctoShorts.Compiler,
    modules: [
      [
        builder: CommonExprBuilder,
        module: __MODULE__.Compiled.Core,
        keys: @core_keys,
        positions: 10
      ],
      [
        builder: CommonExprBuilder,
        module: __MODULE__.Compiled.Temporal,
        keys: @temporal_keys,
        positions: 10
      ]
    ]

  @keys CommonExprBuilder.keys()

  def keys, do: @keys

  def dynamic_expr(selected_binding, key, term, negated, _opts) do
    cond do
      key in @core_keys ->
        __MODULE__.Compiled.Core.dynamic_expr(selected_binding, key, term, negated)

      key in @temporal_keys ->
        __MODULE__.Compiled.Temporal.dynamic_expr(selected_binding, key, term, negated)

      true ->
        nil
    end
  end
end
