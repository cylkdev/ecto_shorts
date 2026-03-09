defmodule EctoShorts.Dynamics.Postgres.CommonExpr do
  alias EctoShorts.Dynamics.Postgres.CommonExprBuilder

  @core_keys [:ids, :before, :after, :until, :since, :exists]
  @temporal_keys [:start_date, :end_date, :since_date, :until_date]
  @keys CommonExprBuilder.keys()

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

  def keys, do: @keys

  def dynamic_expr(binding_selector, key, term, _opts) do
    case key do
      key when key in @core_keys ->
        __MODULE__.Compiled.Core.dynamic_expr(binding_selector, key, term)

      key when key in @temporal_keys ->
        __MODULE__.Compiled.Temporal.dynamic_expr(binding_selector, key, term)

      _ ->
        nil
    end
  end
end
