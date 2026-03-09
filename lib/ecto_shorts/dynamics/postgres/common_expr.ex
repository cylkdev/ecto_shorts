defmodule EctoShorts.Dynamics.Postgres.CommonExpr do
  alias EctoShorts.Dynamics.Postgres.CommonExpr.Spec

  @keys Spec.keys()
  @compiled_module __MODULE__.Compiled

  use EctoShorts.Compiler,
    modules: [
      [
        builder: Spec,
        module: @compiled_module,
        opts: [partitions: 2, positions: 10]
      ]
    ]

  def keys, do: @keys

  def dynamic_expr({kind, _} = binding_selector, key, value, _opts) when kind in [:as, :at] do
    dynamic_expr(@compiled_module, binding_selector, key, value)
  end
end
