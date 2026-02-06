defmodule EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr do
  @moduledoc false

  use EctoShorts.Compiler,
    specs: EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs,
    max_positional_bindings: 10
end
