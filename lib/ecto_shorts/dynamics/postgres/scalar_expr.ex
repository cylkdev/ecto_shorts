defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  @moduledoc false

  use EctoShorts.Dynamics.Compiler,
    specs: EctoShorts.Dynamics.Postgres.ScalarExpr.Specs,
    max_positional_bindings: 10
end
