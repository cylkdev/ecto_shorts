defmodule EctoShorts.QueryBuilder.Dynamics.Postgres.ScalarExpr do
  @moduledoc false

  use EctoShorts.QueryBuilder.Dynamics.Compiler,
    specs: EctoShorts.QueryBuilder.Dynamics.Postgres.ScalarExpr.Specs,
    max_positional_bindings: 10
end
