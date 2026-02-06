defmodule EctoShorts.QueryBuilder.Dynamics.Postgres.CommonExpr do
  @moduledoc false

  use EctoShorts.QueryBuilder.Dynamics.Compiler,
    specs: EctoShorts.QueryBuilder.Dynamics.Postgres.CommonExpr.Specs,
    max_positional_bindings: 10
end
