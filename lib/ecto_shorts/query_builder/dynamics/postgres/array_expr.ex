defmodule EctoShorts.QueryBuilder.Dynamics.Postgres.ArrayExpr do
  @moduledoc false

  use EctoShorts.QueryBuilder.Dynamics.Compiler,
    specs: EctoShorts.QueryBuilder.Dynamics.Postgres.ArrayExpr.Specs,
    max_positional_bindings: 10
end
