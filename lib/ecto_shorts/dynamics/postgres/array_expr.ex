defmodule EctoShorts.Dynamics.Postgres.ArrayExpr do
  @moduledoc false

  use EctoShorts.Dynamics.Compiler,
    specs: EctoShorts.Dynamics.Postgres.ArrayExpr.Specs,
    max_positional_bindings: 10
end
