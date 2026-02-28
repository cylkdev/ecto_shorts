defmodule EctoShorts.Compiler.ClauseSpecProvider do
  @moduledoc """
  Defines the behaviour for clause spec providers.

  A module implementing this behaviour must export `clause_specs/4`, which
  returns a list of clause spec maps (or `ClauseSpec` structs) for a given
  context, binding head AST, target binding variable, and binding body ASTs.
  The compiler calls this at compile time to generate `apply_dynamic_expr/3`
  function clauses.
  """

  alias EctoShorts.Compiler.ClauseSpec

  @callback clause_specs(
              context :: module(),
              binding_head_ast :: Macro.t(),
              target_binding_var :: Macro.t(),
              binding_body_asts :: [Macro.t()]
            ) :: [ClauseSpec.t() | map() | keyword()]
end
