defmodule EctoShorts.Compiler.ClauseSpecProvider do
  @moduledoc false

  alias EctoShorts.Compiler.ClauseSpec

  @callback clause_specs(
              context :: module(),
              binding_head_ast :: Macro.t(),
              target_binding_var :: Macro.t(),
              binding_body_asts :: [Macro.t()]
            ) :: [ClauseSpec.t() | map() | keyword()]
end
