defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseEmitter do
  @moduledoc """
  Defines the contract for building a clause AST from a clause spec.

  `ClauseBuilder` validates the spec and delegates the actual `def` generation
  to an emitter module.
  """

  @typedoc "The clause kind tag from `ClauseSpec`."
  @type kind() :: atom()

  @callback quoted_def(
              kind(),
              binding_head_ast :: Macro.t(),
              key_ast :: Macro.t(),
              head_ast :: Macro.t(),
              body_ast :: Macro.t(),
              guard_ast :: Macro.t() | nil
            ) :: Macro.t()
end

