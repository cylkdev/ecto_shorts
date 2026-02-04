defmodule EctoShorts.QueryBuilder.Dynamics.Expression.ClauseAdapter do
  @moduledoc """
  Defines a repo-adapter contract for compiling `dynamic_field_expr/3` clauses.

  A clause adapter is a module (for example: `MyApp.Postgres`) that describes
  clause specs as data and chooses how those specs get emitted.

  The adapter is compiled first. After compilation, `ClauseBuilder` compiles a
  sibling module named `MyApp.Postgres.Compiled.*` via `Module.create/3`.
  """

  @typedoc "A clause kind tag used to group specs (for example: `:scalar`)."
  @type kind() :: atom()

  @typedoc "Adapter-level options passed to binding helpers."
  @type options() :: keyword()

  @doc """
  Returns the emitter module used to turn specs into quoted `def` clauses.
  """
  @callback emitter_module() :: module()

  @doc """
  Returns adapter options used when generating binding head patterns.

  Options must include `:max_positional_bindings`.
  """
  @callback options() :: options()

  @doc """
  Returns clause specs for the given `kind` and binding pattern.
  """
  @callback clause_specs(
              kind :: kind(),
              context :: term(),
              binding_head_ast :: Macro.t(),
              target_binding_var :: Macro.t(),
              binding_body_asts :: [Macro.t()]
            ) ::
              [
                EctoShorts.QueryBuilder.Dynamics.Expression.ClauseSpec.t()
                | map()
                | keyword()
              ]

  @doc """
  Sets up the current module as a clause adapter.

  This registers an `@after_compile` hook that compiles `#{inspect(__MODULE__)}.Compiled`.
  """
  defmacro __using__(_opts) do
    quote do
      @behaviour EctoShorts.QueryBuilder.Dynamics.Expression.ClauseAdapter
      @after_compile {EctoShorts.QueryBuilder.Dynamics.Expression.ClauseBuilder,
                      :__after_compile__}
    end
  end
end
