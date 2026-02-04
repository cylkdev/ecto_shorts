defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseBuilder do
  @moduledoc """
  Builds quoted `dynamic_field_expr/3` clauses from clause specs.

  This module builds function clauses by describing them as data (maps).
  The output is quoted AST that you inject into a module.

  ClauseBuilder validates the spec shape and delegates the clause generation to
  an emitter module that implements `quoted_def/6`.

  ## How It Works

  `clause_ast/2` takes an emitter module and a spec (or spec attrs) and returns
  a quoted `def dynamic_field_expr/3` clause.

  The function head patterns come from the spec keys:

    * `:binding_head` - first argument pattern
    * `:key` - second argument pattern
    * `:head` - third argument pattern

  The function body comes from `:body`.

  The clause body uses the Ecto Query DSL:

    * `dynamic/2`
    * `field/2`
    * `fragment/1`

  Import `Ecto.Query` in the module that receives the generated clause.

  ClauseBuilder builds one clause at a time. To build many clauses, map over a
  list of specs and call `clause_ast/1` for each one.

  ## Examples

  Build a clause body clause:

      spec = %{
        kind: :clause,
        binding_head: quote(do: {:as, nil}),
        key: Macro.var(:key, nil),
        head: quote(do: {:==, v}),
        body: quote(do: Ecto.Query.dynamic([q], field(q, ^key) == ^v))
      }
      {:ok, clause_ast} = EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseBuilder.clause_ast(spec)
      Macro.to_string(clause_ast)

  ## Error Reasons

  `clause_ast/1` returns `{:error, reason}` for invalid specs:

    * `:missing_key` - a required key is missing (from `ClauseSpec.new/1`)
    * `:invalid_spec` - the input is not a valid clause spec (from `ClauseSpec.new/1`)

    spec = %{
      kind: :clause,
      binding_head: quote(do: {:as, nil}),
      key: Macro.var(:key, nil),
      head: quote(do: {:==, v})
    }
    EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseBuilder.clause_ast(spec)
    {:error, :missing_key}

  > NOTE: The generated clause calls `field/2` and `fragment/1`.
  > Import `Ecto.Query` in the module that receives the generated clause.
  """

  alias EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseSpec
  alias EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.Emitters.DynamicFieldExpr

  @doc false
  def __after_compile__(env, _bytecode) do
    adapter = env.module

    unless function_exported?(adapter, :options, 0) do
      raise ArgumentError, "#{inspect(adapter)} must export `options/0`"
    end

    unless function_exported?(adapter, :emitter_module, 0) do
      raise ArgumentError, "#{inspect(adapter)} must export `emitter_module/0`"
    end

    unless function_exported?(adapter, :clause_specs, 5) do
      raise ArgumentError, "#{inspect(adapter)} must export `clause_specs/5`"
    end

    opts = adapter.options()

    unless Keyword.has_key?(opts, :max_positional_bindings) do
      raise ArgumentError,
            "#{inspect(adapter)}.options/0 must include `:max_positional_bindings`"
    end

    emitter = adapter.emitter_module()

    case validate_emitter(emitter) do
      :ok -> :ok
      {:error, :invalid_emitter} -> raise ArgumentError, "Invalid emitter: #{inspect(emitter)}"
    end

    location = Macro.Env.location(env)

    for kind <- [:common, :scalar, :array] do
      compiled_module = compiled_module(adapter, kind)

      delete_module_if_loaded(compiled_module)

      clause_asts =
        clause_asts_for_kind(kind, adapter, compiled_module, emitter, opts)

      quoted =
        quote do
          import Ecto.Query
          require Ecto.Query

          unquote_splicing(clause_asts)
        end

      Module.create(compiled_module, quoted, location)
    end

    :ok
  end

  @doc "Same as `clause_ast/1`, but raises on error."
  def clause_ast!(spec), do: clause_ast!(DynamicFieldExpr, spec)

  @doc "Same as `clause_ast/2`, but raises on error."
  def clause_ast!(emitter, spec) do
    case clause_ast(emitter, spec) do
      {:ok, ast} ->
        ast

      {:error, reason} ->
        raise ArgumentError, "Failed to build clause: #{inspect(reason)}"
    end
  end

  @doc """
  Builds a quoted `dynamic_field_expr/3` clause from a clause spec.

  This function validates the spec and returns one quoted function clause.

  ## Return values

    * `{:ok, ast}` - quoted code for a single `def dynamic_field_expr/3` clause
    * `{:error, reason}` - an error tuple

  `ClauseSpec.new/1` runs first.

  ## Spec keys

  * `:binding_head` - AST for the binding selector head pattern
  * `:key` - AST for the second argument pattern
  * `:head` - AST for the third argument pattern (example: `{:==, v}`)
  * `:body` - AST returned by the clause body
  * `:kind` - tags the clause (example: `:clause`)

  ## Examples

    iex> spec = %{
    ...>   kind: :clause,
    ...>   binding_head: quote(do: {:as, nil}),
    ...>   key: Macro.var(:key, nil),
    ...>   head: quote(do: {:==, v}),
    ...>   body: quote(do: Ecto.Query.dynamic([q], field(q, ^key) == ^v))
    ...> }
    iex> match?(
    ...>   {:ok, _},
    ...>   EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseBuilder.clause_ast(spec)
    ...> )
    true

    iex> EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseBuilder.clause_ast(%{})
    {:error, :missing_key}
  """
  @spec clause_ast(ClauseSpec.t() | map() | keyword()) :: {:ok, Macro.t()} | {:error, term()}
  def clause_ast(spec), do: clause_ast(DynamicFieldExpr, spec)

  @doc """
  Builds a clause AST using the given emitter module.

  The emitter module must implement `quoted_def/6` (see `ClauseEmitter`).

  ## Error reasons

    * `:invalid_emitter` - the emitter does not export `quoted_def/6`
    * `:missing_key` - a required spec key is missing (from `ClauseSpec.new/1`)
    * `:invalid_spec` - the spec attrs are not valid (from `ClauseSpec.new/1`)
  """
  @spec clause_ast(module(), ClauseSpec.t() | map() | keyword()) ::
          {:ok, Macro.t()} | {:error, term()}
  def clause_ast(emitter, spec) when is_atom(emitter) do
    with :ok <- validate_emitter(emitter),
         {:ok, spec} <- ClauseSpec.new(spec) do
      {:ok,
       emitter.quoted_def(
         spec.kind,
         spec.binding_head,
         spec.key,
         spec.head,
         spec.body,
         spec.guard
       )}
    end
  end

  defp validate_emitter(emitter) do
    case Code.ensure_compiled(emitter) do
      {:module, _} ->
        if function_exported?(emitter, :quoted_def, 6) do
          :ok
        else
          {:error, :invalid_emitter}
        end

      {:error, _} ->
        {:error, :invalid_emitter}
    end
  end

  defp clause_asts_for_kind(kind, adapter, context, emitter, opts) do
    alias EctoShorts.QueryBuilder.BindingHelpers

    {target_binding_var, binding_patterns} =
      BindingHelpers.query_var_and_binding_heads(context, opts)

    for {binding_head_ast, binding_body_asts} <- binding_patterns,
        spec <-
          adapter.clause_specs(
            kind,
            context,
            binding_head_ast,
            target_binding_var,
            binding_body_asts
          ) do
      clause_ast!(emitter, spec)
    end
  end

  defp compiled_module(adapter, kind) do
    Module.concat([adapter, Compiled, kind_module_segment(kind)])
  end

  defp kind_module_segment(kind) when is_atom(kind) do
    kind
    |> Atom.to_string()
    |> Macro.camelize()
    |> String.to_atom()
  end

  defp delete_module_if_loaded(module) do
    case Code.ensure_loaded(module) do
      {:module, _} ->
        :code.purge(module)
        :code.delete(module)
        :ok

      {:error, _} ->
        :ok
    end
  end
end
