defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseBuilder do
  @moduledoc """
  Builds quoted `dynamic_field_expr/3` clauses from clause specs.

  This module builds function clauses by describing them as data (maps).
  The output is quoted AST that you inject into a module.

  ## How It Works

  `clause_ast/1` takes a spec map and returns a quoted `def dynamic_field_expr/3`
  clause.

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

  @doc "Same as `clause_ast/1`, but raises on error."
  def clause_ast!(spec) do
    case clause_ast(spec) do
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
  def clause_ast(spec_or_attrs) do
    with {:ok, spec} <- to_clause_spec(spec_or_attrs) do
      {:ok,
       quoted_def(
         spec.binding_head,
         spec.key,
         spec.head,
         spec.body,
         spec.guard
       )}
    end
  end

  defp to_clause_spec(%ClauseSpec{} = spec) do
    {:ok, spec}
  end

  defp to_clause_spec(attrs) do
    ClauseSpec.new(attrs)
  end

  defp quoted_def(binding_head_ast, key_ast, head_ast, body_ast, guard_ast) do
    if is_nil(guard_ast) do
      quote do
        def dynamic_field_expr(unquote(binding_head_ast), unquote(key_ast), unquote(head_ast)) do
          unquote(body_ast)
        end
      end
    else
      quote do
        def dynamic_field_expr(unquote(binding_head_ast), unquote(key_ast), unquote(head_ast))
            when unquote(guard_ast) do
          unquote(body_ast)
        end
      end
    end
  end
end
