defmodule EctoShorts.Compiler.ClauseBuilder do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Builds quoted `apply_dynamic_expr/3` function clauses from validated clause specs.

  Use this module when you need to generate the AST for a complete function
  clause from a `ClauseSpec` struct. The builder takes the spec's fields and
  produces a quoted `def` statement that can be injected into a module at
  compile time.

  ## How it works

  The clause builder follows this flow:

  1. **Receive a clause spec** - accepts a `ClauseSpec` struct with all
     required fields.
  2. **Extract AST components** - pulls out `:binding_head`, `:key`, `:head`,
     `:body`, and optional `:guard` fields.
  3. **Build function clause** - wraps the components in a `def` statement
     with proper pattern matching.
  4. **Return quoted AST** - produces the complete function clause as quoted
     code.

  ## Clause generation examples

  ### Input: Simple equality spec

      spec = ClauseSpec.new(%{
        binding_head: {:as, nil},
        key: Macro.var(:key, nil),
        head: quote(do: {:==, val}),
        body: quote(do: Ecto.Query.dynamic([r], field(r, ^key) == ^val))
      })

      ClauseBuilder.clause_ast(spec)

  ### Output: Generated clause

      def apply_dynamic_expr({:as, nil}, key, {:==, val}) do
        Ecto.Query.dynamic([r], field(r, ^key) == ^val)
      end

  ### Input: Spec with guard

      spec = ClauseSpec.new(%{
        binding_head: {:as, nil},
        key: Macro.var(:key, nil),
        head: quote(do: {:>, val}),
        body: quote(do: Ecto.Query.dynamic([r], field(r, ^key) > ^val)),
        guard: quote(do: is_number(val))
      })

      ClauseBuilder.clause_ast(spec)

  ### Output: Clause with guard

      def apply_dynamic_expr({:as, nil}, key, {:>, val}) when is_number(val) do
        Ecto.Query.dynamic([r], field(r, ^key) > ^val)
      end

  ## Guard clauses

  When a spec includes a `:guard` field, the builder generates a function
  clause with a `when` guard:

      # Spec with guard
      ClauseSpec.new(%{
        binding_head: {:at, 1},
        key: Macro.var(:key, nil),
        head: quote(do: {:in, vals}),
        body: quote(do: Ecto.Query.dynamic([r], field(r, ^key) in ^vals)),
        guard: quote(do: is_list(vals))
      })

  Generates:

      def apply_dynamic_expr({:at, 1}, key, {:in, vals}) when is_list(vals) do
        Ecto.Query.dynamic([r], field(r, ^key) in ^vals)
      end

  Guards are useful for:

  * **Type validation** - ensure values are the correct type.
  * **Range checks** - verify values are within bounds.
  * **Preventing errors** - skip clauses that would fail at runtime.

  ## Multiple clause generation

  The builder is typically called multiple times to generate many clauses:

      specs = [
        ClauseSpec.new(%{
          binding_head: {:as, nil},
          key: Macro.var(:key, nil),
          head: quote(do: {:==, val}),
          body: quote(do: dynamic([r], field(r, ^key) == ^val))
        }),
        ClauseSpec.new(%{
          binding_head: {:as, nil},
          key: Macro.var(:key, nil),
          head: quote(do: {:>, val}),
          body: quote(do: dynamic([r], field(r, ^key) > ^val))
        }),
        ClauseSpec.new(%{
          binding_head: {:as, nil},
          key: Macro.var(:key, nil),
          head: quote(do: {:<, val}),
          body: quote(do: dynamic([r], field(r, ^key) < ^val))
        })
      ]

      clauses = Enum.map(specs, &ClauseBuilder.clause_ast/1)

  This generates three function clauses that handle different operators.

  ## Integration with compiler

  The clause builder is used by `EctoShorts.Compiler` to generate all
  `apply_dynamic_expr/3` clauses at compile time:

      # Inside EctoShorts.Compiler
      specs = provider.clause_specs(context, binding_head, target_binding, binding_bodies)
      clauses = Enum.map(specs, &ClauseBuilder.clause_ast/1)

      quote do
        defmodule Compiled do
          unquote_splicing(clauses)
        end
      end

  ## Troubleshooting

  **Problem:** Generated clause does not compile.

  **Solution:** Check that the spec's `:body` field contains valid Ecto query
  AST. Use `Macro.to_string/1` to inspect the generated clause:

      spec |> ClauseBuilder.clause_ast() |> Macro.to_string() |> IO.puts()

  **Problem:** Clause does not match at runtime.

  **Solution:** Verify that the `:head` pattern matches the expression
  structure you are passing. The pattern must match exactly.

  **Problem:** Guard clause raises error.

  **Solution:** Ensure the guard expression uses only guard-safe functions.
  Not all Elixir functions can be used in guards.

  See also `EctoShorts.Compiler.ClauseSpec`, `EctoShorts.Compiler`, and
  `EctoShorts.Compiler.AST`.
  """

  alias EctoShorts.Compiler.ClauseSpec

  @doc """
  Builds a quoted `apply_dynamic_expr/3` clause from a clause spec.
  """
  @spec clause_ast(ClauseSpec.t() | map() | keyword()) :: Macro.t()
  def clause_ast(attrs) do
    spec = ClauseSpec.new(attrs)
    quote_def(spec.binding_head, spec.key, spec.head, spec.body, spec.guard)
  end

  defp quote_def(binding_head_ast, key_ast, head_ast, body_ast, nil) do
    quote do
      def apply_dynamic_expr(unquote(binding_head_ast), unquote(key_ast), unquote(head_ast)) do
        unquote(body_ast)
      end
    end
  end

  defp quote_def(binding_head_ast, key_ast, head_ast, body_ast, guard_ast) do
    quote do
      def apply_dynamic_expr(unquote(binding_head_ast), unquote(key_ast), unquote(head_ast))
          when unquote(guard_ast) do
        unquote(body_ast)
      end
    end
  end
end
