defmodule EctoShorts.Compiler.ClauseBuilderTest do
  use ExUnit.Case, async: true

  alias Ecto.Query
  alias EctoShorts.Compiler.ClauseBuilder
  alias EctoShorts.Compiler.ClauseSpec

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  defp compile_clause_module!(clause_ast) do
    # Generate a unique module name to avoid conflicts across async tests.
    module = Module.concat([__MODULE__, :"Tmp#{System.unique_integer([:positive])}"])

    quoted =
      quote do
        # The generated clause uses `field/2` and `fragment/1`, so import Ecto.Query.
        defmodule unquote(module) do
          import Ecto.Query
          require Ecto.Query

          unquote(clause_ast)
        end
      end

    # Compile the quoted module into the VM and return its module name so the
    # test can call the generated function via `apply/3`.
    Code.compile_quoted(quoted)
    module
  end

  test "clause_ast/1 builds a scalar equality clause and it compiles" do
    key_var = Macro.var(:key, nil)
    v_var = Macro.var(:v, nil)

    spec =
      ClauseSpec.new(%{
        binding_head: quote(do: {:as, nil}),
        key: key_var,
        head: quote(do: {:==, unquote(v_var)}),
        body:
          quote do
            unquote(Query).dynamic([q], field(q, ^unquote(key_var)) == ^unquote(v_var))
          end
      })

    clause_ast = ClauseBuilder.clause_ast(spec)

    module = compile_clause_module!(clause_ast)

    # Build the "literal" expected expression using the Ecto DSL.
    #
    # `assert_dynamic/2` compares dynamics via `Macro.to_string/1`, which gives a
    # stable AST-level assertion without needing a repo connection.
    expected_dyn = dynamic([q], field(q, ^:title) == ^"hello")

    # Call the generated function clause and compare to the expected dynamic.
    actual_dyn = module.apply_dynamic_expr({:as, nil}, :title, {:==, "hello"})
    assert_dynamic(expected_dyn, actual_dyn)
  end

  test "clause_ast/1 builds a clause with a guard and it compiles" do
    # ClauseSpec supports an optional `:guard` AST.
    key_var = Macro.var(:key, nil)
    values_var = Macro.var(:values, nil)

    spec =
      ClauseSpec.new(%{
        binding_head: quote(do: {:as, nil}),
        key: key_var,
        head: quote(do: {:==, unquote(values_var)}),
        guard: quote(do: is_list(unquote(values_var))),
        body:
          quote do
            unquote(Query).dynamic([q], field(q, ^unquote(key_var)) in ^unquote(values_var))
          end
      })

    clause_ast = ClauseBuilder.clause_ast(spec)

    # Sanity check the emitted source includes the guard.
    clause_source = Macro.to_string(clause_ast)
    assert String.contains?(clause_source, "when is_list(values)")

    module = compile_clause_module!(clause_ast)
    expected_dyn = dynamic([q], field(q, ^:id) in ^[1, 2])
    actual_dyn = module.apply_dynamic_expr({:as, nil}, :id, {:==, [1, 2]})
    assert_dynamic(expected_dyn, actual_dyn)
  end

  test "clause_ast/1 raises for missing keys" do
    assert_raise ArgumentError, fn -> ClauseBuilder.clause_ast(%{key: :id}) end
  end
end
