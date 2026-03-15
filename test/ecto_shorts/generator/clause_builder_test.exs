defmodule EctoShorts.Generator.ClauseBuilderTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Generator.Builder

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  defmodule EqualityBuilder do
    @behaviour EctoShorts.Generator.ClauseSpec

    alias Ecto.Query
    alias EctoShorts.Generator.Blueprint

    def operators, do: [:title]

    def specs_for(key, _selected_binding, q_var, opts) do
      context = opts[:context]
      negated_var = Macro.var(:_negated, context)
      value_var = Macro.var(:v, context)

      [
        %Blueprint{
          key: key,
          head: [negated_var, value_var],
          guard: nil,
          body:
            quote do
              unquote(Query).dynamic(
                [unquote(q_var)],
                field(unquote(q_var), :title) == ^unquote(value_var)
              )
            end
        }
      ]
    end
  end

  defmodule ListGuardBuilder do
    @behaviour EctoShorts.Generator.ClauseSpec

    alias Ecto.Query
    alias EctoShorts.Generator.Blueprint

    def operators, do: [:id]

    def specs_for(key, _selected_binding, q_var, opts) do
      context = opts[:context]
      negated_var = Macro.var(:_negated, context)
      values_var = Macro.var(:values, context)

      [
        %Blueprint{
          key: key,
          head: [negated_var, values_var],
          guard: quote(do: is_list(unquote(values_var))),
          body:
            quote do
              unquote(Query).dynamic(
                [unquote(q_var)],
                field(unquote(q_var), :id) in ^unquote(values_var)
              )
            end
        }
      ]
    end
  end

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
    Code.compile_quoted(quoted)
    module
  end

  test "positional_clause_asts/3 builds a scalar equality clause and it compiles" do
    clause_ast =
      EqualityBuilder
      |> Builder.positional_clause_asts(1, context: __MODULE__)
      |> List.first()

    module = compile_clause_module!(clause_ast)

    # Build the "literal" expected expression using the Ecto DSL.
    #
    # `assert_dynamic/2` compares dynamics via `Macro.to_string/1`, which gives a
    # stable AST-level assertion without needing a repo connection.
    expected_dyn = dynamic([q], field(q, ^:title) == ^"hello")

    # Call the generated function clause and compare to the expected dynamic.
    actual_dyn = module.dynamic_expr({:at, 1}, :title, nil, "hello")
    assert_dynamic(expected_dyn, actual_dyn)
  end

  test "positional_clause_asts/3 builds a clause with a guard and it compiles" do
    # Blueprint supports an optional `:guard` AST.
    clause_ast =
      ListGuardBuilder
      |> Builder.positional_clause_asts(1, context: __MODULE__)
      |> List.first()

    # Sanity check the emitted source includes the guard.
    clause_source = Macro.to_string(clause_ast)
    assert String.contains?(clause_source, "when is_list(values)")

    module = compile_clause_module!(clause_ast)
    expected_dyn = dynamic([q], field(q, ^:id) in ^[1, 2])
    actual_dyn = module.dynamic_expr({:at, 1}, :id, nil, [1, 2])
    assert_dynamic(expected_dyn, actual_dyn)
  end

  test "positional_clause_asts/3 raises for invalid positions" do
    assert_raise FunctionClauseError, fn ->
      Builder.positional_clause_asts(EqualityBuilder, 0, context: __MODULE__)
    end
  end
end
