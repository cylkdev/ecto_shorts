defmodule EctoShorts.QueryBuilder.Dynamics.Compiler.UsingTest do
  use ExUnit.Case, async: true

  alias EctoShorts.QueryBuilder.Dynamics.Compiler
  alias EctoShorts.QueryBuilder.Dynamics.Compiler.AST

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  defp compile_compiled_module!() do
    unique = System.unique_integer([:positive])

    specs_module = Module.concat([__MODULE__, :"TmpSpecs#{unique}"])
    compiled_module = Module.concat([__MODULE__, :"TmpCompiled#{unique}"])

    quoted =
      quote do
        defmodule unquote(specs_module) do
          @moduledoc false

          def clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
            key_var = Macro.var(:key, context)
            v_var = Macro.var(:v, context)

            expr_ast =
              quote do
                field(unquote(target_binding_var), ^unquote(key_var)) == ^unquote(v_var)
              end

            [
              %{
                binding_head: binding_head_ast,
                key: key_var,
                head: quote(do: {:==, unquote(v_var)}),
                body: unquote(AST).dynamic_ast(binding_body_asts, expr_ast)
              }
            ]
          end
        end

        defmodule unquote(compiled_module) do
          @moduledoc false

          use unquote(Compiler),
            specs: unquote(specs_module),
            max_positional_bindings: 1
        end
      end

    Code.compile_quoted(quoted)
    compiled_module
  end

  test "use Compiler defines dynamic_field_expr/3 in the caller module" do
    compiled_module = compile_compiled_module!()
    assert {:module, _} = Code.ensure_compiled(compiled_module)
  end

  test "generated dynamic_field_expr/3 clauses return the expected dynamic" do
    compiled_module = compile_compiled_module!()

    expected = dynamic([q], field(q, ^:id) == ^1)

    actual =
      apply(compiled_module, :dynamic_field_expr, [
        {:as, nil},
        :id,
        {:==, 1}
      ])

    assert_dynamic(expected, actual)
  end

  test "max_positional_bindings limits generated positional heads" do
    compiled_module = compile_compiled_module!()

    assert_raise FunctionClauseError, fn ->
      apply(compiled_module, :dynamic_field_expr, [
        {:at, 2},
        :id,
        {:==, 1}
      ])
    end
  end
end
