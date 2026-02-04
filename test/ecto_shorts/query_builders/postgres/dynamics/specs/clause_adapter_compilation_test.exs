defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseAdapterCompilationTest do
  use ExUnit.Case, async: true

  alias EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.AST
  alias EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseAdapter
  alias EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.Emitters.DynamicFieldExpr

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  defp compile_adapter_module!() do
    unique = System.unique_integer([:positive])

    adapter =
      Module.concat([
        __MODULE__,
        :"TmpAdapter#{unique}"
      ])

    quoted =
      quote do
        defmodule unquote(adapter) do
          @moduledoc false

          use unquote(ClauseAdapter)

          def emitter_module, do: unquote(DynamicFieldExpr)

          def options, do: [max_positional_bindings: 1]

          def clause_specs(kind, context, binding_head_ast, target_binding_var, binding_body_asts) do
            key_var = Macro.var(:key, context)
            v_var = Macro.var(:v, context)

            expr_ast =
              quote do
                field(unquote(target_binding_var), ^unquote(key_var)) == ^unquote(v_var)
              end

            [
              %{
                kind: kind,
                binding_head: binding_head_ast,
                key: key_var,
                head: quote(do: {:==, unquote(v_var)}),
                body: unquote(AST).dynamic_ast(binding_body_asts, expr_ast)
              }
            ]
          end
        end
      end

    Code.compile_quoted(quoted)
    adapter
  end

  test "use ClauseAdapter compiles Adapter.Compiled.<Kind> modules" do
    adapter = compile_adapter_module!()

    common_mod = Module.concat([adapter, Compiled, Common])
    scalar_mod = Module.concat([adapter, Compiled, Scalar])
    array_mod = Module.concat([adapter, Compiled, Array])

    assert {:module, _} = Code.ensure_compiled(common_mod)
    assert {:module, _} = Code.ensure_compiled(scalar_mod)
    assert {:module, _} = Code.ensure_compiled(array_mod)
  end

  test "compiled module includes generated dynamic_field_expr/3 clauses" do
    adapter = compile_adapter_module!()
    compiled_mod = Module.concat([adapter, Compiled, Common])

    expected = dynamic([q], field(q, ^:id) == ^1)

    actual =
      apply(compiled_mod, :dynamic_field_expr, [
        {:as, nil},
        :id,
        {:==, 1}
      ])

    assert_dynamic(expected, actual)
  end

  test "max_positional_bindings limits generated positional heads" do
    adapter = compile_adapter_module!()
    compiled_mod = Module.concat([adapter, Compiled, Common])

    assert_raise FunctionClauseError, fn ->
      apply(compiled_mod, :dynamic_field_expr, [
        {:at, 2},
        :id,
        {:==, 1}
      ])
    end
  end
end
