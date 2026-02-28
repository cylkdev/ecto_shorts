defmodule EctoShorts.Compiler.UsingTest do
  use ExUnit.Case

  alias EctoShorts.Compiler
  alias EctoShorts.Compiler.AST

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  defp compile_compiled_module!(compiler_opts \\ [max_query_bindings: 1]) do
    unique = System.unique_integer([:positive])

    specs_module = Module.concat([__MODULE__, :"TmpSpecs#{unique}"])
    compiled_module = Module.concat([__MODULE__, :"TmpCompiled#{unique}"])
    opts = Keyword.put(compiler_opts, :specs, specs_module)

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

          use unquote(Compiler), unquote(opts)
        end
      end

    Code.compile_quoted(quoted)
    compiled_module
  end

  test "use Compiler defines apply_dynamic_expr/3 in the caller module" do
    compiled_module = compile_compiled_module!()
    assert {:module, _} = Code.ensure_compiled(compiled_module)
  end

  test "generated apply_dynamic_expr/3 clauses return the expected dynamic" do
    compiled_module = compile_compiled_module!()

    expected = dynamic([q], field(q, ^:id) == ^1)

    actual =
      apply(compiled_module, :apply_dynamic_expr, [
        {:as, nil},
        :id,
        {:==, 1}
      ])

    assert_dynamic(expected, actual)
  end

  test "max_query_bindings limits generated positional heads" do
    compiled_module = compile_compiled_module!()

    assert_raise FunctionClauseError, fn ->
      apply(compiled_module, :apply_dynamic_expr, [
        {:at, 2},
        :id,
        {:==, 1}
      ])
    end
  end

  test "__mix_recompile__?/0 marks stale when config max changes for config-driven modules" do
    previous_compiler_config = Application.get_env(:ecto_shorts, :compiler)
    on_exit(fn -> Application.put_env(:ecto_shorts, :compiler, previous_compiler_config) end)

    Application.put_env(:ecto_shorts, :max_query_bindings, 10)
    compiled_module = compile_compiled_module!([])

    refute apply(compiled_module, :__mix_recompile__?, [])

    Application.put_env(:ecto_shorts, :max_query_bindings, 11)

    assert apply(compiled_module, :__mix_recompile__?, [])
  end

  test "__mix_recompile__?/0 ignores config max changes when module overrides max" do
    previous_compiler_config = Application.get_env(:ecto_shorts, :compiler)
    on_exit(fn -> Application.put_env(:ecto_shorts, :compiler, previous_compiler_config) end)

    Application.put_env(:ecto_shorts, :max_query_bindings, 10)
    compiled_module = compile_compiled_module!(max_query_bindings: 1)

    Application.put_env(:ecto_shorts, :max_query_bindings, 11)

    refute apply(compiled_module, :__mix_recompile__?, [])
  end
end
