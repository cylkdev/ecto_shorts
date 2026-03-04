defmodule EctoShorts.Compiler.UsingTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Compiler
  alias EctoShorts.Compiler.AST

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  defp compile_compiled_module!(compiler_opts \\ [max_binding_positions: 1]) do
    unique = System.unique_integer([:positive])

    specs_module = Module.concat([__MODULE__, :"TmpSpecs#{unique}"])
    compiled_module = Module.concat([__MODULE__, :"TmpCompiled#{unique}"])
    opts = Keyword.put(compiler_opts, :specs, [specs_module])

    quoted =
      quote do
        defmodule unquote(specs_module) do
          @moduledoc false

          def clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
            key_var = Macro.var(:key, context)
            v_var = Macro.var(:v, context)

            expr_ast =
              quote do
                # credo:disable-for-next-line BlitzCredoChecks.StrictComparison
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

  test "use Compiler defines compose/3 in the caller module" do
    compiled_module = compile_compiled_module!()
    assert {:module, _} = Code.ensure_compiled(compiled_module)
  end

  test "generated compose/3 clauses return the expected dynamic" do
    compiled_module = compile_compiled_module!()

    expected = dynamic([q], field(q, ^:id) == ^1)

    actual = compiled_module.compose({:as, nil}, :id, {:==, 1})

    assert_dynamic(expected, actual)
  end

  test "max_binding_positions limits generated positional heads" do
    compiled_module = compile_compiled_module!()

    assert is_nil(compiled_module.compose({:at, 2}, :id, {:==, 1}))
  end

  test "config_stale?/1 returns true when current max differs from compile-time max" do
    compiled_module = compile_compiled_module!([])
    compile_time_max = EctoShorts.Config.max_binding_positions()

    refute compiled_module.config_stale?(compile_time_max)
    assert compiled_module.config_stale?(compile_time_max + 1)
  end

  test "config_stale?/1 always matches compile-time max when module overrides max" do
    explicit_max = 1
    compiled_module = compile_compiled_module!(max_binding_positions: explicit_max)

    refute compiled_module.config_stale?(explicit_max)
    assert compiled_module.config_stale?(explicit_max + 1)
  end
end
