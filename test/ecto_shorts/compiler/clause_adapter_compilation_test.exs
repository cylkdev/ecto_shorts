defmodule EctoShorts.CompilerTest do
  use ExUnit.Case, async: false

  alias EctoShorts.Generator

  defp unique_module(name) do
    Module.concat([__MODULE__, :"#{name}#{System.unique_integer([:positive])}"])
  end

  defp builder_definition(module, key, label) do
    quote do
      defmodule unquote(module) do
        @behaviour EctoShorts.Generator.ClauseSpec
        @label unquote(label)

        def keys, do: [unquote(key)]

        def specs_for(spec_key, _binding_selector, _q_var, opts) do
          context = opts[:context]
          label = @label
          value_var = Macro.var(:value, context)

          [
            %EctoShorts.Generator.Blueprint{
              guard: nil,
              key: spec_key,
              head: value_var,
              body: quote(do: {unquote(label), unquote(value_var)})
            }
          ]
        end
      end
    end
  end

  defp compile_with_modules!(modules, builder_definitions \\ []) do
    caller_module = unique_module("Caller")
    escaped_modules = Macro.escape(modules)

    quoted =
      quote do
        unquote_splicing(builder_definitions)

        defmodule unquote(caller_module) do
          use EctoShorts.Compiler, modules: unquote(escaped_modules)
        end
      end

    Code.compile_quoted(quoted)
    caller_module
  end

  defp compile_invalid_use!(opts) do
    caller_module = unique_module("InvalidCaller")
    escaped_opts = Macro.escape(opts)

    quoted =
      quote do
        defmodule unquote(caller_module) do
          use EctoShorts.Compiler, unquote(escaped_opts)
        end
      end

    Code.compile_quoted(quoted)
  end

  test "use EctoShorts.Compiler with one generated module exposes dynamic_expr/3" do
    builder_module = unique_module("SingleBuilder")
    compiled_module = unique_module("SingleCompiled")

    caller_module =
      compile_with_modules!(
        [[builder: builder_module, module: compiled_module, modes: :named]],
        [builder_definition(builder_module, :id, :single)]
      )

    assert {:single, 1} = caller_module.dynamic_expr({:as, :post}, :id, 1)
    assert is_nil(caller_module.dynamic_expr({:as, :post}, :missing, 1))
  end

  test "multiple generated modules are compiled in one pass and dispatch in declaration order" do
    first_builder = unique_module("FirstBuilder")
    second_builder = unique_module("SecondBuilder")
    first_compiled = unique_module("FirstCompiled")
    second_compiled = unique_module("SecondCompiled")

    caller_module =
      compile_with_modules!(
        [
          [builder: first_builder, module: first_compiled, modes: :named],
          [builder: second_builder, module: second_compiled, modes: :named]
        ],
        [
          builder_definition(first_builder, :id, :first),
          builder_definition(second_builder, :id, :second)
        ]
      )

    assert {:first, 1} = caller_module.dynamic_expr({:as, :post}, :id, 1)

    assert String.starts_with?(
             List.to_string(:code.which(first_compiled)),
             Mix.Project.compile_path()
           )

    assert String.starts_with?(
             List.to_string(:code.which(second_compiled)),
             Mix.Project.compile_path()
           )
  end

  test "dispatcher returns nil when every generated module returns nil" do
    first_builder = unique_module("NilFirstBuilder")
    second_builder = unique_module("NilSecondBuilder")
    first_compiled = unique_module("NilFirstCompiled")
    second_compiled = unique_module("NilSecondCompiled")

    caller_module =
      compile_with_modules!(
        [
          [builder: first_builder, module: first_compiled, modes: :named],
          [builder: second_builder, module: second_compiled, modes: :named]
        ],
        [
          builder_definition(first_builder, :first_key, :first),
          builder_definition(second_builder, :second_key, :second)
        ]
      )

    assert is_nil(caller_module.dynamic_expr({:as, :post}, :missing, 1))
  end

  test "raises when :modules is missing" do
    assert_raise ArgumentError, ~r/expected :modules to be a non-empty list/, fn ->
      compile_invalid_use!([])
    end
  end

  test "raises when :modules is empty" do
    assert_raise ArgumentError, ~r/expected :modules to be a non-empty list/, fn ->
      compile_invalid_use!(modules: [])
    end
  end

  test "raises when a modules entry is missing :builder" do
    compiled_module = unique_module("MissingBuilderCompiled")

    assert_raise ArgumentError, ~r/missing :builder/, fn ->
      compile_invalid_use!(modules: [[module: compiled_module]])
    end
  end

  test "raises when a modules entry is missing :module" do
    builder_module = unique_module("MissingModuleBuilder")

    assert_raise ArgumentError, ~r/missing :module/, fn ->
      compile_invalid_use!(modules: [[builder: builder_module]])
    end
  end

  test "raises when generated modules are duplicated" do
    builder_one = unique_module("DuplicateModuleBuilderOne")
    builder_two = unique_module("DuplicateModuleBuilderTwo")
    compiled_module = unique_module("DuplicateCompiled")

    assert_raise ArgumentError, ~r/duplicate generated modules/, fn ->
      compile_with_modules!(
        [
          [builder: builder_one, module: compiled_module],
          [builder: builder_two, module: compiled_module]
        ],
        [
          builder_definition(builder_one, :first_key, :first),
          builder_definition(builder_two, :second_key, :second)
        ]
      )
    end
  end

  test "raises when resolved generated file paths collide" do
    builder_one = unique_module("DuplicatePathBuilderOne")
    builder_two = unique_module("DuplicatePathBuilderTwo")
    first_compiled = unique_module("DuplicatePathCompiledOne")
    second_compiled = unique_module("DuplicatePathCompiledTwo")
    params = %{path: "shared/path", filename: "compiled.ex"}

    assert_raise ArgumentError, ~r/duplicate generated file paths/, fn ->
      compile_with_modules!(
        [
          [builder: builder_one, module: first_compiled, params: params],
          [builder: builder_two, module: second_compiled, params: params]
        ],
        [
          builder_definition(builder_one, :first_key, :first),
          builder_definition(builder_two, :second_key, :second)
        ]
      )
    end
  end

  test "compile failures include the generated module and path" do
    builder_module = unique_module("BrokenBuilder")
    compiled_module = unique_module("BrokenCompiled")
    expected_path = Generator.module_file_path(builder_module, compiled_module, %{})

    error =
      assert_raise CompileError, fn ->
        compile_with_modules!(
          [
            [
              builder: builder_module,
              module: compiled_module,
              opts: [prologue: "this will not compile"]
            ]
          ],
          [builder_definition(builder_module, :id, :broken)]
        )
      end

    assert Exception.message(error) =~ inspect(compiled_module)
    assert Exception.message(error) =~ expected_path
  end
end
