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

  defp multi_key_builder_definition(module, keys) do
    quote do
      defmodule unquote(module) do
        @behaviour EctoShorts.Generator.ClauseSpec

        def keys, do: unquote(keys)

        def specs_for(spec_key, _binding_selector, _q_var, opts) do
          context = opts[:context]
          value_var = Macro.var(:value, context)

          [
            %EctoShorts.Generator.Blueprint{
              guard: nil,
              key: spec_key,
              head: value_var,
              body: quote(do: {unquote(spec_key), unquote(value_var)})
            }
          ]
        end
      end
    end
  end

  defp broken_builder_definition(module, key) do
    quote do
      defmodule unquote(module) do
        @behaviour EctoShorts.Generator.ClauseSpec

        def keys, do: [unquote(key)]

        def specs_for(spec_key, _binding_selector, _q_var, opts) do
          context = opts[:context]
          value_var = Macro.var(:value, context)

          [
            %EctoShorts.Generator.Blueprint{
              guard: nil,
              key: spec_key,
              head: value_var,
              body: quote(do: this_will_not_compile(unquote(value_var)))
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

  defp compile_use!(opts, builder_definitions \\ []) do
    caller_module = unique_module("Caller")
    escaped_opts = Macro.escape(opts)

    quoted =
      quote do
        unquote_splicing(builder_definitions)

        defmodule unquote(caller_module) do
          use EctoShorts.Compiler, unquote(escaped_opts)
        end
      end

    Code.compile_quoted(quoted)
    caller_module
  end

  test "use EctoShorts.Compiler with one generated module exposes dynamic_expr/3" do
    builder_module = unique_module("SingleBuilder")
    compiled_module = unique_module("SingleCompiled")

    caller_module =
      compile_with_modules!(
        [[builder: builder_module, module: compiled_module]],
        [builder_definition(builder_module, :id, :single)]
      )

    assert {:single, 1} = caller_module.dynamic_expr({:as, :post}, :id, 1)
    assert is_nil(caller_module.dynamic_expr({:as, :post}, :missing, 1))
  end

  test "multiple generated modules are compiled in one pass and dispatch by key" do
    first_builder = unique_module("FirstBuilder")
    second_builder = unique_module("SecondBuilder")
    first_compiled = unique_module("FirstCompiled")
    second_compiled = unique_module("SecondCompiled")

    caller_module =
      compile_with_modules!(
        [
          [builder: first_builder, module: first_compiled],
          [builder: second_builder, module: second_compiled]
        ],
        [
          builder_definition(first_builder, :id, :first),
          builder_definition(second_builder, :slug, :second)
        ]
      )

    assert {:first, 1} = caller_module.dynamic_expr({:as, :post}, :id, 1)
    assert {:second, "post"} = caller_module.dynamic_expr({:as, :post}, :slug, "post")

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
          [builder: first_builder, module: first_compiled],
          [builder: second_builder, module: second_compiled]
        ],
        [
          builder_definition(first_builder, :first_key, :first),
          builder_definition(second_builder, :second_key, :second)
        ]
      )

    assert is_nil(caller_module.dynamic_expr({:as, :post}, :missing, 1))
  end

  test "multiple explicit modules can split one builder into separate compiled modules" do
    builder_module = unique_module("PartitionedBuilder")
    first_compiled = unique_module("PartitionedCompiledFirst")
    second_compiled = unique_module("PartitionedCompiledSecond")

    caller_module =
      compile_with_modules!(
        [
          [builder: builder_module, module: first_compiled, keys: [:first_key]],
          [builder: builder_module, module: second_compiled, keys: [:second_key]]
        ],
        [multi_key_builder_definition(builder_module, [:first_key, :second_key])]
      )

    assert {:second_key, 2} = caller_module.dynamic_expr({:as, :post}, :second_key, 2)
  end

  test "omitting :modules defaults dynamic_expr/3 to nil" do
    caller_module = compile_use!([])

    assert is_nil(caller_module.dynamic_expr({:as, :post}, :id, 1))
  end

  test "compiler passes flattened path and filename options to the generator" do
    builder_module = unique_module("CustomPathBuilder")
    compiled_module = unique_module("CustomPathCompiled")

    expected_path =
      Generator.module_file_path(builder_module, compiled_module,
        path: "custom/path",
        filename: "custom_compiled.ex"
      )

    compile_with_modules!(
      [
        [
          builder: builder_module,
          module: compiled_module,
          path: "custom/path",
          filename: "custom_compiled.ex"
        ]
      ],
      [builder_definition(builder_module, :id, :custom)]
    )

    assert File.exists?(expected_path)
  end

  test "compile failures include the generated module and path" do
    builder_module = unique_module("BrokenBuilder")
    compiled_module = unique_module("BrokenCompiled")
    expected_path = Generator.module_file_path(builder_module, compiled_module)

    error =
      assert_raise CompileError, fn ->
        compile_with_modules!(
          [[builder: builder_module, module: compiled_module]],
          [broken_builder_definition(builder_module, :id)]
        )
      end

    assert Exception.message(error) =~ inspect(compiled_module)
    assert Exception.message(error) =~ expected_path
  end
end
