defmodule EctoShorts.Compiler do
  @allowed_module_options [:builder, :module, :params, :opts]

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @__ecto_shorts_compiler_options__ opts
      @before_compile EctoShorts.Compiler
    end
  end

  defmacro __before_compile__(env) do
    opts = Module.get_attribute(env.module, :__ecto_shorts_compiler_options__) || []

    entries =
      opts
      |> Keyword.fetch!(:modules)
      |> Enum.map(&(&1 |> Keyword.take(@allowed_module_options) |> Map.new()))

    generated =
      Enum.flat_map(entries, fn entry ->
        EctoShorts.Generator.write_module_files(
          entry.builder,
          entry.module,
          entry[:opts] || []
        )
      end)

    {compiled_modules, paths} = Enum.unzip(generated)

    unload_generated_modules!(compiled_modules)
    compile_generated_modules!(paths, env)

    branches =
      compiled_modules
      |> Enum.map(fn compiled_module ->
        quote do
          unquote(compiled_module) ->
            unquote(compiled_module).dynamic_expr(binding_selector, key, value)
        end
      end)
      |> Kernel.++([quote(do: (_ -> nil))])
      |> List.flatten()

    quote do
      def dynamic_expr(compiled_module_name, binding_selector, key, value) do
        case compiled_module_name do
          unquote(branches)
        end
      end
    end
  end

  defp unload_generated_modules!(modules) do
    Enum.each(modules, fn module ->
      case :code.is_loaded(module) do
        false ->
          :ok

        _path ->
          :code.delete(module)
          :code.purge(module)
      end
    end)
  end

  defp compile_generated_modules!(paths, env) do
    # previous = Code.compiler_options()[:ignore_module_conflict]
    # Code.put_compiler_option(:ignore_module_conflict, true)

    # try do
    case Kernel.ParallelCompiler.compile_to_path(paths, Mix.Project.compile_path()) do
      {:ok, _modules, _warnings} ->
        :ok

      {:error, errors, _warnings} ->
        raise CompileError,
          file: env.file,
          line: env.line,
          description: "failed to compile generated files for #{inspect(env.module)}: #{inspect(errors)}"
    end

    # after
    #   Code.put_compiler_option(:ignore_module_conflict, previous)
    # end
  end
end
