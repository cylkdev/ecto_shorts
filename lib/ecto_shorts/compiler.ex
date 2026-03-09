defmodule EctoShorts.Compiler do
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @__ecto_shorts_compiler_options__ opts
      @before_compile EctoShorts.Compiler
    end
  end

  defmacro __before_compile__(env) do
    opts = Module.get_attribute(env.module, :__ecto_shorts_compiler_options__) || []
    entries = Keyword.get(opts, :modules, [])

    generated =
      Enum.map(entries, fn entry ->
        EctoShorts.Generator.generate_module(
          entry[:builder],
          entry[:module],
          Keyword.drop(entry, [:builder, :module])
        )
      end)

    written = Enum.map(generated, &EctoShorts.Generator.write_module/1)

    {compiled_modules, paths} = Enum.unzip(written)

    unload_generated_modules!(compiled_modules)
    compile_generated_modules!(generated, paths, env)

    quote do
      def dynamic_expr(binding_selector, key, value) do
        unquote(dispatcher_body_ast(Enum.map(entries, & &1[:module])))
      end
    end
  end

  defp dispatcher_body_ast(modules) do
    Enum.reduce(Enum.reverse(modules), quote(do: nil), fn module, acc ->
      quote do
        case unquote(module).dynamic_expr(binding_selector, key, value) do
          nil -> unquote(acc)
          result -> result
        end
      end
    end)
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

  defp compile_generated_modules!(generated, paths, env) do
    module_by_path = Map.new(generated, fn {module, path, _content} -> {path, module} end)

    case paths do
      [] ->
        :ok

      _ ->
        case Kernel.ParallelCompiler.compile_to_path(paths, Mix.Project.compile_path()) do
          {:ok, _modules, _warnings} ->
            :ok

          {:error, errors, _warnings} ->
            raise CompileError,
              file: env.file,
              line: env.line,
              description:
                "failed to compile generated files for #{inspect(env.module)}: #{format_compile_errors(errors, module_by_path)}"
        end
    end
  end

  defp format_compile_errors(errors, module_by_path) do
    Enum.map_join(errors, "; ", fn {path, line, description} ->
      module = Map.get(module_by_path, path, :unknown)
      "module=#{inspect(module)} path=#{path} line=#{line} #{description}"
    end)
  end
end
