defmodule EctoShorts.Compiler do
  alias EctoShorts.Generator.ClauseSpec

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

    {_compiled_modules, paths} = Enum.unzip(written)

    compile_modules(generated, paths, env)

    quote do
    end
  end

  defp compile_modules(written_modules, paths, env) do
    module_by_path = Map.new(written_modules, fn {module, path, _content} -> {path, module} end)

    if paths === [] do
      :ok
    else
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
