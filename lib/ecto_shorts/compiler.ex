defmodule EctoShorts.Compiler do
  alias EctoShorts.Generator
  alias EctoShorts.Compiler.QueryBindingBuilder

  defmacro __using__(opts) do
    quote do
      @__ecto_shorts_compiler_options__ unquote(opts)
      @before_compile EctoShorts.Compiler
    end
  end

  defmacro __before_compile__(env) do
    opts = Module.get_attribute(env.module, :__ecto_shorts_compiler_options__) || []

    entries = Keyword.get(opts, :modules, [])

    {paths, meta} =
      Enum.reduce(entries, {[], %{}}, fn entry, {paths, meta} ->
        builder = entry[:builder]
        module_name = entry[:module]
        path = entry[:path] || Path.join(module_to_path(builder), module_to_filename(module_name))
        opts = Keyword.drop(entry, [:builder, :module])

        dest_file = generated_path(path)
        content = Generator.generate_module(builder, module_name, opts)
        :ok = Generator.write_file(dest_file, content)

        {[dest_file | paths], Map.put(meta, module_name, dest_file)}
      end)

    paths
    |> Enum.reverse()
    |> compile_modules(meta, env)

    quote do
    end
  end

  defp priv_dir do
    :ecto_shorts |> :code.priv_dir() |> to_string()
  end

  defp generated_path(path) do
    dir = Path.join(priv_dir(), "generated")
    Path.join(dir, path)
  end

  defp module_to_path(builder) do
    builder
    |> Module.split()
    |> Enum.drop(1)
    |> Enum.map(&Macro.underscore/1)
    |> Enum.join("/")
  end

  defp module_to_filename(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
    |> Kernel.<>(".ex")
  end

  defp compile_modules(paths, meta, env) do
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
              "failed to compile generated files for #{inspect(env.module)}: #{format_compile_errors(errors, meta)}"
      end
    end
  end

  defp format_compile_errors(errors, module_by_path) do
    Enum.map_join(errors, "; ", fn {path, line, description} ->
      module = Map.get(module_by_path, path, :unknown)
      "module=#{inspect(module)} path=#{path} line=#{line} #{description}"
    end)
  end

  @spec query_binding_contracts(pos_integer(), atom()) :: Macro.t()
  def query_binding_contracts(positions, context) do
    QueryBindingBuilder.query_binding_contracts(positions, context)
  end
end
