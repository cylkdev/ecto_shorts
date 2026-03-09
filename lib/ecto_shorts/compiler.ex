defmodule EctoShorts.Compiler do
  @allowed_options [:modules]
  @allowed_module_options [:builder, :module, :modes, :params, :opts]

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

    paths =
      Enum.flat_map(entries, fn entry ->
        EctoShorts.Generator.write_module_files(
          entry.builder,
          entry.module,
          entry[:modes] || :all,
          entry[:params] || %{},
          entry[:opts] || []
        )
      end)

    compile_generated_modules!(paths, entries, env)

    compiled_modules = Enum.map(entries, & &1.module)

    branches =
      compiled_modules
      |> Enum.map(fn compiled_module ->
        quote do
          unquote(compiled_module) ->
            unquote(compiled_module).dynamic_expr(binding_selector, key, value)
        end
      end)
      |> Kernel.++([
        quote do
          _ -> nil
        end
      ])

    quote do
      def dynamic_expr(compiled_module_name, binding_selector, key, value) do
        case compiled_module_name do
          (unquote_splicing(branches))
        end
      end
    end
  end

  defp compile_generated_modules!(paths, entries, env) do
    case Kernel.ParallelCompiler.compile_to_path(paths, Mix.Project.compile_path()) do
      {:ok, _modules, _warnings} ->
        :ok

      {:error, errors, _warnings} ->
        details =
          Enum.map_join(entries, ", ", fn entry ->
            "#{inspect(entry.module)} => #{entry.path}"
          end)

        raise CompileError,
          file: env.file,
          line: env.line,
          description:
            "failed to compile generated files for #{inspect(env.module)} (#{details}): #{inspect(errors)}"
    end
  end
end
