defmodule EctoShorts.Compiler do
  alias EctoShorts.Generator

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
end

# defmodule EctoShorts.Compiler do
#   @moduledoc since: "3.0.0"
#   @moduledoc """
#   Compiles `compose/3` clauses from specs at compile time.

#   Use this module when building a custom dynamic expression adapter that needs
#   to dispatch expression construction to different function clauses based on
#   binding patterns and expression types. The compiler generates all dispatch
#   clauses at compile time from a specs module you provide.

#   ## When to use the compiler

#   Use `EctoShorts.Compiler` when you need to:

#   * **Build a custom dynamic expression adapter** - implement database-specific
#     expression builders (for example, PostgreSQL array operators, MySQL JSON
#     functions).
#   * **Support multiple binding patterns** - generate clauses for named bindings
#     (`:as`), positional bindings (`:at`), and shortcut bindings (`:first`,
#     `:last`).
#   * **Avoid runtime dispatch overhead** - compile all dispatch logic at compile
#     time instead of using runtime pattern matching or case statements.
#   * **Integrate with EctoShorts.CommonFilters** - provide a custom adapter that
#     works seamlessly with the filter language.

#   ## Getting started

#   Define a specs module that exports `clause_specs/4`:

#       defmodule MyApp.Adapter.Specs do
#         alias EctoShorts.Compiler.ClauseSpec

#         def clause_specs(_context, binding_head, _target_binding, binding_bodies) do
#           Enum.map(binding_bodies, fn binding_body ->
#             ClauseSpec.new(%{
#               binding_head: binding_head,
#               key: Macro.var(:key, nil),
#               head: quote(do: {:==, val}),
#               body: quote(do: Ecto.Query.dynamic([{^binding_head, r}], field(r, ^key) == ^val))
#             })
#           end)
#         end
#       end

#   Use the compiler in your adapter module:

#       defmodule MyApp.Adapter do
#         use EctoShorts.Compiler, specs: MyApp.Adapter.Specs
#       end

#   After compilation, `MyApp.Adapter.compose/3` is available:

#       MyApp.Adapter.compose({:as, :post}, :title, {:==, "Hello"})
#       # Returns: dynamic([{:as, :post}, r], field(r, :title) == ^"Hello")

#   ## How it works

#   When a module `X` calls `use EctoShorts.Compiler, specs: MySpecs`:

#   1. **Collect specs** - `EctoShorts.Compiler` calls `MySpecs.clause_specs/4`
#      with each query binding pattern (named and positional) to collect
#      `%EctoShorts.Compiler.ClauseSpec{}` structs.
#   2. **Generate clauses** - it compiles those specs into `compose/3`
#      function clauses and injects them into a generated submodule `X.Compiled`.
#   3. **Define delegator** - it defines `X.compose/3` as a public
#      delegator to `X.Compiled.compose/3`.

#   The specs module (`MySpecs`) must be already compiled before `X` is compiled,
#   and must export `clause_specs/4`.

#   ## Clause spec structure

#   A `ClauseSpec` defines one function clause for `compose/3`:

#       ClauseSpec.new(%{
#         binding_head: binding_head,      # Binding pattern (e.g. {:as, :post})
#         key: Macro.var(:key, nil),       # Field name variable
#         head: quote(do: {:==, val}),     # Expression pattern to match
#         body: quote(do: ...)             # Quoted code to execute
#       })

#   ### Fields

#   * `:binding_head` - the binding pattern this clause matches (for example,
#     `{:as, :post}`, `{:at, 1}`, `:first`, `:last`).
#   * `:key` - a quoted variable that captures the field name (usually
#     `Macro.var(:key, nil)`).
#   * `:head` - a quoted pattern that matches the expression structure (for
#     example, `quote(do: {:==, val})` matches `{:==, "value"}`).
#   * `:body` - quoted code that builds the dynamic expression (for example,
#     `quote(do: Ecto.Query.dynamic([...], ...))`).

#   ### Example clause spec

#   This spec generates a clause that matches equality comparisons:

#       ClauseSpec.new(%{
#         binding_head: {:as, :post},
#         key: Macro.var(:key, nil),
#         head: quote(do: {:==, val}),
#         body: quote(do: Ecto.Query.dynamic([{:as, :post}, r], field(r, ^key) == ^val))
#       })

#   The generated clause looks like:

#       def compose({:as, :post}, key, {:==, val}) do
#         Ecto.Query.dynamic([{:as, :post}, r], field(r, ^key) == ^val)
#       end

#   ## Custom specs module

#   A complete specs module implements `clause_specs/4` and returns a list of
#   `ClauseSpec` structs:

#       defmodule MyApp.Adapter.Specs do
#         alias EctoShorts.Compiler.ClauseSpec

#         def clause_specs(_context, binding_head, _target_binding, binding_bodies) do
#           Enum.flat_map(binding_bodies, fn binding_body ->
#             [
#               # Equality clause
#               ClauseSpec.new(%{
#                 binding_head: binding_head,
#                 key: Macro.var(:key, nil),
#                 head: quote(do: {:==, val}),
#                 body: quote(do: Ecto.Query.dynamic([{^binding_head, r}], field(r, ^key) == ^val))
#               }),

#               # Greater than clause
#               ClauseSpec.new(%{
#                 binding_head: binding_head,
#                 key: Macro.var(:key, nil),
#                 head: quote(do: {:>, val}),
#                 body: quote(do: Ecto.Query.dynamic([{^binding_head, r}], field(r, ^key) > ^val))
#               }),

#               # Less than clause
#               ClauseSpec.new(%{
#                 binding_head: binding_head,
#                 key: Macro.var(:key, nil),
#                 head: quote(do: {:<, val}),
#                 body: quote(do: Ecto.Query.dynamic([{^binding_head, r}], field(r, ^key) < ^val))
#               })
#             ]
#           end)
#         end
#       end

#   ### Parameters

#   * `context` - the module being compiled (for example, `MyApp.Adapter.Compiled`).
#   * `binding_head` - the binding pattern for this set of clauses (for example,
#     `{:as, :post}`).
#   * `target_binding` - the target binding variable (usually not needed).
#   * `binding_bodies` - a list of binding body patterns (usually one element).

#   ## Binding patterns

#   The compiler generates clauses for these binding patterns:

#   * **Named bindings** - `{:as, :post}`, `{:as, :comment}`, etc.
#   * **Positional bindings** - `{:at, 1}`, `{:at, 2}`, etc.
#   * **Shortcut bindings** - `:first`, `:last`.

#   The number of positional bindings is controlled by `:max_binding_positions`.

#   ## Recompilation

#   `EctoShorts.Compiler` implements `__mix_recompile__?/0` so that your adapter
#   module is automatically recompiled whenever the configured
#   `:max_binding_positions` changes (for example, when you update the config
#   value between builds).

#   This ensures the generated clauses always match the current configuration.

#   ## Configuration

#   * `:max_binding_positions` - controls how many positional binding patterns
#     are generated. Increase this when your queries join more tables than the
#     default supports. Defaults to `EctoShorts.Config.max_binding_positions/0`.

#   > #### Compilation time {: .warning}
#   >
#   > Large `:max_binding_positions` values can significantly increase
#   > compilation time because the compiler generates a full set of function
#   > clauses for every positional binding. Only increase this value as far
#   > as your queries require.

#   Example:

#       defmodule MyApp.Adapter do
#         use EctoShorts.Compiler,
#           specs: MyApp.Adapter.Specs,
#           max_binding_positions: 20
#       end

#   ## Troubleshooting

#   **Problem:** Compilation fails with "Expected :specs to be a module".

#   **Solution:** Verify the `:specs` option points to a compiled module. The
#   specs module must be compiled before the adapter module.

#   **Problem:** Compilation fails with "Expected ... to export clause_specs/4".

#   **Solution:** Add a `clause_specs/4` function to your specs module. The
#   function must accept four arguments and return a list of `ClauseSpec` structs.

#   **Problem:** Generated clauses do not match at runtime.

#   **Solution:** Check that the `:head` pattern in your `ClauseSpec` matches
#   the expression structure you are passing to `compose/3`. Use
#   `IO.inspect/2` to see the actual expression structure.

#   **Problem:** Recompilation is not triggered when config changes.

#   **Solution:** Verify the `:max_binding_positions` config is set correctly.
#   Run `mix clean` and `mix compile` to force a full recompile.

#   See also `EctoShorts.Compiler.ClauseSpec`, `EctoShorts.Dynamic`,
#   and `EctoShorts.Config.max_binding_positions/0`.
#   """

#   alias EctoShorts.Config
#   alias EctoShorts.Compiler.ClauseBuilder
#   alias EctoShorts.Compiler.QueryBindingBuilder

#   @doc """
#   Defines `X.Compiled` and `X.compose/3` in the caller module `X`.

#   Injects a `@before_compile` hook that generates the compiled clause module
#   and the public delegator at the end of the caller's compilation. The caller
#   module also receives a `__mix_recompile__?/0` implementation that returns
#   `true` whenever `:max_binding_positions` changes, triggering a full recompile.

#   ## Options

#   * `:specs` (required) - a module, already compiled, that exports
#     `clause_specs/4`. Called once per binding pattern to collect
#     `%EctoShorts.Compiler.ClauseSpec{}` values.
#   * `:max_binding_positions` - the maximum number of positional query bindings
#     to generate clauses for. Defaults to `EctoShorts.Config.max_binding_positions/0`.

#   ## Errors

#   Raises `ArgumentError` at compile time when:

#   * `:specs` is missing from the options.
#   * `:specs` does not resolve to a compiled module atom.
#   * The resolved specs module does not export `clause_specs/4`.

#   ## Examples

#       defmodule MyApp.Adapter do
#         use EctoShorts.Compiler, specs: MyApp.Adapter.Specs
#       end

#       defmodule MyApp.Adapter do
#         use EctoShorts.Compiler,
#           specs: MyApp.Adapter.Specs,
#           max_binding_positions: 20
#       end

#   See also `EctoShorts.Compiler.ClauseSpec` and `EctoShorts.Config.max_binding_positions/0`.
#   """
#   defmacro __using__(opts) do
#     expanded_opts = Macro.prewalk(opts, &Macro.expand(&1, __CALLER__))
#     specs_modules = Keyword.fetch!(expanded_opts, :specs)

#     Enum.each(specs_modules, &validate_specs_module!/1)

#     quote do
#       @compiler_options unquote(opts)
#       @compiler_specs_modules unquote(specs_modules)
#       @before_compile unquote(__MODULE__)
#     end
#   end

#   @doc false
#   defmacro __before_compile__(env) do
#     opts = Module.get_attribute(env.module, :compiler_options)
#     specs_modules = Module.get_attribute(env.module, :compiler_specs_modules)
#     compiled_module = Module.concat(env.module, Compiled)
#     context = compiled_module

#     {sub_modules, sub_module_defs} =
#       specs_modules
#       |> Enum.map(fn specs_module ->
#         clause_asts = build_clauses(context, specs_module, opts)

#         sub_name = specs_module |> Module.split() |> List.last() |> String.to_atom()
#         sub_module = Module.concat(compiled_module, sub_name)

#         sub_def =
#           quote do
#             defmodule unquote(sub_module) do
#               @moduledoc false

#               require Ecto.Query

#               unquote_splicing(clause_asts)

#               def compose(_, _, _), do: nil
#             end
#           end

#         {sub_module, sub_def}
#       end)
#       |> Enum.unzip()

#     dispatch_ast = build_dispatch_chain(sub_modules)

#     quote do
#       @compiled_max_binding_positions unquote(__MODULE__).max_binding_positions(@compiler_options)

#       unquote_splicing(sub_module_defs)

#       @doc false
#       def compose(binding_selector, key, expr) do
#         unquote(dispatch_ast)
#       end

#       @doc false
#       def config_stale?(current_max) do
#         current_max !== @compiled_max_binding_positions
#       end

#       @doc false
#       def __mix_recompile__? do
#         @compiler_options
#         |> unquote(__MODULE__).max_binding_positions()
#         |> config_stale?()
#       end
#     end
#   end

#   defp build_dispatch_chain([single]) do
#     quote do
#       unquote(single).compose(binding_selector, key, expr)
#     end
#   end

#   defp build_dispatch_chain([head | tail]) do
#     quote do
#       unquote(head).compose(binding_selector, key, expr) ||
#         unquote(build_dispatch_chain(tail))
#     end
#   end

#   defp validate_specs_module!(specs_module) do
#     unless is_atom(specs_module) do
#       raise ArgumentError,
#             "Expected :specs to be a module, got: #{inspect(specs_module)}"
#     end

#     case Code.ensure_compiled(specs_module) do
#       {:module, _} ->
#         :ok

#       {:error, reason} ->
#         raise ArgumentError,
#               "Expected :specs to be a compiled module, got: #{inspect(specs_module)} (#{inspect(reason)})"
#     end

#     unless function_exported?(specs_module, :clause_specs, 4) do
#       raise ArgumentError,
#             "Expected #{inspect(specs_module)} to export clause_specs/4"
#     end

#     :ok
#   end

#   @doc false
#   defmacro define_clauses(opts \\ [], do: block) do
#     {quoted_binding_head_var, quoted_binding_body_var, target_binding_var, binding_patterns_var, body_ast} =
#       parse_query_binding_clauses_block!(block)

#     context = __CALLER__.module

#     {target_binding_var_ast, binding_patterns_ast} =
#       get_query_binding_contracts(context, opts)

#     quote do
#       unquote(target_binding_var) = unquote(Macro.escape(target_binding_var_ast))
#       unquote(binding_patterns_var) = unquote(Macro.escape(binding_patterns_ast))

#       for {unquote(quoted_binding_head_var), unquote(quoted_binding_body_var)} <-
#             unquote(Macro.escape(binding_patterns_ast)) do
#         unquote(body_ast)
#       end
#     end
#   end

#   defp parse_query_binding_clauses_block!({:->, _meta, [[a, b, c, d], body_ast]}) do
#     {a, b, c, d, body_ast}
#   end

#   defp parse_query_binding_clauses_block!({:__block__, _meta, [single_clause]}) do
#     parse_query_binding_clauses_block!(single_clause)
#   end

#   defp parse_query_binding_clauses_block!([single_clause]) do
#     parse_query_binding_clauses_block!(single_clause)
#   end

#   defp parse_query_binding_clauses_block!(ast) do
#     raise ArgumentError,
#           """
#           Expected define_clauses/2 block in the form:

#               Compiler.define_clauses do
#                 quoted_binding_head, quoted_binding_body, target_binding_var, binding_patterns ->
#                   ...
#               end

#           Got:
#           #{Macro.to_string(ast)}
#           """
#   end

#   @doc false
#   def build_clauses(context, specs_module, opts) do
#     {target_binding_var, binding_patterns} =
#       get_query_binding_contracts(context, opts)

#     Enum.flat_map(binding_patterns, fn {binding_head_ast, binding_body_asts} ->
#       context
#       |> specs_module.clause_specs(binding_head_ast, target_binding_var, binding_body_asts)
#       |> Enum.map(&clause_ast!/1)
#     end)
#   end

#   @doc false
#   def get_query_binding_contracts(context, opts \\ []) do
#     QueryBindingBuilder.query_binding_contracts(
#       context,
#       max_binding_positions(opts)
#     )
#   end

#   @doc false
#   def max_binding_positions(opts \\ []) do
#     opts[:max_binding_positions] || Config.max_binding_positions()
#   end

#   defp clause_ast!(spec) do
#     ClauseBuilder.clause_ast(spec)
#   end
# end
