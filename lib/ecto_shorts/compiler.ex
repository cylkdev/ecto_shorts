defmodule EctoShorts.Compiler do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Provides a spec-driven, macro-based compiler for `apply_dynamic_expr/3` clauses.

  Use this module when building a custom dynamic expression adapter that needs
  to dispatch expression construction to different function clauses depending on
  the binding pattern and expression type. `EctoShorts.Compiler` generates the
  dispatch module at compile time from a specs module you supply.

  ## How it works

  When a module `X` calls `use EctoShorts.Compiler, specs: MySpecs`:

  1. `EctoShorts.Compiler` calls `MySpecs.clause_specs/4` with each
     query binding pattern (named and positional) to collect
     `%EctoShorts.Compiler.ClauseSpec{}` structs.
  2. It compiles those specs into `apply_dynamic_expr/3` function clauses and
     injects them into a generated submodule `X.Compiled`.
  3. It defines `X.apply_dynamic_expr/3` as a public delegator to
     `X.Compiled.apply_dynamic_expr/3`.

  The specs module (`MySpecs`) must be already compiled before `X` is compiled,
  and must export `clause_specs/4`.

  ## Getting started

      defmodule MyApp.Adapter.Specs do
        alias EctoShorts.Compiler.ClauseSpec

        def clause_specs(_context, binding_head, _target_binding, binding_bodies) do
          Enum.map(binding_bodies, fn binding_body ->
            ClauseSpec.new(%{
              binding_head: binding_head,
              key: Macro.var(:key, nil),
              head: quote(do: {:==, val}),
              body: quote(do: Ecto.Query.dynamic([{^binding_head, r}], field(r, ^key) == ^val))
            })
          end)
        end
      end

      defmodule MyApp.Adapter do
        use EctoShorts.Compiler, specs: MyApp.Adapter.Specs
      end

  After compilation, `MyApp.Adapter.apply_dynamic_expr/3` is available.

  ## Recompilation

  `EctoShorts.Compiler` implements `__mix_recompile__?/0` so that `X` is
  automatically recompiled whenever the configured `:max_binding_positions`
  changes (for example, when you update the config value between builds).

  ## Configuration

  * `:max_binding_positions` - controls how many positional binding patterns are
    generated. Increase this when your queries join more tables than the default
    supports. Defaults to `EctoShorts.Config.max_binding_positions/0`.

  See also `EctoShorts.Compiler.ClauseSpec` and `EctoShorts.Dynamics.Adapter`.
  """

  alias EctoShorts.Config
  alias EctoShorts.Compiler.ClauseBuilder
  alias EctoShorts.Compiler.QueryBindingBuilder

  @doc """
  Defines `X.Compiled` and `X.apply_dynamic_expr/3` in the caller module `X`.

  Injects a `@before_compile` hook that generates the compiled clause module
  and the public delegator at the end of the caller's compilation. The caller
  module also receives a `__mix_recompile__?/0` implementation that returns
  `true` whenever `:max_binding_positions` changes, triggering a full recompile.

  ## Options

  * `:specs` (required) - a module, already compiled, that exports
    `clause_specs/4`. Called once per binding pattern to collect
    `%EctoShorts.Compiler.ClauseSpec{}` values.
  * `:max_binding_positions` - the maximum number of positional query bindings
    to generate clauses for. Defaults to `EctoShorts.Config.max_binding_positions/0`.

  ## Errors

  Raises `ArgumentError` at compile time when:

  * `:specs` is missing from the options.
  * `:specs` does not resolve to a compiled module atom.
  * The resolved specs module does not export `clause_specs/4`.

  ## Examples

      defmodule MyApp.Adapter do
        use EctoShorts.Compiler, specs: MyApp.Adapter.Specs
      end

      defmodule MyApp.Adapter do
        use EctoShorts.Compiler,
          specs: MyApp.Adapter.Specs,
          max_binding_positions: 20
      end

  See also `EctoShorts.Compiler.ClauseSpec` and `EctoShorts.Config.max_binding_positions/0`.
  """
  defmacro __using__(opts) do
    quote do
      opts = unquote(opts)
      @compiler_options opts
      @before_compile unquote(__MODULE__)
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    opts = Module.get_attribute(env.module, :compiler_options)
    specs_opt = Keyword.fetch!(opts, :specs)
    specs_module = Macro.expand(specs_opt, env)

    unless is_atom(specs_module) do
      raise ArgumentError,
            "Expected :specs to be a module, got: #{Macro.to_string(specs_opt)}"
    end

    case Code.ensure_compiled(specs_module) do
      {:module, _} ->
        :ok

      {:error, reason} ->
        raise ArgumentError,
              "Expected :specs to be a compiled module, got: #{inspect(specs_module)} (#{inspect(reason)})"
    end

    unless function_exported?(specs_module, :clause_specs, 4) do
      raise ArgumentError,
            "Expected #{inspect(specs_module)} to export clause_specs/4"
    end

    compiled_module = Module.concat(env.module, Compiled)
    context = compiled_module

    clause_asts = build_clauses(context, specs_module, opts)

    quote do
      @compiled_max_binding_positions unquote(__MODULE__).max_binding_positions(@compiler_options)

      defmodule unquote(compiled_module) do
        @moduledoc false

        require Ecto.Query

        unquote_splicing(clause_asts)
      end

      @doc false
      def apply_dynamic_expr(binding_selector, key, expr) do
        unquote(compiled_module).apply_dynamic_expr(binding_selector, key, expr)
      end

      @doc false
      def config_stale?(current_max) do
        current_max !== @compiled_max_binding_positions
      end

      @doc false
      def __mix_recompile__? do
        @compiler_options
        |> unquote(__MODULE__).max_binding_positions()
        |> config_stale?()
      end
    end
  end

  @doc false
  defmacro define_clauses(opts \\ [], do: block) do
    {quoted_binding_head_var, quoted_binding_body_var, target_binding_var, binding_patterns_var, body_ast} =
      parse_query_binding_clauses_block!(block)

    context = __CALLER__.module

    {target_binding_var_ast, binding_patterns_ast} =
      get_query_binding_contracts(context, opts)

    quote do
      unquote(target_binding_var) = unquote(Macro.escape(target_binding_var_ast))
      unquote(binding_patterns_var) = unquote(Macro.escape(binding_patterns_ast))

      for {unquote(quoted_binding_head_var), unquote(quoted_binding_body_var)} <-
            unquote(Macro.escape(binding_patterns_ast)) do
        unquote(body_ast)
      end
    end
  end

  defp parse_query_binding_clauses_block!({:->, _meta, [[a, b, c, d], body_ast]}) do
    {a, b, c, d, body_ast}
  end

  defp parse_query_binding_clauses_block!({:__block__, _meta, [single_clause]}) do
    parse_query_binding_clauses_block!(single_clause)
  end

  defp parse_query_binding_clauses_block!([single_clause]) do
    parse_query_binding_clauses_block!(single_clause)
  end

  defp parse_query_binding_clauses_block!(ast) do
    raise ArgumentError,
          """
          Expected define_clauses/2 block in the form:

              Compiler.define_clauses do
                quoted_binding_head, quoted_binding_body, target_binding_var, binding_patterns ->
                  ...
              end

          Got:
          #{Macro.to_string(ast)}
          """
  end

  @doc false
  def build_clauses(context, specs_module, opts) do
    {target_binding_var, binding_patterns} =
      get_query_binding_contracts(context, opts)

    Enum.flat_map(binding_patterns, fn {binding_head_ast, binding_body_asts} ->
      context
      |> specs_module.clause_specs(binding_head_ast, target_binding_var, binding_body_asts)
      |> Enum.map(&clause_ast!/1)
    end)
  end

  @doc false
  def get_query_binding_contracts(context, opts \\ []) do
    QueryBindingBuilder.query_binding_contracts(
      context,
      max_binding_positions(opts)
    )
  end

  @doc false
  def max_binding_positions(opts \\ []) do
    opts[:max_binding_positions] || Config.max_binding_positions()
  end

  defp clause_ast!(spec) do
    ClauseBuilder.clause_ast(spec)
  end
end
