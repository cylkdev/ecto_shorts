defmodule EctoShorts.Compiler do
  @moduledoc """
  Defines the spec-driven compiler for `apply_dynamic_expr/3` clauses.

  A module `X` can `use #{inspect(__MODULE__)}` to:

    * define a predictable compiled module named `X.Compiled` containing the
      generated `apply_dynamic_expr/3` clauses
    * define `X.apply_dynamic_expr/3` as the public entrypoint, delegating to
      `X.Compiled.apply_dynamic_expr/3`

  The clause specs must be defined in a separate, already-compiled specs module
  that exports `clause_specs/4`.
  """

  alias EctoShorts.Config
  alias EctoShorts.Compiler.ClauseBuilder
  alias EctoShorts.Compiler.QueryBindingBuilder

  @doc """
  Defines `X.Compiled` and `X.apply_dynamic_expr/3` in the caller module `X`.

  ## Options

    * `:specs` (required) - a module that exports `clause_specs/4`
    * `:max_query_bindings` - passed to `QueryBindingBuilder` (defaults to `10`)
  """
  defmacro __using__(opts) do
    quote do
      opts = unquote(opts)
      @compiler_options opts
      @before_compile EctoShorts.Compiler
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
      @compile_time_max_query_bindings EctoShorts.Compiler.max_query_bindings(@compiler_options)

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
      def __mix_recompile__? do
        EctoShorts.Compiler.max_query_bindings(@compiler_options) !==
          @compile_time_max_query_bindings
      end
    end
  end

  @doc false
  defmacro define_clauses(opts \\ [], do: block) do
    {quoted_binding_head_var, quoted_binding_body_var, target_binding_var, binding_patterns_var,
     body_ast} =
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
      max_query_bindings(opts)
    )
  end

  @doc false
  def max_query_bindings(opts \\ []) do
    opts[:max_query_bindings] || Config.max_query_bindings()
  end

  defp clause_ast!(spec) do
    ClauseBuilder.clause_ast(spec)
  end
end
