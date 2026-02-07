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
    * `:max_positional_bindings` - passed to `QueryBindingBuilder` (defaults to `10`)
  """
  defmacro __using__(opts) do
    quote do
      @compiler_options unquote(opts)
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

    clause_asts = build_clause_asts(context, specs_module, opts)

    quote do
      defmodule unquote(compiled_module) do
        @moduledoc false

        import Ecto.Query
        require Ecto.Query

        unquote_splicing(clause_asts)
      end

      @doc false
      def apply_dynamic_expr(binding_selector, key, expr) do
        unquote(compiled_module).apply_dynamic_expr(binding_selector, key, expr)
      end
    end
  end

  @doc false
  def build_clause_asts(context, specs_module, opts) do
    compiler_config = Config.compiler()

    max_positional_bindings =
      Keyword.get_lazy(opts, :max_positional_bindings, fn ->
        Keyword.get(compiler_config, :max_positional_bindings, 10)
      end)

    {target_binding_var, binding_patterns} =
      QueryBindingBuilder.query_var_and_binding_heads(context, max_positional_bindings)

    Enum.flat_map(binding_patterns, fn {binding_head_ast, binding_body_asts} ->
      specs_module.clause_specs(
        context,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      )
      |> Enum.map(&clause_ast!/1)
    end)
  end

  defp clause_ast!(spec) do
    case ClauseBuilder.clause_ast(spec) do
      {:ok, ast} -> ast
      {:error, reason} -> raise ArgumentError, "Failed to build clause: #{inspect(reason)}"
    end
  end
end
