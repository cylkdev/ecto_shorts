defmodule EctoShorts.Dynamics.Compiler do
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

  alias EctoShorts.Dynamics.Compiler.ClauseSpec

  @doc """
  Defines `X.Compiled` and `X.apply_dynamic_expr/3` in the caller module `X`.

  ## Options

    * `:specs` (required) - a module that exports `clause_specs/4`
    * `:max_positional_bindings` - passed to `BindingHelpers` (defaults to `10`)
  """
  defmacro __using__(opts) do
    specs_module =
      opts
      |> Keyword.fetch!(:specs)
      |> Macro.expand(__CALLER__)

    max_positional_bindings = Keyword.get(opts, :max_positional_bindings, 10)

    unless is_atom(specs_module) do
      raise ArgumentError,
            "Expected :specs to be a module, got: #{Macro.to_string(Keyword.fetch!(opts, :specs))}"
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

    caller_module = __CALLER__.module
    compiled_module = Module.concat(caller_module, Compiled)
    context = compiled_module

    alias EctoShorts.Dynamics.Compiler.BindingHelpers

    {target_binding_var, binding_patterns} =
      BindingHelpers.query_var_and_binding_heads(
        context,
        max_positional_bindings: max_positional_bindings
      )

    clause_asts =
      Enum.flat_map(binding_patterns, fn {binding_head_ast, binding_body_asts} ->
        specs_module.clause_specs(
          context,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        )
        |> Enum.map(&clause_ast!/1)
      end)

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

  @doc """
  Builds a quoted `apply_dynamic_expr/3` clause from a clause spec.

  ## Error reasons

    * `:missing_key` - a required spec key is missing (from `ClauseSpec.new/1`)
    * `:invalid_spec` - the spec attrs are not valid (from `ClauseSpec.new/1`)
  """
  @spec clause_ast(ClauseSpec.t() | map() | keyword()) :: {:ok, Macro.t()} | {:error, term()}
  def clause_ast(spec) do
    with {:ok, spec} <- ClauseSpec.new(spec) do
      {:ok,
       quote_def(
         spec.binding_head,
         spec.key,
         spec.head,
         spec.body,
         spec.guard
       )}
    end
  end

  defp quote_def(binding_head_ast, key_ast, head_ast, body_ast, nil) do
    quote do
      def apply_dynamic_expr(unquote(binding_head_ast), unquote(key_ast), unquote(head_ast)) do
        unquote(body_ast)
      end
    end
  end

  defp quote_def(binding_head_ast, key_ast, head_ast, body_ast, guard_ast) do
    quote do
      def apply_dynamic_expr(unquote(binding_head_ast), unquote(key_ast), unquote(head_ast))
          when unquote(guard_ast) do
        unquote(body_ast)
      end
    end
  end

  defp clause_ast!(spec) do
    case clause_ast(spec) do
      {:ok, ast} -> ast
      {:error, reason} -> raise ArgumentError, "Failed to build clause: #{inspect(reason)}"
    end
  end
end
