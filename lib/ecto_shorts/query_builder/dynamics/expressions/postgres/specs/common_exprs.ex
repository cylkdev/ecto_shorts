defmodule EctoShorts.QueryBuilder.Dynamics.Expressions.Postgres.Specs.CommonExprs do
  @moduledoc false

  alias EctoShorts.QueryBuilder.BindingHelpers
  alias EctoShorts.QueryBuilder.Dynamics.Expression.AST
  alias EctoShorts.QueryBuilder.Dynamics.Expression.ClauseBuilder
  alias EctoShorts.QueryBuilder.Dynamics.Expression.ClauseSpec
  alias EctoShorts.QueryBuilder.Dynamics.Expression.Emitters.DynamicFieldExpr

  @doc """
  Defines common `dynamic_field_expr/3` clauses using the spec-driven ClauseBuilder.

  This builder targets the "common operator" keys used by `Dynamics`:

  * `:ids`
  * `:after`
  * `:before`
  * `:start_date`
  * `:end_date`
  """
  defmacro define_common_exprs(context_ast \\ nil, opts_ast \\ []) do
    context = Macro.expand(context_ast, __CALLER__)
    opts = Macro.expand(opts_ast, __CALLER__)

    {target_binding_var, binding_patterns} =
      BindingHelpers.query_var_and_binding_heads(context, opts)

    clause_asts =
      for {binding_head_ast, binding_body_asts} <- binding_patterns,
          spec <-
            clause_specs(
              :common,
              context,
              binding_head_ast,
              target_binding_var,
              binding_body_asts
            ) do
        clause_ast_or_raise(DynamicFieldExpr, spec)
      end

    quote do
      (unquote_splicing(clause_asts))
    end
  end

  @doc false
  def clause_specs(
        :common = kind,
        context,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      ) do
    id_values_var = Macro.var(:id_values, context)
    cursor_value_var = Macro.var(:cursor_value, context)
    date_value_var = Macro.var(:date_value, context)

    [
      %{
        kind: kind,
        binding_head: binding_head_ast,
        key: :ids,
        head: id_values_var,
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              field(unquote(target_binding_var), :id) in ^unquote(id_values_var)
            end
          )
      },
      %{
        kind: kind,
        binding_head: binding_head_ast,
        key: :after,
        head: cursor_value_var,
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              field(unquote(target_binding_var), :id) > ^unquote(cursor_value_var)
            end
          )
      },
      %{
        kind: kind,
        binding_head: binding_head_ast,
        key: :before,
        head: cursor_value_var,
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              field(unquote(target_binding_var), :id) < ^unquote(cursor_value_var)
            end
          )
      },
      %{
        kind: kind,
        binding_head: binding_head_ast,
        key: :start_date,
        head: date_value_var,
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              field(unquote(target_binding_var), :inserted_at) >= ^unquote(date_value_var)
            end
          )
      },
      %{
        kind: kind,
        binding_head: binding_head_ast,
        key: :end_date,
        head: date_value_var,
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              field(unquote(target_binding_var), :inserted_at) <= ^unquote(date_value_var)
            end
          )
      }
    ]
    |> Enum.map(&ClauseSpec.new!/1)
  end

  def clause_specs(_kind, _context, _binding_head_ast, _target_binding_var, _binding_body_asts) do
    []
  end

  defp clause_ast_or_raise(emitter, spec) when is_atom(emitter) do
    case ClauseBuilder.clause_ast(emitter, spec) do
      {:ok, ast} -> ast
      {:error, reason} -> raise ArgumentError, "Failed to build clause: #{inspect(reason)}"
    end
  end
end
