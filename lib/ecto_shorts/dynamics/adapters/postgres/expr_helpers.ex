defmodule EctoShorts.Dynamics.Adapters.Postgres.ExprHelpers do
  @moduledoc since: "3.0.0"
  @moduledoc false

  alias EctoShorts.Compiler.AST

  @doc false
  def alias_to_canonical_map_ast(op_var) do
    quote do
      case unquote(op_var) do
        :gt -> :>
        :gte -> :>=
        :lt -> :<
        :lte -> :<=
        :eq -> :==
        :ne -> :!=
      end
    end
  end

  @doc false
  def aggregate_dynamic_case_ast(binding_body_asts, aggregate_expr_ast, op_var, value_var) do
    quote do
      case unquote(op_var) do
        :== ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(aggregate_expr_ast) == ^unquote(value_var))
            )
          )

        :!= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(aggregate_expr_ast) != ^unquote(value_var))
            )
          )

        :> ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(aggregate_expr_ast) > ^unquote(value_var))
            )
          )

        :>= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(aggregate_expr_ast) >= ^unquote(value_var))
            )
          )

        :< ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(aggregate_expr_ast) < ^unquote(value_var))
            )
          )

        :<= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(aggregate_expr_ast) <= ^unquote(value_var))
            )
          )
      end
    end
  end

  @doc false
  def not_aggregate_dynamic_case_ast(binding_body_asts, aggregate_expr_ast, op_var, value_var) do
    quote do
      case unquote(op_var) do
        :== ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(aggregate_expr_ast) == ^unquote(value_var)))
            )
          )

        :!= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(aggregate_expr_ast) != ^unquote(value_var)))
            )
          )

        :> ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(aggregate_expr_ast) > ^unquote(value_var)))
            )
          )

        :>= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(aggregate_expr_ast) >= ^unquote(value_var)))
            )
          )

        :< ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(aggregate_expr_ast) < ^unquote(value_var)))
            )
          )

        :<= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(aggregate_expr_ast) <= ^unquote(value_var)))
            )
          )
      end
    end
  end
end
