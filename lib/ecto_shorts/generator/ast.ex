defmodule EctoShorts.Generator.AST do
  alias EctoShorts.Generator.ClauseSpec
  alias EctoShorts.Generator.Blueprint

  @doc """
  Generates a clause that matches a named binding with a nil value.

  ## Examples

      iex> EctoShorts.Generator.positional_clause_asts(EctoShorts.Dynamics.Postgres.CommonExpr, 3)
  """
  def positional_clause_asts(builder, index, opts \\ []) when is_integer(index) and index >= 1 do
    bind_op = :at
    selected_binding = {bind_op, index}

    specs_to_clauses(builder, selected_binding, opts)
  end

  @doc """
  Generates a clause that matches a named binding with a nil value.

  ## Examples

      iex> EctoShorts.Generator.named_clause_asts(EctoShorts.Dynamics.Postgres.CommonExpr)
  """
  def named_clause_asts(builder, opts \\ []) do
    bind_op = :as
    binding_alias_var = Macro.var(:binding_alias, opts[:context])
    selected_binding = {bind_op, binding_alias_var}

    specs_to_clauses(builder, selected_binding, opts)
  end

  defp specs_to_clauses(builder, selected_binding, opts) do
    q_var = Macro.var(:q, opts[:context])

    opts
    |> Keyword.get(:directives, ClauseSpec.directives(builder))
    |> List.wrap()
    |> Enum.flat_map(fn key ->
      builder
      |> ClauseSpec.specs_for(key, selected_binding, q_var, opts)
      |> List.wrap()
      |> Enum.map(&quote_def(selected_binding, &1))
    end)
  end

  defp quote_def(
         selected_binding,
         %Blueprint{
           key: key_ast,
           head: head_ast,
           body: body_ast,
           guard: guard_ast
         }
       ) do
    if is_nil(guard_ast) do
      quote do
        def dynamic_expr(
              unquote(selected_binding),
              unquote(key_ast),
              unquote_splicing(head_ast)
            ) do
          unquote(body_ast)
        end
      end
    else
      quote do
        def dynamic_expr(
              unquote(selected_binding),
              unquote(key_ast),
              unquote_splicing(head_ast)
            )
            when unquote(guard_ast) do
          unquote(body_ast)
        end
      end
    end
  end
end
