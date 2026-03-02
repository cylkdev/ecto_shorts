defmodule EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr.Specs.LikeIlike do
  @moduledoc since: "3.0.0"
  @moduledoc false

  @behaviour EctoShorts.Compiler.ClauseSpecProvider

  alias EctoShorts.Compiler.AST
  alias EctoShorts.Compiler.ClauseSpec

  @doc false
  @impl true
  def clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    like_ilike_specs(context, binding_head_ast, target_binding_var, binding_body_asts)
  end

  @doc false
  def like_ilike_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)
    field_ast = AST.field_ast(target_binding_var, key_var)

    [
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:ilike, unquote(value_var)}}),
        body:
          quote do
            patterns = unquote(AST.normalize_patterns_ast(value_var))

            unquote(AST.dynamic_ast(binding_body_asts, quote do
              fragment("NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n", unquote(field_ast), ^patterns)
            end))
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:like, unquote(value_var)}}),
        body:
          quote do
            patterns = unquote(AST.normalize_patterns_ast(value_var))

            unquote(AST.dynamic_ast(binding_body_asts, quote do
              fragment("NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t LIKE ANY (?)\n)\n", unquote(field_ast), ^patterns)
            end))
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:ilike, unquote(value_var)}),
        body:
          quote do
            patterns = unquote(AST.normalize_patterns_ast(value_var))

            unquote(AST.dynamic_ast(binding_body_asts, quote do
              fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n", unquote(field_ast), ^patterns)
            end))
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:like, unquote(value_var)}),
        body:
          quote do
            patterns = unquote(AST.normalize_patterns_ast(value_var))

            unquote(AST.dynamic_ast(binding_body_asts, quote do
              fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t LIKE ANY (?)\n)\n", unquote(field_ast), ^patterns)
            end))
          end
      }
    ]
  end
end
