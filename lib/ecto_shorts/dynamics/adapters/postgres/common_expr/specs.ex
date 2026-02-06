defmodule EctoShorts.Dynamics.Adapters.Postgres.CommonExpr.Specs do
  @moduledoc false

  alias EctoShorts.Dynamics.Compiler.AST
  alias EctoShorts.Dynamics.Compiler.ClauseSpec

  @doc false
  def clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    id_values_var = Macro.var(:id_values, context)
    cursor_value_var = Macro.var(:cursor_value, context)
    date_value_var = Macro.var(:date_value, context)

    [
      %{
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
end
