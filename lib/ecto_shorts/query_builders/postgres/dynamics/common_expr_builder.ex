defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.CommonExprBuilder do
  @moduledoc false

  alias EctoShorts.QueryBuilder.BindingHelpers
  alias EctoShorts.QueryBuilders.Postgres.Dynamics.CommonExprBuilder

  defmacro define_common_exprs(context_ast \\ nil, opts_ast \\ []) do
    context = Macro.expand(context_ast, __CALLER__)
    opts = Macro.expand(opts_ast, __CALLER__)

    {target_binding_var, binding_patterns} =
      BindingHelpers.query_var_and_binding_heads(context, opts)

    asts = CommonExprBuilder.common_exprs_ast(target_binding_var, binding_patterns)

    quote do
      (unquote_splicing(asts))
    end
  end

  def common_exprs_ast(target_binding_var, binding_patterns) do
    for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
      quote do
        def dynamic_field_expr(unquote(quoted_binding_head), :ids, id_values) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), :id) in ^id_values
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), :after, cursor_value) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), :id) > ^cursor_value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), :before, cursor_value) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), :id) < ^cursor_value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), :start_date, date_value) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), :inserted_at) >= ^date_value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), :end_date, date_value) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), :inserted_at) <= ^date_value
          )
        end
      end
    end
  end
end
