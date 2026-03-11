defmodule EctoShorts.CommonFilters.OrderBy do
  alias EctoShorts.Compiler
  alias Ecto.Query

  require Ecto.Query

  @order_directions [
    :asc,
    :asc_nulls_last,
    :asc_nulls_first,
    :desc,
    :desc_nulls_last,
    :desc_nulls_first
  ]

  {target_binding_var, binding_patterns} =
    Compiler.query_binding_contracts(__MODULE__, positions: 10)

  def build_query(:order_by, _source, query, selected_binding, params, _opts) do
    build_order_by(query, selected_binding, params)
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp build_order_by(query, unquote(quoted_binding_head), field_name)
         when is_atom(field_name) do
      Query.order_by(
        query,
        [unquote_splicing(quoted_binding_body)],
        desc: field(unquote(target_binding_var), ^field_name)
      )
    end

    defp build_order_by(query, unquote(quoted_binding_head), {dir, field_name})
         when dir in @order_directions and is_atom(field_name) do
      Query.order_by(
        query,
        [unquote_splicing(quoted_binding_body)],
        [{^dir, field(unquote(target_binding_var), ^field_name)}]
      )
    end

    defp build_order_by(query, unquote(quoted_binding_head), entries) when is_list(entries) do
      order_exprs =
        Enum.map(entries, fn
          {dir, field_name} when dir in @order_directions and is_atom(field_name) ->
            dyn =
              Query.dynamic(
                [unquote_splicing(quoted_binding_body)],
                field(unquote(target_binding_var), ^field_name)
              )

            {dir, dyn}

          field_name when is_atom(field_name) ->
            dyn =
              Query.dynamic(
                [unquote_splicing(quoted_binding_body)],
                field(unquote(target_binding_var), ^field_name)
              )

            {:desc, dyn}

          %Ecto.Query.DynamicExpr{} = dynamic_expr ->
            dynamic_expr

          other ->
            other
        end)

      Query.order_by(query, ^order_exprs)
    end
  end

  defp build_order_by(query, _selected_binding, expr) do
    Query.order_by(query, ^expr)
  end
end
