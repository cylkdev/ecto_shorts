defmodule EctoShorts.CommonFilters.Distinct do
  alias EctoShorts.QueryBindings
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
    QueryBindings.query_binding_contracts(__MODULE__)

  def build_query(:distinct, _source, query, selected_binding, params, _opts) do
    build_distinct(query, selected_binding, params)
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp build_distinct(query, unquote(quoted_binding_head), expr) when is_boolean(expr) do
      Query.distinct(query, ^expr)
    end

    defp build_distinct(query, unquote(quoted_binding_head), field_name)
         when is_atom(field_name) do
      Query.distinct(
        query,
        [unquote_splicing(quoted_binding_body)],
        field(unquote(target_binding_var), ^field_name)
      )
    end

    defp build_distinct(query, unquote(quoted_binding_head), entries) when is_list(entries) do
      distinct_exprs =
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

            {:asc, dyn}

          %Ecto.Query.DynamicExpr{} = dynamic_expr ->
            dynamic_expr

          other ->
            other
        end)

      Query.distinct(query, ^distinct_exprs)
    end
  end

  defp build_distinct(query, _selected_binding, expr) do
    Query.distinct(query, ^expr)
  end
end
