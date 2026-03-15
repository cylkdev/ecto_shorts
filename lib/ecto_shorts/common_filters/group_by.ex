defmodule EctoShorts.CommonFilters.GroupBy do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.QueryBinding

  require Ecto.Query

  {target_binding_var, binding_patterns} =
    QueryBinding.query_binding_contracts(__MODULE__)

  def build_query(:group_by, _source, query, selected_binding, params, _opts) do
    build_group_by(query, selected_binding, params)
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp build_group_by(query, unquote(quoted_binding_head), field_name)
         when is_atom(field_name) do
      Query.group_by(
        query,
        [unquote_splicing(quoted_binding_body)],
        field(unquote(target_binding_var), ^field_name)
      )
    end

    defp build_group_by(query, unquote(quoted_binding_head), entries) when is_list(entries) do
      group_by_exprs =
        Enum.map(entries, fn
          field_name when is_atom(field_name) ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              field(unquote(target_binding_var), ^field_name)
            )

          %Ecto.Query.DynamicExpr{} = dynamic_expr ->
            dynamic_expr

          other ->
            other
        end)

      Query.group_by(query, ^group_by_exprs)
    end
  end

  defp build_group_by(query, _selected_binding, expr) do
    Query.group_by(query, ^expr)
  end
end
