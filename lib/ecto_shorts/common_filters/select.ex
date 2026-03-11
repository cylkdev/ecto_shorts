defmodule EctoShorts.CommonFilters.Select do
  alias Ecto.Query
  alias EctoShorts.Compiler
  alias EctoShorts.Utils

  require Ecto.Query

  {target_binding_var, binding_patterns} =
    Compiler.query_binding_contracts(__MODULE__, positions: 10)

  def build_query(filter, _source, query, selected_binding, term, _opts) do
    normalized_term = Utils.map_to_list(term)

    case filter do
      :select -> apply_select_expr(query, selected_binding, normalized_term)
      :select_merge -> apply_select_merge_expr(query, selected_binding, normalized_term)
    end
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp apply_select_expr(query, unquote(quoted_binding_head), true) do
      Query.select(query, [unquote_splicing(quoted_binding_body)], unquote(target_binding_var))
    end

    defp apply_select_expr(query, unquote(quoted_binding_head), {:map, params})
         when is_map(params) do
      apply_select_expr(query, unquote(quoted_binding_head), {:map, Map.to_list(params)})
    end

    defp apply_select_expr(query, unquote(quoted_binding_head), {:map, list})
         when is_list(list) do
      if Keyword.keyword?(list) do
        select_map = build_select_map(list, unquote(quoted_binding_head))

        Query.select(
          query,
          [unquote_splicing(quoted_binding_body)],
          ^select_map
        )
      else
        Query.select(
          query,
          [unquote_splicing(quoted_binding_body)],
          map(unquote(target_binding_var), ^list)
        )
      end
    end

    defp apply_select_expr(query, unquote(quoted_binding_head), {:struct, fields})
         when is_list(fields) do
      Query.select(
        query,
        [unquote_splicing(quoted_binding_body)],
        struct(unquote(target_binding_var), ^fields)
      )
    end

    defp apply_select_expr(query, unquote(quoted_binding_head), term) when is_list(term) do
      if Keyword.keyword?(term) do
        select_map = build_select_map(term, unquote(quoted_binding_head))

        Query.select(
          query,
          [unquote_splicing(quoted_binding_body)],
          ^select_map
        )
      else
        Query.select(
          query,
          [unquote_splicing(quoted_binding_body)],
          ^term
        )
      end
    end

    defp apply_select_expr(query, unquote(quoted_binding_head), field_name)
         when is_atom(field_name) do
      Query.select(
        query,
        [unquote_splicing(quoted_binding_body)],
        field(unquote(target_binding_var), ^field_name)
      )
    end

    defp apply_select_merge_expr(query, unquote(quoted_binding_head), {:map, params})
         when is_map(params) do
      apply_select_merge_expr(query, unquote(quoted_binding_head), {:map, Map.to_list(params)})
    end

    defp apply_select_merge_expr(query, unquote(quoted_binding_head), {:map, list})
         when is_list(list) do
      if Keyword.keyword?(list) do
        select_map = build_select_map(list, unquote(quoted_binding_head))

        Query.select_merge(
          query,
          [unquote_splicing(quoted_binding_body)],
          ^select_map
        )
      else
        Query.select_merge(
          query,
          [unquote_splicing(quoted_binding_body)],
          map(unquote(target_binding_var), ^list)
        )
      end
    end

    defp apply_select_merge_expr(query, unquote(quoted_binding_head), term) when is_list(term) do
      if Keyword.keyword?(term) do
        select_map = build_select_map(term, unquote(quoted_binding_head))

        Query.select_merge(
          query,
          [unquote_splicing(quoted_binding_body)],
          ^select_map
        )
      else
        Query.select_merge(
          query,
          [unquote_splicing(quoted_binding_body)],
          ^term
        )
      end
    end

    defp apply_select_merge_expr(query, unquote(quoted_binding_head), {field_alias, field_name})
         when is_atom(field_alias) do
      select_map = build_select_map([{field_alias, field_name}], unquote(quoted_binding_head))

      Query.select_merge(
        query,
        [unquote_splicing(quoted_binding_body)],
        ^select_map
      )
    end

    defp compose(unquote(quoted_binding_head), field_name) when is_atom(field_name) do
      Query.dynamic(
        [unquote_splicing(quoted_binding_body)],
        field(unquote(target_binding_var), ^field_name)
      )
    end

    defp compose(unquote(quoted_binding_head), %Ecto.Query.DynamicExpr{} = dynamic_expr) do
      dynamic_expr
    end
  end

  defp apply_select_expr(query, _selected_binding, term) do
    Query.select(query, ^term)
  end

  defp apply_select_merge_expr(query, _selected_binding, term) do
    Query.select_merge(query, ^term)
  end

  defp compose(_selected_binding, term), do: term

  defp build_select_map(enum, selected_binding) do
    Enum.reduce(enum, %{}, fn {field_alias, field}, acc ->
      Map.put(acc, field_alias, compose(selected_binding, field))
    end)
  end
end
