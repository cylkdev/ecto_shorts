defmodule EctoShorts.CommonFilters.Select do
  alias Ecto.Query
  alias EctoShorts.Compiler
  alias EctoShorts.Logger
  alias EctoShorts.Utils

  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.Select"

  {target_binding_var, binding_patterns} =
    Compiler.query_binding_contracts(10, __MODULE__)

  def build_query(:select, _source, query, selected_binding, term, _opts) do
    normalized_term = Utils.normalize_params(term)

    query
    |> drop_existing_select()
    |> apply_select_expr(selected_binding, normalized_term)
  end

  def build_query(:select_merge, _source, query, selected_binding, term, _opts) do
    normalized_term = Utils.normalize_params(term)

    apply_select_merge_expr(query, selected_binding, normalized_term)
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp initialize_select_map(query, unquote(quoted_binding_head)) do
      Query.select(query, [unquote_splicing(quoted_binding_body)], %{})
    end

    defp apply_select_expr(query, unquote(quoted_binding_head), true) do
      Query.select(query, [unquote_splicing(quoted_binding_body)], unquote(target_binding_var))
    end

    defp apply_select_expr(query, unquote(quoted_binding_head), {:map, term}) do
      if Keyword.keyword?(term) do
        apply_select_alias_entries(query, unquote(quoted_binding_head), term)
      else
        Query.select(
          query,
          [unquote_splicing(quoted_binding_body)],
          map(unquote(target_binding_var), ^term)
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
        apply_select_alias_entries(query, unquote(quoted_binding_head), term)
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
  end

  defp apply_select_expr(query, _selected_binding, term) do
    Query.select(query, ^term)
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp apply_select_merge_expr(query, unquote(quoted_binding_head), {:map, params})
         when is_map(params) do
      apply_select_merge_expr(query, unquote(quoted_binding_head), {:map, Map.to_list(params)})
    end

    defp apply_select_merge_expr(query, unquote(quoted_binding_head), {:map, list})
         when is_list(list) do
      if Keyword.keyword?(list) do
        apply_select_merge_entries(query, unquote(quoted_binding_head), list)
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
        apply_select_merge_entries(query, unquote(quoted_binding_head), term)
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
      apply_select_merge_entry(query, unquote(quoted_binding_head), field_alias, field_name)
    end
  end

  defp apply_select_merge_expr(query, _selected_binding, term) do
    Query.select_merge(query, ^term)
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp apply_select_merge_entry(
           query,
           unquote(quoted_binding_head),
           field_alias,
           field_name
         )
         when is_atom(field_name) do
      Query.select_merge(
        query,
        [unquote_splicing(quoted_binding_body)],
        %{^field_alias => field(unquote(target_binding_var), ^field_name)}
      )
    end

    defp apply_select_merge_entry(
           query,
           unquote(quoted_binding_head),
           field_alias,
           %Ecto.Query.DynamicExpr{} = dynamic_expr
         ) do
      Query.select_merge(
        query,
        [unquote_splicing(quoted_binding_body)],
        %{^field_alias => ^dynamic_expr}
      )
    end

    defp apply_select_merge_entry(query, unquote(quoted_binding_head), field_alias, value) do
      Query.select_merge(
        query,
        [unquote_splicing(quoted_binding_body)],
        %{^field_alias => ^value}
      )
    end
  end

  defp apply_select_merge_entries(query, selected_binding, entries) do
    Enum.reduce(entries, query, fn {field_alias, field}, query_acc ->
      apply_select_merge_entry(query_acc, selected_binding, field_alias, field)
    end)
  end

  defp apply_select_alias_entries(query, selected_binding, entries) do
    query
    |> initialize_select_map(selected_binding)
    |> apply_select_merge_entries(selected_binding, entries)
  end

  defp drop_existing_select(%Ecto.Query{select: nil} = query), do: query

  defp drop_existing_select(query) do
    Logger.warning(
      @logger_prefix,
      "Replacing existing select expression before applying :select filter"
    )

    Query.exclude(query, :select)
  end
end
