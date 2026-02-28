defmodule EctoShorts.CommonFilters.Distinct do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.CommonFilters.BindParams
  alias EctoShorts.Compiler

  require Ecto.Query
  require EctoShorts.Compiler

  @binding_selector_key :bind

  @order_directions [
    :asc,
    :asc_nulls_last,
    :asc_nulls_first,
    :desc,
    :desc_nulls_last,
    :desc_nulls_first
  ]

  @doc false
  def build(_schema_source, :distinct, query, binding_selector, params, _opts) do
    reduce_distinct(query, binding_selector, params)
  end

  defp reduce_distinct(query, binding_selector, {key, params})
       when is_map(params) and not is_struct(params) do
    reduce_distinct(query, binding_selector, {key, Map.to_list(params)})
  end

  defp reduce_distinct(query, binding_selector, {@binding_selector_key, bind_params}) do
    reduce_distinct_bind(query, binding_selector, bind_params)
  end

  defp reduce_distinct(query, binding_selector, {key, value}) do
    apply_distinct_expr(query, binding_selector, {key, value})
  end

  defp reduce_distinct(query, binding_selector, params)
       when is_map(params) and not is_struct(params) do
    reduce_distinct(query, binding_selector, Map.to_list(params))
  end

  defp reduce_distinct(query, binding_selector, entries) when is_list(entries) do
    if Keyword.keyword?(entries) do
      case Enum.split_with(entries, fn {k, _} -> k === @binding_selector_key end) do
        {[], distinct_entries} ->
          Enum.reduce(distinct_entries, query, fn {key, value}, q ->
            reduce_distinct(q, binding_selector, {key, value})
          end)

        {bind_entries, []} ->
          Enum.reduce(bind_entries, query, fn entry, query_acc ->
            reduce_distinct(query_acc, binding_selector, entry)
          end)

        {bind_entries, distinct_entries} ->
          query_with_distinct =
            Enum.reduce(distinct_entries, query, fn {key, value}, q ->
              reduce_distinct(q, binding_selector, {key, value})
            end)

          Enum.reduce(bind_entries, query_with_distinct, fn entry, query_acc ->
            reduce_distinct(query_acc, binding_selector, entry)
          end)
      end
    else
      apply_distinct_expr(query, binding_selector, entries)
    end
  end

  defp reduce_distinct(query, binding_selector, expr) do
    apply_distinct_expr(query, binding_selector, expr)
  end

  defp reduce_distinct_bind(query, _binding_selector, bind_params) do
    BindParams.reduce_submodule_bind_params(query, bind_params, fn q, {mode, target}, value ->
      reduce_distinct(q, {mode, target}, value)
    end)
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      defp apply_distinct_expr(query, unquote(quoted_binding_head), expr) when is_boolean(expr) do
        Query.distinct(query, ^expr)
      end

      defp apply_distinct_expr(query, unquote(quoted_binding_head), field_name)
           when is_atom(field_name) do
        Query.distinct(
          query,
          [unquote_splicing(quoted_binding_body)],
          field(unquote(target_binding_var), ^field_name)
        )
      end

      defp apply_distinct_expr(query, unquote(quoted_binding_head), entries)
           when is_list(entries) do
        distinct_exprs =
          Enum.map(entries, fn
            {dir, field_name} when dir in @order_directions and is_atom(field_name) ->
              {dir,
               Query.dynamic(
                 [unquote_splicing(quoted_binding_body)],
                 field(unquote(target_binding_var), ^field_name)
               )}

            field_name when is_atom(field_name) ->
              {:asc,
               Query.dynamic(
                 [unquote_splicing(quoted_binding_body)],
                 field(unquote(target_binding_var), ^field_name)
               )}

            other ->
              other
          end)

        Query.distinct(query, ^distinct_exprs)
      end
  end

  defp apply_distinct_expr(query, _binding_selector, expr) do
    Query.distinct(query, ^expr)
  end
end
