defmodule EctoShorts.CommonFilters.Preload do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.Compiler

  require Ecto.Query
  require EctoShorts.Compiler

  @boolean_directives [:as, :at]

  def build(schema_source, :preload, query, binding_selector, arg, _opts) do
    reduce_preload(schema_source, query, binding_selector, arg)
  end

  defp reduce_preload(schema_source, query, _binding_selector, {boolean_directive, params})
       when boolean_directive in @boolean_directives do
    Enum.reduce(params, query, fn {binding_target, value}, q2 ->
      reduce_preload(schema_source, q2, {boolean_directive, binding_target}, value)
    end)
  end

  defp reduce_preload(schema_source, query, binding_selector, values) when is_list(values) do
    if Keyword.keyword?(values) do
      case Enum.split_with(values, fn {k, _} -> k in @boolean_directives end) do
        {[], entries} ->
          apply_preload_expr(query, binding_selector, entries)

        {boolean_directives, []} ->
          Enum.reduce(boolean_directives, query, fn {boolean_directive, value}, query_acc ->
            reduce_preload(schema_source, query_acc, binding_selector, {boolean_directive, value})
          end)

        {boolean_directives, entries} ->
          Enum.reduce(boolean_directives, query, fn {boolean_directive, params}, query_acc ->
            Enum.reduce(params, query_acc, fn {binding_target, assoc_key}, q2 ->
              apply_preload_expr(q2, {boolean_directive, binding_target}, assoc_key, entries)
            end)
          end)
      end
    else
      apply_preload_expr(query, binding_selector, values)
    end
  end

  defp reduce_preload(_schema_source, query, binding_selector, key) do
    apply_preload_expr(query, binding_selector, key, nil)
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      defp apply_preload_expr(query, unquote(quoted_binding_head), expr) do
        Query.preload(query, [unquote_splicing(quoted_binding_body)], ^expr)
      end

      defp apply_preload_expr(query, unquote(quoted_binding_head), assoc_key, nil) do
        Query.preload(
          query,
          [unquote_splicing(quoted_binding_body)],
          [{^assoc_key, unquote(target_binding_var)}]
        )
      end

      defp apply_preload_expr(query, unquote(quoted_binding_head), assoc_key, nested) do
        if Keyword.keyword?(nested) do
          Enum.reduce(nested, query, fn nested_entry, query_acc ->
            Query.preload(
              query_acc,
              [unquote_splicing(quoted_binding_body)],
              [{^assoc_key, {unquote(target_binding_var), ^nested_entry}}]
            )
          end)
        else
          Query.preload(
            query,
            [unquote_splicing(quoted_binding_body)],
            [{^assoc_key, {unquote(target_binding_var), ^nested}}]
          )
        end
      end
  end
end
