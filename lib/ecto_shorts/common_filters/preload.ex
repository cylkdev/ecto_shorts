defmodule EctoShorts.CommonFilters.Preload do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.Compiler

  require Ecto.Query
  require EctoShorts.Compiler

  @binding_operators [:as, :at]

  def build(schema_source, :preload, query, binding_selector, arg, _opts) do
    do_preload(schema_source, query, binding_selector, arg)
  end

  defp do_preload(schema_source, query, _binding_selector, {binding_op, params})
       when binding_op in @binding_operators do
    Enum.reduce(params, query, fn {binding_target, value}, q2 ->
      do_preload(schema_source, q2, {binding_op, binding_target}, value)
    end)
  end

  defp do_preload(schema_source, query, binding_selector, values) when is_list(values) do
    if Keyword.keyword?(values) do
      case Enum.split_with(values, fn {k, _} -> k in @binding_operators end) do
        {[], entries} ->
          build_preload(query, binding_selector, entries)

        {binding_ops, []} ->
          Enum.reduce(binding_ops, query, fn {binding_op, value}, query_acc ->
            do_preload(schema_source, query_acc, binding_selector, {binding_op, value})
          end)

        {binding_ops, entries} ->
          Enum.reduce(binding_ops, query, fn {binding_op, params}, query_acc ->
            Enum.reduce(params, query_acc, fn {binding_target, assoc_key}, q2 ->
              build_preload(q2, {binding_op, binding_target}, assoc_key, entries)
            end)
          end)
      end
    else
      build_preload(query, binding_selector, values)
    end
  end

  defp do_preload(_schema_source, query, binding_selector, key) do
    build_preload(query, binding_selector, key, nil)
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      defp build_preload(query, unquote(quoted_binding_head), expr) do
        Query.preload(query, [unquote_splicing(quoted_binding_body)], ^expr)
      end

      defp build_preload(query, unquote(quoted_binding_head), assoc_key, nil) do
        Query.preload(
          query,
          [unquote_splicing(quoted_binding_body)],
          [{^assoc_key, unquote(target_binding_var)}]
        )
      end

      defp build_preload(query, unquote(quoted_binding_head), assoc_key, nested) do
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
