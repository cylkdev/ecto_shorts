defmodule EctoShorts.CommonFilters.Preload do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.Compiler

  require Ecto.Query
  require EctoShorts.Compiler

  @binding_operators [:as, :at]

  def build(_schema_source, :preload, query, binding_selector, term, _opts) do
    do_preload(query, binding_selector, term)
  end

  defp do_preload(query, binding_selector, {binding_operator, value})
       when binding_operator in @binding_operators do
    case value do
      {binding_target, params} ->
        Enum.reduce(params, query, fn {assoc_key, expr}, q2 ->
          do_preload(q2, {binding_operator, binding_target}, {assoc_key, expr})
        end)

      params ->
        Enum.reduce(params, query, fn {binding_target, next_value}, q2 ->
          do_preload(
            q2,
            binding_selector,
            {binding_operator, {binding_target, next_value}}
          )
        end)
    end
  end

  defp do_preload(query, binding_selector, {assoc_key, expr}) do
    build_preload(query, binding_selector, assoc_key, expr)
  end

  defp do_preload(query, binding_selector, term) do
    if is_map(term) or Keyword.keyword?(term) do
      Enum.reduce(term, query, fn {key, value}, q2 ->
        do_preload(q2, binding_selector, {key, value})
      end)
    else
      build_preload(query, binding_selector, term)
    end
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      defp build_preload(query, unquote(quoted_binding_head), expr) do
        Query.preload(
          query,
          [unquote_splicing(quoted_binding_body)],
          ^expr
        )
      end

      defp build_preload(query, unquote(quoted_binding_head), assoc, nil) do
        Query.preload(
          query,
          [unquote_splicing(quoted_binding_body)],
          [{^assoc, unquote(target_binding_var)}]
        )
      end

      defp build_preload(query, unquote(quoted_binding_head), assoc, nested) do
        if Keyword.keyword?(nested) do
          Enum.reduce(nested, query, fn nested_entry, query_acc ->
            Query.preload(
              query_acc,
              [unquote_splicing(quoted_binding_body)],
              [{^assoc, {unquote(target_binding_var), ^nested_entry}}]
            )
          end)
        else
          Query.preload(
            query,
            [unquote_splicing(quoted_binding_body)],
            [{^assoc, {unquote(target_binding_var), ^nested}}]
          )
        end
      end
  end
end
