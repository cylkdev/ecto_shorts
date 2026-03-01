defmodule EctoShorts.CommonFilters.Preload do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Builds `:preload` expressions from data-driven params.

  Accepts atoms, lists of atoms, and keyword lists for nested preloads.
  Supports binding-scoped params via the `:bind` key.
  """

  alias Ecto.Query
  alias EctoShorts.CommonFilters.BindingParams
  alias EctoShorts.Compiler
  alias EctoShorts.Logger

  require Ecto.Query
  require EctoShorts.Compiler

  @logger_prefix "EctoShorts.CommonFilters.Preload"
  @binding_selector_key :bind

  def build(schema_source, :preload, query, binding_selector, arg, _opts) do
    reduce_preload(schema_source, query, binding_selector, arg)
  end

  defp reduce_preload(
         schema_source,
         query,
         binding_selector,
         {@binding_selector_key, bind_params}
       ) do
    reduce_preload_bind(schema_source, query, binding_selector, bind_params)
  end

  defp reduce_preload(schema_source, query, binding_selector, values) when is_list(values) do
    if Keyword.keyword?(values) do
      case Enum.split_with(values, fn {k, _} -> k === @binding_selector_key end) do
        {[], entries} ->
          apply_preload_expr(query, binding_selector, entries)

        {bind_entries, []} ->
          Enum.reduce(bind_entries, query, fn entry, query_acc ->
            reduce_preload(schema_source, query_acc, binding_selector, entry)
          end)

        {bind_entries, entries} ->
          Enum.reduce(bind_entries, query, fn {@binding_selector_key, bind_params}, query_acc ->
            reduce_preload_bind(schema_source, query_acc, binding_selector, bind_params, entries)
          end)
      end
    else
      apply_preload_expr(query, binding_selector, values)
    end
  end

  defp reduce_preload(_schema_source, query, binding_selector, key) do
    apply_preload_expr(query, binding_selector, [key])
  end

  defp reduce_preload_bind(_schema_source, query, _binding_selector, bind_params, entries \\ nil) do
    Enum.reduce(BindingParams.normalize_bind_params(bind_params), query, fn {binding_selector, value}, q ->
      if valid_binding?(q, binding_selector) do
        apply_preload_expr(q, binding_selector, value, entries)
      else
        Logger.warning(
          @logger_prefix,
          "unknown bind name `#{inspect(elem(binding_selector, 1))}` in query"
        )

        q
      end
    end)
  end

  defp valid_binding?(_query, {:as, nil}), do: true

  defp valid_binding?(query, {:as, alias}) when is_atom(alias) do
    Query.has_named_binding?(query, alias)
  end

  defp valid_binding?(_query, _binding_selector), do: true

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
