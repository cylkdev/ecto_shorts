defmodule EctoShorts.CommonFilters.Update do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.Compiler

  require Ecto.Query
  require EctoShorts.Compiler

  @boolean_directives [:as, :at]

  @doc false
  def build(_schema_source, :update, query, binding_selector, term, opts)
      when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> Enum.reduce(query, fn entry, query_acc ->
      build(nil, :update, query_acc, binding_selector, entry, opts)
    end)
  end

  def build(_schema_source, :update, query, _binding_selector, {binding_mode, params}, opts)
      when binding_mode in @boolean_directives and (is_map(params) or is_list(params)) do
    Enum.reduce(params, query, fn {binding_target, term}, query_acc ->
      build(nil, :update, query_acc, {binding_mode, binding_target}, term, opts)
    end)
  end

  def build(_schema_source, :update, query, binding_selector, term, opts) when is_list(term) do
    if Keyword.keyword?(term) do
      case Enum.split_with(term, fn {k, _} -> k in @boolean_directives end) do
        {[], entries} ->
          apply_update_expr(query, binding_selector, normalize_update_entries(entries))

        {boolean_directives, []} ->
          Enum.reduce(boolean_directives, query, fn entry, query_acc ->
            build(nil, :update, query_acc, binding_selector, entry, opts)
          end)

        {boolean_directives, entries} ->
          query_with_update =
            apply_update_expr(query, binding_selector, normalize_update_entries(entries))

          Enum.reduce(boolean_directives, query_with_update, fn entry, query_acc ->
            build(nil, :update, query_acc, binding_selector, entry, opts)
          end)
      end
    else
      apply_update_expr(query, binding_selector, term)
    end
  end

  def build(_schema_source, :update, query, binding_selector, term, _opts) do
    apply_update_expr(query, binding_selector, term)
  end

  defp normalize_update_entries(entries) when is_list(entries) do
    Enum.map(entries, fn
      {op, value} when is_map(value) and not is_struct(value) ->
        {op, Map.to_list(value)}

      {op, value} ->
        {op, value}
    end)
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, _target_binding_var, _binding_patterns ->
      defp apply_update_expr(query, unquote(quoted_binding_head), expr) do
        Query.update(query, [unquote_splicing(quoted_binding_body)], ^expr)
      end
  end
end
