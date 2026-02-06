defmodule EctoShorts.CommonFilters.Select do
  @moduledoc false
  alias Ecto.Query
  alias EctoShorts.Utils
  alias EctoShorts.Dynamics.Compiler.BindingHelpers

  require Ecto.Query

  {target_binding_var, binding_patterns} = BindingHelpers.query_var_and_binding_heads()

  @doc "Builds a select expression for the query."
  def build(filter, schema, query, binding_selector, term) when is_map(term) or is_list(term) do
    if key_values?(term) do
      Enum.reduce(term, query, fn entry, updated_query ->
        build(filter, schema, updated_query, binding_selector, entry)
      end)
    else
      apply_expr(filter, schema, query, binding_selector, term)
    end
  end

  def build(filter, schema, query, _binding_selector, {binding_mode, params})
      when binding_mode in [:at, :as] and (is_map(params) or is_list(params)) do
    Enum.reduce(params, query, fn {binding_target, term}, updated_query ->
      build(filter, schema, updated_query, {binding_mode, binding_target}, term)
    end)
  end

  def build(filter, schema, query, binding_selector, term) do
    apply_expr(filter, schema, query, binding_selector, term)
  end

  defp apply_expr(:select, schema, query, binding_selector, term) do
    apply_select_expr(schema, query, binding_selector, term)
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp apply_select_expr(_schema, query, unquote(quoted_binding_head), true) do
      Query.select(query, [unquote_splicing(quoted_binding_body)], unquote(target_binding_var))
    end

    defp apply_select_expr(
           _schema,
           query,
           unquote(quoted_binding_head),
           {:map, params}
         )
         when is_map(params) do
      select_map = build_select_map(params, unquote(quoted_binding_head))

      Query.select(
        query,
        [unquote_splicing(quoted_binding_body)],
        ^select_map
      )
    end

    defp apply_select_expr(
           _schema,
           query,
           unquote(quoted_binding_head),
           {:map, list}
         )
         when is_list(list) do
      if key_values?(list) do
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

    defp apply_select_expr(
           _schema,
           query,
           unquote(quoted_binding_head),
           {:struct, fields}
         )
         when is_list(fields) do
      Query.select(
        query,
        [unquote_splicing(quoted_binding_body)],
        struct(unquote(target_binding_var), ^fields)
      )
    end

    defp apply_select_expr(schema, query, unquote(quoted_binding_head), term)
         when is_map(term) or is_list(term) do
      if key_values?(term) do
        Enum.reduce(term, query, fn {key, value}, updated_query ->
          apply_select_expr(schema, updated_query, unquote(quoted_binding_head), {key, value})
        end)
      else
        Query.select(query, [unquote_splicing(quoted_binding_body)], ^term)
      end
    end

    defp apply_select_expr(_schema, query, unquote(quoted_binding_head), key) do
      Query.select(
        query,
        [unquote_splicing(quoted_binding_body)],
        field(unquote(target_binding_var), ^key)
      )
    end
  end

  defp key_values?(term), do: Utils.key_values?(term)

  defp build_select_map(enum, binding_selector) do
    Enum.reduce(enum, %{}, fn {field_alias, field}, acc ->
      Map.put(acc, field_alias, dynamic_field_expr(binding_selector, field))
    end)
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp dynamic_field_expr(unquote(quoted_binding_head), field) do
      Query.dynamic(
        [unquote_splicing(quoted_binding_body)],
        field(unquote(target_binding_var), ^field)
      )
    end
  end
end
