defmodule EctoShorts.CommonFilters.Stages.Selects do
  @moduledoc false
  alias Ecto.Query
  alias EctoShorts.Utils

  require Ecto.Query

  {target_binding_var, binding_patterns} =
    EctoShorts.Dynamics.BindingHelpers.query_var_and_binding_heads()

  @doc "Builds a select expression for the query."
  def build(filter, schema, query, binding_selector, term) when is_map(term) or is_list(term) do
    if Utils.key_values?(term) do
      Enum.reduce(term, query, fn value, updated_query ->
        build(filter, schema, updated_query, binding_selector, value)
      end)
    else
      apply_expr(filter, schema, query, binding_selector, term)
    end
  end

  def build(filter, schema, query, _binding_selector, {bind_op, params})
      when bind_op in [:at, :as] do
    Enum.reduce(params, query, fn {binding_target, term}, updated_query ->
      build(filter, schema, updated_query, {bind_op, binding_target}, term)
    end)
  end

  def build(filter, schema, query, binding_selector, term) do
    apply_expr(filter, schema, query, binding_selector, term)
  end

  defp apply_expr(filter, schema, query, binding_selector, term) do
    case filter do
      :select ->
        apply_select_expr(schema, query, binding_selector, term)
        # :select_merge -> apply_select_merge_expr(schema, query, binding_selector, term)
    end
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    def apply_select_expr(_schema, query, unquote(quoted_binding_head), true) do
      Query.select(query, [unquote_splicing(quoted_binding_body)], unquote(target_binding_var))
    end

    def apply_select_expr(_schema, query, unquote(quoted_binding_head), {:map, params})
        when is_map(params) do
      select_map = build_select_map(params, unquote(quoted_binding_head))

      Query.select(
        query,
        [unquote_splicing(quoted_binding_body)],
        ^select_map
      )
    end

    def apply_select_expr(_schema, query, unquote(quoted_binding_head), {:map, list})
        when is_list(list) do
      if Utils.key_values?(list) do
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

    def apply_select_expr(_schema, query, unquote(quoted_binding_head), {:struct, fields})
        when is_list(fields) do
      Query.select(
        query,
        [unquote_splicing(quoted_binding_body)],
        struct(unquote(target_binding_var), ^fields)
      )
    end

    def apply_select_expr(schema, query, unquote(quoted_binding_head), term)
        when is_map(term) or is_list(term) do
      if Utils.key_values?(term) do
        Enum.reduce(term, query, fn {key, value}, updated_query ->
          apply_select_expr(schema, updated_query, unquote(quoted_binding_head), {key, value})
        end)
      else
        Query.select(query, [unquote_splicing(quoted_binding_body)], ^term)
      end
    end

    def apply_select_expr(_schema, query, unquote(quoted_binding_head), key) do
      Query.select(
        query,
        [unquote_splicing(quoted_binding_body)],
        field(unquote(target_binding_var), ^key)
      )
    end
  end

  def build_select_map(enum, binding_head) do
    Enum.reduce(enum, %{}, fn {field_alias, field}, acc ->
      Map.put(acc, field_alias, dynamic_field_expr(binding_head, field))
    end)
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    def dynamic_field_expr(unquote(quoted_binding_head), field) do
      Query.dynamic(
        [unquote_splicing(quoted_binding_body)],
        field(unquote(target_binding_var), ^field)
      )
    end
  end
end
