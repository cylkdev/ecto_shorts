defmodule EctoShorts.CommonFilters.Select do
  @moduledoc """
  Builds `:select` and `:select_merge` expressions from data-driven params.

  Accepts field atoms, `{:map, fields}` tuples, `{:struct, fields}` tuples,
  `true` (select entire binding), keyword lists, and maps. Supports
  binding-scoped params via the `:bind` key.
  """

  alias Ecto.Query
  alias EctoShorts.CommonFilters.BindParams
  alias EctoShorts.Compiler

  require EctoShorts.Compiler
  require Ecto.Query

  @binding_selector_key :bind

  @doc "Builds a select expression for the query."
  def build(schema, filter_op, query, binding_selector, term, opts)
      when is_map(term) or is_list(term) do
    if (is_map(term) and not is_struct(term)) or Keyword.keyword?(term) do
      Enum.reduce(term, query, fn entry, updated_query ->
        build(schema, filter_op, updated_query, binding_selector, entry, opts)
      end)
    else
      reduce_select(filter_op, schema, query, binding_selector, term)
    end
  end

  def build(
        schema,
        filter_op,
        query,
        binding_selector,
        {@binding_selector_key, bind_params},
        opts
      ) do
    reduce_select_bind(schema, filter_op, query, binding_selector, bind_params, opts)
  end

  def build(schema, filter_op, query, binding_selector, term, _opts) do
    reduce_select(filter_op, schema, query, binding_selector, term)
  end

  defp reduce_select(:select, schema, query, binding_selector, term) do
    apply_select_expr(schema, query, binding_selector, term)
  end

  defp reduce_select(:select_merge, schema, query, binding_selector, term) do
    apply_select_merge_expr(schema, query, binding_selector, term)
  end

  defp reduce_select_bind(schema, filter_op, query, _binding_selector, bind_params, opts) do
    BindParams.reduce_submodule_bind_params(query, bind_params, fn q, {mode, target}, value ->
      build(schema, filter_op, q, {mode, target}, value, opts)
    end)
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
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
        if (is_map(term) and not is_struct(term)) or Keyword.keyword?(term) do
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

      defp apply_select_merge_expr(
             _schema,
             query,
             unquote(quoted_binding_head),
             {:map, params}
           )
           when is_map(params) do
        select_map = build_select_map(params, unquote(quoted_binding_head))

        Query.select_merge(
          query,
          [unquote_splicing(quoted_binding_body)],
          ^select_map
        )
      end

      defp apply_select_merge_expr(
             _schema,
             query,
             unquote(quoted_binding_head),
             {:map, list}
           )
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

      defp apply_select_merge_expr(schema, query, unquote(quoted_binding_head), term)
           when is_map(term) or is_list(term) do
        if (is_map(term) and not is_struct(term)) or Keyword.keyword?(term) do
          Enum.reduce(term, query, fn {key, value}, updated_query ->
            apply_select_merge_expr(
              schema,
              updated_query,
              unquote(quoted_binding_head),
              {key, value}
            )
          end)
        else
          Query.select_merge(
            query,
            [unquote_splicing(quoted_binding_body)],
            ^term
          )
        end
      end

      defp apply_select_merge_expr(
             _schema,
             query,
             unquote(quoted_binding_head),
             {field_alias, field}
           ) do
        select_map = build_select_map([{field_alias, field}], unquote(quoted_binding_head))

        Query.select_merge(
          query,
          [unquote_splicing(quoted_binding_body)],
          ^select_map
        )
      end

      defp apply_select_merge_expr(_schema, query, unquote(quoted_binding_head), term) do
        Query.select_merge(
          query,
          [unquote_splicing(quoted_binding_body)],
          ^term
        )
      end
  end

  defp build_select_map(enum, binding_selector) do
    Enum.reduce(enum, %{}, fn {field_alias, field}, acc ->
      Map.put(acc, field_alias, apply_dynamic_expr(binding_selector, field))
    end)
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      defp apply_dynamic_expr(unquote(quoted_binding_head), field) do
        Query.dynamic(
          [unquote_splicing(quoted_binding_body)],
          field(unquote(target_binding_var), ^field)
        )
      end
  end
end
