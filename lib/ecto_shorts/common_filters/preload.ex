defmodule EctoShorts.CommonFilters.Preload do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.Compiler

  require Ecto.Query
  require EctoShorts.Compiler

  @binding_selector_key :bind
  @binding_selector_modes [:as, :at]

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

  defp reduce_preload_bind(schema_source, query, binding_selector, bind_params, entries \\ nil)

  defp reduce_preload_bind(schema_source, query, binding_selector, bind_params, entries)
       when is_map(bind_params) and not is_struct(bind_params) do
    reduce_preload_bind(
      schema_source,
      query,
      binding_selector,
      Map.to_list(bind_params),
      entries
    )
  end

  defp reduce_preload_bind(_schema_source, query, _binding_selector, bind_params, entries)
       when is_list(bind_params) do
    if Keyword.keyword?(bind_params) do
      Enum.reduce(bind_params, query, fn
        {binding_mode, scoped_params}, query_acc when binding_mode in @binding_selector_modes ->
          scoped_params =
            case scoped_params do
              value when is_map(value) and not is_struct(value) ->
                Map.to_list(value)

              value when is_list(value) ->
                value

              value ->
                raise ArgumentError,
                      "Expected :bind -> #{inspect(binding_mode)} payload to be a map or keyword list, got: #{inspect(value)}"
            end

          Enum.reduce(scoped_params, query_acc, fn
            {binding_target, value}, q2 ->
              scoped_binding_selector = {binding_mode, binding_target}
              apply_preload_expr(q2, scoped_binding_selector, value, entries)

            entry, _q2 ->
              raise ArgumentError,
                    "Expected :bind -> #{inspect(binding_mode)} entries to be {target, params} tuples, got: #{inspect(entry)}"
          end)

        {binding_mode, _scoped_params}, _query_acc ->
          raise ArgumentError,
                "Expected :bind keys to be one of #{inspect(@binding_selector_modes)}, got: #{inspect(binding_mode)}"
      end)
    else
      raise ArgumentError,
            "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
    end
  end

  defp reduce_preload_bind(_schema_source, _query, _binding_selector, bind_params, _entries) do
    raise ArgumentError,
          "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
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
