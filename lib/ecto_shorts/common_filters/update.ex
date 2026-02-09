defmodule EctoShorts.CommonFilters.Update do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.Compiler

  require Ecto.Query
  require EctoShorts.Compiler

  @binding_selector_key :bind
  @binding_selector_modes [:as, :at]

  @doc false
  def build(_schema_source, :update, query, binding_selector, term, opts)
      when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> Enum.reduce(query, fn entry, query_acc ->
      build(nil, :update, query_acc, binding_selector, entry, opts)
    end)
  end

  def build(
        _schema_source,
        :update,
        query,
        binding_selector,
        {@binding_selector_key, bind_params},
        opts
      ) do
    reduce_update_bind(query, binding_selector, bind_params, opts)
  end

  def build(_schema_source, :update, query, binding_selector, term, opts) when is_list(term) do
    if Keyword.keyword?(term) do
      case Enum.split_with(term, fn {k, _} -> k == @binding_selector_key end) do
        {[], entries} ->
          apply_update_expr(query, binding_selector, normalize_update_entries(entries))

        {bind_entries, []} ->
          Enum.reduce(bind_entries, query, fn entry, query_acc ->
            build(nil, :update, query_acc, binding_selector, entry, opts)
          end)

        {bind_entries, entries} ->
          query_with_update =
            apply_update_expr(query, binding_selector, normalize_update_entries(entries))

          Enum.reduce(bind_entries, query_with_update, fn entry, query_acc ->
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

  defp reduce_update_bind(query, binding_selector, bind_params, opts)
       when is_map(bind_params) and not is_struct(bind_params) do
    reduce_update_bind(query, binding_selector, Map.to_list(bind_params), opts)
  end

  defp reduce_update_bind(query, _binding_selector, bind_params, opts)
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
            {binding_target, term}, q2 ->
              build(nil, :update, q2, {binding_mode, binding_target}, term, opts)

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

  defp reduce_update_bind(_query, _binding_selector, bind_params, _opts) do
    raise ArgumentError,
          "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, _target_binding_var, _binding_patterns ->
      defp apply_update_expr(query, unquote(quoted_binding_head), expr) do
        Query.update(query, [unquote_splicing(quoted_binding_body)], ^expr)
      end
  end
end
