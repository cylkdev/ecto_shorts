defmodule EctoShorts.CommonFilters do
  @moduledoc since: "3.0.0"

  alias EctoShorts.CommonQuery
  alias EctoShorts.CommonSchema
  alias EctoShorts.Config
  alias EctoShorts.CommonFilters.API

  @logger_prefix "EctoShorts.CommonFilters"

  @binding_operator [:as, :at]

  @behaviour EctoShorts.Adapter.QueryBuilder

  def convert_params_to_filter(source, params, opts) do
    query = CommonSchema.to_query(source)
    sorter = opts[:sorter] || (&sort_filter_params/1)

    params
    |> to_keyword()
    |> sorter.()
    |> Enum.reduce(query, fn {key, value}, query_acc ->
      apply_filters(:where, source, query_acc, {:as, nil}, {key, value}, opts)
    end)
  end

  defp apply_filters(filter, source, query, selected_binding, {key, term}, opts) do
    cond do
      key in @binding_operator ->
        Enum.reduce(term, query, fn {inner_key, inner_value}, query_acc ->
          apply_filters(
            filter,
            source,
            query_acc,
            resolve_binding_selector(query_acc, key, inner_key),
            inner_value,
            opts
          )
        end)

      key in API.filter_group(:predicate) ->
        reduce_filter_group_or_build(key, source, query, selected_binding, term, opts)

      key in API.filter_group(:post_aggregate) ->
        reduce_filter_group_or_build(key, source, query, selected_binding, term, opts)

      association_filter?(source, key, term) ->
        query
        |> ensure_association_binding(source, key, opts)
        |> reduce_association_filters(filter, source, key, term, opts)

      key in API.filters() ->
        build_query(key, source, query, selected_binding, term, opts)

      key == :and ->
        if filter_group_list?(term) do
          Enum.reduce(term, query, fn entry, query_acc ->
            apply_filters(filter, source, query_acc, selected_binding, to_keyword(entry), opts)
          end)
        else
          Enum.reduce(to_keyword(term), query, fn {inner_key, inner_value}, query_acc ->
            apply_filters(filter, source, query_acc, selected_binding, {inner_key, inner_value}, opts)
          end)
        end

      key == :or ->
        if filter_group_list?(term) do
          Enum.reduce(term, query, fn entry, query_acc ->
            apply_filters(:or_where, source, query_acc, selected_binding, to_keyword(entry), opts)
          end)
        else
          Enum.reduce(to_keyword(term), query, fn {inner_key, inner_value}, query_acc ->
            or_entries(source, query_acc, selected_binding, inner_key, inner_value, opts)
          end)
        end

      true ->
        build_query(filter, source, query, selected_binding, {key, term}, opts)
    end
  end

  defp apply_filters(filter, source, query, selected_binding, term, opts) do
    Enum.reduce(term, query, &apply_filters(filter, source, &2, selected_binding, &1, opts))
  end

  defp reduce_filter_group_or_build(filter, source, query, selected_binding, term, opts) do
    cond do
      filter_group_list?(term) ->
        Enum.reduce(term, query, fn entry, query_acc ->
          apply_filters(filter, source, query_acc, selected_binding, to_keyword(entry), opts)
        end)

      reducible_filter_entries?(term) ->
        Enum.reduce(to_keyword(term), query, fn {inner_key, inner_value}, query_acc ->
          apply_filters(
            filter,
            source,
            query_acc,
            selected_binding,
            {inner_key, inner_value},
            opts
          )
        end)

      true ->
        build_query(filter, source, query, selected_binding, term, opts)
    end
  end

  defp or_entries(source, query, selected_binding, key, value, opts) do
    build_query(:or_where, source, query, selected_binding, {key, value}, opts)
  end

  defp resolve_binding_selector(_query, :at, :first), do: {:at, 1}
  defp resolve_binding_selector(query, :at, :last), do: {:at, CommonQuery.query_binding_count(query)}
  defp resolve_binding_selector(_query, key, inner_key), do: {key, inner_key}

  defp reduce_association_filters(query, filter, source, key, term, opts) do
    Enum.reduce(to_keyword(term), query, fn {inner_key, inner_value}, query_acc ->
      apply_filters(filter, source, query_acc, {:as, key}, {inner_key, inner_value}, opts)
    end)
  end

  defp ensure_association_binding(query, source, key, opts) do
    build_query(
      :with_named_binding,
      source,
      query,
      {:as, nil},
      %{key => %{join: [association: [source: key, as: key]]}},
      opts
    )
  end

  defp to_keyword(map) when is_map(map) and not is_struct(map) do
    map |> Map.to_list() |> to_keyword()
  end

  defp to_keyword([]), do: []

  defp to_keyword([head | tail]), do: [to_keyword(head) | to_keyword(tail)]

  defp to_keyword({k, v}), do: {k, to_keyword(v)}

  defp to_keyword(term), do: term

  defp reducible_filter_entries?(term) do
    (is_map(term) and not is_struct(term)) or Keyword.keyword?(term)
  end

  defp filter_group_list?(term) do
    is_list(term) and not Keyword.keyword?(term) and
      Enum.all?(term, fn e -> (is_map(e) and not is_struct(e)) or Keyword.keyword?(e) end)
  end

  defp association_filter?(source, key, term) do
    reducible_filter_entries?(term) and
      key in (CommonSchema.get_schema_reflection(source, :associations) || [])
  end

  @impl EctoShorts.Adapter.QueryBuilder
  @doc false
  def build_query(filter, source, query, selected_binding, term, opts) do
    case opts[:query_builder] || Config.query_builder() do
      nil ->
        API.build_query(filter, source, query, selected_binding, term, opts)

      module when is_atom(module) ->
        if function_exported?(module, :build_query, 6) do
          module.build_query(filter, source, query, selected_binding, term, opts)
        else
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Module does not export the required function build_query/6: #{inspect(module)}"
          )

          query
        end

      term ->
        raise ArgumentError, "Expect :query_builder option to a module, got: #{inspect(term)}"
    end
  end

  defp sort_filter_params(params) do
    where_filters = Enum.filter(params, fn {key, _val} -> key == :where end)
    or_where_filters = Enum.filter(params, fn {key, _val} -> key == :or_where end)
    terminal_filters = Enum.filter(params, fn {key, _val} -> key in [:last, :subquery] end)

    other_filters =
      Enum.filter(params, fn {key, _val} -> key not in [:where, :or_where, :last, :subquery] end)

    where_filters
    |> Kernel.++(other_filters)
    |> Kernel.++(or_where_filters)
    |> Kernel.++(terminal_filters)
  end
end
