defmodule EctoShorts.CommonFilters do
  @moduledoc since: "3.0.0"

  alias EctoShorts.CommonSchema
  alias EctoShorts.CommonFilters.Builder
  alias EctoShorts.Utils

  @binding_directive [:as, :at]

  def convert_params_to_filter(source, params, opts) do
    query = CommonSchema.to_query(source)

    params
    |> Utils.map_to_keyword()
    |> sort_filters()
    |> Enum.reduce(query, fn {key, value}, query_acc ->
      apply_filters(:where, source, query_acc, {:as, nil}, {key, value}, opts)
    end)
  end

  defp apply_filters(filter, source, query, selected_binding, {key, term}, opts) do
    cond do
      key in @binding_directive ->
        Enum.reduce(term, query, fn {inner_key, inner_value}, query_acc ->
          apply_filters(
            filter,
            source,
            query_acc,
            {key, inner_key},
            inner_value,
            opts
          )
        end)

      key in Builder.filter_group(:predicate) ->
        Enum.reduce(term, query, fn {inner_key, inner_value}, query_acc ->
          apply_filters(
            key,
            source,
            query_acc,
            selected_binding,
            {inner_key, inner_value},
            opts
          )
        end)

      key in Builder.filter_group(:post_aggregate) ->
        Enum.reduce(term, query, fn {inner_key, inner_value}, query_acc ->
          apply_filters(
            key,
            source,
            query_acc,
            selected_binding,
            {inner_key, inner_value},
            opts
          )
        end)

      key in Builder.filters() ->
        build_query(key, source, query, selected_binding, term, opts)

      true ->
        build_query(filter, source, query, selected_binding, {key, term}, opts)
    end
  end

  defp apply_filters(filter, source, query, selected_binding, term, opts) do
    Enum.reduce(term, query, &apply_filters(filter, source, &2, selected_binding, &1, opts))
  end

  defp build_query(filter, source, query, selected_binding, term, opts) do
    module = opts[:query_builder] || EctoShorts.Config.query_builder() || EctoShorts.CommonFilters.Builder
    module.build_query(filter, source, query, selected_binding, term, opts)
  end

  defp sort_filters(params) do
    where_filters = Keyword.take(params, [:where])
    or_where_filters = Keyword.take(params, [:or_where])
    terminal_filters = Enum.filter(params, fn {key, _val} -> key in [:last, :subquery] end)
    other_filters = Keyword.drop(params, [:where, :or_where, :last, :subquery])

    where_filters
    |> Kernel.++(other_filters)
    |> Kernel.++(or_where_filters)
    |> Kernel.++(terminal_filters)
  end
end
