defmodule EctoShorts.CommonFilters do
  @moduledoc since: "3.0.0"

  alias EctoShorts.CommonSchema
  alias EctoShorts.Adapters.Postgres
  alias EctoShorts.CommonFilters.{OrderBy, Preload, Where}

  @default_selected_binding {:as, nil}

  @order_by_filters [:order_by]
  @query_filters @order_by_filters ++ [:preload]
  @where_filters [:where, :or_where]
  @filters @query_filters ++ @where_filters

  def convert_params_to_filter(source, params, opts) do
    query = CommonSchema.to_query(source)

    params
    |> sort_filters()
    |> Enum.reduce(query, fn {key, value}, query_acc ->
      apply_filters(:where, source, query_acc, @default_selected_binding, {key, value}, opts)
    end)
  end

  defp apply_filters(filter, source, query, _selected_binding, {bind_op, params}, opts)
       when bind_op in [:as, :at] do
    Enum.reduce(params, query, fn {key, value}, query_acc ->
      apply_filters(filter, source, query_acc, {bind_op, key}, value, opts)
    end)
  end

  defp apply_filters(filter, source, query, selected_binding, {key, params}, opts)
       when is_map(params) and not is_struct(params) do
    apply_filters(filter, source, query, selected_binding, {key, Map.to_list(params)}, opts)
  end

  defp apply_filters(filter, source, query, selected_binding, {key, term}, opts) do
    cond do
      key in @where_filters and is_list(term) ->
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

      key in @query_filters ->
        build_query(key, source, query, selected_binding, term, opts)

      true ->
        build_query(filter, source, query, selected_binding, {key, term}, opts)
    end
  end

  defp apply_filters(filter, source, query, selected_binding, term, opts) do
    cond do
      is_map(term) and not is_struct(term) ->
        apply_filters(filter, source, query, selected_binding, Map.to_list(term), opts)

      true ->
        Enum.reduce(term, query, &apply_filters(filter, source, &2, selected_binding, &1, opts))
    end
  end

  defp build_query(filter, source, query, selected_binding, term, opts) do
    case filter do
      order_by_filter when order_by_filter in @order_by_filters ->
        OrderBy.build_query(
          filter,
          source,
          query,
          selected_binding,
          term,
          opts
        )

      :preload ->
        Preload.build_query(
          filter,
          source,
          query,
          selected_binding,
          term,
          opts
        )

      where_filter when where_filter in @where_filters ->
        dyn =
          Postgres.build_dynamic(
            source,
            selected_binding,
            term,
            opts
          )

        Where.build_query(
          where_filter,
          source,
          query,
          selected_binding,
          dyn,
          opts
        )
    end
  end

  defp sort_filters(params) do
    params
    |> Map.to_list()
    |> Enum.sort_by(fn
      {:where, _} -> 0
      {:or_where, _} -> 2
      _ -> 1
    end)
  end
end
