defmodule EctoShorts.CommonFilters do
  @moduledoc since: "3.0.0"

  alias EctoShorts.CommonSchema
  alias EctoShorts.Adapters.Postgres

  alias EctoShorts.CommonFilters.{
    Distinct,
    Exclude,
    First,
    GroupBy,
    Having,
    Join,
    Last,
    Limit,
    Lock,
    Offset,
    OrderBy,
    Preload,
    PutQueryPrefix,
    RecursiveCtes,
    SetOperation,
    SubQuery,
    Update,
    WithNamedBinding,
    Where
  }

  @default_selected_binding {:as, nil}

  @distinct_filters [:distinct]
  @first_filters [:first]
  @group_by_filters [:group_by]
  @having_filters [:having, :or_having]
  @join_filters [:join]
  @last_filters [:last]
  @order_by_filters [:order_by, :prepend_order_by, :reverse_order]
  @where_filters [:where, :or_where]
  @preload_filters [:preload]
  @put_query_prefix_filters [:put_query_prefix]
  @recursive_ctes_filters [:recursive_ctes]
  @set_operation_filters [:except, :except_all, :intersect, :intersect_all, :union, :union_all]
  @subquery_filters [:subquery]
  @exclude_filters [:exclude]
  @lock_filters [:lock]
  @limit_filters [:limit]
  @offset_filters [:offset]
  @update_filters [:update]
  @with_named_binding_filters [:with_named_binding]
  @query_filters Enum.concat([
                   @distinct_filters,
                   @first_filters,
                   @group_by_filters,
                   @having_filters,
                   @join_filters,
                   @last_filters,
                   @order_by_filters,
                   @preload_filters,
                   @put_query_prefix_filters,
                   @recursive_ctes_filters,
                   @set_operation_filters,
                   @subquery_filters,
                   @exclude_filters,
                   @lock_filters,
                   @limit_filters,
                   @offset_filters,
                   @update_filters,
                   @with_named_binding_filters,
                   @where_filters
                 ])

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

      key in @having_filters and is_list(term) ->
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

  defp build_query(filter, source, query, selected_binding, term, opts) when filter in @distinct_filters do
    Distinct.build_query(
      filter,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(:first, source, query, selected_binding, term, opts) do
    First.build_query(
      :first,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(:last, source, query, selected_binding, term, opts) do
    Last.build_query(
      :last,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(:join, source, query, selected_binding, term, opts) do
    Join.build_query(
      :join,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(filter, source, query, selected_binding, term, opts) when filter in @group_by_filters do
    GroupBy.build_query(
      filter,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(filter, source, query, selected_binding, term, opts) when filter in @having_filters do
    dyn =
      if is_struct(term, Ecto.Query.DynamicExpr) do
        term
      else
        Postgres.build_dynamic(
          source,
          selected_binding,
          term,
          opts
        )
      end

    Having.build_query(
      filter,
      source,
      query,
      selected_binding,
      dyn,
      opts
    )
  end

  defp build_query(filter, source, query, selected_binding, term, opts) when filter in @order_by_filters do
    OrderBy.build_query(
      filter,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(:preload, source, query, selected_binding, term, opts) do
    Preload.build_query(
      :preload,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(:subquery, source, query, selected_binding, term, opts) do
    SubQuery.build_query(
      :subquery,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(:put_query_prefix, source, query, selected_binding, term, opts) do
    PutQueryPrefix.build_query(
      :put_query_prefix,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(:recursive_ctes, source, query, selected_binding, term, opts) do
    RecursiveCtes.build_query(
      :recursive_ctes,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(filter, source, query, selected_binding, term, opts)
       when filter in @set_operation_filters do
    SetOperation.build_query(
      filter,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(:exclude, source, query, selected_binding, term, opts) do
    Exclude.build_query(
      :exclude,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(:lock, source, query, selected_binding, term, opts) do
    Lock.build_query(
      :lock,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(:limit, source, query, selected_binding, term, opts) do
    Limit.build_query(
      :limit,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(:offset, source, query, selected_binding, term, opts) do
    Offset.build_query(
      :offset,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(:update, source, query, selected_binding, term, opts) do
    Update.build_query(
      :update,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(:with_named_binding, source, query, selected_binding, term, opts) do
    WithNamedBinding.build_query(
      :with_named_binding,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  defp build_query(filter, source, query, selected_binding, term, opts) when filter in @where_filters do
    dyn =
      Postgres.build_dynamic(
        source,
        selected_binding,
        term,
        opts
      )

    Where.build_query(
      filter,
      source,
      query,
      selected_binding,
      dyn,
      opts
    )
  end

  defp sort_filters(params) do
    params =
      if is_map(params) and not is_struct(params) do
        Map.to_list(params)
      else
        params
      end

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
