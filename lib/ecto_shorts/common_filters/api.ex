defmodule EctoShorts.CommonFilters.API do
  alias EctoShorts.CommonFilters.{
    Distinct,
    Exclude,
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
    Select,
    SetOperation,
    SubQuery,
    Update,
    Windows,
    WithCte,
    WithTies,
    WithNamedBinding,
    Where
  }

  @uniqueness_filters [:distinct]
  @grouping_filters [:group_by]
  @post_aggregate_filters [:having, :or_having]
  @association_filters [:join]
  @terminal_result_filters [:last]
  @sorting_filters [:order_by, :prepend_order_by, :reverse_order]
  @predicate_filters [:where, :or_where]
  @eager_load_filters [:preload]
  @namespace_filters [:put_query_prefix]
  @recursive_cte_filters [:recursive_ctes]
  @projection_filters [:select, :select_merge]
  @set_composition_filters [:except, :except_all, :intersect, :intersect_all, :union, :union_all]
  @nested_query_filters [:subquery]
  @removal_filters [:exclude]
  @concurrency_filters [:lock]
  @cardinality_filters [:limit, :first]
  @pagination_filters [:offset]
  @mutation_filters [:update]
  @window_function_filters [:windows]
  @cte_filters [:with_cte]
  @tie_handling_filters [:with_ties]
  @binding_filters [:with_named_binding]

  @all_filters Enum.concat([
                 @uniqueness_filters,
                 @grouping_filters,
                 @post_aggregate_filters,
                 @association_filters,
                 @terminal_result_filters,
                 @sorting_filters,
                 @eager_load_filters,
                 @namespace_filters,
                 @recursive_cte_filters,
                 @window_function_filters,
                 @cte_filters,
                 @tie_handling_filters,
                 @projection_filters,
                 @set_composition_filters,
                 @nested_query_filters,
                 @removal_filters,
                 @concurrency_filters,
                 @cardinality_filters,
                 @pagination_filters,
                 @mutation_filters,
                 @binding_filters,
                 @predicate_filters
               ])

  @filter_set MapSet.new(@all_filters)

  @filters_by_group %{
    uniqueness: @uniqueness_filters,
    grouping: @grouping_filters,
    post_aggregate: @post_aggregate_filters,
    association: @association_filters,
    terminal_result: @terminal_result_filters,
    sorting: @sorting_filters,
    predicate: @predicate_filters,
    eager_load: @eager_load_filters,
    namespace: @namespace_filters,
    recursive_cte: @recursive_cte_filters,
    projection: @projection_filters,
    set_composition: @set_composition_filters,
    nested_query: @nested_query_filters,
    removal: @removal_filters,
    concurrency: @concurrency_filters,
    cardinality: @cardinality_filters,
    pagination: @pagination_filters,
    mutation: @mutation_filters,
    window_function: @window_function_filters,
    cte: @cte_filters,
    tie_handling: @tie_handling_filters,
    binding: @binding_filters
  }

  @filter_groups Map.keys(@filters_by_group)

  def filter_groups, do: @filter_groups

  def filter_group(key) do
    case @filters_by_group do
      %{^key => value} -> value
      _ -> nil
    end
  end

  def filters, do: @all_filters

  def filter?(key), do: MapSet.member?(@filter_set, key)

  def build_query(filter, source, query, selected_binding, term, opts) when filter in @uniqueness_filters do
    Distinct.build_query(
      filter,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:last, source, query, selected_binding, term, opts) do
    Last.build_query(
      :last,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:join, source, query, selected_binding, term, opts) do
    Join.build_query(
      :join,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(filter, source, query, selected_binding, term, opts) when filter in @grouping_filters do
    GroupBy.build_query(
      filter,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(filter, source, query, selected_binding, term, opts)
      when filter in @post_aggregate_filters do
    Having.build_query(
      filter,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(filter, source, query, selected_binding, term, opts) when filter in @sorting_filters do
    OrderBy.build_query(
      filter,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:preload, source, query, selected_binding, term, opts) do
    Preload.build_query(
      :preload,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:subquery, source, query, selected_binding, term, opts) do
    SubQuery.build_query(
      :subquery,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:put_query_prefix, source, query, selected_binding, term, opts) do
    PutQueryPrefix.build_query(
      :put_query_prefix,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:recursive_ctes, source, query, selected_binding, term, opts) do
    RecursiveCtes.build_query(
      :recursive_ctes,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:windows, source, query, selected_binding, term, opts) do
    Windows.build_query(
      :windows,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:with_cte, source, query, selected_binding, term, opts) do
    WithCte.build_query(
      :with_cte,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:with_ties, source, query, selected_binding, term, opts) do
    WithTies.build_query(
      :with_ties,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(filter, source, query, selected_binding, term, opts) when filter in @projection_filters do
    Select.build_query(
      filter,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(filter, source, query, selected_binding, term, opts)
      when filter in @set_composition_filters do
    SetOperation.build_query(
      filter,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:exclude, source, query, selected_binding, term, opts) do
    Exclude.build_query(
      :exclude,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:lock, source, query, selected_binding, term, opts) do
    Lock.build_query(
      :lock,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(filter, source, query, selected_binding, term, opts) when filter in @cardinality_filters do
    Limit.build_query(
      filter,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:offset, source, query, selected_binding, term, opts) do
    Offset.build_query(
      :offset,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:update, source, query, selected_binding, term, opts) do
    Update.build_query(
      :update,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(:with_named_binding, source, query, selected_binding, term, opts) do
    WithNamedBinding.build_query(
      :with_named_binding,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end

  def build_query(filter, source, query, selected_binding, term, opts) when filter in @predicate_filters do
    Where.build_query(
      filter,
      source,
      query,
      selected_binding,
      term,
      opts
    )
  end
end
