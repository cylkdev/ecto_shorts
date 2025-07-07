defmodule EctoShorts.CommonQueryAPI do
  @moduledoc false
  alias Ecto.Query

  alias EctoShorts.{
    CommonQueryAPI.DynamicBuilder,
    CommonQueryAPI.FilterBuilder,
    CommonQueryAPI.JoinBuilder,
    CommonQueryAPI.LockBuilder,
    Utils
  }

  require Ecto.Query
  require DynamicBuilder
  require FilterBuilder
  require JoinBuilder
  require LockBuilder

  @default_dynamic_builder_adapter EctoShorts.DynamicBuilders.Postgres

  @doc """
  ...
  """
  def and_dynamic(dyn_a, dyn_b), do: merge_dynamic(dyn_a, :and, dyn_b)

  @doc """
  ...
  """
  def or_dynamic(dyn_a, dyn_b), do: merge_dynamic(dyn_a, :or, dyn_b)

  @doc """
  ...
  """
  def merge_dynamic(dyn_a, _, nil), do: dyn_a
  def merge_dynamic(nil, _, dyn_b), do: dyn_b
  def merge_dynamic(dyn_a, :and, dyn_b), do: Query.dynamic(^dyn_a and ^dyn_b)
  def merge_dynamic(dyn_a, :or, dyn_b), do: Query.dynamic(^dyn_a or ^dyn_b)

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:distinct)
  FilterBuilder.define_positional_binding_api(:distinct)
  FilterBuilder.define_named_binding_api(:distinct)

  @doc """
  ...
  """
  DynamicBuilder.define_base_api()
  DynamicBuilder.define_positional_binding_api()
  DynamicBuilder.define_named_binding_api()

  def dynamic(_source, current_binding, true, _opts) do
    dynamic(current_binding, true)
  end

  def dynamic(source, current_binding, value, opts) do
    if value === true do
      dynamic(current_binding, true)
    else
      value
      |> normalize_conditions()
      |> Enum.reduce(nil, fn
        {condition, params}, dyn ->
          Utils.apply_expressions(
            dyn,
            params,
            fn {key, value}, dyn ->
              build_dynamic(
                source,
                dyn,
                current_binding,
                condition,
                key,
                value,
                opts
              )
            end,
            opts
          )
      end)
    end
  end

  defp normalize_conditions(params) do
    {params_with_conditions, params_without_conditions} =
      Enum.reduce(params, {[], []}, fn
        {:and, con}, {params_with_conditions, params_without_conditions} ->
          {[{:and, con} | params_with_conditions], params_without_conditions}

        {:or, con}, {params_with_conditions, params_without_conditions} ->
          {[{:or, con} | params_with_conditions], params_without_conditions}

        {key, value}, {params_with_conditions, params_without_conditions} ->
          {params_with_conditions, [{key, value} | params_without_conditions]}
      end)

    Enum.sort([and: params_without_conditions] ++ params_with_conditions)
  end

  defp build_dynamic(
         source,
         dyn,
         current_binding,
         condition,
         key,
         value,
         opts
       ) do
    opts
    |> dynamic_builder_adapter()
    |> DynamicBuilder.build_dynamic(
      source,
      dyn,
      current_binding,
      condition,
      key,
      value
    )
  end

  defp dynamic_builder_adapter(opts) do
    opts[:dynamic_builder_adapter] || @default_dynamic_builder_adapter
  end

  @doc """
  ...
  """
  def except(query, other_query) do
    Query.except(query, ^other_query)
  end

  @doc """
  ...
  """
  def except_all(query, other_query) do
    Query.except_all(query, ^other_query)
  end

  @doc """
  ...
  """
  def exclude(query, field) do
    Query.exclude(query, field)
  end

  @doc """
  ...
  """
  def first(queryable, order_by \\ nil) do
    Query.first(queryable, order_by)
  end

  @doc """
  ...
  """
  def from(expr, opts \\ [])

  def from(expr, %{join: join} = opts) do
    as = opts[:as]
    distinct = opts[:distinct]
    group_by = opts[:group_by]
    having = opts[:having]
    limit = opts[:limit]
    offset = opts[:offset]
    on = opts[:on] || true
    order_by = opts[:order_by]
    preload = opts[:preload]
    select = opts[:select]
    select_merge = opts[:select_merge]
    update = opts[:update]
    where = opts[:where]

    Query.from(expr,
      as: ^as,
      distinct: ^distinct,
      group_by: ^group_by,
      having: ^having,
      join: ^join,
      on: ^on,
      limit: ^limit,
      offset: ^offset,
      order_by: ^order_by,
      preload: ^preload,
      select: ^select,
      select_merge: ^select_merge,
      update: ^update,
      where: ^where
    )
  end

  def from(expr, %{left_join: left_join} = opts) do
    as = opts[:as]
    distinct = opts[:distinct]
    group_by = opts[:group_by]
    having = opts[:having]
    limit = opts[:limit]
    offset = opts[:offset]
    on = opts[:on] || true
    order_by = opts[:order_by]
    preload = opts[:preload]
    select = opts[:select]
    select_merge = opts[:select_merge]
    update = opts[:update]
    where = opts[:where]

    Query.from(expr,
      as: ^as,
      distinct: ^distinct,
      group_by: ^group_by,
      having: ^having,
      left_join: ^left_join,
      on: ^on,
      limit: ^limit,
      offset: ^offset,
      order_by: ^order_by,
      preload: ^preload,
      select: ^select,
      select_merge: ^select_merge,
      update: ^update,
      where: ^where
    )
  end

  def from(expr, %{right_join: right_join} = opts) do
    as = opts[:as]
    distinct = opts[:distinct]
    group_by = opts[:group_by]
    having = opts[:having]
    limit = opts[:limit]
    offset = opts[:offset]
    on = opts[:on] || true
    order_by = opts[:order_by]
    preload = opts[:preload]
    select = opts[:select]
    select_merge = opts[:select_merge]
    update = opts[:update]
    where = opts[:where]

    Query.from(expr,
      as: ^as,
      distinct: ^distinct,
      group_by: ^group_by,
      having: ^having,
      right_join: ^right_join,
      on: ^on,
      limit: ^limit,
      offset: ^offset,
      order_by: ^order_by,
      preload: ^preload,
      select: ^select,
      select_merge: ^select_merge,
      update: ^update,
      where: ^where
    )
  end

  def from(expr, %{inner_join: inner_join} = opts) do
    as = opts[:as]
    distinct = opts[:distinct]
    group_by = opts[:group_by]
    having = opts[:having]
    limit = opts[:limit]
    offset = opts[:offset]
    on = opts[:on] || true
    order_by = opts[:order_by]
    preload = opts[:preload]
    select = opts[:select]
    select_merge = opts[:select_merge]
    update = opts[:update]
    where = opts[:where]

    Query.from(expr,
      as: ^as,
      distinct: ^distinct,
      group_by: ^group_by,
      having: ^having,
      inner_join: ^inner_join,
      on: ^on,
      limit: ^limit,
      offset: ^offset,
      order_by: ^order_by,
      preload: ^preload,
      select: ^select,
      select_merge: ^select_merge,
      update: ^update,
      where: ^where
    )
  end

  def from(expr, %{full_join: full_join} = opts) do
    as = opts[:as]
    distinct = opts[:distinct]
    group_by = opts[:group_by]
    having = opts[:having]
    limit = opts[:limit]
    offset = opts[:offset]
    on = opts[:on] || true
    order_by = opts[:order_by]
    preload = opts[:preload]
    select = opts[:select]
    select_merge = opts[:select_merge]
    update = opts[:update]
    where = opts[:where]

    Query.from(expr,
      as: ^as,
      distinct: ^distinct,
      group_by: ^group_by,
      having: ^having,
      full_join: ^full_join,
      on: ^on,
      limit: ^limit,
      offset: ^offset,
      order_by: ^order_by,
      preload: ^preload,
      select: ^select,
      select_merge: ^select_merge,
      update: ^update,
      where: ^where
    )
  end

  def from(expr, %{cross_join: cross_join} = opts) do
    as = opts[:as]
    distinct = opts[:distinct]
    group_by = opts[:group_by]
    having = opts[:having]
    limit = opts[:limit]
    offset = opts[:offset]
    on = opts[:on] || true
    order_by = opts[:order_by]
    preload = opts[:preload]
    select = opts[:select]
    select_merge = opts[:select_merge]
    update = opts[:update]
    where = opts[:where]

    Query.from(expr,
      as: ^as,
      distinct: ^distinct,
      group_by: ^group_by,
      having: ^having,
      cross_join: ^cross_join,
      on: ^on,
      limit: ^limit,
      offset: ^offset,
      order_by: ^order_by,
      preload: ^preload,
      select: ^select,
      select_merge: ^select_merge,
      update: ^update,
      where: ^where
    )
  end

  def from(expr, opts) do
    from(expr, Map.new(opts))
  end

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:group_by)
  FilterBuilder.define_positional_binding_api(:group_by)
  FilterBuilder.define_named_binding_api(:group_by)

  @doc """
  ...
  """
  def has_named_binding?(queryable, key) do
    Query.has_named_binding?(queryable, key)
  end

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:having)
  FilterBuilder.define_positional_binding_api(:having)
  FilterBuilder.define_named_binding_api(:having)

  @doc """
  ...
  """
  def intersect(query, other_query) do
    Query.intersect(query, ^other_query)
  end

  @doc """
  ...
  """
  def intersect_all(query, other_query) do
    Query.intersect_all(query, ^other_query)
  end

  @doc """
  ...
  """
  def has_named_binding(query, name) do
    Query.has_named_binding?(query, name)
  end

  @doc """
  ...
  """
  JoinBuilder.define_base_api()
  JoinBuilder.define_positional_binding_api()
  JoinBuilder.define_named_binding_api()

  @doc """
  ...
  """
  def last(queryable, order_by \\ nil) do
    Query.last(queryable, order_by)
  end

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:limit)
  FilterBuilder.define_positional_binding_api(:limit)
  FilterBuilder.define_named_binding_api(:limit)

  @doc """
  Locks selected rows for update. Other transactions can’t update/delete.
  Most common for pessimistic locking.
  """
  LockBuilder.define_base_api("update", "FOR UPDATE")
  LockBuilder.define_positional_binding_api("update", "FOR UPDATE")
  LockBuilder.define_named_binding_api("update", "FOR UPDATE")

  @doc """
  Locks selected rows for update. Other transactions can’t update/delete.
  Most common for pessimistic locking.
  """
  LockBuilder.define_base_api("no_key_update", "FOR NO KEY UPDATE")
  LockBuilder.define_positional_binding_api("no_key_update", "FOR NO KEY UPDATE")
  LockBuilder.define_named_binding_api("no_key_update", "FOR NO KEY UPDATE")

  @doc """
  Allows other transactions to read, but not update/delete.
  Shared read lock.
  """
  LockBuilder.define_base_api("share", "FOR SHARE")
  LockBuilder.define_positional_binding_api("share", "FOR SHARE")
  LockBuilder.define_named_binding_api("share", "FOR SHARE")

  @doc """
  Allows other transactions to read, but not update/delete.
  Shared read lock.
  """
  LockBuilder.define_base_api("key_share", "FOR KEY SHARE")
  LockBuilder.define_positional_binding_api("key_share", "FOR KEY SHARE")
  LockBuilder.define_named_binding_api("key_share", "FOR KEY SHARE")

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:offset)
  FilterBuilder.define_positional_binding_api(:offset)
  FilterBuilder.define_named_binding_api(:offset)

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:or_having)
  FilterBuilder.define_positional_binding_api(:or_having)
  FilterBuilder.define_named_binding_api(:or_having)

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:or_where)
  FilterBuilder.define_positional_binding_api(:or_where)
  FilterBuilder.define_named_binding_api(:or_where)

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:order_by)
  FilterBuilder.define_positional_binding_api(:order_by)
  FilterBuilder.define_named_binding_api(:order_by)

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:preload)
  FilterBuilder.define_positional_binding_api(:preload)
  FilterBuilder.define_named_binding_api(:preload)

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:prepend_order_by)
  FilterBuilder.define_positional_binding_api(:prepend_order_by)
  FilterBuilder.define_named_binding_api(:prepend_order_by)

  @doc """
  ...
  """
  def put_query_prefix(query, prefix) do
    Query.put_query_prefix(query, prefix)
  end

  @doc """
  ...
  """
  def recursive_ctes(query, value) do
    Query.recursive_ctes(query, value)
  end

  @doc """
  ...
  """
  def reverse_order(query) do
    Query.reverse_order(query)
  end

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:select)
  FilterBuilder.define_positional_binding_api(:select)
  FilterBuilder.define_named_binding_api(:select)

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:select_merge)
  FilterBuilder.define_positional_binding_api(:select_merge)
  FilterBuilder.define_named_binding_api(:select_merge)

  @doc """
  ...
  """
  def subquery(query, opts \\ []) do
    Query.subquery(query, opts)
  end

  @doc """
  ...
  """
  def union(query, other_query) do
    Query.union(query, ^other_query)
  end

  @doc """
  ...
  """
  def union_all(query, other_query) do
    Query.union_all(query, ^other_query)
  end

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:update)
  FilterBuilder.define_positional_binding_api(:update)
  FilterBuilder.define_named_binding_api(:update)

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:where)
  FilterBuilder.define_positional_binding_api(:where)
  FilterBuilder.define_named_binding_api(:where)

  @doc """
  ...
  """
  def with_cte(query, name, opts) do
    as = opts[:as]
    materialized = opts[:materialized]
    operation = opts[:operation] || :all

    Query.with_cte(query, ^name, as: ^as, materialized: materialized, operation: operation)
  end

  @doc """
  ...
  """
  def with_named_binding(query, key, fun) do
    Query.with_named_binding(query, key, fun)
  end

  @doc """
  ...
  """
  FilterBuilder.define_base_api(:with_ties)
  FilterBuilder.define_positional_binding_api(:with_ties)
  FilterBuilder.define_named_binding_api(:with_ties)
end
