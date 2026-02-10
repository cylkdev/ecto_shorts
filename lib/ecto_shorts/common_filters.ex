defmodule EctoShorts.CommonFilters do
  alias EctoShorts.CommonSchema
  alias EctoShorts.CommonQuery

  alias EctoShorts.CommonFilters.{
    Distinct,
    Filter,
    GroupBy,
    Having,
    Join,
    OrderBy,
    Preload,
    Select,
    SubQuery,
    WithCte,
    WithNamedBinding,
    WithTies,
    Windows,
    Update
  }

  alias EctoShorts.SchemaHelpers

  @logger_prefix "EctoShorts.CommonFilters"

  @binding_selector_key :bind
  @binding_selector_modes [:as, :at]
  @default_binding_selector {:as, nil}

  @where :where
  @map_payload_helper_operators [:datetime_add, :date_add, :from_now, :ago]

  @schema_filters [:where, :or_where]

  @query_filters [
    :distinct,
    :except,
    :except_all,
    :exclude,
    :first,
    :group_by,
    :having,
    :or_having,
    :intersect,
    :intersect_all,
    :union,
    :union_all,
    :join,
    :last,
    :lock,
    :limit,
    :offset,
    :put_query_prefix,
    :order_by,
    :windows,
    :reverse_order,
    :prepend_order_by,
    :preload,
    :recursive_ctes,
    :select,
    :select_merge,
    :subquery,
    :with_cte,
    :with_named_binding,
    :with_ties,
    :update
  ]

  def convert_params_to_filter(source, params, opts \\ [])

  def convert_params_to_filter(source, params, opts) when is_map(params) do
    convert_params_to_filter(source, Map.to_list(params), opts)
  end

  def convert_params_to_filter(source, entries, opts) when is_list(entries) do
    query = CommonSchema.to_query(source)

    has_source_or_query? =
      Keyword.has_key?(entries, :source) or Keyword.has_key?(entries, :query)

    if Keyword.keyword?(entries) do
      {schema_source, params} = Keyword.pop(entries, :source, source)

      {other_params, params} = Keyword.pop(params, :query, [])

      normalized_source = CommonSchema.normalize_source(schema_source)

      merged_params =
        other_params
        |> ensure_kw!()
        |> Keyword.merge(params)

      merged_params =
        case {has_source_or_query?, normalized_source} do
          {true, {_table, nil}} ->
            if Keyword.has_key?(merged_params, :select) do
              merged_params
            else
              Keyword.put(merged_params, :select, true)
            end

          _ ->
            merged_params
        end

      do_convert(normalized_source, query, merged_params, opts)
    else
      Enum.reduce(entries, query, fn entry, query_acc ->
        do_convert(source, query_acc, entry, opts)
      end)
    end
  end

  defp ensure_kw!(map) when is_map(map), do: Map.to_list(map)

  defp ensure_kw!(list) when is_list(list) do
    unless Keyword.keyword?(list) do
      raise ArgumentError, "Expected params to be a keyword list, got: #{inspect(list)}"
    end

    list
  end

  defp do_convert(schema_source, query, params, opts) do
    normalized_params = normalize_filter_params(params)

    if is_map(normalized_params) or Keyword.keyword?(normalized_params) do
      reduce_filter_params(
        schema_source,
        query,
        @default_binding_selector,
        @where,
        normalized_params,
        opts
      )
    else
      raise ArgumentError, "Expected params to be a map or list, got: #{inspect(params)}"
    end
  end

  defp reduce_filter_params(
         schema_source,
         query,
         binding_selector,
         _filter_op,
         {key, value},
         opts
       )
       when key in @schema_filters do
    reduce_schema_filter_params(schema_source, query, binding_selector, key, value, opts)
  end

  defp reduce_filter_params(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {@binding_selector_key, bind_params},
         opts
       ) do
    reduce_bind_params(
      schema_source,
      query,
      binding_selector,
      filter_op,
      bind_params,
      opts
    )
  end

  defp reduce_filter_params(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       ) do
    query_filters = Keyword.get(opts, :query_filters, @query_filters)

    if key in query_filters do
      apply_query_builder(schema_source, query, binding_selector, key, value, opts)
    else
      reduce_default_filter_params(
        schema_source,
        query,
        binding_selector,
        filter_op,
        key,
        value,
        opts
      )
    end
  end

  defp reduce_filter_params(schema_source, query, binding_selector, filter_op, params, opts)
       when is_map(params) do
    reduce_filter_params(
      schema_source,
      query,
      binding_selector,
      filter_op,
      Map.to_list(params),
      opts
    )
  end

  defp reduce_filter_params(schema_source, query, binding_selector, filter_op, params, opts)
       when is_list(params) do
    if Keyword.keyword?(params) do
      Enum.reduce(params, query, fn {key, value}, query_acc ->
        reduce_filter_params(
          schema_source,
          query_acc,
          binding_selector,
          filter_op,
          {key, value},
          opts
        )
      end)
    else
      apply_query_builder(schema_source, query, binding_selector, filter_op, params, opts)
    end
  end

  defp reduce_filter_params(schema_source, query, binding_selector, filter_op, params, opts) do
    apply_query_builder(schema_source, query, binding_selector, filter_op, params, opts)
  end

  defp reduce_schema_filter_params(schema_source, query, binding_selector, filter_op, value, opts) do
    if is_map(value) or is_list(value) do
      Enum.reduce(value, query, fn entry, query_acc ->
        build_schema_filters(schema_source, query_acc, binding_selector, filter_op, entry, opts)
      end)
    else
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected params for #{filter_op} to be a map or keyword list, got: #{inspect(value)}"
      )

      query
    end
  end

  defp reduce_default_filter_params(
         schema_source,
         query,
         binding_selector,
         filter_op,
         key,
         value,
         opts
       ) do
    case CommonSchema.get_schema_reflection(schema_source, :associations) do
      nil ->
        build_schema_filters(
          schema_source,
          query,
          binding_selector,
          filter_op,
          {key, value},
          opts
        )

      assocs ->
        if key in assocs do
          build_join_filters(
            schema_source,
            query,
            binding_selector,
            filter_op,
            key,
            value,
            opts
          )
        else
          build_schema_filters(
            schema_source,
            query,
            binding_selector,
            filter_op,
            {key, value},
            opts
          )
        end
    end
  end

  defp reduce_bind_params(
         schema_source,
         query,
         binding_selector,
         filter_op,
         bind_params,
         opts
       )
       when is_map(bind_params) and not is_struct(bind_params) do
    reduce_bind_params(
      schema_source,
      query,
      binding_selector,
      filter_op,
      Map.to_list(bind_params),
      opts
    )
  end

  defp reduce_bind_params(
         schema_source,
         query,
         _binding_selector,
         filter_op,
         bind_params,
         opts
       )
       when is_list(bind_params) do
    unless Keyword.keyword?(bind_params) do
      raise ArgumentError,
            "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
    end

    Enum.reduce(bind_params, query, fn
      {binding_mode, scoped_params}, query_acc ->
        reduce_scoped_bind_params(
          schema_source,
          query_acc,
          binding_mode,
          scoped_params,
          filter_op,
          opts
        )

      entry, _query_acc ->
        raise ArgumentError,
              "Expected :bind entries to be {mode, params} tuples, got: #{inspect(entry)}"
    end)
  end

  defp reduce_bind_params(
         _schema_source,
         _query,
         _binding_selector,
         _filter_op,
         bind_params,
         _opts
       ) do
    raise ArgumentError,
          "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
  end

  defp reduce_scoped_bind_params(
         schema_source,
         query,
         binding_mode,
         scoped_params,
         filter_op,
         opts
       ) do
    unless binding_mode in @binding_selector_modes do
      raise ArgumentError,
            "Expected :bind keys to be one of #{inspect(@binding_selector_modes)}, got: #{inspect(binding_mode)}"
    end

    scoped_entries =
      case scoped_params do
        params when is_map(params) and not is_struct(params) ->
          Map.to_list(params)

        params when is_list(params) ->
          params

        params ->
          raise ArgumentError,
                "Expected :bind -> #{inspect(binding_mode)} payload to be a map or keyword list, got: #{inspect(params)}"
      end

    Enum.reduce(scoped_entries, query, fn
      {binding_target, params}, query_acc ->
        reduce_binding_params(
          schema_source,
          query_acc,
          {binding_mode, binding_target},
          filter_op,
          params,
          opts
        )

      entry, _query_acc ->
        raise ArgumentError,
              "Expected :bind -> #{inspect(binding_mode)} entries to be {target, params} tuples, got: #{inspect(entry)}"
    end)
  end

  defp reduce_binding_params(
         schema_source,
         query,
         {binding_mode, binding_target},
         filter,
         params,
         opts
       ) do
    case {binding_mode, binding_target} do
      {:as, bind_alias} when is_atom(bind_alias) ->
        reduce_filter_params(schema_source, query, {:as, bind_alias}, filter, params, opts)

      {:at, bind_index} when is_integer(bind_index) ->
        reduce_filter_params(schema_source, query, {:at, bind_index}, filter, params, opts)

      binding_selector ->
        raise ArgumentError,
              "Expected binding selector to be one of {:as, atom()} or {:at, integer()}, got: #{inspect(binding_selector)}"
    end
  end

  defp build_join_filters(
         schema_source,
         query,
         binding_selector,
         filter,
         assoc_key,
         params,
         opts
       ) do
    if Keyword.keyword?(params) do
      assoc_schema =
        case schema_source do
          {_, parent_schema} -> SchemaHelpers.get_related_schema(parent_schema, assoc_key)
          parent_schema -> SchemaHelpers.get_related_schema(parent_schema, assoc_key)
        end

      joined_query =
        apply_query_builder(
          schema_source,
          query,
          binding_selector,
          :join,
          [{assoc_key, params}],
          opts
        )

      {join_binding_mode, join_binding_target} =
        if Keyword.has_key?(params, :as) do
          {:as, Keyword.get(params, :as, nil)}
        else
          {:at, CommonQuery.query_binding_count(joined_query)}
        end

      reduce_filter_params(
        assoc_schema,
        joined_query,
        {join_binding_mode, join_binding_target},
        filter,
        Keyword.drop(params, [:as, :on, :type]),
        opts
      )
    else
      query
    end
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       )
       when is_map(value) and not is_struct(value) and
              (is_map_key(value, :datetime_add) or is_map_key(value, :date_add) or
                 is_map_key(value, :from_now) or is_map_key(value, :ago)) do
    apply_query_builder(
      schema_source,
      query,
      binding_selector,
      filter_op,
      {key, value},
      opts
    )
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       )
       when is_map(value) and not is_struct(value) do
    reduce_filter_params(
      schema_source,
      query,
      binding_selector,
      filter_op,
      {key, Map.to_list(value)},
      opts
    )
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       )
       when is_list(value) do
    if Keyword.keyword?(value) do
      Enum.reduce(value, query, fn {key2, value2}, query_acc ->
        reduce_filter_params(
          schema_source,
          query_acc,
          binding_selector,
          filter_op,
          {key, {key2, value2}},
          opts
        )
      end)
    else
      apply_query_builder(
        schema_source,
        query,
        binding_selector,
        filter_op,
        {key, value},
        opts
      )
    end
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       ) do
    apply_query_builder(
      schema_source,
      query,
      binding_selector,
      filter_op,
      {key, value},
      opts
    )
  end

  defp apply_query_builder(schema_source, query, binding_selector, filter_op, params, opts)
       when filter_op in [:having, :or_having] do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    Having.build(binding_source, filter_op, query, binding_selector, params, opts)
  end

  defp apply_query_builder(schema_source, query, binding_selector, :distinct, params, opts) do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    Distinct.build(binding_source, :distinct, query, binding_selector, params, opts)
  end

  defp apply_query_builder(schema_source, query, binding_selector, :group_by, params, opts) do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    GroupBy.build(binding_source, :group_by, query, binding_selector, params, opts)
  end

  defp apply_query_builder(schema_source, query, binding_selector, :join, params, opts) do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    Join.build(binding_source, :join, query, binding_selector, params, opts)
  end

  defp apply_query_builder(schema_source, query, binding_selector, :preload, params, opts) do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    Preload.build(binding_source, :preload, query, binding_selector, params, opts)
  end

  defp apply_query_builder(schema_source, query, binding_selector, :windows, params, opts) do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    Windows.build(binding_source, :windows, query, binding_selector, params, opts)
  end

  defp apply_query_builder(schema_source, query, binding_selector, filter_op, params, opts)
       when filter_op in [:order_by, :prepend_order_by] do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    OrderBy.build(binding_source, filter_op, query, binding_selector, params, opts)
  end

  defp apply_query_builder(schema_source, query, binding_selector, :subquery, params, opts)
       when is_map(params) or is_list(params) do
    binding_source = to_binding_source(schema_source, query, binding_selector)

    filtered_query =
      reduce_filter_params(
        schema_source,
        query,
        binding_selector,
        @where,
        params,
        opts
      )

    SubQuery.build(
      binding_source,
      :subquery,
      filtered_query,
      binding_selector,
      params,
      opts
    )
  end

  defp apply_query_builder(_schema_source, query, _binding_selector, :subquery, params, _opts) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :subquery params to be a map or keyword list, got: #{inspect(params)}"
    )

    query
  end

  defp apply_query_builder(schema_source, query, binding_selector, :with_cte, params, opts) do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    WithCte.build(binding_source, :with_cte, query, binding_selector, params, opts)
  end

  defp apply_query_builder(
         schema_source,
         query,
         binding_selector,
         :with_named_binding,
         params,
         opts
       ) do
    binding_source = to_binding_source(schema_source, query, binding_selector)

    WithNamedBinding.build(
      binding_source,
      :with_named_binding,
      query,
      binding_selector,
      params,
      opts
    )
  end

  defp apply_query_builder(schema_source, query, binding_selector, :with_ties, params, opts) do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    WithTies.build(binding_source, :with_ties, query, binding_selector, params, opts)
  end

  defp apply_query_builder(schema_source, query, binding_selector, filter_op, params, opts)
       when filter_op in [:select, :select_merge] do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    Select.build(binding_source, filter_op, query, binding_selector, params, opts)
  end

  defp apply_query_builder(schema_source, query, binding_selector, :update, params, opts) do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    Update.build(binding_source, :update, query, binding_selector, params, opts)
  end

  defp apply_query_builder(schema_source, query, binding_selector, filter_op, params, opts) do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    Filter.build(binding_source, filter_op, query, binding_selector, params, opts)
  end

  defp to_binding_source(schema_source, _query, {:as, nil}) do
    schema_source
  end

  defp to_binding_source(_schema_source, query, {_binding_mode, binding_target}) do
    CommonQuery.get_query_binding_source(query, binding_target)
  end

  defp normalize_filter_params({k, v})
       when k in @map_payload_helper_operators and is_map(v) and not is_struct(v) do
    {k, v}
  end

  defp normalize_filter_params({k, v}) when is_map(v) or is_list(v) do
    {k, normalize_filter_params(v)}
  end

  defp normalize_filter_params(map) when is_map(map) and not is_struct(map) do
    map
    |> Map.to_list()
    |> normalize_filter_params()
  end

  defp normalize_filter_params(list) when is_list(list) do
    if Keyword.keyword?(list) do
      list
      |> sort_params()
      |> Enum.map(fn {k, v} -> {k, normalize_filter_params(v)} end)
    else
      list
    end
  end

  defp normalize_filter_params(term) do
    term
  end

  defp sort_params(params) do
    where_filters = Keyword.take(params, [:where])

    or_where_filters = Keyword.take(params, [:or_where])

    terminal_filters =
      [:last, :subquery]
      |> Enum.flat_map(fn key ->
        case List.keyfind(params, key, 0) do
          nil -> []
          entry -> [entry]
        end
      end)

    # Regular field filters should be processed with where_filters
    # since they can contain implicit WHERE clauses and must come
    # before or_where filters
    rest = Keyword.drop(params, [:where, :or_where, :last, :subquery])

    where_filters
    |> Kernel.++(rest)
    |> Kernel.++(or_where_filters)
    |> Kernel.++(terminal_filters)
  end
end
