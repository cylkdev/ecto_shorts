defmodule EctoShorts.CommonFilters do
  alias EctoShorts.CommonSchema
  alias EctoShorts.CommonQuery

  alias EctoShorts.CommonFilters.{
    Filter,
    Join,
    Preload,
    Select,
    SubQuery
  }

  alias EctoShorts.SchemaHelpers

  @logger_prefix "EctoShorts.CommonFilters"

  @binding_operators [:as, :at]
  @default_binding_selector {:as, nil}

  @where :where
  @schema_filters [:where, :or_where]
  @query_filters [
    :join,
    :offset,
    :limit,
    :select,
    :select_merge,
    :first,
    :last,
    :order_by,
    :preload,
    :subquery
  ]

  def convert_params_to_filter(source, params, opts \\ []) do
    schema_source = CommonSchema.normalize_source(source)

    query = CommonSchema.to_query(source)

    params
    |> list_wrap()
    |> Enum.reduce(query, fn params, query_acc ->
      normalized_params = normalize_filter_params(params)

      if is_map(normalized_params) or Keyword.keyword?(normalized_params) do
        reduce_filter_params(
          schema_source,
          query_acc,
          @default_binding_selector,
          @where,
          normalized_params,
          opts
        )
      else
        raise ArgumentError, "Expected params to be a map or list, got: #{inspect(params)}"
      end
    end)
  end

  defp reduce_filter_params(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       ) do
    cond do
      key in @schema_filters ->
        if is_map(value) or is_list(value) do
          Enum.reduce(value, query, fn entry, query_acc ->
            build_schema_filters(schema_source, query_acc, binding_selector, key, entry, opts)
          end)
        else
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected params for #{key} to be a map or keyword list, got: #{inspect(value)}"
          )

          query
        end

      key in Keyword.get(opts, :query_filters, @query_filters) ->
        apply_query_builder(schema_source, query, binding_selector, key, value, opts)

      key in @binding_operators ->
        if is_map(value) or is_list(value) do
          Enum.reduce(value, query, fn {binding_target, params}, query_acc ->
            reduce_binding_params(
              schema_source,
              query_acc,
              {key, binding_target},
              filter_op,
              params,
              opts
            )
          end)
        else
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected value for binding selector to be a map or keyword list, got: #{inspect(value)}"
          )

          query
        end

      true ->
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
  end

  defp reduce_filter_params(schema_source, query, binding_selector, filter_op, params, opts) do
    if is_map(params) do
      reduce_filter_params(
        schema_source,
        query,
        binding_selector,
        filter_op,
        Map.to_list(params),
        opts
      )
    else
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
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected binding selector to be one of {:as, atom()} or {:at, integer()}, got: #{inspect(binding_selector)}"
        )

        query
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
       ) do
    cond do
      is_map(value) and not is_struct(value) ->
        reduce_filter_params(
          schema_source,
          query,
          binding_selector,
          filter_op,
          {key, Map.to_list(value)},
          opts
        )

      is_list(value) ->
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

      true ->
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

  defp apply_query_builder(schema_source, query, binding_selector, filter_op, params, opts) do
    binding_source = to_binding_source(schema_source, query, binding_selector)

    case filter_op do
      :join ->
        Join.build(binding_source, filter_op, query, binding_selector, params, opts)

      :preload ->
        Preload.build(binding_source, filter_op, query, binding_selector, params, opts)

      :subquery ->
        if is_map(params) or is_list(params) do
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
            filter_op,
            filtered_query,
            binding_selector,
            params,
            opts
          )
        else
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected :subquery params to be a map or keyword list, got: #{inspect(params)}"
          )

          query
        end

      filter_op when filter_op in [:select, :select_merge] ->
        Select.build(binding_source, filter_op, query, binding_selector, params, opts)

      filter_op ->
        Filter.build(binding_source, filter_op, query, binding_selector, params, opts)
    end
  end

  defp to_binding_source(schema_source, _query, {:as, nil}) do
    schema_source
  end

  defp to_binding_source(_schema_source, query, {_binding_mode, binding_target}) do
    CommonQuery.get_query_binding_source(query, binding_target)
  end

  defp list_wrap(term) do
    cond do
      is_map(term) -> [term]
      is_list(term) -> if Keyword.keyword?(term), do: [term], else: term
      true -> [term]
    end
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
    cond do
      Keyword.keyword?(list) ->
        list
        |> sort_params()
        |> Enum.map(fn {k, v} -> {k, normalize_filter_params(v)} end)

      true ->
        Enum.map(list, &normalize_filter_params/1)
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
