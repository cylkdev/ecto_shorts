defmodule EctoShorts.CommonFilters.WithTies do
  alias Ecto.Query
  alias EctoShorts.CommonFilters.{Limit, OrderBy}
  alias EctoShorts.{CommonSchema, QueryBindings, Utils}

  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.WithTies"
  @default_limit 1000

  {_, binding_patterns} = QueryBindings.query_binding_contracts(__MODULE__)

  def build_query(:with_ties, source, query, selected_binding, params, opts) do
    apply_with_ties(source, query, selected_binding, params, opts)
  end

  defp apply_with_ties(source, query, selected_binding, params, opts)
       when is_map(params) and not is_struct(params) do
    apply_with_ties(source, query, selected_binding, Map.to_list(params), opts)
  end

  defp apply_with_ties(source, query, selected_binding, params, opts) when is_list(params) do
    normalized_params = Utils.map_to_keyword(params)

    if Keyword.keyword?(normalized_params) do
      unknown_keys =
        normalized_params
        |> Keyword.keys()
        |> Enum.reject(&(&1 === :limit))

      cond do
        unknown_keys !== [] ->
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected :with_ties params to only include :limit, got unsupported keys: #{inspect(unknown_keys)}"
          )

          query

        true ->
          apply_limit_payload(source, query, selected_binding, Keyword.get(normalized_params, :limit), opts)
      end
    else
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected :with_ties value to be a boolean or keyword/map payload, got: #{inspect(params)}"
      )

      query
    end
  end

  defp apply_with_ties(source, query, selected_binding, true, opts) do
    query
    |> ensure_limit(source, selected_binding, @default_limit, opts)
    |> ensure_order(source, selected_binding, opts)
    |> apply_with_ties_expr(selected_binding, true)
  end

  defp apply_with_ties(_source, query, selected_binding, false, _opts) do
    if has_limit?(query) do
      apply_with_ties_expr(query, selected_binding, false)
    else
      query
    end
  end

  defp apply_with_ties(_source, query, _selected_binding, value, _opts) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :with_ties value to be a boolean or keyword/map payload, got: #{inspect(value)}"
    )

    query
  end

  defp apply_limit_payload(source, query, selected_binding, nil, opts) do
    query
    |> ensure_limit(source, selected_binding, @default_limit, opts)
    |> ensure_order(source, selected_binding, opts)
    |> apply_with_ties_expr(selected_binding, true)
  end

  defp apply_limit_payload(source, query, selected_binding, limit, opts) when is_integer(limit) do
    query =
      Limit.build_query(
        :limit,
        source,
        query,
        selected_binding,
        limit,
        opts
      )

    query
    |> ensure_order(source, selected_binding, opts)
    |> apply_with_ties_expr(selected_binding, true)
  end

  defp apply_limit_payload(_source, query, _selected_binding, value, _opts) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :with_ties :limit to be an integer or nil, got: #{inspect(value)}"
    )

    query
  end

  defp ensure_limit(query, source, selected_binding, limit, opts) do
    if has_limit?(query) do
      query
    else
      Limit.build_query(:limit, source, query, selected_binding, limit, opts)
    end
  end

  defp ensure_order(query, source, selected_binding, opts) do
    if has_order?(query) do
      query
    else
      sort_keys =
        List.wrap(CommonSchema.get_schema_reflection(source, :primary_key) || :id)

      order_entries = Enum.map(sort_keys, &{:asc, &1})
      OrderBy.build_query(:order_by, source, query, selected_binding, order_entries, opts)
    end
  end

  defp has_limit?(query), do: not is_nil(query.limit)
  defp has_order?(query), do: query.order_bys !== []

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp apply_with_ties_expr(query, unquote(quoted_binding_head), value) when is_boolean(value) do
      Query.with_ties(
        query,
        [unquote_splicing(quoted_binding_body)],
        ^value
      )
    end
  end

  defp apply_with_ties_expr(query, _selected_binding, value) when is_boolean(value) do
    Query.with_ties(query, ^value)
  end
end
