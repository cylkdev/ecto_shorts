defmodule EctoShorts.CommonFilters do
  @moduledoc since: "3.0.0"

  alias EctoShorts.CommonSchema
  alias EctoShorts.Config
  alias EctoShorts.CommonFilters.API
  alias EctoShorts.QueryBuilder
  alias EctoShorts.Utils

  @logger_prefix "EctoShorts.CommonFilters"

  @binding_operator [:as, :at]

  @behaviour EctoShorts.QueryBuilder

  def convert_params_to_filter(source, params, opts) do
    query = CommonSchema.to_query(source)

    sorter = opts[:sorter] || (&sort_filter_params/1)

    params
    |> Utils.map_to_keyword()
    |> sorter.()
    |> Enum.reduce(query, fn {key, value}, query_acc ->
      apply_filters(:where, source, query_acc, {:as, nil}, {key, value}, opts)
    end)
  end

  defp apply_filters(filter, source, query, selected_binding, {key, term}, opts) do
    cond do
      key == :bind ->
        apply_bind_filters(filter, source, query, selected_binding, term, opts)

      key in @binding_operator ->
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

      true ->
        build_query(filter, source, query, selected_binding, {key, term}, opts)
    end
  end

  defp apply_filters(filter, source, query, selected_binding, term, opts) do
    Enum.reduce(term, query, &apply_filters(filter, source, &2, selected_binding, &1, opts))
  end

  defp reduce_filter_group_or_build(filter, source, query, selected_binding, term, opts) do
    if reducible_filter_entries?(term) do
      Enum.reduce(Utils.map_to_keyword(term), query, fn {inner_key, inner_value}, query_acc ->
        apply_filters(
          filter,
          source,
          query_acc,
          selected_binding,
          {inner_key, inner_value},
          opts
        )
      end)
    else
      build_query(filter, source, query, selected_binding, term, opts)
    end
  end

  defp apply_bind_filters(filter, source, query, selected_binding, term, opts) do
    params = Utils.map_to_keyword(term)

    if Keyword.keyword?(params) do
      {next_binding, params} =
        cond do
          Keyword.has_key?(params, :as) ->
            {{:as, Keyword.fetch!(params, :as)}, Keyword.delete(params, :as)}

          Keyword.has_key?(params, :at) ->
            {{:at, Keyword.fetch!(params, :at)}, Keyword.delete(params, :at)}

          true ->
            {selected_binding, params}
        end

      Enum.reduce(params, query, fn {inner_key, inner_value}, query_acc ->
        apply_filters(filter, source, query_acc, next_binding, {inner_key, inner_value}, opts)
      end)
    else
      query
    end
  end

  defp reduce_association_filters(query, filter, source, key, term, opts) do
    Enum.reduce(Utils.map_to_keyword(term), query, fn {inner_key, inner_value}, query_acc ->
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

  defp reducible_filter_entries?(term) do
    (is_map(term) and not is_struct(term)) or Keyword.keyword?(term)
  end

  defp association_filter?(source, key, term) do
    reducible_filter_entries?(term) and
      key in (CommonSchema.get_schema_reflection(source, :associations) || [])
  end

  @impl EctoShorts.QueryBuilder
  def build_query(filter, source, query, selected_binding, term, opts) do
    case opts[:query_builder] || Config.query_builder() do
      nil ->
        API.build_query(filter, source, query, selected_binding, term, opts)

      module when is_atom(module) ->
        if function_exported?(module, :build_query, 6) do
          QueryBuilder.build_query(module, filter, source, query, selected_binding, term, opts)
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
