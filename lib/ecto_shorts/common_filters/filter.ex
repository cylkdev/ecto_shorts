defmodule EctoShorts.CommonFilters.Filter do
  @moduledoc false

  alias Ecto.Query

  alias EctoShorts.CommonSchema
  alias EctoShorts.Dynamics

  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.Filter"

  @boolean_operators [:and, :or]

  @common_filters [:ids, :before, :after, :start_date, :end_date]
  @pagination_filters [:first, :last, :limit, :offset, :order_by]

  @doc "Applies the given filter to the query."
  def build(source, filter, query, binding_selector, {bool_op, params}, opts)
      when bool_op in @boolean_operators and is_list(params) do
    dynamic = Dynamics.convert_to_dynamic(source, binding_selector, {bool_op, params}, opts)

    apply_dynamic(filter, query, dynamic)
  end

  def build(source, filter, query, binding_selector, {common_op, params}, opts)
      when common_op in @common_filters do
    dynamic =
      source
      |> CommonSchema.get_schema_source()
      |> Dynamics.convert_to_dynamic(binding_selector, {common_op, params}, opts)

    apply_dynamic(filter, query, dynamic)
  end

  def build(source, filter, query, binding_selector, value, _opts)
      when filter in @pagination_filters do
    apply_pagination_filter(source, filter, query, binding_selector, value)
  end

  def build(source, filter, query, binding_selector, {key, value}, opts) do
    cond do
      schemaless_source?(source) ->
        build_field(source, filter, query, binding_selector, key, value, opts)

      key in CommonSchema.get_schema_reflection(source, :query_fields) ->
        build_field(source, filter, query, binding_selector, key, value, opts)

      true ->
        warn_non_schema_key(source, key)
        query
    end
  end

  def build(_schema, _filter, query, _binding_selector, term, _opts) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected params to be a map or keyword list, got: #{inspect(term)}"
    )

    query
  end

  defp schemaless_source?(source), do: not source_has_schema?(source)

  defp source_has_schema?({_, schema}), do: is_atom(schema) and not is_nil(schema)
  defp source_has_schema?(_), do: false

  defp build_field(source, filter, query, binding_selector, key, value, opts) do
    dyn = Dynamics.convert_to_dynamic(source, binding_selector, {key, value}, opts)
    apply_dynamic(filter, query, dyn)
  end

  defp apply_pagination_filter(source, :first, query, binding_selector, limit) do
    apply_pagination_filter(source, :limit, query, binding_selector, limit)
  end

  # Applies `:last` pagination by selecting the last `limit` rows
  # according to `sort_keys`.
  #
  # First, any existing `order_by` is removed and the query is ordered
  # in descending order by `sort_keys` so the last rows come first;
  # then `limit` is applied and the result is wrapped in a subquery.
  #
  # Finally, the outer query is ordered in ascending order by the same
  # `sort_keys` so the returned rows are presented in the expected order.
  # This reduces over `sort_keys` to support composite primary keys
  # (multi-field ordering).

  defp apply_pagination_filter(source, :last, query, binding_selector, entries)
       when is_map(entries) or is_list(entries) do
    Enum.reduce(entries, query, fn entry, q ->
      apply_pagination_filter(source, :last, q, binding_selector, entry)
    end)
  end

  defp apply_pagination_filter(source, :last, query, _binding_selector, {sort_key, limit}) do
    sort_keys =
      if is_nil(sort_key) do
        List.wrap(CommonSchema.get_schema_reflection(source, :primary_key) || :id)
      else
        List.wrap(sort_key)
      end

    subquery =
      sort_keys
      |> Enum.reduce(Query.exclude(query, :order_by), &Query.order_by(&2, desc: ^&1))
      |> Query.from(limit: ^limit)
      |> Query.subquery()

    Enum.reduce(sort_keys, subquery, &Query.order_by(&2, asc: ^&1))
  end

  defp apply_pagination_filter(source, :last, query, binding_selector, limit) do
    apply_pagination_filter(source, :last, query, binding_selector, {nil, limit})
  end

  defp apply_pagination_filter(_source, :limit, query, _binding_selector, value) do
    Query.limit(query, ^value)
  end

  defp apply_pagination_filter(_source, :offset, query, _binding_selector, value) do
    Query.offset(query, ^value)
  end

  defp apply_pagination_filter(source, :order_by, query, binding_selector, entries)
       when is_map(entries) or is_list(entries) do
    Enum.reduce(entries, query, fn entry, q ->
      apply_pagination_filter(source, :order_by, q, binding_selector, entry)
    end)
  end

  defp apply_pagination_filter(_source, :order_by, query, _binding_selector, {:asc, key}) do
    Query.order_by(query, asc: ^key)
  end

  defp apply_pagination_filter(_source, :order_by, query, _binding_selector, {:desc, key}) do
    Query.order_by(query, desc: ^key)
  end

  defp apply_pagination_filter(_source, :order_by, query, _binding_selector, key)
       when is_atom(key) do
    Query.order_by(query, desc: ^key)
  end

  defp apply_dynamic(_, query, nil) do
    query
  end

  defp apply_dynamic(:or_where, query, dyn) do
    Query.or_where(query, ^dyn)
  end

  defp apply_dynamic(:where, query, dyn) do
    Query.where(query, ^dyn)
  end

  defp warn_non_schema_key(schema, key) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected a query field for schema #{inspect(schema)}, got: #{inspect(key)}"
    )
  end
end
