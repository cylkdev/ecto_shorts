defmodule EctoShorts.CommonFilters.Filter do
  @moduledoc false

  alias EctoShorts.CommonSchema
  alias EctoShorts.CommonFilters
  alias EctoShorts.Dynamics

  alias Ecto.Query
  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.Filter"

  @boolean_directives [:and, :or]

  @pagination_filters [
    :except,
    :except_all,
    :exclude,
    :first,
    :intersect,
    :intersect_all,
    :last,
    :limit,
    :offset,
    :order_by
  ]

  @custom_filters [:ids, :before, :after, :start_date, :end_date]

  @doc "Applies the given filter to the query."
  def build(schema_source, filter, query, binding_selector, {boolean_directive, params}, opts)
      when boolean_directive in @boolean_directives and is_list(params) do
    dyn =
      Dynamics.convert_to_dynamic(
        schema_source,
        binding_selector,
        {boolean_directive, params},
        opts
      )

    apply_where_expr(filter, query, dyn)
  end

  def build(schema_source, filter, query, binding_selector, {key, params}, opts)
      when key in @custom_filters do
    dyn =
      schema_source
      |> CommonSchema.get_schema_source()
      |> Dynamics.convert_to_dynamic(binding_selector, {key, params}, opts)

    apply_where_expr(filter, query, dyn)
  end

  def build(schema_source, filter, query, binding_selector, value, opts)
      when filter in @pagination_filters do
    apply_pagination_expr(schema_source, filter, query, binding_selector, value, opts)
  end

  def build(schema_source, filter, query, binding_selector, {key, value}, opts) do
    cond do
      schemaless_source?(schema_source) ->
        build_field(schema_source, filter, query, binding_selector, key, value, opts)

      key in CommonSchema.get_schema_reflection(schema_source, :query_fields) ->
        build_field(schema_source, filter, query, binding_selector, key, value, opts)

      true ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected a query field for schema #{inspect(schema_source)}, got: #{inspect(key)}"
        )

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

  defp build_field(schema_source, filter, query, binding_selector, key, value, opts) do
    dyn = Dynamics.convert_to_dynamic(schema_source, binding_selector, {key, value}, opts)
    apply_where_expr(filter, query, dyn)
  end

  defp apply_pagination_expr(_schema_source, :exclude, query, _binding_selector, entries, _opts) do
    entries
    |> List.wrap()
    |> Enum.reduce(query, fn filter, q2 -> Query.exclude(q2, filter) end)
  end

  defp apply_pagination_expr(schema_source, :except, query, _binding_selector, value, opts) do
    if is_struct(value, Ecto.Query) do
      Query.except(query, ^value)
    else
      other_query = CommonFilters.convert_params_to_filter(schema_source, value, opts)
      Query.except(query, ^other_query)
    end
  end

  defp apply_pagination_expr(schema_source, :except_all, query, _binding_selector, value, opts) do
    if is_struct(value, Ecto.Query) do
      Query.except_all(query, ^value)
    else
      other_query = CommonFilters.convert_params_to_filter(schema_source, value, opts)
      Query.except_all(query, ^other_query)
    end
  end

  defp apply_pagination_expr(schema_source, :intersect, query, _binding_selector, value, opts) do
    if is_struct(value, Ecto.Query) do
      Query.intersect(query, ^value)
    else
      other_query = CommonFilters.convert_params_to_filter(schema_source, value, opts)
      Query.intersect(query, ^other_query)
    end
  end

  defp apply_pagination_expr(schema_source, :intersect_all, query, _binding_selector, value, opts) do
    if is_struct(value, Ecto.Query) do
      Query.intersect_all(query, ^value)
    else
      other_query = CommonFilters.convert_params_to_filter(schema_source, value, opts)
      Query.intersect_all(query, ^other_query)
    end
  end

  defp apply_pagination_expr(schema_source, :first, query, binding_selector, limit, opts) do
    apply_pagination_expr(schema_source, :limit, query, binding_selector, limit, opts)
  end

  defp apply_pagination_expr(schema_source, :last, query, binding_selector, entries, opts)
       when is_map(entries) or is_list(entries) do
    Enum.reduce(entries, query, fn entry, q ->
      apply_pagination_expr(schema_source, :last, q, binding_selector, entry, opts)
    end)
  end

  defp apply_pagination_expr(
         schema_source,
         :last,
         query,
         _binding_selector,
         {sort_key, limit},
         _opts
       ) do
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

    sort_keys =
      if is_nil(sort_key) do
        List.wrap(CommonSchema.get_schema_reflection(schema_source, :primary_key) || :id)
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

  defp apply_pagination_expr(schema_source, :last, query, binding_selector, limit, opts) do
    apply_pagination_expr(schema_source, :last, query, binding_selector, {nil, limit}, opts)
  end

  defp apply_pagination_expr(_schema_source, :limit, query, _binding_selector, value, _opts) do
    Query.limit(query, ^value)
  end

  defp apply_pagination_expr(_schema_source, :offset, query, _binding_selector, value, _opts) do
    Query.offset(query, ^value)
  end

  defp apply_pagination_expr(schema_source, :order_by, query, binding_selector, entries, opts) do
    case entries do
      entries when is_map(entries) or is_list(entries) ->
        Enum.reduce(entries, query, fn entry, q ->
          apply_pagination_expr(schema_source, :order_by, q, binding_selector, entry, opts)
        end)

      {:asc, key} ->
        Query.order_by(query, asc: ^key)

      {:desc, key} ->
        Query.order_by(query, desc: ^key)

      key when is_atom(key) ->
        Query.order_by(query, desc: ^key)
    end
  end

  defp apply_where_expr(_, query, nil) do
    query
  end

  defp apply_where_expr(:or_where, query, dyn) do
    Query.or_where(query, ^dyn)
  end

  defp apply_where_expr(:where, query, dyn) do
    Query.where(query, ^dyn)
  end
end
