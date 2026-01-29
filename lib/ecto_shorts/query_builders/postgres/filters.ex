defmodule EctoShorts.QueryBuilder.Filters do
  @moduledoc false

  alias Ecto.Query

  alias EctoShorts.CommonSchema
  alias EctoShorts.QueryBuilders.Postgres.Dynamics
  alias EctoShorts.QueryBuilders.Postgres.Pagination

  require Ecto.Query

  @logger_prefix "EctoShorts.QueryBuilder.Filters"

  @common_operators [:ids, :before, :after, :start_date, :end_date]
  @boolean_operators [:and, :or]

  @pagination_filters Pagination.filters()

  def build_query(source, filter, query, binding_selector, {bool_op, args}, _opts)
      when bool_op in @boolean_operators and is_list(args) do
    dynamic = Dynamics.convert_to_dynamic(source, binding_selector, {bool_op, args})

    apply_query_dynamic(filter, query, dynamic)
  end

  def build_query(source, filter, query, binding_selector, {common_op, args}, _opts)
      when common_op in @common_operators do
    dynamic =
      source
      |> CommonSchema.get_schema_source()
      |> Dynamics.convert_to_dynamic(binding_selector, {common_op, args})

    apply_query_dynamic(filter, query, dynamic)
  end

  def build_query(source, filter, query, _binding_selector, value, _opts)
      when filter in @pagination_filters do
    Pagination.build_query(source, query, filter, value)
  end

  def build_query(source, filter, query, binding_selector, {key, value}, _opts) do
    cond do
      schemaless_source?(source) ->
        build_field(source, filter, query, binding_selector, key, value)

      key in CommonSchema.get_schema_reflection(source, :query_fields) ->
        build_field(source, filter, query, binding_selector, key, value)

      true ->
        warn_non_schema_key(source, key)
        query
    end
  end

  def build_query(_schema, _filter, query, _binding_selector, term, _opts) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected params to be a map or keyword list, got: #{inspect(term)}"
    )

    query
  end

  defp schemaless_source?(source), do: not source_has_schema?(source)

  defp source_has_schema?({_, schema}), do: is_atom(schema) and not is_nil(schema)
  defp source_has_schema?(_), do: false

  defp build_field(source, filter, query, binding_selector, key, value) do
    dynamic = Dynamics.convert_to_dynamic(source, binding_selector, {key, value})
    apply_query_dynamic(filter, query, dynamic)
  end

  defp apply_query_dynamic(_, query, nil) do
    query
  end

  defp apply_query_dynamic(:or_where, query, dynamic) do
    Query.or_where(query, ^dynamic)
  end

  defp apply_query_dynamic(:where, query, dynamic) do
    Query.where(query, ^dynamic)
  end

  defp warn_non_schema_key(schema, key) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected a query field for schema #{inspect(schema)}, got: #{inspect(key)}"
    )
  end
end
