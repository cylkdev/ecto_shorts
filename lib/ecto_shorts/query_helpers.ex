defmodule EctoShorts.QueryHelpers do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Provides utility functions for extracting the underlying schema or source
  from Ecto queries or queryable values.

  These functions are useful when you need to introspect a query and determine
  which schema or database table it references, often as part of a dynamic or
  reusable query-building pipeline.

  ## Examples

      iex> require Ecto.Query
      ...> EctoShorts.QueryHelpers.get_query_source(Ecto.Query.from(EctoShorts.Schema.Post))
      {"posts", EctoShorts.Schema.Post}

      iex> EctoShorts.QueryHelpers.get_query_schema(EctoShorts.Schema.Post)
      EctoShorts.Schema.Post
  """

  @type source :: binary()
  @type query :: Ecto.Query.t()
  @type queryable :: Ecto.Queryable.t()
  @type source_queryable :: {source(), queryable()}

  @doc """
  Returns a tuple of `{source, schema}` given a query or queryable value.

  If the input is already a schema module (atom), it is returned as-is.
  If the input is a query, this function attempts to extract the table source
  name and the associated schema module.

  ## Examples

      iex> require Ecto.Query
      ...> EctoShorts.QueryHelpers.get_query_source(Ecto.Query.from(EctoShorts.Schema.Post))
      {"posts", EctoShorts.Schema.Post}
  """
  @spec get_query_source(query() | queryable() | source_queryable()) ::
          queryable() | source_queryable()
  def get_query_source(queryable) when is_atom(queryable), do: queryable
  def get_query_source(%{from: %{source: {source, queryable}}}), do: {source, queryable}
  def get_query_source(%{from: %{query: %{from: {source, queryable}}}}), do: {source, queryable}

  @doc """
  Extracts and returns the schema module from a query or queryable value.

  This is a simplified version of `get_query_source/1` that always returns
  the schema module, whether it's extracted from a query or returned directly.

  ## Examples

      iex> require Ecto.Query
      ...> EctoShorts.QueryHelpers.get_query_schema(Ecto.Query.from(EctoShorts.Schema.Post))
      EctoShorts.Schema.Post
  """
  @spec get_query_schema(query_or_queryable :: query() | queryable()) :: queryable()
  def get_query_schema(%{from: %{source: {_, query}}}), do: get_query_schema(query)
  def get_query_schema(%{from: %{query: %{from: {_, schema_module}}}}), do: schema_module
  def get_query_schema(queryable) when is_atom(queryable), do: queryable
end
