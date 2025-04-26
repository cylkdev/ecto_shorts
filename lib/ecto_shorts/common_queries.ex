defmodule EctoShorts.CommonQueries do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  @type source :: binary()

  @type query :: Ecto.Query.t()

  @type queryable :: Ecto.Queryable.t()

  @type source_queryable :: {source(), queryable()}

  @doc """
  Returns a `{source, Ecto.Queryable}` tuple given an `Ecto.Query` or `Ecto.Queryable`.

  ### Examples

      iex> require Ecto.Query
      ...> EctoShorts.Schemas.Comment |> Ecto.Query.from() |> EctoShorts.CommonSchemas.get_query_source()
      {"comments", EctoShorts.Schemas.Comment}
  """
  @spec get_query_source(query() | queryable() | source_queryable()) ::
          queryable() | source_queryable()
  def get_query_source(queryable) when is_atom(queryable), do: queryable
  def get_query_source(%{from: %{source: {source, queryable}}}), do: {source, queryable}
  def get_query_source(%{from: %{query: %{from: {source, queryable}}}}), do: {source, queryable}

  @doc """
  Returns a `Ecto.Queryable` given an `Ecto.Query` or `Ecto.Queryable`.

  ### Examples

  iex> require Ecto.Query
  ...> EctoShorts.Schemas.Comment |> Ecto.Query.from() |> EctoShorts.CommonSchemas.get_query_schema()
  EctoShorts.Schemas.Comment
  """
  @spec get_query_schema(query_or_queryable :: query() | queryable()) :: queryable()
  def get_query_schema(%{from: %{source: {_, query}}}), do: get_query_schema(query)
  def get_query_schema(%{from: %{query: %{from: {_, schema_module}}}}), do: schema_module
  def get_query_schema(queryable) when is_atom(queryable), do: queryable
end
