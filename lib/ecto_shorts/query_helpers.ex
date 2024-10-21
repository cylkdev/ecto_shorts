defmodule EctoShorts.QueryHelpers do
  @moduledoc """
  Helper functions for ecto queries.
  """
  @moduledoc since: "2.5.0"
  alias Ecto.Query

  require Ecto.Query

  @type source :: binary()
  @type params :: map()
  @type query :: Ecto.Query.t()
  @type queryable :: Ecto.Queryable.t()
  @type source_queryable :: {source(), queryable()}
  @type filter_key :: atom()
  @type filter_value :: any()

  @doc """
  Returns a `{source, Ecto.Queryable}` tuple given an `Ecto.Query` or `Ecto.Queryable`.

  ### Examples

      iex> require Ecto.Query
      ...> EctoShorts.Support.Schemas.Comment |> Ecto.Query.from() |> EctoShorts.QueryHelpers.get_source_queryable()
      {"comments", EctoShorts.Support.Schemas.Comment}
  """
  @spec get_source_queryable(query :: Ecto.Query.t() | Ecto.Queryable.t()) :: {binary(), Ecto.Queryable.t()}
  def get_source_queryable(%{from: %{source: {source, queryable}}}), do: {source, queryable}
  def get_source_queryable(%{from: %{query: %{from: {source, queryable}}}}), do: {source, queryable}
  def get_source_queryable(query), do: query |> Query.from() |> get_source_queryable()

  @doc """
  Returns a `Ecto.Queryable` given an `Ecto.Query` or `Ecto.Queryable`.

  ### Examples

      iex> require Ecto.Query
      ...> EctoShorts.Support.Schemas.Comment |> Ecto.Query.from() |> EctoShorts.QueryHelpers.get_queryable()
      EctoShorts.Support.Schemas.Comment
  """
  @spec get_queryable(
    query :: Ecto.Query.t() | Ecto.Queryable.t()
  ) :: Ecto.Queryable.t()
  def get_queryable(%_{} = query) do
    with {_source, queryable} <- get_source_queryable(query) do
      queryable
    end
  end

  def get_queryable(query), do: query

  @doc """
  Returns an Ecto.Query for the given schema.

  ### Options

    * `schema_prefix` - Sets the prefix on the `from` expression.
      See the [ecto documentation](https://hexdocs.pm/ecto/multi-tenancy-with-query-prefixes.html#per-from-join-prefixes) for more information.

    * `query_prefix` - Sets the prefix on the `query`.
      See the [ecto documentation](https://hexdocs.pm/ecto/multi-tenancy-with-query-prefixes.html#per-query-and-per-struct-prefixes) for more information.

  See `&build_query_from/2` for more options.

  ### Examples

      iex> EctoShorts.QueryHelpers.build_query_from(EctoShorts.Support.Schemas.Comment)
      iex> EctoShorts.QueryHelpers.build_query_from(EctoShorts.Support.Schemas.Comment, query_prefix: "query_prefix")

      iex> EctoShorts.QueryHelpers.build_query_from({"comments", EctoShorts.Support.Schemas.Comment})
      iex> EctoShorts.QueryHelpers.build_query_from({"comments", EctoShorts.Support.Schemas.Comment}, query_prefix: "query_prefix")

      iex> require Ecto.Query
      ...> EctoShorts.Support.Schemas.Comment |> Ecto.Query.from() |> EctoShorts.QueryHelpers.build_query_from()

      iex> require Ecto.Query
      ...> EctoShorts.Support.Schemas.Comment |> Ecto.Query.from() |> EctoShorts.QueryHelpers.build_query_from(query_prefix: "query_prefix")
  """
  @spec build_query_from(
    query :: query() | queryable() | source_queryable(),
    opts :: keyword()
  ) :: Ecto.Query.t()
  def build_query_from(query, opts \\ [])

  def build_query_from(%_{} = query, opts) do
    put_query_prefix(query, opts)
  end

  def build_query_from(query, opts) do
    query
    |> put_schema_prefix(opts)
    |> put_query_prefix(opts)
  end

  defp put_schema_prefix(query, opts) do
    case opts[:schema_prefix] do
      nil -> Query.from(query)
      schema_prefix -> Query.from(query, prefix: ^schema_prefix)
    end
  end

  defp put_query_prefix(query, opts) do
    case opts[:query_prefix] do
      nil -> query
      query_prefix -> Query.put_query_prefix(query, query_prefix)
    end
  end
end
