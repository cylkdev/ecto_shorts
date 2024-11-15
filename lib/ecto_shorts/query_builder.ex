defmodule EctoShorts.QueryBuilder do
  @moduledoc """
  Specifies the query builder API required from adapters.
  """
  @moduledoc since: "2.5.0"

  @type adapter :: module()
  @type filter_key :: atom()
  @type filter_value :: any()
  @type query :: Ecto.Query.t()
  @type queryable :: Ecto.Queryable.t()

  # @doc """
  # Adds an expression to an ecto query.

  # The expression added by this function is equivalent
  # to if it was written using the ecto query dsl.

  # For example the following query

  # ```elixir
  # iex> import Ecto.Query
  # ...> from p in EctoShorts.Schemas.Post, where: p.id == 1
  # #Ecto.Query<from p0 in EctoShorts.Schemas.Post, where: p0.id == 1>
  # ```

  # is equivalent to

  # ```elixir
  # EctoShorts.QueryBuilder.build_query(EctoShorts.Schemas.Post, :select, [:id])
  # ```
  # """
  # @callback build_query(query :: query(), key :: filter_key(), value :: filter_value()) :: query()

  @doc """
  Adds an expression to they query given a filter key and value.

  The `Ecto.Query` returned should should be same as if it was
  written using the `Ecto.Query` dsl.

  For example the following function call

  ```elixir
  iex> Ecto.Query.from(c in EctoShorts.Schemas.Comment, where: c.id == 1)
  #Ecto.Query<from c0 in EctoShorts.Schemas.Comment, where: c0.id == 1>
  ```

  is equivalent to

  ```elixir
  iex> EctoShorts.QueryBuilder.create_schema_filter(
  ...>   EctoShorts.QueryBuilder.Schema,
  ...>   EctoShorts.Schemas.Comment,
  ...>   :id,
  ...>   1
  ...> )
  #Ecto.Query<from c0 in EctoShorts.Schemas.Comment, where: c0.id == ^1>
  ```
  """
  @callback create_schema_filter(query(), filter_key(), filter_value()) :: query()

  @doc """
  Invokes the callback function `c:EctoShorts.QueryBuilder.create_schema_filter/3`.

  Returns an `Ecto.Query`.

  ### Examples

      iex> EctoShorts.QueryBuilder.create_schema_filter(
      ...>   EctoShorts.QueryBuilder.Common,
      ...>   EctoShorts.Schemas.Comment,
      ...>   :first,
      ...>   1_000
      ...> )
      #Ecto.Query<from c0 in EctoShorts.Schemas.Comment, limit: ^1000>
  """
  @spec create_schema_filter(adapter(), query(), filter_key(), filter_value()) :: query()
  def create_schema_filter(adapter, query, filter_key, filter_value) do
    adapter.create_schema_filter(query, filter_key, filter_value)
  end
end
