defmodule EctoShorts.QueryBuilder do
  @moduledoc """
  Specifies the query builder API required from adapters.
  """
  @moduledoc since: "2.5.0"

  @type t :: module()

  @type key :: atom()
  @type value :: any()
  @type source :: binary()
  @type query :: Ecto.Query.t()
  @type queryable :: Ecto.Queryable.t()
  @type source_queryable :: {source(), queryable()}

  @doc """
  Adds an expression to they query given a filter key and value.

  The `Ecto.Query` returned should should be same as if it was
  written using the `Ecto.Query` dsl.

  For example the following function call

  ```elixir
  iex> Ecto.Query.from(c in EctoShorts.Support.Schemas.Comment, where: c.id == 1)
  #Ecto.Query<from c0 in EctoShorts.Support.Schemas.Comment, where: c0.id == 1>
  ```

  is equivalent to

  ```elixir
  iex> EctoShorts.QueryBuilder.create_schema_filter(
  ...>   EctoShorts.QueryBuilder.Schema,
  ...>   EctoShorts.Support.Schemas.Comment,
  ...>   :id,
  ...>   1
  ...> )
  #Ecto.Query<from c0 in EctoShorts.Support.Schemas.Comment, where: c0.id == ^1>
  ```
  """
  @callback create_schema_filter(query() | queryable() | source_queryable(), key(), value()) :: query()

  @doc """
  Invokes the callback function `c:EctoShorts.QueryBuilder.create_schema_filter/3`.

  Returns an `Ecto.Query`.

  ### Examples

      iex> EctoShorts.QueryBuilder.create_schema_filter(
      ...>   EctoShorts.QueryBuilder.Common,
      ...>   EctoShorts.Support.Schemas.Comment,
      ...>   :first,
      ...>   1_000
      ...> )
      #Ecto.Query<from c0 in EctoShorts.Support.Schemas.Comment, limit: ^1000>
  """
  @spec create_schema_filter(t(), query() | queryable() | source_queryable(), key(), value()) :: query()
  def create_schema_filter(module, query, key, value) do
    module.create_schema_filter(query, key, value)
  end
end
