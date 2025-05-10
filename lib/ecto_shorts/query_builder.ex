defmodule EctoShorts.QueryBuilder do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Defines the query builder behavior for custom filter adapters.

  Modules implementing `EctoShorts.QueryBuilder` are responsible for
  generating query expressions based on filter keys and values. This allows
  filtering logic to be customized per schema or application context.

  This module also provides utility functions like `apply_expression/3` for
  recursively applying transformations across nested filter structures.
  """

  @typedoc """
  The behavior module responsible for generating queries from filters.
  """
  @type t :: module()

  @typedoc """
  The source name of a queryable (e.g., table name). Used in `{schema_source, schema_module}`.
  """
  @type sourceable :: binary()

  @typedoc """
  An Ecto query struct (`%Ecto.Query{}`) built through filter transformations.
  """
  @type query :: Ecto.Query.t()

  @typedoc """
  An Ecto-compatible queryable, such as a schema module or existing query.
  """
  @type queryable :: Ecto.Queryable.t()

  @typedoc """
  A tuple of `{schema_source, schema_module}` used for dynamic or abstract schemas.
  """
  @type source_queryable :: {sourceable(), queryable()}

  @typedoc """
  An alias used to refer to a named binding in a query (typically the `:as` value).
  """
  @type binding_alias :: atom()

  @typedoc """
  A schema module representing an Ecto schema, e.g. `MyApp.Post`.
  """
  @type schema_module :: Ecto.Queryable.t()

  @typedoc """
  A single filter key (field name or custom directive like `:limit`).
  """
  @type key :: atom()

  @typedoc """
  A value associated with a filter key. May be a scalar, list, map, etc.
  """
  @type value :: any()

  @type filter :: atom()

  @type filters :: list(filter())

  @typedoc """
  Options passed to the query builder. May include `:query_builder_adapter`,
  `:prefix`, or other adapter-specific flags.
  """
  @type opts :: keyword()

  @doc """
  Returns a list of supported filters that can be used with this query builder.
  """
  @callback filters :: filters()

  @doc """
  Defines the behavior for building a query from a single filter key and value.

  Adapters implementing this callback are responsible for translating the key and
  value into a query expression. This could be a `where`, `limit`, `order_by`,
  or even a join or subquery, depending on the key and how the adapter is designed.

  ## Parameters

    * `query` — An existing query, queryable, or `{schema_source, schema_module}` tuple.

    * `binding` — An optional alias used to refer to the query binding (e.g. `:post`).

    * `queryable` — The schema module or queryable the filters apply to.

    * `key` — A filter key, such as a field name (`:title`) or virtual key (`:limit`).

    * `value` — The value to filter by. This may be a scalar, list, or expression map
      like `%{ilike: "foo"}`.

  ## Return

  A new query with the filter applied.

  ## Example

      def build_query(query, :post, Post, :title, %{ilike: "hello"}) do
        where(query, [post: p], ilike(p.title, ^"%hello%"))
      end
  """
  @callback build_query(
              query() | queryable() | source_queryable(),
              binding_alias(),
              schema_module(),
              key(),
              value(),
              opts()
            ) :: query() | queryable()

  @doc """
  Returns a list of supported filters that can be used with the
  configured query builder.
  """
  @spec filters(t()) :: filters()
  def filters(adapter), do: adapter.filters()

  @doc """
  Dispatches to the configured query builder adapter to build a query
  based on a filter key and value.

  The adapter used to build the query is chosen in the following order:

    * If `:query_builder_adapter` is present in the options, it is used.

    * Otherwise, the `:query_builder_adapter` option configured in application
      environment is checked.

    * If neither is set, the default is `EctoShorts.CommonFilters`.

  ## Examples

      iex> EctoShorts.QueryBuilder.build_query(Post, :post, Post, :limit, 10)
      #Ecto.Query<from p in Post, limit: 10>
  """
  @spec build_query(
          t(),
          query() | queryable() | source_queryable(),
          binding_alias(),
          schema_module(),
          key(),
          value()
        ) :: query() | queryable()
  @spec build_query(
          t(),
          query() | queryable() | source_queryable(),
          binding_alias(),
          schema_module(),
          key(),
          value(),
          opts()
        ) :: query() | queryable()
  def build_query(adapter, query, binding_alias, schema_module, key, value, opts \\ []) do
    adapter.build_query(
      query,
      binding_alias,
      schema_module,
      key,
      value,
      opts
    )
  end
end
