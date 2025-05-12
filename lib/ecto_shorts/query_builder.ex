defmodule EctoShorts.QueryBuilder do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Defines the query builder behavior for custom filter adapters.

  Modules implementing `EctoShorts.QueryBuilder` are responsible for
  generating query expressions based on filter keys and values. This allows
  filtering logic to be customized per schema or application context.

  This module also provides utility functions like `apply_expressions/3` for
  recursively applying transformations across nested filter structures.
  """

  @type query :: Ecto.Query.t()
  @type schema_module :: Ecto.Queryable.t()
  @type schema_source :: binary()
  @type sourceable :: schema_module() | {schema_source(), schema_module()}
  @type queryable_source :: query() | schema_module()
  @type query_source :: query() | sourceable()
  @type binding_alias :: atom()

  @type adapter :: module()
  @type key :: atom()
  @type value :: any()
  @type params :: map() | keyword()
  @type filter :: atom()
  @type filters :: list(filter())
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
              query_source(),
              binding_alias(),
              schema_module(),
              key(),
              value(),
              opts()
            ) :: queryable_source()

  @doc """
  Returns a list of supported filters that can be used with the
  configured query builder.
  """
  @spec filters(adapter()) :: filters()
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
          adapter(),
          query_source(),
          binding_alias(),
          schema_module(),
          key(),
          value()
        ) :: queryable_source()
  @spec build_query(
          adapter(),
          query_source(),
          binding_alias(),
          schema_module(),
          key(),
          value(),
          opts()
        ) :: queryable_source()
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
