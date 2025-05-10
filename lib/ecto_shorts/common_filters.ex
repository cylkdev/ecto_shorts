defmodule EctoShorts.CommonFilters do
  @moduledoc """
  Data-driven query composition for Ecto.

  `EctoShorts.CommonFilters` provides a declarative interface for building
  Ecto queries using maps or keyword lists. It enables dynamic query generation
  without requiring direct use of Ecto’s query DSL.

  This reduces boilerplate and makes it easier to build data-driven APIs where
  filters are derived from user input or external sources.

  For example, rather than writing:

  ```elixir
  from p in Post,
    where: p.id == 1,
    join: c in assoc(p, :comments),
    where: c.body == "example"
  ```

  You can represent the same logic with:

  ```elixir
  %{id: 1, comments: %{body: "example"}}
  ```

  and pass it to convert_params_to_filter/2.

  ## Example Usage

  Find a post by ID:

  ```elixir
  iex> EctoShorts.CommonFilters.convert_params_to_filter(MyApp.Post, %{id: 1})
  #Ecto.Query<from p in MyApp.Post, where: p.id == 1>
  ```

  Join on an association and filter nested fields:

  ```elixir
  iex> EctoShorts.CommonFilters.convert_params_to_filter(MyApp.Post, %{
  ...>   id: 1,
  ...>   comments: %{id: [1, 2], body: %{ilike: "example"}}
  ...> })
  #Ecto.Query<from p in MyApp.Post,
    join: c in assoc(p, :comments),
    where: p.id == 1 and c.id in [1, 2]
      and ilike(c.body, "%example%")>
  ```

  ## Filters

  The filter API is split into two components:

    * `EctoShorts.QueryBuilders.Common` — Handles common filters like
    pagination, ordering, and range-based conditions.

    * `EctoShorts.QueryBuilders.Schema` — Handles schema-specific filters and
    joins on associations or subqueries.
  """

  alias EctoShorts.{
    CommonSchema,
    QueryBuilder,
    QueryBuilders.Common,
    QueryBuilders.Schema
  }

  @typedoc """
  The source name for a queryable, typically the name of a database table.
  Used in `{schema_source, schema_module}` tuples for abstract or dynamic schemas.
  """
  @type query_source :: binary()

  @typedoc """
  An Ecto query struct (`%Ecto.Query{}`) representing a composed query.
  """
  @type query :: Ecto.Query.t()

  @typedoc """
  An Ecto queryable, such as a schema module or an existing query.
  This is typically the starting point for query composition.
  """
  @type queryable :: Ecto.Queryable.t()

  @typedoc """
  A tuple combining a custom source name and a queryable, used for abstract schemas.
  Example: `{"my_posts", MyApp.Post}`.
  """
  @type source_queryable :: {query_source(), queryable()}

  @typedoc """
  An optional alias used to refer to a binding in the query.
  Often derived from the `:as` field in filter parameters.
  """
  @type binding_alias :: atom()

  @typedoc """
  A database prefix, used to namespace queries (e.g. for multi-tenancy).
  """
  @type prefix :: binary()

  @typedoc """
  A schema module representing an Ecto schema, e.g. `MyApp.Post`.
  """
  @type schema_module :: Ecto.Queryable.t()

  @typedoc """
  A filter key used in param-based query building. This may be a field name,
  a virtual key (like `:limit`), or an association name.
  """
  @type key :: atom()

  @typedoc """
  The value associated with a filter key. May be a scalar, list, map (e.g. `%{ilike: ...}`),
  or nested structure.
  """
  @type value :: any()

  @typedoc """
  Parameters used to construct the query. May be a map or keyword list, and
  can include nested fields and filter expressions.
  """
  @type params :: keyword() | map()

  @typedoc """
  A keyword-list of options.
  """
  @type opts :: keyword()

  @behaviour EctoShorts.QueryBuilder

  @default_query_builder_adapter __MODULE__

  @common_filters Common.filters()
  @schema_filters Schema.filters()
  @filters @common_filters ++ @schema_filters

  @doc """
  Converts a map or keyword list of filter parameters into an Ecto query.

  This function handles both flat and nested filtering expressions. Parameters
  can include association filters, `ilike` conditions, range comparisons, and
  other supported expressions.

  Keyword lists are internally converted to maps.
  """
  @spec convert_params_to_filter(
          query() | queryable() | source_queryable(),
          params()
        ) :: query() | queryable()
  @spec convert_params_to_filter(
          query() | queryable() | source_queryable(),
          params(),
          opts()
        ) :: query() | queryable()
  def convert_params_to_filter(query, params, opts \\ [])

  def convert_params_to_filter(query, params, _opts) when params === %{} or params === [] do
    query
  end

  def convert_params_to_filter(query, values, opts) when is_list(values) do
    convert_params_to_filter(query, Map.new(values), opts)
  end

  def convert_params_to_filter(query, params, opts) do
    schema_module = CommonSchema.module_for_schema(query)

    {query, params} = Map.pop(params, :query, query)

    {current_binding, params} = Map.pop(params, :as)

    params
    |> Map.to_list()
    |> ensure_last_is_final_filter()
    |> Enum.reduce(query, fn {key, value}, query ->
      reduce_filter(query, current_binding, schema_module, key, value, opts)
    end)
  end

  defp reduce_filter(query, current_binding, schema_module, key, value, opts) do
    opts
    |> query_builder_adapter()
    |> QueryBuilder.build_query(
      query,
      current_binding,
      schema_module,
      key,
      value,
      opts
    )
  end

  defp query_builder_adapter(opts) do
    opts[:query_builder_adapter] ||
      EctoShorts.Config.query_builder_adapter() ||
      @default_query_builder_adapter
  end

  @impl EctoShorts.QueryBuilder
  @doc since: "2.5.0"
  @doc """
  ...
  """
  def filters, do: @filters

  @doc since: "2.5.0"
  @doc """
  ...
  """
  def filters(opts) do
    opts
    |> query_builder_adapter()
    |> QueryBuilder.filters()
  end

  @impl EctoShorts.QueryBuilder
  @doc since: "2.5.0"
  @doc """
  Builds a query based on a single filter key and value.

  The schema module given as an argument must be the schema module
  for the query being targeted by the current_binding argument.
  If the current_binding is nil then the binding is the root query
  and the schema module must be for the root query. If the current_binding
  given is an association then the schema module must be for that
  association schema module.

  ## Examples

      iex> build_query(Post, :post, Post, :limit, 10)
      #Ecto.Query<from p in Post, limit: 10>

      iex> build_query(Post, :post, Post, :title, "Hello")
      #Ecto.Query<from p in Post, where: p.title == "Hello">
  """
  @spec build_query(
          query() | queryable() | source_queryable(),
          binding_alias(),
          schema_module(),
          key(),
          value()
        ) :: query() | queryable()
  @spec build_query(
          query() | queryable() | source_queryable(),
          binding_alias(),
          schema_module(),
          key(),
          value(),
          opts()
        ) :: query() | queryable()
  def build_query(query, current_binding, schema_module, key, value, opts \\ [])

  def build_query(query, current_binding, schema_module, key, value, opts)
      when key in @common_filters do
    QueryBuilder.build_query(
      Common,
      query,
      current_binding,
      schema_module,
      key,
      value,
      opts
    )
  end

  def build_query(query, current_binding, schema_module, key, value, opts) do
    QueryBuilder.build_query(
      Schema,
      query,
      current_binding,
      schema_module,
      key,
      value,
      opts
    )
  end

  defp ensure_last_is_final_filter(params) do
    if Keyword.has_key?(params, :last) do
      params
      |> Keyword.delete(:last)
      |> Kernel.++(last: params[:last])
    else
      params
    end
  end
end
