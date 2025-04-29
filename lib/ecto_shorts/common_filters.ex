defmodule EctoShorts.CommonFilters do
  @moduledoc """
  Data-driven query composition for Ecto.

  `EctoShorts.CommonFilters` provides a declarative interface for building
  Ecto queries using maps and keyword lists. This module allows you to
  compose complex queries expressions without writing Ecto's query DSL
  directly.

  This reduces the boilerplate and allows you to build APIs or query
  interfaces where filter parameters are provided as data, such as from
  user input or external sources.

  For example, instead of writing:

  ```elixir
  from p in Post,
    where: p.id == 1,
    join: c in assoc(p, :comments),
    where: c.body == "example"
  ```

  You can express the same logic as:

  ```elixir
  %{id: 1, comments: %{body: "example"}}
  ```

  and pass it to `convert_params_to_filter/2`.

  ## Examples

  Find a post by ID:

  ```elixir
  iex> EctoShorts.CommonFilters.convert_params_to_filter(MyApp.Post, %{id: 1})
  #Ecto.Query<from p in MyApp.Post, where: p.id == 1>
  ```

  Join on an association and filter nested fields:

  ```elixir
  iex> EctoShorts.CommonFilters.convert_params_to_filter(MyApp.Post, %{id: 1, comments: %{id: [1, 2], body: %{ilike: "example"}}})
  #Ecto.Query<from p in MyApp.Post,
    join: c in assoc(p, :comments),
    where: p.id == 1 and c.id in [1, 2]
      and ilike(c.body, "%example%")>
  ```

  This api is split into two main components:

    * `EctoShorts.QueryBuilder.Common` — Provides semantic based filtering
      for common tasks such as pagination and finding records within a
      certain range.

    * `EctoShorts.QueryBuilder.Schema` — Provides schema specific filtering
      on fields, as well as the ability to join on associations/subqueries.
      This also provides access to other ecto query based api functionality.
  """

  alias EctoShorts.{
    CommonSchemas,
    QueryBuilder,
    QueryBuilder.Common,
    QueryBuilder.Schema
  }

  @behaviour EctoShorts.QueryBuilder

  @type source :: binary()

  @type query :: Ecto.Query.t()

  @type queryable :: Ecto.Queryable.t()

  @type source_queryable :: {source(), queryable()}

  @type binding :: atom() | nil

  @type prefix :: binary()

  @type schema_module :: module()

  @type key :: atom()

  @type value :: any()

  @type params :: keyword() | map()

  @type opts :: keyword()

  @common_filters Common.filters()

  @doc """
  Converts a map or keyword list of parameters into an Ecto query.

  ## Examples

      iex> convert_params_to_filter(Post, %{title: "Hello"})
      #Ecto.Query<from p in Post, where: p.title == "Hello">

      iex> convert_params_to_filter(Post, [status: "published"])
      #Ecto.Query<from p in Post, where: p.status == "published">
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

  def convert_params_to_filter(query, params, opts) do
    convert_params_to_filter(
      query,
      CommonSchemas.get_schema_queryable(query),
      params,
      opts
    )
  end

  @doc """
  Converts a map or keyword list of parameters into an Ecto query.

  ## Examples

      iex> convert_params_to_filter(query, Post, %{title: "Hello"}, [])
      #Ecto.Query<from p in Post, where: p.title == "Hello">

      iex> convert_params_to_filter(query, Comment, [post_id: 1], [])
      #Ecto.Query<from c in Comment, where: c.post_id == 1>
  """
  @spec convert_params_to_filter(
          query() | queryable() | source_queryable(),
          schema_module(),
          params(),
          opts()
        ) :: query() | queryable()
  def convert_params_to_filter(query, schema_module, params, opts) when is_list(params) do
    convert_params_to_filter(
      query,
      schema_module,
      Map.new(params),
      opts
    )
  end

  def convert_params_to_filter(query, _schema_module, params, _opts) when params === %{} do
    query
  end

  def convert_params_to_filter(query, schema_module, params, opts) do
    params
    |> Map.to_list()
    |> ensure_last_is_final_filter()
    |> Enum.reduce(query, fn {key, value}, query ->
      QueryBuilder.build_query(
        query,
        params[:as],
        schema_module,
        key,
        value,
        opts
      )
    end)
  end

  @impl EctoShorts.QueryBuilder
  @doc """
  Builds a query based on a filter key and value.

  This function implements the `EctoShorts.QueryBuilder` behaviour and
  serves as a router to either common filters or schema-specific filters
  based on the key. If the key matches one of the common filters, it
  delegates to `Common.build_query/5`. Otherwise, it uses
  `Schema.build_query/5` for schema-specific filtering.

  ## Examples

      iex> build_query(Post, :post, Post, :limit, 10)
      #Ecto.Query<from p in Post, limit: 10>

      iex> build_query(Post, :post, Post, :title, "Hello")
      #Ecto.Query<from p in Post, where: p.title == "Hello">
  """
  @spec build_query(
          query() | queryable() | source_queryable(),
          binding() | nil,
          schema_module(),
          key(),
          value()
        ) :: query() | queryable()
  def build_query(query, current_binding, schema_module, key, value)
      when key in @common_filters do
    Common.build_query(query, current_binding, schema_module, key, value)
  end

  def build_query(query, current_binding, schema_module, key, value) do
    Schema.build_query(query, current_binding, schema_module, key, value)
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
