defmodule EctoShorts.QueryBuilder.Schema do
  @moduledoc since: "2.5.0"
  @moduledoc """
  # EctoShorts.QueryBuilder.Schema

  Provides a schema-centric implementation of the
  EctoShorts.QueryBuilder behaviour, enabling dynamic and
  composable query construction for Ecto schemas.

  This module defines a set of supported filters—such as join,
  select, select_merge, where, and or_where—and provides the
  build_query/4 function to apply these filters to an Ecto query
  based on user-supplied parameters.

  The build_query/4 function iterates over the params map or
  keyword list, applying each filter to the query in sequence.
  Each filter is dispatched to the appropriate handler based on
  its type, allowing for composable and dynamic query construction.

  ## Example Usage

  ```elixir
  params = %{
    select: %{name: true, profile: %{age: true}},
    where: %{active: true}
  }

  query = from(u in User)

  query = EctoShorts.QueryBuilder.Schema.build_query(
    query, nil, User, params
  )
  ```

  This will build a query that selects the user's name and nested
  profile age field, and applies a where clause for active users.

  See the build_query/4 function for details on supported filters.
  """

  alias EctoShorts.{
    CommonSchemas,
    QueryBuilder,
    CommonQueryExpressions
  }

  @type source :: binary()

  @type query :: Ecto.Query.t()

  @type queryable :: Ecto.Queryable.t()

  @type source_queryable :: {source(), queryable()}

  @type binding :: atom()

  @type binding_key :: atom() | binary()

  @type key :: atom()

  @type value :: any()

  @type params :: map() | keyword()

  @type filter :: :join | :select | :select_merge | :or_where

  @behaviour EctoShorts.QueryBuilder

  @filters ~w(
    join
    select
    select_merge
    or_where
  )a

  @doc """
  Returns a list of supported filters that can be used to build
  queries with this schema query builder.

  Supported filters:

    * `:join`: Join associations or related tables.

    * `:select`: Select specific fields or associations.

    * `:select_merge`: Merge additional fields into an existing
      select.

    * `:where`: Apply standard where clauses.

    * `:or_where`: Apply logical OR-based where clauses.

  ## Examples

      iex> EctoShorts.QueryBuilder.Schema.filters()
  """
  @spec filters :: [filter()]
  def filters, do: @filters

  @doc """
  Builds an Ecto query by applying a series of supported filters
  from the provided parameters.

  Iterates over the `params` map or keyword list, applying each
  filter (such as `:select`, `:where`, `:join`, etc.) to the query
  in sequence.
  Each filter is dispatched to the appropriate handler based on
  its type, allowing for composable and dynamic query construction.

  ## Example

      iex> params = %{select: %{name: true}, where: %{active: true}}
      ...> EctoShorts.QueryBuilder.Schema.build_query(query, :user, User, params)
  """
  @spec build_query(
          query :: query() | queryable() | source_queryable(),
          binding :: binding() | nil,
          schema_module :: queryable(),
          params :: params()
        ) :: query()
  def build_query(query, current_binding, schema_module, params) do
    Enum.reduce(params, query, fn {key, value}, query ->
      build_query(query, current_binding, schema_module, key, value)
    end)
  end

  @impl EctoShorts.QueryBuilder
  @doc """
  Applies a single filter to the Ecto query based on the given key
  and value.

  Determines whether the filter key corresponds to a query
  expression (such as `:select`, `:where`, etc.) or an association.
  Dispatches to the appropriate handler for query expressions or
  recursively applies filters to associations.

  ## Example

      iex> EctoShorts.QueryBuilder.build_query(query, :user, User, :where, %{active: true})

      iex> EctoShorts.QueryBuilder.build_query(query, :user, User, :select, %{name: true})
  """
  @spec build_query(
          query() | queryable() | source_queryable(),
          binding() | nil,
          queryable(),
          key(),
          value()
        ) :: query() | queryable()
  def build_query(query, current_binding, schema_module, key, value) do
    cond do
      query_expression_filter?(key) ->
        build_query_expression(query, current_binding, schema_module, key, value)

      association?(schema_module, key) ->
        build_assoc_filters(query, current_binding, schema_module, key, value)

      query_field?(schema_module, key) ->
        build_schema_filters(query, current_binding, schema_module, key, value)

      true ->
        message = """
        Invalid filter key provided.

        The key you passed is not a valid field or supported query filter for the given schema.

          schema: #{inspect(schema_module)}

          key: #{inspect(key)}

        The query will be unchanged and this key will be skipped.

        To resolve this, you can:

        - Remove the key if it’s unnecessary.

        - Use a supported custom filter, such as:

        #{Enum.map_join(@filters, "\n", &"* #{&1}")}

        - Use a valid schema field, such as:

        #{Enum.map_join(schema_module.__schema__(:query_fields), "\n", &"* #{&1}")}
        """

        assoc_warning_message =
          if schema_module.__schema__(:associations) !== [] do
            """
            - Use a valid association, such as:

            #{Enum.map_join(schema_module.__schema__(:associations), "\n", &"* #{&1}")}
            """
          else
            ""
          end

        EctoShorts.Utils.Logger.warning(__MODULE__, message <> assoc_warning_message,
          stacktrace: true
        )

        query
    end
  end

  defp build_schema_filters(query, current_binding, schema_module, key, value) do
    QueryBuilder.apply_expressions(query, value, fn query, value ->
      apply_schema_filter(query, current_binding, schema_module, key, value)
    end)
  end

  defp apply_schema_filter(query, current_binding, _schema_module, key, {operator, value}) do
    CommonQueryExpressions.where(query, current_binding, key, operator, value)
  end

  defp apply_schema_filter(query, current_binding, _schema_module, key, value) do
    CommonQueryExpressions.where(query, current_binding, key, :==, value)
  end

  defp build_query_expression(query, current_binding, schema_module, :join, value) do
    case value do
      {:association, params} ->
        Enum.reduce(params, query, fn {key, value}, query ->
          build_assoc_filters(query, current_binding, schema_module, key, value)
        end)

      {:subquery, params} ->
        build_subquery_filters(query, current_binding, schema_module, params)

      params ->
        Enum.reduce(params, query, fn {key, value}, query ->
          build_query_expression(query, current_binding, schema_module, :join, {key, value})
        end)
    end
  end

  defp build_query_expression(query, current_binding, _schema_module, :or_where, value) do
    CommonQueryExpressions.or_where(query, current_binding, value)
  end

  defp build_query_expression(query, current_binding, _schema_module, :select, value) do
    CommonQueryExpressions.select(query, current_binding, value)
  end

  defp build_query_expression(query, current_binding, _schema_module, :select_merge, value) do
    CommonQueryExpressions.select_merge(query, current_binding, value)
  end

  defp build_assoc_filters(query, current_binding, schema_module, key, params) do
    join_association(
      query,
      current_binding,
      schema_module,
      key,
      params,
      fn query, assoc_binding, assoc_schema_module, params ->
        build_query(query, assoc_binding, assoc_schema_module, params)
      end
    )
  end

  defp build_subquery_filters(
         query,
         current_binding,
         schema_module,
         %{from: from} = params
       ) do
    join_subquery(
      query,
      current_binding,
      schema_module,
      from,
      params,
      fn query, subquery_binding, subquery_schema_module, params ->
        build_query(query, subquery_binding, subquery_schema_module, params)
      end
    )
  end

  @doc """
  Joins an association on the given key and applies additional
  filters to the joined association.

  This function determines the schema module for the association,
  sets up a named binding for the join, and applies the provided
  filter function (`fun`) to the joined association's queryable,
  binding, and parameters.

  ## Example

      iex> EctoShorts.QueryBuilder.join_association(query, :user, User, :posts, %{where: %{published: true}}, fn query, assoc_binding, assoc_schema_module, params ->
        build_query(query, assoc_binding, assoc_schema_module, params)
      end)
  """
  @spec join_association(
          query() | queryable() | source_queryable(),
          binding() | nil,
          queryable(),
          key(),
          params(),
          function()
        ) :: query()
  def join_association(query, current_binding, schema_module, key, params, fun) do
    assoc_schema_module = get_association_schema(schema_module, key)

    {assoc_binding, params} = Map.pop(params, :as)

    assoc_binding = assoc_binding || :"#{named_binding(key)}"

    join_opts =
      params
      |> Map.take([:on, :qualifier, :prefix])
      |> Map.to_list()

    query
    |> CommonQueryExpressions.join({current_binding, assoc_binding}, :association, key, join_opts)
    |> fun.(assoc_binding, assoc_schema_module, Map.drop(params, [:on, :qualifier, :prefix]))
  end

  @doc """
  Joins a subquery and applies additional filters to it.

  This function extracts the subquery schema module and binding,
  sets up the join, and applies the provided filter function (`fun`)
  to the subquery.

  ## Example

      iex> EctoShorts.QueryBuilder.join_subquery(query, :user, User, Post, %{on: ...}, fn query, subquery_binding, subquery_schema_module, params ->
        build_query(query, subquery_binding, subquery_schema_module, params)
      end)
  """
  @spec join_subquery(
          query() | queryable() | source_queryable(),
          binding() | nil,
          queryable(),
          query() | queryable() | source_queryable(),
          params(),
          function()
        ) :: query()
  def join_subquery(
        query,
        current_binding,
        _schema_module,
        from,
        params,
        fun
      ) do
    subquery_schema_module = CommonSchemas.get_schema_queryable(from)

    {subquery_binding, params} = Map.pop(params, :as)

    subquery_binding =
      with nil <- subquery_binding do
        :"#{named_binding_from_module(subquery_schema_module)}"
      end

    join_opts =
      params
      |> Map.take([:on, :qualifier, :prefix])
      |> Map.to_list()

    query
    |> CommonQueryExpressions.join(
      {current_binding, subquery_binding},
      :subquery,
      from,
      join_opts
    )
    |> fun.(
      subquery_binding,
      subquery_schema_module,
      Map.drop(params, [:on, :qualifier, :prefix])
    )
  end

  @doc """
  Returns the related schema module for an association field on a
  schema.

  If the association is a `through` association, recursively
  resolves the related schema module by traversing the association
  path. Otherwise, returns the directly related schema module.

  ## Example

      iex> EctoShorts.QueryBuilder.Schema.get_association_schema(User, :posts)
      MyApp.Post
  """
  @spec get_association_schema(queryable(), key()) :: queryable()
  def get_association_schema(schema_module, key) do
    case schema_module.__schema__(:association, key) do
      %{through: [field1, field2]} ->
        schema_module
        |> get_association_schema(field1)
        |> get_association_schema(field2)

      %{related: related} ->
        related
    end
  end

  @doc """
  Generates a default named binding string for a module.

  ## Example

      iex> EctoShorts.QueryBuilder.Schema.named_binding_from_module(MyApp.User)
      "ecto_shorts_user"
  """
  @spec named_binding_from_module(module()) :: binary()
  def named_binding_from_module(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
    |> named_binding()
  end

  @doc """
  Returns a string that can be used as a named binding in Ecto
  queries for the given key.

  ## Example

      iex> EctoShorts.QueryBuilder.Schema.named_binding(:user)
      "ecto_shorts_user"
  """
  @spec named_binding(binding_key()) :: binary()
  def named_binding(key) do
    "ecto_shorts_#{key}"
  end

  defp query_expression_filter?(key) do
    key in @filters
  end

  defp query_field?(schema_module, key) do
    key in schema_module.__schema__(:query_fields)
  end

  defp association?(schema_module, key) do
    key in schema_module.__schema__(:associations)
  end
end
