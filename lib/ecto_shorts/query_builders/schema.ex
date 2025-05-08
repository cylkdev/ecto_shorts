defmodule EctoShorts.QueryBuilders.Schema do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Provides a schema-centric implementation of the `EctoShorts.QueryBuilder`
  behaviour for building composable Ecto queries.

  This implementation handles schema-aware filters including joins,
  field-level filtering, and nested selects. It supports filters like:

    * `:join` – Joins associations or subqueries
    * `:select` – Projects specific fields from a schema
    * `:select_merge` – Merges additional fields into a `select` expression
    * `:where` – Filters records by field conditions (AND logic)
    * `:or_where` – Filters records by field conditions using OR logic

  ## Example Usage

      iex> params = %{select: [:id], where: %{id: %{>: 123}}}
      ...> EctoShorts.QueryBuilders.Schema.build_query(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, params)
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, where: p0.id > ^123, select: struct(p0, [:id])>

  This will build a query that selects the user's name and profile age,
  and applies a `where` clause to filter for active users.
  """

  alias EctoShorts.{
    CommonQuery,
    CommonSchemas,
    ExpressionBuilder
  }

  @type source :: binary()
  @type query :: Ecto.Query.t()
  @type queryable :: Ecto.Queryable.t()
  @type source_queryable :: {source(), queryable()}
  @type binding_alias :: atom()
  @type binding_key :: atom() | binary()
  @type key :: atom()
  @type value :: any()
  @type params :: map()
  @type opts :: keyword()
  @type filter :: :join | :select | :select_merge | :or_where

  @behaviour EctoShorts.QueryBuilder

  @query_filters ~w(
    from
    join
    select
    select_merge
    or
    or_where
    where
  )a

  @impl EctoShorts.QueryBuilder
  @doc """
  Returns the list of supported filters that can be used in schema-aware queries.

  These filters delegate to `EctoShorts.CommonQuery` and enable dynamic,
  field-driven construction of queries in a composable and reusable way.

  ### Filter behaviors

    * `:join` – Joins an association or subquery into the query. Supports binding aliasing and prefix options.
    * `:select` – Selects fields from the schema or association. Supports nested field selection using maps.
    * `:select_merge` – Adds additional fields to an existing select expression without overwriting previous fields.
    * `:where` – Adds one or more `AND` conditions to the query using field-value pairs or operator tuples.
    * `:or_where` – Adds one or more `OR` conditions, dynamically constructing boolean expressions across fields.

  ## Examples

      iex> EctoShorts.QueryBuilders.Schema.filters()
      [:from, :join, :select, :select_merge, :or, :or_where, :where]
  """
  @spec filters :: [filter()]
  def filters, do: @query_filters

  @doc """
  Dynamically applies a series of filters to a base Ecto query.

  This function accepts a base query and a map or keyword list of filters,
  and applies them in sequence. Each key in the list must correspond to a
  supported query expression or a schema field/association.

  ## Examples

      iex> params = %{select: [:id], where: %{title: "example"}}
      ...> EctoShorts.QueryBuilders.Schema.build_query(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, params)
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, where: p0.title == ^"example", select: struct(p0, [:id])>
  """
  @spec build_query(
          query() | queryable() | source_queryable(),
          binding_alias() | nil,
          queryable(),
          params()
        ) :: query()
  @spec build_query(
          query() | queryable() | source_queryable(),
          binding_alias() | nil,
          queryable(),
          params(),
          opts()
        ) :: query()
  def build_query(query, current_binding, schema_module, params, opts \\ [])

  def build_query(query, current_binding, schema_module, params, opts) do
    Enum.reduce(params, query, fn {key, value}, query ->
      build_query(query, current_binding, schema_module, key, value, opts)
    end)
  end

  @impl EctoShorts.QueryBuilder
  @doc """
  Applies a single filter to the query based on a field, association, or supported expression.

  This function determines how to handle the provided filter key:

    * If the key matches a supported filter (e.g., `:select`, `:join`),
      it applies the appropriate query transformation using
      `EctoShorts.CommonQuery`.

    * If the key matches an association in the schema, the query will be
      joined and any nested filters applied to that association.

    * If the key matches a schema field, it applies a standard `where` condition
      with the value or operator/value tuple.

    * If the key is not recognized, it is skipped and a warning is logged.

  ## Examples

      # joins association key
      iex> EctoShorts.QueryBuilders.Schema.build_query(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :comments, %{id: 1}, [])
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments), as: :ecto_shorts_comments, where: c1.id == ^1>

      # joins association key with operator
      iex> EctoShorts.QueryBuilders.Schema.build_query(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :comments, %{id: %{>=: 2}}, [])
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments), as: :ecto_shorts_comments, where: c1.id >= ^2>

      # join on association using query filter and on clause
      iex> EctoShorts.QueryBuilders.Schema.build_query(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :join, %{association: %{comments: %{on: %{id: 2}}}}, [])
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments), as: :ecto_shorts_comments, on: c1.id == ^2>

      # join on association using query filter, on clause and operator
      iex> EctoShorts.QueryBuilders.Schema.build_query(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :join, %{association: %{comments: %{on: %{id: %{>=: 2}}}}}, [])
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments), as: :ecto_shorts_comments, on: c1.id >= ^2>

      # join on association using query filter, where clause and operator
      iex> EctoShorts.QueryBuilders.Schema.build_query(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :join, %{association: %{comments: %{id: %{>=: 2}}}}, [])
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments), as: :ecto_shorts_comments, where: c1.id >= ^2>

      # join one subquery
      iex> EctoShorts.QueryBuilders.Schema.build_query(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :join, %{subquery: %{as: :comments, query: EctoShorts.Schema.Comment, where: %{id: 2}}}, [])
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in subquery(from c0 in EctoShorts.Schema.Comment), as: :comments, on: true, where: c1.id == ^2>

      # join a list of subqueries
      iex> EctoShorts.QueryBuilders.Schema.build_query(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :join, %{subquery: [%{as: :comments, query: EctoShorts.Schema.Comment, where: %{id: 2}}]}, [])
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in subquery(from c0 in EctoShorts.Schema.Comment), as: :comments, on: true, where: c1.id == ^2>
  """
  @spec build_query(
          query() | queryable() | source_queryable(),
          binding_alias() | nil,
          queryable(),
          key(),
          value(),
          opts()
        ) :: query() | queryable()
  def build_query(query, current_binding, schema_module, key, value, opts) do
    cond do
      function_exported?(schema_module, :filters, 0) and key in schema_module.filters() ->
        if function_exported?(schema_module, :build_query, 4) do
          schema_module.build_query(
            query,
            current_binding,
            key,
            value
          )
        else
          EctoShorts.Utils.Logger.warning(
            __MODULE__,
            "callback function build_query/4 not found in schema: #{inspect(schema_module)}"
          )

          # fallback to the schema filter
          build_schema_filter(query, current_binding, schema_module, key, value, opts)
        end

      key in @query_filters ->
        build_query_filter(query, current_binding, schema_module, key, value, opts)

      key in schema_module.__schema__(:associations) ->
        join_association(query, current_binding, schema_module, key, value, opts)

      key in schema_module.__schema__(:query_fields) ->
        build_schema_filter(query, current_binding, schema_module, key, value, opts)

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

        #{Enum.map_join(@query_filters, "\n", &"* #{&1}")}

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

        EctoShorts.Utils.Logger.warning(__MODULE__, message <> assoc_warning_message)

        query
    end
  end

  defp build_schema_filter(query, current_binding, schema_module, key, value, opts) do
    ExpressionBuilder.apply_expressions(query, value, fn value, query ->
      apply_schema_filter(query, current_binding, schema_module, key, value, opts)
    end)
  end

  defp apply_schema_filter(query, current_binding, schema_module, key, {operator, value}, opts) do
    CommonQuery.where(query, current_binding, schema_module, {key, {operator, value}}, opts)
  end

  defp apply_schema_filter(query, current_binding, schema_module, key, value, opts) do
    apply_schema_filter(query, current_binding, schema_module, key, {:==, value}, opts)
  end

  defp build_query_filter(query, _current_binding, _schema_module, :from, params, _opts) do
    CommonQuery.from(query, params)
  end

  defp build_query_filter(query, current_binding, _schema_module, :select, value, _opts) do
    CommonQuery.select(query, current_binding, value)
  end

  defp build_query_filter(query, current_binding, _schema_module, :select_merge, value, _opts) do
    CommonQuery.select_merge(query, current_binding, value)
  end

  defp build_query_filter(query, current_binding, schema_module, :or, value, opts) do
    CommonQuery.or_where(query, current_binding, schema_module, value, opts)
  end

  defp build_query_filter(query, current_binding, schema_module, :or_where, value, opts) do
    CommonQuery.or_where(query, current_binding, schema_module, value, opts)
  end

  defp build_query_filter(query, current_binding, schema_module, :where, value, opts) do
    CommonQuery.where(query, current_binding, schema_module, value, opts)
  end

  defp build_query_filter(query, current_binding, schema_module, :join, params, opts) do
    Enum.reduce(params, query, fn {key, value}, query ->
      join_filter(query, current_binding, schema_module, key, value, opts)
    end)
  end

  defp join_filter(query, current_binding, schema_module, :association, params, opts) do
    Enum.reduce(params, query, fn {key, value}, query ->
      join_association(query, current_binding, schema_module, key, value, opts)
    end)
  end

  defp join_filter(query, current_binding, schema_module, :subquery, params, opts) do
    params
    |> List.wrap()
    |> Enum.reduce(query, fn params, query ->
      join_subquery(query, current_binding, schema_module, params, opts)
    end)
  end

  defp join_association(query, current_binding, schema_module, key, params, opts) do
    {assoc_schema_module, params} =
      case Map.pop(params, :queryable) do
        {nil, params} -> {get_association_schema(schema_module, key), params}
        {queryable, params} -> {queryable, params}
      end

    as =
      with nil <- params[:as] do
        if named_binding_enabled?(opts) do
          named_binding(key)
        else
          false
        end
      end

    params = Map.put(params, :as, as)

    query
    |> CommonQuery.join(
      current_binding,
      assoc_schema_module,
      :association,
      key,
      Map.take(params, [:as, :qualifier, :on, :prefix]),
      opts
    )
    |> build_query(
      as,
      assoc_schema_module,
      Map.drop(params, [:as, :qualifier, :on, :prefix]),
      opts
    )
  end

  defp join_subquery(query, current_binding, _schema_module, params, opts) do
    {subquery_query, params} = Map.pop(params, :query)

    if is_nil(subquery_query) do
      raise KeyError, "key :query not found: #{inspect(params)}"
    end

    {subquery_schema_module, params} = Map.pop(params, :queryable)

    subquery_schema_module =
      if is_nil(subquery_schema_module) do
        CommonSchemas.get_schema_queryable(subquery_query)
      else
        subquery_schema_module
      end

    as =
      with nil <- params[:as] do
        if named_binding_enabled?(opts) do
          named_binding_from_module(subquery_schema_module)
        else
          false
        end
      end

    params = Map.put(params, :as, as)

    query
    |> CommonQuery.join(
      current_binding,
      subquery_schema_module,
      :subquery,
      subquery_query,
      Map.take(params, [:as, :qualifier, :on, :prefix]),
      opts
    )
    |> build_query(
      as,
      subquery_schema_module,
      Map.drop(params, [:as, :qualifier, :on, :prefix]),
      opts
    )
  end

  defp get_association_schema(schema_module, key) do
    case schema_module.__schema__(:association, key) do
      %{through: [field1, field2]} ->
        schema_module
        |> get_association_schema(field1)
        |> get_association_schema(field2)

      %{related: related} ->
        related
    end
  end

  defp named_binding_enabled?(opts) do
    opts[:named_binding_enabled] ||
      EctoShorts.Config.named_binding_enabled() ||
      true
  end

  defp named_binding_from_module(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
    |> named_binding()
  end

  defp named_binding(key) do
    :"ecto_shorts_#{key}"
  end
end
