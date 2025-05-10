defmodule EctoShorts.QueryBuilders.Schema do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  alias EctoShorts.{
    CommonQuery,
    CommonQueryAPI,
    QueryHelpers,
    SchemaHelpers
  }

  @type query :: Ecto.Query.t()
  @type schema_module :: Ecto.Queryable.t()
  @type schema_source :: binary()
  @type sourceable :: schema_module() | {schema_source(), schema_module()}
  @type query_source :: query() | sourceable()
  @type binding_alias :: atom()

  @type key :: atom()
  @type value :: any()
  @type opts :: keyword()

  @type filter :: :join | :select | :select_merge | :or_where

  @behaviour EctoShorts.QueryBuilder

  @query_api_filters ~w(
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

  These filters delegate to `EctoShorts.CommonQueryAPI` and enable dynamic,
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
  def filters, do: @query_api_filters

  @impl EctoShorts.QueryBuilder
  @doc """
  Applies a single filter to the query based on a field, association, or supported expression.

  This function determines how to handle the provided filter key:

    * If the key matches a supported filter (e.g., `:select`, `:join`),
      it applies the appropriate query transformation using
      `EctoShorts.CommonQueryAPI`.

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
          query_source(),
          binding_alias(),
          schema_module(),
          key(),
          value(),
          opts()
        ) :: query() | schema_module()
  def build_query(query, binding_alias, schema_module, key, value, opts) do
    cond do
      key in @query_api_filters ->
        build_query_api_filter(query, binding_alias, schema_module, key, value, opts)

      key in schema_module.__schema__(:associations) ->
        join_association(query, binding_alias, schema_module, key, value, opts)

      key in schema_module.__schema__(:query_fields) ->
        build_schema_filter(query, binding_alias, schema_module, key, value, opts)

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

        #{Enum.map_join(@query_api_filters, "\n", &"* #{&1}")}

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

  defp build_schema_filter(query, binding_alias, schema_module, key, value, opts) do
    QueryHelpers.apply_expression(query, value, fn value, query ->
      apply_schema_filter(query, binding_alias, schema_module, key, value, opts)
    end)
  end

  defp apply_schema_filter(query, binding_alias, _schema_module, key, {operator, value}, opts) do
    CommonQueryAPI.where(query, binding_alias, %{key => %{operator => value}}, opts)
  end

  defp apply_schema_filter(query, binding_alias, schema_module, key, value, opts) do
    apply_schema_filter(query, binding_alias, schema_module, key, {:==, value}, opts)
  end

  defp build_query_api_filter(query, _binding_alias, _schema_module, :from, params, _opts) do
    CommonQueryAPI.from(query, params)
  end

  defp build_query_api_filter(query, binding_alias, _schema_module, :select, value, _opts) do
    CommonQueryAPI.select(query, binding_alias, value)
  end

  defp build_query_api_filter(query, binding_alias, _schema_module, :select_merge, value, _opts) do
    CommonQueryAPI.select_merge(query, binding_alias, value)
  end

  defp build_query_api_filter(query, binding_alias, _schema_module, :or, value, opts) do
    CommonQueryAPI.or_where(query, binding_alias, value, opts)
  end

  defp build_query_api_filter(query, binding_alias, _schema_module, :or_where, value, opts) do
    CommonQueryAPI.or_where(query, binding_alias, value, opts)
  end

  defp build_query_api_filter(query, binding_alias, _schema_module, :where, value, opts) do
    CommonQueryAPI.where(query, binding_alias, value, opts)
  end

  defp build_query_api_filter(query, binding_alias, schema_module, :join, params, opts) do
    Enum.reduce(params, query, fn {key, value}, query ->
      build_join_filter(query, binding_alias, schema_module, key, value, opts)
    end)
  end

  defp build_join_filter(query, binding_alias, schema_module, :association, params, opts) do
    Enum.reduce(params, query, fn {key, value}, query ->
      join_association(query, binding_alias, schema_module, key, value, opts)
    end)
  end

  defp build_join_filter(query, binding_alias, schema_module, :subquery, params, opts) do
    params
    |> List.wrap()
    |> Enum.reduce(query, fn params, query ->
      join_subquery(query, binding_alias, schema_module, params, opts)
    end)
  end

  @doc false
  def join_association(query, binding_alias, schema_module, key, params, opts)
      when is_map(params) do
    join_association(query, binding_alias, schema_module, key, Map.to_list(params), opts)
  end

  def join_association(query, binding_alias, schema_module, key, params, opts) do
    assoc_schema_module = SchemaHelpers.schema_module_for_association(schema_module, key)

    as =
      with nil <- params[:as] do
        key
        |> named_binding()
        |> String.to_atom()
      end

    params = Keyword.put(params, :as, as)

    query
    |> CommonQueryAPI.join(
      binding_alias,
      :association,
      key,
      take_join_keys(params),
      opts
    )
    |> reduce_filters(
      as,
      assoc_schema_module,
      drop_join_keys(params),
      opts
    )
  end

  @doc false
  def join_subquery(query, binding_alias, schema_module, params, opts) when is_map(params) do
    join_subquery(query, binding_alias, schema_module, Map.to_list(params), opts)
  end

  def join_subquery(query, binding_alias, _schema_module, params, opts) do
    {subquery_data, params} = Keyword.pop(params, :query)

    if is_nil(subquery_data) do
      raise KeyError, "key :query not found: #{inspect(params)}"
    end

    {subquery_schema_module, params} = Keyword.pop(params, :queryable)

    subquery_schema_module =
      if is_nil(subquery_schema_module) do
        subquery_data
        |> CommonQuery.to_query()
        |> CommonQuery.schema_module_for_query_expression!(params[:as])
      else
        subquery_schema_module
      end

    as =
      with nil <- params[:as] do
        subquery_schema_module
        |> named_binding_from_module()
        |> String.to_atom()
      end

    params = Keyword.put(params, :as, as)

    query
    |> CommonQueryAPI.join(
      binding_alias,
      :subquery,
      subquery_data,
      take_join_keys(params),
      opts
    )
    |> reduce_filters(
      as,
      subquery_schema_module,
      drop_join_keys(params),
      opts
    )
  end

  defp take_join_keys(params) do
    Keyword.take(params, [:as, :qualifier, :on, :prefix])
  end

  defp drop_join_keys(params) do
    Keyword.drop(params, [:as, :qualifier, :on, :prefix])
  end

  defp reduce_filters(query, binding_alias, schema_module, params, opts) do
    Enum.reduce(params, query, fn {key, value}, query ->
      build_query(query, binding_alias, schema_module, key, value, opts)
    end)
  end

  @doc false
  def named_binding_from_module(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
    |> named_binding()
  end

  @doc false
  def named_binding(key) do
    "ecto_shorts_#{key}"
  end
end
