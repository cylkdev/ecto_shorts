defmodule EctoShorts.CommonQuery do
  @moduledoc since: "2.5.0"
  @moduledoc """
  `EctoShorts.CommonQuery` provides helper functions for introspecting Ecto queries.
  """

  alias Ecto.Queryable
  alias EctoShorts.SchemaHelpers

  @type subquery :: Ecto.SubQuery.t()
  @type query :: Ecto.Query.t()
  @type schema_module :: Ecto.Queryable.t()
  @type schema_source :: binary()
  @type source_and_schema :: {schema_source(), schema_module()}
  @type sourceable :: schema_module() | source_and_schema()
  @type query_source :: query() | sourceable()
  @type from_expr :: %Ecto.Query.FromExpr{}
  @type join_expr :: %Ecto.Query.JoinExpr{}
  @type binding_alias :: atom()

  @doc """
  Converts the given `schema_module`, or `{schema_source, schema_module}`
  to an Ecto.Query struct or returns an `Ecto.Query` struct as-is.

  ## Examples

      iex> EctoShorts.CommonQuery.to_query(EctoShorts.Schema.Post)
      #Ecto.Query<from p0 in EctoShorts.Schema.Post>

      iex> EctoShorts.CommonQuery.to_query({"custom_source", EctoShorts.Schema.Post})
      #Ecto.Query<from p0 in {"custom_source", EctoShorts.Schema.Post}>

      iex> import Ecto.Query
      ...> query = from p in EctoShorts.Schema.Post
      ...> EctoShorts.CommonQuery.to_query(query)
      #Ecto.Query<from p0 in EctoShorts.Schema.Post>
  """
  @spec to_query(query_source()) :: query()
  def to_query(query) when is_struct(query, Ecto.Query) do
    query
  end

  def to_query(source) do
    Queryable.to_query(source)
  end

  @doc """
  Returns the schema module (queryable) from the given `query`.

  ## Examples

      iex> import Ecto.Query
      ...> query = from p in EctoShorts.Schema.Post
      ...> EctoShorts.CommonQuery.schema_module_for_query(query)
      EctoShorts.Schema.Post
  """
  @spec schema_module_for_query(query() | schema_module()) :: schema_module()
  def schema_module_for_query(query) do
    case get_query_source(query) do
      {_schema_source, schema_module} -> schema_module
      schema_module -> schema_module
    end
  end

  @doc """
  Returns a tuple of `{schema_source, schema_module}` for a given
  query or subquery.

  ## Examples

      # returns modules as-is
      iex> EctoShorts.CommonQuery.get_query_source(EctoShorts.Schema.Post)
      EctoShorts.Schema.Post

      # get source from top-level query
      iex> import Ecto.Query
      ...> query = from p in EctoShorts.Schema.Post
      ...> EctoShorts.CommonQuery.get_query_source(query)
      {"posts", EctoShorts.Schema.Post}

      # get source from subquery
      iex> import Ecto.Query
      ...> inner = from p in EctoShorts.Schema.Post, where: p.id in [1, 2, 3]
      ...> outer = from p in subquery(inner), select: %{id: p.id}
      ...> EctoShorts.CommonQuery.get_query_source(outer)
      {"posts", EctoShorts.Schema.Post}

      # get source from nested subquery
      iex> import Ecto.Query
      ...> base = from p in EctoShorts.Schema.Post, where: p.published == true
      ...> mid = from p in subquery(base), where: p.id > 10
      ...> outer = from p in subquery(mid), select: %{id: p.id, title: p.title}
      ...> EctoShorts.CommonQuery.get_query_source(outer)
      {"posts", EctoShorts.Schema.Post}
  """
  @spec get_query_source(query() | schema_module() | subquery()) :: sourceable()
  def get_query_source(%{from: %{source: {schema_source, schema_module}}} = _query) do
    if is_nil(schema_source) do
      schema_module
    else
      {schema_source, schema_module}
    end
  end

  def get_query_source(%{from: %{source: %{query: query} = _subquery}}) do
    get_query_source(query)
  end

  def get_query_source(%{query: query} = _subquery) do
    get_query_source(query)
  end

  def get_query_source(schema_module) when is_atom(schema_module) do
    schema_module
  end

  @doc """
  Returns the schema module (queryable) for the given named binding
  in an Ecto query.

  Raises an error if an expression with a matching named binding
  cannot not found.

  ## Examples

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post
        ...> EctoShorts.CommonQuery.fetch_query_expression_schema_module!(query, :post)
        ...> EctoShorts.Schema.Post
  """
  @spec fetch_query_expression_schema_module!(query(), binding_alias()) ::
          schema_module() | :error
  def fetch_query_expression_schema_module!(query, binding_alias) do
    with :error <- fetch_query_expression_schema_module(query, binding_alias) do
      raise ArgumentError,
            "named binding alias '#{inspect(binding_alias)}' not found, got: #{inspect(query)}"
    end
  end

  @doc """
  Returns the schema module (queryable) for the given named binding in an Ecto query.

  Returns `{:ok, schema_module}` if found otherwise `:error`.

  ## Examples

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post
        ...> EctoShorts.CommonQuery.fetch_query_expression_schema_module(query, :post)
        ...> EctoShorts.Schema.Post

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post
        ...> EctoShorts.CommonQuery.fetch_query_expression_schema_module(query, nil)
        ...> EctoShorts.Schema.Post

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post
        ...> EctoShorts.CommonQuery.fetch_query_expression_schema_module(query, nil)
        ...> :error
  """
  @spec fetch_query_expression_schema_module(query(), binding_alias()) :: schema_module() | :error
  def fetch_query_expression_schema_module(query, binding_alias) do
    case get_query_source(query, binding_alias) do
      :error -> :error
      {_schema_source, schema_module} -> schema_module
      schema_module -> schema_module
    end
  end

  @doc """
  Returns the source associated with a named binding in an Ecto query.

  ## Examples

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post, join: EctoShorts.Schema.Comment, as: :comments, on: true
        ...> EctoShorts.CommonQuery.get_query_source(query, :post)
        ...> {"posts", EctoShorts.Schema.Post}

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post, join: EctoShorts.Schema.Comment, as: :comments, on: true
        ...> EctoShorts.CommonQuery.get_query_source(query, :comments)
        EctoShorts.Schema.Comment

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post, join: {"comments", EctoShorts.Schema.Comment}, as: :comments, on: true
        ...> EctoShorts.CommonQuery.get_query_source(query, :comments)
        {"comments", EctoShorts.Schema.Comment}
  """
  @spec get_query_source(query(), binding_alias()) :: sourceable() | :error
  def get_query_source(query, binding_alias) do
    case fetch_query_expression(query, binding_alias) do
      :error ->
        :error

      %{source: {nil, schema_module}} ->
        schema_module

      %{source: {schema_source, schema_module}} ->
        {schema_source, schema_module}

      %{assoc: {_binding_position, assoc_key}} ->
        get_query_from_expression_assoc_schema_module(query, assoc_key)
    end
  end

  @doc """
  Fetches the query expression (`from` or `join`) tied to a named binding.

  Returns either a `%Ecto.Query.FromExpr{}` or `%Ecto.Query.JoinExpr{}`
  for the given binding alias, otherwise `:error` if not found.

  ## Examples

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post, join: EctoShorts.Schema.Comment, as: :comments, on: true
        ...> EctoShorts.CommonQuery.fetch_query_expression(query, :post)
        ...> {:ok, %Ecto.Query.FromExpr{
        ...>   source: {"posts", EctoShorts.Schema.Post},
        ...>   file: "iex",
        ...>   line: 2,
        ...>   as: :post,
        ...>   prefix: nil,
        ...>   params: [],
        ...>   hints: []
        ...> }}

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post, join: EctoShorts.Schema.Comment, as: :comments, on: true
        ...> EctoShorts.CommonQuery.fetch_query_expression(query, :comments)
        ...> {:ok,
        ...>     %Ecto.Query.JoinExpr{
        ...>     qual: :inner,
        ...>     source: {nil, EctoShorts.Schema.Comment},
        ...>     on: %Ecto.Query.QueryExpr{expr: true, file: "iex", line: 2, params: []},
        ...>     file: "iex",
        ...>     line: 2,
        ...>     assoc: nil,
        ...>     as: :comments,
        ...>     ix: nil,
        ...>     prefix: nil,
        ...>     params: [],
        ...>     hints: []
        ...> }}
  """
  @spec fetch_query_expression(query(), binding_alias()) :: from_expr() | join_expr() | :error
  def fetch_query_expression(query, binding_alias) do
    with :error <- fetch_query_from_expression(query, binding_alias) do
      fetch_query_join_expression(query, binding_alias)
    end
  end

  def get_query_from_expression_assoc_schema_module(query, assoc_key) do
    query
    |> get_query_from_expression_schema_module()
    |> SchemaHelpers.get_association_schema_module(assoc_key)
  end

  @spec get_query_from_expression_schema_module(query()) :: schema_module()
  def get_query_from_expression_schema_module(query) do
    %{source: {_schema_source, schema_module}} = get_query_from_expression(query)

    schema_module
  end

  @spec get_query_from_expression_source(query()) :: source_and_schema()
  def get_query_from_expression_source(query) do
    %{source: source} = get_query_from_expression(query)

    source
  end

  @spec get_query_from_expression(query()) :: from_expr()
  def get_query_from_expression(%{from: from}) do
    from
  end

  @doc """
  Returns an Ecto query `FromExpr` struct if the name matches the
  given named binding.

  This function returns `{:ok, from_expr}` if the from expression
  name that matches `binding_alias` otherwise `:error`.

  ## Examples

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post
        ...> EctoShorts.CommonQuery.fetch_query_from_expression(query, nil)
        ...> %Ecto.Query.FromExpr{
        ...>   source: {"posts", EctoShorts.Schema.Post},
        ...>   file: "iex",
        ...>   line: 2,
        ...>   as: :post,
        ...>   prefix: nil,
        ...>   params: [],
        ...>   hints: []
        ...> }

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post
        ...> EctoShorts.CommonQuery.fetch_query_from_expression(query, :post)
        ...> %Ecto.Query.FromExpr{
        ...>   source: {"posts", EctoShorts.Schema.Post},
        ...>   file: "iex",
        ...>   line: 2,
        ...>   as: :post,
        ...>   prefix: nil,
        ...>   params: [],
        ...>   hints: []
        ...> }

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post
        ...> EctoShorts.CommonQuery.fetch_query_from_expression(query, :post)
        :error
  """
  @spec fetch_query_from_expression(query(), binding_alias()) :: from_expr() | :error
  def fetch_query_from_expression(%{from: %{as: as} = from}, binding_alias) do
    if as === binding_alias do
      from
    else
      :error
    end
  end

  @doc """
  Returns an Ecto query `JoinExpr` struct for the given named binding.

  This function returns `join_expr` if a join expression is found with
  a name that matches `binding_alias` otherwise `:error`.

  ## Examples

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, join: EctoShorts.Schema.Comment, as: :comments, on: true
        ...> EctoShorts.CommonQuery.fetch_query_join_expression(query, :comments)
        ...> %Ecto.Query.JoinExpr{
        ...>    qual: :inner,
        ...>    source: {nil, EctoShorts.Schema.Comment},
        ...>    on: %Ecto.Query.QueryExpr{expr: true, file: "iex", line: 2, params: []},
        ...>    file: "iex",
        ...>    line: 2,
        ...>    assoc: nil,
        ...>    as: :comments,
        ...>    ix: nil,
        ...>    prefix: nil,
        ...>    params: [],
        ...>    hints: []
        ...> }

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post
        ...> EctoShorts.CommonQuery.fetch_query_join_expression(query, :comments)
        :error
  """
  @spec fetch_query_join_expression(query(), binding_alias()) :: join_expr() | :error
  def fetch_query_join_expression(%{joins: joins}, binding_alias) do
    case Enum.find(joins, &(&1.as === binding_alias)) do
      nil -> :error
      join -> join
    end
  end
end
