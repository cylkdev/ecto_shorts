defmodule EctoShorts.CommonQuery do
  @moduledoc since: "2.5.0"
  @moduledoc """
  `EctoShorts.CommonQuery` provides helper functions
  for introspecting Ecto queries.
  """

  alias Ecto.Queryable

  @type subquery :: Ecto.SubQuery.t()
  @type query :: Ecto.Query.t()
  @type schema_module :: Ecto.Queryable.t()
  @type schema_source :: binary()
  @type sourceable :: schema_module() | {schema_source(), schema_module()}
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
    case source_for_query(query) do
      {_schema_source, schema_module} -> schema_module
      schema_module -> schema_module
    end
  end

  @doc """
  Returns a tuple of `{schema_source, schema_module}` for a given
  query or subquery.

  ## Examples

      # returns modules as-is
      iex> EctoShorts.CommonQuery.source_for_query(EctoShorts.Schema.Post)
      EctoShorts.Schema.Post

      # get source from top-level query
      iex> import Ecto.Query
      ...> query = from p in EctoShorts.Schema.Post
      ...> EctoShorts.CommonQuery.source_for_query(query)
      {"posts", EctoShorts.Schema.Post}

      # get source from subquery
      iex> import Ecto.Query
      ...> inner = from p in EctoShorts.Schema.Post, where: p.id in [1, 2, 3]
      ...> outer = from p in subquery(inner), select: %{id: p.id}
      ...> EctoShorts.CommonQuery.source_for_query(outer)
      {"posts", EctoShorts.Schema.Post}

      # get source from nested subquery
      iex> import Ecto.Query
      ...> base = from p in EctoShorts.Schema.Post, where: p.published == true
      ...> mid = from p in subquery(base), where: p.id > 10
      ...> outer = from p in subquery(mid), select: %{id: p.id, title: p.title}
      ...> EctoShorts.CommonQuery.source_for_query(outer)
      {"posts", EctoShorts.Schema.Post}
  """
  @spec source_for_query(query() | schema_module() | subquery()) :: sourceable()
  def source_for_query(%{from: %{source: {schema_source, schema_module}}} = _query) do
    if is_nil(schema_source) do
      schema_module
    else
      {schema_source, schema_module}
    end
  end

  def source_for_query(%{from: %{source: %{query: query} = _subquery}}) do
    source_for_query(query)
  end

  def source_for_query(%{query: query} = _subquery) do
    source_for_query(query)
  end

  def source_for_query(schema_module) when is_atom(schema_module) do
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
        ...> EctoShorts.CommonQuery.schema_module_for_query_expression!(query, :post)
        ...> EctoShorts.Schema.Post
  """
  @spec schema_module_for_query_expression!(query(), binding_alias()) :: schema_module() | :error
  def schema_module_for_query_expression!(query, binding_alias) do
    with :error <- schema_module_for_query_expression(query, binding_alias) do
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
        ...> EctoShorts.CommonQuery.schema_module_for_query_expression(query, :post)
        ...> EctoShorts.Schema.Post

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post
        ...> EctoShorts.CommonQuery.schema_module_for_query_expression(query, nil)
        ...> EctoShorts.Schema.Post

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post
        ...> EctoShorts.CommonQuery.schema_module_for_query_expression(query, nil)
        ...> :error
  """
  @spec schema_module_for_query_expression(query(), binding_alias()) :: schema_module() | :error
  def schema_module_for_query_expression(query, binding_alias) do
    case source_for_query(query, binding_alias) do
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
        ...> EctoShorts.CommonQuery.source_for_query(query, :post)
        ...> {"posts", EctoShorts.Schema.Post}

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post, join: EctoShorts.Schema.Comment, as: :comments, on: true
        ...> EctoShorts.CommonQuery.source_for_query(query, :comments)
        EctoShorts.Schema.Comment

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post, join: {"comments", EctoShorts.Schema.Comment}, as: :comments, on: true
        ...> EctoShorts.CommonQuery.source_for_query(query, :comments)
        {"comments", EctoShorts.Schema.Comment}
  """
  @spec source_for_query(query(), binding_alias()) :: sourceable() | :error
  def source_for_query(query, binding_alias) do
    case query_expression_for(query, binding_alias) do
      :error -> :error
      %{source: {nil, schema_module}} -> schema_module
      %{source: {schema_source, schema_module}} -> {schema_source, schema_module}
    end
  end

  @doc """
  Fetches the query expression (`from` or `join`) tied to a named binding.

  Returns either a `%Ecto.Query.FromExpr{}` or `%Ecto.Query.JoinExpr{}`
  for the given binding alias, otherwise `:error` if not found.

  ## Examples

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post, as: :post, join: EctoShorts.Schema.Comment, as: :comments, on: true
        ...> EctoShorts.CommonQuery.query_expression_for(query, :post)
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
        ...> EctoShorts.CommonQuery.query_expression_for(query, :comments)
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
  @spec query_expression_for(query(), binding_alias()) :: from_expr() | join_expr() | :error
  def query_expression_for(query, binding_alias) do
    with :error <- query_from_expression_for(query, binding_alias) do
      query_join_expression_for(query, binding_alias)
    end
  end

  @doc """
  Returns an Ecto query `FromExpr` struct if the name matches the
  given named binding.

  This function returns `{:ok, from_expr}` if the from expression
  name that matches `binding_alias` otherwise `:error`.

  ## Examples

        iex> import Ecto.Query
        ...> query = from p in EctoShorts.Schema.Post
        ...> EctoShorts.CommonQuery.query_from_expression_for(query, nil)
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
        ...> EctoShorts.CommonQuery.query_from_expression_for(query, :post)
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
        ...> EctoShorts.CommonQuery.query_from_expression_for(query, :post)
        :error
  """
  @spec query_from_expression_for(query(), binding_alias()) :: from_expr() | :error
  def query_from_expression_for(%{from: from}, nil) do
    from
  end

  def query_from_expression_for(%{from: %{as: as} = from}, binding_alias) do
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
        ...> EctoShorts.CommonQuery.query_join_expression_for(query, :comments)
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
        ...> EctoShorts.CommonQuery.query_join_expression_for(query, :comments)
        :error
  """
  @spec query_join_expression_for(query(), binding_alias()) :: join_expr() | :error
  def query_join_expression_for(%{joins: joins}, binding_alias) do
    case Enum.find(joins, &(&1.as === binding_alias)) do
      nil -> :error
      join -> join
    end
  end
end
