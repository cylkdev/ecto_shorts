defmodule EctoShorts.CommonSchemas do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Provides utility functions for working with Ecto schemas,
  particularly when dealing with polymorphic associations.

  ## Polymorphic Associations

  This module supports [polymorphic associations](https://hexdocs.pm/ecto/Ecto.Schema.html#belongs_to/3-polymorphic-associations)
  by allowing you to use a `{source :: binary(), queryable :: Ecto.Queryable.t()}` tuple
  in place of a traditional schema module. This is useful when you want to query
  different tables using a shared schema definition.

  For example:

      EctoSchema.Actions.all({"posts", EctoShorts.Schema.Post}, %{id: 1})

  In this example, the query runs against the "posts" table instead of the default
  source defined in the schema. When both a `source` and `queryable` are provided,
  the explicit `source` takes precedence.

  This approach allows reusing schema modules across different tables,
  as long as the table structure matches the schema definition.
  """

  alias EctoShorts.QueryHelpers

  @type source :: binary()
  @type query :: Ecto.Query.t()
  @type queryable :: Ecto.Queryable.t()
  @type source_queryable :: {source(), queryable()}
  @type changeset :: Ecto.Changeset.t()
  @type schema_struct :: Ecto.Schema.t()
  @type params :: map()
  @type opts :: keyword()
  @type schema_metadata :: Ecto.Schema.Metadata.t()

  @doc group: "Schema Reflection API"
  @doc """
  Returns the `Ecto.Schema.Metadata` struct from the given schema struct.
  """
  @spec get_schema_metadata(schema_struct()) :: schema_metadata()
  def get_schema_metadata(%{__meta__: meta}), do: meta

  @doc group: "Schema Reflection API"
  @doc """
  Invokes the `__schema__/1` reflection function.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_reflection(MyApp.UserSchema, :fields)
      iex> EctoShorts.CommonSchemas.get_schema_reflection({"posts", MyApp.UserSchema}, :fields)
  """
  @spec get_schema_reflection(queryable() | source_queryable(), any()) :: any()
  def get_schema_reflection({_, queryable}, arg), do: queryable.__schema__(arg)
  def get_schema_reflection(queryable, arg), do: queryable.__schema__(arg)

  @doc group: "Schema Reflection API"
  @doc """
  Invokes the `__schema__/2` reflection function.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_reflection(EctoShorts.Schema.Post, :type, :id)
      :id

      iex> EctoShorts.CommonSchemas.get_schema_reflection({"posts", EctoShorts.Schema.Post}, :type, :id)
      :id
  """
  @spec get_schema_reflection(
          queryable() | source_queryable(),
          any(),
          any()
        ) :: any()
  def get_schema_reflection({_, queryable}, arg1, arg2), do: queryable.__schema__(arg1, arg2)
  def get_schema_reflection(queryable, arg1, arg2), do: queryable.__schema__(arg1, arg2)

  @doc group: "Schema Reflection API"
  @doc """
  Returns the `prefix` defined in the schema, if any.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_prefix(EctoShorts.Schema.Post)
      nil

      iex> EctoShorts.CommonSchemas.get_schema_prefix({"posts", EctoShorts.Schema.Post})
      nil
  """
  @spec get_schema_prefix(queryable() | source_queryable()) :: binary() | nil
  def get_schema_prefix({_, queryable}), do: queryable.__schema__(:prefix)
  def get_schema_prefix(queryable), do: queryable.__schema__(:prefix)

  @doc group: "Schema Reflection API"
  @doc """
  Returns the source (table name) for the schema.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_source(EctoShorts.Schema.Post)
      "posts"

      iex> EctoShorts.CommonSchemas.get_schema_source({"posts", EctoShorts.Schema.Post})
      "posts"
  """
  @spec get_schema_source(queryable() | source_queryable()) :: binary()
  def get_schema_source({source, _}), do: source
  def get_schema_source(queryable), do: queryable.__schema__(:source)

  @doc group: "Schema Reflection API"
  @doc """
  Returns the schema module given {source, queryable}, a schema struct, or query struct.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_queryable(EctoShorts.Schema.Post)
      EctoShorts.Schema.Post

      iex> EctoShorts.CommonSchemas.get_schema_queryable({"posts", EctoShorts.Schema.Post})
      EctoShorts.Schema.Post

      iex> require Ecto.Query
      ...> EctoShorts.CommonSchemas.get_schema_queryable(Ecto.Query.from(p in EctoShorts.Schema.Post))
      EctoShorts.Schema.Post
  """
  @spec get_schema_queryable(
          query_or_schema :: query() | queryable() | source_queryable() | schema_struct()
        ) :: queryable()
  def get_schema_queryable({_source, queryable}), do: queryable
  def get_schema_queryable(%{__meta__: %{schema: queryable}}), do: queryable
  def get_schema_queryable(query), do: QueryHelpers.get_query_schema(query)

  @doc group: "Schema API"
  @doc """
  Builds a struct from a queryable, optionally overriding its source.

  ### Examples

      iex> EctoShorts.CommonSchemas.create_schema_struct(EctoShorts.Schema.Post)
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{source: "posts"}
      }

      iex> EctoShorts.CommonSchemas.create_schema_struct({"custom_source", EctoShorts.Schema.Post})
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{source: "custom_source"}
      }
  """
  @spec create_schema_struct(queryable() | source_queryable()) :: schema_struct()
  def create_schema_struct({source, queryable}) do
    prefix = get_schema_prefix(queryable)

    queryable
    |> struct()
    |> put_schema_meta(state: :loaded, source: source, prefix: prefix)
  end

  def create_schema_struct(queryable), do: struct(queryable)

  @doc group: "Schema API"
  @doc """
  Updates the `__meta__` field on an Ecto schema struct.

  ### Options

      See `Ecto.put_meta/2` for more information.

  ### Examples

      iex> EctoShorts.CommonSchemas.put_schema_meta(%EctoShorts.Schema.Post{}, state: :loaded, source: "custom_source", prefix: "custom_prefix")
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{
          state: :loaded,
          source: "posts",
          prefix: "custom_prefix"
        }
      }

      iex> EctoShorts.CommonSchemas.put_schema_meta(EctoShorts.Schema.Post, state: :loaded, source: "custom_source", prefix: "custom_prefix")
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{
          state: :loaded,
          source: "posts",
          prefix: "custom_prefix"
        }
      }

      # the source given in the tuple takes precedence
      iex> EctoShorts.CommonSchemas.put_schema_meta({"posts", EctoShorts.Schema.Post}, state: :loaded, source: "custom_source", prefix: "custom_prefix")
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{
          state: :loaded,
          source: "posts",
          prefix: "custom_prefix"
        }
      }
  """
  @spec put_schema_meta(
          query_or_schema_struct :: queryable() | source_queryable() | schema_struct()
        ) ::
          schema_struct()
  @spec put_schema_meta(
          query_or_schema_struct :: queryable() | source_queryable() | schema_struct(),
          opts()
        ) :: schema_struct()
  def put_schema_meta(schema_struct, opts \\ [])

  def put_schema_meta(%_{__meta__: state} = schema_struct, opts) do
    Ecto.put_meta(schema_struct,
      context: opts[:context] || state.context,
      prefix: opts[:prefix] || state.prefix,
      source: opts[:source] || state.source,
      state: opts[:state] || state.state || :loaded
    )
  end

  def put_schema_meta({source, queryable}, opts) do
    {source, queryable}
    |> create_schema_struct()
    |> put_schema_meta(Keyword.delete(opts, :source))
  end

  def put_schema_meta(queryable, opts) do
    queryable
    |> create_schema_struct()
    |> put_schema_meta(opts)
  end

  @doc group: "Changeset API"
  @doc """
  Builds an `Ecto.Changeset` from a queryable, schema struct, or changeset.

  This function resolves how to build the changeset based on the input type,
  and delegates to `build_changeset/4`.
  """
  @spec build_changeset(
          queryable() | source_queryable() | schema_struct() | changeset(),
          params()
        ) :: changeset()
  @spec build_changeset(
          queryable() | source_queryable() | schema_struct() | changeset(),
          params(),
          opts()
        ) :: changeset()
  def build_changeset(query_or_schema_or_changeset, params, opts \\ [])

  def build_changeset(%{data: %{__meta__: %{schema: queryable}}} = changeset, params, opts) do
    build_changeset(queryable, changeset, params, opts)
  end

  def build_changeset(%{__meta__: %{schema: queryable}} = struct, params, opts) do
    build_changeset(queryable, struct, params, opts)
  end

  def build_changeset({source, queryable}, params, opts) do
    build_changeset(queryable, create_schema_struct({source, queryable}), params, opts)
  end

  def build_changeset(queryable, params, opts) do
    build_changeset(queryable, create_schema_struct(queryable), params, opts)
  end

  @doc group: "Changeset API"
  @doc """
  Builds an `Ecto.Changeset` using a variety of customization strategies.

  This function wraps the actual call to the `changeset/2` function defined
  on the schema module (or a custom function if specified in options). It provides
  flexibility for injecting custom behavior when generating a changeset.

  ## Options

    - `:build_changeset` - Customizes how the changeset is constructed. Accepted values:

      - `{mod, fun, args}` – Calls `apply(mod, fun, [struct_or_changeset, params] ++ args)`.

      - `{mod, fun}` – Equivalent to `{mod, fun, []}`.

      - `2-arity function` – A function that receives `struct_or_changeset` and `params`.

      - `1-arity function` – Receives the default changeset and returns a modified version.

      - `map` – A map of changes to merge into the params before building the changeset.

  Raises if the result is not an `Ecto.Changeset`.
  """
  @spec build_changeset(
          queryable() | source_queryable(),
          schema_struct() | changeset(),
          params(),
          opts()
        ) :: changeset()
  def build_changeset(
        {source, queryable},
        %{data: %{__meta__: %{schema: _}} = struct} = changeset,
        params,
        opts
      ) do
    build_changeset(
      queryable,
      %{changeset | data: put_schema_meta(struct, source: source)},
      params,
      opts
    )
  end

  def build_changeset({source, queryable}, struct, params, opts) do
    build_changeset(
      queryable,
      put_schema_meta(struct, source: source),
      params,
      opts
    )
  end

  def build_changeset(queryable, struct_or_changeset, params, opts) do
    case opts[:build_changeset] do
      {mod, fun, args} ->
        apply(mod, fun, [struct_or_changeset, params] ++ args)

      {mod, fun} ->
        apply(mod, fun, [struct_or_changeset, params])

      fun when is_function(fun, 2) ->
        fun.(struct_or_changeset, params)

      fun when is_function(fun, 1) ->
        struct_or_changeset
        |> queryable.changeset(params)
        |> fun.()

      changes when is_map(changes) ->
        queryable.changeset(struct_or_changeset, Map.merge(params, changes))

      _ ->
        queryable.changeset(struct_or_changeset, params)
    end
  end
end
