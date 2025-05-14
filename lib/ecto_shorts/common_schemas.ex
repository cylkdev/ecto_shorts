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

  alias EctoShorts.CommonQuery

  @type query :: Ecto.Query.t()
  @type schema_module :: Ecto.Queryable.t()
  @type schema_source :: binary()
  @type schema_struct :: Ecto.Schema.t()
  @type schema_metadata :: Ecto.Schema.Metadata.t()
  @type source_and_schema :: {schema_source(), schema_module()}
  @type sourceable :: schema_module() | source_and_schema()
  @type query_source :: query() | sourceable()
  @type changeset :: Ecto.Changeset.t()
  @type prefix :: binary() | nil

  @type params :: map()
  @type opts :: keyword()

  @doc group: "Reflection API"
  @doc """
  Invokes the `__schema__/1` get_reflection function.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_reflection(MyApp.UserSchema, :fields)
      iex> EctoShorts.CommonSchemas.get_reflection({"posts", MyApp.UserSchema}, :fields)
  """
  @spec get_reflection(sourceable(), any()) :: any()
  def get_reflection({_, schema_module}, arg), do: schema_module.__schema__(arg)
  def get_reflection(schema_module, arg), do: schema_module.__schema__(arg)

  @doc group: "Reflection API"
  @doc """
  Invokes the `__schema__/2` reflection function.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_reflection(EctoShorts.Schema.Post, :type, :id)
      :id

      iex> EctoShorts.CommonSchemas.get_reflection({"posts", EctoShorts.Schema.Post}, :type, :id)
      :id
  """
  @spec get_reflection(sourceable(), any(), any()) :: any()
  def get_reflection({_, schema_module}, arg1, arg2) do
    schema_module.__schema__(arg1, arg2)
  end

  def get_reflection(schema_module, arg1, arg2) do
    schema_module.__schema__(arg1, arg2)
  end

  @doc group: "Introspection API"
  @doc """
  Returns the `prefix` defined in the schema, if any.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_prefix_for(EctoShorts.Schema.Post)
      nil

      iex> EctoShorts.CommonSchemas.get_prefix_for({"posts", EctoShorts.Schema.Post})
      nil
  """
  @spec get_prefix_for(sourceable()) :: prefix()
  def get_prefix_for({_, schema_module}), do: schema_module.__schema__(:prefix)
  def get_prefix_for(schema_module), do: schema_module.__schema__(:prefix)

  @doc group: "Introspection API"
  @doc """
  Returns the source (table name) for the schema.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_source(EctoShorts.Schema.Post)
      "posts"

      iex> EctoShorts.CommonSchemas.get_schema_source({"posts", EctoShorts.Schema.Post})
      "posts"
  """
  @spec get_schema_source(query_source()) :: schema_source()
  def get_schema_source({schema_source, _}) do
    schema_source
  end

  def get_schema_source(schema_module) when is_atom(schema_module) do
    if function_exported?(schema_module, :__schema__, 1) do
      schema_module.__schema__(:source)
    else
      CommonQuery.get_from_expr(schema_module, :source)
    end
  end

  def get_schema_source(query) do
    CommonQuery.get_from_expr(query, :source)
  end

  @doc group: "Introspection API"
  @doc """
  Returns the schema module given {schema_source, schema_module}, a schema struct, or query struct.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_module(EctoShorts.Schema.Post)
      EctoShorts.Schema.Post

      iex> EctoShorts.CommonSchemas.get_schema_module({"posts", EctoShorts.Schema.Post})
      EctoShorts.Schema.Post

      iex> require Ecto.Query
      ...> EctoShorts.CommonSchemas.get_schema_module(Ecto.Query.from(p in EctoShorts.Schema.Post))
      EctoShorts.Schema.Post
  """
  @spec get_schema_module(query_source() | schema_struct()) :: schema_module()
  def get_schema_module(%{__meta__: %{schema: schema_module}}) do
    schema_module
  end

  def get_schema_module({_schema_source, schema_module}) do
    schema_module
  end

  def get_schema_module(query_source) do
    query_source |> CommonQuery.get_from_expr(:source) |> elem(1)
  end

  @doc """
  Returns the schema module (queryable) from the `Ecto.Schema.Metadata` struct
  of the given schema struct.

  ## Examples

      iex> EctoShorts.CommonSchemas.get_metadata_schema_module(%EctoShorts.Schema.Post{})
      EctoShorts.Schema.Post

      iex> changeset = Ecto.Changeset.change(%EctoShorts.Schema.Post{})
      ...> EctoShorts.CommonSchemas.get_metadata_schema_module(changeset)
      EctoShorts.Schema.Post
  """
  @spec get_metadata_schema_module(schema_struct() | changeset()) :: schema_module()
  def get_metadata_schema_module(schema_struct_or_changeset) do
    get_metadata(schema_struct_or_changeset).schema
  end

  @doc group: "Introspection API"
  @doc """
  Returns the `Ecto.Schema.Metadata` struct from the given schema struct.

  ## Examples

      iex> EctoShorts.CommonSchemas.get_metadata(%EctoShorts.Schema.Post{})
      #Ecto.Schema.Metadata<:built, "posts">

      iex> changeset = Ecto.Changeset.change(%EctoShorts.Schema.Post{})
      ...> EctoShorts.CommonSchemas.get_metadata(changeset)
      #Ecto.Schema.Metadata<:built, "posts">
  """
  @spec get_metadata(schema_struct() | changeset()) :: schema_metadata()
  def get_metadata(%{data: %{__meta__: meta}}), do: meta
  def get_metadata(%{__meta__: meta}), do: meta

  @doc group: "Struct API"
  @doc """
  Updates the `__meta__` field on an Ecto schema struct.

  ### Options

      See `Ecto.put_meta/2` for more information.

  ### Examples

      iex> EctoShorts.CommonSchemas.put_metadata(%EctoShorts.Schema.Post{}, state: :loaded, source: "custom_source", prefix: "custom_prefix")
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{
          state: :loaded,
          source: "posts",
          prefix: "custom_prefix"
        }
      }

      iex> EctoShorts.CommonSchemas.put_metadata(EctoShorts.Schema.Post, state: :loaded, source: "custom_source", prefix: "custom_prefix")
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{
          state: :loaded,
          source: "posts",
          prefix: "custom_prefix"
        }
      }

      # the source given in the tuple takes precedence
      iex> EctoShorts.CommonSchemas.put_metadata({"posts", EctoShorts.Schema.Post}, state: :loaded, source: "custom_source", prefix: "custom_prefix")
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{
          state: :loaded,
          source: "posts",
          prefix: "custom_prefix"
        }
      }
  """
  @spec put_metadata(sourceable() | schema_struct()) ::
          schema_struct()
  @spec put_metadata(
          sourceable() | schema_struct(),
          opts()
        ) :: schema_struct()
  def put_metadata(schema_struct, opts \\ [])

  def put_metadata(%_{__meta__: state} = schema_struct, opts) do
    Ecto.put_meta(schema_struct,
      context: opts[:context] || state.context,
      prefix: opts[:prefix] || state.prefix,
      source: opts[:source] || state.source,
      state: opts[:state] || state.state || :loaded
    )
  end

  def put_metadata({schema_source, schema_module}, opts) do
    {schema_source, schema_module}
    |> build_struct()
    |> put_metadata(Keyword.delete(opts, :source))
  end

  def put_metadata(schema_module, opts) do
    schema_module
    |> build_struct()
    |> put_metadata(opts)
  end

  @doc group: "Changeset API"
  @doc """
  Builds an `Ecto.Changeset` given an Ecto queryable, Ecto schema, or
  Ecto changeset.

  This function resolves how to build the changeset based on the input
  type, and delegates to `create_changeset/4`.
  """
  @spec create_changeset(sourceable() | schema_struct() | changeset(), params()) :: changeset()
  @spec create_changeset(sourceable() | schema_struct() | changeset(), params(), opts()) ::
          changeset()
  def create_changeset(query_or_schema_or_changeset, params, opts \\ [])

  def create_changeset(%{data: %{__meta__: %{schema: schema_module}}} = changeset, params, opts) do
    create_changeset(schema_module, changeset, params, opts)
  end

  def create_changeset(%{__meta__: %{schema: schema_module}} = struct, params, opts) do
    create_changeset(schema_module, struct, params, opts)
  end

  def create_changeset({schema_source, schema_module}, params, opts) do
    create_changeset(schema_module, build_struct({schema_source, schema_module}), params, opts)
  end

  def create_changeset(schema_module, params, opts) do
    create_changeset(schema_module, build_struct(schema_module), params, opts)
  end

  @doc group: "Changeset API"
  @doc """
  Builds an `Ecto.Changeset` using a variety of customization strategies.

  This function wraps the actual call to the `changeset/2` function defined
  on the schema module (or a custom function if specified in options). It provides
  flexibility for injecting custom behavior when generating a changeset.

  ## Options

    - `:create_changeset` - Customizes how the changeset is constructed. Accepted values:

      - `{mod, fun, args}` – Calls `apply(mod, fun, [struct_or_changeset, params] ++ args)`.

      - `{mod, fun}` – Equivalent to `{mod, fun, []}`.

      - `2-arity function` – A function that receives `struct_or_changeset` and `params`.

      - `1-arity function` – Receives the default changeset and returns a modified version.

      - `map` – A map of changes to merge into the params before building the changeset.

  Raises if the result is not an `Ecto.Changeset`.
  """
  @spec create_changeset(
          sourceable(),
          schema_struct() | changeset(),
          params(),
          opts()
        ) :: changeset()
  def create_changeset(
        {schema_source, schema_module},
        %{data: %{__meta__: %{schema: _}} = struct} = changeset,
        params,
        opts
      ) do
    create_changeset(
      schema_module,
      %{
        changeset
        | data: put_metadata(struct, source: schema_source)
      },
      params,
      opts
    )
  end

  def create_changeset({schema_source, schema_module}, struct, params, opts) do
    create_changeset(
      schema_module,
      put_metadata(struct, source: schema_source),
      params,
      opts
    )
  end

  def create_changeset(schema_module, struct_or_changeset, params, opts) do
    case opts[:create_changeset] do
      {mod, fun, args} ->
        apply(mod, fun, [struct_or_changeset, params] ++ args)

      {mod, fun} ->
        apply(mod, fun, [struct_or_changeset, params])

      fun when is_function(fun, 2) ->
        fun.(struct_or_changeset, params)

      fun when is_function(fun, 1) ->
        struct_or_changeset
        |> schema_module.changeset(params)
        |> fun.()

      changes when is_map(changes) ->
        schema_module.changeset(struct_or_changeset, Map.merge(params, changes))

      _ ->
        schema_module.changeset(struct_or_changeset, params)
    end
  end

  @doc group: "Struct API"
  @doc """
  Builds a struct from a queryable, optionally overriding its source.

  ### Examples

      iex> EctoShorts.CommonSchemas.build_struct(EctoShorts.Schema.Post)
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{source: "posts"}
      }

      iex> EctoShorts.CommonSchemas.build_struct({"custom_source", EctoShorts.Schema.Post})
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{source: "custom_source"}
      }
  """
  @spec build_struct(sourceable()) :: schema_struct()
  def build_struct({schema_source, schema_module}) do
    schema_module
    |> struct()
    |> put_metadata(
      state: :loaded,
      source: schema_source,
      prefix: get_prefix_for(schema_module)
    )
  end

  def build_struct(schema_module) do
    struct(schema_module)
  end
end
