defmodule EctoShorts.CommonSchema do
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
  @type schema_data :: Ecto.Schema.t()
  @type schema_metadata :: Ecto.Schema.Metadata.t()
  @type query_source :: schema_module() | {schema_source(), schema_module()}
  @type changeset :: Ecto.Changeset.t()
  @type prefix :: binary() | nil

  @type params :: map()
  @type opts :: keyword()

  @doc group: "Schema Reflection API"
  @doc """
  Returns the `Ecto.Schema.Metadata` struct from the given schema struct.
  """
  @spec metadata_for_schema(schema_data()) :: schema_metadata()
  def metadata_for_schema(%{__meta__: meta}), do: meta

  @doc group: "Schema Reflection API"
  @doc """
  Invokes the `__schema__/1` reflection function.

  ### Examples

      iex> EctoShorts.CommonSchema.reflection_for_schema(MyApp.UserSchema, :fields)
      iex> EctoShorts.CommonSchema.reflection_for_schema({"posts", MyApp.UserSchema}, :fields)
  """
  @spec reflection_for_schema(query_source(), any()) :: any()
  def reflection_for_schema({_, schema_module}, arg), do: schema_module.__schema__(arg)
  def reflection_for_schema(schema_module, arg), do: schema_module.__schema__(arg)

  @doc group: "Schema Reflection API"
  @doc """
  Invokes the `__schema__/2` reflection function.

  ### Examples

      iex> EctoShorts.CommonSchema.reflection_for_schema(EctoShorts.Schema.Post, :type, :id)
      :id

      iex> EctoShorts.CommonSchema.reflection_for_schema({"posts", EctoShorts.Schema.Post}, :type, :id)
      :id
  """
  @spec reflection_for_schema(query_source(), any(), any()) :: any()
  def reflection_for_schema({_, schema_module}, arg1, arg2),
    do: schema_module.__schema__(arg1, arg2)

  def reflection_for_schema(schema_module, arg1, arg2), do: schema_module.__schema__(arg1, arg2)

  @doc group: "Schema Reflection API"
  @doc """
  Returns the `prefix` defined in the schema, if any.

  ### Examples

      iex> EctoShorts.CommonSchema.prefix_for_schema(EctoShorts.Schema.Post)
      nil

      iex> EctoShorts.CommonSchema.prefix_for_schema({"posts", EctoShorts.Schema.Post})
      nil
  """
  @spec prefix_for_schema(query_source()) :: prefix()
  def prefix_for_schema({_, schema_module}), do: schema_module.__schema__(:prefix)
  def prefix_for_schema(schema_module), do: schema_module.__schema__(:prefix)

  @doc group: "Schema Reflection API"
  @doc """
  Returns the source (table name) for the schema.

  ### Examples

      iex> EctoShorts.CommonSchema.source_for_schema(EctoShorts.Schema.Post)
      "posts"

      iex> EctoShorts.CommonSchema.source_for_schema({"posts", EctoShorts.Schema.Post})
      "posts"
  """
  @spec source_for_schema(query_source()) :: schema_source()
  def source_for_schema({schema_source, _}), do: schema_source
  def source_for_schema(schema_module), do: schema_module.__schema__(:source)

  @doc group: "Schema Reflection API"
  @doc """
  Returns the schema module given {schema_source, schema_module}, a schema struct, or query struct.

  ### Examples

      iex> EctoShorts.CommonSchema.module_for_schema(EctoShorts.Schema.Post)
      EctoShorts.Schema.Post

      iex> EctoShorts.CommonSchema.module_for_schema({"posts", EctoShorts.Schema.Post})
      EctoShorts.Schema.Post

      iex> require Ecto.Query
      ...> EctoShorts.CommonSchema.module_for_schema(Ecto.Query.from(p in EctoShorts.Schema.Post))
      EctoShorts.Schema.Post
  """
  @spec module_for_schema(query() | query_source() | schema_data()) :: schema_module()
  def module_for_schema({_schema_source, schema_module}), do: schema_module
  def module_for_schema(%{__meta__: %{schema: schema_module}}), do: schema_module
  def module_for_schema(query), do: CommonQuery.schema_module_for_query(query)

  @doc group: "Schema API"
  @doc """
  Builds a struct from a queryable, optionally overriding its source.

  ### Examples

      iex> EctoShorts.CommonSchema.build_struct(EctoShorts.Schema.Post)
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{source: "posts"}
      }

      iex> EctoShorts.CommonSchema.build_struct({"custom_source", EctoShorts.Schema.Post})
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{source: "custom_source"}
      }
  """
  @spec build_struct(query_source()) :: schema_data()
  def build_struct({schema_source, schema_module}) do
    prefix = prefix_for_schema(schema_module)

    schema_module
    |> struct()
    |> put_schema_meta(state: :loaded, source: schema_source, prefix: prefix)
  end

  def build_struct(schema_module), do: struct(schema_module)

  @doc group: "Schema API"
  @doc """
  Updates the `__meta__` field on an Ecto schema struct.

  ### Options

      See `Ecto.put_meta/2` for more information.

  ### Examples

      iex> EctoShorts.CommonSchema.put_schema_meta(%EctoShorts.Schema.Post{}, state: :loaded, source: "custom_source", prefix: "custom_prefix")
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{
          state: :loaded,
          source: "posts",
          prefix: "custom_prefix"
        }
      }

      iex> EctoShorts.CommonSchema.put_schema_meta(EctoShorts.Schema.Post, state: :loaded, source: "custom_source", prefix: "custom_prefix")
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{
          state: :loaded,
          source: "posts",
          prefix: "custom_prefix"
        }
      }

      # the source given in the tuple takes precedence
      iex> EctoShorts.CommonSchema.put_schema_meta({"posts", EctoShorts.Schema.Post}, state: :loaded, source: "custom_source", prefix: "custom_prefix")
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{
          state: :loaded,
          source: "posts",
          prefix: "custom_prefix"
        }
      }
  """
  @spec put_schema_meta(query_source() | schema_data()) ::
          schema_data()
  @spec put_schema_meta(
          query_source() | schema_data(),
          opts()
        ) :: schema_data()
  def put_schema_meta(schema_data, opts \\ [])

  def put_schema_meta(%_{__meta__: state} = schema_data, opts) do
    Ecto.put_meta(schema_data,
      context: opts[:context] || state.context,
      prefix: opts[:prefix] || state.prefix,
      source: opts[:source] || state.source,
      state: opts[:state] || state.state || :loaded
    )
  end

  def put_schema_meta({schema_source, schema_module}, opts) do
    {schema_source, schema_module}
    |> build_struct()
    |> put_schema_meta(Keyword.delete(opts, :source))
  end

  def put_schema_meta(schema_module, opts) do
    schema_module
    |> build_struct()
    |> put_schema_meta(opts)
  end

  @doc group: "Changeset API"
  @doc """
  Builds an `Ecto.Changeset` given an Ecto queryable, Ecto schema, or
  Ecto changeset.

  This function resolves how to build the changeset based on the input
  type, and delegates to `build_changeset/4`.
  """
  @spec build_changeset(
          query_source() | schema_data() | changeset(),
          params()
        ) :: changeset()
  @spec build_changeset(
          query_source() | schema_data() | changeset(),
          params(),
          opts()
        ) :: changeset()
  def build_changeset(query_or_schema_or_changeset, params, opts \\ [])

  def build_changeset(%{data: %{__meta__: %{schema: schema_module}}} = changeset, params, opts) do
    build_changeset(schema_module, changeset, params, opts)
  end

  def build_changeset(%{__meta__: %{schema: schema_module}} = struct, params, opts) do
    build_changeset(schema_module, struct, params, opts)
  end

  def build_changeset({schema_source, schema_module}, params, opts) do
    build_changeset(schema_module, build_struct({schema_source, schema_module}), params, opts)
  end

  def build_changeset(schema_module, params, opts) do
    build_changeset(schema_module, build_struct(schema_module), params, opts)
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
          query_source(),
          schema_data() | changeset(),
          params(),
          opts()
        ) :: changeset()
  def build_changeset(
        {schema_source, schema_module},
        %{data: %{__meta__: %{schema: _}} = struct} = changeset,
        params,
        opts
      ) do
    build_changeset(
      schema_module,
      %{changeset | data: put_schema_meta(struct, source: schema_source)},
      params,
      opts
    )
  end

  def build_changeset({schema_source, schema_module}, struct, params, opts) do
    build_changeset(
      schema_module,
      put_schema_meta(struct, source: schema_source),
      params,
      opts
    )
  end

  def build_changeset(schema_module, struct_or_changeset, params, opts) do
    case opts[:build_changeset] do
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
end
