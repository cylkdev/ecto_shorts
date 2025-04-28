defmodule EctoShorts.CommonSchemas do
  @moduledoc since: "2.5.0"
  @moduledoc """
  This API provides helper functions for building ecto schemas.

  ## Polymorphic Associations

  This API provides an interface for ecto [polymorphic associations](https://hexdocs.pm/ecto/Ecto.Schema.html#belongs_to/3-polymorphic-associations).
  This allows you to use tuple `{binary(), Ecto.Queryable.t()}`
  in place on an ecto schema.

  ```elixir
  EctoSchemas.Actions.all({"users", MyApp.UserSchema}, %{id: 1})
  ```

  Here it explicitly sets the source to "users" and ecto will
  run the query against that database table name.

  When the `source` and `queryable` is passed in this way the
  `source` in the tuple will take precedence over the `source`
  defined in the schema. This allows you to use schemas on any
  database table provided it has a matching schema.
  """

  alias EctoShorts.CommonQueries

  @type source :: binary()

  @type query :: Ecto.Query.t()

  @type queryable :: Ecto.Queryable.t()

  @type source_queryable :: {source(), queryable()}

  @type changeset :: Ecto.Changeset.t()

  @type schema_struct :: Ecto.Schema.t()

  @type params :: map()

  @type opts :: keyword()

  @type schema_metadata :: Ecto.Schema.Metadata.t()

  @doc """
  Returns the `Ecto.Schema.Metadata` struct.
  """
  @spec get_schema_metadata(struct :: schema_struct()) :: schema_metadata()
  def get_schema_metadata(%{__meta__: meta}), do: meta

  @doc """
  This function invokes the `&__schema__/1` callback function.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_reflection(MyApp.UserSchema, :fields)
      iex> EctoShorts.CommonSchemas.get_schema_reflection({"users", MyApp.UserSchema}, :fields)
  """
  @spec get_schema_reflection(query :: queryable() | source_queryable(), arg :: any()) :: any()
  def get_schema_reflection({_, queryable}, arg), do: queryable.__schema__(arg)
  def get_schema_reflection(queryable, arg), do: queryable.__schema__(arg)

  @doc """
  This function invokes the `&__schema__/2` callback function.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_reflection(MyApp.UserSchema, :type, :body)
      iex> EctoShorts.CommonSchemas.get_schema_reflection({"users", MyApp.UserSchema}, :type, :body)
  """
  @spec get_schema_reflection(
          query :: queryable() | source_queryable(),
          arg1 :: any(),
          arg2 :: any()
        ) :: any()
  def get_schema_reflection({_, queryable}, arg1, arg2), do: queryable.__schema__(arg1, arg2)
  def get_schema_reflection(queryable, arg1, arg2), do: queryable.__schema__(arg1, arg2)

  @doc """
  Returns the `prefix` specified in the schema.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_prefix(MyApp.UserSchema)
      iex> EctoShorts.CommonSchemas.get_schema_prefix({"users", MyApp.UserSchema})
  """
  @spec get_schema_prefix(query :: queryable() | source_queryable()) :: binary() | nil
  def get_schema_prefix({_, queryable}), do: queryable.__schema__(:prefix)
  def get_schema_prefix(queryable), do: queryable.__schema__(:prefix)

  @doc """
  Returns the `source` string.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_source(MyApp.UserSchema)
      iex> EctoShorts.CommonSchemas.get_schema_source({"users", MyApp.UserSchema})
  """
  @spec get_schema_source(query :: queryable() | source_queryable()) :: binary()
  def get_schema_source({source, _}), do: source
  def get_schema_source(queryable), do: queryable.__schema__(:source)

  @doc """
  Returns an `Ecto.Queryable`.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_queryable(MyApp.UserSchema)
      iex> EctoShorts.CommonSchemas.get_schema_queryable({"users", MyApp.UserSchema})
  """
  @spec get_schema_queryable(
          query_or_schema :: query() | queryable() | source_queryable() | schema_struct()
        ) :: queryable()
  def get_schema_queryable(%{__meta__: %{schema: queryable}}), do: queryable
  def get_schema_queryable({_source, queryable}), do: queryable
  def get_schema_queryable(query), do: CommonQueries.get_query_schema(query)

  @doc """
  Returns a struct for the given schema.

  ### Examples

      iex> EctoShorts.CommonSchemas.create_schema_struct(MyApp.UserSchema)
      iex> EctoShorts.CommonSchemas.create_schema_struct({"users", MyApp.UserSchema})
  """
  @spec create_schema_struct(query :: queryable() | source_queryable()) :: schema_struct()
  def create_schema_struct({source, queryable}) do
    prefix = get_schema_prefix(queryable)

    queryable
    |> struct()
    |> put_schema_meta(state: :loaded, source: source, prefix: prefix)
  end

  def create_schema_struct(queryable), do: struct(queryable)

  @doc """
  Returns a struct for the given ecto schema.

  ### Options

      See `Ecto.put_meta/2` for more information.

  ### Examples

      iex> EctoShorts.CommonSchemas.put_schema_meta(%MyApp.UserSchema{}, state: :loaded, source: "comment", prefix: "prefix")
      %MyApp.UserSchema{
        __meta__: %Ecto.Schema.Metadata{
          context: nil,
          prefix: "prefix",
          schema: MyApp.UserSchema,
          source: "comment",
          state: :loaded
        }
      }

      iex> EctoShorts.CommonSchemas.put_schema_meta(MyApp.UserSchema, state: :loaded, source: "comment", prefix: "prefix")
      %MyApp.UserSchema{
        __meta__: %Ecto.Schema.Metadata{
          context: nil,
          prefix: "prefix",
          schema: MyApp.UserSchema,
          source: "comment",
          state: :loaded
        }
      }
  """
  @spec put_schema_meta(
          query_or_schema_struct :: queryable() | source_queryable() | schema_struct()
        ) ::
          schema_struct()
  @spec put_schema_meta(
          query_or_schema_struct :: queryable() | source_queryable() | schema_struct(),
          opts :: opts()
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

  def put_schema_meta(query, opts) do
    query
    |> create_schema_struct()
    |> put_schema_meta(opts)
  end

  @doc """
  Returns an `Ecto.Changeset`.

  This function is a wrapper for `EctoShorts.CommonSchemas.prepare_changeset/4` and invokes the function
  as follows based on the first argument:

    * When a `changeset` is given the function is invoked using the `queryable` module
      of the `struct` in the `data` key.

    * When a `struct` is given the function is invoked using the `queryable` module
      of the `struct`.

    * When `{source, queryable}` is given the function is invoked using the `queryable`
      module of the `struct` and a new `struct` created from `{source, queryable}`.
      See `&EctoShorts.CommonSchemas.create_schema_struct/1` for more information.

    * When a `queryable` is given the function is invoked using the `queryable` module
      of the `struct` and a new `struct` created from the `queryable` module.
      See `&EctoShorts.CommonSchemas.create_schema_struct/1` for more information.

  """
  @spec prepare_changeset(
          query_or_schema_or_changeset ::
            queryable() | source_queryable() | schema_struct() | changeset(),
          params :: params()
        ) :: changeset()
  @spec prepare_changeset(
          query_or_schema_or_changeset ::
            queryable() | source_queryable() | schema_struct() | changeset(),
          params :: params(),
          opts :: opts()
        ) :: changeset()
  def prepare_changeset(query_or_schema_or_changeset, params, opts \\ [])

  def prepare_changeset(%{data: %{__meta__: %{schema: queryable}}} = changeset, params, opts) do
    prepare_changeset(queryable, changeset, params, opts)
  end

  def prepare_changeset(%{__meta__: %{schema: queryable}} = struct, params, opts) do
    prepare_changeset(queryable, struct, params, opts)
  end

  def prepare_changeset({source, queryable}, params, opts) do
    prepare_changeset(queryable, create_schema_struct({source, queryable}), params, opts)
  end

  def prepare_changeset(queryable, params, opts) do
    prepare_changeset(queryable, create_schema_struct(queryable), params, opts)
  end

  @doc """
  Returns an `Ecto.Changeset`.

  By default this function invokes the `changeset/2` function in module
  `queryable` with arguments `struct_or_changeset` and `params`
  (eg. `queryable.changeset(struct_or_changeset, params)`). For more
  granular control over how the changeset is built see the option
  `:changeset`.

  when a source is explicitly given this function overwrites the source in the
  ecto schema struct metadata so that operation is executed against that database
  table.

  Raises if the result of the executed function does not return an
  `Ecto.Changeset` struct.

  ### Options

    * `changeset` - The operation used to build the changeset.

      * `{mod, fun, args}` - Invokes `Kernel.apply/3 with the given `mod`,
        `fun` and `args`. The first argument is the given struct or
        changeset. The second argument is the `params`. The `args` are
        appended to end of the argument list. This is equivalent to
        `apply(mod, fun, [struct_or_changeset, params] ++ args)`.

      * `{mod, fun}` - Equivalent to `{mod, fun, args}` when args is an
        empty list (eg. `{mod, fun, []}`).

      * `2-arity function` - The function is invoked with the given struct
        or changeset, and params (eg. `fun.(struct_or_changeset, params)`).

      * `1-arity function` - The function is invoked with a changeset
        (eg. `fun.(changeset)`). The changeset is built by invoking the
        `changeset/2` function in the `queryable` module
        (eg. `queryable.changeset(struct_or_changeset, params)`).

  """
  @spec prepare_changeset(
          query :: queryable() | source_queryable(),
          struct_or_changeset :: schema_struct() | changeset(),
          params :: params(),
          opts :: opts()
        ) :: changeset()
  def prepare_changeset(
        {source, queryable},
        %{data: %{__meta__: %{schema: _}} = struct} = changeset,
        params,
        opts
      ) do
    prepare_changeset(
      queryable,
      %{changeset | data: put_schema_meta(struct, source: source)},
      params,
      opts
    )
  end

  def prepare_changeset({source, queryable}, struct, params, opts) do
    prepare_changeset(
      queryable,
      put_schema_meta(struct, source: source),
      params,
      opts
    )
  end

  def prepare_changeset(queryable, struct_or_changeset, params, opts) do
    case opts[:prepare_changeset] do
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
