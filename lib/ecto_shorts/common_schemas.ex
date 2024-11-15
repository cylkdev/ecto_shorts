defmodule EctoShorts.CommonSchemas do
  @moduledoc """
  An interface for the `Ecto.Schema` abstract table syntax
  `{source :: binary(), query :: Ecto.Queryable.t()}`.
  This allows you to use the abstract table syntax in place
  of your ecto schema.

  For example:

  ```elixir
  EctoSchemas.Actions.all(YourSchema, %{id: 1})
  ```

  This can be written as:

  ```elixir
  EctoSchemas.Actions.all({"source", YourSchema}, %{id: 1})
  ```

  When the `source` and `queryable` is specified in this way
  the `source` in the tuple will take precedence over the
  `source` defined in the schema. This means you can use an
  ecto schema on any database table that has a matching schema.
  """
  @moduledoc since: "2.5.0"
  alias EctoShorts.QueryHelpers

  @doc """
  This function invokes the `&__schema__/1` callback function.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_reflection(EctoShorts.Schemas.Comment, :fields)
      iex> EctoShorts.CommonSchemas.get_schema_reflection({"comments", EctoShorts.Schemas.Comment}, :fields)
  """
  @doc since: "2.5.0"
  @spec get_schema_reflection(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          arg :: atom()
        ) :: any()
  def get_schema_reflection({_source, queryable}, arg) do
    queryable.__schema__(arg)
  end

  def get_schema_reflection(queryable, arg) do
    queryable.__schema__(arg)
  end

  @doc """
  This function invokes the `&__schema__/2` callback function.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_reflection(EctoShorts.Schemas.Comment, :type, :body)
      iex> EctoShorts.CommonSchemas.get_schema_reflection({"comments", EctoShorts.Schemas.Comment}, :type, :body)
  """
  @doc since: "2.5.0"
  @spec get_schema_reflection(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          arg1 :: atom(),
          arg2 :: atom()
        ) :: any()
  def get_schema_reflection({_source, queryable}, arg1, arg2) do
    queryable.__schema__(arg1, arg2)
  end

  def get_schema_reflection(queryable, arg1, arg2) do
    queryable.__schema__(arg1, arg2)
  end

  @doc """
  Returns a struct for the given schema.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_struct(EctoShorts.Schemas.Comment)
      iex> EctoShorts.CommonSchemas.get_schema_struct({"comments", EctoShorts.Schemas.Comment})
  """
  @doc since: "2.5.0"
  @spec get_schema_struct(query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()}) ::
          Ecto.Schema.t()
  def get_schema_struct({source, queryable}) do
    prefix = get_schema_prefix(queryable)

    queryable
    |> struct()
    |> put_meta(state: :loaded, source: source, prefix: prefix)
  end

  def get_schema_struct(queryable), do: struct(queryable)

  @doc """
  Returns the `prefix` specified in the schema.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_prefix(EctoShorts.Schemas.Comment)
      iex> EctoShorts.CommonSchemas.get_schema_prefix({"comments", EctoShorts.Schemas.Comment})
  """
  @doc since: "2.5.0"
  @spec get_schema_prefix(query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()}) ::
          binary() | nil
  def get_schema_prefix({_source, queryable}) do
    queryable.__schema__(:prefix)
  end

  def get_schema_prefix(queryable) do
    queryable.__schema__(:prefix)
  end

  @doc """
  Returns the `source` string.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_source(EctoShorts.Schemas.Comment)
      iex> EctoShorts.CommonSchemas.get_schema_source({"comments", EctoShorts.Schemas.Comment})
  """
  @doc since: "2.5.0"
  @spec get_schema_source(query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()}) ::
          binary()
  def get_schema_source({source, _queryable}) do
    source
  end

  def get_schema_source(queryable) do
    queryable.__schema__(:source)
  end

  @doc """
  Returns an `Ecto.Queryable`.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_queryable(EctoShorts.Schemas.Comment)
      iex> EctoShorts.CommonSchemas.get_schema_queryable({"comments", EctoShorts.Schemas.Comment})
  """
  @doc since: "2.5.0"
  @spec get_schema_queryable(query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()}) ::
          Ecto.Queryable.t()
  def get_schema_queryable({_source, queryable}) do
    queryable
  end

  def get_schema_queryable(%_{} = query) when is_struct(query, Ecto.Query) do
    QueryHelpers.get_queryable(query)
  end

  def get_schema_queryable(queryable) when is_atom(queryable) do
    queryable
  end

  @doc """
  Returns an `Ecto.Query`.

  ### Options

  Options do not apply when an `Ecto.Query` is given.

  See `EctoShorts.QueryHelpers.build_schema_query/2` for more information.

  ### Examples

      iex> EctoShorts.CommonSchemas.get_schema_query(%Ecto.Query{})
      iex> EctoShorts.CommonSchemas.get_schema_query(EctoShorts.Schemas.Comment)
      iex> EctoShorts.CommonSchemas.get_schema_query({"comments", EctoShorts.Schemas.Comment})
  """
  @doc since: "2.5.0"
  @spec get_schema_query(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()}
        ) :: Ecto.Query.t()
  def get_schema_query(query) do
    QueryHelpers.build_query_from(query)
  end

  @doc """
  Returns a struct for the given ecto schema.

  ### Options

      See `Ecto.put_meta/2` for more information.

  ### Examples

      iex> EctoShorts.CommonSchemas.put_meta(%EctoShorts.Schemas.Comment{}, state: :loaded, source: "comment", prefix: "prefix")
      %EctoShorts.Schemas.Comment{
        __meta__: %Ecto.Schema.Metadata{
          context: nil,
          prefix: "prefix",
          schema: EctoShorts.Schemas.Comment,
          source: "comment",
          state: :loaded
        }
      }

      iex> EctoShorts.CommonSchemas.put_meta(EctoShorts.Schemas.Comment, state: :loaded, source: "comment", prefix: "prefix")
      %EctoShorts.Schemas.Comment{
        __meta__: %Ecto.Schema.Metadata{
          context: nil,
          prefix: "prefix",
          schema: EctoShorts.Schemas.Comment,
          source: "comment",
          state: :loaded
        }
      }
  """
  @doc since: "2.5.0"
  @spec put_meta(
          query_or_schema_data ::
            Ecto.Schema.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          meta :: keyword()
        ) :: Ecto.Schema.t()
  def put_meta(%_{__meta__: state} = schema_data, meta) do
    Ecto.put_meta(schema_data,
      context: meta[:context] || state.context,
      prefix: meta[:prefix] || state.prefix,
      source: meta[:source] || state.source,
      state: meta[:state] || state.state || :loaded
    )
  end

  def put_meta(query, meta) do
    query
    |> get_schema_struct()
    |> put_meta(meta)
  end

  @doc """
  See `EctoShorts.CommonSchemas.prepare_changeset/3` for more information.
  """
  @doc since: "2.5.0"
  @spec prepare_changeset(
          query_or_struct_or_changeset ::
            Ecto.Queryable.t()
            | {binary(), Ecto.Queryable.t()}
            | Ecto.Schema.t()
            | Ecto.Changeset.t(),
          params :: map()
        ) :: Ecto.Changeset.t()
  def prepare_changeset(query_or_struct_or_changeset, params) do
    prepare_changeset(query_or_struct_or_changeset, params, [])
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
      See `&EctoShorts.CommonSchemas.get_schema_struct/1` for more information.

    * When a `queryable` is given the function is invoked using the `queryable` module
      of the `struct` and a new `struct` created from the `queryable` module.
      See `&EctoShorts.CommonSchemas.get_schema_struct/1` for more information.

  """
  @doc since: "2.5.0"
  @spec prepare_changeset(
          query_or_struct_or_changeset ::
            Ecto.Queryable.t()
            | {binary(), Ecto.Queryable.t()}
            | Ecto.Schema.t()
            | Ecto.Changeset.t(),
          params :: map(),
          opts :: keyword()
        ) :: Ecto.Changeset.t()
  def prepare_changeset(%{data: %{__meta__: %{schema: queryable}}} = changeset, params, opts) do
    prepare_changeset(queryable, changeset, params, opts)
  end

  def prepare_changeset(%{__meta__: %{schema: queryable}} = struct, params, opts) do
    prepare_changeset(queryable, struct, params, opts)
  end

  def prepare_changeset({source, queryable}, params, opts) do
    prepare_changeset(queryable, get_schema_struct({source, queryable}), params, opts)
  end

  def prepare_changeset(queryable, params, opts) do
    prepare_changeset(queryable, get_schema_struct(queryable), params, opts)
  end

  @doc """
  Returns an `Ecto.Changeset`.

  By default this function invokes the `changeset/2` function in module
  `queryable` with arguments `struct_or_changeset` and `params`
  (eg. `queryable.changeset(struct_or_changeset, params)`). For more
  granular control over how the changeset is built see the option
  `:changeset`.

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
  @doc since: "2.5.0"
  @spec prepare_changeset(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          struct_or_changeset :: Ecto.Schema.t() | Ecto.Changeset.t(),
          params :: map(),
          opts :: keyword()
        ) :: Ecto.Changeset.t()
  def prepare_changeset(
        {source, queryable},
        %{data: %{__meta__: %{schema: _}} = struct} = changeset,
        params,
        opts
      ) do
    prepare_changeset(
      queryable,
      %{changeset | data: put_meta(struct, source: source)},
      params,
      opts
    )
  end

  def prepare_changeset({source, queryable}, struct, params, opts) do
    prepare_changeset(queryable, put_meta(struct, source: source), params, opts)
  end

  def prepare_changeset(queryable, struct_or_changeset, params, opts) do
    case opts[:changeset] do
      {mod, fun, args} ->
        apply(mod, fun, [struct_or_changeset, params] ++ args)

      {mod, fun} ->
        apply(mod, fun, [struct_or_changeset, params])

      func when is_function(func, 2) ->
        func.(struct_or_changeset, params)

      func when is_function(func, 1) ->
        struct_or_changeset
        |> queryable.changeset(params)
        |> func.()

      _ ->
        queryable.changeset(struct_or_changeset, params)
    end
  end
end
