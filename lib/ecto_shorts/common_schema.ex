defmodule EctoShorts.CommonSchema do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Provides utility functions for source normalization, schema introspection,
  struct creation, and changeset building.

  ## Polymorphic associations

  This module supports [polymorphic associations](https://hexdocs.pm/ecto/Ecto.Schema.html#belongs_to/3-polymorphic-associations)
  by accepting a `{source :: binary(), queryable :: Ecto.Queryable.t()}` tuple
  in place of a schema module. This lets you query different tables using a
  shared schema definition.

      EctoShorts.Actions.all({"posts", EctoShorts.Schema.PostAbstract}, %{id: 1})

  When both a `source` and `queryable` are provided, the explicit `source`
  takes precedence over the table name defined in the schema.

  ## Accepted source forms

  Most functions in this module accept any of these as a `source`:

  * A schema module atom (e.g. `EctoShorts.Schema.Post`)
  * A table name string (e.g. `"posts"`)
  * A `{table_name, schema}` tuple (e.g. `{"posts", EctoShorts.Schema.PostAbstract}`)
  * A schema struct (e.g. `%EctoShorts.Schema.Post{}`)
  * An `Ecto.Changeset`
  * An `Ecto.Query`

  ## Quick start

      iex> EctoShorts.CommonSchema.get_schema_source(EctoShorts.Schema.Post)
      {"posts", EctoShorts.Schema.Post}

      iex> EctoShorts.CommonSchema.to_query(EctoShorts.Schema.Post)
      #Ecto.Query<from p0 in EctoShorts.Schema.Post>

      iex> EctoShorts.CommonSchema.create_changeset(EctoShorts.Schema.Post, %{title: "Hi"}, [])
      #Ecto.Changeset<...>

  See also `EctoShorts.CommonQuery` and `EctoShorts.CommonChanges`.
  """

  @moduledoc groups: [
               %{
                 title: "Source resolution",
                 description: "Functions that convert sources to queries or normalize source tuples."
               },
               %{
                 title: "Schema introspection",
                 description: "Functions that inspect schema metadata, fields, and prefixes."
               },
               %{
                 title: "Struct and changeset",
                 description: "Functions that build schema structs, update metadata, and create changesets."
               }
             ]
  alias Ecto.Changeset
  alias Ecto.Queryable
  alias EctoShorts.CommonQuery

  @doc group: "Source resolution"
  @doc """
  Converts a source into an `Ecto.Query`.

  Accepts an `Ecto.Query` (returned as-is), a schema module, a table name
  string, or a `{table_name, schema}` tuple. Normalizes the source via
  `normalize_source/1` before conversion.

  Returns an `Ecto.Query` struct.

  See also `normalize_source/1` and `EctoShorts.CommonQuery`.
  """
  def to_query(%Ecto.Query{} = query), do: query

  def to_query(source) do
    case normalize_source(source) do
      {nil, schema} -> Queryable.to_query(schema)
      {table_name, nil} -> Queryable.to_query(table_name)
      {table_name, schema} -> Queryable.to_query({table_name, schema})
    end
  end

  @doc group: "Source resolution"
  @doc """
  Normalizes a source into a `{table_name, schema}` tuple.

  Accepts a schema module, a table name string, a `{table_name, schema}`
  tuple, a schema struct, a changeset, or an `Ecto.Query`. Returns a
  `{binary() | nil, module() | nil}` tuple.

  Raises `ArgumentError` if the source cannot be recognized.

  See also `to_query/1` and `get_schema/1`.
  """
  def normalize_source(%{data: %{__meta__: %{source: source, schema: schema}}}) do
    {source, schema}
  end

  def normalize_source(%{__meta__: %{source: source, schema: schema}}) do
    {source, schema}
  end

  def normalize_source(%Ecto.Query{} = query) do
    query
    |> CommonQuery.get_query_source()
    |> normalize_source()
  end

  def normalize_source({nil, schema_module})
      when is_atom(schema_module) and schema_module !== nil do
    {nil, schema_module}
  end

  def normalize_source({table_name, nil}) when is_binary(table_name) and table_name !== "" do
    {table_name, nil}
  end

  def normalize_source({table_name, schema_module})
      when is_binary(table_name) and table_name !== "" and is_atom(schema_module) and
             schema_module !== nil do
    {table_name, schema_module}
  end

  def normalize_source(schema_module) when is_atom(schema_module) and schema_module !== nil do
    {nil, schema_module}
  end

  def normalize_source(table_name) when is_binary(table_name) and table_name !== "" do
    {table_name, nil}
  end

  def normalize_source(term) do
    raise ArgumentError, """
    Expected source to be one of the following:

    - `Ecto.Query.t()` - An Ecto.Query struct
    - `binary()` - The table name as a string
    - `Ecto.Schema.t()` - An Ecto.Schema module
    - `{binary(), Ecto.Schema.t()}` - The table name and the Ecto.Schema module
    - `{nil, Ecto.Schema.t()}` - No table name and an Ecto.Schema module
    - `{binary(), nil}` - The table name and no Ecto.Schema module

    got:

    #{inspect(term)}
    """
  end

  @doc group: "Source resolution"
  @doc """
  Extracts the schema module from a source.

  Accepts any input recognized by `normalize_source/1`. Returns the schema
  module atom, or `nil` if no schema is present.

  See also `normalize_source/1` and `get_schema_source/1`.
  """
  def get_schema(source) do
    case normalize_source(source) do
      {_, schema} when is_atom(schema) and schema !== nil ->
        schema

      _ ->
        nil
    end
  end

  @doc group: "Schema introspection"
  @doc """
  Returns a `{source, schema}` tuple where `source`
  is the database table name string or `nil` and `schema` is an
  Ecto schema module.

  ## Examples

      iex> EctoShorts.CommonSchema.get_schema_source(%EctoShorts.Schema.Post{})
      {"posts", EctoShorts.Schema.Post}

      iex> EctoShorts.CommonSchema.get_schema_source({"posts", EctoShorts.Schema.PostAbstract})
      {"posts", EctoShorts.Schema.PostAbstract}

      iex> EctoShorts.CommonSchema.get_schema_source(EctoShorts.Schema.Post)
      {"posts", EctoShorts.Schema.Post}

  See also `normalize_source/1` and `get_schema/1`.
  """
  def get_schema_source(%{from: _, joins: _} = query) do
    CommonQuery.get_query_source(query)
  end

  def get_schema_source(%{data: %{__meta__: %{schema: schema, source: source}}} = _changeset) do
    {source, schema}
  end

  def get_schema_source(%{__meta__: %{schema: schema, source: source}} = _schema_struct) do
    {source, schema}
  end

  def get_schema_source({source, schema})
      when is_nil(source) or (is_binary(source) and is_atom(schema) and schema !== nil) do
    {source, schema}
  end

  def get_schema_source(schema) when is_atom(schema) and schema !== nil do
    {schema.__schema__(:source), schema}
  end

  def get_schema_source(_) do
    nil
  end

  @doc group: "Schema introspection"
  @doc """
  Returns the `prefix` defined in the schema, if any.

  ## Examples

      iex> EctoShorts.CommonSchema.get_schema_prefix(EctoShorts.Schema.PostHasSchemaPrefix)
      "custom_schema_prefix"

      iex> EctoShorts.CommonSchema.get_schema_prefix({"posts", EctoShorts.Schema.PostAbstractHasSchemaPrefix})
      "custom_schema_prefix"

      iex> EctoShorts.CommonSchema.get_schema_prefix(%EctoShorts.Schema.PostHasSchemaPrefix{})
      "custom_schema_prefix"

  See also `get_schema_source/1` and `get_schema_metadata/1`.
  """
  def get_schema_prefix(%{data: %{__meta__: %{prefix: prefix}}}) do
    prefix
  end

  def get_schema_prefix(%{__meta__: %{prefix: prefix}}) do
    prefix
  end

  def get_schema_prefix(source) do
    with schema when schema !== nil <- get_schema(source) do
      schema.__schema__(:prefix)
    end
  end

  @doc group: "Schema introspection"
  @doc """
  Returns the `Ecto.Schema.Metadata` struct from the given schema struct.

  ## Examples

      iex> EctoShorts.CommonSchema.get_schema_metadata(%EctoShorts.Schema.Post{})
      %Ecto.Schema.Metadata{schema: EctoShorts.Schema.Post, source: "posts", state: :built}

      iex> changeset = Ecto.Changeset.change(%EctoShorts.Schema.Post{})
      ...> EctoShorts.CommonSchema.get_schema_metadata(changeset)
      %Ecto.Schema.Metadata{schema: EctoShorts.Schema.Post, source: "posts", state: :built}

  See also `put_schema_metadata/2` and `get_schema_prefix/1`.
  """
  def get_schema_metadata(%{data: %{__meta__: meta}} = _changeset), do: meta
  def get_schema_metadata(%{__meta__: meta} = _schema_struct), do: meta
  def get_schema_metadata(source), do: source |> build_struct() |> get_schema_metadata()

  @doc group: "Schema introspection"
  @doc """
  Invokes the `__schema__/1` reflection function with one argument.

  Returns the result of calling `schema.__schema__(arg)`, or `nil` when
  the source has no schema module.

  ## Examples

      iex> EctoShorts.CommonSchema.get_schema_reflection(EctoShorts.Schema.Post, :primary_key)
      [:id]

      iex> EctoShorts.CommonSchema.get_schema_reflection({"posts", EctoShorts.Schema.PostAbstract}, :primary_key)
      [:id]

  See also `get_schema_reflection/3` and `get_query_fields/2`.
  """
  def get_schema_reflection(source, arg) do
    with schema when schema !== nil <- get_schema(source) do
      schema.__schema__(arg)
    end
  end

  @doc group: "Schema introspection"
  @doc """
  Invokes the `__schema__/2` reflection function with two arguments.

  Returns the result of calling `schema.__schema__(arg1, arg2)`, or `nil`
  when the source has no schema module.

  ## Examples

      iex> EctoShorts.CommonSchema.get_schema_reflection(EctoShorts.Schema.Post, :type, :id)
      :id

      iex> EctoShorts.CommonSchema.get_schema_reflection({"posts", EctoShorts.Schema.PostAbstract}, :type, :id)
      :id

  See also `get_schema_reflection/2` and `get_query_fields/2`.
  """
  def get_schema_reflection(source, arg1, arg2) do
    with schema when schema !== nil <- get_schema(source) do
      schema.__schema__(arg1, arg2)
    end
  end

  @doc group: "Schema introspection"
  @doc """
  Returns the query fields for a source.

  Checks `opts` for an explicit `:query_fields` key first, then falls back
  to the schema's `:query_fields` reflection result.

  ## Examples

      iex> EctoShorts.CommonSchema.get_query_fields([], EctoShorts.Schema.Post)
      [:id, :title, :body, :permalink, :published, :views, :tags, :metadata, :author_id, :inserted_at, :updated_at]

      iex> EctoShorts.CommonSchema.get_query_fields([query_fields: [:title]], EctoShorts.Schema.Post)
      [:title]

  See also `get_schema_reflection/2`.
  """
  def get_query_fields(opts, source) do
    Keyword.get(opts, :query_fields, get_schema_reflection(source, :query_fields))
  end

  @doc group: "Struct and changeset"
  @doc """
  Updates the `__meta__` field on an Ecto schema struct.

  ## Options

  See `Ecto.put_meta/2` for supported keys (`:state`, `:source`, `:prefix`,
  `:context`).

  ## Examples

      iex> EctoShorts.CommonSchema.put_schema_metadata(%EctoShorts.Schema.Post{}, state: :loaded, source: "custom_source", prefix: "custom_prefix")
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{
          schema: EctoShorts.Schema.Post,
          state: :loaded,
          source: "custom_source",
          prefix: "custom_prefix"
        }
      }

      iex> EctoShorts.CommonSchema.put_schema_metadata(EctoShorts.Schema.Post, state: :loaded, source: "custom_source", prefix: "custom_prefix")
      %EctoShorts.Schema.Post{
        __meta__: %Ecto.Schema.Metadata{
          schema: EctoShorts.Schema.Post,
          state: :loaded,
          source: "custom_source",
          prefix: "custom_prefix"
        }
      }

      # the source given in the tuple takes precedence
      iex> EctoShorts.CommonSchema.put_schema_metadata({"posts", EctoShorts.Schema.PostAbstract}, state: :loaded, source: "custom_source", prefix: "custom_prefix")
      %EctoShorts.Schema.PostAbstract{
        __meta__: %Ecto.Schema.Metadata{
          schema: EctoShorts.Schema.PostAbstract,
          state: :loaded,
          source: "custom_source",
          prefix: "custom_prefix"
        }
      }

  See also `get_schema_metadata/1` and `create_schema_struct/1`.
  """
  def put_schema_metadata(source_or_schema_struct, attrs \\ %{})

  def put_schema_metadata(%{__meta__: meta} = schema_struct, attrs) do
    Ecto.put_meta(schema_struct,
      context: attrs[:context] || meta.context,
      prefix: attrs[:prefix] || meta.prefix,
      source: attrs[:source] || meta.source,
      state: attrs[:state] || meta.state || :loaded
    )
  end

  def put_schema_metadata(source, attrs) do
    source
    |> build_struct()
    |> put_schema_metadata(attrs)
  end

  @doc group: "Struct and changeset"
  @doc """
  Creates an Ecto schema struct from a source.

  Accepts a schema module, a `{table_name, schema}` tuple, or any input
  recognized by `normalize_source/1`. When a custom table name is provided,
  the struct's `__meta__` is updated to reflect that source.

  Returns an Ecto schema struct.

  See also `put_schema_metadata/2` and `normalize_source/1`.
  """
  def build_struct({nil, schema}) do
    struct(schema)
  end

  def build_struct({source, schema}) do
    schema
    |> struct()
    |> put_schema_metadata(state: :loaded, source: source, prefix: get_schema_prefix(schema))
  end

  def build_struct(source) do
    source
    |> normalize_source()
    |> build_struct()
  end

  @doc group: "Struct and changeset"
  @doc """
  Builds an `Ecto.Changeset` from various source/data/params combinations.

  Both 3-arity and 4-arity variants accept schema modules,
  `{source, schema}` tuples, schema structs, changesets, or plain param maps
  in flexible combinations. The goal is to always produce a changeset
  regardless of how the caller provides the data.

  When the `:changeset` option is present in `opts`, it is used instead of
  the schema's default `changeset/2`. It can be a 1-arity function
  (receives the built changeset), 2-arity (receives data and params), or
  3-arity (receives schema, data, and params).

  If no `:changeset` option is given and the schema exports `changeset/2`,
  that function is called. Otherwise falls back to `Ecto.Changeset.change/2`.

  Returns an `Ecto.Changeset`.

  ## Options

  * `:changeset` — a 1-arity, 2-arity, or 3-arity function to call instead
    of the schema's `changeset/2`.
  * `:query_fields` — list of field atoms to restrict to when creating the
    schema struct from params.

  See also `create_schema_struct/1` and `EctoShorts.CommonChanges`.
  """
  def create_changeset(%{data: %{__meta__: %{schema: schema}}} = changeset, params, opts) do
    create_changeset(schema, changeset, params, opts)
  end

  def create_changeset(%{__meta__: %{schema: schema}} = schema_struct, params, opts) do
    create_changeset(schema, schema_struct, params, opts)
  end

  def create_changeset({source, schema}, %{data: %{__meta__: _}} = changeset, opts) do
    create_changeset(schema, put_source(changeset, {source, schema}), %{}, opts)
  end

  def create_changeset({source, schema}, %{__meta__: _} = schema_struct, opts) do
    create_changeset(schema, put_source(schema_struct, {source, schema}), %{}, opts)
  end

  def create_changeset({source, schema}, params, opts) do
    create_changeset(schema, build_struct({source, schema}), params, opts)
  end

  def create_changeset(schema, %{data: %{__meta__: _}} = changeset, opts) do
    create_changeset(schema, changeset, %{}, opts)
  end

  def create_changeset(schema, %{__meta__: _} = schema_struct, opts) do
    create_changeset(schema, schema_struct, %{}, opts)
  end

  def create_changeset(schema, params, opts) do
    create_changeset(schema, build_struct(schema), params, opts)
  end

  # ---

  def create_changeset({source, schema}, %{__meta__: _} = schema_struct, params, opts) do
    create_changeset(schema, put_source(schema_struct, {source, schema}), params, opts)
  end

  def create_changeset({source, schema}, %{data: %{__meta__: _}} = changeset, params, opts) do
    create_changeset(schema, put_source(changeset, {source, schema}), params, opts)
  end

  def create_changeset(schema, data_or_changeset, params, opts) do
    if Keyword.has_key?(opts, :changeset) do
      apply_changeset!(schema, data_or_changeset, params, opts[:changeset])
    else
      if function_exported?(schema, :changeset, 2) do
        schema.changeset(data_or_changeset, params)
      else
        Changeset.change(data_or_changeset, params)
      end
    end
  end

  defp apply_changeset!(schema, data_or_changeset, params, callback) do
    case callback do
      fun when is_function(fun, 3) ->
        validate_changeset!(fun.(schema, data_or_changeset, params))

      fun when is_function(fun, 2) ->
        validate_changeset!(fun.(data_or_changeset, params))

      fun when is_function(fun, 1) ->
        changeset =
          if function_exported?(schema, :changeset, 2) do
            schema.changeset(data_or_changeset, params)
          else
            Changeset.change(data_or_changeset, params)
          end

        validate_changeset!(fun.(changeset))

      term ->
        raise ArgumentError,
              "Expected the value for option :changeset to be a 1-arity, 2-arity, or 3-arity function, got: #{inspect(term)}"
    end
  end

  defp validate_changeset!(%Changeset{} = changeset), do: changeset

  defp validate_changeset!(term) do
    raise "Expected an Ecto.Changeset, got: #{inspect(term)}"
  end

  defp put_source(%{data: schema_struct} = changeset, {source, schema}) do
    %{changeset | data: put_schema_metadata(schema_struct, source: source, schema: schema)}
  end

  defp put_source(%{__meta__: _} = schema_struct, {source, schema}) do
    put_schema_metadata(schema_struct, source: source, schema: schema)
  end

  defp put_source(data_or_changeset, _) do
    data_or_changeset
  end
end
