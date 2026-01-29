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

      EctoSchema.Actions.all({"posts", EctoShorts.Schema.PostAbstract}, %{id: 1})

  In this example, the query runs against the "posts" table instead of the default
  source defined in the schema. When both a `source` and `queryable` are provided,
  the explicit `source` takes precedence.

  This approach allows reusing schema modules across different tables,
  as long as the table structure matches the schema definition.
  """
  alias Ecto.Queryable
  alias EctoShorts.CommonQuery

  @doc """
  ...
  """
  def to_query(%Ecto.Query{} = query), do: query

  def to_query(source) do
    case normalize_source(source) do
      {nil, schema} -> Queryable.to_query(schema)
      {table_name, nil} -> Queryable.to_query(table_name)
      {table_name, schema} -> Queryable.to_query({table_name, schema})
    end
  end

  @doc """
  ...
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

  @doc """
  ...
  """
  def get_schema(source) do
    case normalize_source(source) do
      {_, schema} when is_atom(schema) and schema !== nil ->
        schema

      _ ->
        nil
    end
  end

  @doc """
  ...
  """
  def fetch_schema!(source) do
    with nil <- get_schema(source) do
      raise ArgumentError, "Ecto.Schema not found in source: #{inspect(source)}"
    end
  end

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

  @doc """
  Returns the `prefix` defined in the schema, if any.

  ## Examples

      iex> EctoShorts.CommonSchema.get_schema_prefix(EctoShorts.Schema.PostHasSchemaPrefix)
      "custom_schema_prefix"

      iex> EctoShorts.CommonSchema.get_schema_prefix({"posts", EctoShorts.Schema.PostAbstractHasSchemaPrefix})
      "custom_schema_prefix"

      iex> EctoShorts.CommonSchema.get_schema_prefix(%EctoShorts.Schema.PostHasSchemaPrefix{})
      "custom_schema_prefix"
  """
  def get_schema_prefix(%{data: %{__meta__: %{prefix: prefix}}}) do
    prefix
  end

  def get_schema_prefix(%{__meta__: %{prefix: prefix}}) do
    prefix
  end

  def get_schema_prefix(source) do
    with schema <- fetch_schema!(source) do
      schema.__schema__(:prefix)
    end
  end

  @doc """
  Returns the `Ecto.Schema.Metadata` struct from the given schema struct.

  ## Examples

      iex> EctoShorts.CommonSchema.get_schema_metadata(%EctoShorts.Schema.Post{})
      %Ecto.Schema.Metadata{schema: EctoShorts.Schema.Post, source: "posts", state: :built}

      iex> changeset = Ecto.Changeset.change(%EctoShorts.Schema.Post{})
      ...> EctoShorts.CommonSchema.get_schema_metadata(changeset)
      %Ecto.Schema.Metadata{schema: EctoShorts.Schema.Post, source: "posts", state: :built}
  """
  def get_schema_metadata(%{data: %{__meta__: meta}} = _changeset), do: meta
  def get_schema_metadata(%{__meta__: meta} = _schema_struct), do: meta
  def get_schema_metadata(source), do: source |> create_schema_struct() |> get_schema_metadata()

  @doc """
  Invokes the `__schema__/1` get_schema_reflection function.

  ### Examples

      iex> EctoShorts.CommonSchema.get_schema_reflection(EctoShorts.Schema.Post, :primary_key)
      [:id]

      iex> EctoShorts.CommonSchema.get_schema_reflection({"posts", EctoShorts.Schema.PostAbstract}, :primary_key)
      [:id]
  """
  def get_schema_reflection(source, arg) do
    with schema <- fetch_schema!(source) do
      schema.__schema__(arg)
    end
  end

  @doc """
  Invokes the `__schema__/2` reflection function.

  ### Examples

      iex> EctoShorts.CommonSchema.get_schema_reflection(EctoShorts.Schema.Post, :type, :id)
      :id

      iex> EctoShorts.CommonSchema.get_schema_reflection({"posts", EctoShorts.Schema.PostAbstract}, :type, :id)
      :id
  """
  def get_schema_reflection(source, arg1, arg2) do
    with schema <- fetch_schema!(source) do
      schema.__schema__(arg1, arg2)
    end
  end

  @doc """
  Updates the `__meta__` field on an Ecto schema struct.

  ### Options

    See `Ecto.put_meta/2` for more information.

  ### Examples

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
    |> create_schema_struct()
    |> put_schema_metadata(attrs)
  end

  @doc """
  ...
  """
  def create_schema_struct({nil, schema}) do
    struct(schema)
  end

  def create_schema_struct({source, schema}) do
    schema
    |> struct()
    |> put_schema_metadata(state: :loaded, source: source, prefix: get_schema_prefix(schema))
  end

  def create_schema_struct(source) do
    source
    |> normalize_source()
    |> create_schema_struct()
  end

  @doc """
  `(schema_struct :: Ecto.Schema.t(), params :: map(), options :: keyword())`
  `(changeset :: Ecto.Changeset.t(), params :: map(), options :: keyword())`
  `(query_source :: {source_name :: binary(), schema_module :: module()}, schema_struct :: Ecto.Schema.t(), options :: keyword())`
  `(query_source :: {source_name :: binary(), schema_module :: module()}, changeset :: Ecto.Changeset.t(), options :: keyword())`
  `(query_source :: {source_name :: binary(), schema_module :: module()}, params :: map(), options :: keyword())`
  `(schema_module :: module(), schema_struct :: Ecto.Schema.t(), options :: keyword())`
  `(schema_module :: module(), changeset :: Ecto.Changeset.t(), options :: keyword())`
  `(schema_module :: module(), params :: map(), options :: keyword())`
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
    create_changeset(schema, create_schema_struct({source, schema}), params, opts)
  end

  def create_changeset(schema, %{data: %{__meta__: _}} = changeset, opts) do
    create_changeset(schema, changeset, %{}, opts)
  end

  def create_changeset(schema, %{__meta__: _} = schema_struct, opts) do
    create_changeset(schema, schema_struct, %{}, opts)
  end

  def create_changeset(schema, params, opts) do
    create_changeset(schema, create_schema_struct(schema), params, opts)
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
        Ecto.Changeset.change(data_or_changeset, params)
      end
    end
  end

  defp apply_changeset!(schema, data_or_changeset, params, callback) do
    case callback do
      fun when is_function(fun, 3) ->
        term = fun.(schema, data_or_changeset, params)

        if changeset?(term) do
          term
        else
          raise_not_changeset!(term)
        end

      fun when is_function(fun, 2) ->
        term = fun.(data_or_changeset, params)

        if changeset?(term) do
          term
        else
          raise_not_changeset!(term)
        end

      fun when is_function(fun, 1) ->
        changeset =
          if function_exported?(schema, :changeset, 2) do
            schema.changeset(data_or_changeset, params)
          else
            Ecto.Changeset.change(data_or_changeset, params)
          end

        term = fun.(changeset)

        if changeset?(term) do
          term
        else
          raise_not_changeset!(term)
        end

      term ->
        raise ArgumentError,
              "Expected the value for option :changeset to be a 1-arity, 2-arity, or 3-arity function, got: #{inspect(term)}"
    end
  end

  defp raise_not_changeset!(term) do
    raise "Expected an Ecto.Changeset, got: #{inspect(term)}"
  end

  defp changeset?(%Ecto.Changeset{}), do: true
  defp changeset?(_), do: false

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
