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

  def to_query(%Ecto.Query{} = query), do: query

  def to_query(source) do
    case normalize_source(source) do
      {nil, schema} -> Queryable.to_query(schema)
      {table_name, nil} -> Queryable.to_query(table_name)
      {table_name, schema} -> Queryable.to_query({table_name, schema})
    end
  end

  @doc """
  Invokes the `__schema__/1` get_schema_reflection function.

  ### Examples

      iex> EctoShorts.CommonSchema.get_schema_reflection(EctoShorts.Schema.Post, :primary_key)
      [:id]

      iex> EctoShorts.CommonSchema.get_schema_reflection({"posts", EctoShorts.Schema.PostAbstract}, :primary_key)
      [:id]
  """
  def get_schema_reflection(%Ecto.Query{} = query, arg) do
    query
    |> CommonQuery.get_query_source()
    |> get_schema_reflection(arg)
  end

  def get_schema_reflection({_, schema}, arg) when is_atom(schema), do: schema.__schema__(arg)

  def get_schema_reflection(schema, arg) when is_atom(schema), do: schema.__schema__(arg)

  @doc """
  Invokes the `__schema__/2` reflection function.

  ### Examples

      iex> EctoShorts.CommonSchema.get_schema_reflection(EctoShorts.Schema.Post, :type, :id)
      :id

      iex> EctoShorts.CommonSchema.get_schema_reflection({"posts", EctoShorts.Schema.PostAbstract}, :type, :id)
      :id
  """
  def get_schema_reflection(%Ecto.Query{} = query, arg1, arg2) do
    query
    |> CommonQuery.get_query_source()
    |> get_schema_reflection(arg1, arg2)
  end

  def get_schema_reflection({_, schema}, arg1, arg2) when is_atom(schema) do
    schema.__schema__(arg1, arg2)
  end

  def get_schema_reflection(schema, arg1, arg2) when is_atom(schema) do
    schema.__schema__(arg1, arg2)
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
  def get_schema_prefix(%Ecto.Query{} = query) do
    query
    |> CommonQuery.get_query_source()
    |> get_schema_prefix()
  end

  def get_schema_prefix(%{__meta__: %{prefix: schema_prefix}}), do: schema_prefix

  def get_schema_prefix({_, schema}) when is_atom(schema), do: schema.__schema__(:prefix)

  def get_schema_prefix(schema) when is_atom(schema), do: schema.__schema__(:prefix)

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
  def get_schema_source(%Ecto.Query{} = query) do
    CommonQuery.get_query_source(query)
  end

  def get_schema_source(%{__meta__: %{schema: schema, source: source}}) do
    {source, schema}
  end

  def get_schema_source({source, schema}) when is_atom(schema) do
    {source, schema}
  end

  def get_schema_source(schema) when is_atom(schema) do
    {schema.__schema__(:source), schema}
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
  def get_schema_metadata(%{data: %{__meta__: meta}}), do: meta
  def get_schema_metadata(%{__meta__: meta}), do: meta
  def get_schema_metadata(source), do: source |> to_schema_struct() |> get_schema_metadata()

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
  def put_schema_metadata(source_or_schema_data, params \\ [])

  def put_schema_metadata(%{__meta__: state} = schema_data, params) do
    Ecto.put_meta(schema_data,
      context: params[:context] || state.context,
      prefix: params[:prefix] || state.prefix,
      source: params[:source] || state.source,
      state: params[:state] || state.state || :loaded
    )
  end

  def put_schema_metadata(source, params) do
    source |> to_schema_struct() |> put_schema_metadata(params)
  end

  @doc """
  ...
  """
  def to_schema_struct(%Ecto.Query{} = query) do
    query
    |> CommonQuery.get_query_source()
    |> to_schema_struct()
  end

  def to_schema_struct({source, schema}) do
    schema
    |> struct()
    |> put_schema_metadata(state: :loaded, source: source, prefix: get_schema_prefix(schema))
  end

  def to_schema_struct(schema) do
    struct(schema)
  end

  @doc """
  `(schema_data :: Ecto.Schema.t(), params :: map(), options :: keyword())`
  `(changeset :: Ecto.Changeset.t(), params :: map(), options :: keyword())`
  `(query_source :: {source_name :: binary(), schema_module :: module()}, schema_data :: Ecto.Schema.t(), options :: keyword())`
  `(query_source :: {source_name :: binary(), schema_module :: module()}, changeset :: Ecto.Changeset.t(), options :: keyword())`
  `(query_source :: {source_name :: binary(), schema_module :: module()}, params :: map(), options :: keyword())`
  `(schema_module :: module(), schema_data :: Ecto.Schema.t(), options :: keyword())`
  `(schema_module :: module(), changeset :: Ecto.Changeset.t(), options :: keyword())`
  `(schema_module :: module(), params :: map(), options :: keyword())`
  """
  def create_changeset(%{data: %{__meta__: %{schema: schema}}} = changeset, params, opts) do
    create_changeset(schema, changeset, params, opts)
  end

  def create_changeset(%{__meta__: %{schema: schema}} = schema_data, params, opts) do
    create_changeset(schema, schema_data, params, opts)
  end

  def create_changeset({source, schema}, %{data: %{__meta__: _}} = changeset, opts) do
    create_changeset(schema, put_source(changeset, {source, schema}), %{}, opts)
  end

  def create_changeset({source, schema}, %{__meta__: _} = schema_data, opts) do
    create_changeset(schema, put_source(schema_data, {source, schema}), %{}, opts)
  end

  def create_changeset({source, schema}, params, opts) do
    create_changeset(schema, to_schema_struct({source, schema}), params, opts)
  end

  def create_changeset(schema, %{data: %{__meta__: _}} = changeset, opts) do
    create_changeset(schema, changeset, %{}, opts)
  end

  def create_changeset(schema, %{__meta__: _} = schema_data, opts) do
    create_changeset(schema, schema_data, %{}, opts)
  end

  def create_changeset(schema, params, opts) do
    create_changeset(schema, to_schema_struct(schema), params, opts)
  end

  # ---

  def create_changeset({source, schema}, %{__meta__: _} = schema_data, params, opts) do
    create_changeset(schema, put_source(schema_data, {source, schema}), params, opts)
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
        schema
        |> fun.(data_or_changeset, params)
        |> ensure_changeset!()

      fun when is_function(fun, 2) ->
        data_or_changeset
        |> fun.(params)
        |> ensure_changeset!()

      fun when is_function(fun, 1) ->
        changeset =
          if function_exported?(schema, :changeset, 2) do
            schema.changeset(data_or_changeset, params)
          else
            Ecto.Changeset.change(data_or_changeset, params)
          end

        changeset
        |> fun.()
        |> ensure_changeset!()

      term ->
        raise ArgumentError,
              "Expected the value for option :changeset to be a 1-arity, 2-arity, or 3-arity function, got: #{inspect(term)}"
    end
  end

  defp ensure_changeset!(term) do
    if changeset?(term) do
      term
    else
      raise "Expected an Ecto.Changeset, got: #{inspect(term)}"
    end
  end

  defp changeset?(%Ecto.Changeset{}), do: true
  defp changeset?(_), do: false

  defp put_source(%{data: schema_data} = changeset, {source, schema}) do
    %{changeset | data: put_schema_metadata(schema_data, source: source, schema: schema)}
  end

  defp put_source(%{__meta__: _} = schema_data, {source, schema}) do
    put_schema_metadata(schema_data, source: source, schema: schema)
  end

  defp put_source(data_or_changeset, _) do
    data_or_changeset
  end
end
