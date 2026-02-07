defmodule EctoShorts.SchemaHelpers do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Provides helper functions for common checks on
  Ecto schema data.
  """

  @doc """
  Recursively resolves the related schema module for an association
  key on a given schema module.

  This function handles both direct and `:through` associations.
  For `:through` associations, it recursively follows the
  association path until it reaches the final related schema.

  ## Examples

      iex> EctoShorts.SchemaHelpers.get_related_schema(EctoShorts.Schema.Post, :comments)
      EctoShorts.Schema.Comment

      iex> EctoShorts.SchemaHelpers.get_related_schema(EctoShorts.Schema.Post, :comments_authors)
      EctoShorts.Schema.User

      iex> EctoShorts.SchemaHelpers.get_related_schema(EctoShorts.Schema.Post, :does_not_exist)
      nil
  """
  def get_related_schema(schema, key), do: do_get_related_schema(schema, key)

  defp do_get_related_schema(nil, _), do: nil

  defp do_get_related_schema(schema, []), do: schema

  defp do_get_related_schema(schema, [key | path]) do
    schema
    |> do_get_related_schema(key)
    |> do_get_related_schema(path)
  end

  defp do_get_related_schema(schema, key) do
    case schema.__schema__(:association, key) do
      %{related: schema} -> schema
      %{through: path} -> do_get_related_schema(schema, path)
      _ -> nil
    end
  end

  @doc """
  Returns the declared Ecto type of a given field in a schema.

  This uses the schema's `__schema__/2` introspection to
  retrieve the field type, which can be a primitive type
  (e.g. `:string`, `:integer`) or a composite like
  `{:array, :string}`.

  ## Examples

      iex> EctoShorts.SchemaHelpers.schema_field_type(EctoShorts.Schema.Post, :title)
      :string

      iex> EctoShorts.SchemaHelpers.schema_field_type(EctoShorts.Schema.Post, :tags)
      {:array, :string}

  """
  def schema_field_type(schema, key), do: schema.__schema__(:type, key)

  @doc """
  Returns `true` if the value of `key` is an `Ecto.Association.NotLoaded`
  struct, otherwise `false.`

  ## Examples

      # Suppose we have a User struct whose :profile association hasn't been preloaded
      iex> post = %EctoShorts.Schema.Post{comments: %Ecto.Association.NotLoaded{}}
      ...> EctoShorts.SchemaHelpers.association_not_loaded?(post, :comments)
      true
  """
  def association_not_loaded?(schema_struct, key) do
    schema_struct
    |> Map.get(key)
    |> is_struct(Ecto.Association.NotLoaded)
  end

  @doc """
  Returns `true` if all items in the given list are Ecto
  schema structs, otherwise returns `false`.

  ## Examples

      # A list where every element is an Ecto schema struct
      iex> list = [%EctoShorts.Schema.Post{}, %EctoShorts.Schema.Comment{}]
      ...> EctoShorts.SchemaHelpers.all_schema_struct?(list)
      true

      # A list with mixed types (one struct, one map)
      iex> mixed_list = [%EctoShorts.Schema.Post{}, %{title: "Not a schema"}]
      ...> EctoShorts.SchemaHelpers.all_schema_struct?(mixed_list)
      false
  """
  def all_schema_struct?([]), do: false
  def all_schema_struct?(map) when map === %{}, do: false
  def all_schema_struct?(enum), do: Enum.all?(enum, &schema_struct?/1)

  @doc """
  Returns `true` if any item in the given list is an Ecto
  schema struct, otherwise returns `false`.

  ## Examples

      # A list containing at least one Ecto struct
      iex> items = [%EctoShorts.Schema.Post{}, %{id: 1, title: "Frank"}]
      ...> EctoShorts.SchemaHelpers.any_schema_struct?(items)
      true

      # A list of maps with no Ecto structs
      iex> maps = [%{title: "George"}, %{title: "Hannah"}]
      ...> EctoShorts.SchemaHelpers.any_schema_struct?(maps)
      false
  """
  def any_schema_struct?(values), do: Enum.any?(values, &schema_struct?/1)

  @doc """
  Returns `true` if the given value is an Ecto schema struct,
  otherwise returns `false`.

  ## Examples

      iex> EctoShorts.SchemaHelpers.schema_struct?(%EctoShorts.Schema.Post{})
      true

      iex> EctoShorts.SchemaHelpers.schema_struct?(%{title: "Not a schema"})
      false
  """
  def schema_struct?(%{__meta__: %{schema: schema}}), do: schema_module?(schema)
  def schema_struct?(_), do: false

  @doc """
  ...
  """
  def schema_module?(module) when is_atom(module) and module !== nil do
    function_exported?(module, :__schema__, 2)
  end

  def schema_module?(_), do: false

  def any_created?(%{id: id}), do: not is_nil(id)
  def any_created?(%{"id" => id}), do: not is_nil(id)
  def any_created?(_), do: false
end
