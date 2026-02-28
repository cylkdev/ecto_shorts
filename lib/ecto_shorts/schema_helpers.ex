defmodule EctoShorts.SchemaHelpers do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Provides helper predicates and introspection utilities for Ecto schema data.

  Use these functions to check whether values are Ecto schema structs,
  whether a record has been persisted, or to resolve association schemas
  and field types without reaching into Ecto internals directly.

  ## Quick start

      iex> EctoShorts.SchemaHelpers.schema_struct?(%EctoShorts.Schema.Post{})
      true

      iex> EctoShorts.SchemaHelpers.schema_module?(EctoShorts.Schema.Post)
      true

      iex> EctoShorts.SchemaHelpers.get_related_schema(EctoShorts.Schema.Post, :comments)
      EctoShorts.Schema.Comment

  See also `EctoShorts.CommonSchema` and `EctoShorts.CommonChanges`.
  """

  @doc since: "3.0.0"
  @doc """
  Recursively resolves the related schema module for an association
  key on a given schema module.

  Handles both direct and `:through` associations. For `:through`
  associations, follows the association path recursively until it
  reaches the final related schema.

  Returns `nil` when the association does not exist or the schema is `nil`.

  ## Examples

      iex> EctoShorts.SchemaHelpers.get_related_schema(EctoShorts.Schema.Post, :comments)
      EctoShorts.Schema.Comment

      iex> EctoShorts.SchemaHelpers.get_related_schema(EctoShorts.Schema.Post, :comments_authors)
      EctoShorts.Schema.User

      iex> EctoShorts.SchemaHelpers.get_related_schema(EctoShorts.Schema.Post, :does_not_exist)
      nil

  See also `schema_field_type/2` and `EctoShorts.CommonQuery.get_query_binding_source/2`.
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

  @doc since: "3.0.0"
  @doc """
  Returns the declared Ecto type of a given field in a schema.

  Uses the schema's `__schema__(:type, key)` introspection. The return
  value can be a primitive type (e.g. `:string`, `:integer`) or a
  composite type like `{:array, :string}`.

  ## Examples

      iex> EctoShorts.SchemaHelpers.schema_field_type(EctoShorts.Schema.Post, :title)
      :string

      iex> EctoShorts.SchemaHelpers.schema_field_type(EctoShorts.Schema.Post, :tags)
      {:array, :string}

  See also `get_related_schema/2` and `EctoShorts.CommonSchema.get_schema_reflection/3`.
  """
  def schema_field_type(schema, key), do: schema.__schema__(:type, key)

  @doc since: "3.0.0"
  @doc """
  Returns `true` if the value at `key` on `schema_struct` is an
  `Ecto.Association.NotLoaded` struct, otherwise `false`.

  Use this to check whether an association has been preloaded before
  accessing it, to avoid accidentally traversing unloaded associations.

  ## Examples

      iex> post = %EctoShorts.Schema.Post{comments: %Ecto.Association.NotLoaded{}}
      ...> EctoShorts.SchemaHelpers.association_not_loaded?(post, :comments)
      true

      iex> post = %EctoShorts.Schema.Post{comments: []}
      ...> EctoShorts.SchemaHelpers.association_not_loaded?(post, :comments)
      false

  See also `schema_struct?/1` and `EctoShorts.CommonChanges.preload_change_assoc/3`.
  """
  def association_not_loaded?(schema_struct, key) do
    schema_struct
    |> Map.get(key)
    |> is_struct(Ecto.Association.NotLoaded)
  end

  @doc since: "3.0.0"
  @doc """
  Returns `true` if all items in the given list are Ecto schema structs,
  otherwise `false`.

  Returns `false` for empty lists and empty maps.

  ## Examples

      iex> list = [%EctoShorts.Schema.Post{}, %EctoShorts.Schema.Comment{}]
      ...> EctoShorts.SchemaHelpers.all_schema_struct?(list)
      true

      iex> mixed_list = [%EctoShorts.Schema.Post{}, %{title: "Not a schema"}]
      ...> EctoShorts.SchemaHelpers.all_schema_struct?(mixed_list)
      false

  See also `any_schema_struct?/1` and `schema_struct?/1`.
  """
  def all_schema_struct?([]), do: false
  def all_schema_struct?(map) when map === %{}, do: false
  def all_schema_struct?(enum), do: Enum.all?(enum, &schema_struct?/1)

  @doc since: "3.0.0"
  @doc """
  Returns `true` if any item in the given list is an Ecto schema struct,
  otherwise `false`.

  ## Examples

      iex> items = [%EctoShorts.Schema.Post{}, %{id: 1, title: "Frank"}]
      ...> EctoShorts.SchemaHelpers.any_schema_struct?(items)
      true

      iex> maps = [%{title: "George"}, %{title: "Hannah"}]
      ...> EctoShorts.SchemaHelpers.any_schema_struct?(maps)
      false

  See also `all_schema_struct?/1` and `schema_struct?/1`.
  """
  def any_schema_struct?(values), do: Enum.any?(values, &schema_struct?/1)

  @doc since: "3.0.0"
  @doc """
  Returns `true` if the given value is an Ecto schema struct, otherwise `false`.

  A value is considered an Ecto schema struct when it has a `__meta__` field
  whose `:schema` key refers to a module that exports `__schema__/2`.

  ## Examples

      iex> EctoShorts.SchemaHelpers.schema_struct?(%EctoShorts.Schema.Post{})
      true

      iex> EctoShorts.SchemaHelpers.schema_struct?(%{title: "Not a schema"})
      false

  See also `schema_module?/1` and `all_schema_struct?/1`.
  """
  def schema_struct?(%{__meta__: %{schema: schema}}), do: schema_module?(schema)
  def schema_struct?(_), do: false

  @doc since: "3.0.0"
  @doc """
  Returns `true` if the given module exports `__schema__/2`, indicating
  it is an Ecto schema module.

  Returns `false` for `nil`, non-atom values, or modules that do not
  export `__schema__/2`.

  ## Examples

      iex> EctoShorts.SchemaHelpers.schema_module?(EctoShorts.Schema.Post)
      true

      iex> EctoShorts.SchemaHelpers.schema_module?(String)
      false

      iex> EctoShorts.SchemaHelpers.schema_module?(nil)
      false

  See also `schema_struct?/1`.
  """
  def schema_module?(module) when is_atom(module) and module !== nil do
    function_exported?(module, :__schema__, 2)
  end

  def schema_module?(_), do: false

  @doc """
  Returns `true` if the given map or struct has a non-nil `:id` (or `"id"`) key,
  indicating the record has been persisted.

  Accepts any map or schema struct. Returns `false` when there is no `:id`
  key or it is `nil`.

  ## Examples

      iex> EctoShorts.SchemaHelpers.any_created?(%EctoShorts.Schema.Post{id: 1})
      true

      iex> EctoShorts.SchemaHelpers.any_created?(%EctoShorts.Schema.Post{id: nil})
      false

      iex> EctoShorts.SchemaHelpers.any_created?(%{id: 42})
      true

  See also `schema_struct?/1`.
  """
  def any_created?(%{id: id}), do: id !== nil
  def any_created?(%{"id" => id}), do: id !== nil
  def any_created?(_), do: false
end
