defmodule EctoShorts.SchemaHelpers do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  @type schema_struct :: Ecto.Schema.t()

  @type schema_metadata :: Ecto.Schema.Metadata.t()

  @doc """
  Determine if item passed in is a Ecto Schema

  ## Example

    iex> EctoShorts.CommonSchemas.schema?(%EctoShorts.Schemas.Comment{})
    true

    iex> EctoShorts.CommonSchemas.schema?(%{some_map: 1})
    false

    iex> EctoShorts.CommonSchemas.schema?([%EctoShorts.Schemas.Comment{}])
    false
  """
  @spec schema?(schema_data :: schema_struct() | any()) :: boolean()
  def schema?(%{__meta__: %{schema: _}}), do: true
  def schema?(_), do: false

  @doc """
  Returns the `Ecto.Schema.Metadata` struct.
  """
  @spec get_schema_metadata(struct :: schema_struct()) :: schema_metadata()
  def get_schema_metadata(%{__meta__: meta}), do: meta

  @doc """
  Determine if any items in list are a schema

  ## Example

    iex> EctoShorts.SchemaHelpers.has_schemas?([%{some_map: 1}, %EctoShorts.Schemas.Comment{}])
    true

    iex> EctoShorts.SchemaHelpers.has_schemas?([%{some_map: 1}])
    false
  """
  @spec has_schemas?(schema_data :: list(schema_struct() | any())) :: boolean()
  def has_schemas?(items), do: Enum.any?(items, &schema?/1)

  @doc """
  Determine if all items in list are a schema

  ## Example

    iex> EctoShorts.SchemaHelpers.all_schemas?([%{some_map: 1}, %EctoShorts.Schemas.Comment{}])
    false

    iex> EctoShorts.SchemaHelpers.all_schemas?([%EctoShorts.Schemas.Comment{}])
    true
  """
  @spec all_schemas?(schema_data :: list(schema_struct() | any())) :: boolean()
  def all_schemas?(items), do: Enum.all?(items, &schema?/1)

  @doc """
  Returns `true` if the map has the atom key `:id` or
  the string key `"id"` and the value is not nil.

  ## Example

    iex> EctoShorts.SchemaHelpers.created?(%{id: 2})
    true

    iex> EctoShorts.SchemaHelpers.created?(%{"id" => 2})
    true

    iex> EctoShorts.SchemaHelpers.created?(%{item: 3})
    false
  """
  @spec created?(schema_data :: schema_struct() | any()) :: boolean()
  def created?(%{id: id}), do: !is_nil(id)
  def created?(%{"id" => id}), do: !is_nil(id)
  def created?(_), do: false

  @doc """
  Determine if all items in list has been created or not

  ## Example

    iex> EctoShorts.SchemaHelpers.all_created?([%{id: 2}, %{"id" => 5}])
    true

    iex> EctoShorts.SchemaHelpers.all_created?([%{"id" => 2}, %{item: 3}])
    false
  """
  @spec all_created?(schema_data :: list(schema_struct() | any())) :: boolean()
  def all_created?(items), do: Enum.all?(items, &created?/1)

  @doc """
  Returns `true` if any of the items passed as an argument to
  `EctoShorts.SchemaHelpers.created?/1` is `true`.

  ## Example

    iex> EctoShorts.SchemaHelpers.any_created?([%{id: 2}, %{"id" => 5}])
    true

    iex> EctoShorts.SchemaHelpers.any_created?([%{"id" => 2}, %{item: 3}])
    true

    iex> EctoShorts.SchemaHelpers.any_created?([%{test: 3}, %{item: 3}])
    false
  """
  @spec any_created?(schema_data :: list(schema_struct() | any())) :: boolean()
  def any_created?(items), do: Enum.any?(items, &created?/1)
end
