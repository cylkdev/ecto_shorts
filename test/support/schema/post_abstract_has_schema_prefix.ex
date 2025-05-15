defmodule EctoShorts.Schema.PostAbstractHasSchemaPrefix do
  @moduledoc false
  use Ecto.Schema

  @schema_prefix "custom_schema_prefix"

  schema "abstract table: posts" do
    field :title, :string
  end
end
