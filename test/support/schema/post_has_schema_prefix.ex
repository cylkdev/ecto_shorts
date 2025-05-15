defmodule EctoShorts.Schema.PostHasSchemaPrefix do
  use Ecto.Schema

  @schema_prefix "custom_schema_prefix"

  schema "posts" do
    field :title, :string
  end
end
