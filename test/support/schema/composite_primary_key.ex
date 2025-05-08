defmodule EctoShorts.Schema.CompositePrimaryKey do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  require Ecto.Query

  @primary_key false

  schema "composite_primary_keys" do
    field(:post_id, :id, primary_key: true)
    field(:comment_id, :id, primary_key: true)

    field(:role, :string)

    timestamps()
  end

  @available_fields [
    :comment_id,
    :post_id,
    :role
  ]

  def changeset(model_or_changeset, attrs \\ %{}) do
    cast(model_or_changeset, attrs, @available_fields)
  end
end
